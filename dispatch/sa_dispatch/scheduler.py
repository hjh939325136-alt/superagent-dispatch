"""Persistent fixed-interval scheduler for synthetic read-only lab tasks.

Reservation and cursor advance commit together; submission uses the frozen
reservation's idempotency key. A crash between submission and acknowledgement
is repaired by resubmitting that same key, never by inventing a new task.
This module is opt-in and does not start a background service.
"""
from contextlib import closing
from dataclasses import asdict, replace
from datetime import datetime, timedelta
import json
import re
import sqlite3

from .clock import timestamp
from .models import DispatchError, TaskSpec

_UNSET = object()


class ScheduleExhausted(DispatchError):
    code = 'schedule_exhausted'


SCHEMA = """
CREATE TABLE IF NOT EXISTS schedules(
 id TEXT PRIMARY KEY, interval_seconds INTEGER NOT NULL CHECK(interval_seconds>0),
 task_template TEXT NOT NULL, next_fire_at TEXT NOT NULL,
 last_fire_key TEXT, enabled INTEGER NOT NULL CHECK(enabled IN(0,1)));
CREATE TABLE IF NOT EXISTS schedule_firings(
 fire_key TEXT PRIMARY KEY, schedule_id TEXT NOT NULL REFERENCES schedules(id),
 scheduled_at TEXT NOT NULL, task_template TEXT NOT NULL,
 task_id TEXT REFERENCES tasks(task_id), UNIQUE(schedule_id,scheduled_at));
CREATE TABLE IF NOT EXISTS schedule_events(
 seq INTEGER PRIMARY KEY, schedule_id TEXT NOT NULL, version INTEGER NOT NULL,
 kind TEXT NOT NULL, at TEXT NOT NULL, detail_code TEXT NOT NULL,
 UNIQUE(schedule_id,version,kind,at));
"""


def initialize_schema(conn):
    """Explicit additive migration of the original isolated scheduler tables."""
    for statement in SCHEMA.split(';'):
        if statement.strip():
            conn.execute(statement)
    additions = {
        'schedules': {'version': 'INTEGER NOT NULL DEFAULT 1',
                      'catchup': "TEXT NOT NULL DEFAULT 'all'",
                      'max_catchup': 'INTEGER NOT NULL DEFAULT 100',
                      'last_tick_at': 'TEXT',
                      'max_slots': 'INTEGER CHECK(max_slots IS NULL OR max_slots BETWEEN 1 AND 100000)'},
        'schedule_firings': {'version': 'INTEGER NOT NULL DEFAULT 1',
                             'outcome': "TEXT NOT NULL DEFAULT 'pending'", 'at': 'TEXT',
                             'failure_count': 'INTEGER NOT NULL DEFAULT 0',
                             'next_attempt_at': 'TEXT', 'last_attempt_at': 'TEXT',
                             'rejection_code': 'TEXT'},
    }
    for table, columns in additions.items():
        existing = {row[1] for row in conn.execute('PRAGMA table_info(' + table + ')')}
        for name, declaration in columns.items():
            if name not in existing:
                conn.execute('ALTER TABLE ' + table + ' ADD COLUMN ' + name + ' ' + declaration)
    conn.execute("UPDATE schedule_firings SET outcome='created' WHERE task_id IS NOT NULL AND outcome='pending'")


class Scheduler:
    def __init__(self, store):
        self.store = store
        self._sqlite_failures = 0
        self._sqlite_pause_until = None
        with store._tx() as conn:
            initialize_schema(conn)

    def _validate_template(self, template):
        spec = TaskSpec.from_dict(template) if isinstance(template, dict) else template
        if not isinstance(spec, TaskSpec) or spec.access != 'synthetic' or spec.egress != 'none':
            raise DispatchError('scheduler lab requires synthetic tasks with no egress')
        if self.store.kinds is None:
            raise DispatchError('scheduler requires a trusted task-kind registry')
        kind = self.store.kinds.get(spec.kind)
        if kind is None or not kind.enabled or kind.version != spec.kind_version or kind.side_effect != 'none':
            raise DispatchError('scheduler lab permits registered read-only kinds only')
        # Validate before persisting, without creating a task.
        spec = replace(spec, params=kind.validate(spec.params))
        if type(spec.max_attempts) is not int or spec.max_attempts < 1:
            raise DispatchError('invalid attempt limit')
        if any(not isinstance(cap, str) or not cap for cap in spec.required_caps):
            raise DispatchError('invalid capabilities')
        if any(any(not isinstance(value, str) or not value for value in asdict(ref).values())
               or ref.version == 'latest' for ref in spec.input_refs):
            raise DispatchError('fixed reference required')
        return spec

    def upsert(self, schedule_id, *, interval_seconds, task_template, next_fire_at, enabled=True,
               catchup='latest', max_catchup=3, max_slots=_UNSET):
        if not isinstance(schedule_id, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', schedule_id):
            raise DispatchError('invalid schedule id')
        if type(interval_seconds) is not int or interval_seconds < 1 or type(enabled) is not bool:
            raise DispatchError('positive interval and boolean enabled required')
        if catchup not in ('skip', 'latest', 'all') or type(max_catchup) is not int or not 1 <= max_catchup <= 1000:
            raise DispatchError('invalid catchup policy or bound')
        if max_slots is not _UNSET and max_slots is not None and (type(max_slots) is not int or not 1<=max_slots<=100000):
            raise DispatchError('max_slots must be null or an integer between 1 and 100000')
        when = timestamp(datetime.fromisoformat(next_fire_at.replace('Z', '+00:00')))
        try:
            spec = self._validate_template(task_template)
        except (ValueError, TypeError, KeyError, DispatchError):
            with self.store._tx() as conn:
                current = conn.execute('SELECT version FROM schedules WHERE id=?', (schedule_id,)).fetchone()
                self._event(conn, {'id':schedule_id,'version':current['version'] if current else 1},
                            'schedule.template_error', self.store._now(), 'upsert_template_rejected')
            raise
        encoded = json.dumps(asdict(spec), sort_keys=True, separators=(',', ':'), allow_nan=False)
        with self.store._tx() as conn:
            previous = conn.execute('SELECT * FROM schedules WHERE id=?', (schedule_id,)).fetchone()
            used = conn.execute('SELECT COUNT(*) FROM schedule_firings WHERE schedule_id=?',(schedule_id,)).fetchone()[0]
            bound = (previous['max_slots'] if previous else None) if max_slots is _UNSET else max_slots
            if bound is not None and bound < used:
                raise DispatchError('max_slots cannot be smaller than already reserved slots')
            was_exhausted = bool(previous and previous['max_slots'] is not None and used >= previous['max_slots'])
            exhausted = bound is not None and used >= bound
            if enabled and was_exhausted and exhausted:
                raise ScheduleExhausted('schedule_exhausted: raise max_slots or explicitly remove the bound to enable')
            if previous:
                # Already-fired slots must never be recycled by editing a schedule.
                last = conn.execute('SELECT MAX(scheduled_at) FROM schedule_firings WHERE schedule_id=?',
                                    (schedule_id,)).fetchone()[0]
                if last and when <= last:
                    raise DispatchError('next fire must be after every reserved slot')
            version = previous['version'] + 1 if previous else 1
            conn.execute('''INSERT INTO schedules(id,interval_seconds,task_template,next_fire_at,
                last_fire_key,enabled,version,catchup,max_catchup,max_slots) VALUES(?,?,?,?,NULL,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET interval_seconds=excluded.interval_seconds,
                task_template=excluded.task_template,next_fire_at=excluded.next_fire_at,enabled=excluded.enabled,
                version=excluded.version,catchup=excluded.catchup,max_catchup=excluded.max_catchup,
                max_slots=excluded.max_slots''',
                         (schedule_id, interval_seconds, encoded, when, int(enabled and not exhausted), version, catchup, max_catchup,bound))
            event_row={'id':schedule_id,'version':version}
            if previous and previous['max_slots'] is not None and bound is None:
                self._event(conn,event_row,'schedule.bound_removed',self.store._now(),'max_slots_removed')
            if exhausted and not was_exhausted:
                self._event(conn,event_row,'schedule.exhausted',self.store._now(),'max_slots_reached')
        return {'schedule_id': schedule_id, 'version': version}

    def _event(self, conn, row, kind, at, code):
        conn.execute('''INSERT OR IGNORE INTO schedule_events(schedule_id,version,kind,at,detail_code)
            VALUES(?,?,?,?,?)''', (row['id'], row['version'], kind, at, code))

    @staticmethod
    def _busy(error):
        code = getattr(error, 'sqlite_errorcode', None)
        return (code is not None and (code & 255) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)) or \
            str(error).lower() in ('database is locked', 'database table is locked', 'database is busy')

    def _link_rejected_task(self, conn, firing):
        """Retain crash-window task linkage and cancel only unpublished lab work."""
        task = conn.execute('SELECT * FROM tasks WHERE idem_key=?', (firing['fire_key'],)).fetchone()
        if task is None:
            return
        conn.execute('UPDATE schedule_firings SET task_id=? WHERE fire_key=?',
                     (task['task_id'], firing['fire_key']))
        published = conn.execute('SELECT 1 FROM transport_handoffs WHERE task_id=?', (task['task_id'],)).fetchone()
        if not published and task['access'] == 'synthetic' and task['side_effect'] == 'none' and \
                task['status'] in ('queued', 'waiting_device'):
            self.store._transition(conn, task, 'cancelled', 'schedule reservation rejected', 'scheduler')

    def _fail_firing(self, firing, now, code, *, transient=False):
        with self.store._tx() as conn:
            current = conn.execute('SELECT * FROM schedule_firings WHERE fire_key=?', (firing['fire_key'],)).fetchone()
            if current['outcome'] != 'pending':
                return
            failures = current['failure_count'] + 1
            rejected = not transient or failures >= 3
            if transient and rejected:
                code = 'sqlite_retry_exhausted'
            retry_at = None if rejected else timestamp(datetime.fromisoformat(now) + timedelta(seconds=2 ** (failures - 1)))
            conn.execute('''UPDATE schedule_firings SET outcome=?,failure_count=?,next_attempt_at=?,
                last_attempt_at=?,rejection_code=?,at=? WHERE fire_key=?''',
                ('rejected' if rejected else 'pending', failures, retry_at, now, code, now, firing['fire_key']))
            if rejected:
                self._link_rejected_task(conn, firing)
            self._event(conn, {'id':firing['schedule_id'],'version':firing['version']},
                        'schedule.rejected' if rejected else 'schedule.retry_scheduled', now, code)

    def tick(self, *, limit=100, synthetic_handoff=False):
        """Reserve bounded due slots and recover pending submissions.

        Disabling a schedule stops future reservations, not already reserved jobs.
        Permanent failures terminate; BUSY/LOCKED failures receive bounded backoff.
        """
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise DispatchError('tick limit must be between 1 and 1000')
        now = self.store._now()
        if self._sqlite_pause_until and now < self._sqlite_pause_until:
            return []
        try:
            result = self._tick(limit=limit, synthetic_handoff=synthetic_handoff)
        except sqlite3.OperationalError as error:
            if not self._busy(error):
                raise
            # A wholly locked ledger cannot persist a per-slot failure. Pause the
            # caller loop without sleeping, then fail visibly after three tries.
            self._sqlite_failures += 1
            if self._sqlite_failures >= 3:
                raise DispatchError('scheduler ledger busy after three attempts; operator recovery required') from error
            self._sqlite_pause_until = timestamp(datetime.fromisoformat(now) + timedelta(seconds=2 ** (self._sqlite_failures - 1)))
            return []
        self._sqlite_failures = 0
        self._sqlite_pause_until = None
        return result

    def _tick(self, *, limit, synthetic_handoff):
        now = self.store._now()
        with self.store._tx() as conn:
            pending = conn.execute("""SELECT COUNT(*) FROM schedule_firings WHERE outcome='pending'
                AND (next_attempt_at IS NULL OR next_attempt_at<=?)""", (now,)).fetchone()[0]
            capacity = max(0, limit - pending)
            rows = conn.execute('SELECT * FROM schedules WHERE enabled=1 ORDER BY next_fire_at,id').fetchall()
            for row in rows:
                used = None
                if row['max_slots'] is not None:
                    used=conn.execute('SELECT COUNT(*) FROM schedule_firings WHERE schedule_id=?',(row['id'],)).fetchone()[0]
                    if used>=row['max_slots']:
                        conn.execute('UPDATE schedules SET enabled=0 WHERE id=?',(row['id'],))
                        self._event(conn,row,'schedule.exhausted',now,'max_slots_reached')
                        continue
                if row['last_tick_at'] and now < row['last_tick_at']:
                    self._event(conn, row, 'schedule.clock_skew', now, 'clock_before_last_tick')
                    continue
                conn.execute('UPDATE schedules SET last_tick_at=? WHERE id=?', (now, row['id']))
                if row['next_fire_at'] > now or capacity == 0:
                    continue
                try:
                    spec = self._validate_template(json.loads(row['task_template']))
                    if synthetic_handoff and spec.input_refs:
                        raise DispatchError('scheduled handoff requires empty input refs')
                except (ValueError, TypeError, KeyError, DispatchError):
                    self._event(conn, row, 'schedule.template_error', now, 'invalid_template')
                    continue
                first = datetime.fromisoformat(row['next_fire_at'])
                count = int((datetime.fromisoformat(now) - first).total_seconds() // row['interval_seconds']) + 1
                if used is not None:
                    count=min(count,row['max_slots']-used)
                # Each missed slot is auditable. Refuse pathological backlog instead of silently truncating it.
                if count > 100000:
                    self._event(conn, row, 'schedule.backlog_exceeded', now, 'over_100000_slots')
                    continue
                keep = min(count, row['max_catchup'] if row['catchup'] == 'all' else 1, capacity)
                for index in range(count):
                    slot = timestamp(first + timedelta(seconds=index * row['interval_seconds']))
                    key = f"sched:{row['id']}:v{row['version']}:{slot}"
                    frozen = asdict(spec)
                    frozen['idem_key'] = key
                    outcome = 'pending' if index >= count - keep else 'skipped_catchup'
                    conn.execute('''INSERT INTO schedule_firings(fire_key,schedule_id,scheduled_at,
                        task_template,task_id,version,outcome,at) VALUES(?,?,?,?,NULL,?,?,?)''',
                        (key, row['id'], slot, json.dumps(frozen, sort_keys=True), row['version'], outcome, now))
                next_at = timestamp(first + timedelta(seconds=count * row['interval_seconds']))
                conn.execute('UPDATE schedules SET next_fire_at=?,last_fire_key=? WHERE id=?', (next_at, key, row['id']))
                if used is not None and used+count>=row['max_slots']:
                    conn.execute('UPDATE schedules SET enabled=0 WHERE id=?',(row['id'],))
                    self._event(conn,row,'schedule.exhausted',now,'max_slots_reached')
                capacity -= keep
            reserved = conn.execute('''SELECT * FROM schedule_firings WHERE outcome='pending'
                AND (next_attempt_at IS NULL OR next_attempt_at<=?)
                ORDER BY COALESCE(last_attempt_at,''),scheduled_at,fire_key LIMIT ?''', (now,limit)).fetchall()
        result = []
        for firing in reserved:
            try:
                spec = self._validate_template(json.loads(firing['task_template']))
                if synthetic_handoff and spec.input_refs:
                    raise DispatchError('scheduled handoff requires empty input refs')
            except (ValueError, TypeError, KeyError, DispatchError):
                self._fail_firing(firing, now, 'invalid_reserved_template')
                continue
            try:
                task, reused = self.store.submit(spec)
            except sqlite3.OperationalError as error:
                if not self._busy(error):
                    raise
                self._fail_firing(firing, now, 'sqlite_busy', transient=True)
                continue
            except (DispatchError, ValueError, TypeError) as error:
                code = 'idempotency_conflict' if getattr(error, 'code', None) == 'E_IDEMPOTENCY_CONFLICT' else 'submission_rejected'
                self._fail_firing(firing, now, code)
                continue
            with self.store._tx() as conn:
                current = conn.execute('SELECT outcome FROM schedule_firings WHERE fire_key=?', (firing['fire_key'],)).fetchone()
                if current['outcome'] == 'rejected':
                    self._link_rejected_task(conn, firing)
                    continue
                if synthetic_handoff:
                    conn.execute('INSERT OR IGNORE INTO transport_handoffs VALUES(?,?,?)',
                                 (task.task_id, 'scheduler', now))
                conn.execute("UPDATE schedule_firings SET task_id=?,outcome='created',at=?,last_attempt_at=?,next_attempt_at=NULL WHERE fire_key=? AND outcome='pending'",
                             (task.task_id, now, now, firing['fire_key']))
            result.append({'fire_key': firing['fire_key'], 'task_id': task.task_id, 'deduplicated': reused})
        return result

    def firings(self):
        with closing(self.store._connect()) as conn:
            return [dict(row) for row in conn.execute('SELECT * FROM schedule_firings ORDER BY scheduled_at,fire_key')]

    def status(self):
        with closing(self.store._connect()) as conn:
            return {'schedules': [dict(row) for row in conn.execute('SELECT * FROM schedules ORDER BY id')],
                    'firings': [dict(row) for row in conn.execute('SELECT * FROM schedule_firings ORDER BY scheduled_at,fire_key')],
                    'events': [dict(row) for row in conn.execute('SELECT * FROM schedule_events ORDER BY seq')]}
