"""Append-only M0 metadata projection; never mutates or reads source journals.

Callers supply a durable source namespace/id and an explicit trace mapping. Source
ids must include the snapshot/revision for mutable sources such as handoffs.
Unparseable input is represented by a safe parse_error event, not raw payloads.
"""
import json
import re
import secrets
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

from .clock import Clock, timestamp

SCHEMA = """
CREATE TABLE IF NOT EXISTS unified_events (
 seq INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
 source TEXT NOT NULL, source_id TEXT NOT NULL,
 trace_id TEXT NOT NULL, task_id TEXT, attempt_epoch INTEGER,
 actor TEXT NOT NULL, component TEXT NOT NULL, kind TEXT NOT NULL,
 at TEXT NOT NULL, refs_json TEXT NOT NULL,
 class TEXT NOT NULL CHECK(class IN ('public','internal','local_only')),
 metadata_json TEXT NOT NULL, UNIQUE(source, source_id));
CREATE INDEX IF NOT EXISTS unified_events_trace ON unified_events(trace_id,at,seq);
"""
_TOKEN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.:@/#+-]{0,199}$')
_REF = re.compile(r'^[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[^\s@#]+@[^\s@#]+#(?:sha256:)?[0-9a-f]{64}$')
_KINDS = frozenset(('task.submitted task.leased task.running task.succeeded task.failed '
    'task.unknown task.reconciled task.cancelled task.queued task.waiting_device '
    'task.needs_human task.reconciling approval.requested approval.decided '
    'budget.exceeded budget.reserved budget.settled budget.uncertain worker.registered worker.revoked worker.heartbeat_missed '
    'backup.completed restore.verified permission_conflict revocation.propagated '
    'adapter.parse_error hook.observed handoff.offered handoff.accepted '
    'handoff.completed handoff.rejected handoff.expired handoff.observed campaign.context_read campaign.paused '
    'host.observed host.unknown reconciliation.observed knowledge.changed '
    'schedule.backlog_exceeded schedule.bound_removed schedule.clock_skew '
    'schedule.exhausted schedule.rejected schedule.retry_scheduled schedule.template_error').split())
_STATUSES = frozenset(('queued waiting_device leased running succeeded no_new failed '
    'unknown cancelled reconciling needs_human awaiting_approval failed_retryable failed_final').split())
_META_VALUES = {
    'from_status': _STATUSES, 'to_status': _STATUSES,
    'decision': frozenset(('allow','deny','observed')),
    'mode': frozenset(('audit','read_only')),
    'hook_event': frozenset(('PreToolUse','PostToolUse','SessionStart','SessionEnd','Stop','UserPromptSubmit','SubagentStart','SubagentStop','Notification','PermissionRequest','PreCompact')),
    'handoff_state': frozenset(('offered','accepted','completed','rejected','unconfirmed','observed')),
    'error_code': frozenset(('invalid_source_record',)),
}


def initialize_schema(conn):
    """Add tables in the caller transaction, with no executescript implicit commit."""
    for statement in SCHEMA.split(';'):
        if statement.strip():
            conn.execute(statement)


def _token(value):
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise ValueError('invalid metadata identifier')
    return value


def _at(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return timestamp(parsed)


def _ulid(at):
    millis = int(datetime.fromisoformat(at).timestamp() * 1000)
    if not 0 <= millis < 2 ** 48:
        raise ValueError('ULID timestamp outside range')
    value = (millis << 80) | secrets.randbits(80)
    alphabet = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
    chars = []
    for _ in range(26):
        chars.append(alphabet[value & 31]); value >>= 5
    return ''.join(reversed(chars))


class UnifiedEventStore:
    def __init__(self, path, *, clock=None):
        self.path = Path(path)
        self.clock = clock or Clock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn, conn:
            initialize_schema(conn)

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def append(self, *, source, source_id, trace_id, actor, component, kind,
               at=None, task_id=None, attempt_epoch=None, refs=(),
               data_class='internal', metadata=None):
        for value in (source, source_id, trace_id, actor, component):
            _token(value)
        if task_id is not None: _token(task_id)
        if attempt_epoch is not None and (type(attempt_epoch) is not int or attempt_epoch < 0):
            raise ValueError('invalid attempt epoch')
        if kind not in _KINDS or data_class not in ('public','internal','local_only'):
            raise ValueError('unsupported kind or data class')
        refs = list(refs)
        if any(not isinstance(ref, str) or not _REF.fullmatch(ref) for ref in refs):
            raise ValueError('fixed hashed reference required')
        metadata = dict(metadata or {})
        if any(key not in _META_VALUES or value not in _META_VALUES[key]
               for key, value in metadata.items()):
            raise ValueError('metadata must use allowlisted enum values')
        at = _at(at) if at else timestamp(self.clock.now())
        # Persist only declared fields. No raw source body, summary or exception text.
        values = dict(event_id=_ulid(at), source=source, source_id=source_id,
                      trace_id=trace_id, task_id=task_id, attempt_epoch=attempt_epoch,
                      actor=actor, component=component, kind=kind, at=at,
                      refs_json=json.dumps(refs), **{'class': data_class},
                      metadata_json=json.dumps(metadata, sort_keys=True))
        with closing(self._connect()) as conn, conn:
            conn.execute('BEGIN IMMEDIATE')
            existing = conn.execute('SELECT * FROM unified_events WHERE source=? AND source_id=?',
                                    (source, source_id)).fetchone()
            if existing:
                if any(existing[key] != value for key, value in values.items()
                       if key != 'event_id'):
                    raise ValueError('source id already represents a different event')
                return existing['event_id']
            conn.execute('INSERT INTO unified_events (' + ','.join(values) + ') VALUES (' +
                         ','.join('?' for _ in values) + ')', tuple(values.values()))
        return values['event_id']

    def _ingest(self, source, source_id, trace_id, parse):
        # Validate caller-owned routing before touching untrusted payloads.
        for value in (source, source_id, trace_id): _token(value)
        try:
            values = parse()
            return self.append(source=source, source_id=source_id, trace_id=trace_id, **values)
        except (ValueError, TypeError, KeyError, AttributeError):
            # Stable timestamp makes replay of a malformed source id idempotent.
            with closing(self._connect()) as conn:
                prior = conn.execute('SELECT event_id,kind FROM unified_events WHERE source=? AND source_id=?',
                                     (source,source_id)).fetchone()
            if prior:
                if prior['kind'] == 'adapter.parse_error': return prior['event_id']
                raise ValueError('source id conflicts with existing event')
            return self.append(source=source, source_id=source_id, trace_id=trace_id,
                actor='adapter', component=source, kind='adapter.parse_error',
                metadata={'error_code':'invalid_source_record'})

    def ingest_ledger(self, row, *, source_id, trace_id, refs=(), source='dispatch'):
        def parse():
            row_dict = dict(row)
            new, old = row_dict['to_status'], row_dict.get('from_status')
            if new not in _STATUSES or (old is not None and old not in _STATUSES):
                raise ValueError('invalid ledger status')
            if old is None and new == 'queued': kind = 'task.submitted'
            elif new == 'awaiting_approval': kind = 'approval.requested'
            elif new == 'no_new': kind = 'task.succeeded'
            elif new in ('failed_final','failed_retryable'): kind = 'task.failed'
            elif old in ('unknown','reconciling') and new == 'succeeded': kind = 'task.reconciled'
            else: kind = 'task.' + new
            metadata = {'to_status': new}
            if old is not None: metadata['from_status'] = old
            return dict(task_id=row_dict['task_id'], attempt_epoch=row_dict.get('fence'),
                actor=row_dict['actor'],component='dispatch',kind=kind,at=row_dict['at'],
                refs=refs,metadata=metadata)
        return self._ingest(source,source_id,trace_id,parse)

    def ingest_hook(self, row, *, source_id, trace_id, source='audit-hook'):
        def parse():
            row_dict = dict(row)
            body = json.loads(row_dict['body']) if 'body' in row_dict else row_dict
            if body.get('schemaVersion') != 1: raise ValueError('unsupported hook schema')
            return dict(actor='audit-hook',component='audit-hook',kind='hook.observed',at=body['at'],
                        metadata={'hook_event':body['event'],'mode':body['mode'],'decision':body['decision']})
        return self._ingest(source,source_id,trace_id,parse)

    def ingest_handoff(self, row, *, source_id, trace_id, refs=(), source='handoff'):
        def parse():
            row_dict = dict(row)
            _token(row_dict.get('handoff_id',row_dict.get('id')))
            # Producer completion is not recipient business completion.
            state = row_dict.get('state')
            if state is None:
                status = row_dict['status']
                state = {'available_receipt_unconfirmed':'unconfirmed',
                         'received':'observed'}.get(status)
            if state not in _META_VALUES['handoff_state']: raise ValueError('unknown handoff state')
            if state == 'completed' and not refs: raise ValueError('completion evidence required')
            kind = 'handoff.' + ('observed' if state == 'unconfirmed' else state)
            at = row_dict.get('at',row_dict.get('updatedAt'))
            if not isinstance(at, str): raise ValueError('source timestamp required')
            return dict(actor='handoff-adapter',component='mgmt',kind=kind,
                at=at,refs=refs,
                metadata={'handoff_state':state})
        return self._ingest(source,source_id,trace_id,parse)

    def query(self, trace_id, *, limit=1000):
        _token(trace_id)
        if type(limit) is not int or not 1 <= limit <= 10000: raise ValueError('invalid limit')
        with closing(self._connect()) as conn:
            rows = conn.execute('SELECT * FROM unified_events WHERE trace_id=? ORDER BY at,seq LIMIT ?',
                                (trace_id,limit)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item['refs'] = json.loads(item.pop('refs_json'))
            item['metadata'] = json.loads(item.pop('metadata_json'))
            result.append(item)
        return result

    def project_task(self, store, task_id, *, trace_id=None):
        """Refresh one committed task, with receipts belonging to each event's fence."""
        if self.path.resolve() != store.path.resolve():
            raise ValueError('projection and ledger must share the configured database')
        with closing(store._connect()) as conn:
            conn.execute('BEGIN')
            rows = conn.execute('SELECT * FROM events WHERE task_id=? ORDER BY seq', (task_id,)).fetchall()
            evidence = conn.execute('SELECT * FROM evidence WHERE task_id=?', (task_id,)).fetchall()
            hosts = conn.execute('SELECT * FROM host_attempts WHERE task_id=? AND report_ref IS NOT NULL', (task_id,)).fetchall()
            claudes = conn.execute('SELECT * FROM claude_attempts WHERE task_id=? AND report_ref IS NOT NULL', (task_id,)).fetchall()
        for row in rows:
            refs = []
            if row['to_status'] in ('succeeded', 'no_new', 'failed'):
                for item in evidence:
                    if item['fence'] != row['fence']:
                        continue
                    if item['artifact_hash']:
                        refs.append(f"dispatch/receipt/{task_id}@{row['fence']}#{item['artifact_hash']}")
                    if item['ref_json']:
                        ref = json.loads(item['ref_json'])
                        refs.append(f"{ref['owner']}/{ref['kind']}/{ref['id']}@{ref['version']}#{ref['hash']}")
            self.ingest_ledger(row, source_id=str(row['seq']), trace_id=trace_id or task_id,
                               refs=sorted(set(refs)))
        self.project_control(store,task_id=task_id,trace_id=trace_id or task_id)
        for host in hosts:
            source_id=f"{task_id}:{host['fence']}:{host['report_ref'][7:]}"
            self.append(source='host-adapter',source_id=source_id,trace_id=trace_id or task_id,
                actor='host-adapter',component='workbench',kind='host.unknown' if host['phase']=='orphan_unknown' else 'host.observed',
                at=host['updated_at'],task_id=task_id,attempt_epoch=host['fence'],
                refs=[f"dispatch/host-report/{task_id}@{host['fence']}#{host['report_ref']}"])
        for attempt in claudes:
            self.append(source='claude-adapter',
                source_id=f"{task_id}:{attempt['fence']}:{attempt['report_ref'][7:]}",
                trace_id=trace_id or task_id,actor='claude-adapter',component='claude-runtime',
                kind='host.observed',at=attempt['updated_at'],task_id=task_id,attempt_epoch=attempt['fence'],
                refs=[f"dispatch/claude-report/{task_id}@{attempt['fence']}#{attempt['report_ref']}"])
        return self.query(trace_id or task_id)

    def project_control(self,store,*,task_id=None,handoff_id=None,trace_id=None):
        """Project one explicit local policy subject; never send a handoff."""
        if self.path.resolve()!=store.path.resolve() or (task_id is None)==(handoff_id is None):
            raise ValueError('one policy subject and the same ledger are required')
        subject=task_id or handoff_id
        _token(subject)
        with closing(store._connect()) as conn:
            if task_id:
                rows=conn.execute('SELECT * FROM control_events WHERE task_id=? ORDER BY seq',(task_id,)).fetchall()
            else:
                keys=['handoff:'+handoff_id+':'+state for state in ('offered','accepted','completed','rejected','expired')]
                rows=conn.execute('SELECT * FROM control_events WHERE event_key IN (?,?,?,?,?) ORDER BY seq',keys).fetchall()
        for row in rows:
            refs=[f"dispatch/control/{row['event_id']}@1#{ref}" for ref in json.loads(row['refs_json'])]
            self.append(source='control-policy',source_id=row['event_id'],trace_id=trace_id or subject,
                actor=row['actor'],component='dispatch-policy',kind=row['kind'],at=row['at'],
                task_id=row['task_id'],attempt_epoch=row['fence'],refs=refs)
        return self.query(trace_id or subject)

    def diagnostics(self, *, now=None):
        """Read-only checks against the existing v0.1 task/evidence schema.

        A nonempty result is a gap, never suppressed to manufacture acceptance.
        """
        now = now or self.clock.now()
        cutoff = timestamp(now - timedelta(hours=1))
        with closing(self._connect()) as conn:
            unknown = [r[0] for r in conn.execute(
                "SELECT task_id FROM tasks WHERE status='unknown' AND updated_at<? ORDER BY task_id", (cutoff,))]
            missing = [r[0] for r in conn.execute("""SELECT t.task_id FROM tasks t
                WHERE t.status='succeeded' AND NOT EXISTS (
                  SELECT 1 FROM evidence e WHERE e.task_id=t.task_id AND e.fence=t.fence
                    AND (e.ref_json IS NOT NULL OR e.artifact_hash IS NOT NULL)) ORDER BY t.task_id""")]
        return {'unknown_over_one_hour':unknown, 'succeeded_without_output_refs':missing}
