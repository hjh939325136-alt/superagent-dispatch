"""Admission of settled scheduler history in an offline domain checkpoint."""
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
import re

from sa_dispatch.canonical import canonical_json, sha256_ref
from sa_dispatch.clock import timestamp
from sa_dispatch.models import TaskSpec
from sa_dispatch.routing import HTTP_ROUTE, LOCAL_ROUTE, row_digest
from sa_dispatch.states import TERMINAL
from sa_dispatch_hotspot import KINDS

from .domain_binding import _parse


EVENT_CODES = {
    'schedule.template_error': {'upsert_template_rejected', 'invalid_template'},
    'schedule.bound_removed': {'max_slots_removed'},
    'schedule.exhausted': {'max_slots_reached'},
    'schedule.clock_skew': {'clock_before_last_tick'},
    'schedule.backlog_exceeded': {'over_100000_slots'},
    'schedule.retry_scheduled': {'sqlite_busy'},
    'schedule.rejected': {'invalid_reserved_template', 'idempotency_conflict',
                          'submission_rejected', 'sqlite_retry_exhausted'},
}
SCHEDULE_ID = re.compile(r'[A-Za-z0-9_.-]{1,80}')


class ScheduleHistoryError(ValueError):
    pass


def _require(condition, message):
    if not condition:
        raise ScheduleHistoryError(message)


def _integer(value, low, high=None):
    return type(value) is int and value >= low and (high is None or value <= high)


def _time(value):
    try:
        parsed = datetime.fromisoformat(value)
        if timestamp(parsed) == value:
            return parsed
    except (ValueError, TypeError, OverflowError):
        pass
    raise ScheduleHistoryError('invalid scheduler timestamp')


def _template(raw):
    try:
        value = _parse(raw)
        if (set(value) != TaskSpec.__dataclass_fields__.keys() or not isinstance(value['input_refs'], list)
                or not isinstance(value['required_caps'], list)):
            raise ValueError('invalid task spec fields')
        spec = TaskSpec.from_dict(value)
        canonical_json(value)
    except (TypeError, ValueError, RecursionError) as error:
        raise ScheduleHistoryError('invalid schedule task template') from error
    # The standard component-free scheduler can register the shipped hotspot
    # kinds. Admission only uses their metadata and pure parameter validation.
    kind = KINDS.get(spec.kind) if isinstance(spec.kind, str) else None
    _require(kind and kind.enabled and kind.side_effect == 'none' and
             type(spec.kind_version) is int and kind.version == spec.kind_version,
             'schedule template requires a supported read-only task kind')
    _require(spec.access == 'synthetic' and spec.egress == 'none' and _integer(spec.max_attempts, 1) and
             all(isinstance(cap, str) and cap for cap in spec.required_caps) and
             all(all(isinstance(item, str) and item for item in asdict(ref).values())
                 and ref.version != 'latest' for ref in spec.input_refs) and
             all(item is None or isinstance(item, str) for item in (spec.track, spec.parent_task_id)),
             'invalid schedule task template metadata')
    try:
        _require(canonical_json(kind.validate(spec.params)) == canonical_json(spec.params),
                 'schedule template parameters are not normalized')
    except ValueError as error:
        raise ScheduleHistoryError('invalid schedule task template parameters') from error
    return spec


def _spec_fields(spec):
    fields = asdict(spec)
    fields.pop('idem_key')
    return fields


def _task_identity(task):
    try:
        fields = {key: task[key] for key in ('kind', 'kind_version', 'track', 'access', 'egress',
                                             'max_attempts', 'parent_task_id', 'side_effect')}
        for key in ('params', 'scope', 'input_refs', 'required_caps'):
            fields[key] = _parse('{"value":' + task[key + '_json'] + '}')['value']
        encoded = canonical_json(fields)
    except (KeyError, TypeError, ValueError, RecursionError) as error:
        raise ScheduleHistoryError('invalid scheduled task identity') from error
    _require(task['fingerprint'] == sha256_ref(encoded), 'invalid scheduled task fingerprint')
    return encoded


def _validate_task(firing, spec, tasks, tasks_by_key, handoffs, binds):
    ident, key, outcome = firing['task_id'], firing['fire_key'], firing['outcome']
    task = tasks.get(ident)
    if outcome == 'skipped_catchup':
        _require(ident is None, 'skipped schedule firing cannot link a task')
        return
    route = firing['execution_route']
    if ident is not None or outcome == 'created':
        _require(task and task['idem_key'] == key and task['status'] in TERMINAL,
                 'schedule firing does not link its terminal idempotency task')
        inherited_task = ('tasks', ident) in binds
        _require(task['execution_route'] == route or (route is None and inherited_task),
                 'schedule firing task route conflicts with activation history')
        actual = _task_identity(task)
    if outcome == 'created':
        expected = _spec_fields(spec)
        expected['required_caps'] = sorted(set(spec.required_caps))
        expected['input_refs'] = sorted(expected['input_refs'], key=lambda ref: (
            ref['owner'], ref['kind'], ref['id'], ref['version'], ref['hash']))
        expected.update(side_effect='none', scope=KINDS[spec.kind].scope(spec.params))
        _require(actual == canonical_json(expected), 'scheduled task differs from its frozen template')
        # A slot adopted with a historical task may have been completed on the
        # original isolated route, which did not require a transport marker.
        historical = ('schedule_firings', key) in binds and inherited_task
        if route == HTTP_ROUTE and not historical:
            _require(ident in handoffs and not spec.input_refs, 'HTTP schedule firing lacks valid handoff history')
    elif firing['rejection_code'] == 'idempotency_conflict':
        collision = tasks_by_key.get(key)
        _require(collision and (ident is not None or (route is not None and collision['execution_route'] != route)),
                 'schedule idempotency conflict lacks its colliding task')


def _validate_outcome(row, slot_at, rejections, retries):
    outcome = row['outcome']
    _require(outcome != 'pending', 'pending schedule reservation prevents checkpoint')
    _require(outcome in ('created', 'skipped_catchup', 'rejected'), 'invalid schedule firing outcome')
    at = _time(row['at'])
    code, count = row['rejection_code'], row['failure_count']
    key = row['schedule_id'], row['version']
    # A previously reserved slot can be retried after a clock rewind. The
    # skipped slot's timestamp always comes from its reservation transaction.
    _require(row['next_attempt_at'] is None and
             (row['last_attempt_at'] is None and at >= slot_at if outcome == 'skipped_catchup'
              else row['last_attempt_at'] == row['at']), 'invalid settled schedule attempt timestamps')
    if outcome == 'rejected':
        rejection = (*key, row['at'], code)
        _require(_integer(count, 1, 3) and code in EVENT_CODES['schedule.rejected'] and
                 (code != 'sqlite_retry_exhausted' or count == 3) and (*key, row['at']) in rejections,
                 'invalid rejected schedule attempt or missing rejection audit')
        transient_count = count - 1
    else:
        _require(_integer(count, 0, 0 if outcome == 'skipped_catchup' else 2),
                 'invalid settled schedule attempt count')
        # A successful retry updates outcome and timestamps while retaining
        # the last transient error code in the firing row.
        _require(code == ('sqlite_busy' if count else None), 'invalid settled schedule attempt code')
        transient_count = count
        rejection = None
    # Concurrent ticks can read a reservation before another retries it. The
    # audit has no slot identity and deduplicates equal timestamps.
    _require(not transient_count or key in retries, 'schedule attempt count lacks retry audit')
    return rejection


def _seconds_between(previous, current):
    delta = _time(current) - _time(previous)
    _require(delta.microseconds == 0 and (delta.days > 0 or delta.days == 0 and delta.seconds > 0),
             'invalid interval between scheduled slots')
    return delta.days * 86400 + delta.seconds


def validate_history(records):
    """Validate scheduler-owned rows and return every explicit template input ref."""
    schedules = {row['id']: row for row in records['schedules']}
    firings = {row['fire_key']: row for row in records['schedule_firings']}
    tasks = {row['task_id']: row for row in records['tasks']}
    tasks_by_key = {row['idem_key']: row for row in tasks.values()}
    handoffs = {row['task_id'] for row in records['transport_handoffs']}
    binds = {(row['object_table'], row['object_id']): row for row in records['route_audit']
             if row['action'] == 'bind'}
    for table, entities in (('schedules', schedules), ('schedule_firings', firings)):
        for (bound_table, key), proof in binds.items():
            if bound_table == table:
                _require(key in entities, 'orphan schedule activation binding')
                if proof['execution_route'] == LOCAL_ROUTE:
                    _require(proof['pre_state_hash'] == row_digest(entities[key]),
                             'local schedule history differs from its activation binding')
        _require(all(row['execution_route'] != LOCAL_ROUTE or (table, key) in binds
                     for key, row in entities.items()), 'local schedule history lacks an activation binding')

    refs, templates, by_schedule = [], {}, defaultdict(list)
    for ident, row in schedules.items():
        _require(isinstance(ident, str) and SCHEDULE_ID.fullmatch(ident) and _integer(row['version'], 1) and
                 _integer(row['interval_seconds'], 1) and _integer(row['enabled'], 0, 1) and
                 row['catchup'] in ('all', 'latest', 'skip') and _integer(row['max_catchup'], 1, 1000) and
                 (row['max_slots'] is None or _integer(row['max_slots'], 1, 100000)),
                 'invalid schedule metadata or catchup policy')
        _time(row['next_fire_at'])
        if row['last_tick_at'] is not None:
            _time(row['last_tick_at'])
        spec = _template(row['task_template'])
        refs.extend(asdict(ref) for ref in spec.input_refs)
        templates[ident, row['version']] = canonical_json(_spec_fields(spec))

    rejections, retries, exhausted = _validate_events(records['schedule_events'], schedules)
    linked_rejections = set()
    for key, row in firings.items():
        ident, version, when = row['schedule_id'], row['version'], row['scheduled_at']
        schedule = schedules.get(ident)
        _require(schedule and _integer(version, 1, schedule['version']) and key == f'sched:{ident}:v{version}:{when}',
                 'orphan schedule firing or invalid schedule fire key/version')
        if schedule['execution_route'] is not None and (
                row['execution_route'] is None or ('schedule_firings', key) in binds):
            _require(('schedules', ident) in binds, 'historical firing lacks its schedule activation binding')
        spec = _template(row['task_template'])
        refs.extend(asdict(ref) for ref in spec.input_refs)
        original = canonical_json(_spec_fields(spec))
        _require(spec.idem_key == key and templates.setdefault((ident, version), original) == original,
                 'frozen schedule template differs from its fire key or version')
        rejection = _validate_outcome(row, _time(when), rejections, retries)
        if rejection is not None:
            linked_rejections.add(rejection)
        _validate_task(row, spec, tasks, tasks_by_key, handoffs, binds)
        by_schedule[ident].append(row)
    _require(all((*key, code) in linked_rejections for key, code in rejections.items()),
             'orphan rejected schedule audit')

    for ident, row in schedules.items():
        history = sorted(by_schedule[ident], key=lambda firing: firing['scheduled_at'])
        _validate_cursor(row, history, exhausted)
    return refs


def _validate_events(rows, schedules):
    rejections, retries, exhausted = {}, set(), set()
    for row in rows:
        ident, version, kind, code = (row[name] for name in ('schedule_id', 'version', 'kind', 'detail_code'))
        schedule = schedules.get(ident)
        _require(_integer(row['seq'], 1) and isinstance(ident, str) and SCHEDULE_ID.fullmatch(ident) and
                 _integer(version, 1) and
                 code in EVENT_CODES.get(kind, ()) and
                 (version <= schedule['version'] if schedule else
                  (version, kind, code) == (1, 'schedule.template_error', 'upsert_template_rejected')),
                 'invalid scheduler event or orphan schedule audit')
        _time(row['at'])
        if kind == 'schedule.exhausted':
            exhausted.add(ident)
        elif kind == 'schedule.retry_scheduled':
            retries.add((ident, version))
        elif kind == 'schedule.rejected':
            # Scheduler deduplicates by version, event kind and timestamp,
            # even if multiple reservations have different error codes.
            rejections[ident, version, row['at']] = code
    return rejections, retries, exhausted


def _validate_cursor(schedule, history, exhausted):
    bound = schedule['max_slots']
    _require(bound is None or len(history) < bound or
             (len(history) == bound and not schedule['enabled'] and schedule['id'] in exhausted),
             'schedule lifetime slot bound or exhaustion disagrees with history')
    latest = history[-1] if history else None
    _require(schedule['last_fire_key'] == (latest['fire_key'] if latest else None),
             'schedule last fire key disagrees with history')
    if latest is None:
        return
    _require(schedule['next_fire_at'] > latest['scheduled_at'] and latest['outcome'] != 'skipped_catchup' and
             schedule['last_tick_at'] is not None and schedule['last_tick_at'] >= latest['scheduled_at'],
             'schedule cursor disagrees with reserved slots')
    intervals = {schedule['version']: schedule['interval_seconds']}
    if latest['version'] == schedule['version']:
        _require(_seconds_between(latest['scheduled_at'], schedule['next_fire_at']) == schedule['interval_seconds'],
                 'schedule cursor does not advance by its configured interval')
    for previous, current in zip(history, history[1:]):
        version = current['version']
        _require(version >= previous['version'] and
                 (version == previous['version'] or previous['outcome'] != 'skipped_catchup'),
                 'schedule firing history reverses or truncates a version')
        if version == previous['version']:
            interval = _seconds_between(previous['scheduled_at'], current['scheduled_at'])
            _require(intervals.setdefault(version, interval) == interval,
                     'schedule firing version has inconsistent slot intervals')
