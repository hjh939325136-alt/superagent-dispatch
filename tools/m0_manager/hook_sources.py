"""Project Hook metadata snapshot. No body selection, writers, or durable cursor."""
from datetime import datetime
import os
import re
import sqlite3

from .binding import fixed_ref, now
from .readonly import SourceError, local_sqlite_file, readonly_db

FORMAT = 'superagent_hook.events.v1'
MAX_BYTES = 16 * 1024 * 1024
MAX_ROWS = 10000
MAX_BODY_CHARS = 16384
ROLES = {'technical', 'manager', 'progress'}
EVENTS = {'SessionStart', 'PreToolUse', 'PostToolUse'}
MODES = {'audit', 'read_only'}
DECISIONS = {'allow', 'deny', 'observed'}
# .NET/PowerShell timestamps carry seven digits; retain the original precision
# and explicit offset. datetime is used only for freshness, not serialization.
TIME = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})\Z')


def source_check(source, kind, format_name, role):
    """Check the trust boundary before touching a locator, even for direct callers."""
    if not isinstance(role, str) or role not in ROLES or source.get('class') != 'local_only':
        raise SourceError('source_access_denied')
    options = source.get('options', {})
    expected = {'format', 'identity'} if kind == 'hook' else {'format'}
    if source.get('kind') != kind or not isinstance(options, dict) or set(options) != expected or options.get('format') != format_name:
        raise SourceError('source_binding_invalid')
    if '1' not in source.get('accepted_schema_versions', []):
        raise SourceError('source_schema_unsupported')
    if type(source.get('freshness_s')) is not int or not 1 <= source['freshness_s'] <= 31536000:
        raise SourceError('source_binding_invalid')
    if kind == 'hook':
        identity = options['identity']
        if not isinstance(identity, dict) or set(identity) != {'device', 'inode'} or any(type(v) is not int or v < 0 for v in identity.values()):
            raise SourceError('source_binding_invalid')


def request_check(op, args, allowed):
    if op not in allowed:
        raise SourceError('source_operation_unsupported')
    fields = {'source_id', 'limit'} | ({'after_seq'} if op == 'events.query' else set())
    if not isinstance(args, dict) or set(args) - fields:
        raise SourceError('E_FIELD_NOT_ALLOWED')
    limit, after = args.get('limit', 50), args.get('after_seq', 0)
    if type(limit) is not int or not 1 <= limit <= 200 or type(after) is not int or not 0 <= after <= 10**9:
        raise SourceError('E_ARGUMENT_INVALID')
    return limit, after


def source_time(value, *, optional=False):
    if optional and value is None:
        return None
    if not isinstance(value, str) or not TIME.fullmatch(value):
        raise SourceError('source_invalid_time')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError()
    except ValueError:
        raise SourceError('source_invalid_time') from None
    return value


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def basis(updated, observed, freshness):
    return 'current' if updated and 0 <= (instant(observed) - instant(updated)).total_seconds() <= freshness else 'stale'


def _metadata_stat(path, identity):
    # local_sqlite_file uses lstat/mount metadata only: never open/close the DB
    # through another file descriptor while SQLite holds POSIX shared locks.
    path = local_sqlite_file(path)
    info = path.lstat()
    if info.st_nlink != 1 or info.st_size > MAX_BYTES:
        raise SourceError('source_file_not_allowed')
    if identity != {'device': info.st_dev, 'inode': info.st_ino}:
        raise SourceError('source_instance_changed')
    for suffix in ('-wal', '-shm', '-journal'):
        sibling = path.with_name(path.name + suffix)
        if sibling.exists() or sibling.is_symlink():
            raise SourceError('source_sqlite_sidecar_not_allowed')
    return path, info


def _schema(db):
    objects = [tuple(row) for row in db.execute('SELECT type,name,tbl_name FROM sqlite_master ORDER BY type,name')
               if not row['name'].startswith('sqlite_')]
    if objects != [('index', 'events_at', 'events'), ('table', 'events', 'events')]:
        raise SourceError('source_schema_unsupported')
    fields = [tuple(row) for row in db.execute('PRAGMA table_info(events)')]
    if fields != [(0, 'id', 'INTEGER', 0, None, 1), (1, 'at', 'TEXT', 1, None, 0), (2, 'body', 'TEXT', 1, None, 0)]:
        raise SourceError('source_schema_unsupported')
    indexes = [tuple(row)[1:] for row in db.execute('PRAGMA index_list(events)')]
    if indexes != [('events_at', 0, 'c', 0)] or [tuple(row) for row in db.execute('PRAGMA index_info(events_at)')] != [(0, 1, 'at')]:
        raise SourceError('source_schema_unsupported')
    # Reject added CHECK/generated/foreign-key/other semantics with the same columns.
    sql = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='events'").fetchone()[0]
    normalized = re.sub(r'\s+', '', sql).lower()
    if normalized != 'createtableevents(idintegerprimarykey,attextnotnull,bodytextnotnull)':
        raise SourceError('source_schema_unsupported')


def _read_rows(db):
    count = db.execute('SELECT COUNT(*) FROM events').fetchone()[0]
    if count > MAX_ROWS:
        raise SourceError('source_record_limit')
    if db.execute('SELECT 1 FROM events WHERE length(body)>? OR NOT json_valid(body) LIMIT 1', (MAX_BODY_CHARS,)).fetchone():
        raise SourceError('source_invalid')
    if db.execute("SELECT 1 FROM events WHERE json_type(body)!='object' LIMIT 1").fetchone():
        raise SourceError('source_invalid')
    # json_each returns only an aggregate existence result, never keys/values/body.
    if db.execute('SELECT 1 FROM events,json_each(events.body) GROUP BY events.id,json_each.key HAVING COUNT(*)>1 LIMIT 1').fetchone():
        raise SourceError('source_invalid')
    sql = '''SELECT id,at,
        json_type(body,'$.schemaVersion') AS schema_type,
        json_extract(body,'$.schemaVersion') AS schema_version,
        json_extract(body,'$.at') AS body_at,
        json_extract(body,'$.event') AS event,
        json_extract(body,'$.mode') AS mode,
        json_extract(body,'$.decision') AS decision,
        json_type(body,'$.sideEffectsVerified') AS side_effects_type,
        json_type(body,'$.sessionId') AS session_type,
        json_type(body,'$.turnId') AS turn_type,
        json_type(body,'$.toolUseId') AS tool_use_type,
        json_type(body,'$.toolName') AS tool_name_type
        FROM events ORDER BY id'''
    rows = []
    for item in db.execute(sql):
        row = dict(item)
        if (type(row['id']) is not int or row['id'] < 1 or row['schema_type'] != 'integer' or row['schema_version'] != 1 or
                row['event'] not in EVENTS or row['mode'] not in MODES or row['decision'] not in DECISIONS or
                row['side_effects_type'] != 'false'):
            raise SourceError('source_schema_unsupported')
        if row['decision'] not in ({'allow', 'deny'} if row['event'] == 'PreToolUse' else {'observed'}):
            raise SourceError('source_invalid')
        source_time(row['at'])
        if row['body_at'] != row['at']:
            raise SourceError('source_invalid_time')
        if any(row[name] not in {'text', 'null'} for name in ('session_type', 'turn_type', 'tool_use_type', 'tool_name_type')):
            raise SourceError('source_schema_unsupported')
        rows.append(row)
    return rows


def hook_query(source: dict, op: str, args: dict, role: str) -> dict:
    source_check(source, 'hook', FORMAT, role)
    limit, after_seq = request_check(op, args, {'status', 'events.query'})
    identity = source['options']['identity']
    try:
        path, before = _metadata_stat(source['locator'], identity)
        # Inspect mode/header before opening SQLite. No second ordinary DB fd is
        # opened after BEGIN: closing one would release POSIX process locks.
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if identity != {'device': opened.st_dev, 'inode': opened.st_ino} or opened.st_nlink != 1:
                raise SourceError('source_instance_changed')
            header = stream.read(100)
        if (len(header) != 100 or header[:16] != b'SQLite format 3\x00' or header[18:20] != b'\x01\x01' or
                header[60:64] != b'\x00' * 4 or header[68:72] != b'\x00' * 4):
            raise SourceError('source_sqlite_format_unsupported')
        with readonly_db(path, identity=identity) as db:
            _schema(db)
            rows = _read_rows(db)
            _, current = _metadata_stat(path, identity)
            if (before.st_size, before.st_mtime_ns) != (current.st_size, current.st_mtime_ns):
                raise SourceError('source_changed')
    except SourceError:
        raise
    except (OSError, ValueError, TypeError, sqlite3.Error):
        raise SourceError('source_unavailable') from None
    observed = now()
    updated = max((r['at'] for r in rows), key=instant, default=None)
    ids = [r['id'] for r in rows]
    minimum, maximum = (ids[0], ids[-1]) if ids else (None, None)
    gaps = bool(ids and maximum - minimum + 1 != len(ids))
    chronological = all(instant(a['at']) <= instant(b['at']) for a, b in zip(rows, rows[1:]))
    unknown = ['retained_window_only', 'native_source_instance_absent', 'task_and_conversation_mapping_unknown',
               'hook_trust_and_tool_coverage_not_verified', 'side_effects_not_verified',
               'deletions_and_id_reuse_not_observable_without_history', 'no_durable_cursor_or_collection']
    if minimum and minimum > 1:
        unknown.append('earlier_rows_not_retained')
    if gaps:
        unknown.append('retained_id_gaps')
    if not chronological:
        unknown.append('event_times_not_monotonic')
    if minimum and after_seq and after_seq < minimum - 1:
        unknown.append('requested_anchor_not_retained')
    if minimum and minimum <= after_seq <= maximum and after_seq not in ids:
        unknown.append('requested_anchor_missing_inside_window')
    if after_seq and (maximum is None or after_seq > maximum):
        unknown.append('requested_anchor_ahead_of_snapshot')
    if not updated:
        unknown.append('source_updated_at_unknown')
    coverage = {'mode': 'snapshot_only', 'scope': 'current_retained_events', 'native_sequence': False,
                'sequence_kind': 'source_row_id_not_durable_cursor', 'source_instance_basis': 'trusted_local_binding',
                'retained_min_id': minimum, 'retained_max_id': maximum, 'retained_id_gaps': gaps,
                'event_times_monotonic': chronological, 'earliest_at': min((r['at'] for r in rows), key=instant, default=None),
                'latest_at': updated, 'body_selected': False,
                'nullable_fields': {name.removesuffix('_type'): sum(r[name] == 'null' for r in rows)
                                    for name in ('session_type', 'turn_type', 'tool_use_type', 'tool_name_type')}}
    selected = [row for row in rows if row['id'] > after_seq]
    items = []
    if op == 'events.query':
        for row in selected[:limit]:
            item = {'seq': row['id'], 'source_row_id': row['id'], 'kind': 'hook.observed',
                    'event': row['event'], 'mode': row['mode'], 'decision': row['decision'],
                    'side_effects_verified': False, 'task_id': None, 'as_of': row['at'], 'state_basis': 'historical'}
            items.append({**item, 'ref': fixed_ref(source, 'hook-event', str(row['id']), item)})
    else:
        item = {'kind': 'hook.status', 'retained_record_count': len(rows), 'as_of': updated,
                'state_basis': basis(updated, observed, source['freshness_s']), 'side_effects_verified': False,
                'event_counts': {name: sum(r['event'] == name for r in rows) for name in sorted(EVENTS)}}
        items = [{**item, 'ref': fixed_ref(source, 'hook-status', 'retained', item)}]
    return {'items': items, 'coverage': 'partial', 'coverage_details': coverage, 'unknown': unknown,
            'state_basis': basis(updated, observed, source['freshness_s']), 'schema_version': '1',
            'source_instance': source['source_instance'], 'observed_at': observed, 'source_updated_at': updated,
            'record_count': len(rows), 'matched_count': len(selected) if op == 'events.query' else 1,
            'incremental': False, 'collection_modes': ['snapshot_only'], 'actions_taken': 0}
