"""Read-only preflight for a stopped, known 0012 common ledger."""

import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import stat


REPORT_SCHEMA = 'superagent.agent_external.preflight.v1'
MAX_LEDGER_BYTES = 64 * 1024 * 1024
# Fingerprints of sqlite_schema from a fresh production 0012 schema, exercised
# independently by the Store + activate_mixed fixture. Rootpage is excluded.
SCHEMA_SHA256 = '77394cd869213574eb6bfb43a6533b47cd9f6325a4ddf310aaab20b8d3dbd670'
ROUTE_TABLE_SHA256 = {
    'devices': 'a7ade32d6e56c2d3ed03ca773f064008075dbe3362af92576cb241e3e89a2975',
    'route_audit': '0c25681842d2000f8ecc77181e8346f15ba1680b6b82d1e183505978234b48ba',
    'schedule_firings': '973d09d160e2c11e86037949504063a8eb339b722a98b1faae218f8e9e354a46',
    'schedules': 'e85bb0c89ce51f72afc82565d717150037afbe55af11058ee8a54d4c26b72446',
    'tasks': '07cb64911675005d8b7821bbf21a674e0906195b5ac0f24bd96403591c35ba3b',
}
MIGRATIONS = (
    ('0007', 'm0_scheduler_events_reconciliation'), ('0008', 'm0_control_host_adapter'),
    ('0009', 'm0_claude_owned_runtime'), ('0010', 'm0_bounded_schedules'),
    ('0011', 'm0_persistent_collector'), ('0012', 'm0_mixed_execution_routes'),
)
ROUTES = ('http_synthetic', 'local_synthetic')
COMMON_SOURCE = 'common-dispatch'
COLLECTOR_STREAMS = ('events', 'control_events', 'schedule_events')
BLOCKING_QUERIES = {
    'pending_tasks': "SELECT 1 FROM tasks WHERE status IN ('queued','waiting_device') LIMIT 1",
    'leased_tasks': "SELECT 1 FROM tasks WHERE status='leased' LIMIT 1",
    'unknown_tasks': "SELECT 1 FROM tasks WHERE status='unknown' LIMIT 1",
    'active_leases': "SELECT 1 FROM leases WHERE state='active' LIMIT 1",
    'unknown_leases': "SELECT 1 FROM leases WHERE state='unknown' LIMIT 1",
    'pending_schedule_firings': "SELECT 1 FROM schedule_firings WHERE outcome='pending' LIMIT 1",
    'enabled_schedules': 'SELECT 1 FROM schedules WHERE enabled=1 LIMIT 1',
    'held_budget_units': 'SELECT 1 FROM control_budgets WHERE reserved_units!=0 LIMIT 1',
    'unsettled_budget_reservations': "SELECT 1 FROM control_reservations WHERE state!='settled' LIMIT 1",
    'collector_open_gaps': 'SELECT 1 FROM collector_gaps WHERE resolved_at IS NULL LIMIT 1',
}


class PreflightRejected(ValueError):
    """A fixed public refusal code without database or filesystem details."""


def _fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=True, separators=(',', ':')).encode('ascii')).hexdigest()


def _redirected(info):
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, 'st_file_attributes', 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT)


def _offline_file(path):
    for parent in reversed(path.parents):
        directory = parent.lstat()
        if not stat.S_ISDIR(directory.st_mode) or _redirected(directory):
            raise PreflightRejected('ledger_directory_not_plain')
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or _redirected(info) or info.st_nlink != 1:
        raise PreflightRejected('ledger_file_not_regular')
    if info.st_size > MAX_LEDGER_BYTES:
        raise PreflightRejected('ledger_size_limit_exceeded')
    for suffix in ('-wal', '-shm', '-journal'):
        try:
            path.with_name(path.name + suffix).lstat()
        except FileNotFoundError:
            continue
        raise PreflightRejected('sqlite_sidecar_present')
    return info


def _schema(conn):
    rows = conn.execute('SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name LIMIT 79').fetchall()
    tables = {name: sql for kind, name, _, sql in rows if kind == 'table'}
    if 'schema_migrations' not in tables or not ROUTE_TABLE_SHA256.keys() <= tables.keys():
        raise PreflightRejected('required_schema_objects_missing')
    if _fingerprint(rows) != SCHEMA_SHA256:
        raise PreflightRejected('unknown_0012_schema')
    versions = conn.execute('SELECT version,name FROM schema_migrations ORDER BY version LIMIT 7').fetchall()
    if tuple(versions) != MIGRATIONS or conn.execute('PRAGMA user_version').fetchone() != (0,):
        raise PreflightRejected('unknown_migration_history')
    if conn.execute('SELECT id,mode FROM ledger_mode LIMIT 2').fetchall() != [(1, 'mixed')]:
        raise PreflightRejected('mixed_ledger_required')
    evidence = {name: _fingerprint(tables[name]) for name in ROUTE_TABLE_SHA256}
    if evidence != ROUTE_TABLE_SHA256:
        raise PreflightRejected('route_constraint_drift')
    return evidence


def _route_rows(conn):
    for table in ROUTE_TABLE_SHA256:
        if conn.execute(f'''SELECT 1 FROM {table} WHERE execution_route IS NOT NULL
                             AND execution_route NOT IN (?,?) LIMIT 1''', ROUTES).fetchone():
            raise PreflightRejected('unknown_execution_route')
    if conn.execute("SELECT 1 FROM tasks WHERE access!='synthetic' OR egress!='none' LIMIT 1").fetchone() or \
            conn.execute("SELECT 1 FROM evidence WHERE access!='synthetic' LIMIT 1").fetchone():
        raise PreflightRejected('mixed_ledger_has_non_synthetic_rows')


def _collectors(conn):
    rows = conn.execute('''SELECT stream,source_instance,binding_sha256,schema_sha256,schema_version,authority
                           FROM collector_cursors WHERE source_id=? AND stream IN (?,?,?)''',
                        (COMMON_SOURCE, *COLLECTOR_STREAMS)).fetchall()
    cursors = {row[0]: row[1:] for row in rows}
    present = {stream: stream in cursors for stream in COLLECTOR_STREAMS}
    bindings = {cursor[:2] for cursor in cursors.values()}
    consistent = all(present.values()) and len(bindings) == 1
    for stream, (instance, binding, schema_hash, version, authority) in cursors.items():
        columns = conn.execute(f'PRAGMA table_info({stream})').fetchall()
        if (not instance or not re.fullmatch(r'[a-f0-9]{64}', binding)
                or version != '0012' or authority != 'local_owned' or schema_hash != _fingerprint(columns)):
            consistent = False
    return present, consistent


def inspect(ledger_path):
    """Inspect a caller-selected offline DB without migration or writes."""
    path = Path(ledger_path)
    if (not path.is_absolute() or '..' in path.parts
            or (path.drive and not re.fullmatch(r'[A-Za-z]:', path.drive))):
        raise PreflightRejected('absolute_ledger_path_required')
    if path.drive and (path.is_reserved() or any(':' in part for part in path.parts[1:])):
        raise PreflightRejected('invalid_ledger_path')
    before = _offline_file(path)
    with path.open('rb') as handle:
        header = handle.read(20)
    if header[:16] != b'SQLite format 3\x00' or header[18:20] != b'\x01\x01':
        raise PreflightRejected('offline_rollback_journal_ledger_required')
    # Immutable prevents sidecar creation and ignores sidecars, which are
    # rejected above and rechecked before returning. This does not stop writers.
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)) as conn:
        conn.execute('PRAGMA trusted_schema=OFF')
        conn.execute('PRAGMA query_only=ON')
        evidence = _schema(conn)
        _route_rows(conn)
        if conn.execute('PRAGMA integrity_check(1)').fetchone() != ('ok',):
            raise PreflightRejected('sqlite_integrity_check_failed')
        if conn.execute('PRAGMA foreign_key_check').fetchone():
            raise PreflightRejected('sqlite_foreign_key_check_failed')
        blockers = {name: conn.execute(sql).fetchone() is not None for name, sql in BLOCKING_QUERIES.items()}
        cursors, consistent = _collectors(conn)
    after = _offline_file(path)
    if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
        raise PreflightRejected('ledger_changed_during_preflight')
    blockers['collector_cursors_missing_or_inconsistent'] = not consistent
    return {
        'schema': REPORT_SCHEMA,
        'status': 'blocked' if any(blockers.values()) else 'ready_for_next_offline_review',
        'external_execution_allowed': False,
        'ledger_mode': 'mixed', 'ledger_version': '0012', 'schema_sha256': SCHEMA_SHA256,
        'route_constraints': {table: {'ddl_sha256': digest, 'allowed_routes': ROUTES}
                              for table, digest in evidence.items()},
        'collector_cursors': cursors,
        'blockers': blockers,
        'writer_stop_verified': False,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--ledger', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        report = inspect(args.ledger)
    except PreflightRejected as error:
        report = {'schema': REPORT_SCHEMA, 'status': 'refused', 'reason': str(error),
                  'external_execution_allowed': False}
    except (OSError, sqlite3.Error, ValueError):
        report = {'schema': REPORT_SCHEMA, 'status': 'refused', 'reason': 'ledger_unreadable',
                  'external_execution_allowed': False}
    print(json.dumps(report, sort_keys=True))
    return {'ready_for_next_offline_review': 0, 'blocked': 2, 'refused': 1}[report['status']]


if __name__ == '__main__':
    raise SystemExit(main())
