"""Explicit offline upgrade of a common 0012 dispatch ledger to 0013."""

from contextlib import closing
from datetime import datetime
from functools import lru_cache
from hashlib import file_digest, sha256
import json
import os
from pathlib import Path
import sqlite3
import tempfile

from .bundle import snapshot_records
from .canonical import canonical_json
from .clock import Clock, timestamp
from .migrations import apply_m0_schema
from .routing import EXTERNAL_ROUTE, ROUTED_TABLES, RouteRejected, validate_mixed_records


BASE_SCHEMA_VERSION = '0012'
EXTERNAL_SCHEMA_VERSION = '0013'
MIGRATION_NAME = 'm0_common_external_execution_route'
RECEIPT_SCHEMA = 'sa_dispatch.external_migration.recovery.v1'
REBUILD_TABLES = (*ROUTED_TABLES, 'route_audit')
ROUTE_CHECK = "CHECK(execution_route IN('http_synthetic','local_synthetic'))"


class UpgradeRejected(RouteRejected):
    code = 'E_EXTERNAL_MIGRATION'


def _schema(conn):
    return tuple(tuple(row) for row in conn.execute(
        'SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name'))


def _migration_history(conn):
    return tuple(tuple(row) for row in conn.execute(
        'SELECT version,name FROM schema_migrations ORDER BY version'))


def _rebuild(conn, base_schema, columns):
    for table in REBUILD_TABLES:
        ddl = next(sql for kind, name, _, sql in base_schema if kind == 'table' and name == table)
        prefix = f'CREATE TABLE {table}('
        if not ddl.startswith(prefix) or ddl.count(ROUTE_CHECK) != 1:
            raise UpgradeRejected('0012 reference DDL does not match the migration')
        replacement = ROUTE_CHECK[:-2] + f",'{EXTERNAL_ROUTE}'))"
        staging = f'{table}_upgrade_0013'
        conn.execute(f'CREATE TABLE {staging}(' + ddl[len(prefix):].replace(ROUTE_CHECK, replacement))
        # route_audit.seq aliases rowid. Keep rowids on the other four tables too.
        names = ','.join(f'"{name}"' for name in columns[table])
        if table != 'route_audit':
            names = 'rowid,' + names
        conn.execute(f'INSERT INTO {staging}({names}) SELECT {names} FROM {table}')
        conn.execute(f'DROP TABLE {table}')
        conn.execute(f'ALTER TABLE {staging} RENAME TO {table}')
        for kind, _, owner, sql in base_schema:
            if owner == table and kind in ('index', 'trigger') and sql is not None:
                conn.execute(sql)


@lru_cache(maxsize=1)
def _reference():
    """Build both shapes and the column maps from bundled, trusted DDL."""
    with closing(sqlite3.connect(':memory:')) as conn:
        conn.executescript(Path(__file__).with_name('schema.sql').read_text(encoding='utf-8'))
        apply_m0_schema(conn, 'reference')
        schema = _schema(conn)
        history = _migration_history(conn)
        columns = {table: tuple(row[1] for row in conn.execute(f'PRAGMA table_info({table})'))
                   for table in REBUILD_TABLES}
        _rebuild(conn, schema, columns)
        return schema, _schema(conn), history, columns


def verify_schema(conn):
    """Verify exact trusted dispatch DDL and migration history in the caller's read transaction."""
    baseline, upgraded, history, _ = _reference()
    actual = _schema(conn)
    if actual == baseline:
        version = BASE_SCHEMA_VERSION
    elif actual == upgraded:
        version = EXTERNAL_SCHEMA_VERSION
        history = (*history, (EXTERNAL_SCHEMA_VERSION, MIGRATION_NAME))
    else:
        raise UpgradeRejected('ledger does not match the fixed 0012 or 0013 dispatch schema')
    if _migration_history(conn) != history or conn.execute('PRAGMA user_version').fetchone()[0] != 0:
        raise UpgradeRejected('ledger migration history does not match its schema')
    return version


def _digest(value):
    return sha256(canonical_json(value)).hexdigest()


def _cold_snapshot(conn):
    if conn.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
        raise UpgradeRejected('ledger failed SQLite integrity check')
    if conn.execute('PRAGMA foreign_key_check').fetchone():
        raise UpgradeRejected('ledger failed SQLite foreign-key check')
    records = snapshot_records(conn)
    activations = [row for row in records['route_audit'] if row['action'] == 'activate']
    if (records['ledger_mode'] != [{'id': 1, 'mode': 'mixed'}] or len(activations) != 1
            or (activations[0]['object_table'], activations[0]['object_id'],
                activations[0]['execution_route']) != ('ledger_mode', '1', None)):
        raise UpgradeRejected('upgrade requires a mixed ledger with one activation audit')
    if (any(row['status'] in ('leased', 'unknown') for row in records['tasks'])
            or any(row['state'] in ('active', 'unknown') for row in records['leases'])):
        raise UpgradeRejected('active or unknown work blocks the offline upgrade')
    if (any(row['state'] != 'settled' for row in records['control_reservations'])
            or any(row['reserved_units'] != 0 for row in records['control_budgets'])):
        raise UpgradeRejected('unsettled control budget blocks the offline upgrade')
    validate_mixed_records(records)
    return records


def _open(path, mode):
    conn = sqlite3.connect(path.as_uri() + '?mode=' + mode, uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA trusted_schema=OFF')
    conn.execute('PRAGMA foreign_keys=ON')
    return conn


def _copy_database(source, destination):
    with destination.open('xb'):
        pass
    destination.chmod(0o600)
    with closing(_open(source, 'ro')) as reader, closing(_open(destination, 'rw')) as copy:
        reader.backup(copy)
    with destination.open('rb+') as handle:
        os.fsync(handle.fileno())


def _file_sha256(path):
    with path.open('rb') as handle:
        return file_digest(handle, 'sha256').hexdigest()


def _flush_directory(path):
    if os.name == 'posix':
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _create_recovery_root(path):
    missing = []
    for directory in (path, *path.parents):
        if directory.exists():
            break
        missing.append(directory)
    for directory in reversed(missing):
        directory.mkdir(mode=0o700, exist_ok=True)
        _flush_directory(directory.parent)


def _verify_recovery(directory, source_path, records):
    receipt_path = directory / 'receipt.json'
    archive = directory / 'ledger.sqlite3'
    if (directory.is_symlink() or any(path.is_symlink() or not path.is_file()
                                     for path in (receipt_path, archive))
            or (os.name == 'posix' and directory.stat().st_mode & 0o077)):
        raise UpgradeRejected('private recovery copy is incomplete or redirected')
    raw = receipt_path.read_bytes()
    receipt = json.loads(raw)
    if (not isinstance(receipt, dict) or raw != canonical_json(receipt)
            or set(receipt) != {'schema', 'target_path', 'backup_sha256', 'migration_at',
                                'baseline_schema_sha256', 'baseline_rows_sha256'}
            or receipt.get('schema') != RECEIPT_SCHEMA or receipt.get('target_path') != str(source_path)
            or receipt.get('backup_sha256') != _file_sha256(archive)
            or receipt.get('baseline_schema_sha256') != _digest(_reference()[0])
            or receipt.get('baseline_rows_sha256') != _digest(records)
            or not isinstance(receipt.get('migration_at'), str)
            or timestamp(datetime.fromisoformat(receipt['migration_at'])) != receipt['migration_at']):
        raise UpgradeRejected('recovery receipt does not match the target and baseline ledger')
    with tempfile.TemporaryDirectory(prefix='verify-restore-', dir=directory.parent) as temporary:
        restored = Path(temporary) / 'restored.sqlite3'
        _copy_database(archive, restored)
        with closing(_open(restored, 'ro')) as conn:
            conn.execute('BEGIN')
            if verify_schema(conn) != BASE_SCHEMA_VERSION or _cold_snapshot(conn) != records:
                raise UpgradeRejected('restored copy does not match the 0012 source ledger')
    return receipt


def _prepare_recovery(directory, source_path, records, at):
    if directory.is_symlink() or directory.exists():
        return _verify_recovery(directory, source_path, records)
    with tempfile.TemporaryDirectory(prefix='prepare-recovery-', dir=directory.parent) as temporary:
        staging = Path(temporary) / 'recovery'
        staging.mkdir(mode=0o700)
        archive = staging / 'ledger.sqlite3'
        _copy_database(source_path, archive)
        receipt = {'schema': RECEIPT_SCHEMA, 'target_path': str(source_path),
                   'migration_at': at, 'backup_sha256': _file_sha256(archive),
                   'baseline_schema_sha256': _digest(_reference()[0]),
                   'baseline_rows_sha256': _digest(records)}
        with (staging / 'receipt.json').open('xb') as handle:
            handle.write(canonical_json(receipt))
            handle.flush()
            os.fsync(handle.fileno())
        _verify_recovery(staging, source_path, records)
        _flush_directory(staging)
        staging.rename(directory)
        _flush_directory(directory.parent)
    return receipt


def upgrade(path, recovery_dir, *, offline=False, clock=None):
    """Upgrade an existing ledger while the caller holds the maintenance lock.

    The caller stops and keeps stopped every service using this ledger. This
    entry point never starts them. The recovery archive covers this SQLite
    ledger and is verified by restoring it into an isolated directory.
    """
    if offline is not True:
        raise UpgradeRejected('offline maintenance and stopped writers must be explicitly confirmed')
    path = Path(path).absolute()
    recovery_root = Path(recovery_dir).absolute()
    if path != path.resolve() or not path.is_file() or path.stat().st_nlink != 1:
        raise UpgradeRejected('upgrade requires an existing, unredirected ledger file')
    if recovery_root != recovery_root.resolve():
        raise UpgradeRejected('recovery directory must be unredirected')
    try:
        directory = recovery_root / ('dispatch-0012-' + sha256(os.fsencode(path)).hexdigest())
        with closing(_open(path, 'rw')) as conn:
            conn.execute('PRAGMA synchronous=FULL')
            # SQLite cannot switch foreign_keys after BEGIN. The maintenance
            # connection checks all foreign keys before and after table replacement.
            conn.execute('PRAGMA foreign_keys=OFF')
            with conn:
                conn.execute('BEGIN IMMEDIATE')
                version = verify_schema(conn)
                records = _cold_snapshot(conn)
                changed = version == BASE_SCHEMA_VERSION
                if changed:
                    _create_recovery_root(recovery_root)
                    receipt = _prepare_recovery(directory, path, records, timestamp((clock or Clock()).now()))
                    baseline, _, _, columns = _reference()
                    _rebuild(conn, baseline, columns)
                    conn.execute('INSERT INTO schema_migrations VALUES(?,?,?)',
                                 (EXTERNAL_SCHEMA_VERSION, MIGRATION_NAME, receipt['migration_at']))
                    if verify_schema(conn) != EXTERNAL_SCHEMA_VERSION:
                        raise UpgradeRejected('migration did not produce the 0013 schema')
                    after = _cold_snapshot(conn)
                    after['schema_migrations'].pop()
                    if after != records:
                        raise UpgradeRejected('migration changed pre-existing ledger rows')
                else:
                    applied = records['schema_migrations'].pop()['applied_at']
                    receipt = _verify_recovery(directory, path, records)
                    if receipt['migration_at'] != applied:
                        raise UpgradeRejected('recovery receipt does not match the committed migration')
            conn.execute('PRAGMA foreign_keys=ON')
            conn.execute('BEGIN')
            if verify_schema(conn) != EXTERNAL_SCHEMA_VERSION or conn.execute('PRAGMA foreign_key_check').fetchone():
                raise UpgradeRejected('committed upgrade failed schema or foreign-key checks')
            return {'schema_version': EXTERNAL_SCHEMA_VERSION, 'changed': changed,
                    'recovery_path': str(directory), 'recovery_verified': True,
                    'baseline_rows_sha256': receipt['baseline_rows_sha256'],
                    'baseline_schema_sha256': receipt['baseline_schema_sha256'],
                    'schema_sha256': _digest(_schema(conn))}
    except UpgradeRejected:
        raise
    except RouteRejected as error:
        raise UpgradeRejected('ledger rows do not satisfy the pre-external route profile') from error
    except (sqlite3.Error, OSError, ValueError) as error:
        raise UpgradeRejected('could not migrate or verify recovery for the selected ledger') from error
