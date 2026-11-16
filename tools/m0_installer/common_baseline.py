"""Create and attest the offline, exclusively new common Dispatch baseline.

The caller prepares fixed, root-owned non-writable-by-runtime directories and
the common lock, and keeps service, local entry and timers unavailable. This
module verifies quiescence and leaves ownership and exposure to the installer.

Resume requires a durable reservation identity. If a crash leaves the exclusive
scratch file before recording its inode, or leaves a SQLite sidecar, the caller
must investigate that state offline. The initializer preserves those files.
Verify a completed receipt with initialize only during the offline baseline
phase, before handing ownership to the runtime or activating mixed routing.
"""

from contextlib import closing
from dataclasses import asdict, replace
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
from uuid import uuid4

from dispatch.sa_dispatch.bundle import ledger
from dispatch.sa_dispatch.collector import Collector, SourceBinding, source_info
from dispatch.sa_dispatch.routing import row_digest
from dispatch.sa_dispatch.store_sqlite import SqliteStore
from tools.m0_deploy.install_target import validate_install_target, validate_manifest_sha256
from tools.m0_manager.binding import parse_json
from tools.m0_native_mixed.fresh_install import (
    FRESH_NAME as RECEIPT_NAME, FRESH_SCHEMA as RECEIPT_SCHEMA, WORKER_SOURCE_NAME, verify_fresh_baseline,
)
from tools.m0_native_mixed.maintenance import MaintenanceError, _lock, _sync_dir, _write, canonical, digest
from tools.m0_native_mixed.native_adapter import NativeLayout, STREAMS, SystemdService, directory_lock
from tools.m0_native_mixed.recovery_set import SIDECARS, _info, _read


HTTP_DEVICE_ID = 'common-http-synthetic'
HTTP_CAPABILITIES = ('component:mgmtcore',)
COLLECTOR_SOURCE_ID = 'common-dispatch'
LEASE_TTL_SECONDS = 30
STATE_NAME = 'fresh-install-state.json'
STATE_SCHEMA = 'superagent.common.baseline_state.v1'


def _require(condition, message):
    if not condition:
        raise MaintenanceError(message)


def _identity(info):
    return {'device': info.st_dev, 'inode': info.st_ino}


def _protected(info):
    return info.st_uid == 0 and info.st_gid == 0 and not info.st_mode & 0o022


def _directory(path, production):
    _info(path, directory=True)
    if production:
        _require(all(_protected(part.stat()) for part in (*path.parents, path)),
                 'fresh installation requires root-owned protected directories')


def _file(path, production, *, optional=False):
    info = _info(path, optional=optional)
    if production and info is not None:
        _require(_protected(info), 'fresh installation file ownership or permissions are unsafe')
    return info


def _read_record(path, production):
    _file(path, production)
    raw, _ = _read(path, max_bytes=16384)
    return raw


def _write_exact(path, document, production, *, verify_only=False):
    raw = canonical(document)
    if _file(path, production, optional=True) is None:
        _require(not verify_only, 'completed fresh installation document is missing')
        _write(path, raw, exclusive=True)
    else:
        _require(_read_record(path, production) == raw, 'fresh installation document differs from its expected bytes')
    return raw


def _state(path, layout, manifest, production):
    raw = _read_record(path, production)
    try:
        value = parse_json(raw)
    except (ValueError, RecursionError) as exc:
        raise MaintenanceError('invalid fresh installation journal') from exc
    fields = {'schema', 'phase', 'deployment_manifest', 'ledger', 'source_instance', 'ledger_identity'}
    _require(isinstance(value, dict) and set(value) == fields and canonical(value) == raw
             and value['schema'] == STATE_SCHEMA and value['deployment_manifest'] == manifest
             and value['ledger'] == str(layout.ledger) and value['phase'] in ('planned', 'reserved', 'building')
             and isinstance(value['source_instance'], str)
             and re.fullmatch(r'fresh-[0-9a-f]{32}', value['source_instance']),
             'fresh installation journal belongs to a different or invalid operation')
    identity = value['ledger_identity']
    if value['phase'] == 'planned':
        _require(identity is None, 'planned fresh installation already has an inode')
    else:
        _require(isinstance(identity, dict) and set(identity) == {'device', 'inode'}
                 and all(type(item) is int and item >= 0 for item in identity.values()),
                 'missing durable fresh ledger identity')
    return value


def _no_sidecars(path, production):
    for suffix in SIDECARS:
        _require(_file(path.with_name(path.name + suffix), production, optional=True) is None,
                 'fresh installation SQLite sidecar requires offline investigation')


def _publication_status(staged, target, identity, production):
    present = set()
    for path in (staged, target):
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        _require(stat.S_ISREG(info.st_mode) and not getattr(info, 'st_file_attributes', 0) & 0x400
                 and _identity(info) == identity and (not production or _protected(info)),
                 'unexpected file at the fresh ledger reservation or destination')
        present.add(path)
    _require(present and all(path.lstat().st_nlink == len(present) for path in present),
             'fresh ledger reservation disappeared or has an unrecognized link')
    return present


def _publish(staged, layout, identity, production, fault):
    target = layout.ledger
    present = _publication_status(staged, target, identity, production)
    if target not in present:
        # SQLite may create the schema only on the inode from our exclusive
        # reservation. A fully committed schema can survive an interruption.
        _no_sidecars(staged, production)
        if _file(staged, production).st_size == 0:
            SqliteStore(staged, lease_ttl_seconds=LEASE_TTL_SECONDS)
        rows = ledger(staged, include_devices=True)
        _check_rows(rows, allowed={'schema_migrations', 'ledger_mode'})
        with staged.open('r+b') as stream:
            os.fsync(stream.fileno())
        os.link(staged, target)  # Atomic no-replace publication on the same volume.
        _sync_dir(target.parent)
        fault('ledger_linked')
    if staged in _publication_status(staged, target, identity, production):
        staged.unlink()
        _sync_dir(target.parent)
    fault('ledger_published')


def _check_rows(rows, *, allowed):
    _require(rows['ledger_mode'] == [{'id': 1, 'mode': 'isolated'}]
             and [row['version'] for row in rows['schema_migrations']] ==
             ['0007', '0008', '0009', '0010', '0011', '0012'],
             'fresh ledger must contain the isolated 0012 baseline')
    _require(not any(value for table, value in rows.items() if table not in allowed),
             'fresh ledger contains prior business state')
    devices = rows['devices']
    if devices:
        _require(len(devices) == 1 and devices[0]['device_id'] == HTTP_DEVICE_ID
                 and parse_json(devices[0]['caps_json']) == list(HTTP_CAPABILITIES)
                 and devices[0]['execution_route'] is None, 'fresh ledger worker registration drift')


def _existing_cursors(path, binding, rows):
    cursors = rows['collector_cursors']
    existing = {row['stream']: row for row in cursors}
    _require(len(existing) == len(cursors) and existing.keys() <= set(STREAMS),
             'fresh ledger has an unrecognized collector cursor')
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        for stream, cursor in existing.items():
            schema, schema_digest = source_info(conn, binding, stream)
            _require(cursor['source_id'] == binding.source_id and cursor['source_instance'] == binding.source_instance
                     and cursor['binding_sha256'] == binding.fingerprint() and cursor['schema_version'] == schema == '0012'
                     and cursor['schema_sha256'] == schema_digest and cursor['authority'] == 'local_owned'
                     and cursor['epoch'] == 1 and cursor['last_seq'] == 0 and cursor['anchor_sha256'] is None
                     and cursor['redacted_count'] == 0,
                     'fresh collector projection drift before completion')
    return existing


def _write_bindings(layout, binding, identity, device, production, *, verify_only):
    public_source = layout.config_root / WORKER_SOURCE_NAME
    worker = {'schema': 'superagent.common.public_http_worker.v1', 'device_id': HTTP_DEVICE_ID,
              'capabilities': list(HTTP_CAPABILITIES), 'synthetic': True, 'egress': 'none'}
    public_bytes = _write_exact(public_source, worker, production, verify_only=verify_only)
    _write_exact(layout.workers, {'schema': 'native_mixed.http_workers.v1', 'devices': [
        {'device_id': HTTP_DEVICE_ID, 'source_path': str(public_source), 'row_sha256': row_digest(device),
         'source_sha256': digest(public_bytes)}]}, production, verify_only=verify_only)
    collector = {'schema': 'sa_dispatch.collector_binding.v1', 'target': str(layout.ledger),
                 'sources': [asdict(binding)]}
    manager = {'version': 1, 'sources': [{'source_id': binding.source_id, 'source_instance': binding.source_instance,
        'kind': 'dispatch', 'locator': str(layout.ledger), 'accepted_schema_versions': ['0012'],
        'class': 'internal', 'freshness_s': 86400, 'options': {'identity': identity}}]}
    _write_exact(layout.legacy_bindings / 'collector-binding.json', collector, production, verify_only=verify_only)
    _write_exact(layout.legacy_bindings / 'manager-binding.json', manager, production, verify_only=verify_only)


def _receipt(layout, state, production, fault):
    path = layout.ledger
    _no_sidecars(path, production)
    _require(_identity(_file(path, production)) == state['ledger_identity'], 'fresh ledger identity changed')
    binding = SourceBinding.enroll(COLLECTOR_SOURCE_ID, 'dispatch', path, state['source_instance'], ['0012'])
    binding = replace(binding, accepted_schema_versions=['0012'])
    receipt_path = layout.config_root / RECEIPT_NAME
    already_done = _file(receipt_path, production, optional=True) is not None
    rows = ledger(path, include_devices=True)
    allowed = {'schema_migrations', 'ledger_mode', 'devices', 'collector_cursors'}
    _check_rows(rows, allowed=allowed)
    before = _existing_cursors(path, binding, rows)
    if not rows['devices']:
        _require(not already_done and not before, 'fresh worker is missing after initialization')
        SqliteStore(path, lease_ttl_seconds=LEASE_TTL_SECONDS).register_device(HTTP_DEVICE_ID, HTTP_CAPABILITIES)
        fault('worker_registered')
    for stream in STREAMS:
        if stream in before:
            continue
        _require(not already_done, 'completed fresh baseline is missing a collector stream')
        result = Collector(path).sync(binding, stream)
        _require(result['state'] == 'synced' and result['epoch'] == 1 and result['projection_lag'] == 0,
                 'collector failed to establish a fresh projection')
        fault('collector_synced:' + stream)

    rows = verify_fresh_baseline(path, binding)
    receipt = {'schema': RECEIPT_SCHEMA, 'phase': 'completed', 'creation': 'exclusive_new_ledger',
               'deployment_manifest': state['deployment_manifest'], 'ledger': str(path),
               'ledger_identity': state['ledger_identity'], 'source_binding': asdict(binding),
               'baseline_rows_sha256': digest(canonical(rows))}
    if already_done:
        _require(_read_record(receipt_path, production) == canonical(receipt), 'completed fresh baseline drift')
    _write_bindings(layout, binding, state['ledger_identity'], rows['devices'][0], production, verify_only=already_done)
    if not already_done:
        fault('before_receipt')
    raw = _write_exact(receipt_path, receipt, production, verify_only=already_done)
    return {'receipt': receipt, 'receipt_path': str(receipt_path), 'receipt_sha256': digest(raw)}


def _initialize_locked(layout, manifest, production, fault):
    state_path = layout.config_root / STATE_NAME
    protected_outputs = (layout.config_root / RECEIPT_NAME, layout.config_root / WORKER_SOURCE_NAME, layout.workers)
    legacy_names = {'collector-binding.json', 'manager-binding.json'}
    _require(not any(layout.artifacts.iterdir()) and not any(layout.http_journal.iterdir()),
             'fresh ledger requires empty artifact and HTTP journal directories')
    legacy_entries = {item.name for item in layout.legacy_bindings.iterdir()}
    _require(legacy_entries <= legacy_names, 'unrecognized fresh binding directory contents')
    _no_sidecars(layout.ledger, production)
    if _file(state_path, production, optional=True) is None:
        _require(not legacy_entries and all(_file(path, production, optional=True) is None
                 for path in (layout.ledger, *protected_outputs)), 'existing unowned ledger or fresh installation record')
        state = {'schema': STATE_SCHEMA, 'phase': 'planned', 'deployment_manifest': manifest,
                 'ledger': str(layout.ledger), 'source_instance': 'fresh-' + uuid4().hex, 'ledger_identity': None}
        _write(state_path, canonical(state), exclusive=True)
        fault('planned')
    else:
        state = _state(state_path, layout, manifest, production)

    staged = layout.ledger.with_name('.' + state['source_instance'] + '.sqlite3')
    if state['phase'] != 'building':
        _require(not legacy_entries and all(_file(path, production, optional=True) is None for path in protected_outputs),
                 'fresh baseline records precede the ledger publication')
    if state['phase'] == 'planned':
        _require(_file(layout.ledger, production, optional=True) is None, 'existing unowned ledger')
        _no_sidecars(staged, production)
        try:
            fd = os.open(staged, os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        except FileExistsError as exc:
            raise MaintenanceError('fresh reservation exists without durable inode authority') from exc
        with os.fdopen(fd, 'wb') as stream:
            state['ledger_identity'] = _identity(os.fstat(stream.fileno()))
            os.fsync(stream.fileno())
        _sync_dir(staged.parent)
        state['phase'] = 'reserved'
        _write(state_path, canonical(state))
        fault('reserved')
    if state['phase'] == 'reserved':
        _publish(staged, layout, state['ledger_identity'], production, fault)
        state['phase'] = 'building'
        _write(state_path, canonical(state))
    _require(_file(staged, production, optional=True) is None, 'unrecognized baseline scratch file')
    return _receipt(layout, state, production, fault)


def initialize(layout, *, deployment_manifest, fixture_root=None, service=None, fault=None):
    """Create or verify the receipt under the lock during the isolated offline phase."""
    manifest = validate_manifest_sha256(deployment_manifest)
    _require(type(layout) is NativeLayout, 'fixed native layout is required')
    production = fixture_root is None
    if production:
        _require(service is None and fault is None and layout == NativeLayout(layout.source_root),
                 'production requires the fixed native layout and real writer checks')
        validate_install_target(manifest)
        service = SystemdService(layout, deployment_manifest=manifest)
    else:
        base = Path(fixture_root)
        _info(base, directory=True)
        _require(layout == NativeLayout.fixture(base, layout.source_root) and callable(getattr(service, 'verify_stopped', None)),
                 'bounded fresh fixture and explicit stopped-writer check required')
    for directory in (layout.config_root, layout.ledger.parent, layout.legacy_bindings,
                      layout.artifacts, layout.http_journal, layout.maintenance_lock.parent):
        _directory(directory, production)
    _file(layout.maintenance_lock, production)
    fault = fault or (lambda _label: None)
    with _lock(layout.maintenance_lock), directory_lock(layout.ledger.parent):
        if production:
            validate_install_target(manifest)
            local = subprocess.check_output(['systemctl', 'show', 'sa-dispatch-local.service',
                 '--property=ActiveState', '--property=MainPID'], text=True, timeout=15)
            properties = dict(line.split('=', 1) for line in local.splitlines())
            _require(properties.get('MainPID') == '0' and properties.get('ActiveState') in ('inactive', 'failed'),
                     'local Dispatch writer is not stopped')
        service.verify_stopped()
        return _initialize_locked(layout, manifest, production, fault)
