"""Bounded, metadata-only collection from the installed common Dispatch ledger."""

import argparse
from contextlib import closing, contextmanager, ExitStack
from datetime import datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from shutil import disk_usage
import sqlite3
import stat
import sys
from uuid import uuid4

if __package__ in (None, ''):
    source = Path(__file__).resolve().parents[2]
    sys.path[:0] = [str(source / 'dispatch'), str(source)]
    __package__ = 'tools.m0_agent'

from sa_dispatch.clock import Clock, timestamp
from sa_dispatch.campaign_validation import DEVICES as CAMPAIGN_DEVICES, PREFIX as CAMPAIGN_PREFIX
from sa_dispatch.collector import Collector, SourceBinding, digest, readonly, source_info
from sa_dispatch.models import DispatchError
from sa_dispatch.routing import ledger_mode

from . import campaign_install, common_install


LEDGER = common_install.LEDGER
BINDING = common_install.CONFIG / 'collector-binding.json'
MAINTENANCE_LOCK = common_install.MAINTENANCE_LOCK
NATIVE_GUARD = common_install.GUARD
COMMON_AUTHORITY = common_install.AUTHORITY
CAMPAIGN_AUTHORITY = campaign_install.AUTHORITY
HEALTH_DIR = Path('/var/lib/sa-dispatch-collector')
PIN = HEALTH_DIR / 'collector-runtime.json'
HEALTH = HEALTH_DIR / 'health.json'
MACHINE_ID = Path('/etc/machine-id')
BOOT_ID = Path('/proc/sys/kernel/random/boot_id')
PIN_SCHEMA = 'superagent.common.collector_runtime.v1'
HEALTH_SCHEMA = 'superagent.common.collector_health.v1'
SOURCE_ID = 'common-dispatch'
STREAMS = ('events', 'control_events', 'schedule_events')
BATCH_LIMIT = 100
HEALTH_LIMIT = 8192
MAX_LEDGER_BYTES = 32 * 1024**2
MIN_FREE_BYTES = 256 * 1024**2
STALE_AFTER_SECONDS = 180
METRICS_SCOPE = 'ledger_and_sqlite_sidecars'
HEX = re.compile(r'[0-9a-f]{64}')
UUID = re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
PIN_KEYS = frozenset(('schema', 'machine_id', 'ledger_identity', 'binding_raw_sha256', 'binding_sha256',
                      'max_ledger_bytes', 'min_free_bytes', 'stale_after_seconds'))
GAP_CODES = frozenset(('seq_gap', 'source_truncated', 'instance_changed', 'source_rewritten',
                       'schema_changed', 'redaction_rejected'))
STATUS_CODES = {
    'ok': {'synced'},
    'degraded': {'stream_frozen', 'stream_error', 'sqlite_busy'},
    'busy': {'maintenance_lock_busy', 'ledger_lock_busy'},
    'blocked': {'common_not_ready', 'campaign_maintenance_pending', 'capacity_limit',
                'identity_or_guard_invalid'},
    'unobserved': {'no_attempt'},
}


class _Refused(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _require(condition):
    if not condition:
        raise _Refused('identity_or_guard_invalid')


def _identity(info):
    return f'{info.st_dev}:{info.st_ino}'


def _require_root():
    _require(sys.platform.startswith('linux') and os.geteuid() == 0)


def _root_parents(path):
    _require(path.is_absolute() and '..' not in path.parts)
    common_install._root_parents(path)


def _root_bytes(path, limit, *, private=False):
    _root_parents(path)
    info = common_install._root_metadata(path, private=private)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        _require(_identity(os.fstat(fd)) == _identity(info))
        raw = handle.read(limit + 1)
    _require(len(raw) <= limit and _identity(path.lstat()) == _identity(info))
    return raw


def _document(raw):
    value = json.loads(raw, object_pairs_hook=common_install._unique_fields)
    _require(isinstance(value, dict))
    return value


def _machine():
    value = _root_bytes(MACHINE_ID, 64).decode('ascii').strip()
    _require(re.fullmatch(r'[0-9a-f]{32}', value) is not None)
    return value


def _boot():
    # procfs boot_id is kernel-supplied, independent of the collector installation.
    with BOOT_ID.open('rb') as handle:
        value = handle.read(64).decode('ascii').strip()
    _require(UUID.fullmatch(value) is not None)
    return value


def _shared_info(path, *, directory):
    import grp
    import pwd

    dispatch = pwd.getpwnam('sa-dispatch').pw_uid
    group = grp.getgrnam(common_install.SHARED).gr_gid
    info = path.lstat()
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    _require(expected(info.st_mode) and info.st_uid in (0, dispatch) and info.st_gid == group
             and stat.S_IMODE(info.st_mode) == (0o2770 if directory else 0o660)
             and (directory or info.st_nlink == 1))
    return info


def _ledger_identity(expected=None):
    _root_parents(LEDGER.parent)
    _shared_info(LEDGER.parent, directory=True)
    info = _shared_info(LEDGER, directory=False)
    fd = os.open(LEDGER, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        _require(_identity(os.fstat(fd)) == _identity(info))
        header = handle.read(20)
    identity = _identity(info)
    _require(_identity(LEDGER.lstat()) == identity and (expected is None or expected == identity))
    _require(header[:16] == b'SQLite format 3\x00' and header[18:20] == b'\x01\x01')
    return identity


def _binding(identity):
    raw = _root_bytes(BINDING, 65536)
    value = _document(raw)
    _require(set(value) == {'schema', 'target', 'sources'} and value['schema'] == 'sa_dispatch.collector_binding.v2'
             and value['target'] == str(LEDGER) and isinstance(value['sources'], list)
             and len(value['sources']) == 1)
    binding = SourceBinding(**value['sources'][0])
    _require((binding.source_id, binding.locator, binding.file_identity, binding.profile) ==
             (SOURCE_ID, str(LEDGER), identity, 'common_metadata_0013'))
    for stream in STREAMS:
        binding.validate(stream)
    return binding, sha256(raw).hexdigest(), digest(value)


def _validate_pin(pin):
    _require(set(pin) == PIN_KEYS and pin['schema'] == PIN_SCHEMA
             and re.fullmatch(r'[0-9a-f]{32}', pin['machine_id']) is not None
             and re.fullmatch(r'[0-9]+:[0-9]+', pin['ledger_identity']) is not None
             and all(HEX.fullmatch(pin[key]) for key in ('binding_raw_sha256', 'binding_sha256'))
             and all(type(pin[key]) is int and 1 <= pin[key] <= 32 * 1024**3
                     for key in ('max_ledger_bytes', 'min_free_bytes'))
             and type(pin['stale_after_seconds']) is int and 1 <= pin['stale_after_seconds'] <= 86400)
    return pin


def _read_pin():
    return _validate_pin(_document(_root_bytes(PIN, 4096, private=True)))


def _verify_source(binding, *, all_streams=False):
    with closing(readonly(LEDGER)) as conn:
        _require(ledger_mode(conn) == 'mixed')
        for stream in STREAMS if all_streams else STREAMS[:1]:
            _require(source_info(conn, binding, stream)[0] == '0013')


def build_pin(*, max_ledger_bytes=MAX_LEDGER_BYTES, min_free_bytes=MIN_FREE_BYTES,
              stale_after_seconds=STALE_AFTER_SECONDS):
    _require_root()
    machine, identity = _machine(), _ledger_identity()
    binding, raw_hash, canonical_hash = _binding(identity)
    _verify_source(binding, all_streams=True)
    return _validate_pin(dict(schema=PIN_SCHEMA, machine_id=machine, ledger_identity=identity,
                              binding_raw_sha256=raw_hash, binding_sha256=canonical_hash,
                              max_ledger_bytes=max_ledger_bytes, min_free_bytes=min_free_bytes,
                              stale_after_seconds=stale_after_seconds))


@contextmanager
def _flock(path, *, directory):
    import fcntl

    info = _shared_info(path, directory=True) if directory else common_install._root_metadata(path)
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    if directory:
        flags |= os.O_DIRECTORY
    fd = os.open(path, flags)
    try:
        _require(_identity(os.fstat(fd)) == _identity(info))
        try:
            fcntl.flock(fd, (fcntl.LOCK_SH if directory else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        except BlockingIOError:
            raise _Refused('ledger_lock_busy' if directory else 'maintenance_lock_busy') from None
        _require(_identity(path.lstat()) == _identity(info))
        yield
    finally:
        os.close(fd)


def _receipt(path, schema, pin, *, optional=False):
    try:
        data = _document(_root_bytes(path, 65536, private=True))
    except FileNotFoundError:
        if optional and not os.path.lexists(path):
            return None
        raise
    _require(data['schema'] == schema and data['machine_id'] == pin['machine_id'])
    return data


def _maintenance_gate(pin):
    common_install._root_metadata(COMMON_AUTHORITY, directory=True, private=True)
    common = _receipt(COMMON_AUTHORITY / 'state.json', common_install.STATE_SCHEMA, pin)
    if common['phase'] != 'ready':
        raise _Refused('common_not_ready')
    common_manifest = common['manifest_sha256']
    _require(HEX.fullmatch(common_manifest) and common['ledger_identity'] == pin['ledger_identity']
             and common['pre_live_recovery']['restore_verified'] is True
             and common['base_config_hashes'][BINDING.name] == pin['binding_raw_sha256'])
    native_gate = _root_bytes(NATIVE_GUARD, 4096)
    _require(sha256(native_gate).hexdigest() == common['old_guard_closed_sha256']
             and _document(native_gate) == {'phase': 'closed_for_common_external_upgrade'})
    _root_parents(CAMPAIGN_AUTHORITY)
    try:
        common_install._root_metadata(CAMPAIGN_AUTHORITY, directory=True, private=True)
    except FileNotFoundError:
        _require(not os.path.lexists(CAMPAIGN_AUTHORITY) and not _campaign_footprint())
        return
    campaign = _receipt(CAMPAIGN_AUTHORITY / 'state.json', campaign_install.SCHEMA, pin)
    if campaign['phase'] not in ('enabled', 'paused', 'stopped', 'expired'):
        raise _Refused('campaign_maintenance_pending')
    _require(HEX.fullmatch(campaign['manifest_sha256']) and campaign['ledger_identity'] == pin['ledger_identity']
             and campaign['prior_manifest_sha256'] == common_manifest)
    for file, schema, terminal in (('upgrade.json', campaign_install.UPGRADE_SCHEMA, ('complete',)),
                                    ('rotate.json', campaign_install.ROTATION_SCHEMA, ('complete', 'aborted'))):
        receipt = _receipt(CAMPAIGN_AUTHORITY / file, schema, pin, optional=True)
        if receipt is not None and receipt['phase'] not in terminal:
            raise _Refused('campaign_maintenance_pending')


def _campaign_footprint():
    with closing(readonly(LEDGER)) as conn:
        for table, column in (('schedules', 'id'), ('schedule_firings', 'schedule_id'),
                              ('control_budgets', 'budget_id'), ('control_handoffs', 'handoff_id')):
            if conn.execute(f'SELECT 1 FROM {table} WHERE {column}>=? AND {column}<? LIMIT 1',
                            (CAMPAIGN_PREFIX, CAMPAIGN_PREFIX[:-1] + ';')).fetchone():
                return True
        return (conn.execute("SELECT 1 FROM tasks WHERE execution_route='agent_external' AND track IS NOT NULL LIMIT 1")
                .fetchone() is not None
                or conn.execute('SELECT 1 FROM devices WHERE device_id IN (?,?) LIMIT 1',
                                tuple(CAMPAIGN_DEVICES.values())).fetchone() is not None)


def _installed_guard():
    from .collector_install import guard

    return guard()


def _metrics():
    size = LEDGER.lstat().st_size
    sidecars = 0
    for suffix in ('-journal', '-wal', '-shm'):
        path = LEDGER.with_name(LEDGER.name + suffix)
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1)
        sidecars += info.st_size
    return dict(ledger_bytes=size, sidecar_bytes=sidecars, free_bytes=disk_usage(LEDGER.parent).free)


def _at_capacity(metrics, pin):
    return (metrics['ledger_bytes'] + metrics['sidecar_bytes'] >= pin['max_ledger_bytes']
            or metrics['free_bytes'] <= pin['min_free_bytes'])


def _positions(binding):
    with closing(readonly(LEDGER)) as conn:
        positions = {}
        for stream in STREAMS:
            row = conn.execute('SELECT last_seq FROM collector_cursors WHERE source_id=? AND stream=?',
                               (binding.source_id, stream)).fetchone()
            last = row[0] if row else 0
            maximum = conn.execute('SELECT COALESCE(MAX(rowid),0) FROM ' + stream).fetchone()[0]
            positions[stream] = dict(last_seq=last, projection_lag=max(0, maximum - last))
        return positions


def _project_locked(pin, health, clock):
    _require(_read_pin() == pin and _machine() == pin['machine_id'])
    identity = _ledger_identity(pin['ledger_identity'])
    binding, raw_hash, canonical_hash = _binding(identity)
    _require((raw_hash, canonical_hash) == (pin['binding_raw_sha256'], pin['binding_sha256']))
    _maintenance_gate(pin)
    _installed_guard()
    _verify_source(binding)
    for stream, position in _positions(binding).items():
        health['streams'][stream].update(position)
    health['metrics'] = _metrics()
    if _at_capacity(health['metrics'], pin):
        return 'blocked', 'capacity_limit'
    writer = Collector(LEDGER, clock=clock)
    for stream in STREAMS:
        _ledger_identity(identity)
        try:
            result = writer.sync(binding, stream, limit=BATCH_LIMIT)
        except sqlite3.OperationalError as exc:
            code = 'sqlite_busy' if (getattr(exc, 'sqlite_errorcode', 0) & 255) in (5, 6) else 'stream_error'
            health['streams'][stream].update(state='error', code=code, added=0)
        else:
            _apply_stream_result(health['streams'][stream], result)
        health['metrics'] = _metrics()
        if _at_capacity(health['metrics'], pin):
            return 'blocked', 'capacity_limit'
    for stream, position in _positions(binding).items():
        health['streams'][stream].update(position)
    states = health['streams'].values()
    for code in ('stream_error', 'sqlite_busy'):
        if any(state['code'] == code for state in states):
            return 'degraded', code
    if any(state['state'] == 'frozen' for state in states):
        return 'degraded', 'stream_frozen'
    return 'ok', 'synced'


def _apply_stream_result(stream, result):
    if result['state'] == 'frozen' and result['kind'] in GAP_CODES:
        stream.update(state='frozen', code=result['kind'], added=0, last_seq=result['last_seq'])
    elif result['state'] == 'synced':
        stream.update(state='synced', code='synced', added=result['added'], last_seq=result['last_seq'],
                      projection_lag=result['projection_lag'])
    else:
        stream.update(state='error', code='stream_error', added=0)


def _base_health(pin, boot):
    return dict(schema=HEALTH_SCHEMA, status='unobserved', code='no_attempt', boot_id=boot,
                attempt_id=None, attempt_at=None, success_at=None, success_boot_id=None,
                busy=False, stale=True, metrics_scope=METRICS_SCOPE,
                limits={key: pin[key] for key in ('max_ledger_bytes', 'min_free_bytes', 'stale_after_seconds')},
                metrics=dict(ledger_bytes=None, sidecar_bytes=None, free_bytes=None),
                streams={stream: dict(state='unobserved', code='no_attempt', last_seq=None,
                                      projection_lag=None, added=0) for stream in STREAMS})


def _safe_time(value):
    return value is None or timestamp(datetime.fromisoformat(value)) == value


def _health(pin, boot):
    template = _base_health(pin, boot)
    common_install._root_metadata(HEALTH_DIR, directory=True, private=True)
    try:
        data = _document(_root_bytes(HEALTH, HEALTH_LIMIT, private=True))
    except FileNotFoundError:
        _require(not os.path.lexists(HEALTH))
        return template
    _require(data.keys() == template.keys() and data['schema'] == HEALTH_SCHEMA
             and data['metrics_scope'] == METRICS_SCOPE and data['limits'] == template['limits']
             and data['code'] in STATUS_CODES.get(data['status'], ())
             and type(data['busy']) is bool and type(data['stale']) is bool
             and UUID.fullmatch(data['boot_id']) and UUID.fullmatch(data['attempt_id'])
             and (data['success_boot_id'] is None or UUID.fullmatch(data['success_boot_id']))
             and data['attempt_at'] is not None and _safe_time(data['attempt_at']) and _safe_time(data['success_at'])
             and (data['success_boot_id'] is None) == (data['success_at'] is None)
             and data['metrics'].keys() == template['metrics'].keys()
             and data['streams'].keys() == template['streams'].keys())
    _require(all(value is None or type(value) is int and value >= 0 for value in data['metrics'].values()))
    allowed = {'unobserved': {'no_attempt'}, 'pending': {'not_run'}, 'synced': {'synced'},
               'error': {'sqlite_busy', 'stream_error'}, 'frozen': GAP_CODES}
    for stream, result in data['streams'].items():
        _require(result.keys() == template['streams'][stream].keys()
                 and result['code'] in allowed.get(result['state'], ())
                 and type(result['added']) is int and 0 <= result['added'] <= BATCH_LIMIT
                 and all(result[key] is None or type(result[key]) is int and result[key] >= 0
                         for key in ('last_seq', 'projection_lag')))
    return data


def _is_stale(health, pin, boot, now):
    if health['success_at'] is None or health['success_boot_id'] != boot:
        return True
    age = (now - datetime.fromisoformat(health['success_at'])).total_seconds()
    return not 0 <= age < pin['stale_after_seconds']


def status(*, clock=None):
    _require_root()
    pin, boot = _read_pin(), _boot()
    health = _health(pin, boot)
    health['stale'] = _is_stale(health, pin, boot, (clock or Clock()).now())
    return health


def tick(*, clock=None):
    _require_root()
    clock = clock or Clock()
    pin, boot = _read_pin(), _boot()
    health = _health(pin, boot)
    health.update(boot_id=boot, attempt_id=str(uuid4()), attempt_at=timestamp(clock.now()))
    for stream in health['streams'].values():
        stream.update(state='pending', code='not_run', added=0)
    with ExitStack() as locks:
        try:
            _root_parents(MAINTENANCE_LOCK)
            locks.enter_context(_flock(MAINTENANCE_LOCK, directory=False))
            _root_parents(LEDGER.parent)
            locks.enter_context(_flock(LEDGER.parent, directory=True))
            state, code = _project_locked(pin, health, clock)
        except _Refused as exc:
            code = exc.code
            state = 'busy' if code in STATUS_CODES['busy'] else 'blocked'
        except (DispatchError, OSError, TypeError, ValueError, KeyError, AttributeError, sqlite3.Error, RecursionError):
            state, code = 'blocked', 'identity_or_guard_invalid'
        now = clock.now()
        if state == 'ok':
            health.update(success_at=timestamp(now), success_boot_id=boot)
        health.update(status=state, code=code, busy=state == 'busy', stale=_is_stale(health, pin, boot, now))
        content = common_install._json(health)
        _require(len(content) <= HEALTH_LIMIT)
        common_install._atomic(HEALTH, content, mode=0o600)
    return health


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('tick', 'status'))
    command = parser.parse_args(argv).command
    try:
        result = tick() if command == 'tick' else status()
    except (DispatchError, OSError, TypeError, ValueError, KeyError, AttributeError, sqlite3.Error, RecursionError):
        print(json.dumps({'ok': False, 'code': 'collector_runtime_refused'}))
        return 1
    print(json.dumps({'ok': True} | result, ensure_ascii=True, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
