"""Read-only observations of the installed common ledger and its local units."""

from hashlib import sha256
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys

from dispatch.sa_dispatch.campaign_validation import (
    DEVICES, budget_fields, budget_id, schedule_id, schedule_template, validate_config,
)
from dispatch.sa_dispatch.models import DispatchError

from .binding import canonical, instant, now, parse_json


SCHEMA = 'superagent.manager.operational_snapshot.v1'
LEDGER = Path('/var/lib/sa-dispatch/dispatch.sqlite3')
MACHINE_ID = Path('/etc/machine-id')
BOOT_ID = Path('/proc/sys/kernel/random/boot_id')
COMMON_STATE = Path('/var/lib/sa-dispatch-agent-maintenance/state.json')
CAMPAIGN_STATE = Path('/var/lib/sa-dispatch-agent-campaign/state.json')
CAMPAIGN_CONFIG = Path('/etc/sa-dispatch-agent-campaign/campaign.json')
COLLECTOR_STATE = Path('/var/lib/sa-dispatch-collector/state.json')
COLLECTOR_PIN = COLLECTOR_STATE.with_name('collector-runtime.json')
COLLECTOR_HEALTH = COLLECTOR_STATE.with_name('health.json')
COORDINATOR = 'sa-dispatch.service'
LOCAL = 'sa-dispatch-local.service'
COLLECTOR_SERVICE = 'sa-dispatch-collector.service'
COLLECTOR_TIMER = 'sa-dispatch-collector.timer'
CAMPAIGN_TIMERS = ('sa-agent-producer.timer', 'sa-agent-reviewer.timer')
CAMPAIGN_UNITS = frozenset((*CAMPAIGN_TIMERS, 'sa-agent-producer.service', 'sa-agent-reviewer.service'))
UNITS = (COORDINATOR, LOCAL, COLLECTOR_SERVICE, COLLECTOR_TIMER,
         'sa-agent-producer.service', *CAMPAIGN_TIMERS, 'sa-agent-reviewer.service')
SYSTEMCTL = '/usr/bin/systemctl'
PROPERTIES = {
    'LoadState': {'loaded', 'not-found', 'masked', 'error', 'bad-setting', 'merged', 'stub'},
    'ActiveState': {'active', 'reloading', 'inactive', 'failed', 'activating', 'deactivating',
                    'maintenance', 'refreshing'},
    'SubState': {'dead', 'running', 'exited', 'waiting', 'elapsed', 'failed', 'auto-restart',
                 'start-pre', 'start', 'start-post', 'stop', 'stop-post', 'final-sigterm', 'final-sigkill'},
    'UnitFileState': {'', 'enabled', 'enabled-runtime', 'disabled', 'static', 'indirect', 'masked',
                      'masked-runtime', 'generated', 'linked', 'linked-runtime', 'transient', 'alias', 'bad'},
}
LOCAL_ABSENT = {'LoadState': 'not-found', 'ActiveState': 'inactive', 'SubState': 'dead', 'UnitFileState': ''}
HEALTH_CODES = {
    'ok': {'synced'}, 'degraded': {'stream_frozen', 'stream_error', 'sqlite_busy'},
    'busy': {'maintenance_lock_busy', 'ledger_lock_busy'},
    'blocked': {'common_not_ready', 'campaign_maintenance_pending', 'capacity_limit', 'identity_or_guard_invalid'},
    'unobserved': {'no_attempt'},
}
STREAM_STATES = {'unobserved': {'no_attempt'}, 'pending': {'not_run'}, 'synced': {'synced'},
                 'error': {'sqlite_busy', 'stream_error'},
                 'frozen': {'seq_gap', 'source_truncated', 'instance_changed', 'source_rewritten',
                            'schema_changed', 'redaction_rejected'}}
STREAMS = ('events', 'control_events', 'schedule_events')
MACHINE = re.compile(r'[0-9a-f]{32}')
UUID = re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
HEX = re.compile(r'[0-9a-f]{64}')
READ_ERRORS = (OSError, ValueError, TypeError, KeyError, AttributeError, sqlite3.Error, DispatchError, RecursionError)


def _check(condition):
    if not condition:
        raise ValueError('invalid operational evidence')


def _identity(info):
    return {'device': info.st_dev, 'inode': info.st_ino}


def _protected(path, max_bytes, *, private=False):
    _check(path.is_absolute())
    for parent in path.parents:
        info = parent.lstat()
        _check(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022)
    info = path.lstat()
    _check(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid == 0
           and not info.st_mode & 0o022 and (not private or (info.st_gid, stat.S_IMODE(info.st_mode)) == (0, 0o600)))
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        _check(_identity(os.fstat(fd)) == _identity(info))
        raw = handle.read(max_bytes + 1)
    _check(len(raw) <= max_bytes and _identity(path.lstat()) == _identity(info))
    return raw


def _object(raw):
    value = parse_json(raw)
    _check(type(value) is dict)
    return value


def _receipt(path, schema, machine, *, optional=False):
    try:
        record = _object(_protected(path, 65536, private=True))
    except FileNotFoundError:
        if optional and not os.path.lexists(path):
            return None
        raise
    _check((record['schema'], record['machine_id']) == (schema, machine))
    return record


def _current_source(db, source):
    _check(sys.platform == 'linux' and source['locator'] == str(LEDGER)
           and source['class'] == 'local_only' and source['options']['profile'] == 'common_metadata_0013'
           and db.execute('SELECT mode FROM ledger_mode WHERE id=1').fetchone()[0] == 'mixed'
           and db.execute('SELECT MAX(version) FROM schema_migrations').fetchone()[0] == '0013')
    _check(all(stat.S_ISDIR(parent.lstat().st_mode) for parent in LEDGER.parents))
    before = LEDGER.lstat()
    _check(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and _identity(before) == source['options']['identity'])
    machine = _protected(MACHINE_ID, 64).decode('ascii').strip()
    _check(MACHINE.fullmatch(machine))
    common = _receipt(COMMON_STATE, 'superagent.common.external.install.v1', machine)
    ledger_identity = f'{before.st_dev}:{before.st_ino}'
    _check(common['phase'] == 'ready' and common['ledger_identity'] == ledger_identity
           and common['pre_live_recovery']['restore_verified'] is True
           and (common['collector']['source_id'], common['collector']['source_instance']) ==
               (source['source_id'], source['source_instance'])
           and _identity(LEDGER.lstat()) == _identity(before))
    return machine, ledger_identity


def _boot():
    with BOOT_ID.open('rb') as handle:
        value = handle.read(64).decode('ascii').strip()
    _check(UUID.fullmatch(value))
    return {'state': 'observed', 'boot_id': value}


def _offline_mode(machine, ledger_identity):
    """Missing roles are expected only with a verified no-grant installation."""
    record = _receipt(COMMON_STATE, 'superagent.common.external.install.v1', machine)
    if record.get('execution_mode') != 'offline':
        return False
    from tools.m0_agent import common_install

    manifest = record['manifest_sha256']
    _check(isinstance(manifest, str) and HEX.fullmatch(manifest)
           and not os.path.lexists(CAMPAIGN_STATE.parent)
           and not os.path.lexists(CAMPAIGN_CONFIG.parent))
    release = common_install.RELEASES / manifest
    common_install._source_files(release, manifest)
    checked = common_install._Installer(release, machine, manifest).guard()
    _check(checked['execution_mode'] == 'offline' and checked['ledger_identity'] == ledger_identity)
    return True


def _systemd(*, offline=False):
    argv = [SYSTEMCTL, 'show', '--no-pager', '--property=' + ','.join(('Id', *PROPERTIES)), '--', *UNITS]
    call = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                          env={'PATH': '/usr/bin:/bin', 'LANG': 'C', 'SYSTEMD_COLORS': '0'},
                          timeout=3, check=False)
    _check(len(call.stdout) <= 16 * 1024)
    candidates = {}
    invalid = set()
    for block in call.stdout.decode('ascii').strip().split('\n\n'):
        items = [line.partition('=') for line in block.splitlines()]
        value = {key: val for key, match, val in items if match}
        unit = value.pop('Id', None)
        if unit not in UNITS:
            continue
        if (unit in candidates or len(items) != len(PROPERTIES) + 1 or value.keys() != PROPERTIES.keys()
                or any(field not in PROPERTIES[key] for key, field in value.items())):
            invalid.add(unit)
            continue
        local_absent = unit == LOCAL and value == LOCAL_ABSENT
        role_absent = offline and unit in CAMPAIGN_UNITS and value == LOCAL_ABSENT
        if not (local_absent or role_absent) and value['LoadState'] != 'loaded':
            invalid.add(unit)
            continue
        candidates[unit] = dict(state='observed', load_state=value['LoadState'], active_state=value['ActiveState'],
                                sub_state=value['SubState'], unit_file_state=value['UnitFileState'])
        if local_absent:
            candidates[unit]['availability'] = 'on_demand_not_running'
        if role_absent:
            candidates[unit]['availability'] = 'unconfigured_offline'
        elif offline and unit in CAMPAIGN_UNITS:
            invalid.add(unit)
    units = {unit: candidates[unit] if unit in candidates and unit not in invalid else {'state': 'unknown'}
             for unit in UNITS}
    complete = all(item['state'] == 'observed' for item in units.values())
    return dict(state='observed' if complete else 'partial' if candidates else 'unknown', units=units)


def _numbers(data, keys, *, optional=False):
    _check(type(data) is dict and all(key in data and
           ((optional and data[key] is None) or (type(data[key]) is int and data[key] >= 0)) for key in keys))
    return {key: data[key] for key in keys}


def _health_time(value):
    _check(isinstance(value, str) and len(value) == 32)
    parsed = instant(value)
    _check(parsed.isoformat(timespec='microseconds') == value and value.endswith('+00:00'))
    return parsed


def _collector(machine, ledger_identity, current_boot, observed):
    state = _receipt(COLLECTOR_STATE, 'superagent.common.collector_install.v1', machine)
    phase = state['phase']
    _check(state['ledger_identity'] == ledger_identity and phase in ('installing', 'upgrading', 'paused', 'enabled'))
    if phase in ('installing', 'upgrading'):
        return dict(state='maintenance_pending', install_phase=phase)
    pin_raw = _protected(COLLECTOR_PIN, 4096, private=True)
    pin = _object(pin_raw)
    _check(sha256(pin_raw).hexdigest() == state['pin_sha256']
           and (pin['schema'], pin['machine_id'], pin['ledger_identity']) ==
               ('superagent.common.collector_runtime.v1', machine, ledger_identity)
           and all(pin[key] == state[key] and HEX.fullmatch(pin[key])
                   for key in ('binding_raw_sha256', 'binding_sha256')))
    limits = _numbers(pin, ('stale_after_seconds', 'max_ledger_bytes', 'min_free_bytes'))
    _check(1 <= limits['stale_after_seconds'] <= 86400 and limits['max_ledger_bytes'] > 0 and limits['min_free_bytes'] > 0)
    try:
        health_raw = _protected(COLLECTOR_HEALTH, 8192, private=True)
    except FileNotFoundError:
        return dict(state='intentional_idle' if phase == 'paused' else 'unknown',
                    install_phase=phase, freshness='unknown')
    health = _object(health_raw)
    latest = health['status']
    _check(health['schema'] == 'superagent.common.collector_health.v1'
           and health['code'] in HEALTH_CODES.get(latest, ())
           and UUID.fullmatch(health['boot_id']) and health['limits'] == limits
           and health['metrics_scope'] == 'ledger_and_sqlite_sidecars'
           and set(health['streams']) == set(STREAMS))
    attempt_time = _health_time(health['attempt_at'])
    success, success_boot = health['success_at'], health['success_boot_id']
    _check((success is None and success_boot is None) or (success is not None and UUID.fullmatch(success_boot)))
    success_time = _health_time(success) if success is not None else None
    stale_reason = None
    if current_boot is None:
        freshness, stale_reason = 'unknown', 'current_boot_unknown'
    elif health['boot_id'] != current_boot or (success_boot is not None and success_boot != current_boot):
        freshness, stale_reason = 'stale', 'boot_changed'
    elif success is None:
        freshness, stale_reason = 'stale', 'no_success'
    elif min((observed - success_time).total_seconds(), (observed - attempt_time).total_seconds()) < 0:
        freshness, stale_reason = 'stale', 'clock_rollback'
    elif (observed - success_time).total_seconds() >= limits['stale_after_seconds']:
        freshness, stale_reason = 'stale', 'success_overdue'
    else:
        freshness = 'fresh'
    streams = {}
    for name in STREAMS:
        entry = health['streams'][name]
        _check(entry['code'] in STREAM_STATES.get(entry['state'], ()))
        streams[name] = dict(state=entry['state'], code=entry['code'],
                             **_numbers(entry, ('last_seq', 'projection_lag'), optional=True))
    metrics = _numbers(health['metrics'], ('ledger_bytes', 'sidecar_bytes', 'free_bytes'), optional=latest != 'ok')
    if latest == 'ok':
        _check(all((entry['state'], entry['code']) == ('synced', 'synced') for entry in streams.values())
               and success_boot == health['boot_id'] and success_time >= attempt_time
               and metrics['ledger_bytes'] + metrics['sidecar_bytes'] < limits['max_ledger_bytes']
               and metrics['free_bytes'] > limits['min_free_bytes'])
    return dict(state='intentional_idle' if phase == 'paused' else latest if freshness == 'fresh' else freshness,
                install_phase=phase, freshness=freshness, stale_reason=stale_reason,
                latest_status=latest, latest_code=health['code'], attempt_boot_id=health['boot_id'],
                attempt_at=health['attempt_at'], success_at=success, success_boot_id=success_boot,
                streams=streams, limits=limits,
                metrics_scope='ledger_and_sqlite_sidecars',
                metrics=metrics)


def _campaign(db, machine, ledger_identity, observed, *, offline=False):
    if offline:
        return dict(state='intentional_idle', install_phase='unconfigured',
                    authorization_state='not_granted', request_units_used=0,
                    request_units_held=0, budget_limit_units=0, budget_unit='provider_request')
    state = _receipt(CAMPAIGN_STATE, 'superagent.agent_campaign.install.v1', machine)
    phase = state['phase']
    _check(state['ledger_identity'] == ledger_identity)
    if phase in ('prepared', 'upgrading', 'rotating', 'aborting'):
        return dict(state='maintenance_pending', install_phase=phase)
    _check(phase in ('paused', 'enabled', 'stopped'))
    for name, schema, terminal in (('upgrade.json', 'superagent.agent_campaign.upgrade.v1', ('complete',)),
                                   ('rotate.json', 'superagent.agent_campaign.rotation.v1', ('complete', 'aborted'))):
        receipt = _receipt(CAMPAIGN_STATE.with_name(name), schema, machine, optional=True)
        if receipt is not None and receipt['phase'] not in terminal:
            return dict(state='maintenance_pending', install_phase=phase)
    raw = _protected(CAMPAIGN_CONFIG, 8192)
    config = validate_config(_object(raw))
    _check(sha256(raw).hexdigest() == state['config_sha256']
           and (config['machine_id'], config['ledger_identity'], config['campaign_id']) ==
               (machine, ledger_identity, state['campaign_id']))
    highwater = []
    paused = []
    for role in DEVICES:
        sid = schedule_id(config, role)
        schedule = db.execute('SELECT task_template,execution_route,last_tick_at FROM schedules WHERE id=?', (sid,)).fetchone()
        _check(schedule is not None and (schedule['task_template'], schedule['execution_route']) ==
               (canonical(schedule_template(config, role)).decode('utf-8'), 'agent_external'))
        highwater.append(_health_time(schedule['last_tick_at']))
        event = db.execute("SELECT 1 FROM schedule_events WHERE schedule_id=? AND kind='campaign.paused' LIMIT 1",
                           (sid,)).fetchone()
        paused.append(event is not None)
    _check(highwater[0] == highwater[1] and paused[0] == paused[1])
    budget = db.execute('''SELECT budget_id,period,unit,limit_units,starts_at,ends_at,used_units,reserved_units
        FROM control_budgets WHERE budget_id=?''', (budget_id(config),)).fetchone()
    _check(budget is not None and all(budget[key] == value for key, value in budget_fields(config).items())
           and 0 <= budget['used_units'] and 0 <= budget['reserved_units']
           and budget['used_units'] + budget['reserved_units'] <= budget['limit_units'])
    if paused[0]:
        window = 'paused'
    elif observed < highwater[0]:
        window = 'clock_rollback'
    elif observed < instant(config['starts_at']):
        window = 'scheduled'
    elif observed >= instant(config['expires_at']):
        window = 'expired'
    else:
        window = 'active'
    intentional = phase in ('stopped', 'paused') or window in ('paused', 'expired')
    return dict(state='intentional_idle' if intentional else window, install_phase=phase,
                authorization_state=window, campaign_id=config['campaign_id'],
                starts_at=config['starts_at'], expires_at=config['expires_at'],
                request_units_used=budget['used_units'], request_units_held=budget['reserved_units'],
                budget_limit_units=budget['limit_units'], budget_unit='provider_request')


def _observe(probe):
    observed = now()
    try:
        result = probe(observed)
    except (subprocess.SubprocessError, *READ_ERRORS):
        result = {'state': 'unknown'}
    return dict(result, observed_at=observed)


def _attention(observations):
    items = []

    def add(component, code, severity):
        items.append(dict(component=component, code=code, severity=severity,
                          observed_at=observations[component]['observed_at']))

    for name in ('ledger', 'boot', 'systemd', 'collector', 'campaign'):
        if observations[name]['state'] in ('unknown', 'partial'):
            add(name, 'observation_unavailable', 'unknown')
    units = observations['systemd'].get('units', {})
    http = units.get(COORDINATOR, {})
    if http.get('state') == 'observed' and http['active_state'] in ('failed', 'inactive'):
        add('systemd', 'coordinator_unavailable', 'warning')
    collector = observations['collector']
    if collector['state'] in ('blocked', 'degraded', 'stale', 'maintenance_pending'):
        add('collector', 'collector_' + collector['state'], 'warning')
    if collector.get('install_phase') == 'enabled' and units.get(COLLECTOR_TIMER, {}).get('active_state') in ('failed', 'inactive'):
        add('collector', 'collector_timer_inactive', 'warning')
    campaign = observations['campaign']
    if campaign['state'] in ('clock_rollback', 'maintenance_pending'):
        add('campaign', 'campaign_' + campaign['state'], 'warning')
    if campaign['state'] in ('active', 'scheduled') and any(
            units.get(unit, {}).get('active_state') in ('failed', 'inactive') for unit in CAMPAIGN_TIMERS):
        add('campaign', 'campaign_timer_inactive', 'warning')
    if campaign.get('request_units_held', 0) > 0:
        add('campaign', 'campaign_requests_held', 'notice')
    return items


def operational_snapshot(db, source, observed_at):
    """Return an observation window, preserving the existing Manager status contract."""
    try:
        machine, ledger_identity = _current_source(db, source)
        offline = _offline_mode(machine, ledger_identity)
    except READ_ERRORS:
        observations = {name: dict(state='unknown', observed_at=None)
                        for name in ('ledger', 'boot', 'systemd', 'collector', 'campaign')}
        observations['ledger']['observed_at'] = observed_at
        unknown = ['ledger']
    else:
        observations = {'ledger': dict(state='observed', observed_at=observed_at)}
        observations['boot'] = _observe(lambda _: _boot())
        observations['systemd'] = _observe(lambda _: _systemd(offline=offline))
        boot = observations['boot'].get('boot_id')
        observations['collector'] = _observe(lambda at: _collector(machine, ledger_identity, boot, instant(at)))
        observations['campaign'] = _observe(lambda at: _campaign(db, machine, ledger_identity, instant(at), offline=offline))
        unknown = [name for name, item in observations.items() if item['state'] in ('unknown', 'partial')]
    return dict(schema=SCHEMA, kind='operational_snapshot', as_of=observed_at, non_atomic=True,
                observation_started_at=observed_at, observation_completed_at=now(),
                state_basis='unknown' if 'ledger' in unknown else 'current',
                coverage='partial' if unknown else 'complete', observations=observations,
                unknown=unknown, attention=_attention(observations))
