"""Record one bounded operational observation window on the installed common ledger."""

import argparse
from contextlib import contextmanager
from datetime import timedelta
import errno
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    __package__ = 'tools.m0_manager'

from dispatch.sa_dispatch.clock import Clock, timestamp
from tools.m0_agent import common_install

from .binding import canonical, instant, parse_json, token
from .operational_sources import (
    BOOT_ID, COORDINATOR, LEDGER, MACHINE_ID, SCHEMA as OPS_SCHEMA, STREAMS, UNITS, _protected,
)


AUTHORITY = Path('/var/lib/sa-dispatch-soak')
PIN = AUTHORITY / 'pin.json'
LOCK = AUTHORITY / 'soak.lock'
RECORD = AUTHORITY / 'record.json'
BINDING = Path('/etc/sa-dispatch/manager-binding.json')
OBSERVER_RELEASES = Path('/opt/sa-dispatch-observer/releases')
PIN_SCHEMA = 'superagent.manager.soak_pin.v1'
RECORD_SCHEMA = 'superagent.manager.soak_record.v1'
DURATION_SECONDS = 86400
INTERVAL_SECONDS = 300
MAX_GAP_SECONDS = 600
MIN_SAMPLES = 145
MAX_SAMPLES = 512
MAX_RECORD_BYTES = 512 * 1024
SOURCE_ID = 'common-dispatch'
HEX = re.compile(r'[0-9a-f]{64}')
UUID = re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
PIN_FIELDS = frozenset(('schema', 'machine_id', 'ledger_identity', 'observer_manifest_sha256',
                        'binding_sha256', 'source_id', 'source_instance', 'duration_seconds',
                        'interval_seconds', 'max_gap_seconds'))
TERMINAL = {'passed': 'window_passed', 'failed': None, 'clock_invalid': 'clock_rollback',
            'paused': 'operator_paused'}
BAD_REASONS = frozenset(('bad_observations', 'sampling_gap', 'boot_changed', 'insufficient_coverage',
                         'sample_limit'))
SAMPLE_STATES = {
    'coverage': {'complete', 'partial', 'unknown'},
    'collector': {'ok', 'degraded', 'busy', 'blocked', 'stale', 'maintenance_pending', 'intentional_idle', 'unknown'},
    'campaign': {'active', 'scheduled', 'intentional_idle', 'clock_rollback', 'maintenance_pending', 'unknown'},
    'coordinator': {'active', 'activating', 'reloading', 'inactive', 'failed', 'deactivating', 'maintenance',
                    'refreshing', 'unknown'},
}
SAMPLE_FIELDS = frozenset(('recorded_at', 'boot_id', 'boottime_ms', 'observed_from', 'observed_until',
                           'quality', 'coverage', 'collector', 'campaign', 'coordinator',
                           'observed_units', 'warning_count', 'unknown_count', 'max_projection_lag'))
RECORD_FIELDS = frozenset(('schema', 'pin_sha256', 'state', 'reason', 'started_at', 'deadline_at',
                           'ended_at', 'samples', 'reboot_count'))
QUERY_SCRIPT = (
    "import sys; sys.path.insert(0,sys.argv[1]); "
    "from tools.m0_manager.__main__ import main; "
    "sys.argv=['sa-manager','--binding',sys.argv[2]]; raise SystemExit(main())"
)


class SoakRefused(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _check(condition, code='identity_or_record_invalid'):
    if not condition:
        raise SoakRefused(code)


def _root():
    _check(sys.platform == 'linux' and os.getuid() == os.geteuid() == 0, 'linux_root_required')
    common_install._root_parents(AUTHORITY)
    common_install._root_metadata(AUTHORITY, directory=True, private=True)


def _object(raw):
    value = parse_json(raw)
    _check(type(value) is dict)
    return value


def _identity(path):
    info = path.lstat()
    _check(stat.S_ISREG(info.st_mode) and info.st_nlink == 1)
    return f'{info.st_dev}:{info.st_ino}'


def _pin(pin):
    _check(set(pin) == PIN_FIELDS and pin['schema'] == PIN_SCHEMA
           and re.fullmatch(r'[0-9a-f]{32}', pin['machine_id'])
           and re.fullmatch(r'[0-9]{1,20}:[0-9]{1,20}', pin['ledger_identity'])
           and all(HEX.fullmatch(pin[key]) for key in ('binding_sha256', 'observer_manifest_sha256'))
           and pin['source_id'] == SOURCE_ID and token(pin['source_instance'])
           and all(type(pin[key]) is int and pin[key] == fixed for key, fixed in
                   (('duration_seconds', DURATION_SECONDS), ('interval_seconds', INTERVAL_SECONDS),
                    ('max_gap_seconds', MAX_GAP_SECONDS))))
    return pin


def _read_pin():
    raw = _protected(PIN, 4096, private=True)
    return _pin(_object(raw)), sha256(raw).hexdigest()


def _verify_release(manifest):
    _check(isinstance(manifest, str) and HEX.fullmatch(manifest))
    release = OBSERVER_RELEASES / manifest
    common_install._source_files(release, manifest)
    for folder, _, files in os.walk(release, followlinks=False):
        _check(stat.S_IMODE(Path(folder).lstat().st_mode) == 0o555)
        for name in files:
            _check(stat.S_IMODE((Path(folder) / name).lstat().st_mode) == 0o444)
    _check((release / 'tools/m0_manager/operational_sources.py').is_file())
    return release


def _scope(manifest):
    release = _verify_release(manifest)
    machine = _protected(MACHINE_ID, 64).decode('ascii').strip()
    identity = _identity(LEDGER)
    data = _protected(BINDING, 1024 * 1024)
    value = _object(data)
    _check(value['version'] == 1 and isinstance(value['sources'], list) and len(value['sources']) <= 100)
    source, = (item for item in value['sources'] if item['source_id'] == SOURCE_ID)
    device, inode = map(int, identity.split(':'))
    _check((source['kind'], source['locator'], source['class'], source['accepted_schema_versions']) ==
           ('dispatch', str(LEDGER), 'local_only', ['0013'])
           and source['options']['profile'] == 'common_metadata_0013'
           and source['options']['identity'] == {'device': device, 'inode': inode})
    pin = dict(schema=PIN_SCHEMA, machine_id=machine, ledger_identity=identity, observer_manifest_sha256=manifest,
               binding_sha256=sha256(data).hexdigest(), source_id=SOURCE_ID, source_instance=source['source_instance'],
               duration_seconds=DURATION_SECONDS, interval_seconds=INTERVAL_SECONDS, max_gap_seconds=MAX_GAP_SECONDS)
    return _pin(pin), release


def build_pin(observer_manifest_sha256):
    """Build the fixed production-duration pin without writing anything."""
    _check(sys.platform == 'linux' and os.getuid() == os.geteuid() == 0, 'linux_root_required')
    return _scope(observer_manifest_sha256)[0]


@contextmanager
def _lock(*, wait_seconds=0):
    import fcntl

    _check(type(wait_seconds) is int and 0 <= wait_seconds <= 25)
    before = common_install._root_metadata(LOCK, private=True)
    fd = os.open(LOCK, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        after = os.fstat(fd)
        _check((before.st_dev, before.st_ino) == (after.st_dev, after.st_ino))
        deadline = time.monotonic() + wait_seconds
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
                left = deadline - time.monotonic()
                if left <= 0:
                    raise SoakRefused('record_busy') from None
                time.sleep(min(0.05, left))
        _check(_identity(LOCK) == f'{before.st_dev}:{before.st_ino}')
        yield
    finally:
        os.close(fd)


def _guard():
    from .soak_install import guard

    return guard()


def _boot():
    with BOOT_ID.open('rb') as source:
        boot = source.read(64).decode('ascii').strip()
    _check(UUID.fullmatch(boot))
    return boot


def _boottime():
    return time.clock_gettime_ns(time.CLOCK_BOOTTIME) // 1_000_000


def _at(value):
    _check(isinstance(value, str) and value.endswith('+00:00') and timestamp(instant(value)) == value)
    return instant(value)


def _canonical_at(value):
    return timestamp(instant(value))


def _sample(recorded_at, boot, boottime):
    return dict(recorded_at=recorded_at, boot_id=boot, boottime_ms=boottime,
                observed_from=None, observed_until=None, quality='bad', coverage='unknown',
                collector='unknown', campaign='unknown', coordinator='unknown',
                observed_units=0, warning_count=0, unknown_count=0, max_projection_lag=None)


def _normalize(output, pin, sample, probe_started_at):
    _check(output['ok'] is True and output['op'] == 'ops.snapshot' and len(output['items']) == 1)
    source, = output['sources']
    _check((source['source_id'], source['source_instance'], source['schema_version']) ==
           (pin['source_id'], pin['source_instance'], '0013'))
    item, = output['items']
    _check(item['schema'] == OPS_SCHEMA and item['kind'] == 'operational_snapshot' and item['non_atomic'] is True)
    beginning, ending = (_canonical_at(item[key]) for key in ('observation_started_at', 'observation_completed_at'))
    _check(probe_started_at <= beginning <= ending <= sample['recorded_at'])
    observations = item['observations']
    coverage = item['coverage']
    _check(coverage in ('partial', 'complete'))
    units = observations['systemd'].get('units', {})
    _check(type(units) is dict and set(units) <= set(UNITS))
    known_units = sum(unit.get('state') == 'observed' for unit in units.values())
    coordinator = units.get(COORDINATOR, {}).get('active_state', 'unknown')
    collector = observations['collector']['state']
    campaign = observations['campaign']['state']
    values = dict(coverage=coverage, collector=collector, campaign=campaign, coordinator=coordinator)
    _check(all(value in SAMPLE_STATES[key] for key, value in values.items()))
    items = item['attention']
    _check(type(items) is list and len(items) <= 32
           and all(entry['severity'] in ('notice', 'warning', 'unknown') for entry in items))
    warnings = sum(entry['severity'] == 'warning' for entry in items)
    unknowns = sum(entry['severity'] == 'unknown' for entry in items)
    lags = [observations['collector'].get('streams', {}).get(name, {}).get('projection_lag') for name in STREAMS]
    _check(all(value is None or type(value) is int and 0 <= value <= 2**63 - 1 for value in lags))
    observed_boot = observations['boot'].get('boot_id')
    good = (coverage == output['coverage'] == 'complete' and item['state_basis'] == 'current'
            and observations['ledger']['state'] == 'observed' and observations['boot']['state'] == 'observed'
            and observed_boot == sample['boot_id'] and observations['systemd']['state'] == 'observed'
            and known_units == len(UNITS) and coordinator == 'active' and collector == 'ok'
            and observations['collector']['freshness'] == 'fresh'
            and observations['collector']['success_boot_id'] == sample['boot_id']
            and all(value is not None for value in lags)
            and campaign in ('active', 'scheduled', 'intentional_idle') and warnings == unknowns == 0)
    return sample | values | dict(quality='good' if good else 'bad', observed_from=beginning, observed_until=ending,
                                  observed_units=known_units, warning_count=warnings, unknown_count=unknowns,
                                  max_projection_lag=max((value for value in lags if value is not None), default=None))


def _manager_query(release, pin):
    request = canonical(dict(op='ops.snapshot', role='technical', args={'source_id': pin['source_id']}))
    result = subprocess.run(
        ['/usr/bin/python3', '-I', '-B', '-c', QUERY_SCRIPT, str(release), str(BINDING)],
        input=request, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=12, check=False,
        cwd='/', env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'},
    )
    _check(result.returncode == 0 and len(result.stdout) <= 65536, 'snapshot_unavailable')
    return _object(result.stdout)


def _observe(release, pin, clock, boot):
    began = timestamp(clock.now())
    try:
        output = _manager_query(release, pin)
    except (OSError, ValueError, TypeError, KeyError, RecursionError, subprocess.SubprocessError):
        output = None
    sampled = _sample(timestamp(clock.now()), _boot(), _boottime())
    if sampled['boot_id'] != boot or sampled['recorded_at'] < began:
        return sampled
    try:
        return _normalize(output, pin, sampled, began) if output is not None else sampled
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        return sampled


def _check_sample(sample):
    _check(set(sample) == SAMPLE_FIELDS and sample['quality'] in ('good', 'bad')
           and UUID.fullmatch(sample['boot_id'])
           and all(sample[key] in values for key, values in SAMPLE_STATES.items())
           and all(type(sample[key]) is int and sample[key] >= 0 for key in
                   ('boottime_ms', 'observed_units', 'warning_count', 'unknown_count'))
           and sample['observed_units'] <= len(UNITS) and sample['warning_count'] + sample['unknown_count'] <= 32
           and (sample['max_projection_lag'] is None or
                type(sample['max_projection_lag']) is int and 0 <= sample['max_projection_lag'] <= 2**63 - 1))
    recorded = _at(sample['recorded_at'])
    beginning, ending = (sample[key] for key in ('observed_from', 'observed_until'))
    _check((beginning is None and ending is None and sample['quality'] == 'bad')
           or _at(beginning) <= _at(ending) <= recorded)
    if sample['quality'] == 'good':
        _check(sample['coverage'] == 'complete' and sample['collector'] == 'ok' and sample['coordinator'] == 'active'
               and sample['campaign'] in ('active', 'scheduled', 'intentional_idle')
               and sample['observed_units'] == len(UNITS) and sample['warning_count'] == sample['unknown_count'] == 0
               and sample['max_projection_lag'] is not None)


def _read_record(pin_hash):
    try:
        data = _object(_protected(RECORD, MAX_RECORD_BYTES, private=True))
    except FileNotFoundError:
        _check(not os.path.lexists(RECORD))
        return None
    state = data['state']
    samples = data['samples']
    _check(set(data) == RECORD_FIELDS and data['schema'] == RECORD_SCHEMA and data['pin_sha256'] == pin_hash
           and state in ('running', *TERMINAL) and isinstance(samples, list) and 1 <= len(samples) <= MAX_SAMPLES
           and type(data['reboot_count']) is int and data['reboot_count'] == int(data['reason'] == 'boot_changed'))
    if state == 'running':
        _check(data['reason'] is None and data['ended_at'] is None)
    else:
        _check(data['reason'] in BAD_REASONS if state == 'failed' else data['reason'] == TERMINAL[state])
        _at(data['ended_at'])
    for sample in samples:
        _check_sample(sample)
    first = samples[0]
    _check(first['quality'] == 'good' and data['started_at'] == first['recorded_at']
           and data['deadline_at'] == timestamp(_at(data['started_at']) + timedelta(seconds=DURATION_SECONDS)))
    for prior, current in zip(samples, samples[1:]):
        _check(current['boot_id'] == prior['boot_id']
               and current['recorded_at'] >= prior['recorded_at']
               and current['boottime_ms'] >= prior['boottime_ms'])
    if state == 'passed':
        _check(_pass_reason(data) == 'window_passed')
    return data


def _save(data):
    raw = canonical(data)
    _check(len(raw) <= MAX_RECORD_BYTES, 'record_size_limit')
    common_install._atomic(RECORD, raw, mode=0o600)


def _metrics(samples):
    if not samples:
        return dict(sample_count=0, good_samples=0, bad_samples=0, gap_count=0,
                    max_gap_seconds=0, elapsed_seconds=0, boot_elapsed_seconds=0, idle_campaign_samples=0)
    gaps = [max((_at(b['recorded_at']) - _at(a['recorded_at'])).total_seconds(),
                (b['boottime_ms'] - a['boottime_ms']) / 1000)
            for a, b in zip(samples, samples[1:])]
    elapsed = (_at(samples[-1]['recorded_at']) - _at(samples[0]['recorded_at'])).total_seconds()
    good = sum(sample['quality'] == 'good' for sample in samples)
    return dict(sample_count=len(samples), good_samples=good, bad_samples=len(samples) - good,
                gap_count=sum(gap > MAX_GAP_SECONDS for gap in gaps), max_gap_seconds=max(gaps, default=0),
                elapsed_seconds=elapsed, boot_elapsed_seconds=(samples[-1]['boottime_ms'] - samples[0]['boottime_ms']) / 1000,
                idle_campaign_samples=sum(sample['campaign'] == 'intentional_idle' for sample in samples))


def _pass_reason(data):
    metrics = _metrics(data['samples'])
    if metrics['bad_samples']:
        return 'bad_observations'
    if metrics['gap_count']:
        return 'sampling_gap'
    if metrics['good_samples'] < MIN_SAMPLES or min(metrics['elapsed_seconds'], metrics['boot_elapsed_seconds']) < DURATION_SECONDS:
        return 'insufficient_coverage'
    return 'window_passed'


def _view(data):
    bounds = dict(duration_seconds=DURATION_SECONDS, interval_seconds=INTERVAL_SECONDS,
                  max_allowed_gap_seconds=MAX_GAP_SECONDS, min_good_samples=MIN_SAMPLES,
                  observation_scope='sampled_common_operations')
    if data is None:
        return dict(state='not_started', deadline_at=None, **bounds, **_metrics([]))
    return {key: data[key] for key in ('state', 'reason', 'started_at', 'deadline_at', 'ended_at', 'reboot_count')} | {
        'boot_id': data['samples'][0]['boot_id'], 'latest': data['samples'][-1],
        **bounds, **_metrics(data['samples']),
    }


def _end(data, state, reason, at):
    data.update(state=state, reason=reason, ended_at=at)
    _save(data)
    return _view(data)


def _finish_tick(data, state, reason, at):
    _guard()
    return _end(data, state, reason, at)


def _active_scope(pin):
    current, release = _scope(pin['observer_manifest_sha256'])
    _check(current == pin)
    return release


def start(*, clock=None):
    """Record the first healthy observation, once, before enabling the timer."""
    _root()
    clock = clock or Clock()
    with _lock():
        pin, pin_hash = _read_pin()
        data = _read_record(pin_hash)
        release = _active_scope(pin)
        if data is not None:
            _check(data['state'] == 'running', 'record_terminal')
            return _view(data)
        boot = _boot()
        sample = _observe(release, pin, clock, boot)
        _check(sample['quality'] == 'good', 'initial_observation_unavailable')
        start_at = sample['recorded_at']
        data = dict(schema=RECORD_SCHEMA, pin_sha256=pin_hash, state='running', reason=None,
                    started_at=start_at, deadline_at=timestamp(_at(start_at) + timedelta(seconds=DURATION_SECONDS)),
                    ended_at=None, samples=[sample], reboot_count=0)
        _save(data)
        return _view(data)


def tick(*, clock=None):
    _root()
    clock = clock or Clock()
    with _lock():
        pin, pin_hash = _read_pin()
        data = _read_record(pin_hash)
        if data is None or data['state'] != 'running':
            return _view(data)
        _guard()
        prior = data['samples'][-1]
        at, boot, boot_ms = timestamp(clock.now()), _boot(), _boottime()
        if boot != prior['boot_id']:
            data['reboot_count'] = 1
            return _finish_tick(data, 'failed', 'boot_changed', at)
        if at < prior['recorded_at'] or boot_ms < prior['boottime_ms']:
            return _finish_tick(data, 'clock_invalid', 'clock_rollback', at)
        if (at < timestamp(_at(prior['recorded_at']) + timedelta(seconds=INTERVAL_SECONDS))
                or boot_ms - prior['boottime_ms'] < INTERVAL_SECONDS * 1000):
            return _view(data)
        try:
            release = _active_scope(pin)
        except (OSError, ValueError, TypeError, KeyError, AttributeError, RecursionError):
            sample = _sample(at, boot, boot_ms)
        else:
            sample = _observe(release, pin, clock, boot)
        if sample['boot_id'] != boot:
            data['reboot_count'] = 1
            return _finish_tick(data, 'failed', 'boot_changed', sample['recorded_at'])
        if sample['recorded_at'] < at or sample['boottime_ms'] < boot_ms:
            return _finish_tick(data, 'clock_invalid', 'clock_rollback', sample['recorded_at'])
        data['samples'].append(sample)
        first = data['samples'][0]
        if (sample['recorded_at'] >= data['deadline_at']
                and sample['boottime_ms'] - first['boottime_ms'] >= DURATION_SECONDS * 1000):
            reason = _pass_reason(data)
            return _finish_tick(data, 'passed' if reason == 'window_passed' else 'failed', reason, sample['recorded_at'])
        if len(data['samples']) >= MAX_SAMPLES:
            return _finish_tick(data, 'failed', 'sample_limit', sample['recorded_at'])
        _save(data)
        return _view(data)


def pause(*, clock=None, before_commit=None, wait_seconds=25):
    """Serialize the installer pause gate and terminal record with in-flight ticks."""
    _root()
    with _lock(wait_seconds=wait_seconds):
        _, pin_hash = _read_pin()
        data = _read_record(pin_hash)
        if before_commit is not None:
            before_commit()
        if data is None or data['state'] != 'running':
            return _view(data)
        return _end(data, 'paused', 'operator_paused', timestamp((clock or Clock()).now()))


def status(*, clock=None):
    _root()
    _, pin_hash = _read_pin()
    data = _read_record(pin_hash)
    result = _view(data)
    if data is not None and data['state'] == 'running':
        age = ((clock or Clock()).now() - _at(data['samples'][-1]['recorded_at'])).total_seconds()
        result['sampling_overdue'] = age > MAX_GAP_SECONDS
        result['observed_clock_rollback'] = age < 0
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    operations = dict(start=start, tick=tick, pause=pause, status=status)
    parser.add_argument('command', choices=operations)
    try:
        result = operations[parser.parse_args(argv).command]()
    except (OSError, ValueError, TypeError, KeyError, AttributeError, RecursionError, RuntimeError) as error:
        code = error.code if isinstance(error, SoakRefused) else 'soak_runtime_refused'
        print(json.dumps({'ok': False, 'code': code}))
        return 1
    print(json.dumps({'ok': True} | result, ensure_ascii=True, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
