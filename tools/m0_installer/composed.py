"""Install the verified common stack on a fresh Debian guest without model authority."""

import argparse
from contextlib import closing
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys
from tempfile import TemporaryDirectory
import zipfile

if __package__ in (None, ''):
    source = Path(__file__).resolve().parents[2]
    sys.path[:0] = [str(source / 'dispatch'), str(source)]
    __package__ = 'tools.m0_installer'

from tools.m0_agent import collector_install, common_install, common_package
from tools.m0_manager import soak_install

from . import build_common


AUTHORITY = Path('/var/lib/sa-dispatch-composed')
ENTRY = 'tools/m0_installer/composed.py'
SCHEMA = 'superagent.common.composed_install.v1'
PHASES = ('intent', 'baseline_ready', 'common_prepared', 'common_ready',
          'collector_installed', 'collector_enabled', 'observer_installed', 'completed')
INACTIVE = ('inactive', 'failed')
MAX_REPORT = 64 * 1024
CREDENTIAL = Path('/run/credstore/provider_api_key')
CAMPAIGN_AUTHORITY = Path('/var/lib/sa-dispatch-agent-campaign')
CAMPAIGN_CONFIG = Path('/etc/sa-dispatch-agent-campaign')
ROLE_UNITS = tuple(f'sa-agent-{role}.{unit}' for role in ('producer', 'reviewer')
                   for unit in ('service', 'timer'))
_require = common_install._require


def _read_json(path):
    return json.loads(common_install._root_bytes(path, private=True, limit=MAX_REPORT),
                      object_pairs_hook=common_install._unique_fields)


def _baseline_payload(path, manifest, archive_hash):
    report = build_common.verify_archive(path, expected_manifest_sha256=manifest, expected_zip_sha256=archive_hash)
    declaration = report['manifest']
    _require(declaration['distribution'] == 'installer_candidate'
             and declaration['installer_entry'] == build_common.PAYLOAD + build_common.INSTALLER,
             'baseline archive is not a common installer candidate')
    raw = build_common._read(path, build_common.MAX_ARCHIVE_BYTES)
    _require(sha256(raw).hexdigest() == archive_hash, 'baseline archive changed after verification')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        return {name.removeprefix(build_common.PAYLOAD): archive.read(name) for name in declaration['files']}


def _verify_frozen(target, payload):
    common_install._root_parents(target)
    folders = {Path('.')}.union(*(Path(name).parents for name in payload))
    found = set()
    for folder, directories, files in os.walk(target, followlinks=False):
        path = Path(folder)
        info = common_install._root_metadata(path, directory=True)
        _require(stat.S_IMODE(info.st_mode) == 0o555 and path.relative_to(target) in folders,
                 'staged source tree contains a foreign or writable directory')
        for name in directories:
            common_install._root_metadata(path / name, directory=True)
        for name in files:
            file = path / name
            member = file.relative_to(target).as_posix()
            info = common_install._root_metadata(file)
            _require(member in payload and stat.S_IMODE(info.st_mode) == 0o444
                     and info.st_size == len(payload[member])
                     and common_install._root_hash(file) == sha256(payload[member]).hexdigest(),
                     'staged source file changed')
            found.add(member)
    _require(found == set(payload), 'staged source tree is incomplete')


def _freeze(target, payload):
    for directory in reversed(target.parents):
        if not os.path.lexists(directory):
            directory.mkdir(mode=0o755)
            directory.chmod(0o755)
            common_install._sync_parent(directory.parent)
        common_install._root_metadata(directory, directory=True)
    if not os.path.lexists(target):
        with TemporaryDirectory(prefix='.composed-source-', dir=target.parent) as work:
            staging = Path(work) / 'source'
            staging.mkdir()
            for name, data in payload.items():
                path = build_common._archive_path(staging, name)
                path.parent.mkdir(parents=True, exist_ok=True)
                common_install._atomic(path, data, mode=0o444)
            for folder, _, _ in os.walk(staging, topdown=False):
                path = Path(folder)
                path.chmod(0o555)
                common_install._sync_parent(path)
            staging.rename(target)
            common_install._sync_parent(target.parent)
    _verify_frozen(target, payload)


def _command(entry, action, flags):
    result = subprocess.run(
        ['/usr/bin/python3', '-I', '-B', str(entry), action, *flags],
        env=common_install.ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=600, check=False,
    )
    _require(result.returncode == 0 and len(result.stdout) <= MAX_REPORT,
             f'{entry.name} {action} refused; inspect its protected receipt')
    report = json.loads(result.stdout, object_pairs_hook=common_install._unique_fields)
    _require(isinstance(report, dict) and report.get('ok') is True,
             f'{entry.name} {action} returned an invalid receipt')
    return report


def _model_preflight():
    _require(not any(os.path.lexists(path) for path in (CREDENTIAL, CAMPAIGN_AUTHORITY, CAMPAIGN_CONFIG)),
             'external model credentials or a campaign configuration already exist')
    _require(all(common_install._property(unit, 'LoadState') == 'not-found' for unit in ROLE_UNITS),
             'campaign model units already exist')


def _verify_no_models():
    from dispatch.sa_dispatch.agent_control import BUDGET_ID
    from dispatch.sa_dispatch.campaign_validation import PREFIX, ROUTE

    _model_preflight()
    _require(common_install._property(common_install.TIMER, 'UnitFileState') == 'disabled'
             and all(common_install._property(unit, 'ActiveState') in INACTIVE
                     for unit in (common_install.TIMER, common_install.AGENT)),
             'the legacy model unit or timer is not closed')
    with closing(sqlite3.connect(common_install.LEDGER.as_uri() + '?mode=ro', uri=True)) as conn, conn:
        conn.execute('BEGIN')
        for table in ('tasks', 'devices', 'schedules', 'schedule_firings'):
            _require(conn.execute(f'SELECT 1 FROM {table} WHERE execution_route=? LIMIT 1', (ROUTE,)).fetchone() is None,
                     'a model execution record exists in the fresh ledger')
        _require(conn.execute('SELECT 1 FROM control_budgets WHERE budget_id=? OR budget_id GLOB ? LIMIT 1',
                              (BUDGET_ID, PREFIX + '*')).fetchone() is None,
                 'a model request budget exists in the fresh ledger')


class Installer:
    def __init__(self, identity, baseline_path, addon_path):
        self.identity = identity
        self.baseline_path = baseline_path
        self.addon_path = addon_path
        self.manifest = identity['addon_manifest_sha256']
        self.state_file = AUTHORITY / 'state.json'
        self.baseline = AUTHORITY / ('baseline-' + identity['baseline_manifest_sha256'])
        self.common = common_install.RELEASES / self.manifest
        self.collector = collector_install.RELEASES / self.manifest
        self.observer = soak_install.RELEASES / self.manifest

    def _read(self):
        common_install._root_metadata(AUTHORITY, directory=True, private=True)
        state = _read_json(self.state_file)
        _require(state['schema'] == SCHEMA and state['identity'] == self.identity and state['phase'] in PHASES,
                 'composed install receipt belongs to a different package or machine')
        return state

    def _save(self, state, phase, **fields):
        result = {**state, **fields, 'schema': SCHEMA, 'identity': self.identity, 'phase': phase}
        common_install._atomic(self.state_file, common_install._json(result), mode=0o600)
        return result

    def _stage(self):
        baseline = _baseline_payload(self.baseline_path, self.identity['baseline_manifest_sha256'],
                                     self.identity['baseline_zip_sha256'])
        _, addon = common_package.read_archive(self.addon_path, expected_manifest_sha256=self.manifest,
                                               expected_zip_sha256=self.identity['addon_zip_sha256'])
        _require({ENTRY, common_install.ENTRY, collector_install.ENTRY, collector_install.RUNTIME,
                  soak_install.ENTRY, soak_install.RUNTIME} <= addon.keys(),
                 'extension archive lacks a required composed component')
        _freeze(self.baseline, baseline)
        for release in (self.common, self.collector, self.observer):
            _freeze(release, addon)

    def _run_baseline(self):
        return _command(self.baseline / build_common.INSTALLER, 'install', (
            str(self.baseline_path), '--manifest-sha256', self.identity['baseline_manifest_sha256'],
            '--zip-sha256', self.identity['baseline_zip_sha256'], '--expected-machine-id', self.identity['machine_id'],
            '--expected-hostname', self.identity['hostname'],
        ))

    def _addon(self, release, entry, action):
        return _command(release / entry, action, ('--expected-machine', self.identity['machine_id'],
                                                  '--manifest-sha256', self.manifest))

    def _common_result(self, action, phase, identity):
        result = self._addon(self.common, common_install.ENTRY, action)
        _require(result['phase'] == phase and result['manifest_sha256'] == self.manifest
                 and result['machine_id'] == self.identity['machine_id'] and result['ledger_identity'] == identity,
                 'common addon stage or ledger identity differs')
        if phase == 'ready':
            _require(result.get('execution_mode') == 'offline',
                     'common addon is not sealed for offline installation')

    def _verify_common_ready(self, state):
        self._common_result('status', 'ready', state['ledger_identity'])
        _verify_no_models()
        _require(common_install._check_schema() == ('0013', state['ledger_identity'])
                 and common_install._property(common_install.HTTP, 'ActiveState') == 'active',
                 'common 0013 coordinator or ledger is unavailable')

    def _complete(self, state):
        self._verify_common_ready(state)
        collector = self._addon(self.collector, collector_install.ENTRY, 'status')
        observer = self._addon(self.observer, soak_install.ENTRY, 'status')
        _require(collector['manifest_sha256'] == observer['manifest_sha256'] == self.manifest,
                 'collector or observer release differs')
        return {'phase': 'completed', 'ledger_schema': '0013', 'ledger_mode': 'mixed',
                'baseline_manifest_sha256': self.identity['baseline_manifest_sha256'],
                'addon_manifest_sha256': self.manifest, 'coordinator': 'active',
                'collector': {'phase': collector['phase'], 'timer': collector['timer']},
                'observer': {'phase': observer['phase'], 'timer': observer['timer'],
                             'observation': observer['observation']['state']},
                'legacy_agent': 'disabled', 'campaign': 'unconfigured'}

    def install(self):
        _model_preflight()
        if os.path.lexists(self.state_file):
            state = self._read()
        else:
            _require(not any(os.path.lexists(path) for path in (common_install.LEDGER, common_install.AUTHORITY,
                                                                 collector_install.AUTHORITY, soak_install.AUTHORITY)),
                     'unowned common or addon installation already exists')
            state = self._save({}, 'intent')
        if state['phase'] == 'completed':
            return self._complete(state)

        self._stage()
        if state['phase'] == 'intent':
            baseline = self._run_baseline()
            _require(baseline['phase'] == 'completed' and baseline['runtime_healthy'] is True
                     and baseline['manifest_sha256'] == self.identity['baseline_manifest_sha256']
                     and baseline['zip_sha256'] == self.identity['baseline_zip_sha256'],
                     'baseline installer did not verify this package and coordinator')
            version, identity = common_install._check_schema()
            _require(version == '0012', 'fresh common installer did not produce mixed 0012')
            state = self._save(state, 'baseline_ready', ledger_identity=identity,
                               baseline_proof_sha256=baseline['fresh_install_sha256'])
        if state['phase'] == 'baseline_ready':
            self._common_result('install', 'prepared', state['ledger_identity'])
            state = self._save(state, 'common_prepared')
        if state['phase'] == 'common_prepared':
            self._common_result('seal-offline', 'ready', state['ledger_identity'])
            _verify_no_models()
            state = self._save(state, 'common_ready')
        self._verify_common_ready(state)
        if state['phase'] == 'common_ready':
            collector = self._addon(self.collector, collector_install.ENTRY, 'install')
            _require(collector['phase'] == 'paused', 'Collector installation did not stay paused')
            state = self._save(state, 'collector_installed')
        if state['phase'] == 'collector_installed':
            collector = self._addon(self.collector, collector_install.ENTRY, 'enable')
            _require(collector['phase'] == 'enabled' and collector['timer'] == 'active',
                     'Collector timer did not become active')
            state = self._save(state, 'collector_enabled')
        if state['phase'] == 'collector_enabled':
            observer = self._addon(self.observer, soak_install.ENTRY, 'install')
            _require(observer['phase'] == 'paused' and observer['timer'] in INACTIVE
                     and observer['observation']['state'] == 'not_started',
                     'observer installation did not remain unstarted')
            state = self._save(state, 'observer_installed')
        result = self._complete(state)
        self._save(state, 'completed')
        return result

    def status(self):
        state = self._read()
        if state['phase'] == 'completed':
            return self._complete(state)
        return {'phase': state['phase'], 'baseline_manifest_sha256': self.identity['baseline_manifest_sha256'],
                'addon_manifest_sha256': self.manifest}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('command', choices=('install', 'status'))
    parser.add_argument('--baseline-zip', type=Path, required=True)
    parser.add_argument('--baseline-manifest-sha256', required=True)
    parser.add_argument('--baseline-zip-sha256', required=True)
    parser.add_argument('--addon-zip', type=Path, required=True)
    parser.add_argument('--addon-manifest-sha256', required=True)
    parser.add_argument('--addon-zip-sha256', required=True)
    parser.add_argument('--expected-machine-id', required=True)
    parser.add_argument('--expected-hostname', required=True)
    args = parser.parse_args(argv)
    try:
        from tools.m0_deploy.install_target import observe_install_target
        from tools.m0_native_mixed.maintenance import _lock

        _require(sys.platform == 'linux' and os.getuid() == os.geteuid() == 0
                 and sys.flags.isolated and sys.dont_write_bytecode, 'requires Linux root and python3 -I -B')
        identity = {field: getattr(args, field) for field in ('baseline_manifest_sha256', 'baseline_zip_sha256',
                                                              'addon_manifest_sha256', 'addon_zip_sha256')}
        _require(all(common_install.HEX.fullmatch(value) for value in identity.values())
                 and re.fullmatch(r'[0-9a-f]{32}', args.expected_machine_id), 'explicit package hashes and machine ID required')
        for path in (args.baseline_zip, args.addon_zip):
            _require(path.is_absolute() and '..' not in path.parts, 'package path must be an unredirected absolute path')
            common_install._root_parents(path)
            common_install._root_metadata(path)
        source = Path(__file__).absolute().parents[2]
        common_install._root_parents(source)
        common_install._source_files(source, args.addon_manifest_sha256)
        common_package.verify_source(source, expected_manifest_sha256=args.addon_manifest_sha256)
        machine, hostname = observe_install_target()
        _require((machine, hostname) == (args.expected_machine_id, args.expected_hostname),
                 'explicit target identity differs from this host')
        identity.update(machine_id=machine, hostname=hostname)
        installer = Installer(identity, args.baseline_zip, args.addon_zip)
        common_install._root_parents(AUTHORITY)
        os.umask(0o077)
        if args.command == 'status':
            result = installer.status()
        else:
            if not os.path.lexists(AUTHORITY):
                AUTHORITY.mkdir(mode=0o700, exist_ok=True)
                common_install._sync_parent(AUTHORITY.parent)
            common_install._root_metadata(AUTHORITY, directory=True, private=True)
            lock = AUTHORITY / '.lock'
            try:
                fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            except FileExistsError:
                pass
            else:
                os.close(fd)
                common_install._sync_parent(AUTHORITY)
            common_install._root_metadata(lock, private=True)
            with _lock(lock):
                result = installer.install()
    except Exception as error:
        result = {'ok': False, 'operation': args.command, 'error': type(error).__name__}
        if isinstance(error, common_install.InstallError):
            result['detail'] = str(error)
        print(json.dumps(result, sort_keys=True))
        return 1
    print(json.dumps({'ok': True, **result}, sort_keys=True, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
