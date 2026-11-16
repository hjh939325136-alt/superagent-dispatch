"""Install and operate the optional resident Collector on the bound common ledger."""

import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat
import sys

if __package__ in (None, ''):
    source = Path(__file__).resolve().parents[2]
    sys.path[:0] = [str(source / 'dispatch'), str(source)]
    __package__ = 'tools.m0_agent'

from . import common_install as common


RELEASES = Path('/opt/sa-dispatch-collector/releases')
AUTHORITY = Path('/var/lib/sa-dispatch-collector')
SYSTEMD = Path('/etc/systemd/system')
SERVICE = 'sa-dispatch-collector.service'
TIMER = 'sa-dispatch-collector.timer'
ENTRY = 'tools/m0_agent/collector_install.py'
RUNTIME = 'tools/m0_agent/collector_runtime.py'
SCHEMA = 'superagent.common.collector_install.v1'
INACTIVE = ('inactive', 'failed')
PIN_KEYS = ('machine_id', 'ledger_identity', 'binding_raw_sha256', 'binding_sha256')
LIMIT_KEYS = ('max_ledger_bytes', 'min_free_bytes', 'stale_after_seconds')
_require = common._require


def _verify_release(release, manifest):
    from .common_package import verify_source

    common._source_files(release, manifest)
    _require((release / ENTRY).is_file() and (release / RUNTIME).is_file(), 'collector addon source is incomplete')
    for folder, _, files in os.walk(release, followlinks=False):
        _require(stat.S_IMODE(Path(folder).stat().st_mode) == 0o555, 'collector release directory is not frozen')
        for name in files:
            _require(stat.S_IMODE((Path(folder) / name).stat().st_mode) == 0o444,
                     'collector release file is not frozen')
    verify_source(release, expected_manifest_sha256=manifest)


def _unit_bytes(release):
    service = (
        '[Unit]\nDescription=Advance the bound Dispatch Collector cursors\n'
        '[Service]\nType=oneshot\nUser=root\nGroup=root\n'
        f'SupplementaryGroups={common.SHARED}\n'
        f'WorkingDirectory={release}\nExecStart=/usr/bin/python3 -I -B {release / RUNTIME} tick\n'
        'NoNewPrivileges=yes\nProtectSystem=strict\nProtectHome=yes\nPrivateTmp=yes\n'
        'PrivateDevices=yes\nPrivateNetwork=yes\nRestrictAddressFamilies=AF_UNIX\n'
        'ProtectKernelTunables=yes\nProtectKernelModules=yes\nProtectKernelLogs=yes\n'
        'ProtectControlGroups=yes\nProtectClock=yes\nRestrictSUIDSGID=yes\nLockPersonality=yes\n'
        'CapabilityBoundingSet=\nInaccessiblePaths=-/run/credstore\n'
        f'ReadWritePaths={common.LEDGER.parent} {AUTHORITY}\n'
        'UMask=0007\nTimeoutStartSec=45s\nMemoryMax=128M\n'
        'StandardOutput=journal\nStandardError=journal\nLogRateLimitIntervalSec=60s\nLogRateLimitBurst=10\n'
    ).encode('ascii')
    timer = (
        '[Unit]\nDescription=Wake the bound Dispatch Collector\n'
        f'[Timer]\nOnBootSec=30s\nOnUnitInactiveSec=60s\nAccuracySec=2s\nPersistent=false\nUnit={SERVICE}\n'
        '[Install]\nWantedBy=timers.target\n'
    ).encode('ascii')
    return {SYSTEMD / SERVICE: service, SYSTEMD / TIMER: timer}


class Installer:
    def __init__(self, release, machine, manifest):
        self.release = release
        self.machine = machine
        self.manifest = manifest
        self.units = _unit_bytes(release)
        self.state_file = AUTHORITY / 'state.json'

    def _state(self):
        common._root_metadata(AUTHORITY, directory=True, private=True)
        result = json.loads(common._root_bytes(self.state_file, private=True), object_pairs_hook=common._unique_fields)
        _require(result['schema'] == SCHEMA and result['machine_id'] == self.machine
                 and result['phase'] in ('installing', 'paused', 'enabled', 'upgrading'),
                 'collector receipt has a different identity or invalid phase')
        return result

    def _read(self):
        state = self._state()
        _require(state['manifest_sha256'] == self.manifest, 'collector receipt belongs to a different frozen release')
        return state

    def _save(self, state, phase):
        state = {**state, 'schema': SCHEMA, 'machine_id': self.machine, 'manifest_sha256': self.manifest,
                 'phase': phase}
        common._atomic(self.state_file, common._json(state), mode=0o600)
        return state

    def _pin(self, state):
        from .collector_runtime import PIN

        raw = common._root_bytes(PIN, private=True)
        _require(sha256(raw).hexdigest() == state['pin_sha256'], 'collector runtime pin changed')
        pin = json.loads(raw, object_pairs_hook=common._unique_fields)
        _require(all(pin[key] == state[key] for key in PIN_KEYS), 'collector machine, ledger or binding pin differs')
        return pin

    def _verify_live_pin(self, pin):
        from .collector_runtime import build_pin

        _require(build_pin(**{key: pin[key] for key in LIMIT_KEYS}) == pin, 'live Collector binding changed')

    def _check_units(self):
        for path, raw in self.units.items():
            _require(common._root_hash(path) == sha256(raw).hexdigest(), 'collector systemd unit changed')
            _require(common._property(path.name, 'FragmentPath') == str(path)
                     and not common._property(path.name, 'DropInPaths'), 'effective collector systemd unit changed')

    def _publish_units(self, previous=None):
        for path, raw in self.units.items():
            digest = sha256(raw).hexdigest()
            if os.path.lexists(path):
                installed = common._root_hash(path)
                if installed == digest:
                    continue
                _require(previous is not None and installed == previous[path.name], 'unowned collector unit bytes')
            else:
                _require(previous is None, 'collector unit disappeared during source upgrade')
            common._atomic(path, raw, mode=0o644)
        common._systemctl('daemon-reload')
        self._check_units()

    def _pause_timer(self):
        common._systemctl('disable', '--now', TIMER)
        _require(common._property(TIMER, 'UnitFileState') == 'disabled'
                 and common._property(TIMER, 'ActiveState') in INACTIVE, 'collector timer did not become disabled')
        # The enclosing maintenance lock already excluded a complete runtime tick.

    def install(self):
        from .collector_runtime import PIN, build_pin

        if not os.path.lexists(AUTHORITY):
            AUTHORITY.mkdir(mode=0o700)
            common._sync_parent(AUTHORITY.parent)
        common._root_metadata(AUTHORITY, directory=True, private=True)
        if os.path.lexists(self.state_file):
            state = self._read()
            if state['phase'] != 'installing':
                return self.status()
        else:
            _require(all(not os.path.lexists(path) for path in self.units), 'unowned collector unit already exists')
            pin = build_pin()
            _require(pin['machine_id'] == self.machine, 'collector runtime pin names a different machine')
            raw = common._json(pin)
            if os.path.lexists(PIN):
                _require(common._root_bytes(PIN, private=True) == raw, 'existing collector runtime pin differs')
            else:
                common._atomic(PIN, raw, mode=0o600)
            state = self._save({key: pin[key] for key in PIN_KEYS} | {'pin_sha256': sha256(raw).hexdigest()},
                               'installing')
        self._verify_live_pin(self._pin(state))
        self._publish_units()
        self._pause_timer()
        self._save(state, 'paused')
        return self.status()

    def guard(self):
        state = self._read()
        _require(state['phase'] == 'enabled', 'collector startup guard is closed')
        self._pin(state)
        self._check_units()
        return {'phase': state['phase'], 'manifest_sha256': self.manifest}

    def enable(self):
        state = self._read()
        _require(state['phase'] in ('paused', 'enabled'), 'collector installation is still pending')
        self._verify_live_pin(self._pin(state))
        self._check_units()
        # A timer awakened during this transition still cannot enter the maintenance lock.
        try:
            common._systemctl('enable', '--now', TIMER)
            _require(common._property(TIMER, 'ActiveState') == 'active', 'collector timer did not become active')
        except (common.InstallError, OSError, common.subprocess.SubprocessError):
            self._save(state, 'paused')
            self._pause_timer()
            raise
        self._save(state, 'enabled')
        return self.status()

    def pause(self):
        state = self._read()
        _require(state['phase'] in ('paused', 'enabled'), 'collector installation is still pending')
        self._save(state, 'paused')
        self._pause_timer()
        return self.status()

    def status(self):
        from .collector_runtime import status

        state = self._read()
        self._pin(state)
        if state['phase'] in ('paused', 'enabled'):
            self._check_units()
        return {'phase': state['phase'], 'manifest_sha256': self.manifest, 'timer': common._property(TIMER, 'ActiveState'),
                'collector': status()}

    def upgrade(self, previous):
        _require(isinstance(previous, str) and common.HEX.fullmatch(previous) and previous != self.manifest,
                 'a different explicit previous Collector manifest SHA256 is required')
        previous_release = RELEASES / previous
        _verify_release(previous_release, previous)
        state = self._state()
        self._verify_live_pin(self._pin(state))
        if state['manifest_sha256'] == self.manifest:
            _require(state.get('previous_manifest_sha256') == previous, 'previous Collector manifest differs')
            return self.status()
        _require(state['manifest_sha256'] == previous, 'expected previous Collector manifest is not installed')
        if state['phase'] in ('enabled', 'paused'):
            probe = common._run('/usr/bin/python3', '-I', '-B', str(previous_release / ENTRY), 'status',
                                '--expected-machine', self.machine, '--manifest-sha256', previous)
            _require(json.loads(probe.stdout).get('ok') is True, 'previous Collector install anchor did not verify')
            state = {**state, 'phase': 'upgrading', 'pending_manifest_sha256': self.manifest,
                     'previous_units': {path.name: common._root_hash(path) for path in self.units}}
            common._atomic(self.state_file, common._json(state), mode=0o600)
        _require(state['phase'] == 'upgrading' and state['pending_manifest_sha256'] == self.manifest,
                 'a different Collector installation or source upgrade is pending')
        self._pause_timer()
        self._publish_units(state['previous_units'])
        state = {key: value for key, value in state.items() if key not in ('pending_manifest_sha256', 'previous_units')}
        self._save(state | {'previous_manifest_sha256': previous}, 'paused')
        return self.status()


def _installer(expected_machine=None, expected_manifest=None):
    from tools.m0_deploy.install_target import observe_install_target

    _require(sys.platform == 'linux' and os.getuid() == os.geteuid() == 0,
             'collector installer requires Linux root')
    release = Path(__file__).absolute().parents[2]
    manifest = release.name
    _require(release == RELEASES / manifest and common.HEX.fullmatch(manifest)
             and expected_manifest in (None, manifest), 'run the Collector installer from its pinned frozen release')
    _verify_release(release, manifest)
    common._root_parents(AUTHORITY)
    common._root_parents(SYSTEMD / SERVICE)
    machine, _ = observe_install_target()
    _require(expected_machine in (None, machine), 'explicit machine ID differs from this host')
    return Installer(release, machine, manifest)


def guard():
    """Verify startup authority while the runtime holds both maintenance locks."""
    return _installer().guard()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('command', choices=('install', 'enable', 'pause', 'status', 'upgrade'))
    parser.add_argument('--expected-machine', required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--expected-previous-manifest')
    args = parser.parse_args(argv)
    if (args.command == 'upgrade') != (args.expected_previous_manifest is not None):
        parser.error('--expected-previous-manifest is required for upgrade only')
    try:
        _require(re.fullmatch(r'[0-9a-f]{32}', args.expected_machine) and common.HEX.fullmatch(args.manifest_sha256),
                 'explicit machine ID and manifest SHA256 are required')
        installer = _installer(args.expected_machine, args.manifest_sha256)
        os.umask(0o077)
        if args.command == 'status':
            result = installer.status()
        else:
            from tools.m0_native_mixed.maintenance import _lock

            with _lock(common.MAINTENANCE_LOCK):
                result = (installer.upgrade(args.expected_previous_manifest) if args.command == 'upgrade'
                          else getattr(installer, args.command)())
    except Exception as error:
        result = {'ok': False, 'operation': args.command, 'error': type(error).__name__}
        if isinstance(error, common.InstallError):
            result['detail'] = str(error)
        print(json.dumps(result, sort_keys=True))
        return 1
    print(json.dumps({'ok': True, **result}, sort_keys=True, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
