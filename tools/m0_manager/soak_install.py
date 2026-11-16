"""Install and operate a single bounded, read-only Dispatch observation window."""

import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat
import sys
import time

if __package__ in (None, ''):
    source = Path(__file__).resolve().parents[2]
    sys.path[:0] = [str(source / 'dispatch'), str(source)]
    __package__ = 'tools.m0_manager'

from tools.m0_agent import common_install as common


RELEASES = Path('/opt/sa-dispatch-observer/releases')
AUTHORITY = Path('/var/lib/sa-dispatch-soak')
SYSTEMD = Path('/etc/systemd/system')
SERVICE = 'sa-dispatch-soak.service'
TIMER = 'sa-dispatch-soak.timer'
ENTRY = 'tools/m0_manager/soak_install.py'
RUNTIME = 'tools/m0_manager/soak_runtime.py'
SCHEMA = 'superagent.manager.soak_install.v1'
INACTIVE = ('inactive', 'failed')
PIN_KEYS = ('machine_id', 'observer_manifest_sha256', 'ledger_identity', 'binding_sha256',
            'source_id', 'source_instance')
_require = common._require


def _verify_release(release, manifest):
    from tools.m0_agent.common_package import verify_source

    common._source_files(release, manifest)
    _require((release / ENTRY).is_file() and (release / RUNTIME).is_file(), 'observer addon source is incomplete')
    for folder, _, files in os.walk(release, followlinks=False):
        _require(stat.S_IMODE(Path(folder).stat().st_mode) == 0o555, 'observer release directory is not frozen')
        for name in files:
            _require(stat.S_IMODE((Path(folder) / name).stat().st_mode) == 0o444,
                     'observer release file is not frozen')
    verify_source(release, expected_manifest_sha256=manifest)


def _unit_bytes(release):
    service = (
        '[Unit]\nDescription=Record one bounded Dispatch operational observation\n'
        '[Service]\nType=oneshot\nUser=root\nGroup=root\n'
        f'SupplementaryGroups={common.SHARED}\n'
        f'WorkingDirectory={release}\nExecStart=/usr/bin/python3 -I -B {release / RUNTIME} tick\n'
        'NoNewPrivileges=yes\nProtectSystem=strict\nProtectHome=yes\nPrivateTmp=yes\n'
        'PrivateDevices=yes\nPrivateNetwork=yes\nRestrictAddressFamilies=AF_UNIX\n'
        'ProtectKernelTunables=yes\nProtectKernelModules=yes\nProtectKernelLogs=yes\n'
        'ProtectControlGroups=yes\nProtectClock=yes\nRestrictSUIDSGID=yes\nLockPersonality=yes\n'
        'CapabilityBoundingSet=\nInaccessiblePaths=-/run/credstore -/tmp -/var/tmp\n'
        f'ReadWritePaths={AUTHORITY}\n'
        'UMask=0077\nTimeoutStartSec=20s\nMemoryMax=128M\n'
        'StandardOutput=journal\nStandardError=journal\nLogRateLimitIntervalSec=300s\nLogRateLimitBurst=5\n'
    ).encode('ascii')
    timer = (
        '[Unit]\nDescription=Wake the bounded Dispatch observer\n'
        f'[Timer]\nOnBootSec=30s\nOnUnitInactiveSec=300s\nAccuracySec=2s\nPersistent=false\nUnit={SERVICE}\n'
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

    def _read(self):
        common._root_metadata(AUTHORITY, directory=True, private=True)
        state = json.loads(common._root_bytes(self.state_file, private=True), object_pairs_hook=common._unique_fields)
        _require(state['schema'] == SCHEMA and state['machine_id'] == self.machine
                 and state['observer_manifest_sha256'] == self.manifest
                 and state['phase'] in ('installing', 'paused', 'enabled', 'pausing'),
                 'observer receipt has a different identity or invalid phase')
        return state

    def _save(self, state, phase):
        result = {**state, 'schema': SCHEMA, 'machine_id': self.machine, 'observer_manifest_sha256': self.manifest,
                  'phase': phase}
        common._atomic(self.state_file, common._json(result), mode=0o600)
        return result

    def _pin(self, state):
        from .soak_runtime import LOCK, PIN

        raw = common._root_bytes(PIN, private=True)
        common._root_metadata(LOCK, private=True)
        _require(sha256(raw).hexdigest() == state['pin_sha256'], 'observer runtime pin changed')
        pin = json.loads(raw, object_pairs_hook=common._unique_fields)
        _require(all(pin[key] == state[key] for key in PIN_KEYS), 'observer machine, ledger or binding pin differs')
        return pin

    def _check_units(self):
        for path, raw in self.units.items():
            _require(common._root_hash(path) == sha256(raw).hexdigest(), 'observer systemd unit changed')
            _require(common._property(path.name, 'FragmentPath') == str(path)
                     and not common._property(path.name, 'DropInPaths'), 'effective observer systemd unit changed')

    def _publish_units(self):
        for path, raw in self.units.items():
            if os.path.lexists(path):
                _require(common._root_hash(path) == sha256(raw).hexdigest(), 'unowned observer unit bytes')
            else:
                common._atomic(path, raw, mode=0o644)
        common._systemctl('daemon-reload')
        self._check_units()

    def _pause_timer(self):
        common._systemctl('disable', '--now', TIMER)
        _require(common._property(TIMER, 'UnitFileState') == 'disabled'
                 and common._property(TIMER, 'ActiveState') in INACTIVE, 'observer timer did not become disabled')
        deadline = time.monotonic() + 25
        while common._property(SERVICE, 'ActiveState') not in INACTIVE:
            _require(time.monotonic() < deadline, 'observer service is still finishing its current observation')
            time.sleep(1)

    def install(self):
        from .soak_runtime import LOCK, PIN, RECORD, build_pin

        if not os.path.lexists(AUTHORITY):
            AUTHORITY.mkdir(mode=0o700)
            common._sync_parent(AUTHORITY.parent)
        common._root_metadata(AUTHORITY, directory=True, private=True)
        if os.path.lexists(self.state_file):
            state = self._read()
            if state['phase'] != 'installing':
                return self.status()
        else:
            _require(not os.path.lexists(RECORD), 'an unowned observer recording already exists')
            _require(all(not os.path.lexists(path) for path in self.units), 'unowned observer unit already exists')
            pin = build_pin(self.manifest)
            _require(pin['machine_id'] == self.machine and pin['observer_manifest_sha256'] == self.manifest,
                     'observer runtime pin names a different machine or release')
            raw = common._json(pin)
            if os.path.lexists(PIN):
                _require(common._root_bytes(PIN, private=True) == raw, 'existing observer runtime pin differs')
            else:
                common._atomic(PIN, raw, mode=0o600)
            if not os.path.lexists(LOCK):
                common._atomic(LOCK, b'', mode=0o600)
            common._root_metadata(LOCK, private=True)
            state = self._save({key: pin[key] for key in PIN_KEYS} | {'pin_sha256': sha256(raw).hexdigest()},
                               'installing')
        _require(self._pin(state) == build_pin(self.manifest), 'live observer binding changed')
        self._publish_units()
        self._pause_timer()
        self._save(state, 'paused')
        return self.status()

    def guard(self):
        state = self._read()
        _require(state['phase'] == 'enabled', 'observer startup guard is closed')
        self._pin(state)
        self._check_units()
        return {'phase': state['phase'], 'manifest_sha256': self.manifest}

    def enable(self):
        from .soak_runtime import build_pin, start

        state = self._read()
        _require(state['phase'] in ('paused', 'enabled'), 'observer installation or pause is still pending')
        _require(self._pin(state) == build_pin(self.manifest), 'live observer binding changed')
        self._check_units()
        window = start()
        _require(window['state'] == 'running', 'observer window is already terminal')
        self._save(state, 'enabled')
        try:
            common._systemctl('enable', '--now', TIMER)
            _require(common._property(TIMER, 'ActiveState') == 'active', 'observer timer did not become active')
        except (common.InstallError, OSError, common.subprocess.SubprocessError):
            self._save(state, 'paused')
            self._pause_timer()
            raise
        return self.status()

    def pause(self):
        from .soak_runtime import pause

        state = self._read()
        _require(state['phase'] in ('paused', 'enabled', 'pausing'), 'observer installation is still pending')

        def close_gate():
            if state['phase'] != 'pausing':
                self._save(state, 'pausing')

        pause(before_commit=close_gate, wait_seconds=25)
        self._pause_timer()
        self._save(state, 'paused')
        return self.status()

    def status(self):
        from .soak_runtime import status

        state = self._read()
        self._pin(state)
        if state['phase'] in ('paused', 'enabled'):
            self._check_units()
        return {'phase': state['phase'], 'manifest_sha256': self.manifest, 'timer': common._property(TIMER, 'ActiveState'),
                'observation': status()}


def _installer(expected_machine=None, expected_manifest=None):
    from tools.m0_deploy.install_target import observe_install_target

    _require(sys.platform == 'linux' and os.getuid() == os.geteuid() == 0,
             'observer installer requires Linux root')
    release = Path(__file__).absolute().parents[2]
    manifest = release.name
    _require(release == RELEASES / manifest and common.HEX.fullmatch(manifest)
             and expected_manifest in (None, manifest), 'run the observer installer from its pinned frozen release')
    _verify_release(release, manifest)
    common._root_parents(AUTHORITY)
    common._root_parents(SYSTEMD / SERVICE)
    machine, _ = observe_install_target()
    _require(expected_machine in (None, machine), 'explicit machine ID differs from this host')
    return Installer(release, machine, manifest)


def guard():
    """Verify startup authority while the observation runtime holds its own lock."""
    return _installer().guard()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('command', choices=('install', 'enable', 'pause', 'status'))
    parser.add_argument('--expected-machine', required=True)
    parser.add_argument('--manifest-sha256', required=True)
    args = parser.parse_args(argv)
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
                result = getattr(installer, args.command)()
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
