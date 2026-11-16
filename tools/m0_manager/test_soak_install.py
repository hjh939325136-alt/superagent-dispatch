"""Fixed observer unit, protected install receipt, and interruption boundaries."""

from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from . import soak_install as install


class SoakInstallTests(unittest.TestCase):
    def setUp(self):
        self.manifest = 'a' * 64
        self.machine = 'b' * 32
        self.release = install.RELEASES / self.manifest
        self.installer = install.Installer(self.release, self.machine, self.manifest)

    def test_unprivileged_service_has_only_observer_directory_writable(self):
        service, timer = (raw.decode('ascii') for raw in self.installer.units.values())
        for line in ('Type=oneshot', 'User=root', f'SupplementaryGroups={install.common.SHARED}',
                     'CapabilityBoundingSet=', 'PrivateNetwork=yes', 'RestrictAddressFamilies=AF_UNIX',
                     'ProtectSystem=strict', f'ReadWritePaths={install.AUTHORITY}',
                     'InaccessiblePaths=-/run/credstore -/tmp -/var/tmp', 'TimeoutStartSec=20s', 'MemoryMax=128M',
                     'LogRateLimitIntervalSec=300s', 'LogRateLimitBurst=5',
                     f'ExecStart=/usr/bin/python3 -I -B {self.release / install.RUNTIME} tick'):
            self.assertIn(line + '\n', service)
        self.assertEqual([line for line in service.splitlines() if line.startswith('ReadWritePaths=')],
                         [f'ReadWritePaths={install.AUTHORITY}'])
        for value in ('LoadCredential', 'ExecStartPre', 'network-online'):
            self.assertNotIn(value, service)
        for line in ('OnBootSec=30s', 'OnUnitInactiveSec=300s', 'Persistent=false', f'Unit={install.SERVICE}'):
            self.assertIn(line + '\n', timer)

    def test_install_seals_exact_pin_and_lock_then_stays_paused(self):
        root = Path(self.enterContext(TemporaryDirectory())).resolve()
        self.installer.state_file = root / 'state.json'
        pin_path, lock_path, record_path = (root / name for name in ('pin.json', 'soak.lock', 'record.json'))
        pin = dict(machine_id=self.machine, observer_manifest_sha256=self.manifest,
                   ledger_identity='1:2', binding_sha256='c' * 64, source_id='dispatch',
                   source_instance='fixture-ledger')
        runtime = SimpleNamespace(PIN=pin_path, LOCK=lock_path, RECORD=record_path,
                                  build_pin=Mock(return_value=pin))
        writes = []

        def atomic(path, raw, *, mode):
            writes.append((path, mode))
            path.write_bytes(raw)

        with patch.dict('sys.modules', {'tools.m0_manager.soak_runtime': runtime}), \
             patch.object(install, 'AUTHORITY', root), patch.object(self.installer, 'units', {}), \
             patch.object(install.common, '_root_metadata'), \
             patch.object(install.common, '_root_bytes', side_effect=lambda path, **_: path.read_bytes()), \
             patch.object(install.common, '_atomic', side_effect=atomic), \
             patch.object(self.installer, '_publish_units') as publish, \
             patch.object(self.installer, '_pause_timer') as pause, \
             patch.object(self.installer, 'status', return_value={'phase': 'paused'}):
            self.assertEqual(self.installer.install()['phase'], 'paused')
            self.assertEqual(json.loads(pin_path.read_bytes()), pin)
            state = json.loads(self.installer.state_file.read_bytes())
            self.assertEqual(state['pin_sha256'], sha256(pin_path.read_bytes()).hexdigest())
            self.assertEqual(state['phase'], 'paused')
            self.assertFalse(record_path.exists())
            self.assertEqual(writes, [(pin_path, 0o600), (lock_path, 0o600),
                                      (self.installer.state_file, 0o600), (self.installer.state_file, 0o600)])
            self.installer.install()
            self.assertEqual(len(writes), 4)
            publish.assert_called_once_with()
            pause.assert_called_once_with()

    def test_guard_refuses_interrupted_install_or_pause_before_any_sample(self):
        with patch.object(self.installer, '_read') as read, patch.object(self.installer, '_pin') as pin, \
             patch.object(self.installer, '_check_units') as units:
            for phase in ('installing', 'paused', 'pausing'):
                read.return_value = {'phase': phase}
                with self.subTest(phase=phase), self.assertRaisesRegex(install.common.InstallError, 'guard is closed'):
                    self.installer.guard()
            pin.assert_not_called()
            units.assert_not_called()
            read.return_value = {'phase': 'enabled'}
            self.assertEqual(self.installer.guard()['phase'], 'enabled')
            pin.assert_called_once()
            units.assert_called_once()

    def test_enable_commits_only_a_good_observation_and_reuses_existing_deadline(self):
        state = {'phase': 'paused'}
        trace = []
        observed = {'state': 'running', 'deadline_at': '2026-11-17T00:00:00+00:00'}
        runtime = SimpleNamespace(build_pin=lambda _: {}, start=Mock(side_effect=[
            ValueError('initial observation failed'), observed, observed,
        ]))

        def save(_, phase):
            state.update(phase=phase)
            trace.append(phase)
            return dict(state)

        with patch.dict('sys.modules', {'tools.m0_manager.soak_runtime': runtime}), \
             patch.object(self.installer, '_read', side_effect=lambda: dict(state)), \
             patch.object(self.installer, '_pin', return_value={}), patch.object(self.installer, '_check_units'), \
             patch.object(self.installer, '_save', side_effect=save), \
             patch.object(self.installer, '_pause_timer') as pause_timer, \
             patch.object(self.installer, 'status', side_effect=lambda: state | {'observation': observed}), \
             patch.object(install.common, '_property', side_effect=['inactive', 'active']), \
             patch.object(install.common, '_systemctl') as systemctl:
            with self.assertRaisesRegex(ValueError, 'initial observation failed'):
                self.installer.enable()
            systemctl.assert_not_called()
            with self.assertRaisesRegex(install.common.InstallError, 'did not become active'):
                self.installer.enable()
            self.assertEqual(state['phase'], 'paused')
            pause_timer.assert_called_once_with()
            self.assertEqual(self.installer.enable()['observation']['deadline_at'], observed['deadline_at'])
            self.assertEqual(trace, ['enabled', 'paused', 'enabled'])
            self.assertEqual(systemctl.call_count, 2)

    def test_pause_retry_keeps_gate_closed_and_preserves_terminal_observation(self):
        state = {'phase': 'enabled'}
        trace = []

        def pause(*, before_commit, wait_seconds):
            self.assertEqual(wait_seconds, 25)
            trace.append('under recorder lock')
            before_commit()
            self.assertEqual(state['phase'], 'pausing')
            if trace.count('under recorder lock') == 1:
                raise KeyboardInterrupt
            trace.append('record paused')
            return {'state': 'paused'}

        runtime = SimpleNamespace(pause=pause,
                                  build_pin=lambda _: {}, start=Mock(return_value={'state': 'paused'}))

        def save(_, phase):
            state.update(phase=phase)
            trace.append(phase)
            return dict(state)

        with patch.dict('sys.modules', {'tools.m0_manager.soak_runtime': runtime}), \
             patch.object(self.installer, '_read', side_effect=lambda: dict(state)), \
             patch.object(self.installer, '_save', side_effect=save), \
             patch.object(self.installer, '_pause_timer', side_effect=lambda: trace.append('timer drained')), \
             patch.object(self.installer, '_pin', return_value={}), patch.object(self.installer, '_check_units'), \
             patch.object(self.installer, 'status', side_effect=lambda: dict(state)), \
             patch.object(install.common, '_systemctl') as systemctl:
            with self.assertRaises(KeyboardInterrupt):
                self.installer.pause()
            self.assertEqual(state['phase'], 'pausing')
            with self.assertRaisesRegex(install.common.InstallError, 'guard is closed'):
                self.installer.guard()
            with self.assertRaisesRegex(install.common.InstallError, 'still pending'):
                self.installer.enable()
            self.assertEqual(self.installer.pause()['phase'], 'paused')
            with self.assertRaisesRegex(install.common.InstallError, 'already terminal'):
                self.installer.enable()
            systemctl.assert_not_called()
            self.assertEqual(trace, ['under recorder lock', 'pausing', 'under recorder lock', 'record paused',
                                     'timer drained', 'paused'])

    def test_pause_lock_failure_keeps_existing_gate_and_timer(self):
        runtime = SimpleNamespace(pause=Mock(side_effect=TimeoutError('observation lock busy')))
        with patch.dict('sys.modules', {'tools.m0_manager.soak_runtime': runtime}), \
             patch.object(self.installer, '_read', return_value={'phase': 'enabled'}), \
             patch.object(self.installer, '_save') as save, patch.object(self.installer, '_pause_timer') as drain:
            with self.assertRaisesRegex(TimeoutError, 'lock busy'):
                self.installer.pause()
            save.assert_not_called()
            drain.assert_not_called()


if __name__ == '__main__':
    unittest.main()
