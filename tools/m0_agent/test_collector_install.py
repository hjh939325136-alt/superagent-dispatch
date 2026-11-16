"""Collector packaging, fixed systemd unit boundaries, and guarded operator transitions."""

from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from . import collector_install as install, common_package


class CollectorInstallTests(unittest.TestCase):
    def setUp(self):
        self.manifest = 'a' * 64
        self.machine = 'b' * 32
        self.release = install.RELEASES / self.manifest
        self.installer = install.Installer(self.release, self.machine, self.manifest)

    def test_units_confine_a_single_credential_free_tick(self):
        service, timer = [raw.decode('ascii') for raw in self.installer.units.values()]
        for line in ('Type=oneshot', 'User=root', f'SupplementaryGroups={install.common.SHARED}',
                     'CapabilityBoundingSet=', 'PrivateNetwork=yes', 'RestrictAddressFamilies=AF_UNIX',
                     'ProtectSystem=strict', f'ReadWritePaths={install.common.LEDGER.parent} {install.AUTHORITY}',
                     'InaccessiblePaths=-/run/credstore', 'TimeoutStartSec=45s', 'MemoryMax=128M',
                     'LogRateLimitIntervalSec=60s', 'LogRateLimitBurst=10'):
            self.assertIn(line + '\n', service)
        self.assertIn(f'ExecStart=/usr/bin/python3 -I -B {self.release / install.RUNTIME} tick\n', service)
        for unexpected in ('LoadCredential', 'ExecStartPre', '/usr/bin/flock', 'Environment=', 'network-online'):
            self.assertNotIn(unexpected, service)
        for line in ('OnBootSec=30s', 'OnUnitInactiveSec=60s', 'Persistent=false', f'Unit={install.SERVICE}'):
            self.assertIn(line + '\n', timer)

    def test_manifest_keeps_legacy_campaign_and_full_collector_variants(self):
        legacy, campaign, collector = (common_package.LEGACY_REQUIRED, common_package.CAMPAIGN_FILES,
                                       common_package.COLLECTOR_FILES)
        for names in (legacy, legacy | campaign, legacy | campaign | collector):
            common_package._name_set(names)
        for missing in campaign | collector:
            with self.subTest(missing=missing), self.assertRaisesRegex(common_package.PackageError, 'complete'):
                common_package._name_set((legacy | campaign | collector) - {missing})
        with self.assertRaisesRegex(common_package.PackageError, 'complete Collector and campaign'):
            common_package._name_set(legacy | collector)

    def test_guard_refuses_installing_paused_and_upgrading_before_pin_or_units(self):
        with patch.object(self.installer, '_read') as read, patch.object(self.installer, '_pin') as pin, \
             patch.object(self.installer, '_check_units') as units:
            for phase in ('installing', 'paused', 'upgrading'):
                read.return_value = {'phase': phase}
                with self.subTest(phase=phase), self.assertRaisesRegex(install.common.InstallError, 'guard is closed'):
                    self.installer.guard()
            pin.assert_not_called()
            units.assert_not_called()
            read.return_value = {'phase': 'enabled'}
            self.assertEqual(self.installer.guard()['phase'], 'enabled')
            pin.assert_called_once()
            units.assert_called_once()

    def test_first_install_stores_pin_and_finishes_paused_without_sync(self):
        root = Path(self.enterContext(TemporaryDirectory())).resolve()
        self.installer.state_file = root / 'state.json'
        pin_path = root / 'collector-runtime.json'
        pin = dict(machine_id=self.machine, ledger_identity='1:2',
                   binding_raw_sha256='c' * 64, binding_sha256='d' * 64,
                   max_ledger_bytes=32 * 1024**2, min_free_bytes=256 * 1024**2, stale_after_seconds=180)
        writes = []

        def atomic(path, raw, *, mode):
            writes.append((path, mode))
            path.write_bytes(raw)

        with patch.dict('sys.modules', {'tools.m0_agent.collector_runtime':
                        SimpleNamespace(PIN=pin_path, build_pin=lambda **_: pin)}), \
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
            self.assertEqual(writes, [(pin_path, 0o600), (self.installer.state_file, 0o600),
                                      (self.installer.state_file, 0o600)])
            self.installer.install()
            self.assertEqual(len(writes), 3)
            publish.assert_called_once_with()
            pause.assert_called_once_with()

    def test_enable_failure_keeps_gate_closed_and_disables_timer(self):
        with patch.object(self.installer, '_read', return_value={'phase': 'paused'}), \
             patch.object(self.installer, '_pin'), patch.object(self.installer, '_verify_live_pin'), \
             patch.object(self.installer, '_check_units'), \
             patch.object(self.installer, '_save') as save, patch.object(self.installer, '_pause_timer') as pause, \
             patch.object(install.common, '_systemctl') as systemctl, \
             patch.object(install.common, '_property', return_value='inactive'):
            with self.assertRaisesRegex(install.common.InstallError, 'did not become active'):
                self.installer.enable()
            systemctl.assert_called_once_with('enable', '--now', install.TIMER)
            save.assert_called_once_with({'phase': 'paused'}, 'paused')
            pause.assert_called_once_with()

    def test_enable_refuses_live_binding_drift_before_timer_mutation(self):
        pin = dict(max_ledger_bytes=32 * 1024**2, min_free_bytes=256 * 1024**2, stale_after_seconds=180,
                   binding_sha256='a' * 64)
        with patch.dict('sys.modules', {'tools.m0_agent.collector_runtime':
                        SimpleNamespace(build_pin=lambda **_: pin | {'binding_sha256': 'b' * 64})}), \
             patch.object(self.installer, '_read', return_value={'phase': 'paused'}), \
             patch.object(self.installer, '_pin', return_value=pin), \
             patch.object(install.common, '_systemctl') as systemctl:
            with self.assertRaisesRegex(install.common.InstallError, 'live Collector binding changed'):
                self.installer.enable()
            systemctl.assert_not_called()

    def test_upgrade_retries_after_interruption_with_same_pin_and_paused_final_gate(self):
        previous = 'c' * 64
        state = {'phase': 'enabled', 'manifest_sha256': previous, 'pin_sha256': 'pin'}
        trace = []

        def close_gate(path, raw, **_):
            self.assertEqual(path, self.installer.state_file)
            state.update(json.loads(raw))
            trace.append('closed')

        def save(data, phase):
            self.assertNotIn('pending_manifest_sha256', data)
            self.assertNotIn('previous_units', data)
            state.clear()
            state.update(data | {'phase': phase, 'manifest_sha256': self.manifest})
            trace.append('committed')
            return state

        with patch.object(install, '_verify_release'), patch.object(self.installer, '_pin'), \
             patch.object(self.installer, '_verify_live_pin'), \
             patch.object(self.installer, '_state', side_effect=lambda: dict(state)), \
             patch.object(self.installer, 'status', side_effect=lambda: dict(state)), \
             patch.object(self.installer, '_save', side_effect=save), \
             patch.object(self.installer, '_pause_timer', side_effect=lambda: trace.append('paused')), \
             patch.object(self.installer, '_publish_units', side_effect=[KeyboardInterrupt, None]) as publish, \
             patch.object(install.common, '_atomic', side_effect=close_gate), \
             patch.object(install.common, '_root_hash', return_value='original'), \
             patch.object(install.common, '_run', return_value=SimpleNamespace(stdout='{"ok": true}')) as probe:
            with self.assertRaises(KeyboardInterrupt):
                self.installer.upgrade(previous)
            self.assertEqual(state['phase'], 'upgrading')
            with self.assertRaisesRegex(install.common.InstallError, 'previous Collector manifest is not installed'):
                self.installer.upgrade('d' * 64)
            done = self.installer.upgrade(previous)
            self.assertEqual(done['phase'], 'paused')
            self.assertEqual(done['pin_sha256'], 'pin')
            self.assertEqual(done['previous_manifest_sha256'], previous)
            self.assertEqual(self.installer.upgrade(previous), done)
            self.assertEqual(trace, ['closed', 'paused', 'paused', 'committed'])
            self.assertEqual(publish.call_count, 2)
            probe.assert_called_once()


if __name__ == '__main__':
    unittest.main()
