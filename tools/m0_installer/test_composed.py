"""Composed offline install phase replay and fresh-ledger authority boundaries."""

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from . import composed


class ComposedInstallTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory())).resolve()
        self.identity = dict(machine_id='1' * 32, hostname='fixture-offline',
                             baseline_manifest_sha256='a' * 64, baseline_zip_sha256='b' * 64,
                             addon_manifest_sha256='c' * 64, addon_zip_sha256='d' * 64)
        self.installer = composed.Installer(self.identity, self.root / 'baseline.zip', self.root / 'addon.zip')
        self.installer.state_file = self.root / 'state.json'
        self.calls = []
        self.version = '0012'
        self.common_mode = 'offline'
        self.collector_state = 'absent'
        self.observer_state = 'absent'

        self.enterContext(patch.object(composed, '_model_preflight'))
        self.enterContext(patch.object(composed, '_verify_no_models'))
        self.enterContext(patch.object(self.installer, '_stage'))
        self.enterContext(patch.object(self.installer, '_run_baseline', side_effect=self._baseline))
        self.enterContext(patch.object(self.installer, '_addon', side_effect=self._addon))
        self.enterContext(patch.object(composed, 'AUTHORITY', self.root))
        self.enterContext(patch.object(composed.common_install, 'AUTHORITY', self.root / 'common-unowned'))
        self.enterContext(patch.object(composed.common_install, 'LEDGER', self.root / 'fixture.sqlite3'))
        self.enterContext(patch.object(composed.collector_install, 'AUTHORITY', self.root / 'collector-unowned'))
        self.enterContext(patch.object(composed.soak_install, 'AUTHORITY', self.root / 'observer-unowned'))
        self.enterContext(patch.object(composed.common_install, '_root_metadata'))
        self.enterContext(patch.object(composed.common_install, '_root_bytes',
                                       side_effect=lambda path, **_: path.read_bytes()))
        self.enterContext(patch.object(composed.common_install, '_atomic',
                                       side_effect=lambda path, raw, **_: path.write_bytes(raw)))
        self.enterContext(patch.object(composed.common_install, '_check_schema',
                                       side_effect=lambda: (self.version, '1:2')))
        self.enterContext(patch.object(composed.common_install, '_property', return_value='active'))

    def _baseline(self):
        self.calls.append(('baseline', 'install'))
        return self.identity | {'phase': 'completed', 'runtime_healthy': True,
                                'manifest_sha256': self.identity['baseline_manifest_sha256'],
                                'zip_sha256': self.identity['baseline_zip_sha256'],
                                'fresh_install_sha256': 'e' * 64}

    def _addon(self, _release, entry, action):
        self.calls.append((entry, action))
        result = {'manifest_sha256': self.identity['addon_manifest_sha256']}
        if entry == composed.common_install.ENTRY:
            self.version = '0013'
            return result | {'phase': 'prepared' if action == 'install' else 'ready',
                             'machine_id': self.identity['machine_id'], 'ledger_identity': '1:2',
                             'execution_mode': self.common_mode}
        if entry == composed.collector_install.ENTRY:
            if action in ('install', 'enable'):
                self.collector_state = 'enabled' if action == 'enable' else 'paused'
            return result | {'phase': self.collector_state,
                             'timer': 'active' if self.collector_state == 'enabled' else 'inactive'}
        if entry == composed.soak_install.ENTRY:
            if action == 'install':
                self.observer_state = 'paused'
            return result | {'phase': self.observer_state, 'timer': 'inactive',
                             'observation': {'state': 'not_started'}}
        raise AssertionError((entry, action))

    def test_fixed_offline_order_and_repeat_only_read_components(self):
        first = self.installer.install()
        self.assertEqual(first['ledger_mode'], 'mixed')
        self.assertEqual(first['collector'], {'phase': 'enabled', 'timer': 'active'})
        self.assertEqual(first['observer'], {'phase': 'paused', 'timer': 'inactive', 'observation': 'not_started'})
        self.assertEqual(first['campaign'], 'unconfigured')
        self.assertEqual([call for call in self.calls if call[1] != 'status'], [
            ('baseline', 'install'),
            (composed.common_install.ENTRY, 'install'),
            (composed.common_install.ENTRY, 'seal-offline'),
            (composed.collector_install.ENTRY, 'install'),
            (composed.collector_install.ENTRY, 'enable'),
            (composed.soak_install.ENTRY, 'install'),
        ])
        checkpoint = self.installer.state_file.read_bytes()
        self.calls.clear()
        self.assertEqual(self.installer.install(), first)
        self.assertEqual([action for _, action in self.calls], ['status', 'status', 'status'])
        self.assertEqual(self.installer.state_file.read_bytes(), checkpoint)

    def test_each_component_commit_before_receipt_reuses_only_uncheckpointed_components(self):
        save = self.installer._save
        actions = [
            ('baseline', 'install'),
            (composed.common_install.ENTRY, 'install'),
            (composed.common_install.ENTRY, 'seal-offline'),
            (composed.collector_install.ENTRY, 'install'),
            (composed.collector_install.ENTRY, 'enable'),
            (composed.soak_install.ENTRY, 'install'),
        ]
        for index, target in enumerate(composed.PHASES[1:]):
            with self.subTest(checkpoint=target):
                self.installer.state_file.unlink(missing_ok=True)
                self.calls.clear()
                self.version = '0012'
                self.collector_state = self.observer_state = 'absent'

                def interruption(state, phase, **fields):
                    if phase == target:
                        raise KeyboardInterrupt
                    return save(state, phase, **fields)

                with patch.object(self.installer, '_save', side_effect=interruption):
                    with self.assertRaises(KeyboardInterrupt):
                        self.installer.install()
                self.assertEqual(self.installer.status()['phase'], composed.PHASES[index])
                self.calls.clear()
                self.assertEqual(self.installer.install()['phase'], 'completed')
                self.assertEqual([call for call in self.calls if call[1] != 'status'], actions[index:])

    def test_resume_refuses_changed_common_gate_before_installing_dependents(self):
        state = self.installer._save({}, 'common_ready', ledger_identity='1:2')
        self.version = '0013'
        self.common_mode = 'legacy_grant'
        with self.assertRaisesRegex(composed.common_install.InstallError, 'not sealed for offline'):
            self.installer.install()
        self.assertEqual(self.installer._read(), state)
        self.assertEqual(self.calls, [(composed.common_install.ENTRY, 'status')])

    def test_failure_keeps_last_verified_phase_and_status_is_read_only(self):
        self.common_mode = 'legacy_grant'
        with self.assertRaisesRegex(composed.common_install.InstallError, 'not sealed for offline'):
            self.installer.install()
        checkpoint = self.installer.state_file.read_bytes()
        self.calls.clear()
        self.assertEqual(self.installer.status()['phase'], 'common_prepared')
        self.assertFalse(self.calls)
        self.assertEqual(self.installer.state_file.read_bytes(), checkpoint)

    def test_refuses_different_package_identity_without_mutations(self):
        self.installer._save({}, 'intent')
        checkpoint = self.installer.state_file.read_bytes()
        self.installer.identity = self.identity | {'addon_zip_sha256': 'f' * 64}
        with self.assertRaisesRegex(composed.common_install.InstallError, 'different package or machine'):
            self.installer.install()
        self.assertFalse(self.calls)
        self.assertEqual(self.installer.state_file.read_bytes(), checkpoint)

    def test_child_failure_does_not_expose_raw_stdout_or_stderr(self):
        output = SimpleNamespace(returncode=1, stdout=b'unexpected-private-credential-text',
                                 stderr=b'unexpected-private-source-path')
        with patch.object(composed.subprocess, 'run', return_value=output):
            with self.assertRaisesRegex(composed.common_install.InstallError, 'inspect its protected receipt') as error:
                composed._command(self.root / 'fixed.py', 'install', ())
            self.assertNotIn('unexpected-private', str(error.exception))


class FreshLedgerTests(unittest.TestCase):
    def test_readonly_active_grant_detection_uses_real_0013_ledger(self):
        from tools.m0_agent.test_common_worker import CommonWorkerTests

        fixture = CommonWorkerTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.upgrade()
        with patch.object(composed, '_model_preflight'), \
             patch.object(composed.common_install, 'LEDGER', fixture.ledger), \
             patch.object(composed.common_install, '_property',
                          side_effect=lambda _, prop: 'disabled' if prop == 'UnitFileState' else 'inactive'):
            before = fixture.ledger.read_bytes()
            composed._verify_no_models()
            self.assertEqual(fixture.ledger.read_bytes(), before)
            fixture.register()
            registered = fixture.ledger.read_bytes()
            with self.assertRaisesRegex(composed.common_install.InstallError, 'model request budget|model execution'):
                composed._verify_no_models()
            self.assertEqual(fixture.ledger.read_bytes(), registered)


if __name__ == '__main__':
    unittest.main()
