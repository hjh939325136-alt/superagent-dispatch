"""Missing roles require proven offline installation, not a missing-file guess."""
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

from tools.m0_agent import common_install
from . import operational_sources as ops


class OfflineOperationsTests(unittest.TestCase):
    def unit_output(self, changed=None):
        units = {}
        for unit in ops.UNITS:
            units[unit] = dict(ops.LOCAL_ABSENT) if unit in ops.CAMPAIGN_UNITS else {
                'LoadState': 'loaded', 'ActiveState': 'active',
                'SubState': 'running', 'UnitFileState': 'enabled'}
        if changed:
            unit, values = changed
            units[unit].update(values)
        raw = '\n\n'.join('Id=' + unit + '\n' + '\n'.join(f'{k}={v}' for k, v in data.items())
                            for unit, data in units.items()).encode('ascii')
        return subprocess.CompletedProcess([], 0, raw)

    def test_only_verified_offline_allows_missing_roles(self):
        with patch.object(ops.subprocess, 'run', return_value=self.unit_output()):
            normal = ops._systemd()
            self.assertEqual(normal['state'], 'partial')
            self.assertTrue(all(normal['units'][unit] == {'state': 'unknown'} for unit in ops.CAMPAIGN_UNITS))
            offline = ops._systemd(offline=True)
            self.assertEqual(offline['state'], 'observed')
            self.assertTrue(all(offline['units'][unit]['availability'] == 'unconfigured_offline'
                                for unit in ops.CAMPAIGN_UNITS))

    def test_offline_does_not_hide_masked_loaded_or_active_roles(self):
        unit = 'sa-agent-producer.timer'
        for change in ({'LoadState': 'masked'}, {'LoadState': 'loaded'}, {'ActiveState': 'active'}):
            with self.subTest(change=change), patch.object(ops.subprocess, 'run',
                                                         return_value=self.unit_output((unit, change))):
                self.assertEqual(ops._systemd(offline=True)['units'][unit], {'state': 'unknown'})

    def test_offline_does_not_allow_missing_coordinator(self):
        with patch.object(ops.subprocess, 'run',
                          return_value=self.unit_output((ops.COORDINATOR, ops.LOCAL_ABSENT))):
            self.assertEqual(ops._systemd(offline=True)['units'][ops.COORDINATOR], {'state': 'unknown'})

    def test_legacy_receipt_cannot_attest_offline(self):
        with patch.object(ops, '_receipt', return_value={'phase': 'ready'}), \
             patch.object(common_install, '_source_files') as verify:
            self.assertFalse(ops._offline_mode('a' * 32, '1:2'))
            verify.assert_not_called()

    def test_offline_requires_source_guard_and_matching_ledger(self):
        manifest = 'b' * 64
        record = {'execution_mode': 'offline', 'manifest_sha256': manifest}
        with patch.object(ops, '_receipt', return_value=record), \
             patch.object(ops.os.path, 'lexists', return_value=False), \
             patch.object(common_install, '_source_files') as verify, \
             patch.object(common_install, '_Installer') as installer:
            installer.return_value.guard.return_value = dict(record, ledger_identity='1:2')
            self.assertTrue(ops._offline_mode('a' * 32, '1:2'))
            verify.assert_called_once_with(common_install.RELEASES / manifest, manifest)
            installer.assert_called_once_with(common_install.RELEASES / manifest, 'a' * 32, manifest)
            installer.return_value.guard.return_value['ledger_identity'] = '1:3'
            with self.assertRaises(ValueError):
                ops._offline_mode('a' * 32, '1:2')
            installer.return_value.guard.side_effect = common_install.InstallError('offline gate changed')
            with self.assertRaises(ValueError):
                ops._offline_mode('a' * 32, '1:2')

    def test_partial_campaign_authority_cannot_appear_as_idle(self):
        with patch.object(ops, '_receipt', return_value={'execution_mode': 'offline', 'manifest_sha256': 'b' * 64}), \
             patch.object(ops.os.path, 'lexists', return_value=True), \
             patch.object(common_install, '_source_files') as verify:
            with self.assertRaises(ValueError):
                ops._offline_mode('a' * 32, '1:2')
            verify.assert_not_called()


if __name__ == '__main__':
    unittest.main()
