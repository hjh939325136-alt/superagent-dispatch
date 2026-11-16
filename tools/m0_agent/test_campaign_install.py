"""Verify the campaign's systemd boundaries and operator transitions."""

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from . import campaign_install as install, common_package


class CampaignInstallTests(unittest.TestCase):
    def setUp(self):
        self.manifest = 'a' * 64
        self.machine = 'b' * 32
        self.release = install.RELEASES / self.manifest
        self.installer = install.Installer(self.release, self.machine, self.manifest)

    def test_units_have_independent_roles_and_fixed_credential(self):
        units = {path.name: raw.decode('ascii') for path, raw in self.installer.units.items()}
        self.assertEqual(len(units), 4)
        for role, schedule in {'producer': '00,30', 'reviewer': '15,45'}.items():
            account = f'sa-agent-{role}'
            service = units[account + '.service']
            timer = units[account + '.timer']
            self.assertIn(f'User={account}\nGroup=sa-agent-campaign\n', service)
            self.assertIn('SupplementaryGroups=sa-dispatch-ledger\n', service)
            self.assertIn(f'ConditionPathExists={install.CREDENTIAL}', service)
            self.assertIn(f'LoadCredential=provider_api_key:{install.CREDENTIAL}', service)
            self.assertIn(f'flock --shared --nonblock --no-fork {install.old.LEDGER.parent}', service)
            self.assertIn(f'{self.release / install.RUNTIME} tick --role {role}', service)
            self.assertIn(f'guard --expected-machine {self.machine} --manifest-sha256 {self.manifest}', service)
            self.assertIn(f'OnCalendar=*-*-* *:*:{schedule}', timer)
            self.assertIn(f'Unit={account}.service', timer)
            self.assertIn('Persistent=false', timer)
        self.assertNotIn(install.old.TIMER, units)
        self.assertNotIn(install.old.HTTP, units)

    def test_source_allowlist_keeps_legacy_archive_and_rejects_partial_campaign(self):
        common_package._name_set(common_package.LEGACY_REQUIRED)
        common_package._name_set(common_package.REQUIRED)
        with self.assertRaisesRegex(common_package.PackageError, 'complete addon'):
            common_package._name_set(common_package.LEGACY_REQUIRED | {install.ENTRY})

    def test_old_timer_is_stopped_and_disabled(self):
        states = {(install.old.TIMER, 'UnitFileState'): 'disabled',
                  (install.old.TIMER, 'ActiveState'): 'inactive',
                  (install.old.AGENT, 'ActiveState'): 'inactive'}
        with patch.object(install.old, '_systemctl') as ctl, \
             patch.object(install.old, '_property', side_effect=lambda *key: states[key]):
            install._stop_old()
            self.assertEqual([call.args for call in ctl.call_args_list], [
                ('disable', '--now', install.old.TIMER), ('stop', install.old.AGENT),
            ])
            states[(install.old.TIMER, 'UnitFileState')] = 'enabled'
            with self.assertRaisesRegex(install.old.InstallError, 'still enabled'):
                install._verify_old_inactive()

    def test_paused_receipt_closes_startup_guard(self):
        with patch.object(self.installer, '_read', return_value={'phase': 'paused'}), \
             patch.object(self.installer, '_anchored') as anchor:
            with self.assertRaisesRegex(install.old.InstallError, 'guard is closed'):
                self.installer.guard()
            anchor.assert_not_called()

    def test_pause_preserves_campaign_while_stop_permanently_closes_it(self):
        state = {'phase': 'enabled'}
        with patch.object(self.installer, '_read', side_effect=lambda: dict(state)), \
             patch.object(self.installer, '_save',
                          side_effect=lambda _, phase: state.update(phase=phase) or dict(state)), \
             patch.object(self.installer, '_anchored'), \
             patch.object(self.installer, '_pause_units') as pause, \
             patch.object(self.installer, '_operate',
                          side_effect=lambda op: {'status': {'status': 'active', 'stop': 'paused'}[op]}) as operate:
            self.assertEqual(self.installer.pause(), {'phase': 'paused', 'campaign': {'status': 'active'}})
            operate.assert_called_once_with('status')
            self.assertEqual(self.installer.stop(), {'phase': 'stopped', 'campaign': {'status': 'paused'}})
            self.assertEqual(operate.call_args.args, ('stop',))
            self.assertEqual(pause.call_count, 2)
            with self.assertRaisesRegex(install.old.InstallError, 'eligible to resume'):
                self.installer.enable()

    def test_enable_failure_leaves_guard_closed_and_disables_both_timers(self):
        state = {'phase': 'paused'}
        with patch.object(self.installer, '_read', return_value=dict(state)), \
             patch.object(self.installer, '_save',
                          side_effect=lambda _, phase: state.update(phase=phase) or dict(state)), \
             patch.object(self.installer, '_anchored'), patch.object(self.installer, '_check_units'), \
             patch.object(install, '_check_credential_source'), \
             patch.object(self.installer, '_operate', return_value={'status': 'active'}), \
             patch.object(install.old, '_systemctl') as command, \
             patch.object(install.old, '_property', return_value='inactive'), \
             patch.object(self.installer, '_pause_units') as pause:
            with self.assertRaisesRegex(install.old.InstallError, 'did not become active'):
                self.installer.enable()
            self.assertEqual(state['phase'], 'paused')
            command.assert_called_once_with('enable', '--now', *self.installer.timers)
            pause.assert_called_once_with()

    def test_credential_source_absence_is_allowed_but_disk_source_is_refused(self):
        with patch.object(install.os.path, 'lexists', return_value=False), \
             patch.object(install.old, '_run') as fs:
            install._check_credential_source()
            fs.assert_not_called()
        with patch.object(install.os.path, 'lexists', return_value=True), \
             patch.object(install.old, '_root_parents'), patch.object(install.old, '_root_metadata'), \
             patch.object(install.old, '_run', return_value=SimpleNamespace(stdout='ext4\n')):
            with self.assertRaisesRegex(install.old.InstallError, 'private runtime memory'):
                install._check_credential_source()


if __name__ == '__main__':
    unittest.main()
