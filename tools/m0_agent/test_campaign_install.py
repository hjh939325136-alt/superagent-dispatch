"""Verify the campaign's systemd boundaries and operator transitions."""

from types import SimpleNamespace
from contextlib import ExitStack
from datetime import timedelta
import json
from hashlib import sha256
from pathlib import Path
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

    def test_upgrade_drains_and_preserves_expired_campaign_then_replays_inertly(self):
        previous = 'c' * 64
        state = {**self.installer._identity(), 'manifest_sha256': previous, 'phase': 'enabled',
                 'config_sha256': 'd' * 64, 'campaign_id': 'fixture-campaign'}
        receipt = {}
        trace = []

        def save_receipt(data, phase, **fields):
            receipt.update(data | fields | {'phase': phase})
            return dict(receipt)

        def save_state(data, phase):
            state.update(data | self.installer._identity() | {'phase': phase})
            return dict(state)

        def update_gate(path, raw, **_):
            self.assertEqual(path, self.installer.state_file)
            state.update(json.loads(raw))
            trace.append('gate closed')

        def pause():
            self.assertEqual(state['phase'], 'upgrading')
            trace.append('drain')

        def backup(data):
            trace.append('backup')
            return save_receipt(data, 'switching', ledger_sha256='stable', recovery={'restore_verified': True})

        def switch(_):
            self.assertEqual(state['phase'], 'upgrading')
            trace.append('switch')

        def resume():
            self.assertEqual(state['phase'], 'upgrading')
            self.assertEqual(state['manifest_sha256'], self.manifest)
            trace.append('resume')

        current = {'status': 'expired', 'request_units_used': 6, 'request_units_held': 0}
        with ExitStack() as stack:
            for name, callback in {
                '_read_state': lambda: dict(state), '_read_upgrade': lambda _: dict(receipt) or None,
                '_save_upgrade': save_receipt, '_save': save_state, '_anchored': lambda _: None,
                '_pause_units': pause, '_upgrade_backup': backup, '_restore_http': lambda _: None,
                '_switch_units': switch, '_resume_upgraded_timers': resume,
                'status': lambda: dict(state),
            }.items():
                stack.enter_context(patch.object(self.installer, name, side_effect=callback))
            stack.enter_context(patch.object(install, '_verify_release'))
            stack.enter_context(patch.object(install, '_campaign_fingerprint', return_value='stable'))
            stack.enter_context(patch.object(install.old, '_atomic', side_effect=update_gate))
            stack.enter_context(patch.object(install.old, '_run', return_value=SimpleNamespace(stdout='{"ok":true}')))
            stack.enter_context(patch.object(install.old, '_root_hash', return_value='previous-unit'))
            stack.enter_context(patch.object(install.old, '_property', return_value='active'))
            operate = stack.enter_context(patch.object(self.installer, '_operate', return_value=current))
            result = self.installer.upgrade(previous)
            self.assertEqual(result['upgrade']['phase'], 'complete')
            self.assertEqual(result['phase'], 'enabled')
            self.assertEqual(result['campaign'], current)
            self.assertEqual(trace, ['gate closed', 'drain', 'backup', 'switch', 'resume'])
            self.assertEqual(self.installer.upgrade(previous)['upgrade']['phase'], 'complete')
            self.assertEqual(trace, ['gate closed', 'drain', 'backup', 'switch', 'resume'])
            operate.assert_called_once_with('status')

    def test_upgrade_retry_refuses_ledger_drift_while_gate_stays_closed(self):
        previous = 'c' * 64
        state = {**self.installer._identity(), 'manifest_sha256': previous, 'phase': 'upgrading',
                 'config_sha256': 'pinned'}
        receipt = {'phase': 'switching', 'previous_phase': 'paused', 'config_sha256': 'pinned',
                   'ledger_sha256': 'expected'}
        with patch.object(install, '_verify_release'), \
             patch.object(self.installer, '_read_state', return_value=state), \
             patch.object(self.installer, '_read_upgrade', return_value=receipt), \
             patch.object(self.installer, '_anchored'), patch.object(self.installer, '_pause_units') as pause, \
             patch.object(self.installer, '_restore_http'), \
             patch.object(install, '_campaign_fingerprint', return_value='drifted'), \
             patch.object(self.installer, '_switch_units') as switch:
            with self.assertRaisesRegex(install.old.InstallError, 'ledger changed'):
                self.installer.upgrade(previous)
            pause.assert_called_once_with()
            switch.assert_not_called()

    def test_upgrade_switch_resumes_mixed_units_and_rejects_unowned_bytes(self):
        previous = {path.name: 'old-digest' for path in self.installer.units}
        from hashlib import sha256

        paths = tuple(self.installer.units)
        first_digest = sha256(self.installer.units[paths[0]]).hexdigest()
        def installed(path):
            return first_digest if path == paths[0] else 'old-digest'

        with patch.object(install.old, '_root_hash', side_effect=installed), \
             patch.object(install.old, '_atomic') as write, \
             patch.object(install.old, '_systemctl'), patch.object(self.installer, '_check_units'):
            self.installer._switch_units({'previous_units': previous})
            self.assertEqual([call.args[0] for call in write.call_args_list], list(paths[1:]))
        with patch.object(install.old, '_root_hash', return_value='foreign'), \
             patch.object(install.old, '_atomic') as write:
            with self.assertRaisesRegex(install.old.InstallError, 'unit changed'):
                self.installer._switch_units({'previous_units': previous})
            write.assert_not_called()

    def test_rotation_check_reads_new_candidate_against_real_ledger_without_registration(self):
        from sa_dispatch.clock import Clock, timestamp
        from .test_campaign_worker import CampaignWorkerTests

        fixture = CampaignWorkerTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.register()
        fixture.clock.advance(600)
        current = {'campaign': fixture.status(), 'ledger_identity': fixture.config['ledger_identity']}
        future = Clock().now() + timedelta(hours=1)
        candidate = fixture.config | {
            'machine_id': self.machine, 'campaign_id': 'next-campaign', 'authorization_id': 'next-authorization',
            'starts_at': timestamp(future), 'expires_at': timestamp(future + timedelta(seconds=540)),
        }
        before = fixture.f.ledger.read_bytes()
        with patch.object(self.installer, 'status', return_value=current), \
             patch.object(install.old, 'LEDGER', fixture.f.ledger), \
             patch.object(install.old, 'CAS', fixture.f.cas.root), \
             patch('tools.m0_deploy.install_target._read_protected',
                   side_effect=lambda path, limit: json.dumps(candidate).encode()):
            result = self.installer.rotate_check(Path.cwd() / 'fixture-candidate.json')
            self.assertTrue(result['dry_run'])
            self.assertEqual(result['max_requests'], 6)
            self.assertEqual(result['campaign_id'], 'next-campaign')
            candidate['authorization_id'] = fixture.config['authorization_id']
            with self.assertRaisesRegex(install.old.InstallError, 'new identities'):
                self.installer.rotate_check(Path.cwd() / 'fixture-candidate.json')
        self.assertEqual(fixture.f.ledger.read_bytes(), before)

    def test_rotation_recovers_registered_candidate_after_file_drift_and_expiry(self):
        self._assert_rotation_recovers('registration')

    def test_rotation_recovers_after_active_config_replacement(self):
        self._assert_rotation_recovers('active_config')

    def test_rotation_recovers_after_paused_state_before_completion_receipt(self):
        self._assert_rotation_recovers('paused_state')

    def test_rotation_aborts_missing_unregistered_candidate_and_accepts_new_identities(self):
        self._assert_rotation_recovers('before_registration')

    def _assert_rotation_recovers(self, interrupted_at):
        from sa_dispatch.campaign_validation import budget_id
        from sa_dispatch.clock import Clock, timestamp
        from . import campaign_control, campaign_runtime
        from .test_campaign_worker import CampaignWorkerTests

        fixture = CampaignWorkerTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.f.register()
        fixture.f.tick()
        fixture.register()
        for _ in range(3):
            fixture.tick('producer')
            fixture.tick('reviewer')
            fixture.clock.advance(180)
        self.assertEqual(fixture.status()['request_units_used'], 6)

        active = fixture.f.root / 'active-campaign.json'
        active.write_bytes(install.old._json(fixture.config))
        prior_hash = sha256(active.read_bytes()).hexdigest()
        self.installer.state_file = fixture.f.root / 'state.json'
        self.installer.upgrade_file = fixture.f.root / 'upgrade.json'
        self.installer.upgrade_file.write_bytes(b'{"phase":"complete"}')
        self.installer.rotation_file = fixture.f.root / 'rotate.json'
        state = self.installer._identity() | {
            'phase': 'enabled', 'campaign_id': fixture.config['campaign_id'],
            'ledger_identity': fixture.config['ledger_identity'], 'config_sha256': prior_hash,
        }
        self.installer.state_file.write_bytes(install.old._json(state))
        candidate_file = fixture.f.root / 'reviewed-next.json'
        future = Clock().now() + timedelta(minutes=5)
        candidate = fixture.config | {
            'machine_id': self.machine, 'campaign_id': 'new-campaign', 'authorization_id': 'new-authorization',
            'starts_at': timestamp(future), 'expires_at': timestamp(future + timedelta(seconds=540)),
        }
        approved = install.old._json(candidate)
        reviewed = sha256(approved).hexdigest()
        candidate_file.write_bytes(approved)

        def check_anchor(config_state):
            raw = active.read_bytes()
            self.assertEqual(sha256(raw).hexdigest(), config_state['config_sha256'])
            self.assertEqual(json.loads(raw)['campaign_id'], config_state['campaign_id'])

        def use_active(operation):
            self.assertEqual(operation, 'status')
            return campaign_control.status(fixture.f.ledger, fixture.f.cas, json.loads(active.read_bytes()))

        def backup(receipt):
            return self.installer._save_rotation(receipt, 'registering',
                                                 recovery={'restore_verified': True},
                                                 ledger_sha256=original_fingerprint)

        real_candidate_operation = self.installer._candidate_operation
        def interrupt_registration(config, operation):
            if interrupted_at == 'before_registration' and operation == 'register':
                candidate_file.unlink()
                raise KeyboardInterrupt
            outcome = real_candidate_operation(config, operation)
            if operation == 'register':
                candidate_file.unlink()
                raise KeyboardInterrupt
            return outcome

        with ExitStack() as stack:
            patches = (
                (install.old, 'LEDGER', {'new': fixture.f.ledger}),
                (install.old, 'CAS', {'new': fixture.f.cas.root}),
                (install.old, '_root_metadata', {}),
                (install.old, '_root_bytes', {'side_effect': lambda path, **_: path.read_bytes()}),
                (install.old, '_root_hash', {'side_effect': lambda path: sha256(path.read_bytes()).hexdigest()}),
                (install.old, '_atomic', {'side_effect': lambda path, blob, **_: path.write_bytes(blob)}),
                (install.old, '_property', {'return_value': 'inactive'}),
                (campaign_runtime, 'CONFIG', {'new': active}),
                (campaign_runtime, 'verify_execution', {'return_value': 123}),
                (self.installer, '_operate', {'side_effect': use_active}),
                (self.installer, '_anchored', {'side_effect': check_anchor}),
                (self.installer, '_check_units', {}),
                (self.installer, '_pause_units', {}),
                (self.installer, '_rotation_backup', {'side_effect': backup}),
            )
            for owner, name, kwargs in patches:
                stack.enter_context(patch.object(owner, name, **kwargs))
            stack.enter_context(patch('tools.m0_deploy.install_target._read_protected',
                                      side_effect=lambda path, limit: path.read_bytes()))
            original_fingerprint = install._campaign_fingerprint()

            def interrupt_after_write(path, blob, **_):
                path.write_bytes(blob)
                if interrupted_at == 'active_config' and path == active:
                    raise KeyboardInterrupt
                if (interrupted_at == 'paused_state' and path == self.installer.state_file
                        and json.loads(blob)['phase'] == 'paused'):
                    raise KeyboardInterrupt

            interruption = (patch.object(self.installer, '_candidate_operation',
                                         side_effect=interrupt_registration)
                            if interrupted_at in ('registration', 'before_registration') else
                            patch.object(install.old, '_atomic', side_effect=interrupt_after_write))
            with interruption:
                with self.assertRaises(KeyboardInterrupt):
                    self.installer.rotate(candidate_file, reviewed)
            self.assertEqual(self.installer._read_state()['phase'],
                             'paused' if interrupted_at == 'paused_state' else 'rotating')
            self.assertEqual(self.installer._read_rotation()['phase'],
                             'registering' if interrupted_at in ('registration', 'before_registration')
                             else 'switching')
            self.assertEqual(sha256(active.read_bytes()).hexdigest(),
                             prior_hash if interrupted_at in ('registration', 'before_registration') else reviewed)
            with patch.object(install.old, '_systemctl') as systemctl:
                for operation in (self.installer.guard, self.installer.enable):
                    with self.assertRaisesRegex(install.old.InstallError, 'invalid campaign install phase|guard is closed'):
                        operation()
                systemctl.assert_not_called()
            if interrupted_at == 'before_registration':
                self._assert_abort_and_next_rotation(fixture, active, prior_hash, candidate_file, candidate,
                                                     reviewed, original_fingerprint)
                return
            abandoned_ledger = fixture.f.ledger.read_bytes()
            with self.assertRaisesRegex(install.old.InstallError, 'registered ledger records'):
                self.installer.rotate_abort(candidate_file, reviewed)
            self.assertEqual(fixture.f.ledger.read_bytes(), abandoned_ledger)
            if interrupted_at == 'registration':
                with self.assertRaises(FileNotFoundError):
                    self.installer.rotate(candidate_file, reviewed)
                candidate_file.write_bytes(approved + b'\n')
                with self.assertRaisesRegex(install.old.InstallError, 'SHA256 differs'):
                    self.installer.rotate(candidate_file, reviewed)
                other = candidate_file.with_name('different-path.json')
                other.write_bytes(approved)
                with self.assertRaisesRegex(install.old.InstallError, 'original candidate path'):
                    self.installer.rotate(other, reviewed)
                candidate_file.write_bytes(approved)
            registered_before_resume = fixture.f.ledger.read_bytes()
            past_window = SimpleNamespace(now=lambda: future + timedelta(days=1))
            with patch.object(campaign_control, 'Clock', return_value=past_window), \
                 patch.object(self.installer, '_candidate_operation', wraps=real_candidate_operation) as operation:
                result = self.installer.rotate(candidate_file, reviewed)
                self.assertEqual(result['phase'], 'paused')
                self.assertEqual(result['campaign']['status'], 'expired')
                self.assertEqual(result['campaign']['request_units_used'], 0)
                self.assertEqual(result['rotation']['phase'], 'complete')
                if interrupted_at != 'registration':
                    self.assertTrue(operation.call_args_list)
                    self.assertTrue(all(call.args[1] == 'status' for call in operation.call_args_list))
                self.assertEqual(fixture.f.ledger.read_bytes(), registered_before_resume)
                before_repeat = fixture.f.ledger.read_bytes()
                self.assertEqual(self.installer.rotate(candidate_file, reviewed), result)
                self.assertEqual(fixture.f.ledger.read_bytes(), before_repeat)
            self.assertEqual(active.read_bytes(), approved)
            self.assertEqual(install._campaign_fingerprint(exclude_registration=candidate), original_fingerprint)
            budgets = {row['budget_id']: row['used_units'] for row in fixture.records()['control_budgets']}
            self.assertEqual(budgets, {'agent-external:provider-request': 1,
                                       budget_id(fixture.config): 6, budget_id(candidate): 0})

    def _assert_abort_and_next_rotation(self, fixture, active, prior_hash, candidate_path, candidate, reviewed,
                                        previous_fingerprint):
        from sa_dispatch.campaign_validation import budget_id

        previous = fixture.f.ledger.read_bytes()
        self.assertFalse(candidate_path.exists())
        save_rotation = self.installer._save_rotation

        def interrupt_completion(receipt, phase, **fields):
            if phase == 'aborted':
                raise KeyboardInterrupt
            return save_rotation(receipt, phase, **fields)

        with patch.object(self.installer, '_save_rotation', side_effect=interrupt_completion):
            with self.assertRaises(KeyboardInterrupt):
                self.installer.rotate_abort(candidate_path, reviewed)
        self.assertEqual(self.installer._read_state()['phase'], 'paused')
        self.assertEqual(self.installer._read_rotation()['phase'], 'aborting')
        with self.assertRaisesRegex(install.old.InstallError, 'guard is closed'):
            self.installer.enable()
        result = self.installer.rotate_abort(candidate_path, reviewed)
        self.assertEqual(result['rotation']['phase'], 'aborted')
        self.assertEqual(result['phase'], 'paused')
        self.assertEqual(result['campaign_id'], fixture.config['campaign_id'])
        self.assertEqual(sha256(active.read_bytes()).hexdigest(), prior_hash)
        self.assertEqual(fixture.f.ledger.read_bytes(), previous)
        with patch.object(install.old, '_atomic') as write:
            self.assertEqual(self.installer.rotate_abort(candidate_path, reviewed), result)
            write.assert_not_called()
        with self.assertRaisesRegex(install.old.InstallError, 'candidate was cancelled'):
            self.installer.rotate(candidate_path, reviewed)

        next_path = candidate_path.with_name('different-candidate.json')
        changed_bytes = install.old._json(candidate | {'authorization_id': 'different-authorization'})
        next_path.write_bytes(changed_bytes)
        with self.assertRaisesRegex(install.old.InstallError, 'reuses an aborted'):
            self.installer.rotate(next_path, sha256(changed_bytes).hexdigest())
        fresh = candidate | {'campaign_id': 'different-campaign', 'authorization_id': 'different-authorization'}
        fresh_bytes = install.old._json(fresh)
        next_path.write_bytes(fresh_bytes)
        replacement = self.installer.rotate(next_path, sha256(fresh_bytes).hexdigest())
        self.assertEqual(replacement['campaign_id'], fresh['campaign_id'])
        self.assertEqual(replacement['phase'], 'paused')
        self.assertEqual(install._campaign_fingerprint(exclude_registration=fresh), previous_fingerprint)
        budgets = {row['budget_id']: row['used_units'] for row in fixture.records()['control_budgets']}
        self.assertEqual(budgets, {'agent-external:provider-request': 1,
                                   budget_id(fixture.config): 6, budget_id(fresh): 0})


if __name__ == '__main__':
    unittest.main()
