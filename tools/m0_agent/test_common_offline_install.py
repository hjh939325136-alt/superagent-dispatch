"""Exercise the offline common seal without creating a model grant or request."""

from contextlib import closing, nullcontext, redirect_stdout
from hashlib import sha256
from io import StringIO
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from dispatch.sa_dispatch.agent_control import BUDGET_ID
from dispatch.sa_dispatch.external_migration import upgrade

from . import common_install as addon, common_runtime as runtime, test_common_recovery
from . import common_recovery as recovery


class CommonOfflineInstallTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.machine = 'a' * 32
        self.manifest = 'b' * 64
        self.installer = addon._Installer(self.root / self.manifest, self.machine, self.manifest)

    def test_offline_units_do_not_change_http_local_or_legacy_bytes(self):
        args = self.installer.release, self.machine, self.manifest
        legacy, offline = addon._unit_bytes(*args), addon._unit_bytes(*args, offline=True)
        agent = addon.SYSTEMD / addon.AGENT
        self.assertEqual([path for path in legacy if legacy[path] != offline[path]], [agent])
        self.assertIn(b'LoadCredential=provider_api_key', legacy[agent][0])
        self.assertNotIn(b'agent-guard', legacy[agent][0])
        self.assertTrue(offline[agent][0].startswith(addon.OFFLINE_MARKER.encode('ascii')))
        self.assertIn(b'agent-guard', offline[agent][0])
        self.assertIn(b'ExecStart=/usr/bin/false', offline[agent][0])
        self.assertNotIn(b'LoadCredential', offline[agent][0])
        self.assertNotIn(b'common_runtime.py', offline[agent][0])

    def test_sealing_retries_after_unit_rewrite_without_touching_legacy_seal(self):
        state = {'phase': 'prepared'}
        units = addon._unit_bytes(self.installer.release, self.machine, self.manifest)
        agent = addon.SYSTEMD / addon.AGENT
        agent_raw = units[agent][0]
        receipt = {'manifest_sha256': 'c' * 64, 'restore_verified': True, 'ledger_version': '0013'}
        state_history = []

        def read_state():
            return dict(state)

        def save(previous, phase, **extra):
            state.update(previous | extra | {'phase': phase})
            state_history.append(phase)
            return dict(state)

        def atomic(path, raw, *, mode):
            nonlocal agent_raw
            self.assertEqual((path, mode), (agent, 0o644))
            agent_raw = raw

        with (
            patch('tools.m0_native_mixed.maintenance._lock', return_value=nullcontext()),
            patch.object(self.installer, '_read_state', side_effect=read_state),
            patch.object(self.installer, '_save', side_effect=save),
            patch.object(self.installer, '_verify_installed'),
            patch.object(addon, '_verify_inactive'),
            patch.object(addon, '_verify_offline') as verify,
            patch.object(addon, '_root_hash', side_effect=lambda p: sha256(agent_raw).hexdigest()),
            patch.object(addon, '_atomic', side_effect=atomic) as writer,
            patch.object(addon, '_systemctl') as command,
            patch.object(addon, '_recover_offline', side_effect=[OSError('interrupted'), receipt]),
        ):
            with self.assertRaisesRegex(OSError, 'interrupted'):
                self.installer.seal_offline()
            self.assertEqual((state['phase'], state['execution_mode']), (addon.OFFLINE_PHASE, 'offline'))
            with self.assertRaisesRegex(addon.InstallError, 'guard is closed'):
                self.installer.guard()
            self.assertEqual(command.call_args_list[0].args, ('daemon-reload',))
            result = self.installer.seal_offline()
            self.assertEqual((result['phase'], result['execution_mode']), ('ready', 'offline'))
            self.assertEqual(state_history, [addon.OFFLINE_PHASE, 'ready'])
            self.assertEqual(command.call_args_list[-1].args, ('start', addon.HTTP))
            writer.assert_called_once()
            self.assertEqual(self.installer.guard(), result)
            calls = command.call_count
            for action in (self.installer.seal, self.installer.agent_guard, self.installer.enable_timer):
                with self.assertRaisesRegex(addon.InstallError, 'offline common installation'):
                    action()
            self.assertEqual(command.call_count, calls)
            self.assertGreater(verify.call_count, 0)

    def test_ready_legacy_cannot_be_converted_to_offline(self):
        with (
            patch('tools.m0_native_mixed.maintenance._lock', return_value=nullcontext()),
            patch.object(self.installer, '_read_state', return_value={'phase': 'ready'}),
            patch.object(addon, '_systemctl') as command,
        ):
            with self.assertRaisesRegex(addon.InstallError, 'ready legacy'):
                self.installer.seal_offline()
            command.assert_not_called()

    def test_no_model_footprint_even_on_wrong_route_and_normal_synthetic_budget(self):
        fixture = test_common_recovery.CommonRecoveryTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        upgrade(fixture.layout.ledger, self.root / 'upgrade', offline=True)
        with patch.object(addon, 'LEDGER', fixture.layout.ledger):
            addon._verify_offline_ledger()
            with closing(sqlite3.connect(fixture.layout.ledger)) as conn, conn:
                conn.execute("INSERT INTO control_budgets VALUES ('synthetic-budget','daily','test',2,0,0,'a','b')")
            addon._verify_offline_ledger()
            for key in (BUDGET_ID, 'agent-campaign:authorization:fixture'):
                with closing(sqlite3.connect(fixture.layout.ledger)) as conn, conn:
                    conn.execute("INSERT INTO control_budgets VALUES (?,'fixture','provider_request',1,0,0,'a','b')", (key,))
                with self.assertRaisesRegex(addon.InstallError, 'external request budget'):
                    addon._verify_offline_ledger()
                with closing(sqlite3.connect(fixture.layout.ledger)) as conn, conn:
                    conn.execute('DELETE FROM control_budgets WHERE budget_id=?', (key,))
            with closing(sqlite3.connect(fixture.layout.ledger)) as conn, conn:
                conn.execute("""INSERT INTO devices(device_id,caps_json,last_seen_at,execution_route)
                    VALUES ('sa-agent-reviewer','[]','a','local_synthetic')""")
            with self.assertRaisesRegex(addon.InstallError, 'external device identity'):
                addon._verify_offline_ledger()

    def test_offline_guard_rejects_model_files_credentials_and_running_timer(self):
        config, store, campaign_cfg, campaign_auth = (
            self.root / name for name in ('config', 'credentials', 'campaign-config', 'campaign-auth'))
        config.mkdir()
        store.mkdir()

        def properties(unit, prop):
            return {'UnitFileState': 'disabled', 'ActiveState': 'inactive', 'LoadState': 'not-found',
                    'DropInPaths': ''}[prop]

        with (
            patch.multiple(addon, CONFIG=config, CREDENTIAL_STORES=(store,),
                           CAMPAIGN_CONFIG=campaign_cfg, CAMPAIGN_AUTHORITY=campaign_auth),
            patch.object(addon, '_verify_offline_ledger') as ledger,
            patch.object(addon, '_property', side_effect=properties) as get,
        ):
            addon._verify_offline()
            ledger.assert_called_once()
            for path in (config / addon.AGENT_CONFIGS[0], config / addon.AGENT_CONFIGS[1],
                         store / 'provider_api_key', campaign_cfg, campaign_auth):
                path.write_bytes(b'never-read')
                with self.assertRaisesRegex(addon.InstallError, 'credential location exists'):
                    addon._verify_offline()
                path.unlink()
            get.side_effect = lambda unit, prop: 'active' if prop == 'ActiveState' else properties(unit, prop)
            with self.assertRaisesRegex(addon.InstallError, 'timer is not closed'):
                addon._verify_offline()
            ledger.assert_called_once()

    def test_offline_recovery_verifies_completed_quarantine_on_retry(self):
        fixture = test_common_recovery.CommonRecoveryTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.grant.unlink()
        fixture.proxy.unlink()
        upgrade(fixture.layout.ledger, self.root / 'upgrade', offline=True)
        authority = fixture.root / 'authority'
        authority.mkdir()

        def layout():
            return fixture.layout

        layout.__dataclass_fields__ = recovery.RecoveryLayout.__dataclass_fields__
        with patch.object(addon, 'AUTHORITY', authority), patch.object(recovery, 'RecoveryLayout', layout):
            first = addon._recover_offline()
            self.assertEqual(addon._recover_offline(), first)
            marker = authority / 'quarantine-pre-offline-0013' / recovery.QUARANTINE_MARKER
            marker.write_bytes(b'{}')
            with self.assertRaisesRegex(addon.InstallError, 'quarantine receipt differs'):
                addon._recover_offline()

    def test_direct_runtime_refuses_offline_register_and_tick_before_config(self):
        authority = self.root / 'authority'
        authority.mkdir()
        (authority / 'state.json').write_bytes(addon._json({'schema': addon.STATE_SCHEMA, 'execution_mode': 'offline'}))
        unit = self.root / addon.AGENT
        unit.write_text(addon.OFFLINE_MARKER + '\n', encoding='ascii')
        with (
            patch.multiple(addon, AUTHORITY=authority, SYSTEMD=self.root),
            patch.object(addon, '_root_metadata'),
            patch.object(addon, '_root_parents'),
            patch.object(runtime.os, 'name', 'posix'),
            patch.object(runtime.os, 'geteuid', create=True) as uid,
            patch.object(runtime, 'read_config') as config,
            patch.object(runtime, '_verify_service') as service,
        ):
            for command, identity in (('register', 0), ('tick', 123)):
                uid.return_value = identity
                with redirect_stdout(StringIO()):
                    self.assertEqual(runtime.main([command]), 1)
            config.assert_not_called()
            service.assert_not_called()


if __name__ == '__main__':
    unittest.main()
