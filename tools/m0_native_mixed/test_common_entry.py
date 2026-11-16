"""Exercise the frozen common entry on real temporary native state."""

from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from io import StringIO
import json
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dispatch.sa_dispatch.bundle import ledger
from tools.m0_deploy.install_target import TARGET_SCHEMA, TARGET_SCOPE
from tools.m0_installer.common_baseline import initialize
from tools.m0_native_mixed.bootstrap import Bootstrap, MARKER
from tools.m0_native_mixed import common_entry
from tools.m0_native_mixed.domains import ROOT
from tools.m0_native_mixed.maintenance import MaintenanceError, canonical, digest
from tools.m0_native_mixed.native_adapter import NativeLayout, source_files
from tools.m0_native_mixed.test_bootstrap import FakeSystemdBootstrap
from tools.m0_native_mixed.test_native_adapter import FakeSystemd


MANIFEST = 'a' * 64
OPERATION = 'common-' + MANIFEST[:24]


class CommonEntryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='common-native-entry-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        source_sha = digest(canonical(source_files(NativeLayout(ROOT))))
        release = self.root / 'opt/sa-dispatch-native/releases' / source_sha
        self.layout = layout = NativeLayout.fixture(self.root, release)
        layout.assert_fixture_contained(self.root)
        self.assertTrue(release.is_relative_to(self.root))
        for path in (layout.config_root, layout.ledger.parent, layout.legacy_bindings, layout.artifacts,
                     layout.http_journal, layout.maintenance_lock.parent, layout.domain.parent,
                     *(path.parent for path in layout.service_files)):
            path.mkdir(parents=True, exist_ok=True)
        layout.maintenance_lock.write_bytes(b'0')
        target = dict(schema=TARGET_SCHEMA, scope=TARGET_SCOPE, manifest_sha256=MANIFEST,
                      machine_id='b' * 32, hostname='synthetic-native-entry')
        (layout.config_root / 'install-target.json').write_bytes(canonical(target))
        self.service = FakeSystemd()
        self.service.running = False
        installed = initialize(layout, deployment_manifest=MANIFEST, fixture_root=self.root, service=self.service)
        coordinator = dict(db_path=str(layout.ledger), artifact_root=str(layout.artifacts),
            journal_dir=str(layout.http_journal), device_id='coordinator', capabilities=[], plugins=['hotspot'],
            synthetic=True, lease_ttl_seconds=30, component_timeout_seconds=20)
        self.before = ('\n'.join(f'{key} = {json.dumps(value)}' for key, value in coordinator.items())
                       + '\n[components]\n').encode()
        (layout.config_root / 'coordinator.toml').write_bytes(self.before)
        layout.credentials.write_bytes(b'opaque identity fixture')
        for path in layout.service_files[:-1]:
            path.write_bytes(b'[Service]\nUser=sa-dispatch\n')
        platform = FakeSystemdBootstrap(layout)
        platform.current = dict(ActiveState='inactive', MainPID='0', NRestarts='0')
        Bootstrap(ROOT, expected_source_sha256=source_sha, coordinator_before_sha256=digest(self.before),
                  layout=layout, platform=platform, fixture=True).run()
        self.authority = dict(deployment_manifest=MANIFEST, fresh_install_sha256=installed['receipt_sha256'],
                              coordinator_before_sha256=digest(self.before), operation_id=OPERATION)

    def entry(self, **changes):
        return common_entry.fixture_entry(self.layout, fixture_root=self.root, service=self.service,
                                         **{**self.authority, **changes})

    def test_plan_retry_status_and_activation_use_fixed_files(self):
        entry = self.entry()
        baseline = ledger(self.layout.ledger, include_devices=True)
        staged = entry.plan()
        plan_sha = staged['plan_sha256']
        self.assertEqual(staged['phase'], 'staged')
        state_path = entry.coordinator.operation / 'state.json'
        original_mtime = state_path.stat().st_mtime_ns
        self.assertEqual(self.entry().plan(), staged)
        self.assertEqual(state_path.stat().st_mtime_ns, original_mtime)
        self.assertEqual(ledger(self.layout.ledger, include_devices=True), baseline)
        self.assertEqual((self.layout.config_root / 'coordinator.toml').read_bytes(), self.before)
        self.assertFalse(self.service.running)
        status = self.entry().status(plan_sha)
        self.assertEqual((status['phase'], status['ledger_mode'], status['collector_gap_count']),
                         ('staged', 'isolated', 0))
        self.assertEqual(set(status), {'phase', 'ledger_mode', 'plan_sha256', 'collector_cursors', 'collector_gap_count'})
        self.assertEqual(state_path.stat().st_mtime_ns, original_mtime)
        self.assertEqual(self.service.starts, 0)

        complete = self.entry().run(plan_sha)
        self.assertEqual(complete, {'phase': 'completed', 'plan_sha256': plan_sha})
        completed_status = self.entry().status(plan_sha)
        self.assertEqual(completed_status['ledger_mode'], 'mixed')
        self.assertIs(completed_status['runtime_healthy'], True)
        final = ledger(self.layout.ledger, include_devices=True)
        self.assertEqual(final['collector_cursors'], baseline['collector_cursors'])
        self.assertEqual(final['collector_gaps'], [])
        completion_mtime = state_path.stat().st_mtime_ns
        self.assertEqual(self.entry().run(plan_sha), complete)
        self.assertEqual(ledger(self.layout.ledger, include_devices=True), final)
        self.assertEqual((self.service.starts, state_path.stat().st_mtime_ns), (1, completion_mtime))
        with self.assertRaises(MaintenanceError):
            self.entry().plan()

        self.service.stop()
        with self.assertRaisesRegex(MaintenanceError, 'service stopped'):
            self.entry().status(plan_sha)
        self.assertFalse(self.service.running)
        self.service.running = True
        self.service.guards = False
        with self.assertRaisesRegex(MaintenanceError, 'guard not installed'):
            self.entry().status(plan_sha)
        self.service.guards = True
        with patch.object(self.service, 'health', side_effect=MaintenanceError('runtime health probe failed')):
            with self.assertRaisesRegex(MaintenanceError, 'runtime health probe failed'):
                self.entry().status(plan_sha)
        self.assertEqual((self.service.starts, state_path.stat().st_mtime_ns), (1, completion_mtime))

    def test_incomplete_operation_and_wrong_authority_refuse_without_replanning(self):
        with self.assertRaisesRegex(MaintenanceError, 'operation ID'):
            self.entry(operation_id='unrelated')
        with self.assertRaisesRegex(MaintenanceError, 'bootstrap provenance'):
            self.entry(coordinator_before_sha256='b' * 64)
        with self.assertRaisesRegex(MaintenanceError, 'proof hash differs'):
            self.entry(fresh_install_sha256='b' * 64).plan()
        entry = self.entry()
        entry.coordinator.operation.mkdir(parents=True)
        with self.assertRaisesRegex(MaintenanceError, 'required regular file missing'):
            entry.plan()
        self.assertEqual(list(entry.coordinator.operation.iterdir()), [])
        self.assertEqual(self.service.starts, 0)

    def test_release_marker_and_fixture_path_drift_refused(self):
        with self.assertRaisesRegex(MaintenanceError, 'bounded temporary paths'):
            common_entry.fixture_entry(replace(self.layout, ledger=self.root.parent / 'escape.sqlite3'),
                fixture_root=self.root, service=self.service, **self.authority)
        (self.layout.source_root / MARKER).write_bytes(b'{}')
        with self.assertRaisesRegex(MaintenanceError, 'unexpected or changed bytes'):
            self.entry()


class CommonEntryArgumentTests(unittest.TestCase):
    def test_cli_contract_has_no_path_or_unreviewed_run(self):
        common = ['--deployment-manifest', MANIFEST, '--fresh-install-sha256', 'b' * 64,
                  '--coordinator-before-sha256', 'c' * 64, '--operation-id', OPERATION]
        entry = SimpleNamespace(plan=lambda: {'plan_sha256': 'd' * 64, 'phase': 'staged'})
        with patch.object(common_entry, '_production_entry', return_value=entry) as open_entry, \
                redirect_stdout(StringIO()) as output, redirect_stderr(StringIO()):
            self.assertEqual(common_entry.main(['plan', *common]), 0)
            self.assertEqual(json.loads(output.getvalue())['plan_sha256'], 'd' * 64)
            open_entry.assert_called_once()
            for command in (['run', *common], ['status', *common], ['plan', *common, '--ledger', '/tmp/other']):
                with self.subTest(command=command), self.assertRaises(SystemExit):
                    common_entry.main(command)
            open_entry.assert_called_once()

    def test_workspace_cannot_serve_as_production_release(self):
        with patch.object(common_entry, 'validate_install_target') as target:
            with self.assertRaisesRegex(MaintenanceError, 'fixed frozen release'):
                common_entry._production_entry()
            target.assert_not_called()

    def test_runtime_permission_gate_requires_account_group_and_private_modes(self):
        with tempfile.TemporaryDirectory(prefix='common-entry-metadata-') as directory:
            layout = NativeLayout.fixture(Path(directory).resolve(), ROOT)
            account = SimpleNamespace(pw_uid=42, pw_gid=43)
            journal = layout.http_journal / 'l_fixture.jsonl'
            metadata = {
                path: SimpleNamespace(st_mode=mode, st_uid=42, st_gid=43, st_nlink=1)
                for path, mode in ((layout.ledger.parent, stat.S_IFDIR | 0o700),
                    (layout.ledger, stat.S_IFREG | 0o600), (layout.http_journal, stat.S_IFDIR | 0o700),
                    (journal, stat.S_IFREG | 0o600))
            }

            def observed(path):
                return SimpleNamespace(stat=lambda: metadata[path], iterdir=lambda: (journal,))

            with patch.object(common_entry, '_safe', side_effect=observed):
                common_entry._verify_runtime(layout, account)
                for path, name, value in ((layout.ledger.parent, 'st_mode', stat.S_IFDIR | 0o750),
                    (layout.ledger, 'st_uid', 0), (layout.http_journal, 'st_gid', 0), (journal, 'st_nlink', 2)):
                    with self.subTest(path=path.name, field=name), patch.object(metadata[path], name, value):
                        with self.assertRaisesRegex(MaintenanceError, 'permissions differ'):
                            common_entry._verify_runtime(layout, account)


if __name__ == '__main__':
    unittest.main()
