"""Real fresh initialization, frozen bootstrap and mixed ledger; fake systemd."""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

from dispatch.sa_dispatch.bundle import ledger
from dispatch.sa_dispatch.collector import Collector, SourceBinding
from dispatch.sa_dispatch.models import TaskSpec
from dispatch.sa_dispatch.store_sqlite import SqliteStore
from dispatch.sa_dispatch_hotspot import HotspotKind
from tools.m0_deploy.install_target import TARGET_SCHEMA, TARGET_SCOPE
from tools.m0_installer.common_baseline import initialize
from tools.m0_native_mixed.bootstrap import Bootstrap
from tools.m0_native_mixed.common_entry import fixture_entry
from tools.m0_native_mixed.domains import ROOT
from tools.m0_native_mixed.maintenance import canonical, digest
from tools.m0_native_mixed.native_adapter import NativeLayout, source_files
from tools.m0_native_mixed.test_bootstrap import FakeSystemdBootstrap
from tools.m0_native_mixed.test_native_adapter import FakeSystemd


class Stopped:
    def verify_stopped(self):
        pass


class CommonIntegrationTests(unittest.TestCase):
    def test_fresh_baseline_bootstrap_activation_and_later_increment(self):
        with tempfile.TemporaryDirectory(prefix='common-integration-') as directory:
            root = Path(directory).resolve()
            source_sha = digest(canonical(source_files(NativeLayout(ROOT))))
            seed = NativeLayout.fixture(root, ROOT)
            release = seed.maintenance_lock.parent.parent / 'sa-dispatch-native/releases' / source_sha
            layout = replace(seed, source_root=release)
            layout.assert_fixture_contained(root)
            self.assertTrue(release.is_relative_to(root))
            for path in (layout.config_root, layout.ledger.parent, layout.legacy_bindings,
                         layout.artifacts, layout.http_journal, layout.maintenance_lock.parent,
                         layout.domain.parent, *(p.parent for p in layout.service_files)):
                path.mkdir(parents=True, exist_ok=True)
            layout.maintenance_lock.write_bytes(b'0')
            target = dict(schema=TARGET_SCHEMA, scope=TARGET_SCOPE, manifest_sha256='a' * 64,
                          machine_id='b' * 32, hostname='synthetic-common-fixture')
            (layout.config_root / 'install-target.json').write_bytes(canonical(target))
            initialized = initialize(layout, deployment_manifest='a' * 64,
                                     fixture_root=root, service=Stopped())
            before = ledger(layout.ledger, include_devices=True)
            self.assertEqual(len(before['collector_cursors']), 3)
            self.assertEqual(before['tasks'], [])
            self.assertEqual(before['collector_gaps'], [])

            http = dict(db_path=str(layout.ledger), artifact_root=str(layout.artifacts),
                        journal_dir=str(layout.http_journal), device_id='coordinator',
                        capabilities=[], plugins=['hotspot'], lease_ttl_seconds=30,
                        component_timeout_seconds=20, synthetic=True)
            raw = ('\n'.join(f'{key} = {json.dumps(value)}' for key, value in http.items())
                   + '\n[components]\n').encode()
            (layout.config_root / 'coordinator.toml').write_bytes(raw)
            layout.credentials.write_bytes(b'opaque synthetic identity fixture: do not read')
            for path in layout.service_files[:-1]:
                path.write_bytes(b'[Service]\nUser=sa-dispatch\n')
            platform = FakeSystemdBootstrap(layout)
            platform.current = dict(ActiveState='inactive', MainPID='0', NRestarts='0')
            platform.metadata[layout.config_root / 'coordinator.toml'] = ('config', 0o640)
            platform.metadata[layout.credentials] = ('config', 0o640)
            platform.metadata[layout.artifacts] = ('runtime', 0o700)
            result = Bootstrap(ROOT, expected_source_sha256=source_sha,
                               coordinator_before_sha256=digest(raw), layout=layout,
                               platform=platform, fixture=True).run()
            self.assertEqual(result['phase'], 'completed')
            self.assertEqual(ledger(layout.ledger, include_devices=True), before)
            self.assertEqual(platform.current['ActiveState'], 'inactive')

            service = FakeSystemd()
            service.running = False
            entry = fixture_entry(layout, fixture_root=root, service=service,
                                  deployment_manifest='a' * 64,
                                  fresh_install_sha256=initialized['receipt_sha256'],
                                  coordinator_before_sha256=digest(raw),
                                  operation_id='common-' + 'a' * 24)
            staged = entry.plan()
            self.assertEqual(entry.plan(), staged)
            plan = staged['plan_sha256']
            completed = entry.run(plan)
            self.assertEqual(completed['phase'], 'completed')
            after = ledger(layout.ledger, include_devices=True)
            self.assertEqual(after['collector_cursors'], before['collector_cursors'])
            self.assertEqual(after['collector_gaps'], [])
            self.assertEqual(after['ledger_mode'], [{'id': 1, 'mode': 'mixed'}])
            self.assertEqual(after['tasks'], [])
            self.assertEqual(service.starts, 1)
            self.assertEqual(entry.run(plan), completed)
            self.assertEqual(ledger(layout.ledger, include_devices=True), after)
            self.assertEqual(service.starts, 1)
            status = entry.status(plan)
            self.assertEqual(status['phase'], 'completed')
            self.assertEqual(status['ledger_mode'], 'mixed')
            self.assertEqual(status['collector_gap_count'], 0)

            # Subsequent legitimate work projects once from the preserved cursor.
            binding = SourceBinding(**initialized['receipt']['source_binding'])
            kind = HotspotKind('topic_fetch', 'none')
            store = SqliteStore(layout.ledger, lease_ttl_seconds=30, route='http_synthetic',
                                kinds={kind.name: kind})
            store.submit(TaskSpec('common-after-activation', kind.name, 1,
                                  {'op': 'topic.list', 'filters': {}}, (), (),
                                  'synthetic', 'none', 2))
            first = Collector(layout.ledger).sync(binding, 'events')
            second = Collector(layout.ledger).sync(binding, 'events')
            self.assertGreater(first['added'], 0)
            self.assertEqual(second['added'], 0)
            self.assertEqual(first['epoch'], 1)
            self.assertEqual(second['epoch'], 1)


if __name__ == '__main__':
    unittest.main()
