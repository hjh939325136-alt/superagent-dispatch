from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from dispatch.sa_dispatch.bundle import ledger
from dispatch.sa_dispatch.collector_cli import load_binding as load_collector
from dispatch.sa_dispatch.store_sqlite import SqliteStore
from tools.m0_installer import common_baseline as baseline
from tools.m0_manager.binding import load_binding as load_manager, parse_json
from tools.m0_native_mixed.maintenance import MaintenanceError, canonical, digest
from tools.m0_native_mixed.native_adapter import NativeLayout, STREAMS


MANIFEST = 'a' * 64
SOURCE = Path(__file__).resolve().parents[2]


class ProcessLoss(RuntimeError):
    pass


class Stopped:
    def __init__(self):
        self.calls = 0

    def verify_stopped(self):
        self.calls += 1


class CommonBaselineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.fixture = self.base / 'fixture'
        self.fixture.mkdir()
        self.layout = self.make_layout(self.fixture)
        self.service = Stopped()

    def make_layout(self, root):
        layout = NativeLayout.fixture(root, SOURCE)
        for directory in (layout.config_root, layout.ledger.parent, layout.legacy_bindings,
                          layout.artifacts, layout.http_journal, layout.maintenance_lock.parent):
            directory.mkdir(parents=True, exist_ok=True)
        layout.maintenance_lock.write_bytes(b'0')
        return layout

    def initialize(self, *, fault=None, manifest=MANIFEST, fixture=None, layout=None):
        return baseline.initialize(self.layout if layout is None else layout, deployment_manifest=manifest,
                                   fixture_root=self.fixture if fixture is None else fixture,
                                   service=self.service, fault=fault)

    def test_real_store_collector_and_exact_immutable_receipt(self):
        result = self.initialize()
        receipt = result['receipt']
        target, bindings, _ = load_collector(self.layout.legacy_bindings / 'collector-binding.json')
        binding = bindings[baseline.COLLECTOR_SOURCE_ID]
        manager, _ = load_manager(self.layout.legacy_bindings / 'manager-binding.json')
        rows = ledger(target, include_devices=True)
        self.assertEqual(receipt['baseline_rows_sha256'], digest(canonical(rows)))
        self.assertEqual(receipt['source_binding'], binding.__dict__)
        self.assertEqual(receipt['schema'], 'native_mixed.fresh_install.v1')
        self.assertEqual(binding.accepted_schema_versions, ['0012'])
        self.assertEqual(manager['sources'][0]['options']['identity'], receipt['ledger_identity'])
        self.assertEqual(rows['ledger_mode'], [{'id': 1, 'mode': 'isolated'}])
        self.assertEqual(rows['tasks'], [])
        self.assertEqual(rows['collector_gaps'], [])
        self.assertEqual({(c['stream'], c['epoch'], c['authority']) for c in rows['collector_cursors']},
                         {(stream, 1, 'local_owned') for stream in STREAMS})
        worker = parse_json(self.layout.workers.read_bytes())['devices'][0]
        self.assertEqual(worker['device_id'], baseline.HTTP_DEVICE_ID)
        self.assertEqual(worker['source_sha256'], digest(Path(worker['source_path']).read_bytes()))
        paths = (self.layout.ledger, self.layout.workers, Path(result['receipt_path']), *self.layout.legacy_bindings.iterdir())
        before = [(path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes()) for path in paths]
        self.assertEqual(self.initialize(), result)
        self.assertEqual([(path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes()) for path in paths], before)
        self.assertEqual(self.service.calls, 2)

    def test_resume_preserves_ledger_inode_and_committed_cursor(self):
        milestones = ('planned', 'reserved', 'ledger_linked', 'ledger_published', 'worker_registered',
                      'collector_synced:events', 'before_receipt')
        for index, checkpoint in enumerate(milestones):
            with self.subTest(checkpoint=checkpoint):
                fixture = self.base / f'resume-{index}'
                fixture.mkdir()
                layout = self.make_layout(fixture)

                def fault(label):
                    if label == checkpoint:
                        raise ProcessLoss(label)

                with self.assertRaises(ProcessLoss):
                    self.initialize(fixture=fixture, layout=layout, fault=fault)
                prior_inode = layout.ledger.stat().st_ino if layout.ledger.exists() else None
                prior_cursor = None
                if checkpoint in ('collector_synced:events', 'before_receipt'):
                    prior_cursor = next(row for row in ledger(layout.ledger)['collector_cursors'] if row['stream'] == 'events')
                result = self.initialize(fixture=fixture, layout=layout)
                self.assertEqual(layout.ledger.stat().st_nlink, 1)
                if prior_inode is not None:
                    self.assertEqual(result['receipt']['ledger_identity']['inode'], prior_inode)
                if prior_cursor is not None:
                    current = next(row for row in ledger(layout.ledger)['collector_cursors'] if row['stream'] == 'events')
                    self.assertEqual(current, prior_cursor)

    def test_foreign_ledger_is_never_adopted(self):
        SqliteStore(self.layout.ledger, lease_ttl_seconds=30).register_device('foreign-worker', ['fixture'])
        before = self.layout.ledger.read_bytes()
        with self.assertRaisesRegex(MaintenanceError, 'existing unowned ledger'):
            self.initialize()
        self.assertEqual(self.layout.ledger.read_bytes(), before)
        self.assertFalse((self.layout.config_root / baseline.STATE_NAME).exists())

    def test_reservation_without_identified_inode_is_not_adopted(self):
        def fault(label):
            if label == 'planned':
                state = parse_json((self.layout.config_root / baseline.STATE_NAME).read_bytes())
                self.layout.ledger.with_name('.' + state['source_instance'] + '.sqlite3').write_bytes(b'foreign')
                raise ProcessLoss(label)

        with self.assertRaises(ProcessLoss):
            self.initialize(fault=fault)
        with self.assertRaisesRegex(MaintenanceError, 'without durable inode authority'):
            self.initialize()
        self.assertFalse(self.layout.ledger.exists())

    def test_resume_rejects_worker_or_manifest_drift(self):
        def fault(label):
            if label == 'worker_registered':
                raise ProcessLoss(label)

        with self.assertRaises(ProcessLoss):
            self.initialize(fault=fault)
        with self.assertRaisesRegex(MaintenanceError, 'different or invalid operation'):
            self.initialize(manifest='b' * 64)
        SqliteStore(self.layout.ledger, lease_ttl_seconds=30).register_device('foreign-worker', ['fixture'])
        before = self.layout.ledger.read_bytes()
        with self.assertRaisesRegex(MaintenanceError, 'worker registration drift'):
            self.initialize()
        self.assertEqual(self.layout.ledger.read_bytes(), before)

    def test_completed_missing_binding_is_refused_without_repair(self):
        self.initialize()
        binding = self.layout.legacy_bindings / 'collector-binding.json'
        binding.unlink()
        with self.assertRaisesRegex(MaintenanceError, 'completed fresh installation document is missing'):
            self.initialize()
        self.assertFalse(binding.exists())

    def test_replaced_published_ledger_is_refused_on_resume(self):
        def fault(label):
            if label == 'ledger_published':
                raise ProcessLoss(label)

        with self.assertRaises(ProcessLoss):
            self.initialize(fault=fault)
        saved = self.layout.ledger.with_name('saved.sqlite3')
        self.layout.ledger.rename(saved)
        SqliteStore(self.layout.ledger, lease_ttl_seconds=30)
        foreign = self.layout.ledger.read_bytes()
        with self.assertRaisesRegex(MaintenanceError, 'unexpected file at the fresh ledger'):
            self.initialize()
        self.assertEqual(self.layout.ledger.read_bytes(), foreign)
        self.assertTrue(saved.is_file())

    def test_fixture_refuses_escaped_and_production_injected_writes(self):
        escaped = self.base / 'escaped.sqlite3'
        with self.assertRaisesRegex(MaintenanceError, 'bounded fresh fixture'):
            self.initialize(layout=replace(self.layout, ledger=escaped))
        with self.assertRaisesRegex(MaintenanceError, 'fixed native layout and real writer checks'):
            baseline.initialize(self.layout, deployment_manifest=MANIFEST, service=self.service)
        self.assertFalse(escaped.exists())
        self.assertFalse(self.layout.ledger.exists())
        self.assertEqual(self.service.calls, 0)


if __name__ == '__main__':
    unittest.main()
