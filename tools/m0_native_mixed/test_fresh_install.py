"""Real new 0012 ledger, real Collector, and resumable read-only continuation."""

from contextlib import closing
from dataclasses import asdict, replace
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from dispatch.sa_dispatch.collector import Collector, SourceBinding
from dispatch.sa_dispatch.models import TaskSpec
from dispatch.sa_dispatch.routing import row_digest
from dispatch.sa_dispatch.scheduler import Scheduler
from dispatch.sa_dispatch.store_sqlite import SqliteStore
from tools.m0_deploy.install_target import TARGET_SCHEMA, TARGET_SCOPE
from tools.m0_native_mixed.domains import ROOT, stage_domains
from tools.m0_native_mixed.fresh_install import FRESH_FILES, FRESH_MODE, FRESH_SCHEMA, verify_fresh_baseline
from tools.m0_native_mixed.maintenance import Coordinator, MaintenanceError, canonical, digest
from tools.m0_native_mixed.native_adapter import NativeAdapter, NativeLayout, STREAMS
from tools.m0_native_mixed.test_native_adapter import FakeSystemd, ProcessLoss


MANIFEST = 'a' * 64


class ReadKind:
    version = 1
    enabled = True
    side_effect = 'none'

    def validate(self, value):
        if value != {}:
            raise ValueError('test read accepts empty parameters')
        return value

    def scope(self, value):
        return {}


def read_task(key):
    return TaskSpec(key, 'synthetic.read', 1, {}, (), (), 'synthetic', 'none', 2)


def open_store(path, *, route=None):
    return SqliteStore(path, lease_ttl_seconds=10, route=route, kinds={'synthetic.read': ReadKind()})


class FreshInstallTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix='native-fresh-')
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        l = self.layout = NativeLayout.fixture(self.root, ROOT)
        l.assert_fixture_contained(self.root)
        for path in (l.config_root, l.ledger.parent, l.artifacts, l.domain.parent, l.legacy_bindings,
                     l.maintenance_lock.parent, l.http_journal, *(p.parent for p in l.service_files)):
            path.mkdir(parents=True, exist_ok=True)
        l.maintenance_lock.write_bytes(b'0')
        l.credentials.write_bytes(b'credential-fixture-not-a-recovery-payload')
        for path in l.service_files:
            path.write_bytes(b'[Service]\nUser=sa-dispatch\n')
        l.ledger.touch(exist_ok=False)
        self.store = open_store(l.ledger)
        self.store.register_device('fresh-http', ['component:mgmtcore'])
        scheduler = Scheduler(self.store)
        schedule = dict(interval_seconds=30, task_template=read_task('template'),
                        next_fire_at='2026-11-16T00:00:00Z', enabled=False)
        scheduler.upsert('inert', **schedule, max_slots=2)
        scheduler.upsert('inert', **schedule, max_slots=None)
        stage_domains(l.domain, ledger=l.ledger, artifacts=l.artifacts, source_root=ROOT,
                      executable=Path(sys.executable).resolve(), device_id='fresh-local')
        http = dict(db_path=str(l.ledger), artifact_root=str(l.artifacts), journal_dir=str(l.http_journal),
                    device_id='coordinator', capabilities=[], plugins=['hotspot'], lease_ttl_seconds=30,
                    component_timeout_seconds=20, synthetic=True)
        raw = '\n'.join(f'{key} = {json.dumps(value)}' for key, value in http.items()) + '\n[components]\n'
        (l.config_root / 'coordinator.toml').write_text(raw, encoding='utf-8')
        self.binding = SourceBinding.enroll('fresh-dispatch', 'dispatch', l.ledger, 'fresh-instance', ['0012'])
        for stream in STREAMS:
            result = Collector(l.ledger).sync(self.binding, stream)
            self.assertEqual((result['state'], result['projection_lag']), ('synced', 0))
        self.baseline = verify_fresh_baseline(l.ledger, self.binding)
        self.identity = dict(device=l.ledger.stat().st_dev, inode=l.ledger.stat().st_ino)
        config = dict(schema='sa_dispatch.collector_binding.v1', target=str(l.ledger), sources=[asdict(self.binding)])
        manager = dict(version=1, sources=[dict(source_id=self.binding.source_id,
            source_instance=self.binding.source_instance, kind='dispatch', locator=str(l.ledger),
            accepted_schema_versions=['0012'], **{'class': 'internal'}, freshness_s=86400,
            options={'identity': self.identity})])
        self.configs = {'coordinator.toml': raw.replace('[components]', 'execution_route = "http_synthetic"\n[components]').encode(),
                        'local.toml': (l.domain / 'local.toml').read_bytes()}
        for name, value in (('collector-binding.json', config), ('manager-binding.json', manager)):
            self.configs[name] = canonical(value)
            (l.legacy_bindings / name).write_bytes(self.configs[name])
        worker, = self.baseline['devices']
        provenance = l.config_root / 'http-worker-source.json'
        provenance.write_bytes(canonical(worker))
        l.workers.write_bytes(canonical(dict(schema='native_mixed.http_workers.v1', devices=[dict(
            device_id=worker['device_id'], row_sha256=row_digest(worker), source_path=str(provenance),
            source_sha256=digest(provenance.read_bytes()))])))
        (l.config_root / 'install-target.json').write_bytes(canonical(dict(schema=TARGET_SCHEMA,
            scope=TARGET_SCOPE, manifest_sha256=MANIFEST, machine_id='b' * 32, hostname='fresh-fixture')))
        (l.config_root / 'fresh-install-state.json').write_bytes(canonical({'phase': 'completed'}))
        self.proof = dict(schema=FRESH_SCHEMA, phase='completed', creation='exclusive_new_ledger',
                          deployment_manifest=MANIFEST, ledger=str(l.ledger), ledger_identity=self.identity,
                          source_binding=asdict(self.binding), baseline_rows_sha256=digest(canonical(self.baseline)))
        self.service = FakeSystemd()
        self.service.running = False
        self.adapter = self.write_proof(self.proof)

    def write_proof(self, value):
        raw = canonical(value)
        (self.layout.config_root / 'fresh-install.json').write_bytes(raw)
        return NativeAdapter(self.layout, fixture=True, service=self.service, fresh_install_sha256=digest(raw))

    def stage(self):
        self.coordinator = Coordinator(self.layout.config_root, 'fresh', self.adapter)
        self.plan_hash = self.adapter.stage(self.coordinator, self.configs)
        self.plan = json.loads((self.coordinator.operation / 'plan.json').read_bytes())

    def run_operation(self):
        return self.adapter.run(self.coordinator, self.plan_hash)

    def execute(self, sql):
        with closing(sqlite3.connect(self.layout.ledger)) as conn, conn:
            conn.execute(sql)

    def test_receipts_resume_readonly_and_later_increment_is_collected_once(self):
        self.stage()
        self.assertEqual(self.plan['anchor']['collector_mode'], FRESH_MODE)
        self.assertEqual(set(self.plan['anchor']['fresh_files']), set(FRESH_FILES))
        self.assertEqual(self.plan['anchor']['cursors']['schedule_events']['last_seq'], 1)

        with patch.object(Collector, 'sync', side_effect=AssertionError('maintenance may not sync')), \
                patch.object(Collector, 'adopt', side_effect=AssertionError('maintenance may not adopt')):
            def interrupt_before_receipt(label):
                if label == 'stream_adopted:events':
                    raise ProcessLoss()

            self.coordinator.fault = interrupt_before_receipt
            with self.assertRaises(ProcessLoss):
                self.run_operation()

            def interrupt_after_receipt(label):
                if label == 'state:adopting':
                    raise ProcessLoss()

            self.coordinator.fault = interrupt_after_receipt
            with self.assertRaises(ProcessLoss):
                self.run_operation()
            state_path = self.coordinator.operation / 'state.json'
            prior = json.loads(state_path.read_bytes())['adopted']['events']
            self.coordinator.fault = lambda label: None
            result = self.run_operation()
            self.assertEqual(result['phase'], 'completed')
            self.assertEqual(result['adopted']['events'], prior)
            self.assertEqual(set(result['adopted']), set(STREAMS))
            for stream, receipt in result['adopted'].items():
                self.assertEqual((receipt['reason'], receipt['epoch']), ('continue_same_schema', 1))
                self.assertEqual(self.adapter.adopt_stream(self.plan, stream), receipt)
            self.assertEqual(self.adapter.rows()['collector_cursors'], self.baseline['collector_cursors'])
            self.assertEqual(self.adapter.rows()['collector_gaps'], [])
            self.assertEqual(self.adapter.rows()['ledger_mode'], [{'id': 1, 'mode': 'mixed'}])
            recovery = self.coordinator.operation / 'recovery' / 'config'
            for name in FRESH_FILES:
                self.assertEqual((recovery / name).read_bytes(), (self.layout.config_root / name).read_bytes())
            self.assertFalse((recovery / 'identities.json').exists())
            state_mtime = state_path.stat().st_mtime_ns
            starts = self.service.starts
            self.assertEqual(self.run_operation(), result)
            self.assertEqual((state_path.stat().st_mtime_ns, self.service.starts), (state_mtime, starts))

        open_store(self.layout.ledger, route='http_synthetic').submit(read_task('later'))
        writer = Collector(self.layout.ledger)
        self.assertEqual(writer.sync(self.binding, 'events')['added'], 1)
        self.assertEqual(writer.sync(self.binding, 'events')['added'], 0)
        self.assertEqual(self.adapter.rows()['collector_gaps'], [])
        self.assertEqual(self.run_operation(), result)
        legacy = NativeAdapter(self.layout, fixture=True, service=self.service)
        with self.assertRaisesRegex(MaintenanceError, 'explicit fresh installation proof required'):
            legacy.run(Coordinator(self.layout.config_root, 'fresh', legacy), self.plan_hash)

    def test_mode_proof_and_business_history_fail_closed(self):
        legacy = NativeAdapter(self.layout, fixture=True, service=self.service)
        with self.assertRaisesRegex(MaintenanceError, 'old cursor authority'):
            legacy.freeze()
        self.stage()
        with self.assertRaisesRegex(MaintenanceError, 'explicit fresh installation proof required'):
            legacy.validate_plan(self.plan)
        for change in (dict(ledger_identity={'device': 0, 'inode': 0}), dict(phase='initializing'),
                       dict(source_binding=asdict(replace(self.binding, source_instance='other'))),
                       dict(deployment_manifest='f' * 64)):
            with self.subTest(change=change), self.assertRaisesRegex(MaintenanceError, 'fresh installation'):
                self.write_proof({**self.proof, **change}).freeze()
        with self.assertRaisesRegex(MaintenanceError, 'proof hash differs'):
            self.adapter.freeze()
        with self.assertRaisesRegex(MaintenanceError, 'snapshot differs from the initialization proof'):
            self.write_proof({**self.proof, 'baseline_rows_sha256': 'f' * 64}).freeze()
        self.adapter = self.write_proof(self.proof)
        self.service.running = True
        with self.assertRaisesRegex(MaintenanceError, 'untracked writer'):
            self.adapter.freeze()
        self.service.running = False
        self.execute("UPDATE schedules SET enabled=1 WHERE id='inert'")
        with self.assertRaisesRegex(MaintenanceError, 'active scheduling'):
            self.adapter.freeze()
        self.execute("UPDATE schedules SET enabled=0 WHERE id='inert'")
        self.store.submit(read_task('unauthorized-baseline-history'))
        for stream in STREAMS:
            Collector(self.layout.ledger).sync(self.binding, stream)
        with self.assertRaisesRegex(MaintenanceError, 'business history'):
            self.adapter.freeze()

    def test_missing_stream_schema_anchor_and_binding_drift(self):
        for stream, mutation, message in (
            ('events', "DELETE FROM collector_cursors WHERE stream='events'", 'three cursors'),
            ('events', "UPDATE collector_cursors SET schema_sha256=lower(hex(zeroblob(32))) WHERE stream='events'", 'schema or epoch'),
            ('schedule_events', "UPDATE collector_cursors SET anchor_sha256=lower(hex(zeroblob(32))) WHERE stream='schedule_events'", 'tail or anchor'),
        ):
            with self.subTest(mutation=mutation), closing(sqlite3.connect(self.layout.ledger)) as conn:
                conn.execute(mutation)
                conn.commit()
                try:
                    with self.assertRaisesRegex(MaintenanceError, message):
                        self.adapter.freeze()
                finally:
                    cursor = next(row for row in self.baseline['collector_cursors'] if row['stream'] == stream)
                    conn.execute('INSERT OR REPLACE INTO collector_cursors VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                                 tuple(cursor.values()))
                    conn.commit()
        self.execute("UPDATE schedule_events SET detail_code='different' WHERE seq=1")
        with self.assertRaisesRegex(MaintenanceError, 'tail or anchor'):
            self.adapter.freeze()
        self.execute("UPDATE schedule_events SET detail_code='max_slots_removed' WHERE seq=1")
        self.stage()
        self.run_operation()
        path = self.layout.config_root / 'collector-binding.json'
        path.write_bytes(path.read_bytes() + b' ')
        with self.assertRaisesRegex(MaintenanceError, 'binding changed'):
            self.adapter.adopt_stream(self.plan, 'events')

    def test_source_append_during_maintenance_closes_restart_guard(self):
        self.stage()

        def append_after_one_stream(label):
            if label == 'stream_adopted:events':
                open_store(self.layout.ledger, route='http_synthetic').submit(read_task('drift'))

        self.coordinator.fault = append_after_one_stream
        with self.assertRaisesRegex(MaintenanceError, 'fresh Collector source'):
            self.run_operation()
        state = json.loads((self.coordinator.operation / 'state.json').read_bytes())
        self.assertEqual(state['phase'], 'needs_reconciliation')
        self.assertFalse(self.service.running)
        self.assertEqual(self.adapter.rows()['collector_cursors'], self.baseline['collector_cursors'])
        self.assertEqual(self.adapter.rows()['collector_gaps'], [])

    def test_recovery_refuses_changed_initialization_record(self):
        self.stage()

        def mutate_frozen_record(label):
            if label == 'writers_stopped':
                (self.layout.config_root / 'fresh-install-state.json').write_bytes(b'{}')

        self.coordinator.fault = mutate_frozen_record
        with self.assertRaisesRegex(MaintenanceError, 'fresh recovery evidence differs'):
            self.run_operation()
        self.assertFalse(self.service.running)

    def test_continuation_refuses_initialization_proof_drift_after_exposure(self):
        self.stage()

        def mutate_frozen_record(label):
            if label == 'stream_adopted:events':
                (self.layout.config_root / 'fresh-install.json').write_bytes(b'{}')

        self.coordinator.fault = mutate_frozen_record
        with self.assertRaisesRegex(MaintenanceError, 'fresh installation proof hash differs'):
            self.run_operation()
        self.assertFalse(self.service.running)
        self.assertEqual(self.adapter.rows()['collector_cursors'], self.baseline['collector_cursors'])


if __name__ == '__main__':
    unittest.main()
