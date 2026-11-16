"""Temporary synthetic native layout, real SQLite/CAS/mixed transaction, fake service."""
from contextlib import closing
import copy
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'dispatch'))
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.bundle import ledger
from sa_dispatch.canonical import canonical_json
from sa_dispatch.routing import activate_mixed, activation_manifest
from sa_dispatch.store_sqlite import SqliteStore
from tools.m0_native_mixed.maintenance import Coordinator, MaintenanceError, canonical, digest, guard_check, main, _write


class ProcessLoss(BaseException):
    pass


class FixtureAdapter:
    """Only this fixture's complete layout is supported, never arbitrary paths."""
    def __init__(self, root):
        self.root = root
        self.db = root / 'ledger.sqlite3'
        self.store = SqliteStore(self.db, lease_ttl_seconds=10)
        self.cas = ArtifactStore(root / 'cas')
        self.cas.put(b'preserved synthetic historical evidence')
        (root / 'http.json').write_bytes(b'{"mode":"isolated"}')
        (root / 'journal.json').write_bytes(b'{"pending":[]}')
        (root / 'domain.json').write_bytes(b'{"instance":"fixed-synthetic-domain"}')
        (root / 'binding.json').write_bytes(b'{"source":"original-source","schema":"0011"}')
        self.running, self.starts, self.stops, self.activations = True, 0, 0, 0
        self.adoptions = {}
        self.mutate_on_expose = False

    def rows(self):
        return ledger(self.db, include_devices=True)

    def identity(self):
        info = self.db.stat()
        return dict(device=info.st_dev, inode=info.st_ino)

    def freeze(self):
        return dict(identity=self.identity(), rows=self.rows(),
                    domain_sha=digest((self.root / 'domain.json').read_bytes()))

    def validate_plan(self, plan):
        if {c['path'] for c in plan['configs']} != {'http.json', 'local.json', 'binding.json'}:
            raise MaintenanceError('fixture supports only fixed config set')
        if plan['streams'] != ['events', 'control_events', 'schedule_events']:
            raise MaintenanceError('fixture requires explicit three streams')
        if self.identity() != plan['anchor']['identity']:
            raise MaintenanceError('stale ledger identity')
        if digest((self.root / 'domain.json').read_bytes()) != plan['anchor']['domain_sha']:
            raise MaintenanceError('domain drift')

    def stop_writers(self):
        self.running = False
        self.stops += 1

    def verify_quiescent(self, plan):
        if self.running:
            raise MaintenanceError('writer is alive')
        rows = self.rows()
        if rows['ledger_mode'][0]['mode'] == 'isolated' and rows != plan['anchor']['rows']:
            raise MaintenanceError('exact state changed')

    def capture_recovery(self, plan, destination):
        if self.running:
            raise MaintenanceError('cannot checkpoint a writer')
        destination.mkdir(exist_ok=True)
        files = ['ledger.sqlite3', 'journal.json', 'domain.json', 'binding.json', 'http.json']
        files.extend(str(p.relative_to(self.root)).replace('\\', '/')
                     for p in (self.root / 'cas').rglob('*') if p.is_file())
        receipt = {'files': {}}
        for name in files:
            source, target = self.root / name, destination / name
            raw = source.read_bytes()
            if not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
            if target.read_bytes() != raw:
                raise MaintenanceError('partial snapshot drift')
            receipt['files'][name] = digest(raw)
        return receipt

    def verify_recovery(self, plan, destination, receipt):
        if not {'ledger.sqlite3', 'journal.json', 'domain.json', 'http.json', 'binding.json'} <= receipt['files'].keys():
            raise MaintenanceError('incomplete native recovery set')
        for name, expected in receipt['files'].items():
            if digest((destination / name).read_bytes()) != expected:
                raise MaintenanceError('recovery changed')
        if ledger(destination / 'ledger.sqlite3', include_devices=True) != plan['anchor']['rows']:
            raise MaintenanceError('snapshot not exact')

    def prepare_activation(self, plan):
        with closing(sqlite3.connect(self.db)) as conn:
            conn.row_factory = sqlite3.Row
            proof = activation_manifest(conn, actor=plan['operation_id'], bindings={})
        return self.cas.put(canonical_json(proof))

    def activation_evidence(self, plan, proof_ref):
        proof = json.loads(self.cas.get(proof_ref))
        expected_proof = dict(schema='sa_dispatch.mixed_activation.v1', actor=plan['operation_id'],
                              ledger_hash='sha256:' + digest(canonical_json(plan['anchor']['rows'])),
                              bindings_hash='sha256:' + digest(canonical_json({})))
        if proof != expected_proof:
            raise MaintenanceError('wrong activation proof')
        rows = self.rows()
        if rows['ledger_mode'][0]['mode'] == 'isolated':
            if rows != plan['anchor']['rows']:
                raise MaintenanceError('pre-activation drift')
            return None
        audits = rows['route_audit']
        if (len(audits) != 1 or audits[0]['action'] != 'activate' or
                audits[0]['actor'] != plan['operation_id'] or audits[0]['evidence_ref'] != proof_ref):
            raise MaintenanceError('activation receipt mismatch')
        expected = copy.deepcopy(plan['anchor']['rows'])
        expected['ledger_mode'][0]['mode'] = 'mixed'
        expected['route_audit'] = audits
        if expected != rows:
            raise MaintenanceError('unexpected post-activation writes')
        return dict(proof_ref=proof_ref, audit=audits[0], identity=self.identity())

    def activate(self, plan, proof_ref):
        self.activations += 1
        activate_mixed(self.db, actor=plan['operation_id'], evidence_ref=proof_ref,
                       bindings={}, artifacts=self.cas)
        return self.activation_evidence(plan, proof_ref)

    def validate_configs(self, plan):
        for item in plan['configs']:
            if digest((self.root / item['path']).read_bytes()) != item['after_sha256']:
                raise MaintenanceError('config differs')

    def expose(self, plan):
        guard_check(self.root)
        self.running = True
        self.starts += 1
        if self.mutate_on_expose:
            (self.root / 'journal.json').write_bytes(b'{"pending":["unknown"]}')

    def health(self, plan):
        if not self.running:
            raise MaintenanceError('service unhealthy')

    def exposure_fingerprint(self, plan):
        return dict(rows=self.rows(), identity=self.identity(),
                    journal=digest((self.root / 'journal.json').read_bytes()))

    def adopt_stream(self, plan, stream):
        self.adoptions.setdefault(stream, {'source': 'original-source', 'stream': stream,
                                          'old_schema': '0011', 'new_schema': '0012',
                                          'last_seq': {'events': 17, 'control_events': 0, 'schedule_events': 3}[stream]})
        return self.adoptions[stream]


class MaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='native-mixed-core-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.adapter = FixtureAdapter(self.root)
        self.coordinator = Coordinator(self.root, 'synthetic-0012', self.adapter)
        self.configs = {'http.json': b'{"mode":"mixed","route":"http_synthetic"}',
                        'local.json': b'{"mode":"mixed","route":"local_synthetic"}',
                        'binding.json': b'{"source":"original-source","schema":"0012"}'}
        self.plan_hash = self.coordinator.stage(self.configs,
            ['events', 'control_events', 'schedule_events'])

    def interrupt(self, label):
        def fail(observed):
            if observed == label:
                raise ProcessLoss(label)
        self.coordinator.fault = fail
        with self.assertRaises(ProcessLoss):
            self.coordinator.run(self.plan_hash)
        self.coordinator.fault = lambda _: None

    def state(self):
        return json.loads((self.coordinator.operation / 'state.json').read_bytes())

    def test_real_mixed_activation_completion_and_repeat_is_inert(self):
        self.assertEqual(self.coordinator.run(self.plan_hash)['phase'], 'completed')
        self.assertEqual(self.adapter.activations, 1)
        self.assertEqual(self.adapter.store.routing_mode(), 'mixed')
        self.assertEqual(len(self.adapter.adoptions), 3)
        before = {p.relative_to(self.root): (p.read_bytes(), p.stat().st_mtime_ns)
                  for p in self.root.rglob('*') if p.is_file()}
        calls = self.adapter.starts, self.adapter.stops, self.adapter.activations
        self.coordinator.run(self.plan_hash)
        after = {p.relative_to(self.root): (p.read_bytes(), p.stat().st_mtime_ns)
                 for p in self.root.rglob('*') if p.is_file()}
        self.assertEqual(before, after)
        self.assertEqual(calls, (self.adapter.starts, self.adapter.stops, self.adapter.activations))

    def test_process_loss_at_every_transaction_boundary_recovers_forward(self):
        labels = ['guard_installed', 'state:guarded', 'writers_stopped', 'state:quiescent',
                  'recovery_captured', 'state:checkpointed', 'proof_written', 'state:proof_ready',
                  'activation_committed', 'state:activated', 'state:configuring',
                  'config_replaced:0', 'config_replaced:1', 'config_replaced:2', 'state:configured',
                  'state:exposing', 'before_expose', 'after_expose', 'state:healthy',
                  'stream_adopted:events', 'stream_adopted:control_events',
                  'stream_adopted:schedule_events', 'state:adopting', 'state:completed']
        for label in labels:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve()
                adapter = FixtureAdapter(root)
                coordinator = Coordinator(root, 'fault', adapter)
                plan_hash = coordinator.stage(self.configs, ['events', 'control_events', 'schedule_events'])
                coordinator.fault = lambda seen: (_ for _ in ()).throw(ProcessLoss()) if seen == label else None
                with self.assertRaises(ProcessLoss):
                    coordinator.run(plan_hash)
                coordinator = Coordinator(root, 'fault', adapter)
                self.assertEqual(coordinator.run(plan_hash)['phase'], 'completed')
                self.assertEqual(adapter.activations, 1)
                self.assertEqual(len(adapter.rows()['route_audit']), 1)

    def test_absent_or_corrupt_guard_and_phase_are_never_start_permission(self):
        with self.assertRaises(MaintenanceError):
            guard_check(self.root)
        self.interrupt('state:activated')
        with self.assertRaises(MaintenanceError):
            guard_check(self.root)
        state_path = self.coordinator.operation / 'state.json'
        state = self.state(); state.pop('phase')
        state_path.write_bytes(canonical(state))
        with self.assertRaises(MaintenanceError):
            guard_check(self.root)
        state_path.write_bytes(b'{')
        self.assertEqual(main(['guard-check', '--native-root', str(self.root)]), 1)

    def test_tampered_plan_or_frozen_bytes_refused_before_stop(self):
        (self.coordinator.operation / 'config-0.bin').write_bytes(b'wrong')
        with self.assertRaises(MaintenanceError):
            self.coordinator.run(self.plan_hash)
        self.assertEqual(self.adapter.stops, 0)

    def test_plan_hash_tamper_is_refused(self):
        path = self.coordinator.operation / 'plan.json'
        plan = json.loads(path.read_bytes())
        plan['anchor']['identity']['inode'] += 1
        path.write_bytes(canonical(plan))
        with self.assertRaisesRegex(MaintenanceError, 'plan hash'):
            self.coordinator.run(self.plan_hash)

    def test_missing_guard_after_exposure_is_not_recreated(self):
        self.interrupt('after_expose')
        guard = self.coordinator.base / 'guard.json'
        guard.unlink()
        with self.assertRaisesRegex(MaintenanceError, 'guard disappeared'):
            self.coordinator.run(self.plan_hash)
        self.assertFalse(guard.exists())
        self.assertFalse(self.adapter.running)
        self.assertEqual(self.state()['phase'], 'needs_reconciliation')

    def test_corrupt_guard_after_exposure_closes_and_stops(self):
        self.interrupt('after_expose')
        (self.coordinator.base / 'guard.json').write_bytes(b'{}')
        with self.assertRaises(MaintenanceError):
            self.coordinator.run(self.plan_hash)
        self.assertFalse(self.adapter.running)
        self.assertEqual(self.state()['phase'], 'needs_reconciliation')

    def test_journal_write_failure_after_exposure_still_stops_and_revokes_gate(self):
        real_write = _write
        def unavailable(path, raw, **kwargs):
            if Path(path).name == 'state.json' and self.adapter.running:
                raise OSError('synthetic full filesystem')
            return real_write(path, raw, **kwargs)
        with patch('tools.m0_native_mixed.maintenance._write', side_effect=unavailable):
            with self.assertRaises(OSError):
                self.coordinator.run(self.plan_hash)
        self.assertFalse(self.adapter.running)
        with self.assertRaises((MaintenanceError, KeyError)):
            guard_check(self.root)
        self.assertEqual((self.coordinator.base / 'guard.json').read_bytes(), b'{}')

    @unittest.skipIf(sys.platform == 'win32', 'Unix ownership preservation')
    def test_atomic_replacement_preserves_uid_gid_and_mode(self):
        import os
        path = self.root / 'http.json'
        os.chmod(path, 0o640)
        before = path.stat()
        _write(path, b'replacement')
        after = path.stat()
        self.assertEqual((before.st_uid, before.st_gid, before.st_mode),
                         (after.st_uid, after.st_gid, after.st_mode))

    def test_external_config_drift_preserved_and_guard_closed(self):
        self.interrupt('config_replaced:0')
        (self.root / 'http.json').write_bytes(b'external edit')
        with self.assertRaises(MaintenanceError):
            self.coordinator.run(self.plan_hash)
        self.assertEqual((self.root / 'http.json').read_bytes(), b'external edit')
        self.assertEqual(self.state()['phase'], 'needs_reconciliation')
        self.assertEqual(self.adapter.activations, 1)
        self.assertFalse(self.adapter.running)

    def test_unexpected_new_target_even_matching_intended_refused(self):
        (self.root / 'local.json').write_bytes(self.configs['local.json'])
        with self.assertRaises(MaintenanceError):
            self.coordinator.run(self.plan_hash)
        self.assertEqual(self.adapter.activations, 0)

    def test_committed_activation_missing_state_does_not_reactivate(self):
        self.interrupt('activation_committed')
        self.assertEqual(self.state()['phase'], 'proof_ready')
        self.coordinator.run(self.plan_hash)
        self.assertEqual(self.adapter.activations, 1)

    def test_new_write_after_exposure_stops_without_restore(self):
        self.interrupt('after_expose')
        (self.root / 'journal.json').write_bytes(b'{"pending":["unknown"]}')
        with self.assertRaises(MaintenanceError):
            self.coordinator.run(self.plan_hash)
        self.assertEqual(self.state()['phase'], 'needs_reconciliation')
        self.assertEqual((self.root / 'journal.json').read_bytes(), b'{"pending":["unknown"]}')
        self.assertFalse(self.adapter.running)

    def test_new_write_during_exposure_stops_without_restore(self):
        self.adapter.mutate_on_expose = True
        with self.assertRaises(MaintenanceError):
            self.coordinator.run(self.plan_hash)
        self.assertEqual(self.state()['phase'], 'needs_reconciliation')
        self.assertFalse(self.adapter.running)

    def test_stale_ledger_identity_refused(self):
        alternate = self.root / 'alternate.sqlite3'
        shutil.copyfile(self.adapter.db, alternate)
        alternate.replace(self.adapter.db)
        with self.assertRaises(MaintenanceError):
            self.coordinator.run(self.plan_hash)
        self.assertEqual(self.adapter.activations, 0)

    def test_snapshot_corruption_cannot_be_used_for_recovery(self):
        self.interrupt('state:checkpointed')
        (self.coordinator.operation / 'recovery' / 'journal.json').write_bytes(b'corrupt')
        with self.assertRaises(MaintenanceError):
            self.coordinator.run(self.plan_hash)
        self.assertEqual(self.adapter.activations, 0)

    def test_bounded_paths_and_operation_ids(self):
        for name in ('../escape', '/absolute', 'C:/escape', 'folder/../escape', '.mixed-maintenance/guard.json'):
            with self.subTest(name=name), self.assertRaises(MaintenanceError):
                Coordinator(self.root, 'path-test', self.adapter).stage({name: b'{}'}, [])
        with self.assertRaises(MaintenanceError):
            Coordinator(self.root, '../escape', self.adapter)
        with self.assertRaises(MaintenanceError):
            Coordinator(self.root / '..' / self.root.name, 'bad-root', self.adapter)

    def test_hardlinks_refused(self):
        import os
        alias = self.root / 'alias.json'
        os.link(self.root / 'http.json', alias)
        with self.assertRaises(MaintenanceError):
            self.coordinator.run(self.plan_hash)


if __name__ == '__main__':
    unittest.main()
