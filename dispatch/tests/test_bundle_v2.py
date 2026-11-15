"""Portable v2 restore uses only temporary synthetic ledgers and actual fake proofs."""
import copy
from dataclasses import asdict, replace
import json
from pathlib import Path
import sys
import tempfile
import unittest

from sa_dispatch.artifacts import ArtifactError, ArtifactStore
from sa_dispatch.bundle import (FORMAT, TABLES, V1_FORMAT, V1_TABLES, export_bundle,
    import_bundle, import_snapshot_records, ledger, read_bundle, snapshot_records, verify_bundle)
from sa_dispatch.canonical import canonical_json, sha256_ref
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.component_proof import prove_idempotency
from sa_dispatch.config import ComponentConfig, Config
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.models import DispatchError, Evidence, Outcome, TaskSpec
from sa_dispatch.scheduler import Scheduler
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.unified_events import UnifiedEventStore
from sa_dispatch_hotspot import KINDS


class ReadKind:
    name, version, enabled, side_effect = 'lab.read', 1, True, 'none'
    def validate(self, params): return params
    def scope(self, params): return {}


class BundleV2Tests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix='m0-bundle-v2-')
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.clock = FakeClock()
        self.kinds = {**KINDS, 'lab.read': ReadKind()}
        fake = Path(__file__).parent / 'fakes/fake_component.py'
        self.config = Config(self.root / 'source.db', self.root / 'source-artifacts', self.root / 'journal',
            'synthetic-device', ('cpu', 'component:mgmtcore'), 10, 5, ('hotspot',),
            {'mgmtcore': ComponentConfig((sys.executable, str(fake.resolve()), '--data-dir', str(self.root/'component')), 'any')}, True)
        self.store = SqliteStore(self.config.db_path, clock=self.clock, lease_ttl_seconds=10,
                                  kinds=self.kinds, component_config=self.config)
        self.artifacts = ArtifactStore(self.config.artifact_root)
        self.store.register_device(self.config.device_id, self.config.capabilities)
        self.spec = TaskSpec('synthetic-template', 'lab.read', 1, {}, (), ('cpu',), 'synthetic', 'none', 2)

    def target(self):
        config = replace(self.config, db_path=self.root/'target.db', artifact_root=self.root/'target-artifacts',
                         journal_dir=self.root/'target-journal', device_id='new-device')
        return config, SqliteStore(config.db_path, clock=self.clock, lease_ttl_seconds=10,
                                  kinds=self.kinds, component_config=config)

    def complete(self, scheduled=False):
        if scheduled:
            scheduler = Scheduler(self.store)
            scheduler.upsert('synthetic-rule', interval_seconds=10, task_template=self.spec,
                             next_fire_at=timestamp(self.clock.now()))
            task_id = scheduler.tick()[0]['task_id']
            # A standalone rejected upsert event remains part of audit even without a rule row.
            with self.store._tx() as conn:
                conn.execute('INSERT INTO schedule_events(schedule_id,version,kind,at,detail_code) VALUES(?,?,?,?,?)',
                    ('invalid-rule', 1, 'schedule.template_error', timestamp(self.clock.now()), 'synthetic_rejection'))
        else:
            task_id = self.store.submit(self.spec)[0].task_id
        lease = self.store.claim(self.config.device_id)
        ref = self.artifacts.put(b'{"synthetic_result":"complete"}')
        outcome = Outcome(evidence=(Evidence('fixture', ref, 'synthetic', artifact_hash=ref),))
        self.store.complete(lease.lease_id, lease.fence, outcome, self.config.device_id)
        return task_id, ref

    def reconciled(self):
        spec = TaskSpec('synthetic-write', 'hotspot.topic_adopt', 1,
            {'project_id': 'synthetic-project', 'title': 'Synthetic', 'body': 'Synthetic', 'author': 'fixture',
             'evidence_refs': [{'owner': 'fixture', 'kind': 'topic', 'id': 'one', 'version': '1'}]},
            (), ('component:mgmtcore',), 'synthetic', 'none', 2)
        task = self.store.submit(spec)[0]
        lease = self.store.claim(self.config.device_id)
        self.store.mark_unknown(lease.lease_id, lease.fence, 'synthetic lost receipt', self.config.device_id)
        self.store.resolve_unknown(task.task_id, 'still_unknown', 'synthetic-reviewer', expected_fence=lease.fence)
        ref = self.artifacts.put(b'{"synthetic":"reconciliation-output"}')
        outcome = Outcome(evidence=(Evidence('output', ref, 'synthetic', artifact_hash=ref),))
        decision = {'schema': 'sa_dispatch.reconciliation.v1', 'task_id': task.task_id,
            'fence': lease.fence, 'idem_key': task.idem_key, 'decision': 'succeeded',
            'outcome_hash': sha256_ref(canonical_json(asdict(outcome)))}
        decision_ref = self.artifacts.put(canonical_json(decision))
        self.store.resolve_unknown(task.task_id, 'done', 'synthetic-reviewer', manual=True,
            outcome=outcome, evidence_ref=decision_ref, expected_fence=lease.fence, artifacts=self.artifacts)
        return task.task_id, decision_ref

    def proof(self):
        result = prove_idempotency(self.config, 'mgmtcore', KINDS['hotspot.topic_adopt'], artifacts=self.artifacts)
        self.store.register_capability('mgmtcore', 'hotspot.topic_adopt@1', lookup_by_write_key=True,
            idempotency_proof='verified', proof_ref=result['proof_ref'], artifacts=self.artifacts)
        return result

    def test_v2_round_trip_preserves_all_audit_rows_and_disables_schedule(self):
        task_id, receipt_ref = self.complete(scheduled=True)
        reconciled_id, decision_ref = self.reconciled()
        proof = self.proof()
        UnifiedEventStore(self.config.db_path, clock=self.clock).project_task(self.store, reconciled_id)
        with self.store._tx() as conn:
            conn.execute('INSERT INTO transport_handoffs VALUES(?,?,?)', (task_id, 'fixture', timestamp(self.clock.now())))
            conn.execute('INSERT INTO transport_artifact_grants VALUES(?,?,?,?,?,?)',
                ('fixture-grant', task_id, None, receipt_ref, 'synthetic', timestamp(self.clock.now())))
        before = ledger(self.config.db_path)
        bundle = self.root/'bundle'
        manifest = export_bundle(self.config, bundle)
        self.assertEqual(manifest['schema'], FORMAT)
        self.assertIn(decision_ref, manifest['artifacts'])
        for field in ('first_result_hash', 'second_result_hash', 'state_after_first_ref', 'state_after_second_ref'):
            self.assertIn(proof['proof'][field], manifest['artifacts'])
        # request_sha256 and implementation hashes are not arbitrary artifact imports.
        self.assertNotIn(proof['proof']['request_sha256'], manifest['artifacts'])
        self.clock.advance(100)
        config, target = self.target()
        report = import_bundle(config, bundle, target)
        after = ledger(config.db_path)
        for table in TABLES:
            expected = copy.deepcopy(before[table])
            if table == 'schedules':
                for row in expected: row['enabled'] = 0
            self.assertEqual(expected, after[table], table)
        self.assertEqual(target.reconciliation_history(reconciled_id), self.store.reconciliation_history(reconciled_id))
        self.assertEqual(report['disabled_schedule_ids'], ['synthetic-rule'])
        self.assertEqual(target.snapshot()['devices'], [])
        self.assertEqual(Scheduler(target).tick(), [])
        self.assertEqual(len(target.list_tasks()), 2)
        # Restored proof can be registered with the same implementation, including its child blobs.
        target.register_capability('mgmtcore', 'hotspot.topic_adopt@1', idempotency_proof='verified',
            proof_ref=proof['proof_ref'], artifacts=ArtifactStore(config.artifact_root))

    def test_full_store_snapshot_retains_reconciliation_history(self):
        task_id, _ = self.reconciled()
        snapshot = self.store.snapshot()
        config, target = self.target()
        report = target.import_snapshot(snapshot)
        self.assertEqual(target.reconciliation_history(task_id), self.store.reconciliation_history(task_id))
        self.assertEqual(target.snapshot(), snapshot)
        self.assertEqual(report['schedules_enabled'], 0)

    def test_pending_reservation_is_rejected_even_when_schedule_disabled(self):
        scheduler = Scheduler(self.store)
        scheduler.upsert('pending-rule', interval_seconds=10, task_template=self.spec,
                         next_fire_at=timestamp(self.clock.now()), enabled=False)
        with self.store._tx() as conn:
            conn.execute('INSERT INTO schedule_firings(fire_key,schedule_id,scheduled_at,task_template,version,outcome,at) '
                         'VALUES(?,?,?,?,?,?,?)', ('pending', 'pending-rule', timestamp(self.clock.now()),
                         json.dumps(asdict(self.spec)), 1, 'pending', timestamp(self.clock.now())))
        with self.assertRaisesRegex(ArtifactError, 'pending schedule'):
            export_bundle(self.config, self.root/'blocked')
        self.assertFalse((self.root/'blocked').exists())
        _, target = self.target()
        with self.assertRaisesRegex(DispatchError, 'pending schedule'):
            target.import_snapshot(self.store.snapshot())
        self.assertEqual(target.list_tasks(), [])

    def test_nonterminal_and_active_lease_are_rejected(self):
        self.store.submit(self.spec)
        with self.assertRaisesRegex(ArtifactError, 'settle'):
            export_bundle(self.config, self.root/'blocked')
        lease = self.store.claim(self.config.device_id)
        with self.store._tx() as conn:
            conn.execute("UPDATE tasks SET status='succeeded'")
        with self.assertRaisesRegex(ArtifactError, 'active leases'):
            export_bundle(self.config, self.root/'blocked')

    def test_v1_layout_is_read_and_imported_with_empty_extensions(self):
        self.complete()
        source = ledger(self.config.db_path)
        bundle = self.root/'v1'
        bundle.mkdir()
        files = {}
        for table in V1_TABLES:
            blob = b''.join(receipt_bytes(row)+b'\n' for row in source[table])
            (bundle/(table+'.jsonl')).write_bytes(blob)
            files[table+'.jsonl'] = sha256_ref(blob)
        artifacts = sorted({row['artifact_hash'] for row in source['evidence']})
        for ref in artifacts:
            ArtifactStore(bundle/'artifacts').put(self.artifacts.get(ref))
        (bundle/'manifest.json').write_bytes(receipt_bytes({'schema': V1_FORMAT, 'files': files, 'artifacts': artifacts}))
        manifest, records, _ = read_bundle(bundle)
        self.assertEqual(manifest['schema'], V1_FORMAT)
        self.assertEqual(records['reconciliations'], [])
        config, target = self.target()
        report = import_bundle(config, bundle, target)
        self.assertEqual(report['tasks'], 1)
        self.assertEqual(target.snapshot()['devices'], [])
        self.assertTrue(target.snapshot()['schema_migrations'])
        self.assertTrue(report['target_migration_metadata_retained'])

    def test_extension_only_target_refuses_import(self):
        self.complete()
        bundle = self.root/'bundle'
        export_bundle(self.config, bundle)
        config, target = self.target()
        target.register_capability('fixture', 'fixture@1', idempotency_proof='claimed')
        before = target.snapshot()
        with self.assertRaisesRegex(DispatchError, 'empty store'):
            import_bundle(config, bundle, target)
        self.assertEqual(before, target.snapshot())

    def test_unknown_populated_extension_fails_export_instead_of_dropping_rows(self):
        self.complete()
        with self.store._tx() as conn:
            conn.execute('CREATE TABLE future_extension(id TEXT)')
            conn.execute("INSERT INTO future_extension VALUES('synthetic')")
        with self.assertRaisesRegex(ArtifactError, 'future_extension'):
            export_bundle(self.config, self.root/'blocked')
        self.assertFalse((self.root/'blocked').exists())

    def test_invalid_late_extension_rolls_back_every_row_and_migration_metadata(self):
        self.complete()
        snapshot = self.store.snapshot()
        snapshot['transport_handoffs'] = [{'task_id': snapshot['tasks'][0]['task_id'], 'actor': 'fixture',
                                          'created_at': '2026-01-01', 'unexpected': 1}]
        _, target = self.target()
        before = target.snapshot()
        with self.assertRaisesRegex(DispatchError, 'columns'):
            target.import_snapshot(snapshot)
        self.assertEqual(target.snapshot(), before)

    def test_missing_proof_child_blocks_export(self):
        self.complete()
        proof = self.proof()
        self.artifacts.path(proof['proof']['state_after_first_ref']).unlink()
        with self.assertRaisesRegex(ArtifactError, 'missing'):
            export_bundle(self.config, self.root/'blocked')
        self.assertFalse((self.root/'blocked').exists())

    def test_local_only_projection_blocks_export(self):
        self.complete()
        UnifiedEventStore(self.config.db_path).append(source='fixture', source_id='one', trace_id='trace',
            actor='fixture', component='fixture', kind='hook.observed', data_class='local_only')
        with self.assertRaisesRegex(ArtifactError, 'local_only'):
            export_bundle(self.config, self.root/'blocked')

    def test_unlisted_extension_file_cannot_hide_in_v2_manifest(self):
        self.complete()
        bundle = self.root/'bundle'
        export_bundle(self.config, bundle)
        (bundle/'unlisted.jsonl').write_text('{}\n', encoding='utf-8')
        with self.assertRaises(ArtifactError):
            verify_bundle(bundle)

    def test_reconciliation_table_tamper_is_detected(self):
        self.reconciled()
        bundle = self.root/'bundle'
        export_bundle(self.config, bundle)
        (bundle/'reconciliations.jsonl').write_bytes(b'{}\n')
        with self.assertRaisesRegex(ArtifactError, 'hash mismatch'):
            verify_bundle(bundle)


if __name__ == '__main__':
    unittest.main()
