"""Recovery authority and actual same-key component write regressions."""
from dataclasses import asdict, replace
import json
from pathlib import Path
import sys
import tempfile
import unittest

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import sha256_ref
from sa_dispatch.clock import FakeClock
from sa_dispatch.config import ComponentConfig, Config
from sa_dispatch.executor import Executor, receipt_bytes
from sa_dispatch.models import DispatchError, Evidence, FenceRejected, Outcome, TaskSpec
from sa_dispatch.runner import ComponentRunner
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch_hotspot import KINDS


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.clock = FakeClock()
        fake = Path(__file__).parent / 'fakes/fake_component.py'
        self.config = Config(self.root / 'queue.db', self.root / 'artifacts', self.root / 'journal',
                             'fixture', ('component:mgmtcore',), 10, 5, ('hotspot',),
                             {'mgmtcore': ComponentConfig((sys.executable, str(fake.resolve()),
                              '--data-dir', str(self.root / 'fake')), 'any')}, True)
        self.store = SqliteStore(self.config.db_path, clock=self.clock, lease_ttl_seconds=10,
                                 kinds=KINDS, component_config=self.config)
        self.store.register_device('fixture', self.config.capabilities)
        self.runner = ComponentRunner(self.config)
        self.executor = Executor(self.config, self.store, self.runner, KINDS)
        self.artifacts = ArtifactStore(self.config.artifact_root)
        self.spec = TaskSpec('stable-write', 'hotspot.topic_adopt', 1,
                            {'project_id': 'synthetic-project', 'title': 'Synthetic', 'body': 'Fixture',
                             'author': 'Fixture', 'evidence_refs': [
                                 {'owner': 'fixture', 'kind': 'topic', 'id': 'one', 'version': '1'}]},
                            (), self.config.capabilities, 'synthetic', 'none', 3)
        self.capability = 'hotspot.topic_adopt@1'

    def unknown(self):
        task, _ = self.store.submit(self.spec)
        lease = self.store.claim('fixture')
        self.clock.advance(11)
        self.store.sweep()
        return self.store.get_task(task.task_id), lease

    def retry(self, task):
        self.store.resolve_unknown(task.task_id, 'not_done', 'fixture-reconciler',
                                   component='mgmtcore', capability=self.capability,
                                   expected_fence=task.fence, artifacts=self.artifacts)

    def test_unverified_write_retry_is_rejected_without_state_change(self):
        task, _ = self.unknown()
        self.store.register_capability('mgmtcore', self.capability, idempotency_proof='claimed')
        with self.assertRaisesRegex(DispatchError, 'verified capability'):
            self.retry(task)
        self.assertEqual(self.store.get_task(task.task_id).status, 'unknown')
        self.assertEqual(self.store.reconciliation_history(task.task_id), [])

    def test_verified_proof_comes_from_two_actual_subprocess_writes(self):
        task, _ = self.unknown()
        request = KINDS[task.kind].request(task)
        first = self.runner.run_component('mgmtcore', request)
        first_state = json.loads((self.root / 'fake/fake-state.json').read_bytes())
        second = self.runner.run_component('mgmtcore', request)
        second_state = json.loads((self.root / 'fake/fake-state.json').read_bytes())
        self.assertEqual((first.exit, second.exit), (0, 0))
        self.assertEqual(len(first_state['contents']), 1)
        self.assertEqual(first_state, second_state)
        from sa_dispatch.component_proof import prove_idempotency
        proof_ref = prove_idempotency(self.config, 'mgmtcore', KINDS[self.spec.kind],
                                      artifacts=self.artifacts)['proof_ref']
        self.store.register_capability('mgmtcore', self.capability, lookup_by_write_key=True,
                                       idempotency_proof='verified', proof_ref=proof_ref,
                                       artifacts=self.artifacts)
        reopened_without_registry = SqliteStore(self.config.db_path, lease_ttl_seconds=10)
        with self.assertRaisesRegex(DispatchError, 'registered task kind'):
            reopened_without_registry.resolve_unknown(task.task_id, 'not_done', 'fixture',
                component='mgmtcore', capability=self.capability, artifacts=self.artifacts,
                expected_fence=task.fence)
        self.retry(task)
        after = self.executor.run_once()
        self.assertEqual(after.status, 'succeeded')
        self.assertEqual(after.idem_key, task.idem_key)
        self.assertGreater(after.fence, task.fence)
        final_state = json.loads((self.root / 'fake/fake-state.json').read_bytes())
        self.assertEqual(len(final_state['contents']), 1)
        self.assertEqual(self.store.reconciliation_history(task.task_id)[0]['evidence_ref'], proof_ref)
        self.assertEqual(self.store.reconciliation_history(task.task_id)[0]['result'], 'decided')

    def test_incorrect_or_corrupt_proof_is_rejected(self):
        proof_ref = self.artifacts.put(b'{}')
        with self.assertRaises(DispatchError):
            self.store.register_capability('mgmtcore', self.capability,
                                           idempotency_proof='verified', proof_ref=proof_ref,
                                           artifacts=self.artifacts)
        self.artifacts.path(proof_ref).write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError, 'match'):
            self.store.register_capability('mgmtcore', self.capability,
                                           idempotency_proof='verified', proof_ref=proof_ref,
                                           artifacts=self.artifacts)

    def test_external_cannot_retry_and_manual_failure_requires_evidence(self):
        store = SqliteStore(self.root / 'external.db', clock=self.clock, lease_ttl_seconds=10)
        store.register_device('fixture', self.config.capabilities)
        task, _ = store.submit(replace(self.spec, kind='fixture.publish'), side_effect='external')
        store.claim('fixture')
        self.clock.advance(11)
        store.sweep()
        for manual in (False, True):
            with self.assertRaises(DispatchError):
                store.resolve_unknown(task.task_id, 'not_done', 'fixture', manual=manual)
        with self.assertRaises(DispatchError):
            store.resolve_unknown(task.task_id, 'confirmed_failed', 'fixture', manual=True)
        decision = self.artifacts.put(receipt_bytes({'synthetic': True, 'decision': 'failed',
                                                     'task_id': task.task_id, 'fence': 1,
                                                     'reason': 'fixture-lost-receipt'}))
        store.resolve_unknown(task.task_id, 'confirmed_failed', 'fixture-reviewer', manual=True,
                              evidence_ref=decision, artifacts=self.artifacts, expected_fence=1)
        self.assertEqual(store.get_task(task.task_id).status, 'failed')
        self.assertIsNone(store.claim('fixture'))
        self.assertEqual(store.reconciliation_history(task.task_id)[0]['method'], 'human')

    def test_stale_attempt_cannot_resolve_and_history_survives_reopen(self):
        task, _ = self.unknown()
        with self.assertRaises(FenceRejected):
            self.store.resolve_unknown(task.task_id, 'still_unknown', 'fixture', expected_fence=0)
        self.store.resolve_unknown(task.task_id, 'still_unknown', 'fixture', expected_fence=task.fence)
        reopened = SqliteStore(self.config.db_path, lease_ttl_seconds=10)
        self.assertEqual(len(reopened.reconciliation_history(task.task_id)), 1)
        self.assertEqual(reopened.get_task(task.task_id).status, 'unknown')

    def test_completion_requires_readable_task_attempt_and_outcome_bound_receipt(self):
        task, _ = self.unknown()
        raw = self.artifacts.put(b'{"synthetic_lookup":"confirmed"}')
        outcome = Outcome(evidence=(Evidence('lookup', raw, 'synthetic', artifact_hash=raw),))
        with self.assertRaisesRegex(DispatchError, 'attempt fence'):
            self.store.resolve_unknown(task.task_id, 'done', 'fixture', outcome=outcome)
        with self.assertRaises(ValueError):
            self.store.resolve_unknown(task.task_id, 'done', 'fixture', outcome=outcome,
                expected_fence=task.fence, artifacts=self.artifacts, evidence_ref='sha256:'+'0'*64)
        receipt = {'schema': 'sa_dispatch.reconciliation.v1', 'task_id': task.task_id,
                   'fence': task.fence, 'idem_key': task.idem_key, 'component': 'mgmtcore',
                   'outcome_hash': sha256_ref(receipt_bytes(asdict(outcome)))}
        for field, value in [('task_id', 'other'), ('fence', 100), ('idem_key', 'other'),
                             ('component', 'other'), ('outcome_hash', 'sha256:'+'0'*64)]:
            wrong = self.artifacts.put(receipt_bytes({**receipt, field: value}))
            with self.subTest(field=field), self.assertRaises(DispatchError):
                self.store.resolve_unknown(task.task_id, 'done', 'fixture', outcome=outcome,
                    expected_fence=task.fence, artifacts=self.artifacts, evidence_ref=wrong)
            self.assertEqual(self.store.get_task(task.task_id).status, 'unknown')
        valid = self.artifacts.put(receipt_bytes(receipt))
        self.store.resolve_unknown(task.task_id, 'done', 'fixture', outcome=outcome,
            expected_fence=task.fence, artifacts=self.artifacts, evidence_ref=valid)
        self.assertEqual(self.store.get_task(task.task_id).status, 'succeeded')


if __name__ == '__main__':
    unittest.main()
