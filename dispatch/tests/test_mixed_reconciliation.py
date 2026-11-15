"""Exact mixed invocation evidence, transaction-time refusal and v7 closure."""

from contextlib import closing
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from knowledge.m0_content.dispatch_entry import WorkflowKind
from sa_dispatch.artifacts import ArtifactError, ArtifactStore
from sa_dispatch.bundle import export_bundle, import_bundle, read_bundle, verify_bundle
from sa_dispatch.canonical import sha256_ref
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.config import ComponentConfig, Config
from sa_dispatch.control_policy import ControlPolicy
from sa_dispatch.executor import Executor
from sa_dispatch.invocation import (INTENT_KIND, lookup_document, receipt_bytes,
                                    reconciliation_document, workflow_outcome)
from sa_dispatch.models import DispatchError, Evidence, FenceRejected, Outcome, TaskSpec
from sa_dispatch.routing import LOCAL_ROUTE, activate_mixed, activation_manifest
from sa_dispatch.store_sqlite import SqliteStore


class MixedReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.clock = FakeClock(datetime.now(timezone.utc))
        self.config = Config(self.root / 'dispatch.sqlite', self.root / 'cas', self.root / 'journal',
                             'fixture-device', ('cpu',), 30, 5, ('research_content',),
                             {'research_content': ComponentConfig((sys.executable,), 'any')}, True, LOCAL_ROUTE)
        self.cas = ArtifactStore(self.config.artifact_root)
        self.binding = SimpleNamespace(digest=sha256_ref(b'fixture binding'),
                                       research=SimpleNamespace(instance='catalog-fixture'),
                                       content_instance='content-fixture', allowed_traces=('trace-fixture',),
                                       template_id='template-fixture')
        self.kinds = {name: WorkflowKind(name, self.binding)
                      for name in ('content.script_from_context', 'content.approval_record')}
        legacy = SqliteStore(self.config.db_path, lease_ttl_seconds=30, clock=self.clock)
        with closing(legacy._connect()) as conn:
            conn.execute('BEGIN')
            ref = self.put(activation_manifest(conn, actor='fixture', bindings={}))
        activate_mixed(legacy.path, actor='fixture', bindings={}, artifacts=self.cas, evidence_ref=ref,
                       clock=self.clock)
        self.store = self.open(self.config)
        self.store.register_device(self.config.device_id, self.config.capabilities)

    def open(self, config, kinds=None):
        return SqliteStore(config.db_path, lease_ttl_seconds=30, clock=self.clock,
                           kinds=self.kinds if kinds is None else kinds, route=config.execution_route,
                           component_config=config)

    def put(self, document, *, cas=None):
        return (cas or self.cas).put(receipt_bytes(document))

    def submit(self, *, approval=False, key='one'):
        params = {'workflow_binding': self.binding.digest, 'trace_id': self.binding.allowed_traces[0],
                  'context_id': 'ctx_' + 'a' * 26, 'manifest_sha256': 'b' * 64}
        if approval:
            kind = 'content.approval_record'
            params['script_ref'] = 'mgmt/script/fixture@1#' + 'c' * 64
        else:
            kind = 'content.script_from_context'
            params['template_id'] = self.binding.template_id
        return self.store.submit(TaskSpec(key, kind, 1, params, (), ('cpu',), 'synthetic', 'none', 1))[0]

    def decision(self, task, policy, decision, *, seconds):
        expiry = timestamp(self.clock.now() + timedelta(seconds=seconds))
        proof = {'schema': 'm0.synthetic_operator_approval.v1', 'synthetic': True,
                 'task_id': task.task_id, 'fingerprint': task.fingerprint, 'scope': 'script_ready',
                 'decision': decision, 'actor': 'synthetic-operator', 'expires_at': expiry,
                 'human_reviewed': False, 'publish_authorized': False}
        ref = self.put(proof)
        policy.approval_decide(task.task_id, scope='script_ready', decision=decision, actor='synthetic-operator',
                               evidence_ref=ref, expires_at=expiry)
        return ref

    def evidence(self, task, kind=INTENT_KIND):
        return [row for row in self.store.snapshot()['evidence']
                if row['task_id'] == task.task_id and row['kind'] == kind]

    def lost_ack(self, task, *, expire_lease=False):
        calls = []
        def lose(component, request):
            current = self.store.get_task(task.task_id)
            self.assertEqual(current.status, 'leased')
            anchor, = self.evidence(current)
            bound = json.loads(self.cas.get(anchor['artifact_hash']))
            self.assertEqual(self.cas.get(bound['request_ref']), receipt_bytes(request))
            self.assertEqual(component, bound['component'])
            calls.append(request)
            if expire_lease:
                self.clock.advance(31)
            raise RuntimeError('synthetic lost ACK')
        executor = Executor(self.config, self.store, SimpleNamespace(run_component=lose), self.kinds)
        result = executor.run_once()
        self.assertEqual(result.status, 'unknown')
        self.assertEqual(len(calls), 1)
        for path in self.config.journal_dir.glob('*.jsonl'):
            path.unlink()
        self.assertEqual(list(self.config.journal_dir.iterdir()), [])
        return result

    def lookup(self, task):
        anchor_ref, intent, original = self.store.original_invocation(task.task_id, task.fence, self.cas)
        request = lookup_document(task, intent)
        result = {**task.scope, 'synthetic_only': True, 'receipt_content': 'references_only', 'ref': 'fixture'}
        if task.kind == 'content.approval_record':
            result['control_approval'] = original['payload']['control_approval']
        response = {'schema_version': '0.1', 'request_id': task.task_id, 'ok': True, 'data': result}
        envelope = {'schema_version': '0.1', 'request_id': request['request_id'], 'ok': True,
                    'data': {'state': 'committed', **request['payload'], 'current_usable': False,
                             'usable_reason': 'source_revoked', 'response': response,
                             'response_hash': sha256_ref(receipt_bytes(response))}}
        return anchor_ref, intent, request, envelope

    def reconcile(self, task, change=None):
        _, _, expected, envelope = self.lookup(task)
        if change:
            change(envelope)
        def run(component, request):
            self.assertEqual(component, 'research_content')
            self.assertEqual(request, expected)
            return SimpleNamespace(exit=0, json=envelope)
        executor = Executor(self.config, self.store, SimpleNamespace(run_component=run), self.kinds)
        with patch.object(self.kinds[task.kind], 'request_with_context', side_effect=AssertionError('replayed write')):
            return executor.reconcile(task)

    def assert_unchanged(self, task, action, error=DispatchError):
        before = self.store.snapshot()
        with self.assertRaises(error):
            action()
        self.assertEqual(self.store.snapshot(), before)
        self.assertEqual(self.store.get_task(task.task_id).status, task.status)

    def test_anchored_expired_lease_resolves_without_journal_or_replay(self):
        task = self.lost_ack(self.submit(), expire_lease=True)
        self.assertEqual(self.store.list_leases(task.task_id)[0].state, 'expired')
        self.assertEqual(self.reconcile(task), {'verdict': 'confirmed_done', 'task_id': task.task_id,
                                              'current_usable': False, 'historical_commit': True})
        result = self.store.get_task(task.task_id)
        self.assertEqual((result.status, result.attempts), ('succeeded', 1))

    def test_foreign_missing_duplicate_and_malformed_anchors_fail_closed(self):
        task = self.lost_ack(self.submit())
        anchor, = self.evidence(task)
        old_ref = anchor['artifact_hash']
        old_body = self.cas.get(old_ref)
        original = json.loads(old_body)
        cases = {'foreign owner': self.put({**original, 'device_id': 'another-device'}),
                 'wrong task': self.put({**original, 'task_id': 'another-task'}),
                 'wrong route': self.put({**original, 'route': 'http_synthetic'}),
                 'wrong fence': self.put({**original, 'fence': True}),
                 'wrong lease': self.put({**original, 'lease_id': 'another-lease'}),
                 'wrong scope': self.put({**original, 'scope': {**task.scope, 'content_instance': 'other'}}),
                 'missing request': self.put({**original, 'request_ref': sha256_ref(b'not stored')}),
                 'noncanonical request': self.put({**original, 'request_ref': self.cas.put(b'{ "x": 0 }')})}
        runner = SimpleNamespace(run_component=lambda *_: self.fail('invalid anchor reached the component'))
        executor = Executor(self.config, self.store, runner, self.kinds)
        with self.store._tx(execution=True) as conn:
            conn.execute('DELETE FROM evidence WHERE evidence_id=?', (anchor['evidence_id'],))
        self.assertEqual(executor.reconcile(task), {'verdict': 'still_unknown'})
        for label, ref in cases.items():
            with self.subTest(label=label):
                with self.store._tx(execution=True) as conn:
                    conn.execute('INSERT INTO evidence VALUES(?,?,?,?,?,?,?,?,?)',
                                 tuple({**anchor, 'artifact_hash': ref, 'content_hash': ref}.values()))
                self.assertEqual(executor.reconcile(task), {'verdict': 'still_unknown'})
                with self.store._tx(execution=True) as conn:
                    conn.execute('DELETE FROM evidence WHERE evidence_id=?', (anchor['evidence_id'],))
        with self.store._tx(execution=True) as conn:
            conn.execute('INSERT INTO evidence VALUES(?,?,?,?,?,?,?,?,?)', tuple(anchor.values()))
            conn.execute('INSERT INTO evidence VALUES(?,?,?,?,?,?,?,?,?)', tuple({**anchor,
                'evidence_id': 'duplicate', 'artifact_hash': cases['wrong scope'], 'content_hash': cases['wrong scope']}.values()))
        self.assertEqual(executor.reconcile(task), {'verdict': 'still_unknown'})
        with self.store._tx(execution=True) as conn:
            conn.execute("DELETE FROM evidence WHERE evidence_id='duplicate'")
        self.cas.path(old_ref).write_bytes(b'corrupt bytes')
        self.assertEqual(executor.reconcile(task), {'verdict': 'still_unknown'})
        self.cas.path(old_ref).write_bytes(old_body)
        self.assertEqual(self.store.reconciliation_history(task.task_id), [])
        self.assertEqual(self.store.get_task(task.task_id).status, 'unknown')

    def test_transaction_rejects_forged_outcome_and_stale_or_foreign_resolution(self):
        task = self.lost_ack(self.submit())
        anchor_ref, intent, request, envelope = self.lookup(task)
        request_ref, lookup_ref = self.put(request), self.put(envelope)
        valid = workflow_outcome(task, envelope['data']['response'], lookup_ref)
        forged = replace(valid, result={**valid.result, 'ref': 'injected'})
        ref = self.put(reconciliation_document(anchor_ref, intent, request_ref, lookup_ref, False, valid))
        forged_ref = self.put(reconciliation_document(anchor_ref, intent, request_ref, lookup_ref, False, forged))
        writer = self.open(replace(self.config, device_id='other-device'))
        writer.register_device('other-device', ('cpu',))
        for store, outcome, evidence_ref, fence, verdict, manual in (
                (self.store, forged, ref, 1, 'confirmed_done', False),
                (self.store, forged, forged_ref, 1, 'confirmed_done', False),
                (self.store, valid, ref, True, 'confirmed_done', False),
                (self.store, valid, ref, 0, 'confirmed_done', False),
                (self.store, valid, ref, 1, 'done', False),
                (self.store, valid, ref, 1, 'confirmed_not_done', False),
                (self.store, valid, ref, 1, 'confirmed_done', True),
                (writer, valid, ref, 1, 'confirmed_done', False)):
            with self.subTest(fence=fence, verdict=verdict, manual=manual, forged=outcome is forged):
                self.assert_unchanged(task, lambda: store.resolve_unknown(task.task_id, verdict, 'fixture',
                    outcome=outcome, evidence_ref=evidence_ref, expected_fence=fence, manual=manual, artifacts=self.cas))
        self.assertEqual(self.reconcile(task)['verdict'], 'confirmed_done')

    def test_intent_is_idempotent_unique_and_reserved_before_shortcuts(self):
        task = self.submit()
        lease = self.store.claim(self.config.device_id)
        task = self.store.get_task(task.task_id)
        request = self.kinds[task.kind].request(task)
        ref = self.put(request)
        record = lambda content: self.store.record_invocation_intent(lease.lease_id, lease.fence,
                                                                     lease.device_id, content, self.cas)
        anchor_ref = record(ref)
        self.assertEqual(record(ref), anchor_ref)
        self.assertEqual(len(self.evidence(task)), 1)
        changed_ref = self.put({**request, 'payload': {**request['payload'], 'template_id': 'other'}})
        self.assert_unchanged(task, lambda: record(changed_ref))
        injected = Outcome(evidence=(Evidence(INTENT_KIND, anchor_ref, 'synthetic', artifact_hash=anchor_ref),))
        complete = lambda: self.store.complete(lease.lease_id, lease.fence, injected, lease.device_id)
        self.assert_unchanged(task, complete)
        self.store.mark_unknown(lease.lease_id, lease.fence, 'fixture', lease.device_id)
        task = self.store.get_task(task.task_id)
        self.assert_unchanged(task, complete)
        self.assert_unchanged(task, lambda: record(ref), error=FenceRejected)
        self.assert_unchanged(task, lambda: self.store.resolve_unknown(task.task_id, 'still_unknown', 'fixture',
                                                                       outcome=injected))
        with self.store._tx(execution=True) as conn:
            conn.execute("UPDATE leases SET state='completed',completion_json=? WHERE lease_id=?",
                         (receipt_bytes(asdict(injected)).decode(), lease.lease_id))
        self.assert_unchanged(task, complete)

    def test_historical_approval_expiry_and_replacement_deny_do_not_rewrite_request(self):
        task = self.submit(approval=True)
        policy = ControlPolicy(self.store, artifacts=self.cas, authority='route_task')
        policy.bind_task(task.task_id, approval_scope='script_ready')
        approved_ref = self.decision(task, policy, 'approved', seconds=30)
        task = self.lost_ack(task)
        self.clock.advance(31)
        self.decision(task, policy, 'denied', seconds=60)
        self.assertEqual(self.reconcile(task)['verdict'], 'confirmed_done')
        result = self.store.get_task(task.task_id).result
        self.assertEqual(result['control_approval']['proof_hash'], approved_ref)
        manifest = export_bundle(self.config, self.root / 'approval-bundle')
        self.assertIn(approved_ref, manifest['artifacts'])

    def test_executor_cannot_invoke_with_mismatched_component_or_configuration(self):
        task = self.submit()
        calls = []
        def run(*args):
            calls.append(args)
            self.fail('executor invoked a component with inconsistent authority')
        alternate = replace(self.config, components={
            'research_content': ComponentConfig((sys.executable, 'another-component'), 'any')})
        runner = SimpleNamespace(run_component=run)
        mismatched_runner = SimpleNamespace(run_component=run, config=alternate)
        self.assert_unchanged(task, Executor(alternate, self.store, runner, self.kinds).run_once)
        self.assert_unchanged(task, Executor(self.config, self.store, mismatched_runner, self.kinds).run_once)

        other_adapter = WorkflowKind(task.kind, self.binding)
        other_adapter.component = 'different-component'
        other_registry = {task.kind: other_adapter}
        executor = Executor(self.config, self.store, runner, other_registry)
        failed = executor.run_once()
        self.assertEqual((failed.status, failed.result['phase']), ('failed', 'pre_invocation'))

        task = self.lost_ack(self.submit(key='unknown'))
        before = self.store.snapshot()
        for config, registered_runner, kinds in ((alternate, runner, self.kinds),
                                                  (self.config, mismatched_runner, self.kinds),
                                                  (self.config, runner, other_registry)):
            executor = Executor(config, self.store, registered_runner, kinds)
            self.assertEqual(executor.reconcile(task), {'verdict': 'still_unknown'})
        self.assertEqual(self.store.snapshot(), before)
        self.assertEqual(calls, [])

    def test_intent_refuses_expiry_or_newer_denial_before_invocation(self):
        for reason in ('expiry', 'new denial'):
            with self.subTest(reason=reason):
                task = self.submit(approval=True, key=reason)
                policy = ControlPolicy(self.store, artifacts=self.cas, authority='route_task')
                policy.bind_task(task.task_id, approval_scope='script_ready')
                self.decision(task, policy, 'approved', seconds=15)
                lease = self.store.claim(self.config.device_id)
                task = self.store.get_task(task.task_id)
                executor = Executor(self.config, self.store, None, self.kinds)
                ref = self.put(executor._request(task))
                if reason == 'expiry':
                    self.clock.advance(16)
                else:
                    self.decision(task, policy, 'denied', seconds=15)
                self.assert_unchanged(task, lambda: self.store.record_invocation_intent(
                    lease.lease_id, lease.fence, lease.device_id, ref, self.cas))
                self.store.complete(lease.lease_id, lease.fence, Outcome('failed'), lease.device_id)

    def test_v7_round_trip_and_rejects_missing_duplicate_or_tampered_closure(self):
        task = self.lost_ack(self.submit())
        anchor_ref, intent, _, _ = self.lookup(task)
        self.assertEqual(self.reconcile(task)['verdict'], 'confirmed_done')
        source = self.root / 'bundle'
        manifest = export_bundle(self.config, source)
        _, records, _ = read_bundle(source)
        proof_ref = records['reconciliations'][0]['evidence_ref']
        proof = json.loads(self.cas.get(proof_ref))
        for ref in (anchor_ref, intent['request_ref'], proof['lookup_request_ref'], proof['lookup_ref'], proof_ref):
            self.assertIn(ref, manifest['artifacts'])
        restored = replace(self.config, db_path=self.root / 'restored.sqlite', artifact_root=self.root / 'restored-cas',
                           execution_route=None)
        target = self.open(restored)
        import_bundle(restored, source, target)
        self.assertEqual(target.get_task(task.task_id).result, self.store.get_task(task.task_id).result)
        self.assertTrue((target.routing_mode() == 'mixed') and not target.snapshot()['devices'])

        def rewrite(package, manifest, name, rows):
            blob = b''.join(receipt_bytes(row) + b'\n' for row in rows)
            (package / (name + '.jsonl')).write_bytes(blob)
            manifest['files'][name + '.jsonl'] = sha256_ref(blob)
            manifest['source_counts'][name] = len(rows)
            (package / 'manifest.json').write_bytes(receipt_bytes(manifest))

        for ref in (intent['request_ref'], proof['lookup_request_ref'], proof['lookup_ref']):
            with self.subTest(missing=ref):
                package = self.root / ('missing-' + ref[-8:])
                shutil.copytree(source, package)
                ArtifactStore(package / 'artifacts').path(ref).unlink()
                with self.assertRaises(ArtifactError):
                    verify_bundle(package)
        duplicate = self.root / 'duplicate'
        shutil.copytree(source, duplicate)
        duplicate_rows = records['evidence'] + [{**self.evidence(task)[0], 'evidence_id': 'duplicate'}]
        rewrite(duplicate, json.loads((duplicate / 'manifest.json').read_bytes()), 'evidence', duplicate_rows)
        with self.assertRaises(ArtifactError):
            verify_bundle(duplicate)

        malicious = self.root / 'body-injected'
        shutil.copytree(source, malicious)
        malicious_manifest = json.loads((malicious / 'manifest.json').read_bytes())
        malicious_cas = ArtifactStore(malicious / 'artifacts')
        forged_envelope = json.loads(self.cas.get(proof['lookup_ref']))
        forged_response = forged_envelope['data']['response']
        forged_response['data']['nested'] = [{'body': 'fixture forbidden content'}]
        forged_envelope['data']['response_hash'] = sha256_ref(receipt_bytes(forged_response))
        forged_lookup_ref = self.put(forged_envelope, cas=malicious_cas)
        forged_outcome = workflow_outcome(task, forged_response, forged_lookup_ref)
        forged_proof = self.put(reconciliation_document(anchor_ref, intent, proof['lookup_request_ref'],
                                                        forged_lookup_ref, False, forged_outcome), cas=malicious_cas)
        replacements = {proof_ref: forged_proof, proof['lookup_ref']: forged_lookup_ref}
        rewrite(malicious, malicious_manifest, 'reconciliations',
                [{**records['reconciliations'][0], 'evidence_ref': forged_proof}])
        rewrite(malicious, malicious_manifest, 'evidence', [
            {**row, **{key: replacements.get(row[key], row[key]) for key in ('content_hash', 'artifact_hash')}}
            for row in records['evidence']])
        rewrite(malicious, malicious_manifest, 'tasks',
                [{**records['tasks'][0], 'result_json': receipt_bytes(forged_outcome.result).decode()}])
        malicious_manifest['artifacts'] = sorted(replacements.get(ref, ref) for ref in malicious_manifest['artifacts'])
        (malicious / 'manifest.json').write_bytes(receipt_bytes(malicious_manifest))
        with self.assertRaises(ArtifactError):
            verify_bundle(malicious)

if __name__ == '__main__':
    unittest.main()
