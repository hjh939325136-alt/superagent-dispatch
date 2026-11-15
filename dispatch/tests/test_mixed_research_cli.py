"""Normal research/content CLI and real loopback HTTP on one temporary ledger."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import http.client as http_client
import json
from pathlib import Path
import runpy
import sys
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from knowledge.m0_content.run_research_pipeline_lab import Commands, counts, now, prepare
from knowledge.m0_content.workflow_binding import request_base, search_modules
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.control_policy import ControlPolicy
from sa_dispatch.routing import activate_mixed, activation_manifest
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.transport import CoordinatorAPI, create_server
from sa_dispatch_hotspot import KINDS


class MixedResearchCliTests(unittest.TestCase):
    def test_research_approval_with_concurrent_http_and_historical_recovery(self):
        with tempfile.TemporaryDirectory() as folder:
            work = Path(folder)
            binding, config = prepare(work)
            cas = ArtifactStore(work / 'artifacts')
            isolated = SqliteStore(work / 'ledger.sqlite3', lease_ttl_seconds=30)
            policy = ControlPolicy(isolated, artifacts=cas)
            expiry = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
            policy.budget_upsert('fixture-budget', period='fixture', unit='synthetic_units',
                limit_units=10, starts_at=(datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
                ends_at=expiry)
            with closing(isolated._connect()) as conn:
                manifest = activation_manifest(conn, actor='synthetic-integration', bindings={})
            evidence = cas.put(receipt_bytes(manifest))
            activate_mixed(isolated.path, actor='synthetic-integration', evidence_ref=evidence,
                           bindings={}, artifacts=cas)
            config.write_text('execution_route = "local_synthetic"\n' + config.read_text(encoding='utf-8'),
                              encoding='utf-8')
            local = Commands(work, config)
            local.cli('device-register')
            http = SqliteStore(isolated.path, lease_ttl_seconds=30, kinds=KINDS, route='http_synthetic')
            http.register_device('http-worker', ['component:mgmtcore'])
            tokens = {k: k + '-synthetic-' + 'x' * 40 for k in ('dispatcher', 'http-worker')}
            api = CoordinatorAPI(http, {k: hashlib.sha256(v.encode()).hexdigest() for k, v in tokens.items()},
                                 synthetic_handoff=True, roles={'dispatcher': {'dispatcher'},
                                 'http-worker': {'worker'}}, artifact_store=cas)
            smoke = runpy.run_path(str(ROOT / 'dispatch/deploy/smoke-coordinator.py'))
            with create_server(api) as server:
                worker = threading.Thread(target=server.serve_forever, daemon=True)
                worker.start()
                def call(path, data):
                    identity = 'dispatcher' if path == '/submit' else 'http-worker'
                    conn = http_client.HTTPConnection('127.0.0.1', server.server_port, timeout=15)
                    try:
                        conn.request('POST', path, json.dumps(data), headers={
                            'Content-Type': 'application/json', 'Authorization': 'Bearer ' + tokens[identity]})
                        reply = conn.getresponse()
                        body = json.loads(reply.read())
                        if reply.status != 200 or not body.get('ok'):
                            raise AssertionError(body)
                        return body['data']
                    finally:
                        conn.close()
                try:
                    base = request_base(now())
                    search = local.submit('research.search', base | {'q': '剪辑', 'limit': 20}, 'search')
                    remote = smoke['submit_task'](call, 'http-concurrent')
                    with ThreadPoolExecutor(2) as pool:
                        found_future = pool.submit(local.execute, search)
                        http_future = pool.submit(smoke['complete_task'], call, 'http-concurrent', remote['task_id'])
                        found, receipt = found_future.result(), http_future.result()
                    self.assertEqual(receipt['attempts'], 1)
                    selected = [next(r['ref'] for r in found['results'] if
                                     r['ref'].startswith(domain + '/') and r['status'] == 'current')
                                for domain in base['domains']]
                    workflow = {'workflow_binding': binding.digest}
                    assembled = local.submit('context.assemble', workflow | base | {'refs': selected},
                                             'context', parent=search['task_id'], refs=selected)
                    context = local.execute(assembled)
                    self.assertEqual(sorted(context['refs']), sorted(selected))
                    common = workflow | {'trace_id': 'synthetic-content-game',
                              'context_id': context['context_id'], 'manifest_sha256': context['manifest_sha256']}
                    script_task = local.submit('content.script_from_context', common | {'template_id': binding.template_id},
                                               'script', parent=assembled['task_id'], refs=selected)
                    script = local.execute(script_task)
                    self.assertEqual(script['generation'], 'template_synthetic')
                    self.assertIs(script['ai_generated'], False)
                    # Normal CLI approvals and final usage stay bound to this task and route.
                    approval_task = local.submit('content.approval_record', common | {'script_ref': script['script_ref']},
                                                 'approval', parent=script_task['task_id'], refs=[script['script_ref']])
                    local.control('bind_task', {'task_id': approval_task['task_id'], 'approval_scope': 'script_ready',
                                               'budget_id': 'fixture-budget', 'reserve_units': 2})
                    self.assertIsNone(local.cli('run-once')['task'])
                    proof = {'schema': 'm0.synthetic_operator_approval.v1', 'synthetic': True,
                             'task_id': approval_task['task_id'], 'fingerprint': approval_task['fingerprint'],
                             'scope': 'script_ready', 'decision': 'approved', 'actor': 'synthetic-operator',
                             'expires_at': expiry, 'human_reviewed': False, 'publish_authorized': False}
                    approval_ref = cas.put(receipt_bytes(proof))
                    local.control('approval_decide', {'task_id': approval_task['task_id'], 'scope': 'script_ready',
                                  'decision': 'approved', 'actor': 'synthetic-operator',
                                  'evidence_ref': approval_ref, 'expires_at': expiry})
                    local.execute(approval_task, expected='unknown', crash=True)
                    def settle(task, actual_units):
                        usage_ref = cas.put(receipt_bytes({'synthetic': True, 'task_id': task['task_id'],
                            'fence': 1, 'budget_id': 'fixture-budget', 'fingerprint': task['fingerprint'],
                            'actual_units': actual_units, 'final_usage': True}))
                        return local.control('settle', {'task_id': task['task_id'], 'fence': 1,
                            'actual_units': actual_units, 'evidence_ref': usage_ref, 'actor': 'synthetic-operator'})
                    settle(approval_task, 2)
                    self.assertEqual(http.get_task(approval_task['task_id']).status, 'unknown')
                    # The implementation gate must hold an ambiguous write, not launch it again.
                    ambiguous = local.submit('content.script_from_context', common | {'template_id': binding.template_id},
                                             'lost-ack', parent=assembled['task_id'], refs=selected)
                    local.control('bind_task', {'task_id': ambiguous['task_id'],
                        'budget_id': 'fixture-budget', 'reserve_units': 1})
                    local.execute(ambiguous, expected='unknown', crash=True)
                    self.assertIsNone(local.cli('run-once')['task'])
                    held = http.get_task(ambiguous['task_id'])
                    self.assertEqual((held.status, held.attempts, held.execution_route),
                                     ('unknown', 1, 'local_synthetic'))
                    settle(ambiguous, 1)
                    self.assertEqual(http.get_task(ambiguous['task_id']).status, 'unknown')
                    status = local.control('status', {})
                    self.assertFalse(status['status_refreshes_handoff_expiry'])
                    self.assertEqual(status['result']['budget_totals_scope'], 'ledger')
                    self.assertEqual([(b['used_units'], b['reserved_units']) for b in status['result']['budgets']], [(3, 0)])
                    # Revocation and a newer denial must not rewrite the original invocation.
                    _, catalog, _, _ = search_modules()
                    catalog.Catalog(binding.research.catalog_root).apply_batch(
                        'research-scatter', 'mixed-revoke', 'revoke_locked', operator_grant=catalog.OPERATOR)
                    denied_ref = cas.put(receipt_bytes(proof | {'decision': 'denied'}))
                    local.control('approval_decide', {'task_id': approval_task['task_id'], 'scope': 'script_ready',
                                  'decision': 'denied', 'actor': 'synthetic-operator',
                                  'evidence_ref': denied_ref, 'expires_at': expiry})
                    before_lookup = counts(binding.content_path,
                        ['artifacts', 'approvals', 'content_events', 'operation_receipts'])
                    journal = work / 'journal'
                    hidden_journal = work / 'journal-unavailable'
                    self.assertTrue(journal.resolve().is_relative_to(work.resolve()))
                    self.assertTrue(hidden_journal.resolve().is_relative_to(work.resolve()))
                    journal.rename(hidden_journal)
                    for task in (approval_task, ambiguous):
                        reconciled = local.cli('reconcile', task['task_id'])
                        self.assertEqual(reconciled['verdict'], 'confirmed_done')
                        self.assertIs(reconciled['current_usable'], False)
                        saved = http.get_task(task['task_id'])
                        self.assertEqual((saved.status, saved.attempts), ('succeeded', 1))
                    self.assertEqual(counts(binding.content_path, before_lookup), before_lookup)
                    approved = local.receipt(approval_task)
                    self.assertEqual(approved['control_approval']['proof_hash'], approval_ref)
                    status_task = local.submit('content.approval_status', common | {
                        'script_ref': script['script_ref'], 'approval_ref': approved['approval_ref']}, 'revoked-status')
                    self.assertIs(local.execute(status_task)['usable'], False)
                    with closing(http._connect()) as conn:
                        misplaced = conn.execute('''SELECT COUNT(*) FROM leases l JOIN tasks t USING(task_id)
                            WHERE (l.device_id='http-worker') != (t.execution_route='http_synthetic')''').fetchone()[0]
                    self.assertEqual(misplaced, 0)
                    self.assertEqual(http.get_task(remote['task_id']).status, 'succeeded')
                finally:
                    server.shutdown()
                    worker.join(timeout=5)
                self.assertFalse(worker.is_alive())


if __name__ == '__main__':
    unittest.main()
