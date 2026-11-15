"""Normal research/content CLI and real loopback HTTP on one temporary ledger."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
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
from knowledge.m0_content.run_research_pipeline_lab import Commands, now, prepare
from knowledge.m0_content.workflow_binding import request_base
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.routing import activate_mixed, activation_manifest
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.transport import CoordinatorAPI, create_server
from sa_dispatch_hotspot import KINDS


class MixedResearchCliTests(unittest.TestCase):
    def test_research_to_script_with_concurrent_http_and_unknown_held(self):
        with tempfile.TemporaryDirectory() as folder:
            work = Path(folder)
            binding, config = prepare(work)
            cas = ArtifactStore(work / 'artifacts')
            isolated = SqliteStore(work / 'ledger.sqlite3', lease_ttl_seconds=30)
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
                    # The implementation gate must hold an ambiguous write, not launch it again.
                    ambiguous = local.submit('content.script_from_context', common | {'template_id': binding.template_id},
                                             'lost-ack', parent=assembled['task_id'], refs=selected)
                    local.execute(ambiguous, expected='unknown', crash=True)
                    self.assertIsNone(local.cli('run-once')['task'])
                    self.assertEqual(local.cli('reconcile', ambiguous['task_id'])['verdict'], 'still_unknown')
                    held = http.get_task(ambiguous['task_id'])
                    self.assertEqual((held.status, held.attempts, held.execution_route),
                                     ('unknown', 1, 'local_synthetic'))
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
