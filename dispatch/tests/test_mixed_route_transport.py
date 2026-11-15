"""HTTP and local entries share a ledger without sharing execution authority."""
from contextlib import closing
from dataclasses import asdict
import hashlib
import http.client
import json
from pathlib import Path
import tempfile
import threading
import unittest

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.models import DispatchError, Outcome, TaskSpec
from sa_dispatch.routing import activate_mixed, activation_manifest
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.transport import CoordinatorAPI, TransportError, create_server


class Kind:
    name = 'shared.fixture'
    version = 1
    enabled = True
    side_effect = 'none'

    def validate(self, params):
        if params != {}:
            raise ValueError('fixture expects empty params')
        return params

    def scope(self, params):
        return {}


class MixedTransportTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.path = self.root / 'ledger.sqlite'
        self.artifacts = ArtifactStore(self.root / 'cas')
        isolated = SqliteStore(self.path, lease_ttl_seconds=60)
        with closing(isolated._connect()) as conn:
            manifest = activation_manifest(conn, actor='synthetic-test', bindings={})
        evidence = self.artifacts.put(receipt_bytes(manifest))
        activate_mixed(self.path, actor='synthetic-test', evidence_ref=evidence,
                       bindings={}, artifacts=self.artifacts)
        kinds = {Kind.name: Kind()}
        self.http = SqliteStore(self.path, lease_ttl_seconds=60, kinds=kinds, route='http_synthetic')
        self.local = SqliteStore(self.path, lease_ttl_seconds=60, kinds=kinds, route='local_synthetic')
        # Equal capabilities and identical kind deliberately remove accidental isolation.
        self.http.register_device('http-worker', ['cpu'])
        self.local.register_device('local-worker', ['cpu'])
        self.tokens = {name: name + '-' + 'x' * 40 for name in ('dispatcher', 'http-worker')}
        self.hashes = {k: hashlib.sha256(v.encode()).hexdigest() for k, v in self.tokens.items()}
        self.roles = {'dispatcher': {'dispatcher'}, 'http-worker': {'worker'}}
        self.api = CoordinatorAPI(self.http, self.hashes, synthetic_handoff=True,
                                  roles=self.roles, artifact_store=self.artifacts)

    def spec(self, key):
        return TaskSpec(key, Kind.name, 1, {}, (), ('cpu',), 'synthetic', 'none', 2)

    def call(self, path, payload, identity='http-worker'):
        return self.api.invoke(path, payload, 'Bearer ' + self.tokens[identity])

    def test_real_http_claim_ignores_local_task_without_handoff(self):
        local, _ = self.local.submit(self.spec('local'))
        remote = self.call('/submit', {'spec': asdict(self.spec('http')), 'inputs': {}}, 'dispatcher')
        with create_server(self.api) as server:
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                conn = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=5)
                try:
                    conn.request('POST', '/claim', '{}', headers={
                        'Authorization': 'Bearer ' + self.tokens['http-worker'],
                        'Content-Type': 'application/json'})
                    response = conn.getresponse()
                    data = json.loads(response.read())
                    self.assertEqual(response.status, 200, data)
                    lease = data['data']['lease']
                finally:
                    conn.close()
            finally:
                server.shutdown()
                worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
        self.assertEqual(lease['task_id'], remote['task_id'])
        self.assertEqual(self.local.get_task(local.task_id).status, 'queued')
        local_lease = self.local.claim('local-worker')
        self.assertEqual(local_lease.task_id, local.task_id)
        self.call('/complete', {'lease_id': lease['lease_id'], 'fence': lease['fence'],
                               'outcome': {'status': 'succeeded', 'result': {}, 'evidence': []}})
        self.local.complete(local_lease.lease_id, local_lease.fence, Outcome('succeeded', {}), 'local-worker')
        self.assertEqual([t.status for t in self.local.list_tasks()], ['succeeded', 'succeeded'])

    def test_http_crash_gap_still_blocks_and_same_route_replay_repairs(self):
        local, _ = self.local.submit(self.spec('local'))
        broken, _ = self.http.submit(self.spec('broken-http'))
        before = self.local.get_task(local.task_id).to_dict()
        with self.assertRaises(TransportError) as error:
            self.call('/claim', {})
        self.assertEqual(error.exception.code, 'E_HANDOFF_NOT_READY')
        self.http.sweep(worker_ids={'http-worker'})
        self.local.sweep(worker_ids={'local-worker'})
        self.assertEqual(self.local.get_task(local.task_id).to_dict(), before)
        repaired = self.call('/submit', {'spec': asdict(self.spec('broken-http')), 'inputs': {}}, 'dispatcher')
        self.assertTrue(repaired['deduplicated'])
        self.assertEqual(repaired['task_id'], broken.task_id)
        self.assertEqual(self.call('/claim', {})['lease']['task_id'], broken.task_id)

    def test_cross_route_authority_never_transfers(self):
        local, _ = self.local.submit(self.spec('same-key'))
        before = self.local.snapshot()
        with self.assertRaises(DispatchError):
            self.call('/submit', {'spec': asdict(self.spec('same-key')), 'inputs': {}}, 'dispatcher')
        self.assertEqual(self.local.snapshot(), before)
        lease = self.local.claim('local-worker')
        with self.assertRaises(TransportError):
            self.call('/task', {'lease_id': lease.lease_id, 'fence': lease.fence})
        with self.assertRaises(DispatchError):
            self.http.complete(lease.lease_id, lease.fence, Outcome('succeeded', {}), 'local-worker')
        with self.assertRaises(TransportError):
            CoordinatorAPI(self.local, self.hashes, synthetic_handoff=True,
                           roles=self.roles, artifact_store=self.artifacts)
        self.assertEqual(self.local.get_task(local.task_id).status, 'leased')


if __name__ == '__main__':
    unittest.main()
