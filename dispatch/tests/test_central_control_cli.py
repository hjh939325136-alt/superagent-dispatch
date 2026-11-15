"""Actual central CLI processes against a mixed ledger and both task routes."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import canonical_json
from sa_dispatch.central_control import AUTHORITY, AUTHORIZATION_SCHEMA, RECEIPT_SCHEMA, CentralControl, handoff_offer_hash
from sa_dispatch.bundle import export_bundle, import_bundle, verify_bundle
from sa_dispatch.central_control_cli import REQUEST_SCHEMA
from sa_dispatch.control_policy import ControlPolicy
from sa_dispatch.models import TaskSpec
from sa_dispatch.routing import HTTP_ROUTE, LOCAL_ROUTE, activate_mixed, activation_manifest
from sa_dispatch.store_sqlite import SqliteStore


class FixtureKind:
    name = 'fixture.central'
    version = 1
    enabled = True
    side_effect = 'idempotent_write'
    validate = staticmethod(dict)
    scope = staticmethod(lambda params: {})


class CentralControlCliTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.path = self.root / 'ledger.sqlite3'
        self.cas = ArtifactStore(self.root / 'artifacts')
        now = datetime.now(timezone.utc)
        self.start, self.end = (now - timedelta(days=1)).isoformat(), (now + timedelta(days=1)).isoformat()
        isolated = self.open()
        ControlPolicy(isolated, artifacts=self.cas)
        with closing(isolated._connect()) as conn:
            manifest = activation_manifest(conn, actor='fixture', bindings={})
        self.anchor = self.cas.put(canonical_json(manifest))
        activate_mixed(self.path, actor='fixture', evidence_ref=self.anchor, bindings={}, artifacts=self.cas)
        self.local, self.http = self.open(LOCAL_ROUTE), self.open(HTTP_ROUTE)
        self.request_number = 0

    def open(self, route=None):
        return SqliteStore(self.path, lease_ttl_seconds=30, kinds={FixtureKind.name: FixtureKind()}, route=route)

    def cli(self, operation, params, *, ok=True, opt_in=True, anchor=None, module='central_control_cli'):
        self.request_number += 1
        path = self.root / ('request-%s.json' % self.request_number)
        schema = REQUEST_SCHEMA if module == 'central_control_cli' else 'sa_dispatch.control_request.v1'
        path.write_text(json.dumps(dict(schema=schema, synthetic=True, operation=operation, params=params)), encoding='utf-8')
        command = [sys.executable, '-B', '-m', 'sa_dispatch.' + module,
                   '--ledger', str(self.path), '--artifacts', str(self.cas.root), '--request', str(path)]
        if module == 'central_control_cli':
            command += ['--expected-activation-ref', anchor or self.anchor]
            if opt_in:
                command += ['--central-maintenance']
        else:
            command += ['--route', LOCAL_ROUTE]
        result = subprocess.run(command, cwd=Path(__file__).resolve().parents[1],
                                capture_output=True, text=True, encoding='utf-8', timeout=30)
        self.assertEqual(result.returncode == 0, ok, result.stdout + result.stderr)
        response = json.loads(result.stdout)
        self.assertEqual(response['ok'], ok, response)
        self.assertEqual(response['messages_sent'], 0)
        return response.get('result') if ok else response

    def budget(self, limit=10, expected=None):
        return dict(budget_id='shared', period='fixture', unit='units', limit_units=limit,
                    starts_at=self.start, ends_at=self.end, expected_config_hash=expected)

    def test_budget_created_after_activation_covers_both_routes_and_preserves_live_holds(self):
        created = self.cli('budget_upsert', self.budget())
        routes = []
        for store, name, reserve in ((self.local, 'local', 3), (self.http, 'http', 4)):
            task = store.submit(TaskSpec(name, FixtureKind.name, 1, {'name': name}, (), ('cpu',),
                                         'synthetic', 'none', 2),
                                control_rule=dict(budget_id='shared', reserve_units=reserve))[0]
            if store is self.http:
                # Fixture for an already authorized transport handoff; no network server is started.
                with store._tx(execution=True) as conn:
                    conn.execute('INSERT INTO transport_handoffs VALUES(?,?,?)', (task.task_id, 'fixture', store._now()))
            store.register_device(name, ['cpu'])
            routes.append((store, name, task))
        with ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(store.claim, name) for store, name, _ in routes]
            leases = [future.result(timeout=10) for future in futures]
        self.assertEqual({lease.task_id for lease in leases}, {task.task_id for _, _, task in routes})
        before = self.path.read_bytes()
        status = self.cli('status', {})
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(status['budget_totals_scope'], 'ledger')
        self.assertEqual(status['budgets'][0]['reserved_units'], 7)
        self.assertEqual(len(status['reservations']), 2)
        self.cli('budget_upsert', self.budget(6, created['config_hash']), ok=False)
        self.assertEqual(self.path.read_bytes(), before)
        updated = self.cli('budget_upsert', self.budget(12, created['config_hash']))
        self.assertEqual((updated['reserved_units'], updated['used_units']), (7, 0))
        before = self.path.read_bytes()
        self.cli('budget_upsert', self.budget(13, created['config_hash']), ok=False)
        self.assertEqual(self.path.read_bytes(), before)
        store, name, task = routes[0]
        policy = ControlPolicy(store, artifacts=self.cas, authority='route_task')
        lease = leases[0]
        policy.mark_uncertain(task.task_id, lease.fence, actor='fixture')
        store.mark_unknown(lease.lease_id, lease.fence, 'synthetic lost acknowledgement', name)
        proof = self.cas.put(canonical_json(dict(synthetic=True, task_id=task.task_id,
            fingerprint=task.fingerprint, fence=lease.fence, budget_id='shared', actual_units=2, final_usage=True)))
        policy.settle(task.task_id, lease.fence, actual_units=2, evidence_ref=proof, actor='fixture')
        budget = self.cli('status', {})['budgets'][0]
        self.assertEqual((budget['reserved_units'], budget['used_units']), (4, 2))
        self.assertEqual(store.get_task(task.task_id).status, 'unknown')

    def test_handoff_proofs_round_trip_and_cannot_be_replaced(self):
        offer = dict(handoff_id='example', from_conv='sender', to_conv='recipient', project_id='fixture',
                     scope='read_status', scope_refs=[], expires_at=self.end)
        offer_hash = handoff_offer_hash(**offer)
        authorization = self.cas.put(canonical_json(dict(schema=AUTHORIZATION_SCHEMA, synthetic=True,
                                                         offer_hash=offer_hash, authorized_by='fixture-user')))
        params = dict(offer, actor='sender', authorization_ref=authorization)
        self.assertEqual(self.cli('handoff_offer', params)['state'], 'offered')
        def receipt(decision, **extra):
            return self.cas.put(canonical_json(dict(schema=RECEIPT_SCHEMA, synthetic=True,
                offer_hash=offer_hash, recipient='recipient', decision=decision, **extra)))
        accept = dict(handoff_id='example', state='accepted', actor='recipient', receipt_ref=receipt('accepted'))
        before = self.path.read_bytes()
        self.cli('handoff_transition', dict(accept, actor='other'), ok=False)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.cli('handoff_transition', accept)['state'], 'accepted')
        self.assertEqual(self.cli('handoff_offer', params)['state'], 'accepted')
        output = self.cas.put(b'synthetic result')
        complete = dict(handoff_id='example', state='completed', actor='recipient',
                        receipt_ref=receipt('completed', evidence_refs=[output]))
        self.assertEqual(self.cli('handoff_transition', complete)['state'], 'completed')
        before = self.path.read_bytes()
        self.cli('handoff_transition', complete)
        self.cli('handoff_offer', params)
        self.assertEqual(self.path.read_bytes(), before)
        replacement = self.cas.put(b'different synthetic result')
        self.cli('handoff_transition', dict(complete, receipt_ref=receipt('completed', evidence_refs=[replacement])), ok=False)
        self.cli('handoff_offer', dict(params, scope='takeover'), ok=False)
        self.assertEqual(self.path.read_bytes(), before)
        # The supported terminal bundle must retain the new proofs and outputs.
        package = self.root / 'completed-package'
        export_bundle(SimpleNamespace(db_path=self.path, artifact_root=self.cas.root), package)
        verify_bundle(package)
        target = self.root / 'restored.sqlite3'
        target_cas = self.root / 'restored-artifacts'
        restored = SqliteStore(target, lease_ttl_seconds=30, kinds={FixtureKind.name: FixtureKind()})
        import_bundle(SimpleNamespace(artifact_root=target_cas), package, restored)
        self.path.rename(self.root / 'original-ledger-unavailable')
        self.cas.root.rename(self.root / 'original-cas-unavailable')
        control = CentralControl(target, target_cas, self.anchor, authority=AUTHORITY)
        result, = control.status()['handoffs']
        self.assertEqual((result['state'], result['offer_hash']), ('completed', offer_hash))
        self.assertEqual(result['receipt_ref'], complete['receipt_ref'])
        self.assertEqual(ArtifactStore(target_cas).get(output), b'synthetic result')

    def test_cli_authority_anchor_and_operation_boundaries_are_explicit(self):
        before = self.path.read_bytes()
        self.cli('status', {}, opt_in=False, ok=False)
        self.cli('status', {}, anchor='sha256:' + '0' * 64, ok=False)
        for params in ({'authority': 'trusted_local_synthetic_central'}, {'route': LOCAL_ROUTE}, {'ledger': str(self.path)}):
            self.cli('status', params, ok=False)
        for operation in ('claim', 'bind_task', 'approval_decide', 'settle'):
            self.cli(operation, {}, ok=False)
        ordinary = self.budget()
        ordinary.pop('expected_config_hash')
        self.cli('budget_upsert', ordinary, module='control_cli', ok=False)
        self.assertEqual(self.path.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
