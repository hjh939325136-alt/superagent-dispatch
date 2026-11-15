"""Route authority, shared accounting, and read-only control status."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import canonical_json
from sa_dispatch.clock import FakeClock
from sa_dispatch.control_cli import REQUEST_SCHEMA
from sa_dispatch.control_policy import ControlPolicy
from sa_dispatch.models import DispatchError, TaskSpec
from sa_dispatch.routing import HTTP_ROUTE, LOCAL_ROUTE, RouteRejected, activate_mixed, activation_manifest
from sa_dispatch.store_sqlite import SqliteStore


class FixtureKind:
    name = 'fixture.policy'
    version = 1
    enabled = True
    side_effect = 'idempotent_write'
    validate = staticmethod(dict)
    scope = staticmethod(lambda params: {})


class MixedControlPolicyTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.path = self.root / 'ledger.sqlite3'
        self.clock = FakeClock()
        self.cas = ArtifactStore(self.root / 'artifacts')
        self.expiry = '2026-01-02T00:00:00Z'
        isolated = self.open()
        self.stale = ControlPolicy(isolated, artifacts=self.cas)
        for budget_id in ('shared', 'remote-only', 'unlinked'):
            self.stale.budget_upsert(budget_id, period='fixture', unit='units', limit_units=10,
                                     starts_at='2026-01-01T00:00:00Z', ends_at=self.expiry)
        self.stale.handoff_offer('historical', from_conv='sender', to_conv='receiver', project_id='fixture',
                                 scope='read_status', expires_at=self.expiry, actor='sender')
        self.historical = self.submit(isolated, 'historical')
        isolated.cancel(self.historical.task_id, 'operator')
        with closing(isolated._connect()) as conn:
            manifest = activation_manifest(conn, actor='fixture', bindings={})
        activate_mixed(self.path, actor='fixture', evidence_ref=self.cas.put(canonical_json(manifest)),
                       bindings={}, artifacts=self.cas, clock=self.clock)
        self.local, self.http = self.open(LOCAL_ROUTE), self.open(HTTP_ROUTE)
        self.local_policy, self.http_policy = self.policy(self.local), self.policy(self.http)

    def open(self, route=None):
        return SqliteStore(self.path, clock=self.clock, lease_ttl_seconds=30,
                           kinds={FixtureKind.name: FixtureKind()}, route=route)

    def policy(self, store):
        return ControlPolicy(store, artifacts=self.cas, authority='route_task')

    def submit(self, store, name, *, rule=None):
        return store.submit(TaskSpec(name, FixtureKind.name, 1, {'name': name}, (), ('cpu',),
                                     'synthetic', 'none', 2), control_rule=rule)[0]

    def approval(self, task, scope='fixture.run'):
        receipt = self.cas.put(canonical_json({
            'synthetic': True, 'task_id': task.task_id, 'fingerprint': task.fingerprint,
            'scope': scope, 'decision': 'approved', 'actor': 'reviewer', 'expires_at': self.expiry,
        }))
        return dict(scope=scope, decision='approved', actor='reviewer',
                    evidence_ref=receipt, expires_at=self.expiry)

    def settlement(self, task, actual_units, *, budget='shared'):
        receipt = self.cas.put(canonical_json({
            'synthetic': True, 'task_id': task.task_id, 'fingerprint': task.fingerprint,
            'fence': 1, 'budget_id': budget, 'actual_units': actual_units, 'final_usage': True,
        }))
        return dict(actual_units=actual_units, evidence_ref=receipt, actor='accountant')

    def test_route_operations_and_ledgers_share_totals_without_sharing_tasks(self):
        local = self.submit(self.local, 'local')
        remote = self.submit(self.http, 'remote')
        remote_only = self.submit(self.http, 'remote-only')
        self.local_policy.bind_task(local.task_id, budget_id='shared', reserve_units=3, approval_scope='fixture.run')
        self.http_policy.bind_task(remote.task_id, budget_id='shared', reserve_units=4, approval_scope='fixture.run')
        self.http_policy.bind_task(remote_only.task_id, budget_id='remote-only', approval_scope='unapproved')
        with self.http._tx(execution=True) as conn:
            conn.execute('INSERT INTO transport_handoffs VALUES(?,?,?)',
                         (remote.task_id, 'fixture', self.http._now()))
        routes = (
            (self.local, self.local_policy, local, 'local-worker'),
            (self.http, self.http_policy, remote, 'http-worker'),
        )
        for store, policy, task, worker in routes:
            store.register_device(worker, ['cpu'])
            self.assertIsNone(store.claim(worker))
            policy.approval_decide(task.task_id, **self.approval(task))
        with ThreadPoolExecutor(2) as pool:
            pending = [pool.submit(store.claim, worker) for store, _, _, worker in routes]
            leases = [future.result(timeout=10) for future in pending]
        for (store, policy, task, worker), lease in zip(routes, leases):
            self.assertIsNotNone(lease)
            self.assertEqual(lease.task_id, task.task_id)
            policy.mark_uncertain(task.task_id, lease.fence, actor='operator')
            store.mark_unknown(lease.lease_id, lease.fence, 'lost receipt', worker)
        self.clock.advance(3601)
        for task, policy in ((local, self.local_policy), (remote, self.http_policy)):
            status = policy.status(uncertain_after_hours=1)
            for table in ('task_rules', 'approvals', 'reservations', 'events', 'uncertain_over_threshold'):
                self.assertIn(task.task_id, {row['task_id'] for row in status[table]}, table)
                other = remote if task is local else local
                self.assertNotIn(other.task_id, {row['task_id'] for row in status[table]}, table)
            self.assertEqual(status['budget_totals_scope'], 'ledger')
            shared = next(budget for budget in status['budgets'] if budget['budget_id'] == 'shared')
            self.assertEqual((shared['reserved_units'], shared['used_units']), (7, 0))

        local_usage = self.settlement(local, 2)
        for attempt in range(2):
            with self.assertRaises(RouteRejected):
                self.http_policy.settle(local.task_id, 1, **local_usage)
            self.local_policy.settle(local.task_id, 1, **local_usage)
        before = self.path.read_bytes()
        with self.assertRaises(RouteRejected):
            self.local_policy.approval_decide(remote.task_id, **self.approval(remote))
        with self.assertRaises(RouteRejected):
            self.local_policy.mark_uncertain(remote.task_id, 1, actor='operator')
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.local.get_task(local.task_id).status, 'unknown')
        self.local.resolve_unknown(local.task_id, 'still_unknown', 'operator', expected_fence=1)
        self.assertIsNone(self.local.claim('local-worker'))

        status = self.local_policy.status()
        self.assertEqual([budget['budget_id'] for budget in status['budgets']], ['shared'])
        shared, = status['budgets']
        self.assertEqual((shared['reserved_units'], shared['used_units']), (4, 2))
        self.http_policy.settle(remote.task_id, 1, **self.settlement(remote, 1))
        shared, = self.local_policy.status()['budgets']
        self.assertEqual((shared['reserved_units'], shared['used_units']), (0, 3))

    def test_atomic_rule_cannot_be_emptied_replaced_or_take_foreign_approval(self):
        bound = {'budget_id': 'shared', 'reserve_units': 3, 'approval_scope': 'fixture.run'}
        local = self.submit(self.local, 'local', rule=bound)
        remote = self.submit(self.http, 'remote')
        unbound = self.submit(self.local, 'optional')
        prior = self.local_policy.approval_decide(local.task_id, **self.approval(local))
        foreign = self.http_policy.approval_decide(remote.task_id, **self.approval(remote))
        wrong_scope = self.local_policy.approval_decide(local.task_id, **self.approval(local, 'different'))
        for overrides in ({'budget_id': 'unlinked'}, {'reserve_units': 0},
                          {'approval_scope': None}, {'approval_id': foreign}, {'approval_id': wrong_scope}):
            with self.subTest(overrides=overrides), self.assertRaises(DispatchError):
                self.local_policy.bind_task(local.task_id, **(bound | overrides))
        with self.assertRaises(DispatchError):
            self.local_policy.bind_task(unbound.task_id)
        with self.assertRaises(RouteRejected):
            self.local_policy.bind_task(remote.task_id, **bound)
        self.local_policy.bind_task(local.task_id, **bound)
        rule, = self.local_policy.status()['task_rules']
        self.assertEqual(rule['approval_id'], prior)
        self.assertEqual({key: rule[key] for key in bound}, bound)
        self.assertEqual(self.submit(self.local, 'local', rule=bound).task_id, local.task_id)

    def test_readonly_status_and_fail_closed_authority(self):
        self.clock.advance(86401)
        before = self.path.read_bytes()
        with patch.object(self.local, '_write_connect', side_effect=AssertionError('read-only operation')):
            status = self.policy(self.local).status()
        self.assertEqual(status['execution_route'], LOCAL_ROUTE)
        self.assertEqual(status['handoffs'], [])
        self.assertEqual(status['events'], [])
        for func, args, kwargs in (
            (self.local_policy.bind_task, (self.historical.task_id,), {'budget_id': 'shared'}),
            (self.stale.bind_task, (self.historical.task_id,), {'budget_id': 'shared'}),
            (self.stale.status, (), {}),
            (ControlPolicy, (self.local,), {}),
            (ControlPolicy, (self.local,), {'authority': 'central'}),
            (self.policy, (self.open(),), {}),
        ):
            with self.subTest(operation=func), self.assertRaises(RouteRejected):
                func(*args, **kwargs)
        self.assertEqual(self.path.read_bytes(), before)
        with closing(self.local._connect()) as conn:
            self.assertEqual(conn.execute("SELECT state FROM control_handoffs").fetchone()[0], 'offered')

    def test_global_operations_are_refused(self):
        policy = self.local_policy
        before = self.path.read_bytes()
        for func, args, kwargs in (
            (policy.budget_upsert, ('shared',), dict(period='fixture', unit='units', limit_units=1,
             starts_at='2026-01-01T00:00:00Z', ends_at=self.expiry)),
            (policy.handoff_offer, ('new',), dict(from_conv='sender', to_conv='receiver', project_id='fixture',
             scope='read_status', expires_at=self.expiry, actor='sender')),
            (policy.handoff_transition, ('historical', 'accepted'), {'actor': 'receiver'}),
            (policy.settle_host_run, (self.historical.task_id, 'run', None, 'operator'), {}),
        ):
            with self.subTest(operation=func), self.assertRaisesRegex(RouteRejected, 'isolated'):
                func(*args, **kwargs)
        self.assertEqual(self.path.read_bytes(), before)

    def test_missing_schema_is_refused_without_repair(self):
        before = self.path.read_bytes()
        for corruption in ('ALTER TABLE control_reservations DROP COLUMN uncertain_at',
                           'ALTER TABLE control_approvals DROP COLUMN fingerprint',
                           'DROP TABLE control_handoffs'):
            with self.subTest(corruption=corruption):
                with closing(sqlite3.connect(self.path)) as conn, conn:
                    conn.execute(corruption)
                damaged = self.path.read_bytes()
                with self.assertRaisesRegex(RouteRejected, 'initialized control schema'):
                    self.policy(self.local)
                self.assertEqual(self.path.read_bytes(), damaged)
                self.path.write_bytes(before)

    def test_standalone_route_option_works_and_requests_cannot_grant_authority(self):
        path = self.root / 'request.json'
        request = dict(schema=REQUEST_SCHEMA, synthetic=True, operation='status', params={})
        command = [sys.executable, '-B', '-m', 'sa_dispatch.control_cli', '--ledger', str(self.path),
                   '--artifacts', str(self.cas.root), '--request', str(path)]

        def run(envelope, *options):
            path.write_bytes(canonical_json(envelope))
            reply = subprocess.run(command + list(options), capture_output=True,
                                   cwd=Path(__file__).resolve().parents[1], timeout=10)
            self.assertEqual(reply.stderr, b'')
            result = json.loads(reply.stdout)
            self.assertEqual(reply.returncode == 0, result['ok'])
            return result

        before = self.path.read_bytes()
        self.assertEqual(run(request)['error']['code'], 'E_EXECUTION_ROUTE')
        result = run(request, '--route', LOCAL_ROUTE)
        self.assertEqual(result['execution_route'], LOCAL_ROUTE)
        self.assertFalse(result['status_refreshes_handoff_expiry'])
        for envelope in (request | {'authority': 'route_task'},
                         request | {'params': {'authority': 'route_task'}},
                         request | {'execution_route': LOCAL_ROUTE}):
            self.assertEqual(run(envelope, '--route', LOCAL_ROUTE)['error']['code'], 'E_CONTROL_REQUEST')
        self.assertEqual(self.path.read_bytes(), before)
