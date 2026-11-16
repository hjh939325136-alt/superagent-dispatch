"""Exercise the common agent against a real upgraded mixed SQLite ledger."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import timedelta
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from sa_dispatch.agent_control import BUDGET_ID
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.bundle import snapshot_records
from sa_dispatch.canonical import canonical_json
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.external_migration import upgrade
from sa_dispatch.models import DispatchError, Ref, TaskSpec
from sa_dispatch.routing import (EXTERNAL_ROUTE, LOCAL_ROUTE, RouteRejected, activate_mixed,
                                 activation_manifest, validate_mixed_records)
from sa_dispatch.store_sqlite import SqliteStore

from .common_runtime import NETWORK_SCHEMA, SCHEMA, read_config, read_provider_network
from .common_worker import (DEADLINE_SECONDS, DEVICE_ID, LEASE_TTL_SECONDS, CommonGrant,
                            _CommonAgentStore, ledger_identity, register, status, tick)
from .deepseek_client import Completion, DeepSeekError, TokenUsage
from .worker import AgentKind, AgentScheduler, CAPABILITY, KIND, OperatorGrant


class FixtureKind:
    version = 1
    enabled = True
    side_effect = 'none'

    def validate(self, value):
        return value

    def scope(self, value):
        return {}


class ProcessLost(BaseException):
    pass


class CommonWorkerTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.ledger = self.root / 'dispatch.sqlite3'
        self.cas = ArtifactStore(self.root / 'cas')
        self.clock = FakeClock()
        self.kinds = {KIND: AgentKind(), 'fixture.local': FixtureKind()}
        baseline = self.store()
        with closing(baseline._connect()) as conn:
            proof = self.cas.put(canonical_json(activation_manifest(conn, actor='fixture', bindings={})))
        activate_mixed(self.ledger, actor='fixture', evidence_ref=proof, bindings={}, artifacts=self.cas, clock=self.clock)
        self.local = self.store(LOCAL_ROUTE)
        self.local.register_device('local-device', ['worker.compute'])
        self.local_task, _ = self.local.submit(TaskSpec(
            'local-idem', 'fixture.local', 1, {}, (), ('worker.compute',), 'synthetic', 'none', 1))
        self.prompt = 'Approved synthetic workshop prompt.'
        prompt_hash = self.cas.put(self.prompt.encode())
        grant = OperatorGrant('common-grant', 1, Ref('operator', 'agent_prompt', 'fixture', '1', prompt_hash),
                              'synthetic', 'deepseek', 'approved-model', 80, timestamp(self.clock.now()), 'derived', 'live')
        self.config = CommonGrant(grant, timestamp(self.clock.now() + timedelta(hours=1)),
                                  ledger_identity(self.ledger), 'a' * 32)
        self.credential = Mock(return_value='fixture-credential')
        self.reply = Completion('provider-response-1', 'real-model-identity', 'stop', 'provider result',
                                TokenUsage(8, 3, 11, 3, 5))
        self.provider = Mock(return_value=self.reply)

    def store(self, route=None):
        return SqliteStore(self.ledger, clock=self.clock, lease_ttl_seconds=10, kinds=self.kinds, route=route)

    def common_store(self):
        return _CommonAgentStore(self.ledger, self.cas, self.config, clock=self.clock)

    def upgrade(self):
        return upgrade(self.ledger, self.root / 'recovery', offline=True, clock=self.clock)

    def register(self):
        return register(self.ledger, self.cas, self.config, clock=self.clock)

    def status(self):
        return status(self.ledger, self.cas, self.config, clock=self.clock)

    def tick(self):
        return tick(self.ledger, self.cas, self.config, credential_reader=self.credential,
                    provider_call=self.provider, clock=self.clock)

    def test_only_explicitly_migrated_mixed_ledger_and_pinned_identity(self):
        with self.assertRaises(RouteRejected):
            self.register()
        self.assertEqual(self.upgrade()['schema_version'], '0013')
        with self.assertRaises(RouteRejected):
            status(self.ledger, self.cas, replace(self.config, ledger_identity='0:0'), clock=self.clock)
        self.assertEqual(self.status()['status'], 'unregistered')
        with self.assertRaises(RouteRejected):
            self.tick()
        self.credential.assert_not_called()

    def test_concurrent_ticks_use_one_common_slot_and_complete_atomically(self):
        self.upgrade()
        self.assertEqual(self.register()['status'], 'queued')
        self.credential.assert_not_called()
        with ThreadPoolExecutor(max_workers=5) as pool:
            results = list(pool.map(lambda _: self.tick(), range(5)))
        self.assertEqual(len({result['task_id'] for result in results}), 1)
        result = self.tick()
        self.assertEqual(result, self.register())
        self.assertEqual(result['status'], 'succeeded')
        self.assertEqual((result['request_units_used'], result['request_units_held'], result['fence']), (1, 0, 1))
        self.assertEqual(result['response_id'], 'provider-response-1')
        self.assertIsNone(result['cost'])
        self.provider.assert_called_once_with(model='approved-model', approved_text=self.prompt,
                                              api_key='fixture-credential', max_tokens=80,
                                              deadline_seconds=DEADLINE_SECONDS, thinking='disabled')
        self.assertEqual(self.cas.get(result['result_ref']), b'provider result')
        proof = json.loads(self.cas.get(result['approval_ref']))
        self.assertEqual(proof['grant']['prompt_ref']['hash'], self.config.grant.prompt_ref.hash)
        self.assertEqual(proof['ledger_identity'], self.config.ledger_identity)
        usage = json.loads(self.cas.get(result['usage_evidence_ref']))
        self.assertEqual((usage['actual_units'], usage['unit'], usage['response']['response_id']),
                         (1, 'provider_request', result['response_id']))
        with closing(self.local._connect()) as conn:
            records = snapshot_records(conn)
            validate_mixed_records(records, allow_external=True)
            evidence = conn.execute('SELECT kind,access FROM evidence WHERE task_id=?', (result['task_id'],)).fetchall()
            self.assertEqual([tuple(row) for row in evidence], [('agent_result', 'local_only')])
        self.assertEqual(self.local.get_task(self.local_task.task_id).status, 'queued')

    def test_due_slot_and_expiry(self):
        self.upgrade()
        slot = timestamp(self.clock.now() + timedelta(minutes=10))
        self.config = replace(self.config, grant=replace(self.config.grant, scheduled_at=slot))
        self.assertEqual(self.register()['status'], 'scheduled')
        self.assertEqual(self.tick()['status'], 'scheduled')
        self.credential.assert_not_called()
        self.clock.advance(600)
        self.assertEqual(self.tick()['status'], 'succeeded')

    def test_expired_approval_credential_missing_and_approval_artifact_drift(self):
        self.upgrade()
        ref = self.register()['approval_ref']
        self.credential.side_effect = ValueError('missing credential')
        with self.assertRaises(ValueError):
            self.tick()
        self.assertEqual(self.status()['fence'], 0)
        self.credential.side_effect = None
        self.cas.path(ref).write_bytes(b'changed approval')
        with self.assertRaises(DispatchError) as caught:
            self.tick()
        self.assertEqual(str(caught.exception), 'common agent artifact does not match its approved content')
        self.clock.advance(3600)
        self.assertEqual(self.tick()['status'], 'queued')
        self.provider.assert_not_called()
        self.assertEqual(self.status()['request_units_held'], 0)

    def test_revoked_approval_and_missing_rule_cannot_claim(self):
        self.upgrade()
        registered = self.register()
        with closing(self.local._write_connect()) as conn, conn:
            conn.execute("""INSERT INTO control_approvals(approval_id,task_id,fingerprint,scope,decision,actor,
                evidence_ref,expires_at,at) SELECT 'test-denial',task_id,fingerprint,scope,'denied',actor,
                evidence_ref,expires_at,at FROM control_approvals WHERE task_id=?""", (registered['task_id'],))
        self.assertEqual(self.tick()['status'], 'queued')
        self.register()
        self.assertEqual(self.tick()['status'], 'queued')
        with closing(self.local._write_connect()) as conn, conn:
            conn.execute('DELETE FROM control_task_rules WHERE task_id=?', (registered['task_id'],))
        self.assertEqual(self.tick()['status'], 'queued')
        self.provider.assert_not_called()
        self.assertEqual(self.status()['request_units_held'], 0)

    def test_already_spent_budget_and_changed_grant_do_not_create_new_slot(self):
        self.upgrade()
        self.register()
        with closing(self.local._write_connect()) as conn, conn:
            conn.execute('UPDATE control_budgets SET used_units=1 WHERE budget_id=?', (BUDGET_ID,))
        self.assertEqual(self.tick()['status'], 'queued')
        previous = self.config
        self.config = replace(previous, grant=replace(previous.grant, model='drifted-model'))
        with self.assertRaises(RouteRejected):
            self.tick()
        self.config = replace(previous, grant=replace(previous.grant, grant_id='fresh-slot'))
        with self.assertRaises(RouteRejected):
            self.register()
        self.provider.assert_not_called()

    def test_timeout_and_process_loss_do_not_reissue(self):
        self.upgrade()
        self.register()
        self.provider.side_effect = ProcessLost
        with self.assertRaises(ProcessLost):
            self.tick()
        self.assertEqual(self.tick()['status'], 'leased')
        self.clock.advance(LEASE_TTL_SECONDS + 1)
        result = self.tick()
        self.assertEqual((result['status'], result['request_units_used'], result['request_units_held'],
                          result['reservation_state'], result['lease_state']), ('unknown', 0, 1, 'uncertain', 'expired'))
        self.assertEqual(self.tick(), result)
        self.provider.assert_called_once()

    def test_ambiguous_response_immediately_retains_reservation(self):
        self.upgrade()
        self.register()
        self.provider.side_effect = DeepSeekError('deadline_exceeded', outcome_unknown=True)
        result = self.tick()
        self.assertEqual((result['status'], result['reservation_state'], result['request_units_held']),
                         ('unknown', 'uncertain', 1))
        self.assertEqual(self.tick(), result)
        self.provider.assert_called_once()

    def test_http_rejection_consumes_the_confirmed_request(self):
        self.upgrade()
        self.register()
        self.provider.side_effect = DeepSeekError('auth_rejected', status=401)
        result = self.tick()
        self.assertEqual((result['status'], result['http_status'], result['request_units_used'],
                          result['request_units_held']), ('failed', 401, 1, 0))
        self.assertIsNone(result['result_ref'])
        self.assertEqual(self.tick(), result)
        self.provider.assert_called_once()

    def test_completion_ack_loss_and_replay_never_reexecutes(self):
        self.upgrade()
        self.register()
        original = _CommonAgentStore.complete
        invocations = []

        def lose_ack(store, *args):
            original(store, *args)
            invocations.append(args)
            if len(invocations) == 1:
                raise sqlite3.OperationalError('simulated lost acknowledgement')

        with patch.object(_CommonAgentStore, 'complete', lose_ack):
            self.assertEqual(self.tick()['status'], 'succeeded')
        self.assertEqual(invocations[0], invocations[1])
        self.provider.assert_called_once()

    def test_late_completion_keeps_unknown_and_ignores_receipt(self):
        self.upgrade()
        self.register()

        def delayed_provider(**kwargs):
            self.clock.advance(LEASE_TTL_SECONDS + 1)
            return self.reply

        self.provider.side_effect = delayed_provider
        result = self.tick()
        self.assertEqual((result['status'], result['request_units_used'], result['request_units_held']), ('unknown', 0, 1))
        self.assertEqual(len(self.local.late_results(result['task_id'])), 1)
        self.assertEqual(self.tick(), result)
        self.provider.assert_called_once()

    def test_scoped_sweep_and_claim_leave_other_route_leases_untouched(self):
        self.upgrade()
        registered = self.register()
        local_lease = self.local.claim('local-device')
        self.clock.advance(20)
        common = self.common_store()
        with self.assertRaises(RouteRejected):
            common.sweep()
        with self.assertRaises(RouteRejected):
            common.sweep(task_id=local_lease.task_id)
        with self.assertRaises(RouteRejected):
            common.claim(DEVICE_ID, (CAPABILITY,), kinds=(KIND,))
        with self.assertRaises(RouteRejected):
            self.store(EXTERNAL_ROUTE)
        with self.assertRaises(RouteRejected):
            AgentScheduler(common, self.config.grant)
        self.assertIsNone(self.local.claim('local-device', task_id=registered['task_id']))
        self.assertEqual(self.tick()['status'], 'succeeded')
        self.assertEqual(self.local.get_task(local_lease.task_id).status, 'leased')

    def test_canonical_config_and_mode(self):
        path = self.root / 'config.json'
        doc = self.config.authority()
        del doc['device_id'], doc['thinking']
        doc['schema'] = SCHEMA
        path.write_bytes(canonical_json(doc))
        self.assertEqual(read_config(path), self.config)
        doc['grant']['execution_mode'] = 'simulation'
        path.write_bytes(canonical_json(doc))
        with self.assertRaises(DispatchError) as caught:
            read_config(path)
        self.assertEqual(str(caught.exception), 'invalid common agent live grant')

    def test_provider_network_config_is_neither_ambient_nor_an_arbitrary_proxy(self):
        path = self.root / 'provider-network.json'
        with patch.dict('os.environ', {'HTTPS_PROXY': 'http://not-used.example:3131'}):
            self.assertIsNone(read_provider_network(path))
        proxy = {'host': '10.0.2.2', 'port': 7897}
        doc = {'schema': NETWORK_SCHEMA, 'https_connect_proxy': proxy}
        path.write_bytes(canonical_json(doc))
        with patch('tools.m0_agent.common_runtime._safe_system_path') as verified:
            self.assertEqual(read_provider_network(path), proxy)
            verified.assert_called_once_with(path, directory=False)
            path.write_bytes(canonical_json(doc | {'https_connect_proxy': proxy | {'port': 8080}}))
            with self.assertRaises(ValueError):
                read_provider_network(path)


if __name__ == '__main__':
    unittest.main()
