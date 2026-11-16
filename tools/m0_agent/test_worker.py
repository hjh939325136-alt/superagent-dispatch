"""Simulated worker tests with a temporary real ledger and CAS."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.models import DispatchError, Ref, TaskSpec
from sa_dispatch.routing import RouteRejected
from sa_dispatch.scheduler import Scheduler
from sa_dispatch.store_sqlite import SqliteStore

from .deepseek_client import Completion, DeepSeekError, MAX_REQUEST_BYTES, TokenUsage
from .worker import AgentKind, AgentScheduler, CAPABILITY, KIND, OperatorGrant, run_once


class LabKind:
    enabled = True
    version = 1
    side_effect = 'none'

    def validate(self, params):
        return params

    def scope(self, params):
        return {}


class SimulatedProcessLoss(BaseException):
    pass


class WorkerTest(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.clock = FakeClock()
        self.store = SqliteStore(root / 'ledger.sqlite3', clock=self.clock, lease_ttl_seconds=10,
                                 kinds={KIND: AgentKind(), 'lab.read': LabKind()})
        self.artifacts = ArtifactStore(root / 'cas')
        self.prompt = 'simulated approved prompt'
        prompt_hash = self.artifacts.put(self.prompt.encode())
        self.grant = OperatorGrant('simulated-grant', 3, Ref('operator', 'agent_prompt', 'fixture', '1', prompt_hash),
                                   'synthetic', 'deepseek', 'simulated-model', 48, timestamp(self.clock.now()), 'derived')
        self.store.register_device('worker', [CAPABILITY])
        self.reply = Completion('simulated-response', 'simulated-model', 'stop', 'simulated answer',
                                TokenUsage(7, 3, 10, 0, 7))
        self.complete = Mock(return_value=self.reply)

    def run_grant(self, grant=None, *, device_id='worker'):
        return run_once(self.store, self.artifacts, grant or self.grant, device_id=device_id,
                        api_key='simulated-key', provider_call=self.complete)

    def test_submit_before_ack_replay_uses_same_task_and_sends_once(self):
        submit = self.store.submit

        def lose_ack(spec):
            submit(spec)
            raise SimulatedProcessLoss

        with patch.object(self.store, 'submit', side_effect=lose_ack):
            with self.assertRaises(SimulatedProcessLoss):
                self.run_grant()
        self.complete.assert_not_called()
        task_id = self.store.list_tasks()[0].task_id
        result = self.run_grant()
        self.assertEqual((result.task_id, result.status), (task_id, 'succeeded'))
        self.assertEqual(result.result['usage']['total_tokens'], 10)
        self.assertIsNone(result.result['cost'])
        self.assertEqual(self.artifacts.get(result.result['result_ref']), b'simulated answer')
        self.clock.advance(3600)
        with patch.object(self.store, 'submit', wraps=submit) as submit_again:
            self.assertEqual(self.run_grant(), result)
            submit_again.assert_not_called()
        self.complete.assert_called_once_with(model=self.grant.model, approved_text=self.prompt,
                                              api_key='simulated-key', max_tokens=48)
        scheduler = AgentScheduler(self.store, self.grant)
        self.assertEqual(len(scheduler.firings()), 1)
        self.assertEqual(scheduler.firings()[0]['fire_key'], self.grant.fire_key)

    def test_process_loss_after_call_expires_to_unknown_without_resend(self):
        with patch.object(self.artifacts, 'put', side_effect=SimulatedProcessLoss):
            with self.assertRaises(SimulatedProcessLoss):
                self.run_grant()
        self.assertEqual(self.run_grant().status, 'leased')
        self.clock.advance(11)
        self.assertEqual(self.store.sweep().unknown, 1)
        self.assertEqual(self.run_grant().status, 'unknown')
        self.complete.assert_called_once()

    def test_concurrent_workers_share_one_firing_and_provider_call(self):
        with ThreadPoolExecutor(max_workers=4) as threads:
            results = list(threads.map(lambda _: self.run_grant(), range(4)))
        self.assertEqual(len({task.task_id for task in results}), 1)
        self.assertEqual(self.run_grant().status, 'succeeded')
        self.complete.assert_called_once()

    def test_same_kind_unrelated_queued_task_is_not_claimed(self):
        foreign, _ = self.store.submit(replace(self.grant, grant_id='other-grant').template())
        self.clock.advance(1)
        result = self.run_grant()
        self.assertEqual(result.status, 'succeeded')
        self.assertEqual(self.store.get_task(foreign.task_id).status, 'queued')
        self.complete.assert_called_once()

    def test_unrelated_schedule_cursors_events_and_pending_are_untouched(self):
        scheduler = Scheduler(self.store)
        normal = TaskSpec('lab-template', 'lab.read', 1, {}, (), (), 'synthetic', 'none', 1)
        for schedule_id in ('unrelated-due', 'unrelated-pending'):
            scheduler.upsert(schedule_id, interval_seconds=10, task_template=normal,
                             next_fire_at=timestamp(self.clock.now()))
        with patch.object(self.store, 'submit', side_effect=SimulatedProcessLoss):
            with self.assertRaises(SimulatedProcessLoss):
                scheduler.tick(schedule_id='unrelated-pending')

        def unrelated():
            return {key: [row for row in rows if (row.get('id') or row['schedule_id']).startswith('unrelated-')]
                    for key, rows in scheduler.status().items()}

        before = unrelated()
        self.assertEqual(before['firings'][0]['outcome'], 'pending')
        self.assertEqual(self.run_grant().status, 'succeeded')
        self.assertEqual(unrelated(), before)

    def test_capability_and_due_time_before_provider_call(self):
        future = replace(self.grant, scheduled_at=timestamp(self.clock.now().replace(second=1)))
        self.assertIsNone(self.run_grant(future))
        self.store.register_device('wrong-worker', ['agent.deepseek.lookalike'])
        self.clock.advance(1)
        self.assertEqual(self.run_grant(future, device_id='wrong-worker').status, 'queued')
        self.complete.assert_not_called()
        self.assertEqual(self.run_grant(future).status, 'succeeded')

    def test_route_egress_and_template_drift_are_refused(self):
        with patch.object(self.store, 'routing_mode', return_value='mixed'):
            with self.assertRaises(RouteRejected):
                self.run_grant()
        for changes in ({'source': 'local_only'}, {'source': 'restricted'}, {'egress': 'none'},
                        {'provider': 'other'}, {'scheduled_at': '2026-01-01T00:00:00Z'}):
            with self.subTest(changes=changes), self.assertRaises(DispatchError):
                replace(self.grant, **changes)
        scheduler = AgentScheduler(self.store, self.grant)
        for changes in ({'access': 'synthetic'}, {'egress': 'none'}, {'max_attempts': 2},
                        {'required_caps': ()}):
            with self.subTest(changes=changes), self.assertRaises(DispatchError):
                scheduler._validate_template(replace(self.grant.template(), **changes))
        with self.assertRaises(DispatchError):
            scheduler.tick(synthetic_handoff=True)
        self.complete.assert_not_called()

    def test_budget_and_grant_drift_fail_closed_on_restart(self):
        self.assertEqual(self.run_grant().status, 'succeeded')
        for changes in ({'version': 4}, {'max_tokens': 49}, {'model': 'different-model'}, {'source': 'public'},
                        {'prompt_ref': replace(self.grant.prompt_ref, hash='sha256:' + '0' * 64)}):
            with self.subTest(changes=changes), self.assertRaises(DispatchError):
                self.run_grant(replace(self.grant, **changes))
        with self.store._tx(execution=True) as conn:
            conn.execute('UPDATE schedules SET max_slots=2 WHERE id=?', (self.grant.grant_id,))
        with self.assertRaises(DispatchError):
            self.run_grant()
        self.complete.assert_called_once()

    def test_prompt_hash_mismatch_blocks_provider(self):
        self.artifacts.path(self.grant.prompt_ref.hash).write_bytes(b'simulated tampered prompt')
        with self.assertRaises(DispatchError):
            self.run_grant()
        self.complete.assert_not_called()
        self.assertEqual(self.store.list_tasks()[0].status, 'queued')

    def test_oversized_approved_artifact_blocks_provider(self):
        ref = replace(self.grant.prompt_ref, hash=self.artifacts.put(b'x' * (MAX_REQUEST_BYTES + 1)))
        with self.assertRaises(DispatchError):
            self.run_grant(replace(self.grant, prompt_ref=ref))
        self.complete.assert_not_called()

    def test_revoked_key_fails_safely(self):
        self.complete.side_effect = DeepSeekError('auth_rejected', status=403)
        task = self.run_grant()
        self.assertEqual(task.status, 'failed')
        self.assertEqual(task.result, {'error': 'auth_rejected', 'http_status': 403, 'cost': None})
        self.assertEqual(self.run_grant(), task)
        self.complete.assert_called_once()

    def test_timeout_marks_unknown_immediately(self):
        self.complete.side_effect = DeepSeekError('timeout', outcome_unknown=True)
        self.assertEqual(self.run_grant().status, 'unknown')
        self.assertEqual(self.run_grant().status, 'unknown')
        self.complete.assert_called_once()

    def test_truncated_result_preserves_usage_and_cas_as_incomplete(self):
        self.complete.return_value = replace(self.reply, finish_reason='length')
        task = self.run_grant()
        self.assertEqual(task.status, 'failed')
        self.assertTrue(task.result['truncated'])
        self.assertEqual(task.result['usage']['total_tokens'], 10)
        self.assertEqual(self.artifacts.get(task.result['result_ref']), b'simulated answer')
