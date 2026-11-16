"""Resume synthetic ledgers written by the M0.12 worker from a47665d."""

import importlib.util
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.models import DispatchError, Ref
from sa_dispatch.scheduler import Scheduler
from sa_dispatch.store_sqlite import SqliteStore


DISPATCH_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(DISPATCH_ROOT.parent))

from tools.m0_agent import worker
from tools.m0_agent.deepseek_client import Completion, DeepSeekError, TokenUsage


class SimulatedProcessLoss(BaseException):
    pass


class AgentWorkerCompatibilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture = DISPATCH_ROOT / 'tests' / 'fixtures' / 'agent_worker_m0_12.py'
        spec = importlib.util.spec_from_file_location('tools.m0_agent._legacy_fixture', fixture)
        cls.legacy = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = cls.legacy
        cls.addClassCleanup(sys.modules.pop, spec.name)
        spec.loader.exec_module(cls.legacy)

    def prepare_ledger(self, module):
        root = Path(self.enterContext(TemporaryDirectory(prefix='sa-agent-upgrade-')))
        self.ledger = root / 'ledger.sqlite3'
        self.clock = FakeClock()
        self.artifacts = ArtifactStore(root / 'artifacts')
        self.prompt = 'fixture prompt for an approved synthetic call'
        prompt_ref = Ref('fixture', 'agent_prompt', 'prompt', '1', self.artifacts.put(self.prompt.encode()))
        self.grant_fields = {
            'grant_id': 'compat-fixture', 'version': 1, 'prompt_ref': prompt_ref,
            'source': 'synthetic', 'provider': 'deepseek', 'model': 'fixture-model',
            'max_tokens': 16, 'scheduled_at': timestamp(self.clock.now()), 'egress': 'derived',
        }
        self.reply = Completion('fixture-response', 'fixture-model', 'stop', 'fixture answer',
                                TokenUsage(7, 3, 10, 0, 7))
        store = self.open_ledger(module)
        store.register_device('fixture-device', [module.CAPABILITY])
        return store

    def open_ledger(self, module=worker):
        return SqliteStore(self.ledger, clock=self.clock, lease_ttl_seconds=10,
                           kinds={module.KIND: module.AgentKind()})

    def run_worker(self, module, store, provider, **extra_grant_fields):
        grant = module.OperatorGrant(**self.grant_fields, **extra_grant_fields)
        return module.run_once(store, self.artifacts, grant, device_id='fixture-device',
                               api_key=None, provider_call=provider)

    @staticmethod
    def task_identity(task):
        return tuple(task[field] for field in ('task_id', 'idem_key', 'fingerprint', 'params', 'input_refs'))

    @classmethod
    def task_state(cls, task):
        return cls.task_identity(task), task.status, task.attempts, task.result

    @staticmethod
    def schedule_state(store):
        status = Scheduler(store).status()
        schedules = [tuple(row[key] for key in ('id', 'version', 'task_template', 'last_fire_key',
                                               'enabled', 'max_slots'))
                     for row in status['schedules']]
        firings = [tuple(row[key] for key in ('schedule_id', 'version', 'fire_key', 'task_template',
                                             'task_id', 'outcome')) for row in status['firings']]
        return schedules, firings

    def assert_one_task(self, store, expected):
        self.assertEqual([self.task_state(task) for task in store.list_tasks()], [self.task_state(expected)])

    def assert_resumed_once(self, store, original):
        provider = Mock(return_value=self.reply)
        resumed = self.run_worker(worker, store, provider)
        self.assertEqual(self.task_identity(resumed), self.task_identity(original))
        self.assertEqual((resumed.status, resumed.attempts), ('succeeded', 1))
        self.assertEqual(resumed.result['response_id'], self.reply.response_id)
        self.assertEqual(self.artifacts.get(resumed.result['result_ref']), self.reply.text.encode())
        self.assertNotIn('execution_mode', resumed.result)
        self.assertNotIn('provider', resumed.result)
        self.assertEqual(self.task_state(self.run_worker(worker, store, provider)), self.task_state(resumed))
        provider.assert_called_once_with(model=self.reply.model, approved_text=self.prompt,
                                         api_key=None, max_tokens=16)
        self.assert_one_task(store, resumed)

    def test_existing_terminal_tasks_retain_results_without_provider_replay(self):
        for status, error in (
            ('succeeded', None),
            ('failed', DeepSeekError('auth_rejected', status=403)),
            ('unknown', DeepSeekError('timeout', outcome_unknown=True)),
        ):
            with self.subTest(status=status):
                old_store = self.prepare_ledger(self.legacy)
                old_provider = Mock(return_value=self.reply, side_effect=error)
                original = self.run_worker(self.legacy, old_store, old_provider)
                self.assertEqual((original.status, original.attempts), (status, 1))
                old_provider.assert_called_once()
                before = self.schedule_state(old_store)

                current_store = self.open_ledger()
                provider = Mock()
                replay = self.run_worker(worker, current_store, provider)
                provider.assert_not_called()
                self.assertEqual(self.task_state(replay), self.task_state(original))
                self.assert_one_task(current_store, original)
                self.assertEqual(self.schedule_state(current_store), before)

    def test_queued_legacy_task_runs_once_without_rewriting_schedule(self):
        old_store = self.prepare_ledger(self.legacy)
        old_grant = self.legacy.OperatorGrant(**self.grant_fields)
        task_id = self.legacy.AgentScheduler(old_store, old_grant).tick()
        original = old_store.get_task(task_id)
        self.assertEqual((original.status, original.attempts), ('queued', 0))
        before = self.schedule_state(old_store)

        current_store = self.open_ledger()
        self.assert_resumed_once(current_store, original)
        self.assertEqual(self.schedule_state(current_store), before)

    def test_pending_legacy_firing_recovers_a_committed_submission(self):
        old_store = self.prepare_ledger(self.legacy)
        submit = old_store.submit

        def lose_ack(spec):
            submit(spec)
            raise SimulatedProcessLoss

        old_provider = Mock()
        with patch.object(old_store, 'submit', side_effect=lose_ack), self.assertRaises(SimulatedProcessLoss):
            self.run_worker(self.legacy, old_store, old_provider)
        old_provider.assert_not_called()
        original, = old_store.list_tasks()
        self.assertEqual((original.status, original.attempts), ('queued', 0))
        schedules, firings = self.schedule_state(old_store)
        pending, = firings
        self.assertEqual(pending[-2:], (None, 'pending'))

        current_store = self.open_ledger()
        self.assert_resumed_once(current_store, original)
        self.assertEqual(self.schedule_state(current_store),
                         (schedules, [pending[:-2] + (original.task_id, 'created')]))

    def test_legacy_lease_expires_to_unknown_without_provider_replay(self):
        old_store = self.prepare_ledger(self.legacy)
        old_provider = Mock(return_value=self.reply)
        with patch.object(self.artifacts, 'put', side_effect=SimulatedProcessLoss), self.assertRaises(SimulatedProcessLoss):
            self.run_worker(self.legacy, old_store, old_provider)
        old_provider.assert_called_once()
        original, = old_store.list_tasks()
        self.assertEqual((original.status, original.attempts), ('leased', 1))
        before = self.schedule_state(old_store)

        store = self.open_ledger()
        provider = Mock()
        leased = self.run_worker(worker, store, provider)
        self.assertEqual(self.task_state(leased), self.task_state(original))
        self.clock.advance(11)
        self.assertEqual(store.sweep().unknown, 1)
        unknown = self.run_worker(worker, store, provider)
        self.assertEqual(self.task_identity(unknown), self.task_identity(original))
        self.assertEqual((unknown.status, unknown.attempts, unknown.result), ('unknown', 1, None))
        self.assert_one_task(store, unknown)
        self.assertEqual(self.schedule_state(store), before)
        provider.assert_not_called()

    def test_legacy_schedule_rejects_both_explicit_modes(self):
        old_store = self.prepare_ledger(self.legacy)
        grant = self.legacy.OperatorGrant(**self.grant_fields)
        self.legacy.AgentScheduler(old_store, grant).tick()
        self.assert_mode_changes_rejected(old_store, ('simulation', 'live'))

    def test_explicit_modes_reject_legacy_and_opposite_mode(self):
        for initial, opposite in (('simulation', 'live'), ('live', 'simulation')):
            with self.subTest(initial=initial):
                store = self.prepare_ledger(worker)
                grant = worker.OperatorGrant(**self.grant_fields, execution_mode=initial)
                worker.AgentScheduler(store, grant).tick()
                self.assert_mode_changes_rejected(store, (None, opposite))

    def assert_mode_changes_rejected(self, original_store, rejected_modes):
        task, = original_store.list_tasks()
        self.assertEqual((task.status, task.attempts), ('queued', 0))
        before = self.schedule_state(original_store)
        current_store = self.open_ledger()
        provider = Mock()
        for mode in rejected_modes:
            with self.subTest(requested_mode=mode):
                fields = {} if mode is None else {'execution_mode': mode}
                with self.assertRaises(DispatchError):
                    self.run_worker(worker, current_store, provider, **fields)
                provider.assert_not_called()
                self.assert_one_task(current_store, task)
                self.assertEqual(self.schedule_state(current_store), before)
