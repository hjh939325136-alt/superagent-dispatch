"""R1 runtime tests using temporary state and mocked provider transport."""

from contextlib import contextmanager, redirect_stdout
from dataclasses import asdict, replace
from io import StringIO
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from sa_dispatch.artifacts import ArtifactError, ArtifactStore
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.models import DispatchError, Ref
from sa_dispatch.store_sqlite import SqliteStore

from . import runtime
from .deepseek_client import Completion, TokenUsage
from .worker import AgentKind, KIND, OperatorGrant


@contextmanager
def unlocked_fixture(state):
    yield True


class SimulatedProcessLoss(BaseException):
    pass


class RuntimeTest(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.state = self.root / 'state'
        self.ledger = self.state / 'ledger.sqlite3'
        self.journal = self.state / 'journal'
        self.journal.mkdir(parents=True)
        self.artifacts = ArtifactStore(self.state / 'artifacts')
        self.clock = FakeClock()
        prompt_hash = self.artifacts.put(b'approved test-only input')
        self.grant = OperatorGrant('r1-fixture', 1, Ref('operator', 'agent_prompt', 'fixture', '1', prompt_hash),
                                   'synthetic', 'deepseek', 'fixture-model', 16, timestamp(self.clock.now()), 'derived', 'simulation')
        self.config = self.root / 'agent.json'
        self.key = Mock(return_value='simulated-test-only-key')
        self.provider = Mock(return_value=Completion('mock-response', 'mock-provider-model', 'stop',
                                                     'mocked output', TokenUsage(5, 3, 8, 1, 4)))

    def tick(self, grant=None, lock=unlocked_fixture):
        return runtime.tick(self.state, grant or self.grant, credential_reader=self.key,
                            live_provider=self.provider, lock=lock, clock=self.clock)

    def write_config(self, grant=None, *, omit_mode=False):
        data = asdict(grant or self.grant)
        mode = data.pop('execution_mode')
        config = {'schema': runtime.CONFIG_SCHEMA, 'grant': data}
        if not omit_mode:
            config['execution_mode'] = mode
        self.config.write_text(json.dumps(config), encoding='utf-8')

    def test_simulation_reads_fake_credential_and_never_invokes_provider(self):
        first = self.tick()
        second = self.tick()
        self.assertEqual(first, second)
        self.assertEqual(first['status'], 'succeeded')
        self.assertEqual(first['execution_mode'], 'simulation')
        self.assertEqual(first['provider'], 'deepseek')
        self.assertEqual(first['model'], 'simulated-fixture-model')
        self.assertEqual(first['usage']['total_tokens'], 0)
        self.assertEqual(first['real_calls'], 0)
        self.assertIsNone(first['cost'])
        self.assertTrue(first['credential_read'])
        self.assertNotIn(self.key.return_value, json.dumps(first))
        self.assertEqual(self.key.call_count, 2)
        self.provider.assert_not_called()
        self.assertEqual(runtime.query(self.state, self.grant)['task_id'], first['task_id'])
        store = SqliteStore(self.ledger, lease_ttl_seconds=120)
        self.assertEqual(len(store.list_tasks()), 1)
        self.assertEqual(len(store.list_leases()), 1)

    def test_live_is_explicit_and_uses_hard_deadline_adapter(self):
        live = replace(self.grant, execution_mode='live')
        result = self.tick(live)
        self.assertEqual(result['status'], 'succeeded')
        self.assertEqual(result['execution_mode'], 'live')
        self.assertEqual(result['model'], 'mock-provider-model')
        self.assertEqual(result['usage']['total_tokens'], 8)
        self.assertNotIn('real_calls', result)
        self.provider.assert_called_once_with(model=live.model, approved_text='approved test-only input',
                                             api_key=self.key.return_value, max_tokens=16, deadline_seconds=45)
        self.assertEqual(self.artifacts.get(result['result_ref']), b'mocked output')

    def test_mode_flip_refused_before_reading_credential_or_calling_provider(self):
        self.tick()
        self.key.reset_mock()
        live = replace(self.grant, execution_mode='live')
        with self.assertRaises(DispatchError):
            self.tick(live)
        with self.assertRaises(DispatchError):
            runtime.query(self.state, live)
        self.key.assert_not_called()
        self.provider.assert_not_called()

    def test_check_and_query_missing_ledger_leave_state_untouched(self):
        self.write_config()
        for command in ('check', 'query'):
            output = StringIO()
            with (patch.object(runtime, 'CONFIG_PATH', self.config), patch.object(runtime, 'STATE_PATH', self.state),
                  patch.object(runtime, '_verify_service'), patch.object(runtime, 'read_systemd_key') as read_key,
                  patch('sys.argv', ['runtime', command]),
                  redirect_stdout(output)):
                self.assertEqual(runtime.main(), 0)
            read_key.assert_not_called()
            status = json.loads(output.getvalue())
            self.assertTrue(status['ok'])
            self.assertEqual(status['status'], 'not_submitted')
            self.assertFalse(status['ledger_exists'])
            self.assertNotIn('credential_read', status)
        self.assertFalse(self.ledger.exists())
        self.assertEqual(list(self.journal.iterdir()), [])

    def test_config_requires_explicit_mode_and_rejects_unknown_fields(self):
        self.write_config(omit_mode=True)
        with self.assertRaises(ValueError):
            runtime.read_config(self.config)
        self.write_config(replace(self.grant, execution_mode='live'))
        self.assertEqual(runtime.read_config(self.config).execution_mode, 'live')
        config = json.loads(self.config.read_text(encoding='utf-8'))
        config['grant']['credential_path'] = '/alternate'
        self.config.write_text(json.dumps(config), encoding='utf-8')
        with self.assertRaises(ValueError):
            runtime.read_config(self.config)

    def test_missing_credential_does_not_initialize_ledger(self):
        self.key.side_effect = ValueError('simulated unavailable credential')
        with self.assertRaises(ValueError):
            self.tick()
        self.assertFalse(self.ledger.exists())
        self.assertFalse((self.journal / 'last-tick.json').exists())
        self.assertIsNone(runtime.check(self.state, self.grant)['credential_read_proof'])
        self.provider.assert_not_called()

    def test_legacy_mode_never_enters_r1_runtime(self):
        legacy = replace(self.grant, execution_mode=None)
        for operation in (lambda: self.tick(legacy), lambda: runtime.query(self.state, legacy),
                          lambda: runtime.check(self.state, legacy)):
            with self.assertRaises(ValueError):
                operation()
        self.key.assert_not_called()
        self.provider.assert_not_called()
        self.assertFalse(self.ledger.exists())
        self.write_config(legacy)
        with self.assertRaises(ValueError):
            runtime.read_config(self.config)

    def test_credential_proof_is_durable_and_scope_bound(self):
        invocation = '12ab' * 8
        with patch.dict(os.environ, {'INVOCATION_ID': invocation}):
            result = self.tick()
        path = self.journal / 'last-tick.json'
        raw = path.read_text(encoding='utf-8')
        proof = json.loads(raw)
        self.assertNotIn(self.key.return_value, raw)
        self.assertEqual(proof['invocation_id'], invocation)
        self.assertEqual(proof['task_id'], result['task_id'])
        self.assertEqual(proof['execution_mode'], 'simulation')
        self.assertEqual(proof['schema'], runtime.TICK_SCHEMA)
        self.assertTrue(proof['credential_read'])
        self.assertEqual(runtime.check(self.state, self.grant)['credential_read_proof'], proof)

        self.key.side_effect = ValueError('simulated unavailable credential')
        with self.assertRaises(ValueError):
            self.tick()
        self.assertEqual(path.read_text(encoding='utf-8'), raw)

        proof['grant_id'] = 'stale-grant'
        path.write_text(json.dumps(proof), encoding='utf-8')
        self.assertIsNone(runtime.check(self.state, self.grant)['credential_read_proof'])

    def test_check_rejects_missing_or_corrupt_success_artifact(self):
        result_ref = self.tick()['result_ref']
        self.assertEqual(runtime.check(self.state, self.grant)['result_ref'], result_ref)
        artifact = self.artifacts.path(result_ref)
        artifact.unlink()
        with self.assertRaises(ArtifactError) as missing:
            runtime.check(self.state, self.grant)
        self.assertEqual(missing.exception.code, 'E_ARTIFACT_MISSING')

        artifact.write_bytes(b'wrong content at the expected digest path')
        with self.assertRaises(ArtifactError) as corrupt:
            runtime.check(self.state, self.grant)
        self.assertEqual(corrupt.exception.code, 'E_HASH_MISMATCH')
        self.assertEqual(runtime.query(self.state, self.grant)['result_ref'], result_ref)
        self.assertEqual(self.key.call_count, 1)
        self.provider.assert_not_called()

    def test_live_process_loss_sweeps_to_unknown_and_does_not_resend(self):
        live = replace(self.grant, execution_mode='live')
        self.provider.side_effect = SimulatedProcessLoss
        with self.assertRaises(SimulatedProcessLoss):
            self.tick(live)
        self.assertEqual(runtime.query(self.state, live)['status'], 'leased')
        self.clock.advance(runtime.LEASE_TTL_SECONDS + 1)
        result = self.tick(live)
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(self.tick(live)['status'], 'unknown')
        self.provider.assert_called_once()

    def test_foreign_ledger_refused_before_any_sweep_or_credential_read(self):
        store = SqliteStore(self.ledger, lease_ttl_seconds=120, kinds={KIND: AgentKind()}, clock=self.clock)
        foreign = replace(self.grant, grant_id='foreign-grant')
        task, _ = store.submit(foreign.template())
        with self.assertRaises(DispatchError):
            self.tick()
        self.assertEqual(store.get_task(task.task_id).status, 'queued')
        self.key.assert_not_called()
        self.provider.assert_not_called()

    @unittest.skipUnless(os.name == 'posix', 'Linux flock required')
    def test_lock_contention_reads_no_key_and_creates_no_ledger(self):
        with runtime.tick_lock(self.state) as acquired:
            self.assertTrue(acquired)
            result = self.tick(lock=runtime.tick_lock)
        self.assertEqual(result['status'], 'busy')
        self.key.assert_not_called()
        self.provider.assert_not_called()
        self.assertFalse(self.ledger.exists())
