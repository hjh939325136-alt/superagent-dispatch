"""Exercise the fixed status entry point against an actual temporary R1 ledger."""

from contextlib import ExitStack, closing, redirect_stderr, redirect_stdout
from dataclasses import asdict
from io import StringIO
import json
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.models import Ref

from . import readonly_status, runtime
from .test_runtime import unlocked_fixture
from .worker import OperatorGrant


class ReadonlyStatusTest(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / 'state'
        (self.state / 'journal').mkdir(parents=True)
        self.ledger = self.state / 'ledger.sqlite3'
        artifacts = ArtifactStore(self.state / 'artifacts')
        prompt = artifacts.put(b'PRIVATE_PROMPT_FIXTURE')
        self.clock = FakeClock()
        self.grant = OperatorGrant(
            'query-fixture', 1, Ref('operator', 'agent_prompt', 'fixture', '1', prompt),
            'synthetic', 'deepseek', 'fixture-model', 16, timestamp(self.clock.now()),
            'derived', 'simulation',
        )
        self.config = self.root / 'agent.json'
        grant_data = asdict(self.grant)
        mode = grant_data.pop('execution_mode')
        self.config.write_text(json.dumps({
            'schema': runtime.CONFIG_SCHEMA, 'grant': grant_data, 'execution_mode': mode,
        }), encoding='utf-8')

    def create_task(self):
        return runtime.tick(self.state, self.grant, credential_reader=lambda: 'fixture-key',
                            lock=unlocked_fixture, clock=self.clock)

    def snapshot(self):
        return {path.relative_to(self.root): path.read_bytes()
                for path in self.root.rglob('*') if path.is_file()}

    def run_status(self, *args, verify_error=None):
        out, err = StringIO(), StringIO()
        prohibited = ('read_systemd_key', 'complete_bounded', 'tick', 'check', '_read_proof')
        with ExitStack() as stack:
            guards = [stack.enter_context(patch.object(runtime, name, side_effect=AssertionError(name)))
                      for name in prohibited]
            with (patch.object(runtime, 'CONFIG_PATH', self.config),
                  patch.object(runtime, 'STATE_PATH', self.state),
                  patch.object(runtime, '_verify_service', side_effect=verify_error) as verify,
                  patch('sys.argv', ['readonly_status', *args]),
                  patch('sys.stdin', None),
                  patch.dict(os.environ, {'SSH_ORIGINAL_COMMAND': 'tick SHELL_PAYLOAD'}),
                  redirect_stdout(out), redirect_stderr(err)):
                code = readonly_status.main()
        for guard in guards:
            guard.assert_not_called()
        if args:
            verify.assert_not_called()
        else:
            verify.assert_called_once_with(self.config, self.state)
        raw = out.getvalue()
        self.assertEqual(err.getvalue(), '')
        self.assertEqual(raw.count('\n'), 1)
        self.assertLessEqual(len(raw.encode('utf-8')), readonly_status.OUTPUT_LIMIT)
        return code, json.loads(raw), raw

    def mutate_result(self, change):
        with closing(sqlite3.connect(self.ledger)) as connection, connection:
            raw, = connection.execute('SELECT result_json FROM tasks').fetchone()
            result = json.loads(raw)
            change(result)
            connection.execute('UPDATE tasks SET result_json=?', (json.dumps(result),))

    def test_status_of_real_task_does_not_change_any_business_file(self):
        expected = self.create_task()
        before = self.snapshot()
        code, status, raw = self.run_status()
        self.assertEqual(code, 0)
        self.assertEqual(status, {
            'schema_version': 1, 'ok': True, 'task_id': expected['task_id'],
            'status': 'succeeded', 'scheduled_at': self.grant.scheduled_at,
            'requested_model': self.grant.model, 'execution_mode': 'simulation',
            'usage': expected['usage'], 'cost': None, 'result_ref': expected['result_ref'],
            'ledger_exists': True, 'real_calls': 0,
        })
        self.assertNotIn('PRIVATE_PROMPT_FIXTURE', raw)
        self.assertEqual(self.snapshot(), before)

    def test_missing_ledger_stays_missing(self):
        before = self.snapshot()
        code, status, _ = self.run_status()
        self.assertEqual(code, 0)
        self.assertEqual(status['status'], 'not_submitted')
        self.assertIsNone(status['task_id'])
        self.assertFalse(status['ledger_exists'])
        self.assertFalse(self.ledger.exists())
        self.assertEqual(self.snapshot(), before)

    def test_cli_arguments_rejected_with_fixed_error(self):
        for args in [('tick',), ('check',), ('install',), ('--config', 'PRIVATE_PATH'), ('--help',)]:
            with self.subTest(args=args):
                code, status, _ = self.run_status(*args)
                self.assertEqual((code, status), (2, {
                    'schema_version': 1, 'ok': False, 'error': 'r1_status_invalid_arguments',
                }))

    def test_unknown_fields_and_provider_model_are_omitted(self):
        self.create_task()

        def inject(result):
            result.update(model='SecretTokenShapedLikeAValidModel', credential_read_proof='PRIVATE_PROOF',
                          result_body='PRIVATE_RESULT', cost='PRIVATE_COST')
            result['usage']['unknown_counter'] = 'PRIVATE_METERING'

        self.mutate_result(inject)
        before = self.snapshot()
        code, status, raw = self.run_status()
        self.assertEqual(code, 0)
        self.assertEqual(status['requested_model'], self.grant.model)
        self.assertIsNone(status['cost'])
        for marker in ('SecretToken', 'PRIVATE_', 'credential_read_proof', 'unknown_counter'):
            self.assertNotIn(marker, raw)
        self.assertEqual(self.snapshot(), before)

    def test_invalid_known_fields_and_verification_errors_are_redacted(self):
        expected = self.create_task()
        for field, value in [('usage', {'prompt_tokens': 'PRIVATE_COUNTER'}),
                             ('result_ref', 'PRIVATE_RESULT_BODY' * 500)]:
            with self.subTest(field=field):
                self.mutate_result(lambda result: result.update({field: value}))
                before = self.snapshot()
                code, status, _ = self.run_status()
                self.assertEqual((code, status), (1, {
                    'schema_version': 1, 'ok': False, 'error': 'r1_status_refused',
                }))
                self.assertEqual(self.snapshot(), before)
                self.mutate_result(lambda result: result.update({field: expected[field]}))
        code, status, _ = self.run_status(verify_error=RuntimeError('PRIVATE_RUNTIME_ERROR'))
        self.assertEqual((code, status), (1, {
            'schema_version': 1, 'ok': False, 'error': 'r1_status_refused',
        }))
