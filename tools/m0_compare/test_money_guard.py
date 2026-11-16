"""Account-balance stop rules exercised entirely with fake balance and transport."""

from decimal import Decimal
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

from .money_guard import MoneyGuard, MoneyRefused


PAYLOAD = {'model': 'deepseek-flash', 'max_tokens': 64, 'thinking': {'type': 'disabled'},
           'stream': False, 'messages': [{'role': 'user', 'content': 'synthetic-only prompt marker'}]}
REPLY = {'usage': {'prompt_tokens': 10, 'completion_tokens': 5, 'total_tokens': 15},
         'choices': [{'message': {'role': 'assistant', 'content': 'synthetic-only response marker'}}]}


def balance(value, currency='CNY'):
    return {'is_available': True, 'balance_infos': [{'currency': currency, 'total_balance': value}]}


class MoneyGuardTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory())).resolve()
        self.path = self.root / 'comparison-money.json'

    def guard(self, balances, *, path=None, forward=None):
        reader = Mock(side_effect=balances)
        target = Mock(return_value=REPLY) if forward is None else forward
        return MoneyGuard(path or self.path, balance=reader, forward=target, credential_id='fixture-credential-id')

    def state(self, path=None):
        return json.loads((path or self.path).read_bytes())

    def test_initial_headroom_rejects_before_forward_in_both_supported_currencies(self):
        for currency, amount in (('CNY', '0.19'), ('USD', '0.029')):
            with self.subTest(currency=currency):
                path = self.root / f'{currency}.json'
                guard = self.guard([balance(amount, currency)], path=path)
                with self.assertRaises(MoneyRefused):
                    guard.perform('worker', PAYLOAD)
                guard.forward.assert_not_called()
                self.assertEqual(self.state(path)['state'], 'closed_budget')

    def test_cny_and_usd_stop_at_exact_cumulative_headroom_threshold(self):
        for currency, before, after, drop in (('CNY', '3', '2.2', '0.8'),
                                              ('USD', '2', '1.88', '0.12')):
            with self.subTest(currency=currency):
                path = self.root / f'{currency}.json'
                guard = self.guard([balance(before, currency), balance(after, currency), balance(after, currency)],
                                   path=path)
                self.assertEqual(guard.perform('worker', PAYLOAD), REPLY)
                value = self.state(path)
                self.assertEqual(value['state'], 'closed_budget')
                self.assertEqual(Decimal(value['observed_drop']), Decimal(drop))
                self.assertEqual(value['calls'][0]['state'], 'returned')
                with self.assertRaises(MoneyRefused):
                    guard.perform('hermes', PAYLOAD)
                guard.forward.assert_called_once_with(PAYLOAD)

    def test_topup_before_next_call_closes_without_forward(self):
        guard = self.guard([balance('10'), balance('9.90'), balance('10.10')])
        guard.perform('worker', PAYLOAD)
        with self.assertRaises(MoneyRefused):
            guard.perform('hermes', PAYLOAD)
        self.assertEqual(self.state()['state'], 'closed_balance_increase')
        self.assertEqual(Decimal(self.state()['observed_drop']), Decimal('0.1'))
        self.assertEqual(len(self.state()['calls']), 1)
        guard.forward.assert_called_once_with(PAYLOAD)

    def test_topup_during_call_closes_unknown_and_does_not_release_reply(self):
        guard = self.guard([balance('10'), balance('11'), balance('11')])
        with self.assertRaises(MoneyRefused):
            guard.perform('worker', PAYLOAD)
        current = self.state()
        self.assertEqual(current['state'], 'closed_unknown')
        self.assertEqual(current['calls'][0]['state'], 'sent')
        self.assertNotIn('after', current['calls'][0])
        with self.assertRaises(MoneyRefused):
            guard.perform('hermes', PAYLOAD)
        guard.forward.assert_called_once_with(PAYLOAD)

    def test_process_exit_leaves_durable_inflight_and_reopen_does_not_resend(self):
        program = '''
import json, os, sys
from tools.m0_compare.money_guard import MoneyGuard

def balance():
    return {'is_available': True, 'balance_infos': [{'currency': 'CNY', 'total_balance': '10'}]}

def terminate(_):
    os._exit(97)

guard = MoneyGuard(sys.argv[1], balance=balance, forward=terminate, credential_id='fixture-credential-id')
guard.perform('worker', json.loads(sys.argv[2]))
'''
        process = subprocess.run([sys.executable, '-B', '-c', program, str(self.path), json.dumps(PAYLOAD)],
                                 cwd=Path(__file__).resolve().parents[2], capture_output=True, timeout=15)
        self.assertEqual(process.returncode, 97, process.stderr.decode(errors='replace'))
        original = self.path.read_bytes()
        self.assertEqual(self.state()['state'], 'inflight')
        self.assertEqual(self.state()['calls'][0]['state'], 'sent')
        reopened = self.guard([balance('10'), balance('10')])
        for arm in ('worker', 'hermes'):
            with self.assertRaises(MoneyRefused):
                reopened.perform(arm, PAYLOAD)
        reopened.forward.assert_not_called()
        self.assertEqual(self.path.read_bytes(), original)

    def test_twenty_calls_per_arm_without_resetting_shared_balance(self):
        guard = self.guard([balance('10')] * 43)
        for _ in range(20):
            guard.perform('worker', PAYLOAD)
        current = self.path.read_bytes()
        with self.assertRaises(MoneyRefused):
            guard.perform('worker', PAYLOAD)
        self.assertEqual(self.path.read_bytes(), current)
        self.assertEqual(guard.forward.call_count, 20)
        guard.perform('hermes', PAYLOAD)
        self.assertEqual(guard.forward.call_count, 21)
        self.assertEqual([item['arm'] for item in self.state()['calls']], ['worker'] * 20 + ['hermes'])

    def test_shared_account_drop_closes_both_arms_before_additional_forward(self):
        guard = self.guard([balance(value) for value in ('10', '9.65', '9.65', '9.30', '9.20', '9.20')])
        guard.perform('worker', PAYLOAD)
        guard.perform('hermes', PAYLOAD)
        for arm in ('worker', 'hermes'):
            with self.assertRaises(MoneyRefused):
                guard.perform(arm, PAYLOAD)
        current = self.state()
        self.assertEqual(current['state'], 'closed_budget')
        self.assertEqual(Decimal(current['observed_drop']), Decimal('0.8'))
        self.assertEqual([item['arm'] for item in current['calls']], ['worker', 'hermes'])
        self.assertEqual(guard.forward.call_count, 2)

    def test_provider_exception_preserves_unknown_and_reopen_never_resends(self):
        transport = Mock(side_effect=TimeoutError('fixture timeout'))
        guard = self.guard([balance('10')], forward=transport)
        with self.assertRaises(TimeoutError):
            guard.perform('worker', PAYLOAD)
        self.assertEqual(self.state()['state'], 'closed_unknown')
        self.assertEqual(self.state()['calls'][0]['state'], 'sent')
        reopened = self.guard([balance('10')])
        with self.assertRaises(MoneyRefused):
            reopened.perform('worker', PAYLOAD)
        reopened.forward.assert_not_called()
        transport.assert_called_once_with(PAYLOAD)

    def test_balance_failure_after_provider_reply_closes_unknown_and_keeps_metadata_only(self):
        guard = self.guard([balance('10'), {'is_available': False}, balance('10')])
        with self.assertRaises(MoneyRefused):
            guard.perform('worker', PAYLOAD)
        value = self.state()
        self.assertEqual(value['state'], 'closed_unknown')
        self.assertEqual(value['calls'][0]['usage'], REPLY['usage'])
        self.assertEqual(value['calls'][0]['state'], 'sent')
        raw = self.path.read_text(encoding='utf-8')
        self.assertNotIn('synthetic-only prompt marker', raw)
        self.assertNotIn('synthetic-only response marker', raw)
        with self.assertRaises(MoneyRefused):
            guard.perform('hermes', PAYLOAD)
        guard.forward.assert_called_once_with(PAYLOAD)

    def test_receipt_projects_known_usage_fields_only(self):
        reply = REPLY | {'usage': REPLY['usage'] | {'extra': {'message': 'unexpected provider text'}}}
        guard = self.guard([balance('10'), balance('9.99')], forward=Mock(return_value=reply))
        self.assertEqual(guard.perform('worker', PAYLOAD), reply)
        self.assertEqual(self.state()['calls'][0]['usage'], REPLY['usage'])
        self.assertNotIn('unexpected provider text', self.path.read_text(encoding='utf-8'))

    def test_missing_inconsistent_or_unbounded_usage_never_releases_response(self):
        cases = (None, {}, REPLY['usage'] | {'prompt_tokens': True},
                 REPLY['usage'] | {'prompt_tokens': -1},
                 REPLY['usage'] | {'prompt_tokens': 2**31}, REPLY['usage'] | {'total_tokens': 99},
                 REPLY['usage'] | {'prompt_tokens': 'unexpected provider text'})
        for index, usage in enumerate(cases):
            with self.subTest(usage=usage):
                path = self.root / f'malformed-{index}.json'
                guard = self.guard([balance('10'), balance('10')], path=path,
                                   forward=Mock(return_value=REPLY | {'usage': usage}))
                with self.assertRaises(MoneyRefused):
                    guard.perform('worker', PAYLOAD)
                value = self.state(path)
                self.assertEqual(value['state'], 'closed_unknown')
                self.assertNotIn('usage', value['calls'][0])
                with self.assertRaises(MoneyRefused):
                    guard.perform('hermes', PAYLOAD)
                guard.forward.assert_called_once_with(PAYLOAD)

    def test_overrun_closes_unknown_but_preserves_known_numeric_usage(self):
        usage = {'prompt_tokens': 10, 'completion_tokens': 513, 'total_tokens': 523}
        guard = self.guard([balance('10')], forward=Mock(return_value=REPLY | {'usage': usage}))
        with self.assertRaises(MoneyRefused):
            guard.perform('worker', PAYLOAD | {'max_tokens': 512})
        self.assertEqual(self.state()['state'], 'closed_unknown')
        self.assertEqual(self.state()['calls'][0]['usage'], usage)


if __name__ == '__main__':
    unittest.main()
