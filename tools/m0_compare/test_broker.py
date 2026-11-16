"""The comparison broker is tested only with injected non-network forwarders."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from unittest.mock import Mock, patch

from dispatch.sa_dispatch.artifacts import ArtifactStore
from dispatch.sa_dispatch.canonical import canonical_json, sha256_ref

from . import broker


METHOD = 'b' * 64
PROMPT = 'Compare the synthetic fixture evidence.'
QUERY = {'q': '剪辑 返工', 'domains': ['research-scatter', 'research-content'], 'limit': 2}
RESTRICTED = 'research-scatter/note/1TKW8H659WK91W9FWWSA8C7VYQ@1#40c7fc2e1740dddbc0623ba72c338692e2cd02dd639c48be1d1072ff77e657d4'


def completion(*, text='synthetic result', call=None, usage=True, finish='stop'):
    message = {'role': 'assistant', 'content': text}
    if call is not None:
        name, args = call
        message = {'role': 'assistant', 'content': None, 'tool_calls': [
            dict(id='call_1', type='function', function={'name': name, 'arguments': canonical_json(args).decode()})]}
        finish = 'tool_calls'
    result = dict(id='fixture-completion', object='chat.completion', created=1, model=broker.MODEL,
                  choices=[dict(index=0, finish_reason=finish, message=message)])
    if usage:
        result['usage'] = {'prompt_tokens': 11, 'completion_tokens': 7, 'total_tokens': 18}
    return result


class BrokerTests(unittest.TestCase):
    def setUp(self):
        root = Path(self.enterContext(TemporaryDirectory())).resolve()
        self.cas = ArtifactStore(root / 'cas')
        self.ledger = broker.RequestLedger(root / 'compare.sqlite3', self.cas, 'fixture-broker',
                                           count_method_sha256=METHOD, create=True)
        self.forward = Mock(return_value=completion() | {'provider_header': 'DO_NOT_EXPORT'})
        self.proof = Mock(return_value={'input_upper_bound': 30, 'method_sha256': METHOD})
        self.verify = Mock(return_value=True)
        self.bridge = self.make_broker()

    def make_broker(self, **options):
        configuration = {'forward': self.forward, 'count_proof': self.proof, 'verify_count_proof': self.verify,
                         'mode': 'fixture'} | options
        return broker.Broker(self.ledger, self.cas, **configuration)

    def run_binding(self, *, tools=False, name='run-1', arm='hermes', **options):
        return self.bridge.new_run(arm=arm, run_id=name, approved_prompt=PROMPT, tools_enabled=tools, **options)

    def payload(self, *, tools=False):
        payload = {'model': broker.MODEL, 'max_tokens': 64, 'think': False,
                   'messages': [{'role': 'system', 'content': 'Fixed read-only research scope.'},
                                {'role': 'user', 'content': PROMPT}]}
        if tools:
            payload['tools'] = [{'type': 'function', 'function': schema} for schema in reversed(broker.tool_schemas())]
        return payload

    def continuation(self, initial, result, tool_name, tool_reply):
        message = deepcopy(result['response']['choices'][0]['message'])
        message['reasoning_content'] = ' '  # Pinned Hermes DeepSeek tool-history pad.
        return initial | {'messages': initial['messages'] + [message, {'role': 'tool', 'name': tool_name,
                                'tool_call_id': 'call_1', 'content': canonical_json(tool_reply).decode()}]}

    def test_normalizes_accounts_and_replays_without_new_proof_or_provider_call(self):
        run = self.run_binding(arm='current_worker')
        request = self.payload()
        result = run.complete(0, request)
        self.assertEqual(result['state'], 'settled')
        self.assertNotIn('provider_header', result['response'])
        outbound, = self.forward.call_args.args
        self.assertNotIn('think', outbound)
        self.assertEqual(outbound['thinking'], {'type': 'disabled'})
        self.assertIs(outbound['stream'], False)
        proof, exact = self.verify.call_args.args
        self.assertEqual(exact, canonical_json(outbound))
        self.assertEqual(proof['payload_hash'], sha256_ref(exact))
        self.assertEqual(self.verify.call_args.kwargs['scope'], 'synthetic_fixture')
        result['response']['choices'][0]['message']['content'] = 'host mutation'
        cached = run.complete(0, request | {'stream': False})
        self.assertEqual((cached['state'], cached['response']['choices'][0]['message']['content']),
                         ('cached', 'synthetic result'))
        self.forward.assert_called_once()
        self.proof.assert_called_once()
        ledger = self.ledger.status('worker')['arms']['worker']
        self.assertEqual({key: value['used_units'] for key, value in ledger['budgets'].items()},
                         {'provider_requests': 1, 'input_tokens': 11, 'output_tokens': 7})
        self.assertEqual(run.complete(0, request | {'max_tokens': 63})['code'], 'SEQUENCE_CONFLICT')
        self.assertEqual(run.complete(1, request)['code'], 'RUN_ALREADY_FINISHED')
        with self.assertRaisesRegex(broker.BrokerError, 'RUN_REPLAY_UNAVAILABLE'):
            self.make_broker().new_run(arm='worker', run_id='run-1', approved_prompt=PROMPT, tools_enabled=False)

    def test_cache_and_post_proof_send_recheck_current_permissions(self):
        run = self.run_binding()
        self.assertEqual(run.complete(0, self.payload())['state'], 'settled')
        with patch.object(run.fixture, 'revalidate_refs', return_value=False):
            self.assertEqual(run.complete(0, self.payload())['code'], 'REFERENCE_NOT_VISIBLE')
        second = self.run_binding(name='run-2')
        with patch.object(second.fixture, 'revalidate_refs', side_effect=[True, False]):
            self.assertEqual(second.complete(0, self.payload())['code'], 'REFERENCE_NOT_VISIBLE')
        self.forward.assert_called_once()

    def test_denies_unknown_features_tools_and_prompt_changes_before_reservation(self):
        run = self.run_binding(tools=True)
        valid = self.payload(tools=True)
        cases = (
            {'model': 'other-model'}, {'stream': True}, {'max_tokens': 513}, {'max_tokens': True},
            {'thinking': {'type': 'enabled'}}, {'think': True}, {'metadata': {'arbitrary': 'data'}},
            {'tools': valid['tools'] + [{'type': 'function', 'function': {'name': 'shell'}}]},
            {'tools': [valid['tools'][0], valid['tools'][0]]},
            {'messages': [{'role': 'user', 'content': 'unapproved prompt'}]},
            {'messages': [{'role': 'user', 'content': [{'type': 'image_url', 'image_url': {}}]}]},
        )
        for extra in cases:
            with self.subTest(keys=list(extra)):
                self.assertEqual(run.complete(0, valid | extra)['state'], 'rejected')
        self.forward.assert_not_called()
        self.proof.assert_not_called()
        self.assertEqual(self.ledger.status('hermes')['arms']['hermes']['requests'], [])

    def test_tool_result_must_match_real_receipt_and_is_revalidated_before_forwarding(self):
        audit = []
        run = self.run_binding(tools=True, audit=audit.append)
        initial = self.payload(tools=True)
        self.forward.side_effect = [completion(call=('knowledge_search', QUERY)), completion()]
        first = run.complete(0, initial)
        self.assertEqual(first['state'], 'settled')
        self.assertEqual(run.tool(0, 'knowledge_search', QUERY | {'limit': 1})['error']['code'], 'TOOL_CALL_REFUSED')
        reply = run.tool(1, 'knowledge_search', QUERY)
        self.assertTrue(reply['ok'])
        self.assertEqual(run.tool(1, 'knowledge_search', QUERY), reply)
        actual = self.continuation(initial, first, 'knowledge_search', reply)
        forged = self.continuation(initial, first, 'knowledge_search', {'ok': True, 'results': ['forged']})
        self.assertEqual(run.complete(1, forged)['code'], 'TRANSCRIPT_BINDING_REFUSED')
        for padding in ('  ', '\n', 'untrusted internal reasoning'):
            reasoning = deepcopy(actual)
            reasoning['messages'][2]['reasoning_content'] = padding
            self.assertEqual(run.complete(1, reasoning)['code'], 'REQUEST_REFUSED')
        with patch.object(run.fixture, 'revalidate_refs', return_value=False):
            self.assertEqual(run.complete(1, actual)['code'], 'REFERENCE_NOT_VISIBLE')
            self.assertEqual(run.tool(1, 'knowledge_search', QUERY)['error']['code'], 'REFERENCE_NOT_VISIBLE')
        self.assertEqual(run.complete(1, actual)['state'], 'settled')
        self.assertNotIn('reasoning_content', self.forward.call_args.args[0]['messages'][2])
        self.assertEqual(self.forward.call_count, 2)
        self.assertEqual([entry['code'] for entry in audit], ['TOOL_CALL_REFUSED', 'OK'])
        self.assertTrue(run.fixture.successful_refs())

    def test_real_restricted_fixture_ref_has_server_denial(self):
        audit = []
        run = self.run_binding(tools=True, audit=audit.append)
        args = {'domains': ['research-scatter'], 'ref': RESTRICTED}
        self.forward.return_value = completion(call=('knowledge_get', args))
        self.assertEqual(run.complete(0, self.payload(tools=True))['state'], 'settled')
        reply = run.tool(0, 'knowledge_get', args)
        self.assertEqual(reply, {'ok': False, 'error': {'code': 'NOT_FOUND', 'retryable': False}})
        self.assertEqual((audit[0]['code'], audit[0]['requested_ref'], audit[0]['delivered_refs']),
                         ('NOT_FOUND', RESTRICTED, []))

    def test_nonstream_provider_tool_position_is_checked_and_removed(self):
        wire = completion(call=('knowledge_search', QUERY))
        wire['system_fingerprint'] = 'synthetic-fingerprint'
        wire['choices'][0]['logprobs'] = None
        call = wire['choices'][0]['message']['tool_calls'][0]
        call['index'] = 0
        wire['usage'].update(prompt_tokens_details={'cached_tokens': 0},
                             prompt_cache_hit_tokens=0, prompt_cache_miss_tokens=11)
        for malformed in (-1, True, '0', 1):
            with self.subTest(index=malformed):
                invalid = deepcopy(wire)
                invalid['choices'][0]['message']['tool_calls'][0]['index'] = malformed
                with self.assertRaises(broker.BrokerError):
                    broker._provider_response(invalid, tools_enabled=True)
        double = deepcopy(wire)
        second = deepcopy(call) | {'id': 'call_2', 'index': 1}
        double['choices'][0]['message']['tool_calls'].append(second)
        clean, _ = broker._provider_response(double, tools_enabled=True)
        self.assertEqual([item['id'] for item in clean['choices'][0]['message']['tool_calls']], ['call_1', 'call_2'])
        self.assertTrue(all('index' not in item for item in clean['choices'][0]['message']['tool_calls']))

        self.forward.side_effect = [wire, completion()]
        run = self.run_binding(tools=True)
        initial = self.payload(tools=True)
        first = run.complete(0, initial)
        self.assertEqual(first['state'], 'settled')
        self.assertNotIn('index', first['response']['choices'][0]['message']['tool_calls'][0])
        reply = run.tool(0, 'knowledge_search', QUERY)
        self.assertTrue(reply['ok'])
        self.assertEqual(run.complete(1, self.continuation(initial, first, 'knowledge_search', reply))['state'], 'settled')

    def test_transport_error_is_unknown_and_not_replayed(self):
        self.forward.side_effect = OSError('RAW ERROR NEEDS REDACTION')
        run = self.run_binding()
        request = self.payload()
        expected = {'state': 'unknown', 'response': None, 'code': 'REQUEST_OUTCOME_UNKNOWN'}
        self.assertEqual(run.complete(0, request), expected)
        self.assertEqual(run.complete(0, request), expected)
        other = self.run_binding(name='run-2')
        self.assertEqual(other.complete(0, request)['code'], 'ARM_BLOCKED')
        self.forward.assert_called_once()
        ledger = self.ledger.status('hermes')['arms']['hermes']
        self.assertEqual(ledger['state'], 'closed_unknown')
        self.assertEqual(ledger['budgets']['provider_requests']['held_units'], 1)

    def test_missing_usage_or_usage_exceeding_bound_keeps_reservation_held(self):
        for arm, result in (
            ('worker', completion(usage=False)),
            ('hermes', completion() | {'usage': {'prompt_tokens': 31, 'completion_tokens': 1, 'total_tokens': 32}}),
        ):
            self.forward.return_value = result
            run = self.run_binding(arm=arm)
            self.assertEqual(run.complete(0, self.payload())['state'], 'unknown')
            ledger = self.ledger.status(arm)['arms'][arm]
            self.assertEqual(ledger['budgets']['input_tokens']['held_units'], 30)
            self.assertEqual(ledger['budgets']['input_tokens']['used_units'], 0)

    def test_length_is_settled_for_usage_but_not_returned_as_complete(self):
        self.forward.return_value = completion(finish='length')
        run = self.run_binding()
        expected = {'response': None, 'code': 'OUTPUT_TRUNCATED'}
        self.assertEqual(run.complete(0, self.payload()), expected | {'state': 'settled'})
        self.assertEqual(run.complete(0, self.payload()), expected | {'state': 'cached'})
        ledger = self.ledger.status('hermes')['arms']['hermes']
        self.assertEqual(ledger['budgets']['output_tokens']['used_units'], 7)
        self.forward.assert_called_once()

    def test_live_disabled_by_default_and_approved_mode_verifies_distinct_scope(self):
        settings = dict(forward=self.forward, count_proof=self.proof, verify_count_proof=self.verify)
        unconfigured = broker.Broker(self.ledger, self.cas, **settings)
        run = unconfigured.new_run(arm='hermes', run_id='disabled', approved_prompt=PROMPT, tools_enabled=False)
        self.assertEqual(run.complete(0, self.payload())['code'], 'FORWARDING_DISABLED')
        with self.assertRaises(broker.BrokerError):
            self.make_broker(mode='live')
        deny = self.make_broker(mode='live', authorize_live=lambda *_: False)
        run = deny.new_run(arm='hermes', run_id='denied', approved_prompt=PROMPT, tools_enabled=False)
        self.assertEqual(run.complete(0, self.payload())['code'], 'LIVE_ACCOUNTING_NOT_AUTHORIZED')
        self.proof.assert_not_called()
        approved = self.make_broker(mode='live', authorize_live=lambda *_: True)
        self.verify.side_effect = lambda *args, **kwargs: kwargs['scope'] == 'synthetic_fixture'
        run = approved.new_run(arm='hermes', run_id='no-proof', approved_prompt=PROMPT, tools_enabled=False)
        self.assertEqual(run.complete(0, self.payload())['code'], 'COUNT_PROOF_REFUSED')
        self.assertEqual(self.verify.call_args.kwargs['scope'], 'authorized_live_estimate')
        self.forward.assert_not_called()

    def test_revocation_during_count_proof_closes_forward_gate(self):
        authorized = True
        self.bridge = self.make_broker(mode='live', authorize_live=lambda *_: authorized)

        def verify(*args, **kwargs):
            nonlocal authorized
            authorized = False
            return True

        self.verify.side_effect = verify
        run = self.run_binding()
        self.assertEqual(run.complete(0, self.payload())['code'], 'LIVE_ACCOUNTING_NOT_AUTHORIZED')
        self.assertEqual(self.ledger.status('hermes')['arms']['hermes']['requests'], [])
        self.forward.assert_not_called()

    def test_parallel_duplicate_cannot_forward_again(self):
        running, release = Event(), Event()

        def forward(_):
            running.set()
            self.assertTrue(release.wait(2))
            return completion()

        self.forward.side_effect = forward
        run = self.run_binding()
        with ThreadPoolExecutor(max_workers=1) as pool:
            active = pool.submit(run.complete, 0, self.payload())
            self.assertTrue(running.wait(2))
            self.assertEqual(run.complete(0, self.payload())['state'], 'inflight')
            release.set()
            self.assertEqual(active.result(timeout=2)['state'], 'settled')
        self.forward.assert_called_once()


if __name__ == '__main__':
    unittest.main()
