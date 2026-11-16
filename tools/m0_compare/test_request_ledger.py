"""Real isolated SQLite/CAS tests for the comparison request budget and fences."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Barrier
import unittest

from dispatch.sa_dispatch.artifacts import ArtifactStore
from dispatch.sa_dispatch.canonical import canonical_json, sha256_ref
from dispatch.sa_dispatch.clock import FakeClock
from dispatch.sa_dispatch.models import FenceRejected, IdempotencyConflict
from dispatch.sa_dispatch.store_sqlite import SqliteStore

from . import request_ledger as compare


class RequestLedgerTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory())).resolve()
        self.path = self.root / 'ledger.sqlite3'
        self.cas = ArtifactStore(self.root / 'cas')
        self.clock = FakeClock()
        self.count_method = 'b' * 64
        self.ledger = compare.RequestLedger(self.path, self.cas, 'fixture-experiment',
                                            count_method_sha256=self.count_method, create=True, clock=self.clock)
        self.reply = self.cas.put(b'public synthetic provider result')

    def args(self, sequence=0, *, arm='worker', run_id='round-one', bound=10, maximum=30, content=b'payload'):
        payload = sha256_ref(content)
        proof = self.cas.put(canonical_json({
            'schema': compare.COUNT_SCHEMA, 'payload_hash': payload, 'model': 'deepseek-flash',
            'input_upper_bound': bound, 'method_sha256': self.count_method,
        }))
        return dict(arm=arm, run_id=run_id, sequence=sequence, payload_hash=payload,
                    input_upper_bound=bound, max_output_tokens=maximum, count_proof_ref=proof)

    def reserve(self, *args, **kwargs):
        return self.ledger.reserve(**self.args(*args, **kwargs))

    def finish(self, held, *, inputs=6, outputs=12):
        self.ledger.mark_sent(held['lease_id'], held['fence'])
        return self.ledger.settle(held['lease_id'], held['fence'], response_ref=self.reply,
                                  usage={'input_tokens': inputs, 'output_tokens': outputs})

    def status(self, arm='worker'):
        return self.ledger.status(arm)['arms'][arm]

    def test_case_identity_sent_gate_settlement_and_cache(self):
        held = self.reserve()
        self.assertEqual((held['state'], held['fence']), ('reserved', 1))
        self.assertEqual(self.reserve()['state'], 'inflight')
        self.assertNotIn('lease_id', self.reserve())
        with self.assertRaises(IdempotencyConflict):
            self.reserve(content=b'changed')
        with self.assertRaisesRegex(compare.RequestLedgerError, 'E_COMPARE_NOT_SENT'):
            self.ledger.settle(held['lease_id'], 1, response_ref=self.reply,
                               usage={'input_tokens': 6, 'output_tokens': 12})
        settled = self.finish(held)
        self.assertEqual((settled['state'], settled['response_ref']), ('settled', self.reply))
        cached = self.reserve()
        self.assertEqual(cached, settled | {'state': 'cached'})
        self.assertNotIn('lease_id', cached)
        self.assertEqual(self.ledger.settle(held['lease_id'], 1, response_ref=self.reply,
                                            usage={'input_tokens': 6, 'output_tokens': 12}), cached)
        budget = self.status()['budgets']
        self.assertEqual({unit: (row['used_units'], row['held_units']) for unit, row in budget.items()},
                         {'provider_requests': (1, 0), 'input_tokens': (6, 0), 'output_tokens': (12, 0)})
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT access,egress,side_effect,attempts,max_attempts,fence FROM tasks')
                             .fetchone(), ('exportable', 'derived', 'external', 1, 1, 1))
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM control_reservations').fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM control_events WHERE kind='compare.request_settled'")
                             .fetchone()[0], 1)

    def test_request_cap_blocks_twenty_first_without_appending_task(self):
        for sequence in range(20):
            self.finish(self.reserve(sequence), inputs=1, outputs=1)
        snapshot = self.status()
        self.assertEqual(snapshot['budgets']['provider_requests']['used_units'], 20)
        with self.assertRaisesRegex(compare.RequestLedgerError, 'BUDGET_EXHAUSTED'):
            self.reserve(20)
        self.assertEqual(self.status(), snapshot)
        self.assertEqual(self.reserve(0)['state'], 'cached')
        self.assertEqual(self.reserve(1, arm='hermes')['state'], 'reserved')

    def test_each_token_limit_and_invalid_max_output_reservation(self):
        held = self.reserve(bound=32_000, maximum=512)
        self.finish(held, inputs=32_000, outputs=1)
        with self.assertRaisesRegex(compare.RequestLedgerError, 'BUDGET_EXHAUSTED'):
            self.reserve(1, bound=1)
        with self.assertRaisesRegex(compare.RequestLedgerError, 'INVALID_RESERVATION'):
            self.reserve(1, arm='hermes', maximum=513)
        for sequence in range(15):
            self.finish(self.reserve(sequence, arm='hermes', bound=1, maximum=512), inputs=1, outputs=512)
        self.finish(self.reserve(15, arm='hermes', bound=1, maximum=320), inputs=1, outputs=320)
        self.assertEqual(self.status('hermes')['budgets']['output_tokens']['used_units'], 8000)
        with self.assertRaisesRegex(compare.RequestLedgerError, 'BUDGET_EXHAUSTED'):
            self.reserve(16, arm='hermes', bound=1, maximum=1)
        self.assertEqual(self.status('hermes')['budgets']['provider_requests']['used_units'], 16)

    def test_concurrent_same_arm_claim_and_send_permit_are_single(self):
        barrier = Barrier(2)
        arguments_by_sequence = {sequence: self.args(sequence) for sequence in (0, 1)}

        def attempt(sequence):
            arguments = arguments_by_sequence[sequence]
            barrier.wait(timeout=5)
            try:
                return self.ledger.reserve(**arguments)
            except compare.RequestLedgerError as error:
                return {'state': error.code}

        with ThreadPoolExecutor(max_workers=2) as pool:
            result = list(pool.map(attempt, (0, 1)))
        self.assertEqual(sorted(item['state'] for item in result), ['E_COMPARE_ARM_BLOCKED', 'reserved'])
        held = next(item for item in result if item['state'] == 'reserved')
        self.ledger.mark_sent(held['lease_id'], 1)
        with self.assertRaisesRegex(compare.RequestLedgerError, 'ALREADY_SENT'):
            self.ledger.mark_sent(held['lease_id'], 1)

    def test_restart_and_expired_fence_require_explicit_unknown_and_preserve_all_held(self):
        held = self.reserve(bound=20, maximum=40)
        self.ledger.mark_sent(held['lease_id'], 1)
        self.clock.advance(121)
        reopened = compare.RequestLedger(self.path, self.cas, 'fixture-experiment',
                                          count_method_sha256=self.count_method, clock=self.clock)
        self.assertEqual(reopened.status('worker')['arms']['worker']['state'], 'inflight')
        for operation in (reopened.mark_sent, lambda lease, fence: reopened.settle(
                lease, fence, response_ref=self.reply, usage={'input_tokens': 1, 'output_tokens': 1})):
            with self.assertRaises(FenceRejected):
                operation(held['lease_id'], 1)
        self.assertEqual(reopened.recover_unknown('worker')['state'], 'unknown')
        self.assertIsNone(reopened.recover_unknown('worker'))
        self.assertEqual(self.reserve(bound=20, maximum=40)['state'], 'unknown')
        with self.assertRaisesRegex(compare.RequestLedgerError, 'ARM_BLOCKED'):
            self.reserve(1, run_id='different-run')
        budgets = self.status()['budgets']
        self.assertEqual({unit: (row['used_units'], row['held_units']) for unit, row in budgets.items()},
                         {'provider_requests': (0, 1), 'input_tokens': (0, 20), 'output_tokens': (0, 40)})
        self.assertEqual(self.status()['state'], 'closed_unknown')

    def test_invalid_usage_fence_and_count_proof_leave_reserved_state_intact(self):
        arguments = self.args()
        with self.assertRaisesRegex(compare.RequestLedgerError, 'COUNT_PROOF'):
            self.ledger.reserve(**(arguments | {'input_upper_bound': 11}))
        self.assertEqual(self.status()['requests'], [])
        held = self.ledger.reserve(**arguments)
        self.ledger.mark_sent(held['lease_id'], 1)
        snapshot = self.status()
        for usage in (None, {'input_tokens': 2}, {'input_tokens': True, 'output_tokens': 1},
                      {'input_tokens': 11, 'output_tokens': 1}, {'input_tokens': 2, 'output_tokens': 31}):
            with self.assertRaisesRegex(compare.RequestLedgerError, 'USAGE'):
                self.ledger.settle(held['lease_id'], 1, response_ref=self.reply, usage=usage)
        with self.assertRaises(FenceRejected):
            self.ledger.mark_unknown(held['lease_id'], 2)
        self.assertEqual(self.status(), snapshot)
        first = self.ledger.mark_unknown(held['lease_id'], 1, reason='unverified_usage')
        self.assertEqual(self.ledger.mark_unknown(held['lease_id'], 1), first)

    def test_counting_method_is_immutable_and_proof_must_use_approved_method(self):
        arguments = self.args()
        altered = self.cas.put(canonical_json({
            'schema': compare.COUNT_SCHEMA, 'payload_hash': arguments['payload_hash'],
            'model': 'deepseek-flash', 'input_upper_bound': arguments['input_upper_bound'],
            'method_sha256': 'c' * 64,
        }))
        before = self.path.read_bytes()
        with self.assertRaisesRegex(compare.RequestLedgerError, 'COUNT_PROOF'):
            self.ledger.reserve(**(arguments | {'count_proof_ref': altered}))
        with self.assertRaisesRegex(compare.RequestLedgerError, 'EXPERIMENT_MISMATCH'):
            compare.RequestLedger(self.path, self.cas, 'fixture-experiment',
                                  count_method_sha256='c' * 64, create=True, clock=self.clock)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.status()['requests'], [])
        self.assertEqual(self.ledger.reserve(**arguments)['state'], 'reserved')

    def test_invalid_counting_method_refuses_before_creating_ledger(self):
        fresh = self.root / 'uncreated.sqlite3'
        for method in (None, '', 'B' * 64, 'b' * 63, True):
            with self.subTest(method=method), self.assertRaisesRegex(
                    compare.RequestLedgerError, 'INVALID_EXPERIMENT'):
                compare.RequestLedger(fresh, self.cas, 'fixture-experiment',
                                      count_method_sha256=method, create=True, clock=self.clock)
            self.assertFalse(fresh.exists())

    def test_status_and_open_are_read_only_and_reject_foreign_or_drifted_ledger(self):
        held = self.reserve()
        self.finish(held)
        latest = self.path.read_bytes()
        compare.RequestLedger(self.path, self.cas, 'fixture-experiment',
                              count_method_sha256=self.count_method, clock=self.clock).status()
        with self.assertRaisesRegex(compare.RequestLedgerError, 'EXPERIMENT_MISMATCH'):
            compare.RequestLedger(self.path, self.cas, 'another-experiment',
                                  count_method_sha256=self.count_method, create=True, clock=self.clock)
        self.assertEqual(self.path.read_bytes(), latest)
        foreign = self.root / 'foreign.sqlite3'
        SqliteStore(foreign, lease_ttl_seconds=120)
        before = foreign.read_bytes()
        with self.assertRaisesRegex(compare.RequestLedgerError, 'NOT_EXPERIMENT'):
            compare.RequestLedger(foreign, self.cas, 'fixture-experiment',
                                  count_method_sha256=self.count_method, create=True)
        self.assertEqual(foreign.read_bytes(), before)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("UPDATE control_budgets SET used_units=0 WHERE unit='output_tokens'")
        drift = self.path.read_bytes()
        with self.assertRaisesRegex(compare.RequestLedgerError, 'INTEGRITY'):
            self.status()
        self.assertEqual(self.path.read_bytes(), drift)

    def known_response(self, held, args, *, proof_changes=None, usage=None, indexed_tool_call=False):
        from .money_guard import APPROVAL, SCHEMA

        counts = {'prompt_tokens': 6, 'completion_tokens': 12, 'total_tokens': 18}
        projected = {'id': 'chatcmpl-known', 'object': 'chat.completion', 'created': 1, 'model': 'deepseek-flash',
                     'choices': [{'index': 0, 'message': {'role': 'assistant', 'content': 'fixture response'},
                                  'finish_reason': 'stop'}], 'usage': counts}
        raw = projected | {'fixture_extra': 'projection omits this'}
        if indexed_tool_call:
            call = {'id': 'call-fixture', 'type': 'function',
                    'function': {'name': 'knowledge_search', 'arguments': '{"domains":["research-scatter"],"q":"test"}'}}
            projected['choices'] = [{'index': 0, 'finish_reason': 'tool_calls',
                                     'message': {'role': 'assistant', 'content': '', 'tool_calls': [call]}}]
            raw['choices'] = [{'index': 0, 'finish_reason': 'tool_calls',
                               'message': {'role': 'assistant', 'content': None, 'tool_calls': [call | {'index': 0}]}}]
        response_ref = self.cas.put(canonical_json(projected))
        raw_ref = self.cas.put(canonical_json(raw))
        bridge = {'schema': SCHEMA, 'approval': APPROVAL, 'credential_id': 'a' * 64,
                  'calls': [{'arm': args['arm'], 'payload_sha256': args['payload_hash'].removeprefix('sha256:'),
                             'state': 'returned', 'usage': counts}]}
        bridge_ref = self.cas.put(canonical_json(bridge))
        usage = usage if usage is not None else {'input_tokens': 6, 'output_tokens': 12}
        proof = {key: args[key] for key in ('arm', 'run_id', 'sequence', 'payload_hash')}
        proof.update(schema=compare.TRUSTED_RECEIPT_SCHEMA, source='trusted_windows_bridge',
                     experiment_id='fixture-experiment', model='deepseek-flash', task_id=held['task_id'], fence=1,
                     bridge_receipt_ref=bridge_ref, bridge_call_index=0, raw_response_ref=raw_ref,
                     response_ref=response_ref, usage=usage)
        evidence_ref = self.cas.put(canonical_json(proof | (proof_changes or {})))
        return {'response_ref': response_ref, 'usage': usage, 'evidence_ref': evidence_ref}

    def unknown_known_response(self, *, reason='unverified_usage'):
        args = self.args(arm='hermes')
        held = self.ledger.reserve(**args)
        self.ledger.mark_sent(held['lease_id'], 1)
        self.ledger.mark_unknown(held['lease_id'], 1, reason=reason)
        return args, held

    def test_manual_reconciliation_settles_original_unknown_after_lease_expired_and_is_idempotent(self):
        args, held = self.unknown_known_response()
        proof = self.known_response(held, args, indexed_tool_call=True)
        self.clock.advance(150)
        with self.assertRaises(FenceRejected):
            self.ledger.settle(held['lease_id'], 1, response_ref=proof['response_ref'], usage=proof['usage'])
        result = self.ledger.reconcile_known_response(held['task_id'], **proof)
        self.assertEqual(result['state'], 'settled')
        before = self.path.read_bytes()
        self.assertEqual(self.ledger.reconcile_known_response(held['task_id'], **proof), result | {'state': 'cached'})
        self.assertEqual(self.ledger.reserve(**args), result | {'state': 'cached'})
        with self.assertRaises(IdempotencyConflict):
            self.ledger.reconcile_known_response(held['task_id'], **(proof | {'evidence_ref': self.reply}))
        self.assertEqual(self.path.read_bytes(), before)
        snapshot = self.status('hermes')
        self.assertEqual(snapshot['state'], 'ready')
        self.assertEqual(snapshot['requests'][0]['reconciliation_ref'], proof['evidence_ref'])
        self.assertEqual({unit: (row['used_units'], row['held_units']) for unit, row in snapshot['budgets'].items()},
                         {'provider_requests': (1, 0), 'input_tokens': (6, 0), 'output_tokens': (12, 0)})
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute('SELECT attempts,fence FROM tasks').fetchall(), [(1, 1)])
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM control_events WHERE kind='compare.request_sent'")
                             .fetchone()[0], 1)

    def test_reconciliation_refuses_changed_identity_fence_and_unverified_raw_result(self):
        args, held = self.unknown_known_response()
        current = self.path.read_bytes()
        for changes in ({'payload_hash': sha256_ref(b'wrong request')}, {'fence': 2}, {'fence': True},
                        {'bridge_call_index': 1}, {'raw_response_ref': self.reply}, {'bridge_receipt_ref': self.reply}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.ledger.reconcile_known_response(held['task_id'], **self.known_response(held, args,
                                                                                          proof_changes=changes))
        with self.assertRaisesRegex(compare.RequestLedgerError, 'USAGE_EXCEEDS_RESERVED'):
            self.ledger.reconcile_known_response(held['task_id'], **self.known_response(
                held, args, usage={'input_tokens': 11, 'output_tokens': 12}))
        self.assertEqual(self.path.read_bytes(), current)
        self.assertEqual(self.status('hermes')['state'], 'closed_unknown')

    def test_manual_reconciliation_does_not_clear_transport_unknown(self):
        args, held = self.unknown_known_response(reason='provider_transport_error')
        current = self.path.read_bytes()
        with self.assertRaisesRegex(compare.RequestLedgerError, 'RECONCILIATION_REASON'):
            self.ledger.reconcile_known_response(held['task_id'], **self.known_response(held, args))
        self.assertEqual(self.path.read_bytes(), current)
        self.assertEqual(self.status('hermes')['state'], 'closed_unknown')

    def test_manual_reconciliation_cannot_allocate_same_bridge_call_to_another_task(self):
        args, held = self.unknown_known_response()
        self.ledger.reconcile_known_response(held['task_id'], **self.known_response(held, args))
        later = args | {'run_id': 'next-run'}
        new = self.ledger.reserve(**later)
        self.ledger.mark_sent(new['lease_id'], 1)
        self.ledger.mark_unknown(new['lease_id'], 1, reason='unverified_usage')
        current = self.path.read_bytes()
        with self.assertRaisesRegex(compare.RequestLedgerError, 'BRIDGE_RECEIPT_USED'):
            self.ledger.reconcile_known_response(new['task_id'], **self.known_response(new, later))
        self.assertEqual(self.path.read_bytes(), current)
        self.assertEqual(self.status('hermes')['state'], 'closed_unknown')


if __name__ == '__main__':
    unittest.main()
