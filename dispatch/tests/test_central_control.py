"""Central reads and maintenance against temporary synthetic mixed ledgers."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
import json
import os
import shutil
import sqlite3
import tempfile
from threading import Event
import unittest
from unittest.mock import patch

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import canonical_json
from sa_dispatch.central_control import (
    AUTHORITY, AUTHORIZATION_SCHEMA, RECEIPT_SCHEMA, CentralControl, budget_config_hash, handoff_offer_hash,
)
from sa_dispatch.clock import FakeClock
from sa_dispatch.control_policy import ControlPolicy
from sa_dispatch.models import DispatchError, IdempotencyConflict, TaskSpec
from sa_dispatch.routing import HTTP_ROUTE, LOCAL_ROUTE, RouteRejected, activate_mixed, activation_manifest
from sa_dispatch.store_sqlite import SqliteStore


class FixtureKind:
    name = 'fixture.central'
    version = 1
    enabled = True
    side_effect = 'idempotent_write'
    validate = staticmethod(dict)
    scope = staticmethod(lambda params: {})


class CentralControlTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.ledger = self.root / 'ledger.sqlite3'
        self.cas = ArtifactStore(self.root / 'artifacts')
        self.clock = FakeClock()
        self.start = '2026-01-01T00:00:00Z'
        self.end = '2026-01-02T00:00:00Z'
        self.isolated = self.open_store()
        policy = ControlPolicy(self.isolated, artifacts=self.cas)
        policy.handoff_offer('legacy', from_conv='sender', to_conv='receiver', project_id='fixture',
                             scope='read_status', expires_at=self.end, actor='sender')
        with closing(self.isolated._connect()) as conn:
            manifest = activation_manifest(conn, actor='fixture', bindings={})
        self.anchor = self.put(manifest)

    def activate(self):
        activate_mixed(self.ledger, actor='fixture', evidence_ref=self.anchor, bindings={},
                       artifacts=self.cas, clock=self.clock)
        return self.open_central()

    def open_store(self, route=None):
        return SqliteStore(self.ledger, clock=self.clock, lease_ttl_seconds=30, route=route,
                           kinds={FixtureKind.name: FixtureKind()})

    def open_central(self, **overrides):
        args = dict(ledger=self.ledger, artifacts=self.cas.root, expected_activation_ref=self.anchor,
                    authority=AUTHORITY, clock=self.clock)
        return CentralControl(**(args | overrides))

    def put(self, obj):
        return self.cas.put(canonical_json(obj))

    def offer(self, handoff_id, **overrides):
        return dict(handoff_id=handoff_id, from_conv='sender', to_conv='receiver', project_id='fixture',
                    scope='read_status', expires_at=self.end, scope_refs=()) | overrides

    def offer_args(self, offer, **proof_overrides):
        proof = dict(schema=AUTHORIZATION_SCHEMA, synthetic=True, offer_hash=handoff_offer_hash(**offer),
                     authorized_by='synthetic-operator') | proof_overrides
        return dict(offer, actor=offer['from_conv'], authorization_ref=self.put(proof))

    def receipt(self, offer, state, **overrides):
        return dict(schema=RECEIPT_SCHEMA, synthetic=True, offer_hash=handoff_offer_hash(**offer),
                    recipient=offer['to_conv'], decision=state) | overrides

    def decide(self, control, offer, state, **overrides):
        return control.handoff_transition(offer['handoff_id'], state, actor=offer['to_conv'],
                                          receipt_ref=self.put(self.receipt(offer, state, **overrides)))

    def budget(self, control, *, limit=10, expected=None, **overrides):
        config = dict(budget_id='shared', period='fixture', unit='units', limit_units=limit,
                      starts_at=self.start, ends_at=self.end) | overrides
        return control.budget_upsert(**config, expected_config_hash=expected)

    def sql(self, query, args=()):
        with closing(sqlite3.connect(self.ledger)) as conn, conn:
            return conn.execute(query, args).fetchall()

    def submit(self, store, name, units):
        task = store.submit(TaskSpec(name, FixtureKind.name, 1, {'name': name}, (), ('cpu',),
                                    'synthetic', 'none', 2),
                            control_rule={'budget_id': 'shared', 'reserve_units': units})[0]
        if store.route == HTTP_ROUTE:
            self.sql('INSERT INTO transport_handoffs VALUES(?,?,?)', (task.task_id, 'fixture', store._now()))
        return task

    def test_readonly_and_exact_authority(self):
        before = self.ledger.read_bytes()
        for authority in (None, 'route_task', 'central'):
            with self.assertRaisesRegex(RouteRejected, 'explicit'):
                self.open_central(authority=authority)
        with self.assertRaisesRegex(RouteRejected, 'activated mixed ledger'):
            self.open_central()
        self.assertEqual(before, self.ledger.read_bytes())
        control = self.activate()
        offer = self.offer('active')
        control.handoff_offer(**self.offer_args(offer))
        self.clock.advance(days=2)
        before = self.ledger.read_bytes()
        before_files = set(self.root.rglob('*'))
        status = self.open_central().status()
        self.assertEqual(AUTHORITY, status['scope'])
        self.assertEqual('ledger', status['budget_totals_scope'])
        self.assertEqual(0, status['messages_sent'])
        self.assertEqual([('active', 'offered', True), ('legacy', 'offered', True)],
                         [(row['handoff_id'], row['state'], row['expiry_due']) for row in status['handoffs']])
        self.assertEqual(before, self.ledger.read_bytes())
        self.assertEqual(before_files, set(self.root.rglob('*')))
        with self.assertRaises(RouteRejected):
            ControlPolicy(self.open_store(LOCAL_ROUTE), authority=AUTHORITY)

    def test_anchor_absence_duplication_and_invalid_cas_are_refused(self):
        control = self.activate()
        before = self.ledger.read_bytes()
        with self.assertRaisesRegex(RouteRejected, 'unique expected'):
            self.open_central(expected_activation_ref=self.put({'synthetic': True}))
        sql = """INSERT INTO route_audit(action,object_table,object_id,execution_route,actor,at,evidence_ref,pre_state_hash,fence)
            SELECT action,object_table,object_id,execution_route,actor,at,evidence_ref,pre_state_hash,fence
            FROM route_audit WHERE action='activate'"""
        self.sql(sql)
        damaged = self.ledger.read_bytes()
        with self.assertRaisesRegex(RouteRejected, 'unique expected'):
            self.budget(control)
        self.assertEqual(damaged, self.ledger.read_bytes())
        self.ledger.write_bytes(before)
        self.cas.path(self.anchor).write_bytes(canonical_json({'schema': 'forged'}))
        with self.assertRaisesRegex(RouteRejected, 'route evidence'):
            self.budget(control)
        self.assertEqual(before, self.ledger.read_bytes())
        self.cas.path(self.anchor).unlink()
        with self.assertRaisesRegex(RouteRejected, 'route evidence'):
            control.status()

    def test_paths_are_pinned_and_missing_paths_stay_missing(self):
        control = self.activate()
        other = self.root / 'new.sqlite3'
        with self.assertRaises(FileNotFoundError):
            self.open_central(ledger=other)
        self.assertFalse(other.exists())
        with self.assertRaises(FileNotFoundError):
            self.open_central(artifacts=self.root / 'absent')
        self.assertFalse((self.root / 'absent').exists())
        shutil.copyfile(self.ledger, other)
        original = self.root / 'original.sqlite3'
        self.ledger.rename(original)
        other.rename(self.ledger)
        before = self.ledger.read_bytes()
        with self.assertRaisesRegex(RouteRejected, 'ledger file changed'):
            self.budget(control)
        self.assertEqual(before, self.ledger.read_bytes())
        self.ledger.unlink()
        original.rename(self.ledger)
        shutil.copytree(self.cas.root, self.root / 'new-artifacts')
        self.cas.root.rename(self.root / 'old-artifacts')
        (self.root / 'new-artifacts').rename(self.cas.root)
        with self.assertRaisesRegex(RouteRejected, 'artifact root changed'):
            self.budget(control)
        self.assertEqual(before, self.ledger.read_bytes())
        link = self.root / 'alias.sqlite3'
        os.link(self.ledger, link)
        with self.assertRaisesRegex(RouteRejected, 'hard links'):
            self.open_central(ledger=link)

    def test_wal_does_not_create_sidecars_on_read(self):
        self.activate()
        with closing(sqlite3.connect(self.ledger)) as conn:
            self.assertEqual(conn.execute('PRAGMA journal_mode=WAL').fetchone()[0], 'wal')
        files = set(self.root.rglob('*'))
        before = self.ledger.read_bytes()
        with self.assertRaisesRegex(RouteRejected, 'rollback journal'):
            self.open_central()
        self.assertEqual(files, set(self.root.rglob('*')))
        self.assertEqual(before, self.ledger.read_bytes())

    def test_complete_schema_and_synthetic_checks_run_inside_writes(self):
        control = self.activate()
        self.budget(control)
        task = self.submit(self.open_store(LOCAL_ROUTE), 'synthetic', 1)
        before = self.ledger.read_bytes()
        for sql, params, message in (
            ("UPDATE tasks SET access='local_only' WHERE task_id=?", (task.task_id,), 'synthetic no-egress'),
            ("UPDATE tasks SET egress='summary' WHERE task_id=?", (task.task_id,), 'synthetic no-egress'),
            ("INSERT INTO evidence VALUES('e',?,0,'fixture',NULL,'hash',NULL,'local_only',?)",
             (task.task_id, self.start), 'synthetic evidence'),
            ('ALTER TABLE control_reservations DROP COLUMN uncertain_at', (),
             'initialized control schema'),
        ):
            with self.subTest(sql=sql):
                self.sql(sql, params)
                damaged = self.ledger.read_bytes()
                with self.assertRaisesRegex(RouteRejected, message):
                    control.handoff_offer(**self.offer_args(self.offer('new')))
                self.assertEqual(damaged, self.ledger.read_bytes())
                self.ledger.write_bytes(before)

    def test_budget_cas_preserves_shared_used_and_reserved_units(self):
        control = self.activate()
        budget = self.budget(control)
        config = budget['config_hash']
        self.assertEqual(config, budget_config_hash('shared', period='fixture', unit='units',
                         limit_units=10, starts_at='2025-12-31T19:00:00-05:00', ends_at=self.end))
        with self.assertRaises(IdempotencyConflict):
            self.budget(control)
        local, remote = self.open_store(LOCAL_ROUTE), self.open_store(HTTP_ROUTE)
        task_a, task_b = self.submit(local, 'a', 3), self.submit(remote, 'b', 4)
        for store, device in ((local, 'local-worker'), (remote, 'remote-worker')):
            store.register_device(device, ['cpu'])
        with ThreadPoolExecutor(2) as pool:
            claim_a = pool.submit(local.claim, 'local-worker')
            claim_b = pool.submit(remote.claim, 'remote-worker')
            self.assertEqual(task_a.task_id, claim_a.result(timeout=10).task_id)
            self.assertEqual(task_b.task_id, claim_b.result(timeout=10).task_id)
        evidence = self.put(dict(synthetic=True, task_id=task_a.task_id, fence=1, budget_id='shared',
                                 fingerprint=task_a.fingerprint, actual_units=2, final_usage=True))
        ControlPolicy(local, artifacts=self.cas, authority='route_task').settle(
            task_a.task_id, 1, actual_units=2, evidence_ref=evidence, actor='accountant')
        status = control.status()
        self.assertEqual({task_a.task_id, task_b.task_id}, {row['task_id'] for row in status['task_rules']})
        self.assertEqual(config, status['budgets'][0]['config_hash'])
        with self.assertRaisesRegex(DispatchError, 'used or held'):
            self.budget(control, limit=5, expected=config)
        with self.assertRaisesRegex(DispatchError, 'period identity'):
            self.budget(control, limit=8, expected=config, unit='different')
        with ThreadPoolExecutor(2) as pool:
            attempts = [pool.submit(self.budget, operator, limit=limit, expected=config)
                        for operator, limit in ((control, 8), (self.open_central(), 9))]
            results = []
            for attempt in attempts:
                try:
                    results.append(attempt.result(timeout=10))
                except IdempotencyConflict:
                    continue
        updated, = results
        self.assertEqual((2, 4), (updated['used_units'], updated['reserved_units']))
        self.assertEqual(updated, self.budget(control, limit=updated['limit_units'], expected=updated['config_hash']))
        with self.assertRaises(IdempotencyConflict):
            self.budget(control, budget_id='absent', expected=config)

    def test_offer_idempotency_and_full_scope_binding(self):
        control = self.activate()
        offer = self.offer('offer', scope_refs=(self.put({'synthetic': True}),))
        args = self.offer_args(offer)
        offered = control.handoff_offer(**args)
        self.assertEqual(offered, control.handoff_offer(**args))
        accepted = self.decide(control, offer, 'accepted')
        self.assertEqual(accepted, control.handoff_offer(**args))
        before = self.ledger.read_bytes()
        mutations = dict(from_conv='different.from', to_conv='different.to', project_id='different.project',
                         scope='takeover', expires_at='2026-01-03T00:00:00Z', scope_refs=())
        for key, value in mutations.items():
            with self.subTest(field=key), self.assertRaises(IdempotencyConflict):
                control.handoff_offer(**self.offer_args(offer | {key: value}))
        with self.assertRaises(IdempotencyConflict):
            control.handoff_offer(**self.offer_args(offer, authorized_by='changed-proof'))
        for scope in ('read_status', 'instruct', 'takeover'):
            other = self.offer('other', scope=scope)
            with self.assertRaises(DispatchError):
                control.handoff_offer(**dict(self.offer_args(other), authorization_ref=None))
            with self.assertRaisesRegex(DispatchError, 'exact offer'):
                control.handoff_offer(**self.offer_args(other, offer_hash=offered['offer_hash']))
        with self.assertRaisesRegex(DispatchError, 'sender'):
            control.handoff_offer(**dict(args, actor='wrong-sender'))
        with self.assertRaises(IdempotencyConflict):
            control.handoff_offer(**self.offer_args(self.offer('legacy')))
        with self.assertRaises(DispatchError):
            self.decide(control, self.offer('legacy'), 'accepted')
        self.assertEqual(before, self.ledger.read_bytes())

    def test_decisions_require_exact_receipts_and_terminal_outputs_are_immutable(self):
        control = self.activate()
        offer = self.offer('strict')
        control.handoff_offer(**self.offer_args(offer))
        before = self.ledger.read_bytes()
        output = self.put({'synthetic': True, 'example': 'output'})
        with self.assertRaisesRegex(DispatchError, 'current state'):
            self.decide(control, offer, 'completed', evidence_refs=[output])
        for wrong_proof in (dict(synthetic=1), dict(decision='rejected'), dict(recipient='other'),
                            dict(offer_hash=handoff_offer_hash(**self.offer('unknown'))),
                            dict(extra='not in receipt schema')):
            with self.subTest(wrong_proof=wrong_proof), self.assertRaises(DispatchError):
                self.decide(control, offer, 'accepted', **wrong_proof)
        correct = self.receipt(offer, 'accepted')
        receipt = self.put(correct)
        with self.assertRaisesRegex(DispatchError, 'intended recipient'):
            control.handoff_transition('strict', 'accepted', actor='wrong', receipt_ref=receipt)
        with self.assertRaisesRegex(DispatchError, 'existing handoff'):
            self.decide(control, self.offer('unknown'), 'accepted')
        self.cas.path(receipt).write_bytes(b'corrupted')
        with self.assertRaisesRegex(DispatchError, 'do not match'):
            control.handoff_transition('strict', 'accepted', actor='receiver', receipt_ref=receipt)
        self.assertEqual(before, self.ledger.read_bytes())
        self.cas.path(receipt).unlink()
        accepted = self.decide(control, offer, 'accepted')
        self.assertEqual(accepted, self.decide(control, offer, 'accepted'))
        alternate = self.cas.put(json.dumps(correct, indent=2).encode())
        with self.assertRaises(IdempotencyConflict):
            control.handoff_transition('strict', 'accepted', actor='receiver', receipt_ref=alternate)
        for refs in ([], [None], ['sha256:' + '0'*64]):
            with self.subTest(refs=refs), self.assertRaises(DispatchError):
                self.decide(control, offer, 'completed', evidence_refs=refs)
        self.cas.path(output).write_bytes(b'corrupt output')
        with self.assertRaisesRegex(DispatchError, 'do not match'):
            self.decide(control, offer, 'completed', evidence_refs=[output])
        self.cas.path(output).unlink()
        output = self.put({'synthetic': True, 'example': 'output'})
        completed = self.decide(control, offer, 'completed', evidence_refs=[output])
        self.assertEqual(completed, self.decide(control, offer, 'completed', evidence_refs=[output]))
        with self.assertRaises(IdempotencyConflict):
            self.decide(control, offer, 'completed', evidence_refs=[output, output])
        with self.assertRaisesRegex(DispatchError, 'current state'):
            self.decide(control, offer, 'rejected')
        rejected_offer = self.offer('reject')
        control.handoff_offer(**self.offer_args(rejected_offer))
        self.decide(control, rejected_offer, 'accepted')
        rejected = self.decide(control, rejected_offer, 'rejected')
        self.assertEqual(rejected, self.decide(control, rejected_offer, 'rejected'))
        direct = self.offer('reject-direct')
        control.handoff_offer(**self.offer_args(direct))
        self.assertEqual('rejected', self.decide(control, direct, 'rejected')['state'])

    def test_expiry_is_named_and_invalid_commands_do_not_refresh_it(self):
        control = self.activate()
        first, second = self.offer('first'), self.offer('second')
        for offer in (first, second):
            control.handoff_offer(**self.offer_args(offer))
        self.decide(control, second, 'accepted')
        with self.assertRaisesRegex(DispatchError, 'active overdue'):
            control.handoff_expire('first')
        self.clock.advance(days=1)
        before = self.ledger.read_bytes()
        with self.assertRaisesRegex(DispatchError, 'authorization has expired'):
            self.decide(control, first, 'accepted')
        with self.assertRaisesRegex(DispatchError, 'existing handoff'):
            control.handoff_expire('unknown')
        with self.assertRaisesRegex(DispatchError, 'already expired'):
            control.handoff_offer(**self.offer_args(self.offer('new-expired')))
        self.assertEqual(before, self.ledger.read_bytes())
        expired = control.handoff_expire('first')
        self.assertEqual('expired', expired['state'])
        self.assertEqual(expired, control.handoff_expire('first'))
        self.assertEqual(expired, control.handoff_offer(**self.offer_args(first)))
        states = {row['handoff_id']: row['state'] for row in control.status()['handoffs']}
        self.assertEqual({'first': 'expired', 'second': 'accepted', 'legacy': 'offered'}, states)
        legacy = control.handoff_expire('legacy')
        self.assertEqual('expired', legacy['state'])
        self.assertIsNone(legacy['authorization_ref'])
        self.assertEqual(legacy, control.handoff_expire('legacy'))
        states = {row['handoff_id']: row['state'] for row in control.status()['handoffs']}
        self.assertEqual({'first': 'expired', 'second': 'accepted', 'legacy': 'expired'}, states)
        self.assertEqual('expired', control.handoff_expire('second')['state'])

    def test_expiry_uses_time_after_obtaining_the_write_lock(self):
        control = self.activate()
        offer = self.offer('lock-wait', expires_at='2026-01-01T00:00:01Z')
        control.handoff_offer(**self.offer_args(offer))
        receipt = self.put(self.receipt(offer, 'accepted'))
        before = self.ledger.read_bytes()
        begin = Event()
        connect = sqlite3.connect

        def listen(*args, **kwargs):
            conn = connect(*args, **kwargs)
            conn.set_trace_callback(lambda sql: begin.set() if sql == 'BEGIN IMMEDIATE' else None)
            return conn

        with ThreadPoolExecutor(1) as pool, closing(connect(self.ledger)) as locker:
            locker.execute('BEGIN IMMEDIATE')
            with patch('sa_dispatch.central_control.sqlite3.connect', side_effect=listen):
                work = pool.submit(control.handoff_transition, 'lock-wait', 'accepted',
                                   actor='receiver', receipt_ref=receipt)
                try:
                    self.assertTrue(begin.wait(timeout=5))
                    self.assertFalse(work.done())
                    self.clock.advance(seconds=1)
                finally:
                    locker.commit()
                with self.assertRaisesRegex(DispatchError, 'authorization has expired'):
                    work.result(timeout=5)
        self.assertEqual(before, self.ledger.read_bytes())

    def test_event_failure_rolls_back_offer_transition_and_expiry(self):
        control = self.activate()
        offer = self.offer('event')
        self.sql("""CREATE TRIGGER ignore_handoff_event BEFORE INSERT ON control_events
            WHEN NEW.kind='handoff.offered' BEGIN SELECT RAISE(IGNORE); END""")
        before = self.ledger.read_bytes()
        with self.assertRaisesRegex(DispatchError, 'event was not recorded'):
            control.handoff_offer(**self.offer_args(offer))
        self.assertEqual(before, self.ledger.read_bytes())
        self.sql('DROP TRIGGER ignore_handoff_event')
        control.handoff_offer(**self.offer_args(offer))
        self.sql("""CREATE TRIGGER fail_handoff_event BEFORE INSERT ON control_events
            WHEN NEW.kind IN ('handoff.accepted','handoff.expired') BEGIN SELECT RAISE(ABORT,'fixture'); END""")
        before = self.ledger.read_bytes()
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'fixture'):
            self.decide(control, offer, 'accepted')
        self.clock.advance(days=1)
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'fixture'):
            control.handoff_expire('event')
        self.assertEqual(before, self.ledger.read_bytes())


if __name__ == '__main__':
    unittest.main()
