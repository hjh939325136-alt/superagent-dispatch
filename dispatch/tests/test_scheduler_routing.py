import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import canonical_json
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.models import DispatchError, Ref, TaskSpec
from sa_dispatch.routing import (
    HTTP_ROUTE, LOCAL_ROUTE, ROUTED_TABLES, RouteRejected, activate_mixed,
    activation_manifest, binding_evidence, row_digest,
)
from sa_dispatch.scheduler import Scheduler
from sa_dispatch.store_sqlite import SqliteStore


class ReadKind:
    name = 'lab.read'
    version = 1
    enabled = True
    side_effect = 'none'

    def validate(self, params):
        return params

    def scope(self, params):
        return {}


class SchedulerRoutingTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.path = root / 'ledger.sqlite3'
        self.artifacts = ArtifactStore(root / 'cas')
        self.clock = FakeClock()
        self.kinds = {'lab.read': ReadKind()}
        self.spec = TaskSpec('template', 'lab.read', 1, {}, (), ('cpu',), 'synthetic', 'none', 1)
        self.old_store = self.open_store()
        self.old_scheduler = Scheduler(self.old_store)

    def open_store(self, route=None):
        return SqliteStore(self.path, clock=self.clock, lease_ttl_seconds=10,
                           kinds=self.kinds, route=route)

    def schedule(self, scheduler, name, **kwargs):
        scheduler.upsert(name, interval_seconds=10, task_template=kwargs.pop('spec', self.spec),
                         next_fire_at=timestamp(self.clock.now()), **kwargs)

    def activate(self, owners=None):
        bindings = {}
        actor = 'fixture.maintenance'
        with closing(self.old_store._connect()) as conn:
            conn.execute('BEGIN')
            for table, rows in (owners or {}).items():
                key = ROUTED_TABLES[table]
                bindings[table] = {}
                for identity, route in rows.items():
                    row = conn.execute(f'SELECT * FROM {table} WHERE {key}=?', (identity,)).fetchone()
                    digest = row_digest(row)
                    fence = row['fence'] if table == 'tasks' else None
                    evidence = binding_evidence(table, identity, route, digest, 'sha256:' + 'a' * 64,
                                                fence=fence)
                    binding = {
                        'execution_route': route,
                        'pre_state_hash': digest,
                        'evidence_ref': self.artifacts.put(canonical_json(evidence)),
                    }
                    if table == 'tasks':
                        binding['fence'] = fence
                    bindings[table][identity] = binding
            evidence_ref = self.artifacts.put(canonical_json(activation_manifest(
                conn, actor=actor, bindings=bindings)))
        activate_mixed(self.path, actor=actor, evidence_ref=evidence_ref, bindings=bindings,
                       clock=self.clock, artifacts=self.artifacts)
        self.http = self.open_store(HTTP_ROUTE)
        self.local = self.open_store(LOCAL_ROUTE)
        return Scheduler(self.http)

    def rows(self, table):
        with closing(self.old_store._connect()) as conn:
            return [dict(row) for row in conn.execute(f'SELECT * FROM {table} ORDER BY rowid')]

    def test_mixed_status_and_tick_leave_local_and_null_history_frozen(self):
        self.schedule(self.old_scheduler, 'legacy')
        legacy_fire = self.old_scheduler.tick()[0]
        self.old_store.cancel(legacy_fire['task_id'], 'fixture')
        self.schedule(self.old_scheduler, 'local')
        for name in ('local', 'legacy'):
            with self.assertRaises(DispatchError):
                self.schedule(self.old_scheduler, name, spec=replace(self.spec, egress='summary'))
        with patch.object(self.old_store, 'submit', side_effect=RuntimeError('before submit')):
            with self.assertRaises(RuntimeError):
                self.old_scheduler.tick()
        local_fire = next(row for row in self.old_scheduler.firings() if row['schedule_id'] == 'local')
        scheduler = self.activate({'schedules': {'local': LOCAL_ROUTE},
                                   'schedule_firings': {local_fire['fire_key']: LOCAL_ROUTE}})
        tables = ('schedules', 'schedule_firings', 'schedule_events', 'tasks', 'transport_handoffs')
        frozen = {table: self.rows(table) for table in tables}
        self.assertEqual(scheduler.status(), {'schedules': [], 'firings': [], 'events': []})
        for store in (self.local, self.open_store()):
            with self.assertRaises(RouteRejected):
                Scheduler(store)
        with self.assertRaises(RouteRejected):
            self.old_scheduler.status()
        with self.assertRaises(RouteRejected):
            self.old_scheduler.tick()
        for name in ('local', 'legacy'):
            with self.assertRaises(RouteRejected):
                self.schedule(scheduler, name)
            with self.assertRaises(RouteRejected):
                self.schedule(scheduler, name, spec=replace(self.spec, egress='summary'))
        self.assertEqual({table: self.rows(table) for table in tables}, frozen)

        self.clock.advance(10)
        self.schedule(scheduler, 'http')
        self.assertEqual(len(scheduler.tick(limit=1)), 1)
        state = scheduler.status()
        self.assertEqual([row['id'] for row in state['schedules']], ['http'])
        self.assertEqual([row['execution_route'] for row in state['firings']], [HTTP_ROUTE])
        for table in tables:
            self.assertEqual(self.rows(table)[:len(frozen[table])], frozen[table])

    def test_pending_http_task_repairs_missing_marker_on_original_route(self):
        scheduler = self.activate()
        self.schedule(scheduler, 'http')
        submit = self.http.submit

        def commit_then_crash(spec):
            submit(spec)
            raise RuntimeError('before marker')

        with patch.object(self.http, 'submit', side_effect=commit_then_crash):
            with self.assertRaisesRegex(RuntimeError, 'before marker'):
                scheduler.tick()
        self.assertEqual(self.rows('transport_handoffs'), [])
        pending, = scheduler.firings()
        task, = self.rows('tasks')
        self.assertEqual((pending['execution_route'], pending['outcome'], task['execution_route']),
                         (HTTP_ROUTE, 'pending', HTTP_ROUTE))
        recovered = Scheduler(self.open_store(HTTP_ROUTE))
        self.assertEqual(recovered.tick(), [{'fire_key': pending['fire_key'], 'task_id': task['task_id'],
                                             'deduplicated': True}])
        marker, = self.rows('transport_handoffs')
        self.assertEqual(marker['task_id'], task['task_id'])
        self.assertEqual(recovered.firings()[0]['outcome'], 'created')
        self.assertEqual(self.rows('tasks'), [task])

    def test_key_collision_does_not_link_publish_or_cancel_local_or_null_task(self):
        slot = timestamp(self.clock.now())
        historical, _ = self.old_store.submit(replace(self.spec, idem_key=f'sched:past:v1:{slot}'))
        self.old_store.cancel(historical.task_id, 'fixture')
        scheduler = self.activate()
        self.local.submit(replace(self.spec, idem_key=f'sched:local:v1:{slot}'))
        before = self.rows('tasks')
        for name in ('local', 'past'):
            self.schedule(scheduler, name)
        self.assertEqual(scheduler.tick(), [])
        self.assertEqual(self.rows('transport_handoffs'), [])
        self.assertEqual(self.rows('tasks'), before)
        self.assertEqual([(row['outcome'], row['task_id']) for row in scheduler.firings()],
                         [('rejected', None), ('rejected', None)])

    def test_null_historical_slot_still_counts_toward_http_lifetime_bound(self):
        self.schedule(self.old_scheduler, 'bounded', max_slots=2)
        first, = self.old_scheduler.tick()
        self.old_store.cancel(first['task_id'], 'fixture')
        scheduler = self.activate({'schedules': {'bounded': HTTP_ROUTE}})
        self.assertEqual(scheduler.firings(), [])
        self.clock.advance(10)
        self.assertEqual(len(scheduler.tick()), 1)
        self.assertEqual(scheduler.status()['schedules'][0]['enabled'], 0)
        self.assertEqual(len(self.rows('schedule_firings')), 2)
        self.clock.advance(10)
        self.assertEqual(scheduler.tick(), [])

    def test_mixed_tick_requires_empty_refs_even_with_default_flag(self):
        scheduler = self.activate()
        ref = Ref('fixture', 'lab', 'object', 'v1', 'sha256:' + 'a' * 64)
        self.schedule(scheduler, 'with-refs', spec=replace(self.spec, input_refs=(ref,)))
        self.assertEqual(scheduler.tick(), [])
        self.assertEqual(self.rows('tasks'), [])
        self.assertEqual(scheduler.status()['events'][0]['detail_code'], 'invalid_template')
