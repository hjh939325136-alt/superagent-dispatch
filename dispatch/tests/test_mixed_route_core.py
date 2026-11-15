"""Synthetic ledger activation, write isolation and v7 restore."""
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from sa_dispatch.artifacts import ArtifactError, ArtifactStore
from sa_dispatch.bundle import V6_FORMAT, V6_TABLES, export_bundle, import_bundle, read_bundle, verify_bundle
from sa_dispatch.canonical import canonical_json, sha256_ref
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.control_policy import ControlPolicy
from sa_dispatch.models import FenceRejected, IdempotencyConflict, Outcome, TaskSpec
from sa_dispatch.routing import (HTTP_ROUTE, LOCAL_ROUTE, RouteRejected, activate_mixed,
                                 activation_manifest, binding_evidence, row_digest)
from sa_dispatch.store_sqlite import SqliteStore


class FixtureKind:
    name = 'fixture.shared'
    version = 1
    enabled = True
    side_effect = 'none'

    def validate(self, params):
        return params

    def scope(self, params):
        return {}


class MixedRouteCoreTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.path = self.root / 'ledger.sqlite'
        self.cas = ArtifactStore(self.root / 'cas')
        self.clock = FakeClock()
        self.kind = FixtureKind()
        self.legacy = self.open()

    def open(self, route=None, *, path=None, kind=None):
        return SqliteStore(path or self.path, lease_ttl_seconds=10, clock=self.clock,
                           kinds={self.kind.name: kind or self.kind}, route=route)

    def spec(self, key):
        return TaskSpec(key, self.kind.name, 1, {}, (), ('cpu',), 'synthetic', 'none', 2)

    def seal(self, bindings):
        with closing(self.legacy._connect()) as conn:
            conn.execute('BEGIN')
            record = activation_manifest(conn, actor='fixture', bindings=bindings)
        return self.cas.put(canonical_json(record))

    def activate(self, bindings=None, evidence_ref=None):
        bindings = bindings or {}
        activate_mixed(self.path, actor='fixture', bindings=bindings, artifacts=self.cas,
                       evidence_ref=evidence_ref or self.seal(bindings), clock=self.clock)

    def binding(self, table, key, route):
        ids = {'tasks': 'task_id', 'devices': 'device_id', 'schedules': 'id', 'schedule_firings': 'fire_key'}
        row = next(row for row in self.legacy.snapshot()[table] if row[ids[table]] == key)
        pre_state = row_digest(row)
        fence = {'fence': row['fence']} if table == 'tasks' else {}
        proof = binding_evidence(table, key, route, pre_state, sha256_ref(b'offline evidence'), **fence)
        return {'execution_route': route, 'pre_state_hash': pre_state,
                'evidence_ref': self.cas.put(canonical_json(proof)), **fence}

    def marker(self, store, task):
        with store._tx(execution=True) as conn:
            store.require_task_route(task, conn=conn)
            conn.execute('INSERT INTO transport_handoffs VALUES(?,?,?)',
                         (task.task_id, 'fixture', timestamp(self.clock.now())))

    def test_activation_freezes_legacy_rows_and_stale_writer(self):
        with self.assertRaises(RouteRejected):
            self.open(LOCAL_ROUTE)
        for index in range(8):
            task, _ = self.legacy.submit(self.spec(f'historical-{index}'))
            self.legacy.cancel(task.task_id, 'fixture')
        before = self.legacy.snapshot()['tasks']
        self.legacy.register_device('unknown-device', ['cpu'])
        self.activate()
        with patch('sa_dispatch.migrations.apply_m0_schema', side_effect=AssertionError('unexpected migration')):
            reader = self.open()
        self.assertEqual(reader.routing_mode(), 'mixed')
        self.assertEqual(reader.snapshot()['tasks'], before)
        with closing(reader._connect()) as conn, self.assertRaises(sqlite3.OperationalError):
            conn.execute('DELETE FROM events')
        for store in (self.legacy, reader):
            snapshot = store.snapshot()
            with self.assertRaises(RouteRejected):
                store.submit(self.spec('stale'))
            with self.assertRaises(RouteRejected):
                store.register_device('writer', [])
            with self.assertRaises(RouteRejected):
                store.sweep()
            self.assertEqual(store.snapshot(), snapshot)
        local = self.open(LOCAL_ROUTE)
        with self.assertRaises(RouteRejected):
            local.register_device('unknown-device', ['cpu'])
        with self.assertRaises(IdempotencyConflict):
            local.submit(self.spec('historical-0'))

    def test_activation_checks_evidence_and_rolls_back_every_row(self):
        task, _ = self.legacy.submit(self.spec('old'))
        self.legacy.register_device('device', ['cpu'])
        mapping = {'tasks': {task.task_id: self.binding('tasks', task.task_id, LOCAL_ROUTE)},
                   'devices': {'device': self.binding('devices', 'device', LOCAL_ROUTE)}}
        snapshot = self.legacy.snapshot()
        with self.assertRaisesRegex(RouteRejected, 'unbound live'):
            self.activate()
        self.assertEqual(self.legacy.snapshot(), snapshot)
        before = self.seal(mapping)
        wrong_id = {'tasks': {task.task_id: dict(mapping['tasks'][task.task_id], fence=5)}}
        missing = {'tasks': {task.task_id: dict(mapping['tasks'][task.task_id],
                                                evidence_ref=sha256_ref(b'missing'))}}
        wrong_source = {'tasks': {task.task_id: dict(mapping['tasks'][task.task_id],
                                                     evidence_ref=mapping['devices']['device']['evidence_ref'])}}
        for bindings, evidence in ((mapping, sha256_ref(b'missing-seal')),
                                   (wrong_id, before), (wrong_id, None), (missing, None), (wrong_source, None)):
            with self.assertRaises(RouteRejected):
                self.activate(bindings, evidence_ref=evidence)
            self.assertEqual(self.legacy.snapshot(), snapshot)
        self.activate(mapping)
        local = self.open(LOCAL_ROUTE)
        self.assertEqual(local.get_task(task.task_id).execution_route, LOCAL_ROUTE)
        self.assertEqual(local.claim('device').task_id, task.task_id)

    def test_active_lease_and_non_synthetic_block_activation(self):
        task, _ = self.legacy.submit(self.spec('active'))
        self.legacy.register_device('worker', ['cpu'])
        lease = self.legacy.claim('worker')
        self.assertEqual(task.task_id, lease.task_id)
        with self.assertRaisesRegex(RouteRejected, 'active leases'):
            self.activate()
        self.legacy.complete(lease.lease_id, lease.fence, Outcome(), 'worker')
        task, _ = self.legacy.submit(replace(self.spec('private'), access='local_only'))
        self.legacy.cancel(task.task_id, 'fixture')
        with self.assertRaisesRegex(RouteRejected, 'synthetic'):
            self.activate()
        self.assertEqual(self.legacy.routing_mode(), 'isolated')

    def test_activation_rejects_duplicate_keys_and_non_metadata_evidence(self):
        task, _ = self.legacy.submit(self.spec('queued-binding'))
        binding = self.binding('tasks', task.task_id, LOCAL_ROUTE)
        before = self.legacy.snapshot()
        valid = {'tasks': {task.task_id: binding}}
        manifest = self.seal(valid)
        for duplicate, ref in ((b'actor', manifest), (b'source_hash', binding['evidence_ref'])):
            raw = self.cas.get(ref)
            variants = (b'{"' + duplicate + b'":"SYNTHETIC_HIDDEN_BODY",' + raw[1:],
                        b'{"extra":NaN,' + raw[1:],
                        canonical_json(dict(json.loads(raw), extra='SYNTHETIC_HIDDEN_BODY')))
            for payload in variants:
                bad_ref = self.cas.put(payload)
                with self.subTest(key=duplicate, payload=payload), self.assertRaises(RouteRejected):
                    if duplicate == b'actor':
                        self.activate(valid, evidence_ref=bad_ref)
                    else:
                        damaged = {'tasks': {task.task_id: dict(binding, evidence_ref=bad_ref)}}
                        self.activate(damaged)
                self.assertEqual(self.legacy.snapshot(), before)

    @staticmethod
    def rewrite_bundle_records(bundle, table, rows, *, ref_swap=None):
        manifest = json.loads((bundle / 'manifest.json').read_bytes())
        content = b''.join(canonical_json(row) + b'\n' for row in rows)
        filename = table + '.jsonl'
        (bundle / filename).write_bytes(content)
        manifest['files'][filename] = sha256_ref(content)
        manifest['source_counts'][table] = len(rows)
        if ref_swap is not None:
            old_ref, new_ref = ref_swap
            manifest['artifacts'] = sorted(new_ref if ref == old_ref else ref for ref in manifest['artifacts'])
        (bundle / 'manifest.json').write_bytes(canonical_json(manifest))

    def test_v7_rejects_preexisting_route_evidence_with_hidden_body(self):
        task, _ = self.legacy.submit(self.spec('bound'))
        self.legacy.cancel(task.task_id, 'fixture')
        binding = self.binding('tasks', task.task_id, LOCAL_ROUTE)
        self.activate({'tasks': {task.task_id: binding}})
        source = SimpleNamespace(db_path=self.path, artifact_root=self.cas.root)
        package = self.root / 'v7-clean'
        export_bundle(source, package)
        self.assertEqual(verify_bundle(package)['tasks'], 1)
        old_ref = binding['evidence_ref']
        payload = b'{"source_hash":"SYNTHETIC_HIDDEN_BODY",' + self.cas.get(old_ref)[1:]
        new_ref = self.cas.put(payload)
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute('UPDATE route_audit SET evidence_ref=? WHERE evidence_ref=?', (new_ref, old_ref))
        with self.assertRaisesRegex(ArtifactError, 'route evidence'):
            export_bundle(source, self.root / 'blocked-evidence')
        self.assertFalse((self.root / 'blocked-evidence').exists())
        ArtifactStore(package / 'artifacts').put(payload)
        self.rewrite_bundle_records(package, 'route_audit', self.legacy.snapshot()['route_audit'],
                                    ref_swap=(old_ref, new_ref))
        for reader in (read_bundle, verify_bundle):
            with self.assertRaises(ArtifactError) as error:
                reader(package)
            self.assertIsInstance(error.exception.__cause__, RouteRejected)

    def test_v7_checks_route_graph_before_export_verify_and_restore(self):
        self.activate()
        http, local = self.open(HTTP_ROUTE), self.open(LOCAL_ROUTE)
        http.register_device('http', ['cpu'])
        local.register_device('local', ['cpu'])
        local_task, _ = local.submit(self.spec('local'))
        http_task, _ = http.submit(self.spec('http'))
        self.marker(http, http_task)
        for store, device in ((http, 'http'), (local, 'local')):
            lease = store.claim(device)
            store.complete(lease.lease_id, lease.fence, Outcome(), device)
        now = timestamp(self.clock.now())
        with http._tx(execution=True) as conn:
            conn.executemany('INSERT INTO schedules(id,interval_seconds,task_template,next_fire_at,enabled,execution_route) '
                             'VALUES(?,?,?,?,?,?)', [('http-schedule', 10, '{}', now, 0, HTTP_ROUTE),
                                                     ('local-schedule', 10, '{}', now, 0, LOCAL_ROUTE)])
            conn.execute('INSERT INTO schedule_firings(fire_key,schedule_id,scheduled_at,task_template,task_id,outcome,execution_route) '
                         'VALUES(?,?,?,?,?,?,?)', ('http-slot', 'http-schedule', now, '{}', http_task.task_id,
                                                   'created', HTTP_ROUTE))
        source = SimpleNamespace(db_path=self.path, artifact_root=self.cas.root)
        package = self.root / 'clean-package'
        export_bundle(source, package)
        self.assertEqual(verify_bundle(package)['tasks'], 2)
        pristine = self.legacy.snapshot()
        destination = SimpleNamespace(db_path=self.root / 'restored.sqlite', artifact_root=self.root / 'restored-cas')
        target = self.open(path=destination.db_path)
        untouched = target.snapshot()
        local_lease = next(row['lease_id'] for row in pristine['leases'] if row['task_id'] == local_task.task_id)
        artifact = self.cas.put(b'fixture')
        cases = (
            ('transport_handoffs', 'INSERT INTO transport_handoffs VALUES(?,?,?)',
             (local_task.task_id, 'fixture', now), 'local task has HTTP handoff authority'),
            ('transport_artifact_grants', 'INSERT INTO transport_artifact_grants VALUES(?,?,?,?,?,?)',
             ('bad-grant', local_task.task_id, None, artifact, 'synthetic', now),
             'local task has HTTP artifact authority'),
            ('transport_artifact_grants', 'INSERT INTO transport_artifact_grants VALUES(?,?,?,?,?,?)',
             ('bad-lease-grant', http_task.task_id, local_lease, artifact, 'synthetic', now),
             'HTTP artifact authority disagrees with its lease task'),
            ('schedule_firings', 'UPDATE schedule_firings SET task_id=? WHERE fire_key=?',
             (local_task.task_id, 'http-slot'), 'inconsistent schedule firing'),
            ('schedule_firings', 'UPDATE schedule_firings SET schedule_id=? WHERE fire_key=?',
             ('local-schedule', 'http-slot'), 'inconsistent schedule firing'),
            ('leases', "UPDATE leases SET device_id='http' WHERE task_id=?",
             (local_task.task_id,), 'multiple routes'),
        )
        for index, (table, statement, values, message) in enumerate(cases):
            with self.subTest(table=table):
                damaged_db = self.root / f'damaged-{index}.sqlite'
                shutil.copyfile(self.path, damaged_db)
                with closing(sqlite3.connect(damaged_db)) as conn, conn:
                    conn.row_factory = sqlite3.Row
                    conn.execute(statement, values)
                    changed = [dict(row) for row in conn.execute('SELECT * FROM ' + table)]
                damaged_source = SimpleNamespace(db_path=damaged_db, artifact_root=self.cas.root)
                with self.assertRaisesRegex(ArtifactError, message):
                    export_bundle(damaged_source, self.root / f'blocked-{index}')
                damaged_package = self.root / f'damaged-package-{index}'
                shutil.copytree(package, damaged_package)
                self.rewrite_bundle_records(damaged_package, table, changed)
                with self.assertRaises(ArtifactError) as error:
                    verify_bundle(damaged_package)
                self.assertIn(message, str(error.exception.__cause__))
                with self.assertRaises(ArtifactError):
                    import_bundle(destination, damaged_package, target)
                with self.assertRaisesRegex(RouteRejected, message):
                    target.import_snapshot({**pristine, table: changed})
                self.assertEqual(target.snapshot(), untouched)

    def test_route_controls_device_scope_idempotency_claim_and_late_receipts(self):
        self.activate()
        http, local = self.open(HTTP_ROUTE), self.open(LOCAL_ROUTE)
        http.register_device('http', ['cpu'])
        local.register_device('local', ['cpu'])
        task_http, _ = http.submit(self.spec('http'))
        task_local, _ = local.submit(self.spec('local'))
        self.assertEqual(local.submit(self.spec('local')), (task_local, True))
        with self.assertRaises(IdempotencyConflict):
            http.submit(self.spec('local'))
        self.assertIsNone(http.claim('http'))
        self.marker(http, task_http)
        with self.assertRaises(RouteRejected):
            local.register_device('http', [])
        with self.assertRaises(RouteRejected):
            http.claim('local')
        self.assertIsNone(self.open(LOCAL_ROUTE, kind=SimpleNamespace(version=2, enabled=True)).claim('local'))
        self.assertIsNone(self.open(LOCAL_ROUTE, kind=SimpleNamespace(version=1, enabled=False)).claim('local'))
        self.assertIsNone(self.open(LOCAL_ROUTE, kind=SimpleNamespace(
            version=1, enabled=True, dedicated_runner=True)).claim('local'))
        http.sweep(worker_ids={'local'})
        self.assertEqual(local.get_task(task_http.task_id).status, 'waiting_device')
        self.assertEqual(local.get_task(task_local.task_id).status, 'queued')
        http.sweep(worker_ids={'http'})
        http_lease = http.claim('http')
        local_lease = local.claim('local')
        before = local.snapshot()
        for operation in (lambda: local.renew(http_lease.lease_id, http_lease.fence, 'local'),
                          lambda: local.complete(http_lease.lease_id, http_lease.fence, Outcome(), 'http'),
                          lambda: local.mark_unknown(http_lease.lease_id, http_lease.fence, 'fixture', 'http'),
                          lambda: local.cancel(task_http.task_id, 'fixture')):
            with self.assertRaises(RouteRejected):
                operation()
            self.assertEqual(local.snapshot(), before)
        http.complete(http_lease.lease_id, http_lease.fence, Outcome(), 'http')
        http.complete(http_lease.lease_id, http_lease.fence, Outcome(), 'http')
        with self.assertRaises(RouteRejected):
            local.complete(http_lease.lease_id, http_lease.fence, Outcome(), 'http')
        self.assertEqual(local.late_results(task_http.task_id), [])
        self.clock.advance(11)
        self.assertEqual(http.sweep().expired, 0)
        self.assertEqual(local.sweep().expired, 1)
        with self.assertRaises(FenceRejected):
            local.complete(local_lease.lease_id, local_lease.fence, Outcome(), 'local')
        self.assertEqual(len(local.late_results(task_local.task_id)), 1)

    def test_unknown_binding_never_requeues_and_generic_transactions_reject(self):
        self.legacy.register_device('old-worker', ['cpu'])
        task, _ = self.legacy.submit(self.spec('unknown'))
        lease = self.legacy.claim('old-worker')
        self.legacy.mark_unknown(lease.lease_id, lease.fence, 'fixture', 'old-worker')
        bindings = {'tasks': {task.task_id: self.binding('tasks', task.task_id, LOCAL_ROUTE)},
                    'devices': {'old-worker': self.binding('devices', 'old-worker', LOCAL_ROUTE)}}
        self.activate(bindings)
        local, http = self.open(LOCAL_ROUTE), self.open(HTTP_ROUTE)
        with self.assertRaises(RouteRejected):
            with local._tx():
                self.fail('legacy transaction yielded a write connection')
        with self.assertRaises(RouteRejected):
            http.resolve_unknown(task.task_id, 'still_unknown', 'fixture')
        with self.assertRaises(RouteRejected):
            local.resolve_unknown(task.task_id, 'done', 'fixture')
        local.resolve_unknown(task.task_id, 'still_unknown', 'fixture', expected_fence=lease.fence)
        local.sweep()
        self.assertIsNone(local.claim('old-worker'))
        self.assertEqual(local.get_task(task.task_id).status, 'unknown')

    def test_mixed_control_rule_denies_without_approval_or_budget(self):
        self.kind.requires_control_rule = True
        policy = ControlPolicy(self.legacy, artifacts=self.cas)
        starts = timestamp(self.clock.now())
        self.clock.advance(120)
        ends = timestamp(self.clock.now())
        self.clock.advance(-100)
        policy.budget_upsert('limited', period='fixture', unit='units', limit_units=0,
                             starts_at=starts, ends_at=ends)
        self.activate()
        local = self.open(LOCAL_ROUTE)
        local.register_device('local', ['cpu'])
        with self.assertRaises(RouteRejected):
            local.submit(self.spec('without-control'))
        with self.assertRaises(RouteRejected):
            local.submit(self.spec('unknown-budget'), control_rule={'budget_id': 'missing'})
        local.submit(self.spec('needs-approval'), control_rule={'approval_scope': 'fixture'})
        local.submit(self.spec('needs-budget'), control_rule={'budget_id': 'limited', 'reserve_units': 1})
        self.assertIsNone(local.claim('local'))
        events = local.snapshot()['control_events']
        self.assertEqual({row['kind'] for row in events}, {'approval.requested', 'budget.exceeded'})
        self.assertEqual(local.snapshot()['control_reservations'], [])

    def test_v7_and_v6_compatibility(self):
        self.activate()
        local = self.open(LOCAL_ROUTE)
        local.register_device('local', ['cpu'])
        task, _ = local.submit(self.spec('settled'))
        local.cancel(task.task_id, 'fixture')
        source = SimpleNamespace(db_path=self.path, artifact_root=self.cas.root)
        bundle = self.root / 'v7'
        manifest = export_bundle(source, bundle)
        self.assertNotIn('devices.jsonl', manifest['files'])
        destination = SimpleNamespace(db_path=self.root / 'restored.sqlite', artifact_root=self.root / 'restored-cas')
        target = self.open(path=destination.db_path)
        import_bundle(destination, bundle, target)
        self.assertEqual(target.snapshot(), {**local.snapshot(), 'devices': []})
        with self.assertRaises(RouteRejected):
            target.submit(self.spec('blocked'))
        with self.assertRaises(RouteRejected):
            self.open(HTTP_ROUTE, path=destination.db_path).register_device('local', [])
        self.open(LOCAL_ROUTE, path=destination.db_path).register_device('local', ['cpu'])
        snapshot_target = self.open(path=self.root / 'snapshot.sqlite')
        snapshot_target.import_snapshot(local.snapshot())
        self.assertEqual(snapshot_target.snapshot(), local.snapshot())

        v6 = self.root / 'v6'
        v6.mkdir()
        files, counts = {}, {}
        for table in V6_TABLES:
            rows = [{k: v for k, v in row.items() if k != 'execution_route'}
                    for row in local.snapshot()[table]
                    if table != 'schema_migrations' or row['version'] != '0012']
            name = table + '.jsonl'
            content = b''.join(canonical_json(row) + b'\n' for row in rows)
            (v6 / name).write_bytes(content)
            files[name], counts[table] = sha256_ref(content), len(rows)
        (v6 / 'manifest.json').write_bytes(canonical_json({
            'schema': V6_FORMAT, 'files': files, 'source_counts': counts,
            'artifacts': [], 'import_policy': {'schedules': 'disabled', 'devices': 'excluded'}}))
        old = SimpleNamespace(db_path=self.root / 'legacy-restored.sqlite', artifact_root=self.root / 'legacy-cas')
        old_target = self.open(path=old.db_path)
        import_bundle(old, v6, old_target)
        self.assertEqual(old_target.routing_mode(), 'isolated')
        self.assertIsNone(old_target.get_task(task.task_id).execution_route)
        self.assertEqual(old_target.snapshot()['route_audit'], [])

        rows = json.loads((bundle / 'tasks.jsonl').read_bytes())
        rows.pop('execution_route')
        malformed = canonical_json(rows) + b'\n'
        (bundle / 'tasks.jsonl').write_bytes(malformed)
        manifest['files']['tasks.jsonl'] = sha256_ref(malformed)
        (bundle / 'manifest.json').write_bytes(canonical_json(manifest))
        with self.assertRaises(ArtifactError):
            read_bundle(bundle)


if __name__ == '__main__':
    unittest.main()
