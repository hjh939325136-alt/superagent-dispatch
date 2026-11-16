"""Read-only upgrade preflight against synthetic, normally initialized ledgers."""

from contextlib import closing, redirect_stdout
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import sqlite3
import stat
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dispatch.sa_dispatch.artifacts import ArtifactStore
from dispatch.sa_dispatch.canonical import canonical_json
from dispatch.sa_dispatch.collector import Collector, SourceBinding
from dispatch.sa_dispatch.models import TaskSpec
from dispatch.sa_dispatch.routing import LOCAL_ROUTE, activate_mixed, activation_manifest
from dispatch.sa_dispatch.store_sqlite import SqliteStore
from tools.m0_agent import external_preflight


class FixtureKind:
    name = 'fixture.agent_preflight'
    version = 1
    enabled = True
    side_effect = 'none'

    def validate(self, params):
        return params

    def scope(self, params):
        return {}


class ExternalPreflightTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ledger = self.root / 'fixture #.sqlite3'
        self.store = SqliteStore(self.ledger, lease_ttl_seconds=30)
        binding = SourceBinding.enroll('common-dispatch', 'dispatch', self.ledger,
                                       'fixture-instance', ['0012'])
        for stream in ('events', 'control_events', 'schedule_events'):
            result = Collector(self.ledger).sync(binding, stream)
            self.assertEqual(result['state'], 'synced')

    def activate(self):
        artifacts = ArtifactStore(self.root / 'artifacts')
        with closing(self.store._connect()) as connection:
            receipt = activation_manifest(connection, actor='fixture', bindings={})
        evidence = artifacts.put(canonical_json(receipt))
        activate_mixed(self.ledger, actor='fixture', bindings={}, evidence_ref=evidence, artifacts=artifacts)

    def update(self, sql, parameters=()):
        with closing(sqlite3.connect(self.ledger)) as connection, connection:
            connection.execute(sql, parameters)

    def snapshot(self):
        return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in self.root.iterdir()
                if path.is_file()}

    def report(self, exit_code):
        before = self.snapshot()
        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(external_preflight.main(['--ledger', str(self.ledger)]), exit_code)
        self.assertEqual(self.snapshot(), before)
        report = json.loads(output.getvalue())
        self.assertIs(report['external_execution_allowed'], False)
        return report

    def test_known_mixed_schema_and_three_cursors(self):
        self.activate()
        report = self.report(0)
        self.assertEqual(report['status'], 'ready_for_next_offline_review')
        self.assertEqual(report['ledger_version'], '0012')
        self.assertEqual(report['ledger_mode'], 'mixed')
        self.assertIs(report['writer_stop_verified'], False)
        self.assertEqual(set(report['route_constraints']),
                         {'tasks', 'devices', 'schedules', 'schedule_firings', 'route_audit'})
        for table in report['route_constraints'].values():
            self.assertEqual(table['allowed_routes'], ['http_synthetic', 'local_synthetic'])
            self.assertRegex(table['ddl_sha256'], r'^[0-9a-f]{64}$')
        self.assertEqual(report['collector_cursors'], dict(events=True, control_events=True, schedule_events=True))
        self.assertFalse(any(report['blockers'].values()))

    def test_missing_table_is_refused_without_exposing_data(self):
        self.activate()
        self.update('DROP TABLE schedule_firings')
        self.assertEqual(self.report(1)['reason'], 'required_schema_objects_missing')

    def test_version_row_does_not_authorize_unknown_schema(self):
        self.activate()
        self.update('DROP TRIGGER tasks_route_immutable')
        self.assertEqual(self.report(1)['reason'], 'unknown_0012_schema')

    def test_unknown_migration_history_is_refused_without_echoing_it(self):
        self.activate()
        self.update('INSERT INTO schema_migrations VALUES(?,?,?)', ('0013', 'private-value', 'fixture-date'))
        report = self.report(1)
        self.assertEqual(report['reason'], 'unknown_migration_history')
        self.assertNotIn('private-value', json.dumps(report))

    def test_isolated_ledger_is_refused(self):
        self.assertEqual(self.report(1)['reason'], 'mixed_ledger_required')

    def test_missing_cursor_is_blocked(self):
        self.activate()
        self.update("DELETE FROM collector_cursors WHERE stream='control_events'")
        report = self.report(2)
        self.assertFalse(report['collector_cursors']['control_events'])
        self.assertTrue(report['blockers']['collector_cursors_missing_or_inconsistent'])

    def test_pending_and_active_then_unknown_are_blocked(self):
        self.activate()
        kind = FixtureKind()
        store = SqliteStore(self.ledger, lease_ttl_seconds=30, kinds={kind.name: kind}, route=LOCAL_ROUTE)
        store.register_device('local-fixture', ['fixture'])
        task, _ = store.submit(TaskSpec('fixed', kind.name, 1, {'body': 'private-value'}, (),
                                       ('fixture',), 'synthetic', 'none', 1))
        self.assertTrue(self.report(2)['blockers']['pending_tasks'])
        lease = store.claim('local-fixture', task_id=task.task_id)
        report = self.report(2)
        self.assertNotIn('private-value', json.dumps(report))
        self.assertTrue(report['blockers']['leased_tasks'])
        self.assertTrue(report['blockers']['active_leases'])
        store.mark_unknown(lease.lease_id, lease.fence, 'fixture', 'local-fixture')
        report = self.report(2)
        self.assertTrue(report['blockers']['unknown_tasks'])
        self.assertTrue(report['blockers']['unknown_leases'])

    def test_held_budget_is_blocked(self):
        self.activate()
        self.update('INSERT INTO control_budgets VALUES(?,?,?,?,?,?,?,?)',
                    ('private-id', 'fixture', 'units', 1, 1, 0, 'fixture-start', 'fixture-end'))
        report = self.report(2)
        self.assertTrue(report['blockers']['held_budget_units'])
        self.assertNotIn('private-id', json.dumps(report))

    def test_uncertain_budget_reservation_is_blocked(self):
        self.activate()
        kind = FixtureKind()
        store = SqliteStore(self.ledger, lease_ttl_seconds=30, kinds={kind.name: kind}, route=LOCAL_ROUTE)
        task, _ = store.submit(TaskSpec('fixed', kind.name, 1, {}, (), (), 'synthetic', 'none', 1))
        store.cancel(task.task_id, 'fixture')
        self.update('INSERT INTO control_budgets VALUES(?,?,?,?,?,?,?,?)',
                    ('budget', 'fixture', 'units', 1, 0, 0, 'fixture-start', 'fixture-end'))
        self.update('''INSERT INTO control_reservations(task_id,fence,budget_id,fingerprint,reserved_units,
                      state,created_at) VALUES(?,1,'budget',?,1,'uncertain','fixture-date')''',
                    (task.task_id, task.fingerprint))
        report = self.report(2)
        self.assertTrue(report['blockers']['unsettled_budget_reservations'])
        self.assertFalse(report['blockers']['held_budget_units'])

    def test_journal_sidecar_is_not_removed_or_opened(self):
        self.activate()
        self.ledger.with_name(self.ledger.name + '-journal').write_bytes(b'unclear write state')
        self.assertEqual(self.report(1)['reason'], 'sqlite_sidecar_present')

    def test_wal_mode_is_refused_even_without_sidecars(self):
        self.activate()
        self.update('PRAGMA journal_mode=WAL')
        self.assertEqual(self.report(1)['reason'], 'offline_rollback_journal_ledger_required')

    def test_size_limit_is_enforced_before_sqlite_open(self):
        self.activate()
        self.assertEqual(external_preflight.MAX_LEDGER_BYTES, 64 * 1024 * 1024)
        with (patch.object(external_preflight, 'MAX_LEDGER_BYTES', self.ledger.stat().st_size - 1),
              patch.object(external_preflight.sqlite3, 'connect') as connect):
            self.assertEqual(self.report(1)['reason'], 'ledger_size_limit_exceeded')
            connect.assert_not_called()

    def test_directory_symlink_is_refused_before_open(self):
        self.activate()
        redirected = self.root / 'redirected'
        try:
            redirected.symlink_to(self.root, target_is_directory=True)
        except OSError:
            self.skipTest('directory symlink creation is unavailable')
        self.ledger = redirected / self.ledger.name
        self.assertEqual(self.report(1)['reason'], 'ledger_directory_not_plain')

    def test_parent_reparse_directory_is_refused_before_sqlite_open(self):
        real_lstat = type(self.ledger).lstat

        def lstat(path):
            if path == self.root:
                return SimpleNamespace(st_mode=stat.S_IFDIR | 0o700,
                                       st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT)
            return real_lstat(path)

        with (patch.object(type(self.ledger), 'lstat', lstat),
              patch.object(external_preflight.sqlite3, 'connect') as connect):
            self.assertEqual(self.report(1)['reason'], 'ledger_directory_not_plain')
            connect.assert_not_called()

    @unittest.skipUnless(os.name == 'nt', 'Windows UNC path syntax')
    def test_wsl_unc_is_refused_without_access(self):
        for server in ('wsl$', 'wsl.localhost'):
            with self.subTest(server=server):
                self.ledger = Path(rf'\\{server}\Debian\var\lib\dispatch.sqlite3')
                self.assertEqual(self.report(1)['reason'], 'absolute_ledger_path_required')
