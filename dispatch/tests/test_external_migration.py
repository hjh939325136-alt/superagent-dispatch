"""Cold 0012 upgrade, pre-commit crash recovery, and route checks."""
from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.bundle import snapshot_records
from sa_dispatch.canonical import canonical_json
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.external_migration import UpgradeRejected, upgrade, verify_schema
from sa_dispatch.models import Evidence, Outcome, TaskSpec
from sa_dispatch.routing import (EXTERNAL_ROUTE, LOCAL_ROUTE, RouteRejected, activate_mixed,
                                 activation_manifest, validate_mixed_records)
from sa_dispatch.store_sqlite import SqliteStore


class FixtureKind:
    name = 'fixture.upgrade'
    version = 1
    side_effect = 'none'
    enabled = True

    def validate(self, params):
        return params

    def scope(self, params):
        return {}


class ExternalMigrationTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.ledger = self.root / 'dispatch.sqlite3'
        self.recovery = self.root / 'recovery'
        self.clock = FakeClock()
        self.at = timestamp(self.clock.now())
        self.cas = ArtifactStore(self.root / 'cas')
        self.kinds = {FixtureKind.name: FixtureKind()}
        old = self.store()
        prior, _ = old.submit(self.spec('prior'))
        old.cancel(prior.task_id, 'fixture')
        with closing(old._connect()) as conn:
            proof = self.cas.put(canonical_json(activation_manifest(conn, actor='operator', bindings={})))
        activate_mixed(self.ledger, actor='operator', evidence_ref=proof, bindings={}, artifacts=self.cas)
        local = self.store(LOCAL_ROUTE)
        local.register_device('local-runner', ['fixture'])
        parent, _ = local.submit(self.spec('parent'))
        local.cancel(parent.task_id, 'fixture')
        completed, _ = local.submit(self.spec('current', parent.task_id))
        lease = local.claim('local-runner')
        body = self.cas.put(b'fixture evidence')
        local.complete(lease.lease_id, lease.fence, Outcome(evidence=(Evidence('fixture', body, 'synthetic'),)),
                       'local-runner')
        self.task_id = completed.task_id
        with closing(self.connect()) as conn, conn:
            # Give rebuilt rowid tables nontrivial identities to detect implicit remapping.
            conn.execute('UPDATE tasks SET rowid=17 WHERE task_id=?', (self.task_id,))
            conn.execute('UPDATE route_audit SET seq=14 WHERE action=\'register\'')
            conn.execute('INSERT INTO schedules(id,interval_seconds,task_template,next_fire_at,enabled,execution_route) '
                         'VALUES(?,?,?,?,?,?)', ('local', 17, '{}', self.at, 0, LOCAL_ROUTE))
            conn.execute('INSERT INTO schedule_firings(fire_key,schedule_id,scheduled_at,task_template,task_id,'
                         'outcome,execution_route) VALUES(?,?,?,?,?,?,?)',
                         ('slot', 'local', self.at, '{}', self.task_id, 'created', LOCAL_ROUTE))
            conn.execute("INSERT INTO control_budgets VALUES('budget','slot','calls',5,0,1,?,?)",
                         (self.at, '2026-02-01T00:00:00.000000+00:00'))
            conn.execute('INSERT INTO control_task_rules VALUES(?,?,\'budget\',1,NULL,NULL)',
                         (self.task_id, completed.fingerprint))
            conn.execute('INSERT INTO control_reservations VALUES(?,1,\'budget\',?,1,1,\'settled\',?,?,?,NULL)',
                         (self.task_id, completed.fingerprint, body, self.at, self.at))
            conn.executemany('INSERT INTO collector_cursors VALUES(?,?,?,1,3,?,?,?,?,'
                             "'0012','local_owned',0)",
                             [('common-dispatch', stream, 'fixture-source', 'a' * 64, self.at, 'b' * 64, 'c' * 64)
                              for stream in ('events', 'schedule_events', 'control_events')])

    def store(self, route=None):
        return SqliteStore(self.ledger, clock=self.clock, lease_ttl_seconds=20, kinds=self.kinds, route=route)

    def spec(self, key, parent=None):
        return TaskSpec(key, FixtureKind.name, 1, {}, (), ('fixture',), 'synthetic', 'none', 1,
                        parent_task_id=parent)

    def connect(self, path=None):
        conn = sqlite3.connect(path or self.ledger)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys=ON')
        return conn

    def snapshot(self, path=None):
        with closing(self.connect(path)) as conn:
            conn.execute('BEGIN')
            return verify_schema(conn), snapshot_records(conn)

    def migrate(self):
        return upgrade(self.ledger, self.recovery, offline=True, clock=self.clock)

    def test_upgrade_preserves_all_rows_constraints_and_recovery_copy(self):
        old_version, before = self.snapshot()
        self.assertEqual(old_version, '0012')
        receipt = self.migrate()
        self.assertTrue(receipt['changed'])
        self.assertTrue(receipt['recovery_verified'])
        version, after = self.snapshot()
        self.assertEqual(version, '0013')
        self.assertEqual(after['schema_migrations'].pop()['version'], '0013')
        self.assertEqual(before, after)
        self.assertEqual(self.snapshot(Path(receipt['recovery_path']) / 'ledger.sqlite3'), ('0012', before))
        self.assertEqual(self.migrate(), dict(receipt, changed=False))
        with closing(self.connect()) as conn:
            self.assertEqual(conn.execute('SELECT rowid FROM tasks WHERE task_id=?', (self.task_id,)).fetchone()[0], 17)
            self.assertEqual(conn.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
            self.assertIsNone(conn.execute('PRAGMA foreign_key_check').fetchone())
            guards = (
                ('INSERT INTO devices VALUES(?, ?, ?, NULL)', ('new', '[]', self.at), 'required'),
                ('INSERT INTO devices VALUES(?, ?, ?, ?)', ('new', '[]', self.at, 'unregistered'), 'CHECK'),
                ('UPDATE tasks SET execution_route=? WHERE task_id=?', (EXTERNAL_ROUTE, self.task_id), 'immutable'),
                ("UPDATE ledger_mode SET mode='isolated'", (), 'irreversible'),
                ('DELETE FROM ledger_mode', (), 'cannot be deleted'),
                ('UPDATE tasks SET parent_task_id=? WHERE task_id=?', ('missing', self.task_id), 'FOREIGN KEY'),
            )
            for sql, args, message in guards:
                with self.subTest(sql=sql), self.assertRaisesRegex(sqlite3.IntegrityError, message), conn:
                    conn.execute(sql, args)
            with conn:
                conn.execute('INSERT INTO devices VALUES(?, ?, ?, ?)',
                             ('agent-test', '["agent.deepseek"]', self.at, EXTERNAL_ROUTE))
            with self.assertRaisesRegex(RouteRejected, 'outside its execution profile'):
                validate_mixed_records(snapshot_records(conn))
            with self.assertRaisesRegex(RouteRejected, 'inconsistent device, slot, or control'):
                validate_mixed_records(snapshot_records(conn), allow_external=True)

    def test_abrupt_process_exit_after_rebuild_rolls_back_and_reuses_verified_copy(self):
        before = self.snapshot()
        script = """
import os
import sys
from sa_dispatch import external_migration as migration

original = migration._rebuild
def stop_after_rebuild(conn, *args):
    conn.execute('PRAGMA cache_size=1')
    original(conn, *args)
    os._exit(97)

migration._reference()
migration._rebuild = stop_after_rebuild
migration.upgrade(sys.argv[1], sys.argv[2], offline=True)
"""
        process = subprocess.run([sys.executable, '-c', script, str(self.ledger), str(self.recovery)],
                                 capture_output=True, timeout=20, check=False)
        self.assertEqual(process.returncode, 97, process.stderr.decode(errors='replace'))
        archive, = self.recovery.glob('dispatch-0012-*')
        frozen = (archive / 'receipt.json').read_bytes()
        self.assertEqual(self.snapshot(), before)
        self.assertTrue(self.migrate()['changed'])
        self.assertEqual((archive / 'receipt.json').read_bytes(), frozen)
        self.assertFalse(self.migrate()['changed'])

    def test_rejects_live_unknown_unsettled_and_mismatched_schema(self):
        with self.assertRaisesRegex(UpgradeRejected, 'offline maintenance'):
            upgrade(self.ledger, self.recovery)
        with self.assertRaisesRegex(UpgradeRejected, 'existing'):
            upgrade(self.root / 'missing.sqlite3', self.recovery, offline=True)
        for sql, reverse, message in (
                ("UPDATE tasks SET status='unknown' WHERE task_id=?", "UPDATE tasks SET status='succeeded' WHERE task_id=?",
                 'active or unknown'),
                ("UPDATE leases SET state='active' WHERE task_id=?", "UPDATE leases SET state='completed' WHERE task_id=?",
                 'active or unknown'),
                ("UPDATE control_reservations SET state='uncertain' WHERE task_id=?",
                 "UPDATE control_reservations SET state='settled' WHERE task_id=?", 'unsettled control budget')):
            with self.subTest(sql=sql), closing(self.connect()) as conn:
                with conn:
                    conn.execute(sql, (self.task_id,))
                before = self.snapshot()
                with self.assertRaisesRegex(UpgradeRejected, message):
                    self.migrate()
                self.assertEqual(self.snapshot(), before)
                with conn:
                    conn.execute(reverse, (self.task_id,))
        with closing(self.connect()) as conn, conn:
            conn.execute('DROP TRIGGER tasks_route_required')
        with self.assertRaisesRegex(UpgradeRejected, 'fixed 0012 or 0013'):
            self.migrate()
        self.assertFalse(self.recovery.exists())

    def test_rejects_tampered_recovery_and_corrupt_foreign_key(self):
        receipt = self.migrate()
        archive = Path(receipt['recovery_path']) / 'ledger.sqlite3'
        with archive.open('ab') as handle:
            handle.write(b'changed')
        with self.assertRaisesRegex(UpgradeRejected, 'receipt does not match'):
            self.migrate()
        with closing(self.connect()) as conn, conn:
            conn.execute('PRAGMA foreign_keys=OFF')
            conn.execute("UPDATE tasks SET parent_task_id='absent' WHERE task_id=?", (self.task_id,))
        with self.assertRaisesRegex(UpgradeRejected, 'foreign-key check'):
            self.migrate()


if __name__ == '__main__':
    unittest.main()
