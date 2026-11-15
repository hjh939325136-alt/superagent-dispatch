import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from dataclasses import asdict
import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.models import DispatchError, Evidence, Outcome, TaskSpec
from sa_dispatch.scheduler import Scheduler, SCHEMA
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


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.clock = FakeClock()
        self.path = Path(self.temp.name) / 'ledger.sqlite3'
        self.kinds = {'lab.read': ReadKind()}
        self.store = self.open_store()
        self.scheduler = Scheduler(self.store)
        self.spec = TaskSpec('template', 'lab.read', 1, {}, (), ('cpu',), 'synthetic', 'none', 2)
        self.store.register_device('program', ['cpu'])

    def open_store(self):
        return SqliteStore(self.path, clock=self.clock, lease_ttl_seconds=10, kinds=self.kinds)

    def schedule(self, **kwargs):
        self.scheduler.upsert('read-status', interval_seconds=10,
                              task_template=kwargs.pop('spec', self.spec),
                              next_fire_at=timestamp(self.clock.now()), **kwargs)

    def test_three_slots_complete_once_each(self):
        self.schedule()
        for _ in range(3):
            self.assertEqual(len(self.scheduler.tick()), 1)
            self.assertEqual(self.scheduler.tick(), [])
            lease = self.store.claim('program')
            self.store.complete(lease.lease_id, lease.fence,
                                Outcome(evidence=(Evidence('fixture', 'sha256:' + 'a' * 64, 'synthetic'),)),
                                'program')
            self.clock.advance(10)
        firings = self.scheduler.firings()
        self.assertEqual(len({row['fire_key'] for row in firings}), 3)
        self.assertEqual(len(self.store.list_tasks()), 3)
        self.assertTrue(all(row.status == 'succeeded' for row in self.store.list_tasks()))

    def test_crash_after_submit_before_acknowledgement_recovers_same_task(self):
        self.schedule()
        real_submit = self.store.submit

        def crash_after_commit(spec):
            real_submit(spec)
            raise RuntimeError('simulated process termination')

        with patch.object(self.store, 'submit', side_effect=crash_after_commit):
            with self.assertRaises(RuntimeError):
                self.scheduler.tick()
        recovered = Scheduler(self.open_store())
        result = recovered.tick()
        self.assertTrue(result[0]['deduplicated'])
        self.assertEqual(len(self.store.list_tasks()), 1)
        self.assertEqual(recovered.tick(), [])

    def test_concurrent_ticks_do_not_duplicate_slots(self):
        self.schedule()
        with ThreadPoolExecutor(4) as pool:
            list(pool.map(lambda _: Scheduler(self.open_store()).tick(), range(4)))
        self.assertEqual(len(self.store.list_tasks()), 1)
        self.assertEqual(len(self.scheduler.firings()), 1)

    def test_reserved_template_survives_edit(self):
        self.schedule()
        with patch.object(self.store, 'submit', side_effect=RuntimeError('before submit')):
            with self.assertRaises(RuntimeError):
                self.scheduler.tick()
        self.clock.advance(10)
        self.schedule(spec=replace(self.spec, params={'revision': 2}), enabled=False)
        self.scheduler.tick()
        self.assertEqual(self.store.list_tasks()[0].params, {})
        self.clock.advance(10)
        self.assertEqual(self.scheduler.tick(), [])

    def test_bounded_catchup_and_no_recycle(self):
        self.schedule(catchup='all', max_catchup=2)
        self.clock.advance(100)
        self.assertEqual(len(self.scheduler.tick(limit=2)), 2)
        self.assertEqual(len(self.scheduler.firings()), 11)
        self.assertEqual(sum(row['outcome'] == 'skipped_catchup' for row in self.scheduler.firings()), 9)
        with self.assertRaises(DispatchError):
            self.scheduler.upsert('read-status', interval_seconds=10, task_template=self.spec,
                                  next_fire_at='2026-01-01T00:00:00Z')

    def test_real_data_and_effectful_kinds_rejected(self):
        with self.assertRaises(DispatchError):
            self.schedule(spec=replace(self.spec, access='local_only'))
        self.kinds['lab.read'].side_effect = 'external'
        with self.assertRaises(DispatchError):
            self.schedule()
        self.assertEqual(self.store.list_tasks(), [])

    def test_three_day_downtime_policies(self):
        for policy, expected in [('skip', 1), ('latest', 1), ('all', 2)]:
            with self.subTest(policy=policy):
                scheduler = Scheduler(self.open_store())
                scheduler.upsert(policy, interval_seconds=86400, task_template=self.spec,
                                 next_fire_at=timestamp(self.clock.now()), catchup=policy, max_catchup=2)
        self.clock.advance(86400 * 2)
        self.scheduler.tick()
        for policy, expected in [('skip', 1), ('latest', 1), ('all', 2)]:
            rows = [row for row in self.scheduler.firings() if row['schedule_id'] == policy]
            self.assertEqual(len(rows), 3)
            self.assertEqual(sum(row['outcome'] == 'created' for row in rows), expected)

    def test_rule_version_and_clock_rollback(self):
        self.schedule()
        self.scheduler.tick()
        self.clock.advance(10)
        self.schedule(spec=replace(self.spec, params={'revision': 2}))
        self.scheduler.tick()
        self.assertIn(':v2:', self.scheduler.firings()[-1]['fire_key'])
        self.clock.advance(-20)
        self.assertEqual(self.scheduler.tick(), [])
        self.assertEqual(self.scheduler.status()['events'][-1]['kind'], 'schedule.clock_skew')
        self.assertEqual(len(self.store.list_tasks()), 2)

    def test_invalid_stored_template_records_error_without_claiming_created(self):
        self.schedule()
        with self.store._tx() as conn:
            conn.execute("UPDATE schedules SET task_template='not json'")
        self.assertEqual(self.scheduler.tick(), [])
        self.assertEqual(self.scheduler.firings(), [])
        self.assertEqual(self.scheduler.status()['events'][-1]['kind'], 'schedule.template_error')
        self.assertEqual(self.store.list_tasks(), [])

    def test_explicit_additive_upgrade_preserves_old_schedule(self):
        legacy_path = Path(self.temp.name) / 'old.sqlite3'
        conn = sqlite3.connect(legacy_path)
        try:
            conn.executescript(SCHEMA)
            conn.execute('INSERT INTO schedules VALUES(?,?,?,?,?,?)',
                         ('legacy',10,json.dumps(asdict(self.spec)),timestamp(self.clock.now()),None,1))
            conn.commit()
        finally:
            conn.close()
        legacy = SqliteStore(legacy_path, clock=self.clock, lease_ttl_seconds=10, kinds=self.kinds)
        with legacy._tx() as conn:
            before = {table:[dict(row) for row in conn.execute('SELECT * FROM '+table)]
                      for table in ('tasks','events','leases','evidence','late_results','devices')}
        upgraded = Scheduler(legacy)
        row = upgraded.status()['schedules'][0]
        self.assertEqual((row['id'],row['version'],row['catchup']), ('legacy',1,'all'))
        with legacy._tx() as conn:
            after = {table:[dict(row) for row in conn.execute('SELECT * FROM '+table)] for table in before}
        self.assertEqual(after, before)
        self.assertEqual(len(upgraded.tick()),1)

    def audit_fires(self):
        with self.store._tx() as conn:
            created_orphans = conn.execute("""SELECT count(*) FROM schedule_firings f LEFT JOIN tasks t
                ON t.task_id=f.task_id WHERE f.outcome='created' AND t.task_id IS NULL""").fetchone()[0]
            tasks_without_fires = conn.execute("""SELECT count(*) FROM tasks t LEFT JOIN schedule_firings f
                ON f.task_id=t.task_id WHERE t.idem_key LIKE 'sched:%' AND f.fire_key IS NULL""").fetchone()[0]
            exhausted_pending = conn.execute("SELECT count(*) FROM schedule_firings WHERE outcome='pending' AND failure_count>=3").fetchone()[0]
        self.assertEqual((created_orphans,tasks_without_fires,exhausted_pending),(0,0,0))

    def reserve_two_bad_slots(self):
        self.schedule(catchup='all', max_catchup=2)
        self.clock.advance(10)
        with patch.object(self.store, 'submit', side_effect=RuntimeError('process stops before submission')):
            with self.assertRaises(RuntimeError):
                self.scheduler.tick(limit=2)
        self.assertEqual(len(self.scheduler.firings()),2)

    def add_healthy_schedule(self):
        self.kinds['lab.good'] = ReadKind()
        self.scheduler.upsert('healthy', interval_seconds=60,
            task_template=replace(self.spec,kind='lab.good'),next_fire_at=timestamp(self.clock.now()))

    def test_two_permanent_bad_slots_terminate_and_healthy_schedule_runs(self):
        self.reserve_two_bad_slots()
        self.kinds['lab.read'].enabled = False
        self.add_healthy_schedule()
        self.assertEqual(self.scheduler.tick(limit=2), [])
        self.assertEqual(len(self.scheduler.tick(limit=2)),1)
        bad = [row for row in self.scheduler.firings() if row['schedule_id']=='read-status']
        self.assertTrue(all(row['outcome']=='rejected' and row['rejection_code']=='invalid_reserved_template' for row in bad))
        before = len(self.scheduler.status()['events'])
        self.scheduler.tick(limit=2)
        self.assertEqual(len(self.scheduler.status()['events']),before)
        self.audit_fires()

    def test_sqlite_retries_are_bounded_backed_off_and_fair(self):
        self.reserve_two_bad_slots()
        real_submit = self.store.submit
        attempted = []
        def busy_bad(spec):
            if spec.kind=='lab.read':
                attempted.append(spec.idem_key)
                raise sqlite3.OperationalError('database is locked')
            return real_submit(spec)
        with patch.object(self.store,'submit',side_effect=busy_bad):
            self.scheduler.tick(limit=2)
            self.add_healthy_schedule()
            self.assertEqual(len(self.scheduler.tick(limit=2)),1)
            for _ in range(5):
                self.scheduler.tick(limit=2)
            self.assertEqual(len(attempted),2)
            self.clock.advance(1)
            self.scheduler.tick(limit=2)
            self.clock.advance(2)
            self.scheduler.tick(limit=2)
        self.assertEqual(len(attempted),6)
        bad = [row for row in self.scheduler.firings() if row['schedule_id']=='read-status']
        self.assertTrue(all(row['outcome']=='rejected' and row['failure_count']==3 and
                            row['rejection_code']=='sqlite_retry_exhausted' for row in bad))
        self.audit_fires()

    def test_fingerprint_conflict_after_process_commit_preserves_link(self):
        self.schedule()
        real_submit = self.store.submit
        def crash(spec):
            real_submit(spec)
            raise RuntimeError('death after commit')
        with patch.object(self.store,'submit',side_effect=crash):
            with self.assertRaises(RuntimeError):
                self.scheduler.tick()
        self.kinds['lab.read'].validate = lambda params: dict(params, changed_default=True)
        self.scheduler.tick(synthetic_handoff=True)
        row = self.scheduler.firings()[0]
        self.assertEqual((row['outcome'],row['rejection_code']),('rejected','idempotency_conflict'))
        self.assertEqual(self.store.get_task(row['task_id']).status,'cancelled')
        self.audit_fires()

    def test_whole_ledger_lock_pauses_then_fails_visibly(self):
        with patch.object(self.store,'_tx',side_effect=sqlite3.OperationalError('database is locked')) as blocked:
            self.assertEqual(self.scheduler.tick(),[])
            self.assertEqual(self.scheduler.tick(),[])
            self.assertEqual(blocked.call_count,1)
            self.clock.advance(1)
            self.assertEqual(self.scheduler.tick(),[])
            self.clock.advance(2)
            with self.assertRaisesRegex(DispatchError,'three attempts'):
                self.scheduler.tick()


if __name__ == '__main__':
    unittest.main()
