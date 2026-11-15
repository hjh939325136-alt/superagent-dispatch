"""Lifetime reservation bounds, including actual competing/crashing processes."""
from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

import test_scheduler as fixtures
from sa_dispatch.clock import timestamp
from sa_dispatch.models import DispatchError
from sa_dispatch.scheduler import Scheduler,ScheduleExhausted


class BoundedSchedulerTests(unittest.TestCase):
    setUp=fixtures.SchedulerTests.setUp
    open_store=fixtures.SchedulerTests.open_store
    schedule=fixtures.SchedulerTests.schedule

    def update(self,**kwargs):
        row=self.scheduler.status()['schedules'][0]
        return self.scheduler.upsert(row['id'],interval_seconds=kwargs.pop('interval_seconds',row['interval_seconds']),
            task_template=self.spec,next_fire_at=kwargs.pop('next_fire_at',row['next_fire_at']),**kwargs)

    def exhausted_events(self):
        return [e for e in self.scheduler.status()['events'] if e['kind']=='schedule.exhausted']

    def test_earliest_three_slots_before_each_catchup_policy(self):
        for policy in ('skip','latest','all'):
            self.scheduler.upsert(policy,interval_seconds=10,task_template=self.spec,
                next_fire_at=timestamp(self.clock.now()),catchup=policy,max_catchup=3,max_slots=3)
        self.clock.advance(100);self.scheduler.tick()
        state=self.scheduler.status()
        for policy in ('skip','latest','all'):
            rows=[f for f in state['firings'] if f['schedule_id']==policy]
            self.assertEqual([r['scheduled_at'] for r in rows],
                ['2026-01-01T00:00:00.000000+00:00','2026-01-01T00:00:10.000000+00:00','2026-01-01T00:00:20.000000+00:00'])
            self.assertEqual(sum(r['outcome']=='created' for r in rows),3 if policy=='all' else 1)
        self.assertTrue(all(r['enabled']==0 for r in state['schedules']))
        self.assertTrue(all(r['next_fire_at']=='2026-01-01T00:00:30.000000+00:00' for r in state['schedules']))
        self.scheduler=Scheduler(self.open_store());self.clock.advance(1000)
        self.assertEqual(self.scheduler.tick(),[]);self.assertEqual(len(self.exhausted_events()),3)

    def test_capacity_skipped_and_rejected_reservations_still_consume_slots(self):
        self.schedule(max_slots=3,catchup='all',max_catchup=3);self.clock.advance(100)
        with patch.object(self.store,'submit',side_effect=DispatchError('permanent rejection')):
            self.scheduler.tick(limit=1)
        self.assertEqual(sorted(r['outcome'] for r in self.scheduler.firings()),['rejected','skipped_catchup','skipped_catchup'])
        self.assertEqual(self.scheduler.status()['schedules'][0]['enabled'],0)
        self.assertEqual(len(self.exhausted_events()),1)

    def test_clock_rollback_and_edit_do_not_reset_lifetime_count(self):
        self.schedule(max_slots=3);self.scheduler.tick();self.clock.advance(10);self.scheduler.tick()
        self.clock.advance(-20);self.assertEqual(self.scheduler.tick(),[])
        self.assertEqual(len(self.scheduler.firings()),2)
        self.update(interval_seconds=7)
        self.clock.advance(30);self.scheduler.tick()
        self.assertEqual(len(self.scheduler.firings()),3)
        self.assertIn(':v2:',self.scheduler.firings()[-1]['fire_key'])
        with self.assertRaises(ScheduleExhausted):self.update()
        with self.assertRaises(DispatchError):self.update(max_slots=2,enabled=False)
        self.update(max_slots=5,catchup='all',max_catchup=3)
        self.clock.advance(100);self.scheduler.tick()
        self.assertEqual(len(self.scheduler.firings()),5)
        self.assertEqual(len({r['fire_key'] for r in self.scheduler.firings()}),5)
        self.assertEqual(len(self.exhausted_events()),2)

    def test_omission_preserves_bound_explicit_null_removes_with_event(self):
        self.schedule(max_slots=3)
        self.update(enabled=False);self.assertEqual(self.scheduler.status()['schedules'][0]['max_slots'],3)
        self.update(max_slots=None)
        self.assertIsNone(self.scheduler.status()['schedules'][0]['max_slots'])
        self.update()
        self.assertEqual(sum(e['kind']=='schedule.bound_removed' for e in self.scheduler.status()['events']),1)
        for value in (True,False,0,-1,100001,3.0,'3'):
            with self.subTest(value=value):
                with self.assertRaises(DispatchError):self.update(max_slots=value)

    def test_equal_used_disables_once_and_pending_still_recovers(self):
        self.schedule()
        with patch.object(self.store,'submit',side_effect=RuntimeError('crash after reservation')):
            with self.assertRaises(RuntimeError):self.scheduler.tick()
        self.update(max_slots=1)
        self.assertEqual(self.scheduler.status()['schedules'][0]['enabled'],0)
        self.update(enabled=False)
        self.assertEqual(len(self.exhausted_events()),1)
        self.scheduler.tick();self.assertEqual(self.scheduler.firings()[0]['outcome'],'created')
        self.clock.advance(100);self.assertEqual(self.scheduler.tick(),[])
        self.assertEqual(len(self.store.list_tasks()),1)

    def child(self,code,*args):
        env=dict(os.environ)
        env['PYTHONPATH']=os.pathsep.join((str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)))
        return subprocess.Popen([sys.executable,'-B','-c',code,str(self.path),*map(str,args)],env=env,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE)

    def test_two_processes_each_tick_100_times_never_exceed_three(self):
        self.schedule(max_slots=3,catchup='all',max_catchup=3)
        code='''import sys,time
from pathlib import Path
from test_scheduler import ReadKind
from sa_dispatch.clock import FakeClock
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.scheduler import Scheduler
s=Scheduler(SqliteStore(sys.argv[1],clock=FakeClock('2026-01-01T00:01:40Z'),lease_ttl_seconds=10,kinds={'lab.read':ReadKind()}))
Path(sys.argv[2]).write_text('ready');deadline=time.monotonic()+10
while not Path(sys.argv[3]).exists():
 if time.monotonic()>deadline:raise RuntimeError('barrier timeout')
 time.sleep(.01)
for _ in range(100):s.tick()
'''
        root=self.path.parent;gate=root/'go';children=[self.child(code,root/f'ready-{i}',gate) for i in range(2)]
        try:
            deadline=time.monotonic()+10
            while not all((root/f'ready-{i}').exists() for i in range(2)):
                if time.monotonic()>deadline:self.fail('children did not reach barrier')
                time.sleep(.01)
            gate.write_text('go')
            for child in children:
                out,err=child.communicate(timeout=20);self.assertEqual(child.returncode,0,(out,err))
        finally:
            for child in children:
                if child.poll() is None:child.kill();child.communicate(timeout=5)
        self.assertEqual(len(self.scheduler.firings()),3);self.assertEqual(len(self.store.list_tasks()),3)
        self.assertEqual(len(self.exhausted_events()),1)

    def test_actual_process_crash_after_third_reservation_before_submit(self):
        self.schedule(max_slots=3);self.scheduler.tick();self.clock.advance(10);self.scheduler.tick()
        code='''import os,sys
from test_scheduler import ReadKind
from sa_dispatch.clock import FakeClock
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.scheduler import Scheduler
s=SqliteStore(sys.argv[1],clock=FakeClock('2026-01-01T00:00:20Z'),lease_ttl_seconds=10,kinds={'lab.read':ReadKind()})
s.submit=lambda spec:os._exit(73)
Scheduler(s).tick()
'''
        child=self.child(code);out,err=child.communicate(timeout=10)
        self.assertEqual(child.returncode,73,(out,err))
        self.assertEqual(len(self.scheduler.firings()),3)
        self.assertEqual(self.scheduler.status()['schedules'][0]['enabled'],0)
        self.clock.advance(10);Scheduler(self.open_store()).tick()
        with self.store._tx() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM schedule_firings WHERE outcome='pending'").fetchone()[0],0)
            self.assertEqual(conn.execute("SELECT count(*) FROM schedule_firings f LEFT JOIN tasks t ON t.task_id=f.task_id WHERE f.outcome='created' AND t.task_id IS NULL").fetchone()[0],0)
            self.assertEqual(conn.execute("SELECT count(*) FROM tasks t LEFT JOIN schedule_firings f ON f.task_id=t.task_id WHERE t.idem_key LIKE 'sched:%' AND f.task_id IS NULL").fetchone()[0],0)
        self.assertEqual(len(self.store.list_tasks()),3);self.assertEqual(len(self.exhausted_events()),1)


if __name__=='__main__':unittest.main()
