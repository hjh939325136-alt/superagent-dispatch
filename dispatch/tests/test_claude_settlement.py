"""Synthetic Claude launch-count accounting; no model or authentication use."""
import json,unittest
from contextlib import closing
from datetime import timedelta
from unittest.mock import patch
import test_claude_adapter as fixture
from sa_dispatch.control_policy import ControlPolicy
from sa_dispatch.clock import timestamp

class ClaudeSettlementTests(unittest.TestCase):
    setUp=fixture.ClaudeAdapterTests.setUp
    task=fixture.ClaudeAdapterTests.task
    expire=fixture.ClaudeAdapterTests.expire
    calls=fixture.ClaudeAdapterTests.calls
    def reserved(self,text='hello',unit='claude_run'):
        with patch.object(self.store,'claim',return_value=None):task,_=self.task(text)
        policy=ControlPolicy(self.store,artifacts=self.artifacts)
        now=self.store.clock.now()
        policy.budget_upsert('fixture',period='test',unit=unit,limit_units=10,
            starts_at=timestamp(now-timedelta(minutes=1)),ends_at=timestamp(now+timedelta(hours=1)))
        policy.bind_task(task.task_id,budget_id='fixture',reserve_units=1)
        return task,self.store.claim('claude')
    def reservations(self):
        with closing(self.store._connect()) as conn:return [dict(r) for r in conn.execute('SELECT * FROM control_reservations ORDER BY fence')]
    def test_origin_one_observer_zero_resume_new_origin_one(self):
        task,lease=self.reserved('sleep');report=self.adapter.run(lease,timeout_s=.2)
        self.assertTrue(report['cost_unknown']);self.assertEqual(self.reservations()[0]['actual_units'],1)
        self.expire(lease);observer=self.store.claim('claude')
        observed=self.adapter.run(observer)
        self.assertEqual(observed['origin_fence'],1)
        self.assertEqual([r['actual_units'] for r in self.reservations()],[1,0])
        self.expire(observer);resume=self.store.claim('claude');self.adapter.run(resume,resume=True)
        self.assertEqual([r['actual_units'] for r in self.reservations()],[1,0,1]);self.assertEqual(len(self.calls()),2)
        before=self.reservations();result=self.adapter.settle(task.task_id)
        self.assertEqual(before,self.reservations());self.assertEqual(len(result['already_settled']),3)
        self.assertTrue(result['cost_unknown'])
        self.assertEqual(len(result['report_refs']),3)
    def test_currency_and_unknown_launch_keep_reservation(self):
        task,lease=self.reserved(unit='usd');self.adapter.run(lease)
        self.assertNotEqual(self.reservations()[0]['state'],'settled')
        self.assertEqual(self.adapter.settle(task.task_id)['held'][0]['fence'],lease.fence)
    def test_missing_or_foreign_exit_cannot_settle(self):
        task,lease=self.reserved()
        with patch.object(self.adapter,'settle',return_value={}):report=self.adapter.run(lease)
        row=self.adapter.attempt(task.task_id,lease.fence)
        foreign=dict(report);foreign['exit_ref']=self.artifacts.put(b'{"exit_code":0,"child_pid":999,"supervisor_pid":999}')
        with self.store._tx() as conn:conn.execute('UPDATE claude_attempts SET report_ref=? WHERE task_id=?',(self.artifacts.put(json.dumps(foreign).encode()),task.task_id))
        result=self.adapter.settle(task.task_id)
        self.assertEqual(len(result['held']),1);self.assertNotEqual(self.reservations()[0]['state'],'settled')
        with self.store._tx() as conn:conn.execute('UPDATE claude_attempts SET report_ref=? WHERE task_id=?',(row['report_ref'],task.task_id))
        self.adapter.settle(task.task_id);self.assertEqual(self.reservations()[0]['actual_units'],1)
    def test_unstarted_attempt_does_not_refund(self):
        task,lease=self.reserved();self.adapter._prepare(lease,10,.01,False)
        result=self.adapter.settle(task.task_id)
        self.assertEqual(len(result['held']),1);self.assertIsNone(self.reservations()[0]['actual_units'])

    def test_cross_run_identity_and_boolean_pid_cannot_release_budget(self):
        task,lease=self.reserved()
        with patch.object(self.adapter,'settle',return_value={}):report=self.adapter.run(lease)
        for ref_field,field,value in [('exit_ref','task_id','foreign'),('launch_ref','session_id','foreign'),
                ('launch_ref','origin_fence',99),('exit_ref','child_pid',True)]:
            with self.subTest(ref_field=ref_field,field=field):
                body=json.loads(self.artifacts.get(report[ref_field]));body[field]=value
                forged={**report,ref_field:self.artifacts.put(json.dumps(body).encode())}
                with self.store._tx() as conn:conn.execute('UPDATE claude_attempts SET report_ref=? WHERE task_id=?',(self.artifacts.put(json.dumps(forged).encode()),task.task_id))
                result=self.adapter.settle(task.task_id)
                self.assertEqual(len(result['held']),1)
                self.assertIsNone(self.reservations()[0]['actual_units'])

    def test_unbudgeted_unobserved_or_unproven_attempt_never_means_zero_cost(self):
        with patch.object(self.store,'claim',return_value=None):task,_=self.task()
        empty=self.adapter.settle(task.task_id)
        self.assertTrue(empty['cost_unknown']);self.assertEqual(empty['observation'],'not_observed')
        lease=self.store.claim('claude');self.adapter._prepare(lease,10,.01,False)
        pending=self.adapter.settle(task.task_id)
        self.assertTrue(pending['cost_unknown']);self.assertEqual(pending['held'],[])
        self.assertEqual(pending['unproven_attempts'][0]['reason'],'report_missing')
        with self.store._tx() as conn:conn.execute('UPDATE claude_attempts SET report_ref=? WHERE task_id=?',(self.artifacts.put(b'{}'),task.task_id))
        invalid=self.adapter.settle(task.task_id)
        self.assertTrue(invalid['cost_unknown']);self.assertEqual(invalid['unproven_attempts'][0]['reason'],'terminal_evidence_unproven')

if __name__=='__main__':unittest.main()
