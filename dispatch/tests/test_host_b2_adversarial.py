"""B.2 crash boundaries using isolated HTTP fixtures and temporary ledgers."""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
import unittest

import test_host_adapter as fixture
from sa_dispatch.control_policy import ControlPolicy
from sa_dispatch.host_adapter import HostError


class HostB2AdversarialTests(unittest.TestCase):
    setUp = fixture.HostTests.setUp
    task = fixture.HostTests.task

    def expire(self, lease):
        with self.store._tx() as conn:
            conn.execute("UPDATE leases SET expires_at='2000-01-01' WHERE lease_id=?",(lease.lease_id,))
        self.store.sweep()

    def start_waiting(self, **kwargs):
        task, lease = self.task('wait', **kwargs)
        with self.assertRaisesRegex(HostError, 'HOST_OBSERVATION_TIMEOUT'):
            self.adapter.run(lease, timeout_s=.03)
        return task, lease

    def test_cancel_intent_survives_observer_fence_expiry(self):
        task, lease = self.start_waiting()
        self.adapter.cancel(task.task_id, lease.fence)
        self.expire(lease)
        adopted = self.store.claim('host')
        report = self.adapter.run(adopted)
        self.assertEqual(report['status'], 'cancelled')
        self.assertEqual(sum(p.endswith('/cancel') for p,_ in self.fixture.calls), 1)
        self.assertEqual(self.adapter.attempt(task.task_id,2)['adopted_fence'],1)

    def test_cancel_attempt_with_lost_response_is_not_reposted_after_adoption(self):
        task, lease = self.start_waiting()
        self.adapter.cancel(task.task_id,lease.fence)
        original = self.client.json
        attempted = []
        def lost_response(path, body=None):
            if path.endswith('/cancel'):
                attempted.append(path)
                # Remote result is unknown; keep fixture running to ensure that
                # terminal-state detection cannot hide a duplicate POST.
                raise HostError('HOST_UNREACHABLE')
            return original(path,body)
        with patch.object(self.client,'json',side_effect=lost_response):
            with self.assertRaisesRegex(HostError,'HOST_UNREACHABLE'):
                self.adapter._dispatch_cancel(lease)
        self.assertEqual(len(attempted),1)
        sent = self.adapter.attempt(task.task_id,1)['cancel_sent_at']
        self.assertIsNotNone(sent)
        self.expire(lease); adopted = self.store.claim('host')
        with self.assertRaisesRegex(HostError,'HOST_OBSERVATION_TIMEOUT'):
            self.adapter.run(adopted,timeout_s=.03)
        self.adapter.cancel(task.task_id,adopted.fence)
        self.adapter._dispatch_cancel(adopted)
        self.assertEqual(self.adapter.attempt(task.task_id,2)['cancel_sent_at'],sent)
        self.assertEqual(sum(p.endswith('/cancel') for p,_ in self.fixture.calls),0)
        row = self.adapter.attempt(task.task_id,2)
        self.fixture.finish(row['session_id'],'cancelled')
        self.assertEqual(self.adapter.run(adopted)['status'],'cancelled')

    def test_cached_orphan_terminal_replays_usage_after_crash_boundary(self):
        task,_ = self.task('wait',max_attempts=1,claim=False)
        policy = ControlPolicy(self.store,artifacts=self.artifacts)
        now = datetime.now(timezone.utc)
        policy.budget_upsert('hosts',period='fixture',unit='host_run',limit_units=2,
            starts_at=(now-timedelta(minutes=1)).isoformat(),ends_at=(now+timedelta(hours=1)).isoformat())
        policy.bind_task(task.task_id,budget_id='hosts',reserve_units=1)
        lease = self.store.claim('host')
        with self.assertRaisesRegex(HostError,'HOST_OBSERVATION_TIMEOUT'):
            self.adapter.run(lease,timeout_s=.03)
        self.expire(lease)
        with patch.object(self.adapter,'_settle_usage',side_effect=RuntimeError('crash after report commit')):
            with self.assertRaisesRegex(RuntimeError,'crash after report commit'):
                self.adapter.settle(task.task_id)
        self.assertEqual(self.adapter.attempt(task.task_id,1)['phase'],'orphan_cancelled')
        self.assertEqual(policy.status()['budgets'][0]['reserved_units'],1)
        report = self.adapter.settle(task.task_id)
        self.assertTrue(report['settled'])
        budget = policy.status()['budgets'][0]
        self.assertEqual((budget['used_units'],budget['reserved_units']),(1,0))
        proof = policy.status()['reservations'][0]['evidence_ref']
        self.assertEqual(self.adapter.settle(task.task_id),report)
        self.assertEqual(policy.status()['reservations'][0]['evidence_ref'],proof)
        self.assertEqual(sum(p.endswith('/cancel') for p,_ in self.fixture.calls),1)

    def test_cancel_before_submit_survives_fence_with_or_without_session(self):
        for create_session in (False,True):
            with self.subTest(create_session=create_session):
                task,lease = self.task('wait')
                row,_ = self.adapter._prepare(lease,False)
                if create_session:
                    session = self.client.json('/api/sessions',{'agent_id':row['agent_id'],'title':row['title']})
                    self.adapter._save(lease,session_id=session['id'],phase='session_ready')
                self.adapter.cancel(task.task_id,1)
                self.expire(lease); adopted = self.store.claim('host')
                report = self.adapter.run(adopted)
                self.assertEqual(report['status'],'cancelled')
                self.assertTrue(report['not_started'])
                self.assertEqual(self.adapter.attempt(task.task_id,1)['phase'],'cancelled')
        self.assertEqual(sum(p.endswith('/messages') or p.endswith('/cancel') for p,_ in self.fixture.calls),0)

    def test_foreign_prompt_is_never_cancelled_by_observer_or_orphan_settlement(self):
        task,lease = self.start_waiting(max_attempts=1)
        row = self.adapter.attempt(task.task_id,1)
        session = self.fixture.sessions[row['session_id']]
        user_event = next(e for e in session['events'] if e.get('type')=='message')
        user_event['message']['text']='foreign user work'
        self.adapter.cancel(task.task_id,1)
        with self.assertRaisesRegex(HostError,'HOST_409_TEXT_MISMATCH'):
            self.adapter._dispatch_cancel(lease)
        self.assertIsNone(self.adapter.attempt(task.task_id,1)['cancel_sent_at'])
        # Lost response before recording the run id must not weaken ownership.
        self.adapter._save(lease,run_id=None,phase='submit_requested')
        self.expire(lease)
        report = self.adapter.settle(task.task_id)
        self.assertFalse(report['settled'])
        self.assertEqual(report['reports'][0]['status'],'orphan_unknown')
        self.assertEqual(sum(p.endswith('/cancel') for p,_ in self.fixture.calls),0)
        self.assertEqual(session['status'],'running')


if __name__ == '__main__':
    unittest.main()
