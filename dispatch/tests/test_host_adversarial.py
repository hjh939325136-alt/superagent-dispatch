"""Independent adversarial review: fence ownership and cross-fence recovery."""
import unittest
from unittest.mock import patch

import test_host_adapter as host_fixture
from sa_dispatch.host_adapter import HostError


class HostAdversarialTests(unittest.TestCase):
    setUp = host_fixture.HostTests.setUp
    task = host_fixture.HostTests.task

    def expire_and_reclaim(self, lease):
        with self.store._tx() as conn:
            conn.execute("UPDATE leases SET expires_at='2000-01-01' WHERE lease_id=?", (lease.lease_id,))
        self.store.sweep()
        new = self.store.claim('host')
        self.assertIsNotNone(new)
        self.assertEqual(new.fence, lease.fence + 1)
        return new

    def test_stale_fence_cannot_cancel_run_adopted_by_new_attempt(self):
        task, lease = self.task('wait')
        with self.assertRaisesRegex(HostError,'HOST_OBSERVATION_TIMEOUT'):
            self.adapter.run(lease,timeout_s=.03)
        new = self.expire_and_reclaim(lease)
        with self.assertRaisesRegex(HostError,'HOST_OBSERVATION_TIMEOUT'):
            self.adapter.run(new,timeout_s=.03)
        row = self.adapter.attempt(task.task_id,new.fence)
        self.assertEqual(row['adopted_fence'],lease.fence)
        before = sum(path.endswith('/cancel') for path, _ in self.fixture.calls)
        with self.assertRaises(HostError):
            self.adapter.cancel(task.task_id,lease.fence)
        self.assertEqual(sum(path.endswith('/cancel') for path, _ in self.fixture.calls),before)
        self.assertEqual(self.fixture.sessions[row['session_id']]['requests'][0]['status'],'running')

    def test_lost_create_response_recovers_owned_session_after_lease_expiry(self):
        task, lease = self.task()
        self.fixture.lose_create=True
        with self.assertRaises((HostError,OSError)):
            self.adapter.run(lease)
        self.assertEqual(len(self.fixture.sessions),1)
        self.assertIsNone(self.adapter.attempt(task.task_id,lease.fence)['session_id'])
        new = self.expire_and_reclaim(lease)
        result = self.adapter.run(new)
        self.assertEqual(result['status'],'completed')
        self.assertEqual(len(self.fixture.sessions),1)
        self.assertEqual(len(next(iter(self.fixture.sessions.values()))['requests']),1)

    def test_saved_session_before_message_recovers_after_lease_expiry(self):
        task, lease = self.task()
        real_json = self.client.json
        def crash_before_post(path,body=None):
            if path.endswith('/messages'):
                raise RuntimeError('adapter died before message POST')
            return real_json(path,body)
        with patch.object(self.client,'json',side_effect=crash_before_post):
            with self.assertRaises(RuntimeError):
                self.adapter.run(lease)
        row = self.adapter.attempt(task.task_id,lease.fence)
        self.assertIsNotNone(row['session_id'])
        self.assertEqual(self.fixture.sessions[row['session_id']]['requests'],[])
        new = self.expire_and_reclaim(lease)
        result = self.adapter.run(new)
        self.assertEqual(result['status'],'completed')
        self.assertEqual(len(self.fixture.sessions),1)
        self.assertEqual(len(self.fixture.sessions[row['session_id']]['requests']),1)


if __name__=='__main__': unittest.main()
