import json
from pathlib import Path
import tempfile
import unittest
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import canonical_json
from sa_dispatch.clock import FakeClock,timestamp
from sa_dispatch.control_policy import ControlPolicy
from sa_dispatch.models import TaskSpec
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.unified_events import UnifiedEventStore


class ControlProjectionTests(unittest.TestCase):
    def test_real_budget_denial_and_handoff_expiry_are_visible_and_deduplicated(self):
        with tempfile.TemporaryDirectory() as temp:
            clock=FakeClock();store=SqliteStore(Path(temp)/'ledger.db',clock=clock,lease_ttl_seconds=10)
            cas=ArtifactStore(Path(temp)/'cas');policy=ControlPolicy(store,artifacts=cas)
            policy.budget_upsert('empty',period='day1',unit='fixture',limit_units=0,
                                 starts_at=timestamp(clock.now()),ends_at='2027-01-01T00:00:00Z')
            task,_=store.submit(TaskSpec('k','lab.read',1,{},(),('cpu',),'synthetic','none',2))
            policy.bind_task(task.task_id,budget_id='empty',reserve_units=1)
            store.register_device('test',['cpu']);self.assertIsNone(store.claim('test'))
            events=UnifiedEventStore(store.path,clock=clock)
            first=events.project_task(store,task.task_id)
            self.assertIn('budget.exceeded',[e['kind'] for e in first])
            self.assertEqual(first,events.project_task(store,task.task_id))
            policy.handoff_offer('h1',from_conv='sender',to_conv='receiver',project_id='lab',
                scope='read_status',expires_at='2026-01-01T00:01:00Z',actor='sender')
            clock.advance(61);policy.status()
            rows=events.project_control(store,handoff_id='h1')
            self.assertEqual([e['kind'] for e in rows],['handoff.offered','handoff.expired'])
            self.assertEqual(rows,events.project_control(store,handoff_id='h1'))


if __name__=='__main__':unittest.main()
