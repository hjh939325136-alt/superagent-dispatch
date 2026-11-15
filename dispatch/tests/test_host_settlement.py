from dataclasses import replace
import json
import subprocess
import sys
import time
import unittest
import test_host_adapter as fixture
from sa_dispatch.bundle import export_bundle,import_bundle,read_bundle
from sa_dispatch.config import Config
from sa_dispatch.control_policy import ControlPolicy
from datetime import datetime,timedelta,timezone
from sa_dispatch.host_adapter import HostError
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.unified_events import UnifiedEventStore


class HostSettlementTests(unittest.TestCase):
    setUp=fixture.HostTests.setUp
    task=fixture.HostTests.task
    def config(self):
        return Config(self.store.path,self.artifacts.root,self.root/'journal','host',('host.fixture',),10,3,(),{},True)
    def test_other_process_cancel_intent_interrupts_active_observer_once(self):
        task,lease=self.task('wait')
        cp=subprocess.Popen([sys.executable,'-B','-m','sa_dispatch.host_adapter','--ledger',str(self.store.path),
            '--artifacts',str(self.artifacts.root),'--lease-id',lease.lease_id,'--base-url',self.url,
            '--isolated-synthetic','--timeout','15'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            deadline=time.monotonic()+6
            while time.monotonic()<deadline:
                row=self.adapter.attempt(task.task_id,1)
                if row and row['last_event_seq']>=3:break
                time.sleep(.03)
            else:self.fail('stream not reached')
            self.assertTrue(self.adapter.cancel(task.task_id,1)['cancel_requested'])
            out,err=cp.communicate(timeout=6);self.assertEqual(cp.returncode,0,out+err)
            self.assertEqual(json.loads(out)['status'],'cancelled')
            self.assertEqual(sum(p.endswith('/cancel') for p,_ in self.fixture.calls),1)
            self.assertEqual(self.store.get_task(task.task_id).status,'failed')
        finally:
            if cp.poll() is None:cp.kill();cp.communicate(timeout=3)
    def test_exhausted_task_orphan_is_cancelled_then_bundle_roundtrips(self):
        task,lease=self.task('wait',max_attempts=1)
        with self.assertRaisesRegex(HostError,'HOST_OBSERVATION_TIMEOUT'):self.adapter.run(lease,timeout_s=.03)
        with self.store._tx() as c:c.execute("UPDATE leases SET expires_at='2000-01-01' WHERE lease_id=?",(lease.lease_id,))
        self.store.sweep();self.assertEqual(self.store.get_task(task.task_id).status,'failed')
        report=self.adapter.settle(task.task_id);self.assertTrue(report['settled'])
        self.assertEqual(report['reports'][0]['status'],'orphan_cancelled')
        self.assertFalse(report['reports'][0]['host_running'])
        self.assertEqual(self.adapter.settle(task.task_id),report)
        projection=UnifiedEventStore(self.store.path)
        trace=projection.project_task(self.store,task.task_id)
        self.assertIn('host.observed',[e['kind'] for e in trace])
        self.assertEqual(trace,projection.project_task(self.store,task.task_id))
        cfg=self.config();package=self.root/'closed';export_bundle(cfg,package)
        dest=replace(cfg,db_path=self.root/'restored.db',artifact_root=self.root/'restored-cas')
        target=SqliteStore(dest.db_path,lease_ttl_seconds=10);import_bundle(dest,package,target)
        self.assertEqual(target.snapshot()['host_attempts'],self.store.snapshot()['host_attempts'])
        self.assertEqual(sum(p.endswith('/cancel') for p,_ in self.fixture.calls),1)
    def test_cancelled_before_attempt_keeps_prompt_and_projection_refs(self):
        # The projected blob appears nowhere in evidence or host tables.
        task,_=self.task(claim=False);self.store.cancel(task.task_id,'operator')
        extra=self.artifacts.put(b'synthetic projection-only evidence')
        events=UnifiedEventStore(self.store.path)
        events.append(source='fixture',source_id='one',trace_id=task.task_id,actor='test',component='test',
            kind='task.cancelled',task_id=task.task_id,refs=[f'dispatch/evidence/one@1#{extra}'])
        manifest=export_bundle(self.config(),self.root/'reference-bundle')
        self.assertIn(task.params['prompt_ref'],manifest['artifacts']);self.assertIn(extra,manifest['artifacts'])
        read_bundle(self.root/'reference-bundle')

    def test_host_run_budget_adoption_settles_origin_once_and_observer_zero(self):
        task,_=self.task('wait',claim=False)
        policy=ControlPolicy(self.store,artifacts=self.artifacts)
        now=datetime.now(timezone.utc)
        policy.budget_upsert('hosts',period='one',unit='host_run',limit_units=3,
            starts_at=(now-timedelta(minutes=1)).isoformat(),ends_at=(now+timedelta(hours=1)).isoformat())
        policy.bind_task(task.task_id,budget_id='hosts',reserve_units=1)
        lease=self.store.claim('host')
        with self.assertRaisesRegex(HostError,'HOST_OBSERVATION_TIMEOUT'):self.adapter.run(lease,timeout_s=.03)
        with self.store._tx() as c:c.execute("UPDATE leases SET expires_at='2000-01-01' WHERE lease_id=?",(lease.lease_id,))
        self.store.sweep();status=policy.status()
        self.assertEqual(status['reservations'][0]['state'],'uncertain')
        self.assertEqual(status['budgets'][0]['reserved_units'],1)
        new=self.store.claim('host');row=self.adapter.attempt(task.task_id,1)
        self.fixture.finish(row['session_id']);self.adapter.run(new)
        status=policy.status();reservations=sorted(status['reservations'],key=lambda r:r['fence'])
        self.assertEqual([r['actual_units'] for r in reservations],[1,0])
        self.assertEqual(status['budgets'][0]['used_units'],1)
        self.assertEqual(status['budgets'][0]['reserved_units'],0)
        manifest=export_bundle(self.config(),self.root/'usage-bundle')
        for reservation in reservations:
            proof=json.loads(self.artifacts.get(reservation['evidence_ref']))
            self.assertIn(proof['terminal_event_ref'],manifest['artifacts'])
        read_bundle(self.root/'usage-bundle')


if __name__=='__main__':unittest.main()
