import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from pathlib import Path

from sa_dispatch.clock import FakeClock
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import sha256_ref
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.models import (TaskSpec, Outcome, Evidence, FenceRejected,
                                IdempotencyConflict, DuplicateInflight, DispatchError, InvalidTransition)
from sa_dispatch.states import TRANSITIONS, validate_transition
from sa_dispatch.store_sqlite import SqliteStore

class StoreContract(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.clock=FakeClock()
        self.store=SqliteStore(Path(self.temp.name)/'ledger.db',clock=self.clock,lease_ttl_seconds=10)
        self.store.register_device('d',{'cpu'})
        self.spec=TaskSpec('key','test.read',1,{},(),('cpu',),'synthetic','none',2)

    def test_idempotency_and_authorization(self):
        row,hit=self.store.submit(self.spec)
        self.assertFalse(hit)
        self.assertTrue(self.store.submit(self.spec)[1])
        for field,value in [('access','local_only'),('egress','summary'),('track','other'),('max_attempts',3),('required_caps',('gpu',))]:
            with self.subTest(field=field),self.assertRaises(IdempotencyConflict):
                self.store.submit(replace(self.spec,**{field:value}))
        other,_=self.store.submit(replace(self.spec,idem_key='other'))
        self.assertNotEqual(row.task_id,other.task_id)

    def test_write_duplicate_atomic(self):
        def submit(key):
            try:
                self.store.submit(replace(self.spec,idem_key=key),side_effect='idempotent_write',scope={'target':'one'})
                return True
            except DuplicateInflight: return False
        with ThreadPoolExecutor(2) as pool:
            self.assertEqual(sorted(pool.map(submit,['a','b'])),[False,True])

    def test_claim_atomic(self):
        self.store.submit(self.spec)
        with ThreadPoolExecutor(2) as pool:
            leases=list(pool.map(lambda _:self.store.claim('d'),range(2)))
        self.assertEqual(sum(v is not None for v in leases),1)

    def test_expiry_fence_device_and_replay(self):
        row,_=self.store.submit(self.spec)
        lease=self.store.claim('d')
        with self.assertRaises(FenceRejected): self.store.renew(lease.lease_id,lease.fence,'other')
        self.clock.advance(10)
        with self.assertRaises(FenceRejected): self.store.complete(lease.lease_id,lease.fence,Outcome(),'d')
        self.store.sweep()
        new=self.store.claim('d')
        self.assertEqual(new.fence,2)
        with self.assertRaises(FenceRejected): self.store.complete(lease.lease_id,lease.fence,Outcome(),'d')
        outcome=Outcome(result={'value':1})
        self.store.complete(new.lease_id,new.fence,outcome,'d')
        events=len(self.store.events(row.task_id))
        self.store.complete(new.lease_id,new.fence,outcome,'d')
        self.assertEqual(len(self.store.events(row.task_id)),events)
        with self.assertRaises(FenceRejected): self.store.complete(new.lease_id,new.fence,Outcome(result={'value':2}),'d')
        self.assertEqual(self.store.get_task(row.task_id).result,{'value':1})
        self.assertEqual(len(self.store.late_results(row.task_id)),3)

    def test_unknown_external_never_blind_retries(self):
        row,_=self.store.submit(self.spec,side_effect='external')
        self.store.claim('d'); self.clock.advance(11); self.store.sweep()
        self.assertEqual(self.store.get_task(row.task_id).status,'unknown')
        self.assertIsNone(self.store.claim('d'))
        with self.assertRaises(DispatchError): self.store.resolve_unknown(row.task_id,'not_done','system')
        artifacts = ArtifactStore(Path(self.temp.name)/'artifacts')
        data_ref = artifacts.put(b'{"synthetic":true}')
        outcome = Outcome(evidence=(Evidence('replay_check',data_ref,'synthetic',artifact_hash=data_ref),))
        proof = artifacts.put(receipt_bytes({'schema':'sa_dispatch.reconciliation.v1',
            'task_id':row.task_id,'fence':1,'idem_key':row.idem_key,'decision':'succeeded',
            'outcome_hash':sha256_ref(receipt_bytes(asdict(outcome)))}))
        self.store.resolve_unknown(row.task_id,'confirmed_done','reviewer',outcome=outcome,
            manual=True,expected_fence=1,evidence_ref=proof,artifacts=artifacts)
        self.assertEqual(self.store.get_task(row.task_id).status,'succeeded')
        with self.assertRaises(InvalidTransition): self.store.resolve_unknown(row.task_id,'not_done','person',manual=True)

    def test_waiting_and_limits(self):
        row,_=self.store.submit(replace(self.spec,required_caps=('gpu',),max_attempts=1))
        self.store.sweep()
        self.assertEqual(self.store.get_task(row.task_id).status,'waiting_device')
        self.store.register_device('gpu',{'gpu'})
        self.store.claim('gpu');self.clock.advance(11);self.store.sweep()
        self.assertEqual(self.store.get_task(row.task_id).status,'failed')

    def test_all_forbidden_state_pairs(self):
        for old in TRANSITIONS:
            for new in TRANSITIONS:
                if new not in TRANSITIONS[old]:
                    with self.subTest(old=old,new=new),self.assertRaises(InvalidTransition): validate_transition(old,new)

    def test_snapshot_atomic_import(self):
        row,_=self.store.submit(self.spec);lease=self.store.claim('d')
        self.store.complete(lease.lease_id,lease.fence,Outcome(),'d')
        target=SqliteStore(Path(self.temp.name)/'target.db',lease_ttl_seconds=10)
        target.import_snapshot(self.store.snapshot())
        self.assertEqual(target.get_task(row.task_id).spec,self.spec)
        with self.assertRaises(DispatchError): target.import_snapshot(self.store.snapshot())

    def test_no_new_rejected_for_unregistered_kind(self):
        self.store.submit(self.spec);lease=self.store.claim('d')
        with self.assertRaises(DispatchError): self.store.complete(lease.lease_id,lease.fence,Outcome('no_new'),'d')

if __name__=='__main__': unittest.main()
