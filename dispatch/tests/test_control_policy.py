"""Synthetic-only transactional budget, approval and handoff policy tests."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sqlite3
import sys
import tempfile
import time
import unittest

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.clock import FakeClock
from sa_dispatch.control_policy import ControlPolicy, before_claim
from sa_dispatch.models import DispatchError, TaskSpec
from sa_dispatch.store_sqlite import SqliteStore


class ControlPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory(); self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name)
        self.clock=FakeClock()
        self.store=SqliteStore(self.root/'ledger.sqlite3',clock=self.clock,lease_ttl_seconds=10)
        self.artifacts=ArtifactStore(self.root/'artifacts')
        self.policy=ControlPolicy(self.store,artifacts=self.artifacts)
        self.spec=TaskSpec('first','fixture.read',1,{},(),('cpu',),'synthetic','none',3)
        self.expiry='2026-01-02T00:00:00Z'

    def task(self,key):
        return self.store.submit(replace(self.spec,idem_key=key))[0]

    def proof(self,value):
        return self.artifacts.put(json.dumps({'synthetic':True,**value},sort_keys=True).encode())

    def budget(self,limit=5):
        self.policy.budget_upsert('daily',period='2026-01-01',unit='fixture_units',limit_units=limit,
                                 starts_at='2026-01-01T00:00:00Z',ends_at=self.expiry)

    def reserve(self,task_id):
        with self.store._tx() as conn:
            task=conn.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()
            return before_claim(conn,task,task['fence']+1,'fixture-worker',self.store._now())

    def test_concurrent_reservations_never_exceed_budget_and_repeat_is_free(self):
        self.budget()
        tasks=[self.task('one'),self.task('two')]
        for task in tasks: self.policy.bind_task(task.task_id,budget_id='daily',reserve_units=3)
        with ThreadPoolExecutor(2) as workers:
            outcomes=list(workers.map(lambda task:self.reserve(task.task_id),tasks))
        self.assertEqual(sorted(outcomes),[False,True])
        winner=tasks[outcomes.index(True)]
        self.assertTrue(self.reserve(winner.task_id))
        state=self.policy.status()
        self.assertEqual(state['budgets'][0]['reserved_units'],3)
        self.assertEqual(len(state['reservations']),1)
        self.assertEqual(sum(row['kind']=='budget.exceeded' for row in state['events']),1)

    def test_reservation_and_lease_transaction_rollback_together(self):
        self.budget(); task=self.task('rollback')
        self.policy.bind_task(task.task_id,budget_id='daily',reserve_units=3)
        with self.assertRaises(RuntimeError):
            with self.store._tx() as conn:
                row=conn.execute('SELECT * FROM tasks WHERE task_id=?',(task.task_id,)).fetchone()
                self.assertTrue(before_claim(conn,row,1,'worker',self.store._now()))
                raise RuntimeError('lease insert failed')
        state=self.policy.status()
        self.assertEqual(state['budgets'][0]['reserved_units'],0)
        self.assertEqual(state['reservations'],[])

    def test_unknown_holds_budget_until_bound_final_usage_receipt(self):
        self.budget(); first=self.task('first'); second=self.task('second')
        for task in (first,second): self.policy.bind_task(task.task_id,budget_id='daily',reserve_units=4)
        self.assertTrue(self.reserve(first.task_id))
        self.policy.mark_uncertain(first.task_id,1,actor='operator')
        self.assertFalse(self.reserve(second.task_id))
        bad=self.proof({'task_id':second.task_id,'fence':1,'actual_units':0,'final_usage':True})
        with self.assertRaises(DispatchError):
            self.policy.settle(first.task_id,1,actual_units=0,evidence_ref=bad,actor='operator')
        self.assertEqual(self.policy.status()['budgets'][0]['reserved_units'],4)
        proof=self.proof({'task_id':first.task_id,'fence':1,'budget_id':'daily',
                          'fingerprint':first.fingerprint,'actual_units':1,'final_usage':True})
        self.policy.settle(first.task_id,1,actual_units=1,evidence_ref=proof,actor='operator')
        self.policy.settle(first.task_id,1,actual_units=1,evidence_ref=proof,actor='operator')
        self.assertTrue(self.reserve(second.task_id))
        budget=self.policy.status()['budgets'][0]
        self.assertEqual((budget['reserved_units'],budget['used_units']),(4,1))

    def test_actual_overrun_is_accounted_and_blocks_future_claims(self):
        self.budget(); task=self.task('overrun')
        self.policy.bind_task(task.task_id,budget_id='daily',reserve_units=2); self.reserve(task.task_id)
        proof=self.proof({'task_id':task.task_id,'fence':1,'budget_id':'daily','fingerprint':task.fingerprint,
                          'actual_units':8,'final_usage':True})
        self.policy.settle(task.task_id,1,actual_units=8,evidence_ref=proof,actor='operator')
        next_task=self.task('next'); self.policy.bind_task(next_task.task_id,budget_id='daily',reserve_units=1)
        self.assertFalse(self.reserve(next_task.task_id))
        state=self.policy.status()
        self.assertEqual(state['budgets'][0]['used_units'],8)
        self.assertTrue(any(row['detail_code']=='actual_usage_overrun' for row in state['events']))

    def test_real_expired_lease_keeps_charge_and_retry_has_new_fence(self):
        self.budget(5); task=self.task('expired-attempt')
        self.policy.bind_task(task.task_id,budget_id='daily',reserve_units=4)
        self.store.register_device('worker',['cpu'])
        first=self.store.claim('worker')
        self.assertEqual(first.fence,1)
        self.clock.advance(11); self.store.sweep()
        self.assertIsNone(self.store.claim('worker'))
        self.assertEqual(self.policy.status()['budgets'][0]['reserved_units'],4)
        proof=self.proof({'task_id':task.task_id,'fence':1,'budget_id':'daily','fingerprint':task.fingerprint,
                          'actual_units':0,'final_usage':True})
        self.policy.settle(task.task_id,1,actual_units=0,evidence_ref=proof,actor='operator')
        second=self.store.claim('worker')
        self.assertEqual(second.fence,2)
        state=self.policy.status()
        self.assertEqual(len(state['reservations']),2)
        self.assertEqual(state['budgets'][0]['reserved_units'],4)
        self.evidence('expired-attempt-retains-budget',{'task_id':task.task_id,'first_fence':1,'retry_fence':2,'policy':state})

    def approve(self,task,decision='approved'):
        proof=self.proof({'task_id':task.task_id,'fingerprint':task.fingerprint,'scope':'fixture.execute',
                          'decision':decision,'actor':'reviewer','expires_at':self.expiry})
        return self.policy.approval_decide(task.task_id,scope='fixture.execute',decision=decision,
            actor='reviewer',evidence_ref=proof,expires_at=self.expiry)

    def test_sweep_uncertainty_is_atomic_idempotent_and_age_queryable(self):
        self.budget(); task=self.task('sweep-atomic')
        self.policy.bind_task(task.task_id,budget_id='daily',reserve_units=2)
        self.store.register_device('worker',['cpu']); self.store.claim('worker')
        self.clock.advance(11)
        with self.store._tx() as conn:
            conn.execute("""CREATE TRIGGER fail_uncertainty BEFORE INSERT ON control_events
                WHEN NEW.kind='budget.uncertain' BEGIN SELECT RAISE(ABORT,'injected'); END""")
        with self.assertRaises(sqlite3.IntegrityError): self.store.sweep()
        state=self.policy.status()
        self.assertEqual(state['reservations'][0]['state'],'reserved')
        with self.store._tx() as conn:
            self.assertEqual(conn.execute('SELECT status FROM tasks WHERE task_id=?',(task.task_id,)).fetchone()[0],'leased')
            conn.execute('DROP TRIGGER fail_uncertainty')
        self.store.sweep(); self.store.sweep()
        state=self.policy.status(uncertain_after_hours=1)
        self.assertEqual(state['reservations'][0]['state'],'uncertain')
        self.assertEqual(state['budgets'][0]['reserved_units'],2)
        self.assertEqual(sum(e['kind']=='budget.uncertain' for e in state['events']),1)
        self.assertEqual(state['uncertain_over_threshold'],[])
        self.clock.advance(3601)
        self.assertEqual(len(self.policy.status(uncertain_after_hours=1)['uncertain_over_threshold']),1)
        proof=self.proof({'task_id':task.task_id,'fence':1,'budget_id':'daily','fingerprint':task.fingerprint,
                          'actual_units':0,'final_usage':True})
        self.policy.settle(task.task_id,1,actual_units=0,evidence_ref=proof,actor='operator')
        self.assertEqual(self.policy.status(uncertain_after_hours=1)['uncertain_over_threshold'],[])

    def host_accounting_fixture(self,unit='host_run',observers=1):
        self.policy.budget_upsert('daily',period='2026-01-01',unit=unit,limit_units=5,
                                 starts_at='2026-01-01T00:00:00Z',ends_at=self.expiry)
        task=self.store.submit(replace(self.spec,kind='host.synthetic'))[0]
        self.policy.bind_task(task.task_id,budget_id='daily',reserve_units=1)
        self.store.register_device('worker',['cpu'])
        prompt=self.proof({'prompt':'fixture'})
        for fence in range(1,observers+2):
            claim=self.store.claim('worker'); self.assertEqual(claim.fence,fence)
            with self.store._tx() as conn:
                conn.execute('''INSERT INTO host_attempts(task_id,fence,endpoint,agent_id,prompt_ref,
                    title,request_id,session_id,run_id,phase,adopted_fence,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (task.task_id,fence,'http://127.0.0.1:12345','fixture',prompt,'fixture','request',
                     'session','run','completed',None if fence==1 else fence-1,self.store._now()))
            if fence<=observers: self.clock.advance(11); self.store.sweep()
        raw=self.artifacts.put(json.dumps({'type':'run.completed','status':'completed',
                    'seq':7,'run_id':'run','usage':{'tokens':99999}}).encode())
        wrapper=self.proof({'schema':'sa_dispatch.host_terminal.v1','task_id':task.task_id,
             'endpoint':'http://127.0.0.1:12345','session_id':'session','run_id':'run','event_ref':raw})
        return task,wrapper,raw

    def test_host_run_settlement_counts_origin_once_and_observer_zero(self):
        task,wrapper,raw=self.host_accounting_fixture()
        result=self.policy.settle_host_run(task.task_id,'run',wrapper,'operator')
        self.assertEqual([r['actual_units'] for r in result['settled']],[1,0])
        state=self.policy.status(); self.assertEqual(state['budgets'][0]['used_units'],1)
        self.assertEqual(state['budgets'][0]['reserved_units'],0)
        refs=[r['evidence_ref'] for r in state['reservations']]
        for ref in refs:
            proof=json.loads(self.artifacts.get(ref))
            self.assertEqual(proof['terminal_event_ref'],raw)
            self.assertEqual(proof['unit'],'host_run')
        again=self.policy.settle_host_run(task.task_id,'run',wrapper,'operator')
        self.assertEqual(len(again['already_settled']),2)
        self.assertEqual(self.policy.status()['budgets'][0]['used_units'],1)
        self.assertEqual([r['evidence_ref'] for r in self.policy.status()['reservations']],refs)

    def test_host_run_does_not_convert_currency_or_tokens(self):
        task,wrapper,raw=self.host_accounting_fixture(unit='tokens',observers=0)
        result=self.policy.settle_host_run(task.task_id,'run',wrapper,'operator')
        self.assertEqual(result['held'],[{'fence':1,'reason':'unit_not_host_run'}])
        state=self.policy.status()
        self.assertEqual(state['reservations'][0]['state'],'uncertain')
        self.assertEqual((state['budgets'][0]['used_units'],state['budgets'][0]['reserved_units']),(0,1))

    def test_host_terminal_binding_and_event_type_rejection_hold_budget(self):
        task,wrapper,raw=self.host_accounting_fixture(observers=0)
        original=json.loads(self.artifacts.get(wrapper))
        for changed in ({'session_id':'wrong-session'},{'run_id':'other-run'},
                        {'event_ref':self.artifacts.put(json.dumps({'type':'run.started',
                            'status':'running','run_id':'run','seq':1}).encode())}):
            bad=self.proof({**original,**changed})
            with self.assertRaises(DispatchError): self.policy.settle_host_run(task.task_id,'run',bad,'operator')
        state=self.policy.status()
        self.assertEqual((state['budgets'][0]['used_units'],state['budgets'][0]['reserved_units']),(0,1))

    def test_approval_binds_task_and_input_fingerprint_and_later_denial_wins(self):
        first=self.task('approval-one')
        second=self.store.submit(replace(self.spec,idem_key='approval-two',params={'revision':2}))[0]
        approval=self.approve(first)
        for task in (first,second):
            self.policy.bind_task(task.task_id,approval_scope='fixture.execute',approval_id=approval)
        self.assertTrue(self.reserve(first.task_id))
        self.assertFalse(self.reserve(second.task_id))
        self.approve(first,'denied')
        self.assertFalse(self.reserve(first.task_id))
        self.policy.bind_task(first.task_id,approval_scope='fixture.execute',approval_id=approval)
        self.assertFalse(self.reserve(first.task_id))

    def test_expired_approval_and_budget_deny_but_unmanaged_tasks_keep_compatibility(self):
        self.budget(); task=self.task('expiry')
        approval=self.approve(task)
        self.policy.bind_task(task.task_id,budget_id='daily',reserve_units=1,
                             approval_scope='fixture.execute',approval_id=approval)
        self.clock.advance(86400)
        self.assertFalse(self.reserve(task.task_id))
        self.assertTrue(self.reserve(self.task('unmanaged').task_id))
        self.assertEqual(self.policy.status()['reservations'],[])

    def offer(self,ident,scope='read_status',auth=None):
        self.policy.handoff_offer(ident,from_conv='sender',to_conv='recipient',project_id='fixture-project',
            scope=scope,expires_at=self.expiry,actor='sender',authorization_ref=auth)

    def test_instruction_and_takeover_require_exact_authorization(self):
        for scope in ('instruct','takeover'):
            with self.assertRaises(DispatchError): self.offer(scope,scope)
        auth=self.proof({'handoff_id':'instruction','from_conv':'sender','to_conv':'recipient','project_id':'fixture-project',
            'scope':'instruct','scope_refs':[],'expires_at':self.expiry,'authorized_by':'fixture-user'})
        self.offer('instruction','instruct',auth)
        with self.assertRaises(DispatchError): self.offer('widened','takeover',auth)
        self.assertEqual(len(self.policy.status()['handoffs']),1)

    def test_handoff_recipient_receipt_and_output_evidence_required(self):
        self.offer('one')
        with self.assertRaises(DispatchError): self.policy.handoff_transition('one','accepted',actor='sender')
        self.policy.handoff_transition('one','accepted',actor='recipient')
        with self.assertRaises(DispatchError): self.policy.handoff_transition('one','completed',actor='recipient')
        output=self.artifacts.put(b'synthetic completed work')
        receipt=self.proof({'handoff_id':'one','recipient':'recipient','decision':'completed','evidence_refs':[output]})
        self.policy.handoff_transition('one','completed',actor='recipient',receipt_ref=receipt)
        self.policy.handoff_transition('one','completed',actor='recipient',receipt_ref=receipt)
        state=self.policy.status()
        self.assertEqual(state['handoffs'][0]['state'],'completed')
        self.assertEqual(sum(row['kind']=='handoff.completed' for row in state['events']),1)

    def test_expiry_is_committed_even_when_acceptance_is_rejected(self):
        self.offer('expired')
        self.clock.advance(86400)
        with self.assertRaises(DispatchError):
            self.policy.handoff_transition('expired','accepted',actor='recipient')
        self.assertEqual(self.policy.status()['handoffs'][0]['state'],'expired')

    def evidence(self,name,data):
        destination=os.environ.get('SA_CONTROL_POLICY_EVIDENCE_DIR')
        if destination:
            path=Path(destination); path.mkdir(parents=True,exist_ok=True)
            with (path/(name+'.json')).open('x',encoding='utf-8') as stream:
                json.dump({'schema':'sa-dispatch.control-policy-lab.v1','synthetic_only':True,**data},stream,indent=2)

    def test_real_claim_rolls_back_budget_when_lease_transaction_fails(self):
        self.budget(); task=self.task('real-rollback')
        self.policy.bind_task(task.task_id,budget_id='daily',reserve_units=3)
        self.store.register_device('worker',['cpu'])
        with self.store._tx() as conn:
            conn.execute("""CREATE TRIGGER reject_fixture_lease BEFORE INSERT ON leases
                BEGIN SELECT RAISE(ABORT,'fixture lease insert rejected'); END""")
        with self.assertRaises(sqlite3.IntegrityError): self.store.claim('worker')
        state=self.policy.status()
        self.assertEqual(state['budgets'][0]['reserved_units'],0)
        self.assertEqual(state['reservations'],[])
        self.assertEqual(self.store.get_task(task.task_id).fence,0)
        self.assertEqual(self.store.list_leases(),[])
        self.evidence('claim-rollback',{'reservations':0,'held_units':0,'leases':0,'task_fence':0})

    def test_two_real_process_claims_compete_without_overspending(self):
        self.budget(3)
        for key in ('race-one','race-two'):
            task=self.task(key)
            self.policy.bind_task(task.task_id,budget_id='daily',reserve_units=3)
        self.store.register_device('worker',['cpu'])
        gate=self.root/'gate'; ready=[self.root/('ready'+str(index)) for index in range(2)]
        script='''import sys,json,time
from dataclasses import asdict
from pathlib import Path
from sa_dispatch.clock import FakeClock
from sa_dispatch.store_sqlite import SqliteStore
store=SqliteStore(sys.argv[1],clock=FakeClock(),lease_ttl_seconds=10)
Path(sys.argv[3]).touch()
while not Path(sys.argv[2]).exists(): time.sleep(.01)
lease=store.claim('worker')
print(json.dumps(asdict(lease) if lease else None))
'''
        processes=[subprocess.Popen([sys.executable,'-B','-c',script,str(self.store.path),str(gate),str(marker)],
                    cwd=Path(__file__).resolve().parents[1],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for marker in ready]
        def stop(process):
            if process.poll() is None: process.kill()
            process.wait(timeout=10)
        for process in processes: self.addCleanup(stop,process)
        deadline=time.monotonic()+10
        while not all(path.exists() for path in ready) and time.monotonic()<deadline: time.sleep(.01)
        self.assertTrue(all(path.exists() for path in ready)); gate.touch()
        leases=[]
        for process in processes:
            out,error=process.communicate(timeout=10)
            self.assertEqual(process.returncode,0,error)
            leases.append(json.loads(out))
        self.assertEqual(sum(lease is not None for lease in leases),1)
        state=self.policy.status()
        self.assertEqual(len(state['reservations']),1)
        self.assertEqual(state['budgets'][0]['reserved_units'],3)
        self.assertTrue(any(row['kind']=='budget.exceeded' for row in state['events']))
        self.evidence('two-process-claims',{'leases':leases,'policy':state})


if __name__=='__main__': unittest.main()
