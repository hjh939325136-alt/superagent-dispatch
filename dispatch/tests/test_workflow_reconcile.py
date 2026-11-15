"""Read-only plugin lookup proves historical commit, not current use or retry permission."""
import copy,json,unittest
from types import SimpleNamespace
import test_reconciliation as legacy
from sa_dispatch.executor import Executor,receipt_bytes
from sa_dispatch.canonical import sha256_ref
from sa_dispatch_hotspot import KINDS

class WorkflowLookupTests(unittest.TestCase):
    setUp=legacy.ReconciliationTests.setUp
    unknown=legacy.ReconciliationTests.unknown

    def prepare(self,change=None,request_change=None,validator=None):
        task,_=self.unknown();base=KINDS[task.kind]
        class Kind:
            enabled=True
            component=base.component
            version=base.version
            side_effect=base.side_effect
            request=staticmethod(base.request)
            result=staticmethod(base.result)
            def lookup_request(self,t,original,h):
                value={**original,'operation':'operation.lookup','request_id':'reconcile-'+t.task_id,
                    'idempotency_key':None,'payload':{'task_id':t.task_id,'fence':t.fence,
                    'operation':original['operation'],'write_key':original['idempotency_key'],'request_hash':h}}
                if request_change:request_change(value)
                return value
        kind=Kind()
        if validator is not None:kind.validate_lookup=validator
        original=kind.request(task)
        response={'schema_version':'0.1','ok':True,'request_id':task.task_id,
            'data':{'content':{'ref':{'owner':'fixture','kind':'topic','id':'one','version':'1'}}}}
        data={'state':'committed','task_id':task.task_id,'fence':task.fence,'operation':original['operation'],
            'write_key':original['idempotency_key'],'request_hash':sha256_ref(receipt_bytes(original)),
            'response':response,'response_hash':sha256_ref(receipt_bytes(response)),'current_usable':False}
        if change:change(data)
        self.calls=[]
        def run(component,request):
            self.calls.append(copy.deepcopy(request))
            return SimpleNamespace(exit=0,json={'ok':True,'request_id':request['request_id'],'data':data})
        executor=Executor(self.config,self.store,SimpleNamespace(run_component=run),{task.kind:kind})
        return task,executor

    def test_commit_resolves_only_history_and_cas_binds_all_fields(self):
        task,executor=self.prepare();answer=executor.reconcile(task)
        self.assertEqual(answer['verdict'],'confirmed_done');self.assertFalse(answer['current_usable'])
        stored=self.store.get_task(task.task_id);self.assertEqual(stored.status,'succeeded')
        history=self.store.reconciliation_history(task.task_id)
        proof=json.loads(self.artifacts.get(history[0]['evidence_ref']))
        for key in ('task_id','fence','write_key','request_hash','outcome_hash','lookup_ref'):self.assertIn(key,proof)
        self.assertEqual(proof['task_id'],task.task_id);self.assertEqual(proof['fence'],task.fence)
        envelope=json.loads(self.artifacts.get(proof['lookup_ref']))
        self.assertEqual(envelope['data']['request_hash'],proof['request_hash'])
        self.assertEqual(len(self.calls),1);self.assertIsNone(self.calls[0]['idempotency_key'])
        self.assertEqual(executor.reconcile(stored),{'verdict':'still_unknown'})

    def test_absent_conflict_wrong_bindings_or_hash_never_replay(self):
        # Each subcase has an independent task/ledger to prove no state transition.
        for field,value in [('state','absent'),('state','conflict'),('task_id','other'),('fence',99),
                ('write_key','other'),('request_hash','sha256:'+'0'*64),('operation','other'),
                ('response_hash','sha256:'+'0'*64),('current_usable','yes')]:
            with self.subTest(field=field,value=value):
                self.setUp();task,executor=self.prepare(lambda d:d.update({field:value}))
                self.assertEqual(executor.reconcile(task),{'verdict':'still_unknown'})
                self.assertEqual(self.store.get_task(task.task_id).status,'unknown')
                self.assertEqual(self.store.reconciliation_history(task.task_id),[])
                self.assertEqual(len(self.calls),1)

    def test_write_request_extension_rejected_before_runner(self):
        for field,value in [('operation','content.put'),('idempotency_key','write-again')]:
            with self.subTest(field=field):
                self.setUp();task,executor=self.prepare(request_change=lambda r:r.update({field:value}))
                self.assertEqual(executor.reconcile(task),{'verdict':'still_unknown'})
                self.assertEqual(self.calls,[])
                self.assertEqual(self.store.get_task(task.task_id).status,'unknown')

    def test_wrong_historical_request_and_stale_task_cannot_settle(self):
        def change(data):
            data['response']['request_id']='other-task'
            data['response_hash']=sha256_ref(receipt_bytes(data['response']))
        task,executor=self.prepare(change)
        self.assertEqual(executor.reconcile(task),{'verdict':'still_unknown'})
        self.assertEqual(self.store.get_task(task.task_id).status,'unknown')
        # A trusted operator may finish an unknown task meanwhile; stale evidence cannot overwrite it.
        task,executor=self.prepare()
        proof=self.artifacts.put(receipt_bytes({'schema':'sa_dispatch.reconciliation.v1',
            'task_id':task.task_id,'fence':task.fence,'decision':'failed'}))
        self.store.resolve_unknown(task.task_id,'confirmed_failed','operator',manual=True,evidence_ref=proof,
            expected_fence=task.fence,artifacts=self.artifacts)
        self.assertEqual(executor.reconcile(task),{'verdict':'still_unknown'})
        self.assertEqual(self.store.get_task(task.task_id).status,'failed')

    def test_instance_extensions_require_validator_and_exact_echo(self):
        fields={'binding_hash':'sha256:'+'a'*64,'catalog_instance':'catalog-one','content_instance':'content-one'}
        extend=lambda r:r['payload'].update(fields)
        for mode in ('valid','missing-validator','wrong-instance','validator-reject'):
            with self.subTest(mode=mode):
                self.setUp()
                def change(data):
                    data.update(fields)
                    if mode=='wrong-instance':data['content_instance']='other'
                validator=None if mode=='missing-validator' else lambda t,o,d:mode!='validator-reject'
                task,executor=self.prepare(change,extend,validator)
                answer=executor.reconcile(task)
                self.assertEqual(answer['verdict'],'confirmed_done' if mode=='valid' else 'still_unknown')
                self.assertEqual(self.store.get_task(task.task_id).status,'succeeded' if mode=='valid' else 'unknown')
                if mode=='missing-validator':self.assertEqual(self.calls,[])

    def test_actual_invocation_has_no_attempt_identity(self):
        task,_=self.prepare()
        kind=KINDS[task.kind]
        original=kind.request(task)
        newer=type(task)({**task,'fence':task.fence+1})
        self.assertEqual(original['request_id'],task.task_id)
        self.assertEqual(kind.request(newer),original)
        self.assertEqual(sha256_ref(receipt_bytes(kind.request(newer))),sha256_ref(receipt_bytes(original)))

    def test_context_request_is_journaled_and_not_rebuilt_for_lookup(self):
        base=KINDS[self.spec.kind];seen=[]
        def hook(task,context):
            seen.append(context)
            request=base.request(task)
            request['payload']={**request['payload'],'fixed_approval':'metadata'}
            return request
        kind=SimpleNamespace(component=base.component,side_effect=base.side_effect,request=base.request,
            request_with_context=hook,result=base.result)
        def lost(component,request):raise RuntimeError('response lost')
        executor=Executor(self.config,self.store,SimpleNamespace(run_component=lost),{self.spec.kind:kind})
        task,_=self.store.submit(self.spec);result=executor.run_once()
        self.assertEqual(result.status,'unknown')
        self.assertEqual(seen,[{'task_fingerprint':task.fingerprint,'control_approval':None}])
        kind.request_with_context=lambda *args:(_ for _ in ()).throw(AssertionError('must not reconstruct'))
        original=executor._original_request(result)
        self.assertEqual(original['payload']['fixed_approval'],'metadata')
        self.assertEqual(original['request_id'],task.task_id)

    def test_preinvocation_and_verified_precommit_fail_without_unknown(self):
        from sa_dispatch.models import Outcome
        for phase in ('request','component'):
            with self.subTest(phase=phase):
                self.setUp();base=KINDS[self.spec.kind];calls=[]
                def hook(task,context):
                    if phase=='request':raise ValueError('missing control approval')
                    return base.request(task)
                def run(component,request):
                    calls.append(request)
                    return SimpleNamespace(exit=2,json={'request_id':request['request_id'],'ok':False,'precommit':True})
                kind=SimpleNamespace(component=base.component,side_effect=base.side_effect,request=base.request,
                    request_with_context=hook,result=base.result,
                    rejection_outcome=lambda response,task:Outcome('failed',{'code':'DEPENDENCY_REVOKED'}) if response=={'request_id':task.task_id,'ok':False,'precommit':True} else None)
                executor=Executor(self.config,self.store,SimpleNamespace(run_component=run),{self.spec.kind:kind})
                self.store.submit(self.spec);result=executor.run_once()
                self.assertEqual(result.status,'failed')
                self.assertEqual(len(calls),0 if phase=='request' else 1)
                if phase=='component':self.artifacts.get(result.result['receipt'])

    def test_context_contains_actual_task_bound_approval_and_readable_proof(self):
        from datetime import timedelta
        from sa_dispatch.clock import timestamp
        from sa_dispatch.control_policy import ControlPolicy
        task,_=self.store.submit(self.spec)
        policy=ControlPolicy(self.store,artifacts=self.artifacts)
        policy.bind_task(task.task_id,approval_scope='script_ready')
        expiry=timestamp(self.clock.now()+timedelta(hours=1))
        proof={'task_id':task.task_id,'fingerprint':task.fingerprint,'scope':'script_ready',
            'actor':'synthetic-operator','decision':'approved','expires_at':expiry,
            'synthetic':True,'human_reviewed':False,'publish_authorized':False}
        ref=self.artifacts.put(receipt_bytes(proof))
        approval_id=policy.approval_decide(task.task_id,scope='script_ready',decision='approved',
            actor='synthetic-operator',evidence_ref=ref,expires_at=expiry)
        base=KINDS[self.spec.kind];seen=[]
        def hook(t,c):seen.append(c);return base.request(t)
        kind=SimpleNamespace(request_with_context=hook)
        executor=Executor(self.config,self.store,None,{self.spec.kind:kind})
        before=self.config.db_path.read_bytes();executor._request(task)
        self.assertEqual(before,self.config.db_path.read_bytes())
        approval=seen[0]['control_approval']
        self.assertEqual(approval['approval_id'],approval_id)
        self.assertEqual(approval['proof_ref'],ref)
        self.assertEqual(approval['proof'],proof)

if __name__=='__main__':unittest.main()

