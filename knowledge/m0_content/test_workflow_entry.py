"""Independent workflow entry authority tests; synthetic metadata only."""
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
import copy,unittest
from knowledge.m0_content.dispatch_entry import WorkflowKind
from knowledge.m0_content.workflow_binding import WorkflowError
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.canonical import sha256_ref

class WorkflowEntryTests(unittest.TestCase):
    def setUp(self):
        self.binding=SimpleNamespace(digest='sha256:'+'a'*64,research=SimpleNamespace(instance='catalog-one'),
            content_instance='content-one',allowed_traces=('synthetic-content-game',),template_id='synthetic-script-v1')
        params={'workflow_binding':self.binding.digest,'trace_id':'synthetic-content-game',
            'context_id':'ctx_'+'a'*26,'manifest_sha256':'b'*64,'script_ref':'mgmt/script/one@1#'+'c'*64}
        self.task=SimpleNamespace(task_id='t_one',fingerprint='fixed-input',fence=1,
            spec=SimpleNamespace(access='synthetic',egress='none',max_attempts=1,params=params))
        self.kind=WorkflowKind('content.approval_record',self.binding)
        expiry=(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()
        proof={'task_id':'t_one','fingerprint':'fixed-input','scope':'script_ready','decision':'approved',
            'actor':'synthetic-operator','expires_at':expiry,'synthetic':True,'human_reviewed':False,'publish_authorized':False}
        ref=sha256_ref(receipt_bytes(proof))
        self.context={'task_fingerprint':'fixed-input','control_approval':{**proof,'approval_id':'ap_one',
            'evidence_ref':ref,'proof_ref':ref,'proof':proof}}

    def test_expired_control_row_cannot_borrow_longer_proof(self):
        self.context['control_approval']['expires_at']=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
        with self.assertRaises(WorkflowError):self.kind.request_with_context(self.task,self.context)

    def test_proof_mutation_or_other_task_rejected(self):
        for field,value in [('task_id','t_other'),('scope','publish'),('actor','user'),('synthetic',False)]:
            with self.subTest(field=field):
                context=copy.deepcopy(self.context);context['control_approval']['proof'][field]=value
                with self.assertRaises(WorkflowError):self.kind.request_with_context(self.task,context)

    def test_instance_lookup_exact_and_request_is_fence_independent(self):
        first=self.kind.request_with_context(self.task,self.context)
        self.task.fence=2
        second=self.kind.request_with_context(self.task,self.context)
        self.assertEqual(first,second)
        lookup=self.kind.lookup_request(self.task,first,sha256_ref(receipt_bytes(first)))
        self.assertEqual(lookup['payload']['fence'],2)
        self.assertTrue(self.kind.validate_lookup(self.task,first,lookup['payload']))
        bad={**lookup['payload'],'content_instance':'restored-other'}
        with self.assertRaises(WorkflowError):self.kind.validate_lookup(self.task,first,bad)

    def test_task_params_cannot_inject_control_approval(self):
        self.task.spec.params['control_approval']={'approval_id':'injected'}
        with self.assertRaises(WorkflowError):self.kind.validate(self.task.spec.params)
        with self.assertRaises(WorkflowError):self.kind.request_with_context(self.task,self.context)

    def test_shorter_row_expiry_is_the_transmitted_deadline(self):
        expiry=(datetime.now(timezone.utc)+timedelta(minutes=5)).isoformat()
        self.context['control_approval']['expires_at']=expiry
        request=self.kind.request_with_context(self.task,self.context)
        approval=request['payload']['control_approval']
        self.assertEqual(approval['expires_at'],expiry)
        self.assertEqual(set(approval),{'approval_id','proof_hash','task_id','scope','actor','expires_at'})
        self.context['control_approval']['expires_at']=(datetime.now(timezone.utc)+timedelta(hours=2)).isoformat()
        with self.assertRaises(WorkflowError):self.kind.request_with_context(self.task,self.context)

    def test_committed_replay_precedes_current_guard_and_never_writes(self):
        from contextlib import nullcontext
        from unittest.mock import patch
        from knowledge.m0_content import dispatch_component as component,workflow_content as writer
        request=self.kind.request_with_context(self.task,self.context)
        historical={'script_ref':self.task.spec.params['script_ref'],'history':'committed'}
        with patch.object(component,'search_modules',return_value=(None,None,None,None)),patch.object(component,'dependency_guard',return_value=nullcontext(object())),patch.object(component,'require_guard',side_effect=AssertionError('new-write guard called for historical commit')),patch.object(writer,'lookup_content',return_value={'state':'committed','result':historical,'current_usable':False}) as lookup,patch.object(writer,'write_content',side_effect=AssertionError('historical replay wrote')) as write,patch.dict(component.os.environ,{'M0_RESEARCH_CONTENT_TEST_CRASH':''}):
            response=component.invoke(self.binding,request)
            self.assertTrue(response['ok']);self.assertEqual(response['data']['history'],'committed')
            lookup.assert_called_once();write.assert_not_called()

    def test_absent_then_guard_reject_is_precommit_and_conflict_is_not(self):
        from contextlib import nullcontext
        from unittest.mock import patch
        from knowledge.m0_content import dispatch_component as component,workflow_content as writer
        request=self.kind.request_with_context(self.task,self.context)
        for state in ('absent','conflict'):
            with self.subTest(state=state),patch.object(component,'search_modules',return_value=(None,None,None,None)),patch.object(component,'dependency_guard',return_value=nullcontext(object())),patch.object(component,'require_guard',side_effect=WorkflowError('DEPENDENCY_UNUSABLE')) as guard,patch.object(writer,'lookup_content',return_value={'state':state}),patch.object(writer,'write_content') as write:
                with self.assertRaises(WorkflowError) as caught:component.invoke(self.binding,request)
                self.assertEqual(isinstance(caught.exception,component.PrecommitRejected),state=='absent')
                self.assertEqual(guard.call_count,1 if state=='absent' else 0)
                write.assert_not_called()

    def test_component_rejects_each_foreign_instance_before_source_lookup(self):
        from unittest.mock import patch
        from knowledge.m0_content import dispatch_component as component,workflow_content as writer
        request=self.kind.request_with_context(self.task,self.context)
        for field in ('binding_hash','catalog_instance','content_instance'):
            lookup=self.kind.lookup_request(self.task,request,sha256_ref(receipt_bytes(request)))
            lookup['payload'][field]='foreign-instance'
            with self.subTest(field=field),patch.object(component,'search_modules',return_value=(None,None,None,None)),patch.object(component,'dependency_guard',side_effect=AssertionError('source accessed')),patch.object(writer,'lookup_content') as read:
                response=component.invoke(self.binding,lookup)
                self.assertEqual(response['data']['state'],'instance_mismatch');read.assert_not_called()

    def test_only_verified_rollback_marker_crosses_component_boundary(self):
        from unittest.mock import patch
        from contextlib import redirect_stdout
        from pathlib import Path
        import tempfile,io,json
        from knowledge.m0_content import dispatch_component as component
        with tempfile.TemporaryDirectory() as tmp:
            request=Path(tmp)/'request.json';request.write_text(json.dumps({'request_id':self.task.task_id}))
            for code,marker,accepted in [('DEPENDENCY_UNUSABLE','rolled_back',True),('DEPENDENCY_UNUSABLE',None,False),('UNKNOWN_COMMIT','rolled_back',False),('DEPENDENCY_UNUSABLE','committed',False)]:
                with self.subTest(code=code,marker=marker):
                    error=WorkflowError(code)
                    if marker:error.commit_state=marker
                    out=io.StringIO()
                    with patch.object(component.sys,'argv',['component','invoke','--request',str(request),'--workflow-binding',str(Path(tmp)/'binding.json')]),patch.object(component,'load_workflow_binding',return_value=self.binding),patch.object(component,'invoke',side_effect=error),redirect_stdout(out):
                        self.assertEqual(component.main(),2)
                    response=json.loads(out.getvalue())
                    self.assertEqual('commit_state' in response,accepted)
                    result=self.kind.rejection_outcome(response,self.task)
                    self.assertEqual(result is not None,accepted)
                    if accepted:self.assertEqual(result.result['commit_state'],'rolled_back')

    def test_projection_and_result_reject_each_wrong_or_missing_instance(self):
        from knowledge.m0_content.dispatch_component import projection,envelope
        data=projection(self.binding,{'historical':True})
        self.assertEqual(self.kind.result(envelope(self.task.task_id,data),self.task),data)
        for field in ('workflow_binding','catalog_instance','content_instance'):
            with self.subTest(field=field):
                with self.assertRaises(WorkflowError):projection(self.binding,{field:'foreign'})
                for missing in (False,True):
                    wrong=dict(data)
                    if missing:wrong.pop(field)
                    else:wrong[field]='foreign'
                    with self.assertRaises(WorkflowError):self.kind.result(envelope(self.task.task_id,wrong),self.task)

    def test_fault_environment_is_forwarded_only_for_synthetic_runner(self):
        from unittest.mock import patch
        from sa_dispatch.runner import ComponentRunner
        import os,json
        for synthetic in (False,True):
            captured=[]
            config=SimpleNamespace(synthetic=synthetic,component_timeout_seconds=2,
                components={'fixture':SimpleNamespace(argv=('fixture',),cwd=None)})
            def run(*args,**kwargs):
                captured.append(kwargs['env'])
                kwargs['stdout'].write(json.dumps({'ok':True}).encode())
                return SimpleNamespace(returncode=0)
            with patch.dict(os.environ,{'M0_RESEARCH_CONTENT_TEST_CRASH':'after_commit'}),patch('sa_dispatch.runner.subprocess.run',side_effect=run):
                ComponentRunner(config).run_component('fixture',{'operation':'test'})
            self.assertEqual('M0_RESEARCH_CONTENT_TEST_CRASH' in captured[0],synthetic)

if __name__=='__main__':unittest.main()
