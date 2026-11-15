"""No authentication or model calls: bounded subprocess lifecycle fixtures."""
from pathlib import Path
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import canonical_json
from sa_dispatch.claude_adapter import ClaudeAdapter,ClaudeError
from sa_dispatch.models import Ref,TaskSpec
from sa_dispatch.store_sqlite import SqliteStore

FAKE = r'''
import json,sys,time,os
from pathlib import Path
a=sys.argv[1:];text=sys.stdin.read()
assert '--safe-mode' in a and a[a.index('--tools')+1]==''
assert '--strict-mcp-config' in a and json.loads(a[a.index('--mcp-config')+1])=={'mcpServers':{}}
assert os.environ.get('CLAUDE_CODE_DISABLE_TERMINAL_TITLE')=='1'
with Path(__file__).with_suffix('.calls').open('a') as f:f.write(json.dumps(a)+'\n')
model=a[a.index('--model')+1]
sid=a[a.index('--resume' if '--resume' in a else '--session-id')+1]
marker=Path('owned-'+sid)
if '--resume' in a:assert marker.exists()
else:marker.write_text('synthetic')
init={'type':'system','subtype':'init','session_id':sid,'model':model,'tools':[],'mcp_servers':[]}
if text in ('has-tools','sleep-tools'):init['tools']=['Read']
if text in ('has-mcp','sleep-mcp'):init['mcp_servers']=[{'name':'unexpected'}]
if text=='sleep-model':init['model']='claude-foreign'
if text=='sleep-session':init['session_id']='foreign-session'
if text!='no-init':print(json.dumps(init),flush=True)
if text.startswith('sleep') and '--resume' not in a:time.sleep(3)
if text=='wrong-model':model='claude-wrong'
if text=='bad-json':print('not json');sys.exit(0)
data={'type':'result','subtype':'success','is_error':False,'session_id':sid,'result':'FIXTURE_OK',
      'modelUsage':{model:{'inputTokens':2,'outputTokens':2,'costUSD':0.001}}}
if text=='bad-usage':data['modelUsage'][model]['inputTokens']='2'
if text=='error':data['is_error']=True;data['modelUsage']={}
print(json.dumps(data))
sys.exit(1 if text=='error' else 0)
'''


class ClaudeAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.fake=self.root/'fake.py';self.fake.write_text(FAKE,encoding='utf-8')
        self.store=SqliteStore(self.root/'ledger.db',lease_ttl_seconds=10)
        self.store.register_device('claude',['claude.synthetic'])
        self.artifacts=ArtifactStore(self.root/'cas')
        self.adapter=ClaudeAdapter(self.store,self.artifacts,cli_command=[sys.executable,str(self.fake)],
            run_root=self.root/'runs',isolated_synthetic=True,allowed_models=('claude-fixture-1',))

    def task(self,text='hello',effect='none'):
        ref=self.artifacts.put(canonical_json({'synthetic':True,'text':text}))
        task,_=self.store.submit(TaskSpec('claude:'+uuid.uuid4().hex,'claude.synthetic',1,
            {'prompt_ref':ref,'model':'claude-fixture-1'},(Ref('dispatch','prompt',ref[7:],'1',ref),),
            ('claude.synthetic',),'synthetic','none',3),side_effect=effect)
        return task,self.store.claim('claude')

    def expire(self,lease):
        with self.store._tx() as conn:
            conn.execute("UPDATE leases SET expires_at='2000-01-01' WHERE lease_id=?",(lease.lease_id,))
        self.store.sweep()

    def calls(self):
        path=self.fake.with_suffix('.calls')
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_success_has_bound_model_session_and_artifact(self):
        task,lease=self.task();result=self.adapter.run(lease)
        self.assertEqual(result['status'],'succeeded')
        self.assertEqual(set(result['modelUsage']),{'claude-fixture-1'})
        self.assertEqual(self.store.get_task(task.task_id).status,'succeeded')
        self.assertEqual(len(self.calls()),1)
        self.artifacts.get(result['stdout_ref'])
        self.assertEqual(len(list((self.root/'runs'/task.task_id/'1'/'cwd').iterdir())),1)

    def test_title_disable_is_child_only_even_with_conflicting_parent_value(self):
        task,lease=self.task()
        with patch.dict(os.environ,{'CLAUDE_CODE_DISABLE_TERMINAL_TITLE':'0'}):
            report=self.adapter.run(lease)
            self.assertEqual(report['status'],'succeeded')
            self.assertEqual(os.environ['CLAUDE_CODE_DISABLE_TERMINAL_TITLE'],'0')

    def test_result_rejections_are_final_and_do_not_hide_exit_error(self):
        for text,code in [('wrong-model','MODEL_MISMATCH'),('bad-json','CLAUDE_INVALID_RESULT'),
                          ('bad-usage','CLAUDE_INVALID_USAGE'),('error','CLAUDE_PROCESS_ERROR'),
                          ('has-tools','CLAUDE_UNVERIFIED_TOOL_ISOLATION'),
                          ('has-mcp','CLAUDE_UNVERIFIED_TOOL_ISOLATION'),
                          ('no-init','CLAUDE_UNVERIFIED_TOOL_ISOLATION')]:
            with self.subTest(text=text):
                task,lease=self.task(text);result=self.adapter.run(lease)
                self.assertEqual(result['error'],code)
                self.assertEqual(self.store.get_task(task.task_id).status,'failed')

    def test_write_task_is_rejected_before_spawn(self):
        task,lease=self.task(effect='external')
        with self.assertRaisesRegex(ClaudeError,'CLAUDE_SYNTHETIC_READ_ONLY_REQUIRED'):self.adapter.run(lease)
        self.assertEqual(self.calls(),[])

    def test_timeout_requires_new_fence_explicit_resume_same_owned_session(self):
        task,lease=self.task('sleep');result=self.adapter.run(lease,timeout_s=.2)
        self.assertEqual(result['status'],'interrupted')
        self.assertEqual(len(self.calls()),1)
        repeat=self.adapter.run(lease,timeout_s=.2)
        self.assertEqual(repeat['session_id'],result['session_id']);self.assertEqual(len(self.calls()),1)
        self.expire(lease);second=self.store.claim('claude')
        resumed=self.adapter.run(second,resume=True)
        self.assertEqual(resumed['status'],'succeeded')
        self.assertEqual(resumed['session_id'],result['session_id'])
        self.assertIn('--resume',self.calls()[1]);self.assertEqual(len(self.calls()),2)

    def test_launch_intent_crash_never_blindly_respawns(self):
        task,lease=self.task()
        with patch('sa_dispatch.claude_adapter.subprocess.Popen',side_effect=RuntimeError('crash')):
            with self.assertRaises(RuntimeError):self.adapter.run(lease)
        with self.assertRaisesRegex(ClaudeError,'CLAUDE_OBSERVATION_UNCERTAIN'):
            self.adapter.run(lease,observe_timeout_s=.1)
        self.assertEqual(self.calls(),[])

    def test_real_adapter_process_kill_recovers_supervised_run_without_second_spawn(self):
        task,lease=self.task('sleep')
        script='''import sys
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.claude_adapter import ClaudeAdapter
from sa_dispatch.models import Lease
from sa_dispatch.store_sqlite import SqliteStore
s=SqliteStore(sys.argv[1],lease_ttl_seconds=10)
r=s.get_lease(sys.argv[4]);l=Lease(**{k:r[k] for k in Lease.__dataclass_fields__})
ClaudeAdapter(s,ArtifactStore(sys.argv[2]),cli_command=[sys.executable,sys.argv[3]],run_root=sys.argv[5],isolated_synthetic=True,allowed_models=('claude-fixture-1',)).run(l,timeout_s=10)
'''
        child=subprocess.Popen([sys.executable,'-B','-c',script,str(self.store.path),str(self.artifacts.root),
            str(self.fake),lease.lease_id,str(self.root/'runs')],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            deadline=time.monotonic()+5
            while not self.calls() and time.monotonic()<deadline:time.sleep(.02)
            self.assertEqual(len(self.calls()),1)
            child.kill();child.wait(timeout=5)
            self.expire(lease);second=self.store.claim('claude')
            self.assertGreater(second.fence,lease.fence)
            result=self.adapter.run(second)
            self.assertEqual(result['origin_fence'],lease.fence)
            self.assertEqual(result['status'],'succeeded');self.assertEqual(len(self.calls()),1)
            old=self.adapter.attempt(task.task_id,lease.fence)
            self.assertEqual(old['phase'],'succeeded')
            recovered=json.loads(self.artifacts.get(old['report_ref']))
            self.assertEqual(recovered['fence'],lease.fence)
            self.assertEqual(recovered['observed_by_fence'],second.fence)
            self.assertEqual(recovered['observation_basis'],'recovered_same_origin_exit')
            self.assertNotEqual(old['report_ref'],self.adapter.attempt(task.task_id,second.fence)['report_ref'])
            from sa_dispatch.bundle import export_bundle,read_bundle
            from types import SimpleNamespace
            package=self.root/'adopted-bundle'
            export_bundle(SimpleNamespace(db_path=self.store.path,artifact_root=self.artifacts.root),package)
            self.assertTrue((package/'manifest.json').exists())

        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)

    def test_untrusted_model_and_nonfinite_limits_reject_before_spawn(self):
        task,lease=self.task()
        default_adapter=ClaudeAdapter(self.store,self.artifacts,cli_command=[sys.executable,str(self.fake)],
            run_root=self.root/'runs',isolated_synthetic=True)
        with self.assertRaisesRegex(ClaudeError,'CLAUDE_FULL_MODEL_REQUIRED'):default_adapter.run(lease)
        for key in ('timeout_s','budget_usd','observe_timeout_s'):
            for value in (float('nan'),float('inf'),True,'1'):
                with self.subTest(key=key,value=value):
                    with self.assertRaisesRegex(ClaudeError,'CLAUDE_LIMIT_RANGE'):
                        self.adapter.run(lease,**{key:value})
        self.assertEqual(self.calls(),[])

    def test_historical_authority_and_foreign_directory_reject_recovery(self):
        task,lease=self.task();self.adapter._prepare(lease,10,.01,False)
        with self.store._tx() as conn:
            conn.execute("UPDATE claude_attempts SET execution_authority='historical_only' WHERE task_id=?",(task.task_id,))
        with self.assertRaisesRegex(ClaudeError,'CLAUDE_HISTORICAL_ONLY'):self.adapter.run(lease)
        with self.store._tx() as conn:
            conn.execute("UPDATE claude_attempts SET execution_authority='local_owned',run_dir=? WHERE task_id=?",(str(self.root/'foreign'),task.task_id))
        with self.assertRaisesRegex(ClaudeError,'CLAUDE_RUN_DIRECTORY_NOT_OWNED'):self.adapter.run(lease)
        self.assertEqual(self.calls(),[])

    def test_supervisor_kill_without_exit_proof_never_relaunches(self):
        task,lease=self.task('sleep');spawned=[];original=subprocess.Popen
        def capture(*args,**kwargs):
            child=original(*args,**kwargs);spawned.append(child);return child
        with patch('sa_dispatch.claude_adapter.subprocess.Popen',side_effect=capture):
            with self.assertRaisesRegex(ClaudeError,'CLAUDE_OBSERVATION_UNCERTAIN'):
                self.adapter.run(lease,observe_timeout_s=.3,timeout_s=5)
        self.assertEqual(len(spawned),1)
        spawned[0].kill();spawned[0].wait(timeout=5)
        with self.assertRaisesRegex(ClaudeError,'CLAUDE_OBSERVATION_UNCERTAIN'):
            self.adapter.run(lease,observe_timeout_s=.1)
        self.assertEqual(len(self.calls()),1)
        # Fixture child self-terminates; no arbitrary recorded PID is signalled.
        directory=Path(self.adapter.attempt(task.task_id,1)['run_dir'])
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            if b'FIXTURE_OK' in (directory/'stdout.json').read_bytes():break
            time.sleep(.05)
        else:self.fail('synthetic orphan did not finish its bounded fixture')
        self.assertFalse((directory/'exit.json').exists())
        self.assertEqual(len(self.calls()),1)


    def test_resume_refuses_unverified_exit_or_failed_phase(self):
        for phase,exit_code,error in [('interrupted',None,'CLAUDE_TIMEOUT'),('failed',0,None),('interrupted',0,None)]:
            with self.subTest(phase=phase,exit_code=exit_code):
                task,lease=self.task();row,_=self.adapter._prepare(lease,10,.01,False)
                directory=Path(row['run_dir'])
                (directory/'exit.json').write_text(json.dumps({'exit_code':exit_code,'error':error}))
                with self.store._tx() as conn:conn.execute('UPDATE claude_attempts SET phase=? WHERE task_id=?',(phase,task.task_id))
                self.expire(lease);second=self.store.claim('claude')
                with self.assertRaises(ClaudeError):self.adapter.run(second,resume=True)
                # Prevent this deliberately unfinished fixture from being selected next iteration.
                with self.store._tx() as conn:
                    conn.execute("UPDATE tasks SET status='cancelled' WHERE task_id=?",(task.task_id,))
                    conn.execute("UPDATE leases SET expires_at='2000-01-01' WHERE lease_id=?",(second.lease_id,))
        self.assertEqual(self.calls(),[])

    def test_unverified_exit_remains_uncertain_without_completion(self):
        task,lease=self.task();row,_=self.adapter._prepare(lease,10,.01,False)
        (Path(row['run_dir'])/'exit.json').write_text(json.dumps({'exit_code':None,'error':'CLAUDE_TIMEOUT'}))
        with self.assertRaisesRegex(ClaudeError,'CLAUDE_UNVERIFIED_EXIT'):self.adapter.run(lease)
        self.assertEqual(self.adapter.attempt(task.task_id,lease.fence)['phase'],'uncertain')
        self.assertEqual(self.store.get_task(task.task_id).status,'leased')
        self.assertEqual(self.calls(),[])

    def test_resume_requires_immutable_bound_interruption_report(self):
        task,lease=self.task('sleep');self.adapter.run(lease,timeout_s=.2)
        with self.store._tx() as conn:conn.execute('UPDATE claude_attempts SET report_ref=? WHERE task_id=?',(self.artifacts.put(b'{}'),task.task_id))
        self.expire(lease);second=self.store.claim('claude')
        with self.assertRaisesRegex(ClaudeError,'CLAUDE_RESUME_REQUIRES_BOUND_REPORT'):self.adapter.run(second,resume=True)
        self.assertEqual(len(self.calls()),1)

    def test_partial_init_preserved_without_inventing_zero_usage(self):
        task,lease=self.task('sleep');report=self.adapter.run(lease,timeout_s=.2)
        self.assertEqual(report['status'],'interrupted')
        self.assertTrue(report['isolation_verified']);self.assertTrue(report['resume_permitted'])
        self.assertFalse(report['usage_known']);self.assertTrue(report['cost_unknown'])
        self.assertEqual(report['modelUsage'],{})
        self.assertEqual(report['observed_model'],'claude-fixture-1')
        self.assertEqual(report['requested_model'],'claude-fixture-1')
        init=json.loads(self.artifacts.get(report['init_ref']))
        self.assertEqual(init['tools'],[]);self.assertEqual(init['mcp_servers'],[])

    def test_timeout_never_masks_init_isolation_or_identity_failure(self):
        for text in ('sleep-tools','sleep-mcp','sleep-model','sleep-session'):
            with self.subTest(text=text):
                task,lease=self.task(text);report=self.adapter.run(lease,timeout_s=.2)
                self.assertEqual(report['error'],'CLAUDE_TIMEOUT')
                self.assertFalse(report['isolation_verified']);self.assertFalse(report['resume_permitted'])
                self.assertIsNotNone(report['isolation_error'])
                self.expire(lease);second=self.store.claim('claude')
                before=len(self.calls())
                with self.assertRaisesRegex(ClaudeError,'CLAUDE_RESUME_ISOLATION_UNVERIFIED'):self.adapter.run(second,resume=True)
                self.assertEqual(len(self.calls()),before)
                with self.store._tx() as conn:
                    conn.execute("UPDATE tasks SET status='cancelled' WHERE task_id=?",(task.task_id,))
                    conn.execute("UPDATE leases SET expires_at='2000-01-01' WHERE lease_id=?",(second.lease_id,))

    def test_resume_requires_complete_matching_cas_exit_chain(self):
        for mutation in ('missing-init','missing-exit','foreign-launch','tools-with-permitted-flag'):
            with self.subTest(mutation=mutation):
                # Independent ledger prevents missing CAS in one case affecting another.
                self.setUp()
                task,lease=self.task('sleep');report=self.adapter.run(lease,timeout_s=.2)
                old=self.adapter.attempt(task.task_id,lease.fence)
                if mutation.startswith('missing-'):
                    field='init_ref' if mutation=='missing-init' else 'exit_ref'
                    self.artifacts.path(report[field]).unlink()
                else:
                    field='launch_ref' if mutation=='foreign-launch' else 'init_ref'
                    body=json.loads(self.artifacts.get(report[field]))
                    if field=='launch_ref':body['session_id']='foreign-session'
                    else:body['tools']=['Read']
                    report[field]=self.artifacts.put(json.dumps(body).encode())
                    self.assertTrue(report['resume_permitted'])
                    with self.store._tx() as conn:conn.execute('UPDATE claude_attempts SET report_ref=? WHERE task_id=?',(self.artifacts.put(json.dumps(report).encode()),task.task_id))
                self.expire(lease);second=self.store.claim('claude')
                with self.assertRaisesRegex(ClaudeError,'CLAUDE_RESUME_EVIDENCE_UNVERIFIED'):
                    self.adapter.run(second,resume=True)
                self.assertEqual(len(self.calls()),1)
                self.assertIsNone(self.adapter.attempt(task.task_id,second.fence))
                self.assertFalse((self.root/'runs'/task.task_id/str(second.fence)).exists())

if __name__=='__main__':unittest.main()
