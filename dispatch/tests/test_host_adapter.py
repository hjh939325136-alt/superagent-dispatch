"""Contract fixture HTTP tests; real Rust host is separately exercised by lab."""
from contextlib import contextmanager
from dataclasses import asdict
import json
from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import canonical_json
from sa_dispatch.host_adapter import HostAdapter,HostError,WorkbenchClient,task_lock
from sa_dispatch.models import Ref,TaskSpec
from sa_dispatch.store_sqlite import SqliteStore


class Fixture:
    def __init__(self):
        self.sessions={};self.calls=[];self.lose_create=False;self.gap=False
    def emit(self,s,typ,**data):
        e={'seq':len(s['events'])+1,'type':typ,**data};s['events'].append(e);return e
    def finish(self,sid,status='completed'):
        s=self.sessions[sid];r=s['requests'][-1];r['status']=status;s['status']=status
        self.emit(s,'run.'+status,run_id=r['run_id'],status=status,usage={'input_tokens':10,'output_tokens':5})
    def api(self,path,body):
        self.calls.append((path,body))
        path=path.split('?')[0]
        if path=='/api/sessions':
            if body is None:return list(self.sessions.values())
            sid=str(uuid.uuid4());s={'id':sid,**body,'codex_thread_id':None,'status':'idle','events':[],'requests':[],'messages':[]}
            self.sessions[sid]=s;self.emit(s,'session')
            return s
        parts=path.split('/');s=self.sessions[parts[3]]
        if len(parts)==4:return s
        if parts[4]=='messages':
            for r in s['requests']:
                if r['request_id']==body['request_id']:
                    if r['text']!=body['text']:return (409,'IDEMPOTENCY_CONFLICT')
                    return {'session_id':s['id'],'run_id':r['run_id']}
            r={**body,'run_id':str(uuid.uuid4()),'status':'running'};s['requests'].append(r)
            s['status']='running';s['codex_thread_id']=s['codex_thread_id'] or 'fake-thread'
            self.emit(s,'message',run_id=r['run_id'],message={'role':'user','text':body['text']})
            self.emit(s,'run.started',run_id=r['run_id'],status='running')
            if body['text']!='wait':self.finish(s['id'])
            return {'session_id':s['id'],'run_id':r['run_id']}
        if parts[4]=='cancel':
            self.finish(s['id'],'cancelled');return {'cancelling':True}
        raise ValueError(path)


@contextmanager
def fixture_server():
    fixture=Fixture()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def handle(self):
            try:super().handle()
            except ConnectionResetError:pass  # expected when the adapter process is killed
        def do_GET(self):self.handle_api(None)
        def do_POST(self):self.handle_api(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
        def handle_api(self,body):
            if '/events?' in self.path:
                sid=self.path.split('/')[3];after=int(self.path.split('after=')[1])
                rows=[e for e in fixture.sessions[sid]['events'] if e['seq']>after]
                if fixture.gap and rows:rows=[{**rows[0],'seq':rows[0]['seq']+1}]
                blob=b''.join(('data: '+json.dumps(e)+'\n\n').encode() for e in rows[:1])
                self.send_response(200);self.send_header('Content-Type','text/event-stream')
            else:
                result=fixture.api(self.path,body)
                if body is not None and self.path=='/api/sessions' and fixture.lose_create:
                    fixture.lose_create=False;self.close_connection=True;return
                if isinstance(result,tuple):
                    self.send_response(result[0]);blob=json.dumps({'ok':False,'error':{'code':result[1]}}).encode()
                else:
                    def public(item):return {k:v for k,v in item.items() if k!='requests'}
                    if isinstance(result,list):result=[public(s) for s in result]
                    elif isinstance(result,dict):result=public(result)
                    self.send_response(200);blob=json.dumps({'ok':True,'data':result}).encode()
            self.send_header('Content-Length',str(len(blob)));self.end_headers()
            try:self.wfile.write(blob)
            except (BrokenPipeError,ConnectionResetError):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:yield fixture,'http://127.0.0.1:'+str(server.server_port)
    finally:server.shutdown();server.server_close();thread.join()


class HostTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.store=SqliteStore(self.root/'ledger.db',lease_ttl_seconds=10)
        self.artifacts=ArtifactStore(self.root/'cas');self.store.register_device('host',['host.fixture'])
        self.ctx=fixture_server();self.fixture,self.url=self.ctx.__enter__()
        self.addCleanup(lambda:self.ctx.__exit__(None,None,None))
        self.client=WorkbenchClient(self.url,timeout=.2)
        self.adapter=HostAdapter(self.store,self.artifacts,self.client,isolated_synthetic=True)
    def task(self,text='hello',effect='none',max_attempts=3,claim=True):
        ref=self.artifacts.put(canonical_json({'synthetic':True,'text':text}))
        task,_=self.store.submit(TaskSpec('host:'+uuid.uuid4().hex,'host.synthetic',1,
            {'prompt_ref':ref,'agent_id':'lead'},(Ref('dispatch','prompt',ref[7:],'1',ref),),
            ('host.fixture',),'synthetic','none',max_attempts),side_effect=effect)
        return task,self.store.claim('host') if claim else None
    def test_terminal_receipt_and_sse_cursor(self):
        task,lease=self.task();result=self.adapter.run(lease)
        self.assertEqual(result['status'],'completed');self.assertEqual(self.store.get_task(task.task_id).status,'succeeded')
        self.assertEqual(len(self.fixture.sessions),1);self.assertGreater(result['host_run']['last_event_seq'],0)
        row=self.adapter.attempt(task.task_id,1);self.artifacts.get(row['report_ref'])
    def test_create_response_lost_recovers_exact_owned_title(self):
        task,lease=self.task();self.fixture.lose_create=True
        with self.assertRaises((HostError,OSError)):self.adapter.run(lease)
        result=self.adapter.run(lease);self.assertEqual(result['status'],'completed')
        self.assertEqual(len(self.fixture.sessions),1)
    def test_cursor_committed_before_receipt_can_recover(self):
        task,lease=self.task()
        with patch.object(self.adapter,'_finish',side_effect=RuntimeError('kill boundary')):
            with self.assertRaises(RuntimeError):self.adapter.run(lease)
        result=self.adapter.run(lease);self.assertEqual(result['status'],'completed')
        self.assertEqual(len(next(iter(self.fixture.sessions.values()))['requests']),1)
    def test_event_gap_uses_full_snapshot(self):
        self.fixture.gap=True;task,lease=self.task();self.assertEqual(self.adapter.run(lease)['status'],'completed')
    def test_real_subprocess_kill_then_resume_does_not_submit_again(self):
        task,lease=self.task('wait')
        command=[sys.executable,'-m','sa_dispatch.host_adapter','--ledger',str(self.store.path),
            '--artifacts',str(self.artifacts.root),'--lease-id',lease.lease_id,'--base-url',self.url,
            '--isolated-synthetic','--timeout','20']
        process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            deadline=time.monotonic()+8
            while time.monotonic()<deadline:
                row=self.adapter.attempt(task.task_id,1)
                if row and row['last_event_seq']>=2:break
                time.sleep(.03)
            else:self.fail('adapter did not reach stream')
            with self.assertRaisesRegex(HostError,'HOST_ADAPTER_BUSY'):
                with task_lock(self.store.path,task.task_id):pass
            process.kill();process.communicate(timeout=5)
            self.fixture.finish(row['session_id'])
            self.assertEqual(self.adapter.run(lease)['status'],'completed')
            self.assertEqual(len(self.fixture.sessions[row['session_id']]['requests']),1)
        finally:
            if process.poll() is None:process.kill();process.communicate(timeout=5)
    def test_new_fence_adopts_existing_run_without_resubmission(self):
        task,lease=self.task('wait')
        with self.assertRaisesRegex(HostError,'HOST_OBSERVATION_TIMEOUT'):self.adapter.run(lease,timeout_s=.05)
        with self.store._tx() as c:c.execute("UPDATE leases SET expires_at='2000-01-01' WHERE lease_id=?",(lease.lease_id,))
        self.store.sweep();new=self.store.claim('host');row=self.adapter.attempt(task.task_id,1)
        self.fixture.finish(row['session_id']);result=self.adapter.run(new)
        self.assertEqual(result['host_run']['adopted_fence'],1)
        self.assertEqual(len(self.fixture.sessions[row['session_id']]['requests']),1)
    def test_interrupted_requires_explicit_resume_in_same_thread(self):
        task,lease=self.task('wait')
        with self.assertRaises(HostError):self.adapter.run(lease,timeout_s=.05)
        row=self.adapter.attempt(task.task_id,1);self.fixture.finish(row['session_id'],'interrupted')
        self.assertEqual(self.adapter.run(lease)['status'],'interrupted')
        with self.store._tx() as c:c.execute("UPDATE leases SET expires_at='2000-01-01' WHERE lease_id=?",(lease.lease_id,))
        self.store.sweep();new=self.store.claim('host')
        with self.assertRaisesRegex(HostError,'HOST_EXPLICIT_RESUME_REQUIRED'):self.adapter.run(new)
        with self.assertRaisesRegex(HostError,'HOST_OBSERVATION_TIMEOUT'):
            self.adapter.run(new,timeout_s=.05,resume_interrupted=True)
        self.assertEqual(len(self.fixture.sessions[row['session_id']]['requests']),2)
        self.assertEqual(self.adapter.attempt(task.task_id,2)['session_id'],row['session_id'])
    def test_foreign_cancel_denied_and_owned_cancel_records_failed(self):
        task,lease=self.task('wait')
        with self.assertRaisesRegex(HostError,'HOST_NOT_OWNER'):self.adapter.cancel(task.task_id,1)
        with self.assertRaises(HostError):self.adapter.run(lease,timeout_s=.05)
        self.adapter.cancel(task.task_id,1)
        self.assertEqual(self.adapter.run(lease)['status'],'cancelled')
        self.assertEqual(self.store.get_task(task.task_id).status,'failed')
    def test_write_task_rejected_before_any_host_request(self):
        task,lease=self.task(effect='external')
        with self.assertRaisesRegex(HostError,'HOST_SYNTHETIC_READ_ONLY_REQUIRED'):self.adapter.run(lease)
        self.assertEqual(self.fixture.calls,[])
    def test_same_request_different_text_is_final_protocol_error(self):
        task,lease=self.task('wait')
        with self.assertRaises(HostError):self.adapter.run(lease,timeout_s=.05)
        row=self.adapter.attempt(task.task_id,1)
        with self.assertRaisesRegex(HostError,'HOST_409_TEXT_MISMATCH'):
            self.client.json('/api/sessions/'+row['session_id']+'/messages',{'text':'changed','request_id':row['request_id']})
        self.fixture.sessions[row['session_id']]['requests'][0]['text']='changed'
        self.fixture.sessions[row['session_id']]['events'][1]['message']['text']='changed'
        self.assertEqual(self.adapter.run(lease)['status'],'failed_final')
        self.assertEqual(self.store.get_task(task.task_id).status,'failed')
    def test_endpoint_and_fixture_boundary(self):
        for url in ('http://example.com:80','http://127.0.0.1:99/x','http://u:p@127.0.0.1:99'):
            with self.assertRaises(HostError):WorkbenchClient(url)
        with self.assertRaises(HostError):HostAdapter(self.store,self.artifacts,self.client)

    def test_normal_cli_submit_run_receipt_and_trace_avoid_unrelated_task(self):
        cfg=self.root/'config.toml'
        values={'db_path':str(self.store.path),'artifact_root':str(self.artifacts.root),
                'journal_dir':str(self.root/'journal'),'device_id':'host','capabilities':['host.fixture'],
                'lease_ttl_seconds':10,'component_timeout_seconds':3,'plugins':['host_fixture'],'synthetic':True}
        cfg.write_text('\n'.join(k+' = '+json.dumps(v) for k,v in values.items())+'\n[components]\n',encoding='utf-8')
        def cli(*args):
            result=subprocess.run([sys.executable,'-B','-m','sa_dispatch','--config',str(cfg),*args],capture_output=True)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            return json.loads(result.stdout)['data']
        unrelated,_=self.store.submit(TaskSpec('unrelated','lab.read',1,{},(),(),'synthetic','none',2))
        ref=self.artifacts.put(canonical_json({'synthetic':True,'text':'hello via normal cli'}))
        spec=TaskSpec('cli-host','host.synthetic',1,{'prompt_ref':ref,'agent_id':'lead'},
            (Ref('dispatch','prompt','cli','1',ref),),('host.fixture',),'synthetic','none',2)
        req=self.root/'request.json';req.write_bytes(canonical_json(asdict(spec)))
        task=cli('submit','--input',str(req))['task']
        self.assertTrue(cli('submit','--input',str(req))['deduplicated'])
        result=cli('host-run','--base-url',self.url,'--isolated-synthetic')
        self.assertEqual(result['status'],'completed')
        receipt=cli('receipt',task['task_id']);self.assertEqual(receipt['response']['task_id'],task['task_id'])
        trace=cli('trace',task['task_id']);self.assertIn('task.succeeded',[e['kind'] for e in trace['events']])
        self.assertEqual(self.store.get_task(unrelated.task_id).status,'queued')


if __name__=='__main__':unittest.main()
