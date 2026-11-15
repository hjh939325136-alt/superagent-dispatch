import hashlib
import base64
from dataclasses import asdict,replace
import http.client
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from sa_dispatch.clock import FakeClock
from sa_dispatch.models import TaskSpec,Ref,FenceRejected
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.transport import CoordinatorAPI,create_server,TransportError

class TransportTests(unittest.TestCase):
    def test_literal_loopback_startup_does_not_resolve_dns(self):
        with patch('socket.getfqdn', side_effect=AssertionError('unexpected hostname lookup')):
            with create_server(self.api) as server:
                self.assertEqual(server.server_name, '127.0.0.1')

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.clock=FakeClock()
        self.store=SqliteStore(Path(self.temp.name)/'ledger.db',clock=self.clock,lease_ttl_seconds=10)
        self.store.register_device('a',{'cpu'});self.store.register_device('b',{'cpu'})
        self.tokens={'a':'synthetic-a-'+'a'*40,'b':'synthetic-b-'+'b'*40}
        api=CoordinatorAPI(self.store,{k:hashlib.sha256(v.encode()).hexdigest() for k,v in self.tokens.items()})
        self.api=api;self.server=create_server(api)
        thread=threading.Thread(target=self.server.serve_forever,daemon=True);thread.start()
        def close():
            self.server.shutdown();self.server.server_close();thread.join()
        self.addCleanup(close)
        self.task,_=self.store.submit(TaskSpec('key','fixture',1,{},(),('cpu',),'synthetic','none',2))

    def call(self,path,payload,device='a',extra=None):
        c=http.client.HTTPConnection(*self.server.server_address,timeout=2)
        headers={'Content-Type':'application/json','Authorization':'Bearer '+self.tokens.get(device,'bad')}
        headers.update(extra or {})
        c.request('POST',path,json.dumps(payload),headers)
        response=c.getresponse();result=(response.status,json.loads(response.read()));c.close();return result

    def test_identity_binding_and_replay(self):
        status,body=self.call('/claim',{});self.assertEqual(status,200)
        lease=body['data']['lease'];args={k:lease[k] for k in ('lease_id','fence')}
        self.assertEqual(self.call('/renew',args,'b')[0],404)
        self.assertEqual(self.call('/status',{'task_id':self.task.task_id},'b')[0],404)
        self.assertEqual(self.call('/renew',args)[0],200)
        args['outcome']={'status':'succeeded','result':{'count':0},'evidence':[]}
        self.assertEqual(self.call('/complete',args)[0],200)
        self.assertEqual(self.call('/complete',args)[0],200)
        self.assertEqual(self.call('/status',{'task_id':self.task.task_id})[1]['data']['status'],'succeeded')

    def test_auth_payload_and_browser_boundaries(self):
        self.assertEqual(self.call('/claim',{},'missing')[0],401)
        self.assertEqual(self.call('/claim',{'device_id':'b'})[0],400)
        self.assertEqual(self.call('/claim',{'caps':['cpu']})[0],400)
        self.assertEqual(self.call('/claim',{},extra={'Origin':'https://untrusted.invalid'})[0],403)
        self.assertEqual(self.call('/claim',{},extra={'Host':'untrusted.invalid'})[0],403)
        self.assertEqual(self.call('/status',{'task_id':self.task.task_id})[0],404)

    def test_expired_lease_is_rejected_before_sweep(self):
        lease=self.call('/claim',{})[1]['data']['lease'];self.clock.advance(10)
        self.assertEqual(self.call('/renew',{k:lease[k] for k in ('lease_id','fence')})[0],409)

    def test_remote_bind_denied(self):
        for host in ('0.0.0.0','192.0.2.1','localhost','::1'):
            with self.subTest(host=host),self.assertRaises(ValueError):create_server(self.api,host)

class SyntheticHandoffTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.clock=FakeClock()
        class Kind:
            name='fixture';version=1;enabled=True;side_effect='none'
            def validate(self,params):
                if params!={}:raise ValueError('unknown parameters')
                return params
            def scope(self,params):return {}
        self.store=SqliteStore(Path(self.temp.name)/'db',clock=self.clock,lease_ttl_seconds=10,kinds={'fixture':Kind()})
        for name in ('worker','foreign'):self.store.register_device(name,{'cpu'})
        self.tokens={name:name+'x'*40 for name in ('dispatcher','worker','foreign')}
        self.api=CoordinatorAPI(self.store,{k:hashlib.sha256(v.encode()).hexdigest() for k,v in self.tokens.items()},
                                synthetic_handoff=True,roles={'dispatcher':{'dispatcher'},'worker':{'worker'},'foreign':{'worker'}},
                                artifact_store=ArtifactStore(Path(self.temp.name)/'artifacts'))
        content=b'{"synthetic":true}'
        self.digest='sha256:'+hashlib.sha256(content).hexdigest()
        self.encoded=base64.b64encode(content).decode()
        self.spec=TaskSpec('synthetic','fixture',1,{},(Ref('fixture','input','one','1',self.digest),),('cpu',),'synthetic','none',2)
        self.task=self.call('/submit',{'spec':asdict(self.spec),'inputs':{self.digest:self.encoded}},'dispatcher')
        self.lease=self.call('/claim',{},'worker')['lease']
        self.args={k:self.lease[k] for k in ('lease_id','fence')}

    def call(self,path,payload,identity='worker'):
        return self.api.invoke(path,payload,'Bearer '+self.tokens[identity])

    def test_handoff_input_and_bound_receipt(self):
        self.assertEqual(self.call('/task',self.args)['spec']['access'],'synthetic')
        self.assertEqual(self.call('/artifact/get',{**self.args,'hash':self.digest})['content_base64'],self.encoded)
        content=b'{"receipt":"synthetic"}';digest='sha256:'+hashlib.sha256(content).hexdigest()
        outcome={'status':'succeeded','result':{'receipt':digest},'evidence':[{'kind':'component_response','content_hash':digest,'access':'synthetic','artifact_hash':digest}]}
        with self.assertRaises(TransportError):self.call('/complete',{**self.args,'outcome':outcome})
        self.call('/artifact/put',{**self.args,'hash':digest,'content_base64':base64.b64encode(content).decode()})
        self.assertTrue(self.call('/complete',{**self.args,'outcome':outcome})['completed'])
        with self.assertRaises(FenceRejected):self.call('/artifact/get',{**self.args,'hash':digest})

    def test_no_foreign_or_unbound_artifact_reads(self):
        with self.assertRaises(TransportError):self.call('/task',self.args,'foreign')
        with self.assertRaises(TransportError):self.call('/artifact/get',{**self.args,'hash':self.digest},'foreign')
        unknown=self.api.artifact_store.put(b'not granted')
        with self.assertRaises(TransportError):self.call('/artifact/get',{**self.args,'hash':unknown})
        with self.assertRaises(TransportError):self.call('/submit',{'spec':asdict(self.spec),'inputs':{}},'worker')
        self.clock.advance(10)
        with self.assertRaises(FenceRejected):self.call('/task',self.args)

    def test_local_only_rejected_even_if_added_later(self):
        spec=replace(self.spec,idem_key='private',access='local_only')
        with self.assertRaises(TransportError):self.call('/submit',{'spec':asdict(spec),'inputs':{self.digest:self.encoded}},'dispatcher')
        self.store.submit(spec)
        for path,payload in [('/task',self.args),('/artifact/get',{**self.args,'hash':self.digest}),('/claim',{})]:
            with self.subTest(path=path),self.assertRaises(TransportError):self.call(path,payload)

    def test_invalid_hash_and_non_synthetic_receipt(self):
        with self.assertRaises(TransportError):self.call('/artifact/put',{**self.args,'hash':self.digest,'content_base64':base64.b64encode(b'wrong').decode()})
        outcome={'status':'succeeded','result':{},'evidence':[{'kind':'artifact','content_hash':self.digest,'artifact_hash':self.digest,'access':'local_only'}]}
        with self.assertRaises(TransportError):self.call('/complete',{**self.args,'outcome':outcome})

    def test_restart_preserves_input_and_attempt_output_grants(self):
        content=b'persisted synthetic output';digest='sha256:'+hashlib.sha256(content).hexdigest()
        self.call('/artifact/put',{**self.args,'hash':digest,'content_base64':base64.b64encode(content).decode()})
        self.api=CoordinatorAPI(self.store,{k:hashlib.sha256(v.encode()).hexdigest() for k,v in self.tokens.items()},
                                synthetic_handoff=True,roles={'dispatcher':{'dispatcher'},'worker':{'worker'},'foreign':{'worker'}},
                                artifact_store=ArtifactStore(Path(self.temp.name)/'artifacts'))
        self.assertEqual(self.call('/artifact/get',{**self.args,'hash':self.digest})['content_base64'],self.encoded)
        self.assertEqual(base64.b64decode(self.call('/artifact/get',{**self.args,'hash':digest})['content_base64']),content)
        with self.assertRaises(TransportError):self.call('/artifact/get',{**self.args,'hash':digest},'foreign')
        outcome={'status':'succeeded','result':{'receipt':digest},'evidence':[{'kind':'artifact','content_hash':digest,'artifact_hash':digest,'access':'synthetic'}]}
        self.call('/complete',{**self.args,'outcome':outcome})

    def test_failed_staging_publishes_no_task(self):
        spec=replace(self.spec,idem_key='fails')
        before=len(self.store.list_tasks())
        with patch.object(self.api.artifact_store,'put',side_effect=OSError('synthetic disk failure')):
            with self.assertRaises(OSError):self.call('/submit',{'spec':asdict(spec),'inputs':{self.digest:self.encoded}},'dispatcher')
        self.assertEqual(len(self.store.list_tasks()),before)

    def test_crash_window_claim_gate_and_replay_repair(self):
        spec=replace(self.spec,idem_key='interrupted')
        interrupted,_=self.store.submit(spec)
        with self.assertRaises(TransportError) as caught:self.call('/claim',{},'foreign')
        self.assertEqual(caught.exception.code,'E_HANDOFF_NOT_READY')
        self.assertEqual(self.store.get_task(interrupted.task_id).status,'queued')
        self.call('/submit',{'spec':asdict(spec),'inputs':{self.digest:self.encoded}},'dispatcher')
        self.assertEqual(self.call('/claim',{},'foreign')['lease']['task_id'],interrupted.task_id)

if __name__=='__main__':unittest.main()
