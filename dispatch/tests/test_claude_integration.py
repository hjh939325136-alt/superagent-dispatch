"""Lifecycle route and inert migration tests, with synthetic subprocess only."""
from contextlib import redirect_stdout
from dataclasses import replace
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from test_claude_adapter import FAKE
from sa_dispatch.artifacts import ArtifactError,ArtifactStore
from sa_dispatch.bundle import (V3_FORMAT,V3_TABLES,export_bundle,import_bundle,
                                read_bundle,ledger)
from sa_dispatch.canonical import canonical_json,sha256_ref
from sa_dispatch.claude_adapter import ClaudeAdapter,ClaudeError
from sa_dispatch.claude_kind import KINDS
from sa_dispatch.cli import main
from sa_dispatch.config import Config
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.models import Ref,TaskSpec
from sa_dispatch.store_sqlite import SqliteStore

class Output(io.StringIO):
    def __init__(self):super().__init__();self.buffer=io.BytesIO()

class ClaudeIntegrationTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name);self.fake=self.root/'fake.py';self.fake.write_text(FAKE,encoding='utf-8')
        self.config=Config(self.root/'ledger.db',self.root/'cas',self.root/'journal','fixture',
                           ('claude.synthetic',),20,5,('claude_runtime',),{},True)
        self.config_file=self.root/'config.toml'
        fields={**self.config.__dict__}
        fields={key:str(value) if isinstance(value,Path) else list(value) if isinstance(value,tuple) else value for key,value in fields.items()}
        self.config_file.write_text('\n'.join(key+' = '+json.dumps(value) for key,value in fields.items() if key!='components')+'\n[components]\n',encoding='utf-8')
        self.store=SqliteStore(self.config.db_path,lease_ttl_seconds=20,kinds=KINDS)
        self.store.register_device('fixture',['claude.synthetic'])
        self.artifacts=ArtifactStore(self.config.artifact_root)
        ref=self.artifacts.put(canonical_json({'synthetic':True,'text':'hello'}))
        self.task=self.store.submit(TaskSpec('fixture','claude.synthetic',1,{'prompt_ref':ref,'model':'claude-fable-5-2'},
            (Ref('dispatch','prompt',ref[7:],'1',ref),),('claude.synthetic',),'synthetic','none',3))[0]

    def adapter(self,store=None,artifacts=None,**kwargs):
        return ClaudeAdapter(store or self.store,artifacts or self.artifacts,
            cli_command=[sys.executable,str(self.fake)],run_root=kwargs.get('run_root',self.root/'runs'),isolated_synthetic=True)

    def cli(self,*args):
        output=Output()
        with redirect_stdout(output):code=main(['--config',str(self.config_file),*args])
        data=json.loads(output.buffer.getvalue());self.assertEqual(code,0,data)
        return data['data']

    def finish(self):
        lease=self.store.claim('fixture')
        return self.adapter().run(lease)

    def target(self):
        config=replace(self.config,db_path=self.root/'restored.db',artifact_root=self.root/'restored-cas')
        return config,SqliteStore(config.db_path,lease_ttl_seconds=20,kinds=KINDS)

    def test_normal_cli_dedicated_route_receipt_and_trace(self):
        self.assertIsNone(self.cli('run-once')['task'])
        self.assertEqual(self.store.get_task(self.task.task_id).status,'queued')
        with patch('sa_dispatch.claude_adapter.ClaudeAdapter',side_effect=self.adapter):
            report=self.cli('claude-run','--executable',sys.executable,'--run-root',str(self.root/'runs'),'--isolated-synthetic')
        self.assertEqual(report['status'],'succeeded')
        receipt=self.cli('receipt',self.task.task_id)
        self.assertEqual(receipt['response']['session_id'],report['session_id'])
        trace=self.cli('trace',self.task.task_id)
        self.assertTrue(any(event['source']=='claude-adapter' for event in trace['events']))
        self.assertEqual(trace,self.cli('trace',self.task.task_id))

    def test_bundle_v4_includes_raw_init_and_result_but_not_execution_path(self):
        report=self.finish();bundle=self.root/'package'
        manifest=export_bundle(self.config,bundle)
        self.assertEqual(manifest['schema'],'sa_dispatch.bundle.v6')
        for field in ('prompt_ref','stdout_ref','init_ref'):self.assertIn(report[field],manifest['artifacts'])
        row=json.loads((bundle/'claude_attempts.jsonl').read_text())
        self.assertEqual(row['run_dir'],'');self.assertEqual(row['execution_authority'],'historical_only')
        config,target=self.target();import_bundle(config,bundle,target)
        restored=ledger(target.path)['claude_attempts'][0]
        self.assertEqual(restored['execution_authority'],'historical_only')
        self.assertEqual(ArtifactStore(config.artifact_root).get(report['stdout_ref']),self.artifacts.get(report['stdout_ref']))
        # Even a trusted local requeue does not grant an imported session authority.
        with target._tx() as conn:
            conn.execute("UPDATE tasks SET status='queued' WHERE task_id=?",(self.task.task_id,))
        target.register_device('fixture',['claude.synthetic']);lease=target.claim('fixture')
        with self.assertRaisesRegex(ClaudeError,'HISTORICAL|AUTHORITY'):
            self.adapter(target,ArtifactStore(config.artifact_root)).run(lease,resume=True)

    def test_unsettled_attempt_blocks_export_even_after_task_cancel(self):
        lease=self.store.claim('fixture');self.adapter()._prepare(lease,1,.01,False)
        with self.store._tx() as conn:conn.execute("UPDATE leases SET expires_at='2000-01-01' WHERE lease_id=?",(lease.lease_id,))
        self.store.sweep()
        self.store.cancel(self.task.task_id,'fixture')
        with self.assertRaises(ArtifactError):export_bundle(self.config,self.root/'blocked')

    def test_confirmed_timeout_history_can_migrate_after_explicit_resume(self):
        ref=self.artifacts.put(canonical_json({'synthetic':True,'text':'sleep'}))
        task=self.store.submit(TaskSpec('sleep','claude.synthetic',1,{'prompt_ref':ref,'model':'claude-fable-5-2'},
            (Ref('dispatch','prompt',ref[7:],'1',ref),),('claude.synthetic',),'synthetic','none',3))[0]
        self.store.cancel(self.task.task_id,'fixture')
        lease=self.store.claim('fixture');first=self.adapter().run(lease,timeout_s=.2)
        self.assertEqual(first['status'],'interrupted')
        with self.store._tx() as conn:conn.execute("UPDATE leases SET expires_at='2000-01-01' WHERE lease_id=?",(lease.lease_id,))
        self.store.sweep();second=self.store.claim('fixture')
        self.assertEqual(self.adapter().run(second,resume=True)['status'],'succeeded')
        bundle=self.root/'resumed';export_bundle(self.config,bundle)
        config,target=self.target();import_bundle(config,bundle,target)
        self.assertEqual([r['phase'] for r in ledger(target.path)['claude_attempts']],['interrupted','succeeded'])
        self.assertEqual(target.get_task(task.task_id).status,'succeeded')

    def test_wrong_report_binding_and_missing_child_rejected(self):
        report=self.finish();original=self.store.snapshot()['claude_attempts'][0]['report_ref']
        changed={**report,'task_id':'t_wrong'};bad=self.artifacts.put(receipt_bytes(changed))
        with self.store._tx() as conn:conn.execute('UPDATE claude_attempts SET report_ref=?',(bad,))
        with self.assertRaises(ArtifactError):export_bundle(self.config,self.root/'wrong')
        with self.store._tx() as conn:conn.execute('UPDATE claude_attempts SET report_ref=?',(original,))
        # Corrupting the only synthetic init blob must prevent an incomplete package.
        path=self.artifacts.root/report['init_ref'][7:9]/report['init_ref'][9:]
        if not path.exists():
            path=next(p for p in self.artifacts.root.rglob('*') if p.is_file() and sha256_ref(p.read_bytes())==report['init_ref'])
        path.write_bytes(b'broken')
        with self.assertRaises(ArtifactError):export_bundle(self.config,self.root/'missing')

    def test_required_child_references_cannot_be_omitted_from_success(self):
        report=self.finish()
        for field in ('stdout_ref','init_ref'):
            changed=dict(report);changed.pop(field)
            bad=self.artifacts.put(receipt_bytes(changed))
            with self.store._tx() as conn:conn.execute('UPDATE claude_attempts SET report_ref=?',(bad,))
            with self.assertRaises(ArtifactError):export_bundle(self.config,self.root/('omit-'+field))

    def test_v3_layout_remains_readable_without_claude_table(self):
        # Structural compatibility fixture; independently frozen v1/v2 tests remain separate.
        self.store.cancel(self.task.task_id,'fixture')
        source=ledger(self.store.path);rows={key:source[key] for key in V3_TABLES}
        rows['schema_migrations']=[r for r in rows['schema_migrations'] if r['version']!='0009']
        package=self.root/'v3';package.mkdir();files={}
        for name,records in rows.items():
            raw=b''.join(receipt_bytes(row)+b'\n' for row in records)
            filename=name+'.jsonl';(package/filename).write_bytes(raw);files[filename]=sha256_ref(raw)
        ref=self.task.input_refs[0]['hash'];ArtifactStore(package/'artifacts').put(self.artifacts.get(ref))
        manifest={'schema':V3_FORMAT,'files':files,'artifacts':[ref],
            'import_policy':{'schedules':'disabled','devices':'excluded'},
            'source_counts':{key:len(value) for key,value in rows.items()}}
        (package/'manifest.json').write_bytes(receipt_bytes(manifest))
        self.assertEqual(read_bundle(package)[0]['schema'],V3_FORMAT)
        config,target=self.target();import_bundle(config,package,target)
        self.assertEqual(target.get_task(self.task.task_id).status,'cancelled')
        self.assertEqual(ledger(target.path)['claude_attempts'],[])

if __name__=='__main__':unittest.main()
