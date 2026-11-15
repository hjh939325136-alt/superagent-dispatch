"""Collector tests use only temporary synthetic sources and real child processes."""
from contextlib import closing
from dataclasses import asdict,replace
import hashlib,json,os
from pathlib import Path
import sqlite3,subprocess,sys,tempfile,unittest
import zipfile
from types import SimpleNamespace
from unittest.mock import patch

from sa_dispatch.collector import Collector,SourceBinding,canonical,plain,readonly
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.bundle import export_bundle,import_bundle,ledger
from sa_dispatch.artifacts import ArtifactError
from sa_dispatch.models import TaskSpec

class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='collector-test-');self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.source=self.root/'source.db';self.target=self.root/'target.db'
        SqliteStore(self.target,lease_ttl_seconds=10)
        with closing(sqlite3.connect(self.source)) as c,c:
            c.executescript('CREATE TABLE collector_fixture_meta(key TEXT PRIMARY KEY,value TEXT);CREATE TABLE hook_events(seq INTEGER PRIMARY KEY,body TEXT);')
            c.executemany('INSERT INTO collector_fixture_meta VALUES(?,?)',[('schema','fixture-v1'),('instance','instance-one'),('synthetic_only','true')])
        self.add(1);self.add(2)
        self.binding=SourceBinding.enroll('synthetic-hook','fixture_hook',self.source,'instance-one',('fixture-v1',))
        self.collector=Collector(self.target)
    def add(self,seq,**extra):
        body={'schemaVersion':1,'at':'2026-11-15T00:00:00+00:00','event':'PreToolUse','mode':'audit','decision':'observed',**extra}
        with closing(sqlite3.connect(self.source)) as c,c:c.execute('INSERT INTO hook_events VALUES(?,?)',(seq,json.dumps(body)))
    def rows(self,table):
        with closing(sqlite3.connect(self.target)) as c,c:
            c.row_factory=sqlite3.Row;return [dict(r) for r in c.execute('SELECT * FROM '+table)]
    def sync(self,**kw):return self.collector.sync(self.binding,'hook_events',**kw)
    def test_atomic_idempotent_source_unchanged_and_redacted(self):
        with closing(sqlite3.connect(self.source)) as c,c:c.execute("UPDATE hook_events SET body=json_set(body,'$.reason',?) WHERE seq=1",('SENTINEL_SECRET_BODY',))
        before=self.source.read_bytes();mtime=self.source.stat().st_mtime_ns
        self.assertEqual(self.sync()['added'],2);self.assertEqual(self.sync()['added'],0)
        self.assertEqual(self.source.read_bytes(),before);self.assertEqual(self.source.stat().st_mtime_ns,mtime)
        self.assertNotIn('SENTINEL_SECRET_BODY',canonical(self.rows('unified_events')))
        self.assertEqual(self.rows('collector_cursors')[0]['last_seq'],2)
        self.assertEqual([r['source_id'] for r in self.rows('unified_events')],['hook_events:1','hook_events:2'])

    def test_same_file_large_batch_spills_rolls_back_and_keeps_anchor(self):
        at='2026-11-15T00:00:00+00:00'
        store=SqliteStore(self.target,lease_ttl_seconds=10)
        task,_=store.submit(TaskSpec('self-collect','fixture.self',1,{},(),(),'synthetic','none',5),side_effect='idempotent_write')
        with closing(sqlite3.connect(self.target)) as db,db:
            db.executemany('INSERT INTO events(task_id,from_status,to_status,fence,reason,actor,at) VALUES(?,?,?,?,?,?,?)',
                [(task.task_id,'queued','queued',0,'fixture','fixture',at)]*999)
        alias=self.root/'target-alias.db'
        os.link(self.target,alias)
        binding=SourceBinding.enroll('self-collect','dispatch',alias,'instance-one',('0012',))
        connect=self.collector._connect

        def small_cache():
            db=connect()
            db.execute('PRAGMA cache_size=8')
            db.execute('PRAGMA busy_timeout=10')
            self.assertGreater(db.execute('PRAGMA cache_spill').fetchone()[0],0)
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0],'delete')
            return db

        def fail_after_cursor(stage):
            if stage=='cursor_updated':raise RuntimeError('test rollback')

        with patch.object(self.collector,'_connect',side_effect=small_cache), \
                patch('sa_dispatch.collector.readonly',side_effect=AssertionError('same file opened twice')):
            with self.assertRaisesRegex(RuntimeError,'test rollback'):
                self.collector.sync(binding,'events',limit=1000,fault=fail_after_cursor)
            self.assertEqual(self.rows('unified_events'),[])
            self.assertEqual(self.rows('collector_cursors'),[])
            self.assertEqual(self.collector.sync(binding,'events',limit=1000)['added'],1000)
            self.assertEqual(self.collector.sync(binding,'events',limit=1000)['added'],0)
            with closing(sqlite3.connect(self.target)) as db,db:
                db.execute("UPDATE events SET reason='rewritten' WHERE seq=1000")
            self.assertEqual(self.collector.sync(binding,'events')['kind'],'source_rewritten')
            self.assertEqual(self.collector.adopt(binding,'events',reason='accept_rewrite',actor='operator',resume_after=1000)['epoch'],2)
        self.assertEqual(len(self.rows('unified_events')),1000)

    def test_relative_and_cross_os_paths_rejected_before_open(self):
        for path in ('relative.db','../source.db','//server/share/a.db',r'\\wsl$\Debian\source.db',r'\\server\share\source.db'):
            with self.assertRaises(ValueError):plain(path)

    def test_readonly_authorizer_denies_writes_and_unneeded_functions(self):
        with closing(readonly(self.source)) as source:
            self.assertEqual(source.execute('SELECT count(*) FROM hook_events').fetchone()[0],2)
            for sql in ("UPDATE hook_events SET body='x'","SELECT load_extension('never-open')","SELECT lower('x')",'COMMIT'):
                with self.assertRaises(sqlite3.DatabaseError):source.execute(sql)
    def test_missing_sequence_freezes_without_partial_projection(self):
        self.add(4);r=self.sync();self.assertEqual(r['kind'],'seq_gap')
        self.assertEqual(self.rows('unified_events'),[])
        self.add(3);self.assertEqual(self.sync()['state'],'frozen')
        self.collector.adopt(self.binding,'hook_events',reason='accept_gap',actor='operator')
        self.assertEqual(self.sync()['added'],4)
        self.assertEqual(len(self.rows('collector_gaps')),1)
    def test_anchor_rewrite_and_explicit_adoption_epoch(self):
        self.sync()
        with closing(sqlite3.connect(self.source)) as c,c:c.execute("UPDATE hook_events SET body=json_set(body,'$.reason','changed') WHERE seq=2")
        self.assertEqual(self.sync()['kind'],'source_rewritten')
        with self.assertRaises(ValueError):self.collector.adopt(self.binding,'hook_events',reason='free secret prose',actor='operator')
        self.collector.adopt(self.binding,'hook_events',reason='accept_rewrite',actor='operator',resume_after=2)
        self.add(3);self.assertEqual(self.sync()['epoch'],2)
        self.assertEqual(len(self.rows('unified_events')),3)
    def test_truncated_source_freezes(self):
        self.sync()
        with closing(sqlite3.connect(self.source)) as c,c:c.execute('DELETE FROM hook_events WHERE seq=2')
        self.assertEqual(self.sync()['kind'],'source_truncated')
    def test_same_path_replacement_is_new_instance(self):
        self.sync();replacement=self.root/'replacement.db'
        with closing(sqlite3.connect(self.source)) as a,closing(sqlite3.connect(replacement)) as b,b:a.backup(b)
        os.replace(replacement,self.source)
        self.assertEqual(self.sync()['kind'],'instance_changed')
        current=SourceBinding.enroll('synthetic-hook','fixture_hook',self.source,'instance-one',('fixture-v1',))
        self.collector.adopt(current,'hook_events',reason='accept_new_instance',actor='operator',resume_after=2)
        self.assertEqual(self.collector.sync(current,'hook_events')['added'],0)
    def test_schema_drift_freezes_and_requires_accepted_rebind(self):
        self.sync()
        with closing(sqlite3.connect(self.source)) as c,c:c.execute('ALTER TABLE hook_events ADD COLUMN new_field TEXT')
        self.assertEqual(self.sync()['kind'],'schema_changed')
        self.assertEqual(len(self.rows('unified_events')),2)
    def test_invalid_allowlist_fields_freeze_without_raw_body(self):
        with closing(sqlite3.connect(self.source)) as c,c:c.execute("UPDATE hook_events SET body=json_set(body,'$.decision','SENTINEL') WHERE seq=2")
        self.assertEqual(self.sync()['kind'],'redaction_rejected')
        self.assertEqual(self.rows('unified_events'),[])
        self.assertNotIn('SENTINEL',canonical(self.rows('collector_gaps')))
    def test_real_process_kill_rolls_back_events_and_cursor(self):
        binding=self.root/'binding.json';binding.write_text(json.dumps(asdict(self.binding)))
        script="""import json,os,sys
from sa_dispatch.collector import Collector,SourceBinding
b=SourceBinding(**json.load(open(sys.argv[2])))
Collector(sys.argv[1]).sync(b,'hook_events',fault=lambda stage:os._exit(73) if stage==sys.argv[3] else None)
"""
        for stage in ('events_inserted','cursor_updated'):
            p=subprocess.run([sys.executable,'-B','-c',script,str(self.target),str(binding),stage],env=dict(os.environ,PYTHONPATH=str(Path(__file__).parents[1])),capture_output=True,timeout=20)
            self.assertEqual(p.returncode,73,p.stderr)
            self.assertEqual(self.rows('unified_events'),[]);self.assertEqual(self.rows('collector_cursors'),[])
        self.assertEqual(self.sync()['added'],2);self.assertEqual(self.sync()['added'],0)
    def test_competing_processes_do_not_duplicate(self):
        binding=self.root/'binding.json';binding.write_text(json.dumps(asdict(self.binding)))
        script="import json,sys;from sa_dispatch.collector import Collector,SourceBinding;b=SourceBinding(**json.load(open(sys.argv[2])));print(Collector(sys.argv[1]).sync(b,'hook_events'))"
        args=[sys.executable,'-B','-c',script,str(self.target),str(binding)]
        env=dict(os.environ,PYTHONPATH=str(Path(__file__).parents[1]))
        children=[subprocess.Popen(args,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE) for _ in range(2)]
        for child in children:
            out,err=child.communicate(timeout=20);self.assertEqual(child.returncode,0,err)
        self.assertEqual(len(self.rows('unified_events')),2)
    def test_local_only_event_blocks_portable_bundle(self):
        self.binding=replace(self.binding,data_class='local_only');self.sync()
        cfg=SimpleNamespace(db_path=self.target,artifact_root=self.root/'cas')
        with self.assertRaises(ArtifactError):export_bundle(cfg,self.root/'blocked')
    def test_bundle_v6_retains_cursor_gaps_but_requires_adoption(self):
        self.sync();self.add(4);self.sync()
        cfg=SimpleNamespace(db_path=self.target,artifact_root=self.root/'cas');package=self.root/'bundle'
        export_bundle(cfg,package)
        dest=SimpleNamespace(db_path=self.root/'import.db',artifact_root=self.root/'import-cas')
        report=import_bundle(dest,package,SqliteStore(dest.db_path,lease_ttl_seconds=10))
        self.assertEqual(report['schema'],'sa_dispatch.bundle.v7')
        restored=ledger(dest.db_path)
        self.assertEqual(restored['collector_cursors'][0]['authority'],'historical_only')
        self.assertEqual(restored['collector_gaps'],self.rows('collector_gaps'))
        collector=Collector(dest.db_path);self.assertEqual(collector.sync(self.binding,'hook_events')['state'],'frozen')
        collector.adopt(self.binding,'hook_events',reason='bind_imported_history',actor='operator',resume_after=4)
        self.assertEqual(collector.sync(self.binding,'hook_events')['added'],0)
    def test_true_frozen_v5_import_preserves_history_no_collector_rows(self):
        base=Path(__file__).parent/'fixtures/legacy_bundles';proof=json.loads((base/'provenance-v5.json').read_bytes())
        self.assertEqual(hashlib.sha256((base/'runtime-v5.zip').read_bytes()).hexdigest(),proof['source_archive_sha256'])
        for name,h in proof['fixture_files'].items():self.assertEqual(hashlib.sha256((base/'v5'/name).read_bytes()).hexdigest(),h)
        dest=SimpleNamespace(db_path=self.root/'legacy.db',artifact_root=self.root/'legacy-cas')
        import_bundle(dest,base/'v5',SqliteStore(dest.db_path,lease_ttl_seconds=10))
        restored=ledger(dest.db_path);self.assertEqual(restored['collector_cursors'],[])
        self.assertEqual(restored['collector_gaps'],[])
        self.assertIn('0011',[r['version'] for r in restored['schema_migrations']])

    def test_cli_only_selects_pinned_sources_and_adopt_is_explicit(self):
        binding=self.root/'trusted.json'
        binding.write_text(json.dumps({'schema':'sa_dispatch.collector_binding.v1','target':str(self.target),'sources':[asdict(self.binding)]}))
        def call(command,request):
            return subprocess.run([sys.executable,'-B','-m','sa_dispatch.collector_cli','--binding',str(binding),command],
                input=json.dumps(request),text=True,capture_output=True,env=dict(os.environ,PYTHONPATH=str(Path(__file__).parents[1])),timeout=20)
        request={'source_id':self.binding.source_id,'stream':'hook_events'}
        bad=call('sync',{**request,'locator':'not-allowed'})
        self.assertEqual(json.loads(bad.stdout)['error'],'E_FIELD_NOT_ALLOWED')
        self.assertEqual(self.rows('unified_events'),[])
        good=call('sync',request);self.assertEqual(good.returncode,0,good.stdout)
        self.assertEqual(json.loads(good.stdout)['result']['added'],2)
        self.add(4);self.assertEqual(json.loads(call('sync',request).stdout)['result']['state'],'frozen')
        adopted=call('adopt',{**request,'reason':'accept_gap','actor':'operator','resume_after':4})
        self.assertEqual(adopted.returncode,0,adopted.stdout)
        self.assertEqual(json.loads(adopted.stdout)['result']['epoch'],2)

    def test_genuine_0010_database_upgrades_additively(self):
        base=Path(__file__).parent/'fixtures/legacy_bundles';old=self.root/'old-runtime';old.mkdir()
        with zipfile.ZipFile(base/'runtime-v5.zip') as z:z.extractall(old)
        path=self.root/'old.db';cas=self.root/'old-cas'
        script="""from pathlib import Path
from types import SimpleNamespace
import json,sys
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.bundle import import_bundle,ledger,FORMAT
assert FORMAT=='sa_dispatch.bundle.v5'
c=SimpleNamespace(db_path=Path(sys.argv[1]),artifact_root=Path(sys.argv[2]))
import_bundle(c,Path(sys.argv[3]),SqliteStore(c.db_path,lease_ttl_seconds=10))
print(json.dumps(ledger(c.db_path)))
"""
        child=subprocess.run([sys.executable,'-B','-c',script,str(path),str(cas),str((base/'v5').resolve())],cwd=old,env=dict(os.environ,PYTHONPATH=str(old)),capture_output=True,text=True,timeout=20)
        self.assertEqual(child.returncode,0,child.stderr);before=json.loads(child.stdout)
        self.assertEqual(before['schema_migrations'][-1]['version'],'0010')
        SqliteStore(path,lease_ttl_seconds=10);after=ledger(path)
        for name,rows in before.items():
            if name in ('tasks','schedules','schedule_firings'):
                rows=[dict(row,execution_route=None) for row in rows]
            self.assertEqual(after[name][:len(rows)] if name=='schema_migrations' else after[name],rows,name)
        self.assertEqual([row['version'] for row in after['schema_migrations'][len(before['schema_migrations']):]],
                         ['0011','0012'])
        self.assertEqual(after['collector_cursors'],[]);self.assertEqual(after['collector_gaps'],[])

    def test_real_dispatch_streams_project_only_metadata(self):
        path=self.root/'dispatch-source.db';store=SqliteStore(path,lease_ttl_seconds=10)
        task,_=store.submit(TaskSpec('synthetic-key','fixture.read',1,{},(),(),'synthetic','none',2))
        at='2026-11-15T00:00:00+00:00';ref='sha256:'+'a'*64
        with closing(sqlite3.connect(path)) as c,c:
            c.execute('INSERT INTO control_events VALUES(1,?,?,?,?,?,?,?,?,?)',('ev-one','ev-key','budget.exceeded',task.task_id,0,'fixture',at,'[]','redact-me'))
            c.execute('INSERT INTO schedule_events(schedule_id,version,kind,at,detail_code) VALUES(?,?,?,?,?)',('fixture',1,'schedule.exhausted',at,'max_slots_reached'))
            c.execute('INSERT INTO reconciliations VALUES(?,?,?,?,?,?,?,?)',('rec-one',task.task_id,0,'human','decided',ref,'fixture',at))
            c.execute('INSERT INTO host_events VALUES(?,?,?,?)',(task.task_id,0,1,ref))
        binding=SourceBinding.enroll('dispatch-fixture','dispatch',path,'trusted-ledger-one',('0012',))
        before=path.read_bytes()
        for stream in ('events','control_events','schedule_events','reconciliations','host_events'):
            result=self.collector.sync(binding,stream)
            self.assertEqual(result['state'],'synced',result);self.assertEqual(result['added'],1)
        self.assertEqual(path.read_bytes(),before)
        self.assertEqual(len(self.rows('unified_events')),5)
        self.assertNotIn('redact-me',canonical(self.rows('unified_events')))

    def test_real_research_catalog_changes_and_missing_signal_changes(self):
        knowledge=Path(os.environ.get('SA_COLLECTOR_KNOWLEDGE_ROOT',str(Path(__file__).resolve().parents[2]/'knowledge/m0_search')))
        sys.path.insert(0,str(knowledge));self.addCleanup(lambda:sys.path.remove(str(knowledge)))
        from catalog import initialize,MARKER
        from catalog import OPERATOR
        root=self.root/'catalog'
        result=initialize(root,operator_grant=OPERATOR)
        for domain in ('research-scatter','research-content'):
            source=root/(domain+'.sqlite3')
            binding=SourceBinding.enroll(domain,'catalog',source,result['instance'],(MARKER,))
            output=self.collector.sync(binding,'changes')
            self.assertEqual(output['state'],'synced',output);self.assertEqual(output['added'],30)
        binding=SourceBinding.enroll('signalhub','catalog',root/'signalhub.sqlite3',result['instance'],('m0-signalhub-summary-fixture-v1',))
        before=self.target.read_bytes()
        snapshot=self.collector.sync(replace(binding,collection_mode='snapshot_only'),'changes')
        self.assertEqual(snapshot['incremental'],'unsupported')
        self.assertEqual(self.target.read_bytes(),before)
        self.assertEqual(self.collector.sync(binding,'changes')['kind'],'schema_changed')
        self.assertEqual(len(self.rows('unified_events')),60)

    def test_filesystem_gate_precedes_sqlite_open(self):
        from unittest.mock import patch
        from pathlib import PurePosixPath
        from sa_dispatch import collector as module
        for fs in ('9p','drvfs','cifs','nfs','ntfs','fuseblk'):
            mounts='1 0 0:1 / / rw - ext4 root rw\n2 1 0:2 / /mnt/host rw - '+fs+' host rw'
            with patch.object(module.sys,'platform','linux'),patch.object(module,'plain',return_value=PurePosixPath('/mnt/host/source.db')),patch.object(module,'Path',PurePosixPath),patch.object(module,'_mount_info',return_value=mounts),patch.object(module.sqlite3,'connect') as connect:
                with self.assertRaises(ValueError):readonly(self.source)
                with self.assertRaises(ValueError):self.collector._connect()
                connect.assert_not_called()
        with patch.object(module.sys,'platform','win32'),patch.object(module,'_windows_drive_type',return_value=4),patch.object(module.sqlite3,'connect') as connect:
            with self.assertRaises(ValueError):readonly(self.source)
            connect.assert_not_called()

    def test_snapshot_only_validates_without_target_open(self):
        from unittest.mock import patch
        binding=replace(self.binding,collection_mode='snapshot_only')
        self.assertNotEqual(binding.fingerprint(),self.binding.fingerprint())
        before=self.target.read_bytes()
        with patch.object(self.collector,'_connect',side_effect=AssertionError('target opened')):
            result=self.collector.sync(binding,'hook_events')
            self.assertEqual(result['incremental'],'unsupported')
            with self.assertRaises(LookupError):self.collector.sync(replace(binding,source_instance='wrong'),'hook_events')
            with self.assertRaises(ValueError):self.collector.sync(replace(binding,accepted_schema_versions=('wrong',)),'hook_events')
            with self.assertRaises(ValueError):self.collector.sync(replace(binding,file_identity='wrong'),'hook_events')
        self.assertEqual(self.target.read_bytes(),before)
        self.assertEqual(self.rows('collector_cursors'),[])
        self.assertEqual(self.rows('collector_gaps'),[])

    def test_old_cli_binding_defaults_incremental_unknown_field_rejected(self):
        from sa_dispatch.collector_cli import load_binding
        row=asdict(self.binding);row.pop('collection_mode')
        value={'schema':'sa_dispatch.collector_binding.v1','target':str(self.target),'sources':[row]}
        path=self.root/'old-binding.json';path.write_text(json.dumps(value),encoding='utf-8')
        self.assertEqual(load_binding(path)[1][self.binding.source_id].collection_mode,'incremental')
        row['unexpected']=True;path.write_text(json.dumps(value),encoding='utf-8')
        with self.assertRaises(ValueError):load_binding(path)

if __name__=='__main__':unittest.main()
