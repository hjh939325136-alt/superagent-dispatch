"""Create NEW, bounded synthetic data; no discovery or adoption of existing data."""
from contextlib import closing
from dataclasses import asdict
from datetime import timedelta
import os
from pathlib import Path
import sqlite3
import uuid
from .common import (APP_ID,GRANT,MAX_CAS,MAX_LAB,canonical,cas_path,digest,document,
    fail,identity,instant,now,quota,sealed,secure_mkdir,target_lock,write,directory_handle)
from .binding import load_binding,require_fixture
from .plan import plan_retention,ledger_state
from .apply import apply_fixture,recover_fixture
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.models import TaskSpec,Ref
from sa_dispatch.collector import SourceBinding,Collector
from sa_dispatch.clock import FakeClock

def create_fixture(new_root):
    root=Path(new_root)
    if not root.is_absolute() or '..' in root.parts or root.exists():fail('E_NEW_ROOT_REQUIRED')
    # Only an already-existing local parent can host this new exclusive root.
    with directory_handle(root.parent):os.mkdir(root)
    for name in ('cas','journals','sources','plans','operations'):secure_mkdir(root/name)
    instance=uuid.uuid4().hex;at=now();old=(instant(at)-timedelta(days=200)).isoformat()
    generations={};objects={}
    def blob(label,value=None,age=60):
        value=value or {'schema':'m0.retention_leaf.v1','synthetic_only':True,'label':label}
        data=canonical(value);ref='sha256:'+digest(value);write(root,cas_path(root/'cas',ref),data)
        if age is not None:generations[ref]={'created_at':(instant(at)-timedelta(days=age)).isoformat(),'bytes':len(data)}
        objects[label]=ref;return ref
    prompt=blob('prompt',{'synthetic':True,'text':'synthetic retention probe'})
    stdout=blob('stdout');init=blob('init');exit_ref=blob('exit',{'schema':'sa_dispatch.claude_exit.v1','synthetic_only':True})
    launch=blob('launch',{'schema':'sa_dispatch.claude_launch.v1','synthetic_only':True})
    report=blob('claude_report',{'schema':'sa_dispatch.claude_report.v1','prompt_ref':prompt,'stdout_ref':stdout,
        'init_ref':init,'exit_ref':exit_ref,'launch_ref':launch})
    terminal=blob('terminal',{'schema':'sa_dispatch.claude_terminal.v1','prompt_ref':prompt,'stdout_ref':stdout,
        'init_ref':init,'exit_ref':exit_ref,'launch_ref':launch})
    usage=blob('usage',{'schema':'sa_dispatch.claude_run_usage.v1','terminal_ref':terminal})
    hostevent=blob('hostevent');hostusage=blob('hostusage',{'schema':'sa_dispatch.host_run_usage.v1','terminal_event_ref':hostevent})
    evidence=blob('evidence');inputref=blob('input');journal=blob('journal');consumer=blob('consumer')
    proofchildren=[blob('proof'+str(i)) for i in range(4)]
    proof=blob('proof',{'schema':'sa_dispatch.idempotency_proof.v1',**dict(zip(
        ('first_result_hash','second_result_hash','state_after_first_ref','state_after_second_ref'),proofchildren))})
    lookup=blob('lookup');reconciliation=blob('reconciliation',{'schema':'sa_dispatch.reconciliation.v1','lookup_ref':lookup})
    orphan=blob('old_orphan');blob('new',age=1);blob('age_unknown',age=None)
    store=SqliteStore(root/'ledger.sqlite3',lease_ttl_seconds=10)
    task,_=store.submit(TaskSpec('retention-unknown','fixture.retention',1,{},
        (Ref('fixture','input','input','1',inputref),),(),'synthetic','none',1),side_effect='external')
    with closing(store._connect()) as db,db:
        db.execute('PRAGMA application_id='+str(APP_ID));db.execute('PRAGMA journal_mode=DELETE')
        db.execute("UPDATE tasks SET status='unknown',result_json=?,created_at=?,updated_at=? WHERE task_id=?",
            (canonical({'claude_report_ref':report}).decode(),old,old,task.task_id))
        for i,ref in enumerate((usage,hostusage,evidence,proof,reconciliation)):
            db.execute('INSERT INTO evidence VALUES(?,?,?,?,?,?,?,?,?)',('fixture-evidence-'+str(i),task.task_id,1,
                'fixture','null',ref,ref,'synthetic',old))
        db.execute('INSERT INTO control_handoffs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ('retention-handoff','fixture-from','fixture-to','fixture','read_status','[]','fixture',evidence,old,'offered',None,'[]',old,old))
    source=root/'sources'/'hook.sqlite3'
    with closing(sqlite3.connect(source)) as db,db:
        db.execute('CREATE TABLE collector_fixture_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        db.executemany('INSERT INTO collector_fixture_meta VALUES(?,?)',[('schema','retention-hook-v1'),('instance',instance),('synthetic_only','true')])
        db.execute('CREATE TABLE hook_events(seq INTEGER PRIMARY KEY,body TEXT NOT NULL)')
        for seq in range(1,5):
            body={'schemaVersion':1,'at':old,'event':'PreToolUse','mode':'audit','decision':'allow'}
            db.execute('INSERT INTO hook_events VALUES(?,?)',(seq,canonical(body).decode()))
    sourcebinding=SourceBinding.enroll('retention-hook','fixture_hook',source,instance,('retention-hook-v1',))
    Collector(store.path).sync(sourcebinding,'hook_events')
    # Pin one old projection and leave the highest sequence old on purpose.
    with closing(store._connect()) as db,db:
        db.execute('UPDATE unified_events SET refs_json=? WHERE seq=2',
            (canonical(['fixture/artifact/consumer@1#'+consumer[7:]]).decode(),))
    write(root,root/'journals'/'fixture.jsonl',canonical({'phase':'intent','request_ref':journal})+b'\n')
    document(root,root/'generations.json',{'schema':'m0.retention_generations.v1','instance':instance,'objects':generations})
    document(root,root/'consumers.json',{'schema':'m0.retention_consumers.v1','instance':instance,'complete':True,'refs':[consumer]})
    with closing(store._connect()) as db:schema_hash=digest(ledger_state(db)['schema'])
    marker=sealed({'schema':'m0.retention_fixture.v1','instance':instance,'synthetic_only':True,'created_new':True,
        'producer_protocol':'closed-local-fixture-v1','quota_bytes':MAX_LAB,'cas_quota_bytes':MAX_CAS,'grant':GRANT,
        'created_at':at,'objects':objects,'source_task_id':task.task_id,'ledger_schema_sha256':schema_hash,
        'generated_objects':{ref:{**gen,'identity':identity(cas_path(root/'cas',ref))}
                             for ref,gen in generations.items()}})
    document(root,root/'fixture.json',marker)
    document(root,root/'binding.json',{'schema':'m0.retention_binding.v1','mode':'fixture','root':str(root),'instance':instance,
        'ledger_identity':identity(store.path),'marker_sha256':digest(marker),'sources':[{**asdict(sourcebinding),'stream':'hook_events'}]})
    quota(root)
    return root/'binding.json'

def append_hook(binding):
    require_fixture(binding,GRANT)
    with target_lock(binding.root):
        entry=binding.sources[0];bound=SourceBinding(**{k:v for k,v in entry.items() if k!='stream'})
        with closing(sqlite3.connect(bound.locator)) as db,db:
            body={'schemaVersion':1,'at':now(),'event':'PreToolUse','mode':'audit','decision':'allow'}
            db.execute('INSERT INTO hook_events(body) VALUES(?)',(canonical(body).decode(),))
        return Collector(binding.ledger).sync(bound,entry['stream'])

def run(new_root):
    binding=load_binding(create_fixture(new_root));plan=plan_retention(binding)
    planpath=binding.root/'plans'/'initial.json';document(binding.root,planpath,plan)
    receipt=apply_fixture(binding,planpath,GRANT)
    repeated=apply_fixture(binding,planpath,GRANT)
    from .common import readonly_db,read,sha
    with readonly_db(binding.ledger) as db:
        post=ledger_state(db);high=db.execute('SELECT MAX(seq) FROM unified_events').fetchone()[0]
    absent=all(not cas_path(binding.cas,item['ref']).exists() for item in plan['blobs'])
    entry=binding.sources[0];bound=SourceBinding(**{k:v for k,v in entry.items() if k!='stream'})
    cursor=post['rows']['collector_cursors'][0]
    source_path=Path(bound.locator);source_before=(sha(read(source_path)),source_path.stat().st_mtime_ns)
    # Empty sync normally writes updated_at. Pin its clock to the existing value
    # to test replay on the actual deleted state without invalidating recovery.
    sync=Collector(binding.ledger,clock=FakeClock(cursor['updated_at'])).sync(bound,entry['stream'])
    with readonly_db(binding.ledger) as db:
        after_sync=ledger_state(db)
        candidate_absent=all(db.execute('SELECT 1 FROM unified_events WHERE seq=? OR event_id=?',
                                         (e['seq'],e['event_id'])).fetchone() is None for e in plan['events'])
    cursor_unchanged=after_sync['rows']['collector_cursors']==[cursor]
    recovery=recover_fixture(binding,receipt['operation_dir'],GRANT)
    source_unchanged=source_before==(sha(read(source_path)),source_path.stat().st_mtime_ns)
    retained_verified=sum(sha(read(cas_path(binding.cas,ref)))==ref[7:] for ref in plan['pins'])
    append=append_hook(binding)
    with readonly_db(binding.ledger) as db:newhigh=db.execute('SELECT MAX(seq) FROM unified_events').fetchone()[0]
    result={'schema':'m0.retention_lab.v1','instance':binding.instance,'binding':str(binding.path),
        'plan_sha256':plan['sha256'],'candidate_events':len(plan['events']),'candidate_blobs':len(plan['blobs']),
        'coverage':plan['coverage'],'unknown':plan['unknown'],'actual_unlink':absent,'apply':receipt,
        'repeat_exact':repeated==receipt,'recovery':recovery,'sync_added':sync['added'],
        'candidate_events_still_absent_after_sync':candidate_absent,'cursor_unchanged_after_sync':cursor_unchanged,
        'source_bytes_and_mtime_unchanged_by_retention':source_unchanged,'retained_refs_verified':retained_verified,
        'append_added':append['added'],'high_seq_preserved':high==plan['high_seq'],'new_seq_monotonic':newhigh>high,
        'bytes':quota(binding.root),'quota_bytes':MAX_LAB,'cas_quota_bytes':MAX_CAS,
        'production_apply':False,'source_tables_deleted':False,'vacuum':False,'power_loss_tested':False}
    if not (absent and candidate_absent and cursor_unchanged and source_unchanged and
            retained_verified==len(plan['pins']) and result['repeat_exact'] and result['high_seq_preserved'] and
            result['new_seq_monotonic'] and sync['added']==0 and append['added']==1):fail('E_LAB_VERIFY')
    return result
