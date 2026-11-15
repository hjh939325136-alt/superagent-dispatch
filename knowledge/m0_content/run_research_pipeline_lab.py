"""Isolated normal-CLI acceptance: research -> context -> synthetic script -> approval.

Never opens business sources or starts a service. Fixture initialization and an
explicit synthetic operator proof are the only out-of-band preparation steps.
"""
import argparse
from contextlib import closing
from datetime import datetime,timedelta,timezone
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import platform
import sqlite3
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]
for path in (ROOT,ROOT/'dispatch',ROOT/'knowledge/m0_search'):
    if str(path) not in sys.path:sys.path.insert(0,str(path))
from knowledge.m0_content.workflow_binding import initialize_binding,search_modules,request_base,canonical
from knowledge.m0_content.workflow_content import initialize_workflow
from knowledge.m0_content.contract import Contract
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.collector import SourceBinding
from tools.m0_manager.manager import run as manager_query


def write(path,value):
    path.write_bytes(json.dumps(value,ensure_ascii=False,indent=2).encode('utf-8')+b'\n')


def now():return datetime.now(timezone.utc).isoformat()


def prepare(work):
    bound,catalog,_,workflow=search_modules()
    catalog.initialize(work/'research',operator_grant=catalog.OPERATOR)
    bound.initialize_binding(work/'research',work/'research-binding.json')
    research=bound.load_binding(work/'research-binding.json')
    workflow.initialize_receipts(research,operator_grant=catalog.OPERATOR)
    with closing(Contract(work/'content.sqlite3',create=True)):pass
    initialize_workflow(work/'content.sqlite3')
    binding=initialize_binding(research.path,work/'content.sqlite3',work/'workflow-binding.json')
    values={'db_path':str(work/'ledger.sqlite3'),'artifact_root':str(work/'artifacts'),
            'journal_dir':str(work/'journal'),'device_id':'research-content-fixture',
            'capabilities':['research:read','signalhub:refs'],'lease_ttl_seconds':30,
            'component_timeout_seconds':20,'plugins':['research','research_content'],'synthetic':True}
    lines=[f'{key} = {json.dumps(value)}' for key,value in values.items()]
    for name,script,flag,path in (
            ('research',ROOT/'knowledge/m0_search/dispatch_component.py','--source-binding',research.path),
            ('research_content',ROOT/'knowledge/m0_content/dispatch_component.py','--workflow-binding',binding.path)):
        lines += ['[components.'+name+']','platform = "any"',
                  'argv = '+json.dumps([sys.executable,'-B',str(script),flag,str(path)])]
    config=work/'dispatch.toml';config.write_text('\n'.join(lines)+'\n',encoding='utf-8')
    return binding,config


def fixed_input(ref):
    owner,kind,rest=ref.split('/',2);ident,revision=rest.split('@',1);version,sha=revision.split('#',1)
    return {'owner':owner,'kind':kind,'id':ident,'version':version,'hash':sha if sha.startswith('sha256:') else 'sha256:'+sha}


class Commands:
    def __init__(self,work,config):self.work,self.config,self.history=work,config,[]
    def cli(self,*args,ok=True,crash=False):
        env=dict(os.environ)
        env.pop('M0_RESEARCH_CONTENT_TEST_CRASH',None)
        if crash:env['M0_RESEARCH_CONTENT_TEST_CRASH']='after_commit'
        result=subprocess.run([sys.executable,'-B',str(Path(__file__).with_name('dispatch_entry.py')),
                               '--config',str(self.config),*map(str,args)],capture_output=True,env=env,timeout=45)
        try:parsed=json.loads(result.stdout)
        except ValueError:raise AssertionError((args,result.returncode,result.stdout[-2000:],result.stderr[-2000:]))
        self.history.append({'args':list(map(str,args)),'exit':result.returncode,'response':parsed,'crash_after_commit':crash})
        write(self.work/'commands.json',self.history)
        if ok:
            assert result.returncode==0 and parsed['ok'],parsed
            return parsed['data']
        assert result.returncode!=0 and not parsed['ok'],parsed
        return parsed
    def submit(self,kind,params,key,*,parent=None,refs=()):
        spec={'idem_key':key,'kind':kind,'kind_version':1,'params':params,
              'required_caps':['research:read','signalhub:refs'],'input_refs':[fixed_input(ref) for ref in refs],
              'access':'synthetic','egress':'none','max_attempts':1,'track':'synthetic-research-content',
              'parent_task_id':parent}
        path=self.work/(key+'.json');write(path,spec)
        return self.cli('submit','--input',path)['task']
    def execute(self,task,*,expected='succeeded',crash=False):
        result=self.cli('run-once',crash=crash)['task']
        assert result and result['task_id']==task['task_id'] and result['status']==expected,result
        self.cli('trace',task['task_id'])
        if expected=='succeeded':return self.receipt(task)
        return result
    def receipt(self,task):
        response=self.cli('receipt',task['task_id'])['response']
        if response.get('data',{}).get('state')=='committed':response=response['data']['response']
        return response['data']
    def control(self,operation,params):
        path=self.work/('control-'+str(len(self.history))+'.json')
        write(path,{'schema':'sa_dispatch.control_request.v1','synthetic':True,'operation':operation,'params':params})
        return self.cli('control','--input',path)


def manager_binding(work,binding):
    info=(work/'ledger.sqlite3').stat()
    def source(id,kind,path,version,instance,options):
        return {'source_id':id,'kind':kind,'locator':str(path),'accepted_schema_versions':[version],
                'source_instance':instance,'class':'internal','freshness_s':86400,'options':options}
    sources=[source('workflow-dispatch','dispatch',work/'ledger.sqlite3','0011','workflow-dispatch',
                    {'identity':{'device':info.st_dev,'inode':info.st_ino}}),
             source('workflow-knowledge','knowledge',binding.research.path,'m0-synthetic-research-catalog-v1',
                    'workflow-knowledge',{'binding_sha256':binding.research.digest,'catalog_instance':binding.research.instance}),
             source('workflow-content','content',binding.content_path,'m0-content-synthetic-v1',binding.content_instance,
                    {'trace_ids':list(binding.allowed_traces),'workflow_binding':str(binding.path)})]
    path=work/'manager-binding.json';write(path,{'version':1,'sources':sources});return path


def counts(path,tables):
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
        return {table:db.execute('SELECT count(*) FROM '+table).fetchone()[0] for table in tables}


def snapshot(paths):
    result={}
    for path in paths:
        for selected in (path,Path(str(path)+'-wal'),Path(str(path)+'-shm'),Path(str(path)+'-journal')):
            result[str(selected)]={'sha256':hashlib.sha256(selected.read_bytes()).hexdigest(),'mtime_ns':selected.stat().st_mtime_ns} if selected.exists() else None
    return result


def sync_events(work,binding):
    sources=[SourceBinding.enroll('workflow-dispatch','dispatch',work/'ledger.sqlite3','workflow-dispatch',('0011',))]
    for domain in ('research-scatter','research-content'):
        sources.append(SourceBinding.enroll(domain,'catalog',binding.research.catalog_root/(domain+'.sqlite3'),
                                           binding.research.instance,('m0-synthetic-research-catalog-v1',)))
    trusted=work/'collector-binding.json'
    write(trusted,{'schema':'sa_dispatch.collector_binding.v1','target':str(work/'ledger.sqlite3'),
                   'sources':[asdict(source) for source in sources]})
    calls=[]
    for source in sources:
        for stream in (('events','control_events','reconciliations') if source.kind=='dispatch' else ('changes',)):
            for iteration in range(2):
                child=subprocess.run([sys.executable,'-B','-m','tools.m0_manager','sync','--binding',str(trusted)],
                    input=canonical({'source_id':source.source_id,'stream':stream}),cwd=ROOT,
                    capture_output=True,timeout=30)
                result=json.loads(child.stdout)
                assert child.returncode==0 and result['ok'],result
                if iteration:assert result['result']['added']==0,result
                calls.append({'source_id':source.source_id,'stream':stream,'iteration':iteration,'response':result})
    write(work/'sync-results.json',calls)
    return {'calls':len(calls),'replay_added':0,'streams':['events','control_events','reconciliations','changes'],
            'content_events_collected':False,'signalhub':'snapshot_only'}


def run(work):
    binding,config=prepare(work);commands=Commands(work,config)
    commands.cli('init-db');commands.cli('device-register')
    manager=manager_binding(work,binding)
    base=request_base(now());wf={'workflow_binding':binding.digest}
    research_task=commands.submit('research.search',base|{'q':'剪辑','limit':20},'search')
    found=commands.execute(research_task)
    selected=[next(row['ref'] for row in found['results'] if row['ref'].startswith(domain+'/') and row['status']=='current')
              for domain in base['domains']]
    for index,ref in enumerate(selected):
        task=commands.submit('research.get',base|{'ref':ref},'read-'+str(index),parent=research_task['task_id'],refs=[ref])
        assert commands.execute(task)['result']['ref']==ref
    context_task=commands.submit('context.assemble',wf|base|{'refs':selected},'assemble',parent=research_task['task_id'],refs=selected)
    context=commands.execute(context_task)
    assert sorted(context['refs'])==sorted(selected),context
    common=wf|{'trace_id':'synthetic-content-game','context_id':context['context_id'],'manifest_sha256':context['manifest_sha256']}
    script_task=commands.submit('content.script_from_context',common|{'template_id':binding.template_id},'script',parent=context_task['task_id'],refs=selected)
    script=commands.execute(script_task)
    assert script['generation']=='template_synthetic' and script['ai_generated'] is False
    # A second task may reuse the script, but needs its own durable operation receipt.
    before=counts(binding.content_path,['artifacts','approvals','content_events','operation_receipts'])
    alias_task=commands.submit('content.script_from_context',common|{'template_id':binding.template_id},'script-alias',parent=context_task['task_id'],refs=selected)
    alias=commands.execute(alias_task)
    assert alias['script_ref']==script['script_ref']
    after=counts(binding.content_path,before)
    assert all(after[k]==v+(k=='operation_receipts') for k,v in before.items()),(before,after)
    approval_task=commands.submit('content.approval_record',common|{'script_ref':script['script_ref']},'approval',parent=script_task['task_id'],refs=[script['script_ref']])
    commands.control('bind_task',{'task_id':approval_task['task_id'],'approval_scope':'script_ready'})
    assert commands.cli('run-once')['task'] is None
    assert counts(binding.content_path,['approvals'])['approvals']==0
    attention=manager_query({'op':'attention.list','role':'manager','args':{}},manager)
    assert any(row['rule']=='A4@1' and row.get('subject_id')==approval_task['task_id'] for row in attention['items']),attention
    write(work/'attention-pending.json',attention)
    expiry=(datetime.now(timezone.utc)+timedelta(hours=2)).isoformat()
    proof={'schema':'m0.synthetic_operator_approval.v1','synthetic':True,'task_id':approval_task['task_id'],
           'fingerprint':approval_task['fingerprint'],'scope':'script_ready','decision':'approved',
           'actor':'synthetic-operator','expires_at':expiry,'human_reviewed':False,'publish_authorized':False}
    proof_ref=ArtifactStore(work/'artifacts').put(receipt_bytes(proof))
    approval_decision=commands.control('approval_decide',{'task_id':approval_task['task_id'],'scope':'script_ready',
          'decision':'approved','actor':'synthetic-operator','evidence_ref':proof_ref,'expires_at':expiry})
    approved=commands.execute(approval_task)
    assert approved['control_approval']['proof_hash']==proof_ref
    state_params=common|{'script_ref':script['script_ref'],'approval_ref':approved['approval_ref']}
    status_task=commands.submit('content.approval_status',state_params,'approval-status',parent=approval_task['task_id'],refs=[approved['approval_ref']])
    current=commands.execute(status_task)
    assert current['usable'] is True,current
    # No control rule or proof must not turn a second task into an approved alias.
    unapproved=commands.submit('content.approval_record',common|{'script_ref':script['script_ref']},'approval-unapproved')
    commands.execute(unapproved,expected='failed')
    assert counts(binding.content_path,['approvals'])['approvals']==1
    # Commit with lost stdout/ack: only lookup can settle; version count stays fixed.
    fault_common=common|{'trace_id':'synthetic-content-ai-editing'}
    fault=commands.submit('content.script_from_context',fault_common|{'template_id':binding.template_id},'script-lost-ack',refs=selected)
    commands.execute(fault,expected='unknown',crash=True)
    fault_counts=counts(binding.content_path,['artifacts','approvals','content_events','operation_receipts'])
    _,catalog,_,_=search_modules()
    revocation=catalog.Catalog(binding.research.catalog_root).apply_batch('research-scatter','pipeline-revoke','revoke_locked',operator_grant=catalog.OPERATOR)
    reconciliation=commands.cli('reconcile',fault['task_id'])
    assert reconciliation['verdict']=='confirmed_done' and reconciliation['current_usable'] is False,reconciliation
    assert counts(binding.content_path,fault_counts)==fault_counts
    # Historical completion does not reactivate the old context or approval.
    status_after=commands.submit('content.approval_status',state_params,'approval-status-revoked',parent=approval_task['task_id'])
    stale=commands.execute(status_after)
    assert stale['usable'] is False,stale
    rejected=commands.submit('content.script_from_context',common|{'template_id':binding.template_id},'script-after-revoke')
    rejection=commands.execute(rejected,expected='failed')
    assert rejection['result']['commit_state']=='not_started',rejection
    paths=[work/'ledger.sqlite3',binding.content_path,*binding.research.catalog_root.glob('*.sqlite3')]
    before_read=snapshot(paths)
    attention=manager_query({'op':'attention.list','role':'manager','args':{}},manager)
    assert any(row['rule']=='A12@1' for row in attention['items']),attention
    content_state=manager_query({'op':'content.status','role':'manager','args':{'source_id':'workflow-content'}},manager)
    approvals=[approval for item in content_state['items'] for approval in item.get('approvals',[])]
    assert len(approvals)==1 and approvals[0]['ref']==approved['approval_ref'] and approvals[0]['usable'] is False
    context_state=manager_query({'op':'context.status','role':'manager','args':{'source_id':'workflow-knowledge','context_id':context['context_id']}},manager)
    assert snapshot(paths)==before_read,'Manager read changed a database or sidecar'
    write(work/'attention-revoked.json',attention);write(work/'content-revoked.json',content_state)
    write(work/'context-revoked.json',context_state)
    # An append-only rollback creates a new source version; old grants stay stale.
    rollback=catalog.Catalog(binding.research.catalog_root).rollback('research-scatter','pipeline-revoke',operator_grant=catalog.OPERATOR)
    rollback_task=commands.submit('content.approval_status',state_params,'approval-after-rollback')
    assert commands.execute(rollback_task)['usable'] is False
    old_read=commands.submit('research.get',base|{'ref':selected[0]},'revoked-ref-after-rollback')
    commands.execute(old_read,expected='failed')
    stale_assemble=commands.submit('context.assemble',wf|base|{'refs':selected},'assemble-revoked')
    refused=commands.execute(stale_assemble,expected='failed')
    assert refused['result']['commit_state']=='rolled_back',refused
    synced=sync_events(work,binding)
    event_state=manager_query({'op':'events.query','role':'manager','args':{'source_id':'workflow-dispatch','limit':200}},manager)
    assert event_state['items']
    write(work/'events-after-sync.json',event_state)
    # Follow fixed lineage directly; no prebuilt fixture chain participated.
    with closing(sqlite3.connect(binding.content_path.as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row
        raw=json.loads(db.execute('SELECT payload FROM artifacts WHERE ref=?',(script['script_ref'],)).fetchone()[0])
        mappings=[dict(row) for row in db.execute('SELECT * FROM external_contexts')]
        assert any(row['context_id']==context['context_id'] and row['manifest_sha256']==context['manifest_sha256'] and sorted(json.loads(row['refs_json']))==sorted(selected) for row in mappings)
        assert '@1#' in script['script_ref']
        write(work/'script-artifact.json',raw);write(work/'external-context-mappings.json',mappings)
    assert commands.cli('run-once')['task'] is None
    return {'passed':True,'synthetic_only':True,'ai_generated':False,'human_reviewed':False,'publish_authorized':False,
            'model_calls':0,'platform_calls':0,'business_sources_opened':False,'services_modified':False,
            'catalog_instance':binding.research.instance,'content_instance':binding.content_instance,
            'workflow_binding':binding.digest,'context':context,'script':script,'approval':approved,
            'approval_decision':approval_decision,'approval_before':current,'approval_after':stale,
            'revocation':revocation,'reconciliation_after_revoke':reconciliation,
            'rollback':rollback,'old_approval_stays_revoked_after_rollback':True,'event_sync':synced,
            'source_refs':selected,'manager_readonly_unchanged':True,'a4_observed':True,'a12_observed':True,
            'same_script_alias_no_new_version':True,'unknown_reconciled_without_replay':True,
            'content_counts':counts(binding.content_path,['artifacts','approvals','content_events','operation_receipts']),
            'command_count':len(commands.history)}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();work=args.output.absolute();work.mkdir(parents=True,exist_ok=False)
    report=run(work);report.update(python=sys.version,platform=platform.platform())
    write(work/'report.json',report)
    print(json.dumps({'passed':True,'output':str(work),'commands':report['command_count']}))


if __name__=='__main__':main()
