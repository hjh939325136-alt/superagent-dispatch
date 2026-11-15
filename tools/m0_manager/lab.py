"""Explicitly build an isolated synthetic manager fixture, never a live source."""
import argparse
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'dispatch'))
sys.path.insert(0,str(ROOT/'knowledge'/'m0_content'))
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.models import TaskSpec
from contract import Contract
from fixtures import chain, observation
from .binding import now
from .knowledge_sources import _modules, CALLER
from tools.m0_storage.target import LocalDirTarget
from tools.m0_backup.backup import canonical,sha


def prepare(root):
    root=Path(root).absolute()
    root.mkdir(parents=True,exist_ok=False)
    sources=[]
    def source(id,kind,path,version,instance,options=None):
        row={'source_id':id,'kind':kind,'locator':str(path),'accepted_schema_versions':[version],
             'source_instance':instance,'class':'internal','freshness_s':86400,'options':options or {}}
        sources.append(row);return row
    ledger=root/'ledger.sqlite3';store=SqliteStore(ledger,lease_ttl_seconds=10)
    task,_=store.submit(TaskSpec('manager-lab-read','synthetic.read',1,{},(),(),'synthetic','none',2))
    info=ledger.stat()
    source('dispatch-synthetic','dispatch',ledger,'0011','manager-synthetic-ledger',
           {'identity':{'device':info.st_dev,'inode':info.st_ino}})['accepted_schema_versions']=['0010','0011']
    content=root/'content.sqlite3'
    with closing(Contract(content,create=True)) as contract:
        game=chain(contract,'game');editing=chain(contract,'ai-editing')
        prep=contract.invoke('publish.prepare',game['request'])
        contract.invoke('fixture.submit_unknown',{'preparation_id':prep['preparation_id'],'at':'2020-01-01T00:00:00Z'})
        other=contract.invoke('publish.prepare',editing['request'])
        contract.invoke('publish.observe',observation(editing,other['preparation_id']))
        instance=contract.db.execute("SELECT value FROM meta WHERE key='instance'").fetchone()[0]
    source('content-synthetic','content',content,'m0-content-synthetic-v1',instance,
           {'trace_ids':['synthetic-content-game','synthetic-content-ai-editing']})
    bound,catalog,search=_modules()
    catalog.initialize(root/'research',operator_grant=catalog.OPERATOR)
    bound.initialize_binding(root/'research',root/'research-binding.json')
    binding=bound.load_binding(root/'research-binding.json')
    source('knowledge-synthetic','knowledge',root/'research-binding.json','m0-synthetic-research-catalog-v1',
           'manager-synthetic-knowledge',{'binding_sha256':binding.digest,'catalog_instance':binding.instance})
    params={'caller':CALLER,'domains':list(catalog.SOURCES),'scopes':['research:read','signalhub:refs'],
            'max_class':'internal','as_of':now(),'q':'剪辑','limit':10}
    selected=search.search(params,binding.grant,catalog=catalog.Catalog(root/'research'))
    refs=[item['ref'] for item in selected['results'] if item['status']=='current']
    context=catalog.Catalog(root/'research').assemble({k:v for k,v in params.items() if k not in ('q','limit')}|{'refs':refs},binding.grant)
    inventory=root/'inventory';inventory.mkdir()
    data={
      'projects':[{'id':'synthetic-project','name':'Synthetic Project','status':'active'}],
      'threads':[{'id':'synthetic-thread','title':'Synthetic Chat','projectId':'synthetic-project','status':'active'}],
      'artifacts':[{'id':'synthetic-artifact','name':'Synthetic Result','status':'available'}],
      'environments':[{'id':'synthetic-environment','name':'Synthetic Python','status':'available','kind':'runtime'}],
      'cleanup-candidates':[{'id':'synthetic-cleanup','name':'Synthetic candidate','status':'recommendation_ready','canDelete':False}],
    }
    for category,items in data.items():
        path=inventory/(category+'.json')
        path.write_text(json.dumps({'schemaVersion':1,'updatedAt':now(),'items':items}),encoding='utf-8')
        source('inventory-'+category,'inventory',path,'1','synthetic-inventory-'+category,{'category':category})
    quota=root/'backup-quota';quota.mkdir();target_root=quota/'target';target_root.mkdir()
    target=LocalDirTarget(target_root,quota_root=quota);target.initialize()
    receipt=canonical({'schema':'m0.synthetic_evidence.v1','synthetic_only':True,'evidence_type':'backup_run',
                       'version':1,'status':'restore_failed','at':now()})
    target.put('receipts/synthetic-failure.json',receipt,sha(receipt))
    index,_=target.readonly_snapshot()
    source('backup-synthetic','backup',target_root,'m0.storage_target.v1',index['instance_id'],
           {'quota_root':str(quota),'receipt_names':['receipts/synthetic-failure.json']})
    manager_binding=root/'manager-binding.json'
    manager_binding.write_text(json.dumps({'version':1,'sources':sources},ensure_ascii=False,indent=2),encoding='utf-8')
    launcher={'version':1,'argv':[sys.executable,'-B','-m','tools.m0_manager','--binding',str(manager_binding)],
              'working_directory':str(ROOT),'allowed_source_ids':[s['source_id'] for s in sources],'timeout_ms':10000}
    (root/'studio-launcher.json').write_text(json.dumps(launcher,indent=2),encoding='utf-8')
    result={'synthetic_only':True,'root':str(root),'binding':str(manager_binding),
            'studio_launcher':str(root/'studio-launcher.json'),'task_id':task.task_id,
            'context_id':context['context_id'],'knowledge_ref':refs[0] if refs else None,'source_count':len(sources)}
    (root/'fixture.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True)
    print(json.dumps(prepare(parser.parse_args().output),ensure_ascii=False))
