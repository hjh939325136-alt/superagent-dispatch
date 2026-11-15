"""Trusted local binding for the synthetic research-to-content workflow."""
from dataclasses import dataclass
import hashlib
import importlib
import json
from pathlib import Path
import re
import sys

ROOT=Path(__file__).resolve().parents[2]
SEARCH_ROOT=ROOT/'knowledge/m0_search'
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from tools.m0_manager.readonly import local_file, local_sqlite_file, readonly_db
from tools.m0_backup.domain_binding import (ENVELOPE_SCHEMA, IDENTITY, RecoveryAdmission,
    historical_path, load_recovery_binding)

SCHEMA='m0.research_content_binding.v1'
AUTHORITY='fixture://m0-research-content/workflow-v1'
CALLER={'agent':'fixture-agent','task_id':'fixture-three-source-task','purpose':'fixture-review'}
DOMAINS=('research-scatter','research-content','signalhub')
SCOPES=('research:read','signalhub:refs')
TRACES=('synthetic-content-game','synthetic-content-ai-editing')
TEMPLATE={'schema':'m0.workflow_template.v1','id':'synthetic-script-v1','synthetic_only':True,
          'title':'合成研究到内容演示','lines':['仅用于合成流程验证。','采用 {source_count} 条已授权来源。',
          '上下文清单：{manifest_sha256}。','本脚本仍需审核，未获发布许可。']}


class WorkflowError(ValueError):
    def __init__(self,code):
        self.code=code
        super().__init__(code)


def canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')


def hashed(value):
    return 'sha256:'+hashlib.sha256(canonical(value)).hexdigest()


def parse_json(raw):
    def pairs(items):
        result=dict(items)
        if len(result)!=len(items):raise WorkflowError('DUPLICATE_FIELD')
        return result
    def invalid(_):raise WorkflowError('INVALID_JSON')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)


def search_modules():
    sys.path.insert(0,str(SEARCH_ROOT))
    try:
        modules=tuple(importlib.import_module(name) for name in ('dispatch_binding','catalog','search','workflow_catalog'))
        if any(Path(module.__file__).resolve().parent!=SEARCH_ROOT for module in modules):
            raise WorkflowError('ADAPTER_MODULE_CONFLICT')
        return modules
    finally:
        sys.path.remove(str(SEARCH_ROOT))


def request_base(as_of):
    return {'caller':dict(CALLER),'domains':list(DOMAINS),'scopes':list(SCOPES),'max_class':'internal','as_of':as_of}


@dataclass(frozen=True)
class WorkflowBinding:
    path: Path
    digest: str
    research: object
    content_path: Path
    content_instance: str
    allowed_traces: tuple
    template_id: str
    template_sha256: str
    template: dict
    recovery_digest: str | None = None
    recovery_admission: RecoveryAdmission | None = None


FIELDS={'schema','synthetic_only','authorization_source','research_binding','research_binding_sha256',
        'catalog_instance','content_db','content_instance','allowed_traces','template','template_sha256'}


def load_workflow_binding(path):
    path=local_file(path)
    if path.stat().st_nlink!=1 or path.stat().st_size>65536:
        raise WorkflowError('INVALID_WORKFLOW_BINDING')
    raw=path.read_bytes();value=parse_json(raw)
    recovered=None
    if isinstance(value,dict) and value.get('schema')==ENVELOPE_SCHEMA:
        recovered=load_recovery_binding(path,'workflow')
        value=recovered.original
    if (not isinstance(value,dict) or set(value)!=FIELDS or value['schema']!=SCHEMA or
        value['synthetic_only'] is not True or value['authorization_source']!=AUTHORITY or
        value['template']!=TEMPLATE or value['template_sha256']!=hashed(TEMPLATE) or
        not isinstance(value['allowed_traces'],list) or not value['allowed_traces'] or
        any(not isinstance(trace,str) for trace in value['allowed_traces']) or
        len(set(value['allowed_traces']))!=len(value['allowed_traces']) or
        not set(value['allowed_traces'])<=set(TRACES)):
        raise WorkflowError('INVALID_WORKFLOW_BINDING')
    bound,catalog,search,_=search_modules()
    if recovered is None:
        research=bound.load_binding(local_file(value['research_binding']))
        if research.recovery_digest is not None:
            raise WorkflowError('RECOVERY_RESEARCH_REQUIRES_RECOVERY_WORKFLOW')
        selected_content=value['content_db']
    else:
        if (not historical_path(value['research_binding']) or not historical_path(value['content_db']) or
            any(not isinstance(value[key],str) or not IDENTITY.fullmatch(value[key])
                for key in ('catalog_instance','content_instance'))):
            raise WorkflowError('INVALID_WORKFLOW_BINDING')
        research=bound.load_binding(recovered.role_paths['research_binding'])
        if research.recovery_digest!=recovered.recovery_digest:
            raise WorkflowError('RECOVERY_RESEARCH_BINDING_MISMATCH')
        selected_content=recovered.role_paths['content_db']
    if research.digest!=value['research_binding_sha256'] or research.instance!=value['catalog_instance']:
        raise WorkflowError('RESEARCH_BINDING_CHANGED')
    # Authorize before inspecting the source databases. A binding hash is not a grant.
    from datetime import datetime,timezone
    search.authorize(request_base(datetime.now(timezone.utc).isoformat())|{'context_id':'binding-preflight'},research.grant,'context_id')
    for name in ('catalog',*DOMAINS):
        local_sqlite_file(research.catalog_root/(name+'.sqlite3'))
    if recovered is not None:
        with research.catalog().transaction(DOMAINS):
            pass
    content_path=local_sqlite_file(selected_content)
    with readonly_db(content_path) as db:
        meta=dict(db.execute('SELECT key,value FROM meta'))
    if (meta.get('schema')!='m0-content-synthetic-v1' or meta.get('instance')!=value['content_instance'] or
        meta.get('platform_calls')!='0'):
        raise WorkflowError('CONTENT_INSTANCE_CHANGED')
    return WorkflowBinding(path,hashed(value),research,content_path,value['content_instance'],tuple(value['allowed_traces']),
                           TEMPLATE['id'],hashed(TEMPLATE),dict(TEMPLATE),
                           None if recovered is None else recovered.recovery_digest,
                           None if recovered is None else recovered.admission)


def initialize_binding(research_binding,content_path,output,*,allowed_traces=TRACES):
    """Explicit fixture enrollment; does not initialize/migrate either database."""
    bound,_,_,_=search_modules()
    research=bound.load_binding(local_file(research_binding))
    if research.recovery_digest is not None:
        raise WorkflowError('RECOVERY_RESEARCH_REQUIRES_RECOVERY_WORKFLOW')
    content_path=local_sqlite_file(content_path)
    with readonly_db(content_path) as db:
        meta=dict(db.execute('SELECT key,value FROM meta'))
    if meta.get('schema')!='m0-content-synthetic-v1' or meta.get('platform_calls')!='0':
        raise WorkflowError('CONTENT_NOT_SYNTHETIC')
    value={'schema':SCHEMA,'synthetic_only':True,'authorization_source':AUTHORITY,
           'research_binding':str(Path(research_binding).absolute()),'research_binding_sha256':research.digest,
           'catalog_instance':research.instance,'content_db':str(content_path),'content_instance':meta['instance'],
           'allowed_traces':list(allowed_traces),'template':TEMPLATE,'template_sha256':hashed(TEMPLATE)}
    output=Path(output)
    if not output.is_absolute() or output.exists() or not output.parent.is_dir():
        raise WorkflowError('NEW_BINDING_PATH_REQUIRED')
    with output.open('xb') as stream:stream.write(canonical(value)+b'\n')
    return load_workflow_binding(output)
