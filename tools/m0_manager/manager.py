"""A bounded JSON query router. This module never dispatches actions."""
from datetime import timedelta
import importlib
import json
import sqlite3
from .binding import ROLES, load_binding, digest, now, instant, token, fixed_ref
from .readonly import SourceError
from .dispatch_source import dispatch_query

FIELDS = {
    'status': {'source_id','limit'},
    'task.get': {'source_id','task_id'},
    'trace.get': {'source_id','task_id','trace_id','ref','limit'},
    'events.query': {'source_id','after_seq','limit'},
    'handoff.list': {'source_id','limit'},
    'attention.list': {'source_id','limit'},
    'inventory.search': {'source_id','q','limit','offset'},
    'inventory.get': {'source_id','id'},
    'knowledge.search': {'source_id','q','limit'},
    'knowledge.get': {'source_id','ref'},
    'context.status': {'source_id','context_id','limit'},
    'content.status': {'source_id','trace_id','limit'},
}
OPS = {
    'dispatch': {'status','task.get','trace.get','events.query','handoff.list','attention.list'},
    'inventory': {'inventory.search','inventory.get'},
    'knowledge': {'knowledge.search','knowledge.get','context.status'},
    'content': {'content.status','trace.get'},
    'backup': {'status','attention.list'},
    'hook': {'status','events.query'},
    'handoff': {'status','handoff.list'},
}
TECHNICAL_RULES = {'A2','A5','A7','A8','A9','A10'}
POLICY = {'version':1,'roles':sorted(ROLES),'technical_attention':sorted(TECHNICAL_RULES),
          'knowledge_body_roles':['manager'], 'trusted_local_only':True}


def validate(request):
    if not isinstance(request, dict) or set(request) != {'op','role','args'}:
        raise SourceError('E_FIELD_NOT_ALLOWED')
    op, role, args = request['op'], request['role'], request['args']
    if not isinstance(op,str) or not isinstance(role,str) or op not in FIELDS or role not in ROLES:
        raise SourceError('E_OPERATION_NOT_ALLOWED')
    if not isinstance(args, dict) or set(args)-FIELDS[op]:
        raise SourceError('E_FIELD_NOT_ALLOWED')
    if op in ('knowledge.search','knowledge.get') and role!='manager':
        raise SourceError('E_ROLE_NOT_ALLOWED')
    for key, value in args.items():
        if key in ('limit','offset','after_seq'):
            cap = 200 if key=='limit' else 10**9
            if type(value) is not int or not (1 if key=='limit' else 0) <= value <= cap:
                raise SourceError('E_ARGUMENT_INVALID')
        elif not isinstance(value,str) or not value or len(value)>1000:
            raise SourceError('E_ARGUMENT_INVALID')
        elif key in ('source_id','task_id','trace_id','context_id') and not token(value):
            raise SourceError('E_ARGUMENT_INVALID')
    required = {'task.get':'task_id','inventory.get':'id','knowledge.search':'q','knowledge.get':'ref'}
    if op in required and required[op] not in args:
        raise SourceError('E_ARGUMENT_REQUIRED')
    if op=='trace.get' and ('task_id' in args)==('trace_id' in args):
        raise SourceError('E_ARGUMENT_REQUIRED')
    return op, role, args


def adapter(source, op, args, role):
    if source['kind']=='dispatch':
        return dispatch_query(source,op,args,role)
    module = importlib.import_module('.'+source['kind']+'_sources', __package__)
    return getattr(module,source['kind']+'_query')(source,op,args,role)


def descriptor(source, result):
    return {key: result.get(key, source.get(key)) for key in
            ('source_id','source_instance','schema_version','class','observed_at','coverage','state_basis','unknown','projection_lag',
             'source_updated_at','source_sha256','coverage_details','record_count','matched_count','index_versions',
             'source_binding','catalog_instance','incremental','collection_modes')}


def attention_item(source, code, observed, *, rule='A10'):
    data = {'rule':rule+'@1','state':code,'subject_id':source['source_id']}
    return {**data,'ref':fixed_ref(source,'attention',source['source_id'],data),
            'source_id':source['source_id'],'as_of':observed,'state_basis':'current',
            'owner':'technical','next_step':'inspect_source'}


def run(request, binding_path):
    op, role, args = validate(request)
    binding, binding_hash = load_binding(binding_path)
    sources = binding['sources']
    if 'source_id' in args:
        sources = [s for s in sources if s['source_id']==args['source_id']]
        if not sources:
            raise SourceError('E_SOURCE_NOT_BOUND')
    if op not in ('status','attention.list'):
        sources = [s for s in sources if op in OPS[s['kind']]
                   and not (op=='trace.get' and (('task_id' in args)!=(s['kind']=='dispatch')))]
        if 'source_id' in args and not sources:
            raise SourceError('E_SOURCE_KIND_NOT_ALLOWED')
    output = {'ok':True,'op':op,'role':role,
              'role_template':f'manager/role/{role}@1#sha256:{digest(POLICY)}',
              'binding':f'manager/binding@1#sha256:{binding_hash}',
              'sources':[],'items':[],'withheld':{'by_role':0,'by_class':0,'by_policy':0},
              'authorization_basis':'trusted_local_caller_role_declaration'}
    for source in sources:
        if source['kind']=='inventory':
            category=source.get('options',{}).get('category')
            allowed={'technical':{'environments','artifacts'},'progress':{'projects','threads'}}
            if role in allowed and category not in allowed[role]:
                if 'source_id' in args:
                    raise SourceError('E_ROLE_NOT_ALLOWED')
                output['withheld']['by_role']+=1
                continue
        try:
            routed_op = op
            if op in ('status','attention.list') and op not in OPS[source['kind']]:
                routed_op = {'inventory':'inventory.search','knowledge':'context.status','content':'content.status',
                             'hook':'status','handoff':'status'}[source['kind']]
            result = adapter(source,routed_op,args,role)
            result.setdefault('state_basis','current')
            result.setdefault('observed_at',now())
            result.setdefault('unknown',[])
            if result.get('schema_version') not in source['accepted_schema_versions']:
                raise SourceError('source_schema_unsupported')
            selected = result.get('items',[])
            for reason,count in result.get('withheld',{}).items():
                if reason in output['withheld'] and type(count) is int and count>=0:
                    output['withheld'][reason]+=count
            if op=='attention.list' and routed_op!=op:
                # Each specialist adapter can return explicit attention derived
                # from current source state; status rows are never invented tasks.
                selected = result.get('attention',[])
            if result['state_basis']=='stale':
                if op=='attention.list':
                    selected = selected + [attention_item(source,'source_stale',result['observed_at'])]
            if op=='attention.list' and role=='technical':
                permitted = [r for r in selected if r.get('rule','').split('@')[0] in TECHNICAL_RULES]
                output['withheld']['by_role'] += len(selected)-len(permitted)
                selected = permitted
            for row in selected:
                output['items'].append({**row,'source_id':source['source_id']})
            if op=='trace.get':
                output.setdefault('events',[]).extend(result.get('events',[]))
                output.setdefault('evidence',[]).extend(result.get('evidence',[]))
                output.setdefault('attempts',[]).extend(result.get('attempts',[]))
                output.setdefault('reservations',[]).extend(result.get('reservations',[]))
            output['sources'].append(descriptor(source,result))
        except (OSError,sqlite3.Error,ValueError,TypeError,KeyError,SourceError) as exc:
            code = exc.code if isinstance(exc,SourceError) else 'source_unavailable' if isinstance(exc,OSError) else 'source_invalid'
            observed = now()
            output['sources'].append(descriptor(source,{'coverage':'unavailable','observed_at':observed,
                'schema_version':None,'state_basis':'unknown','unknown':[code]}))
            if op in ('attention.list','status'):
                output['items'].append(attention_item(source,code,observed))
    if op=='attention.list' and not any(s['kind']=='backup' for s in sources) and 'source_id' not in args:
        missing={'source_id':'backup-unbound','source_instance':'unbound'}
        output['items'].append(attention_item(missing,'unknown:no_backup_evidence',now(),rule='A7'))
    limit=args.get('limit',50)
    if op=='attention.list':
        output['items'].sort(key=lambda item: (item.get('rule')!='A10@1',item.get('source_id',''),item.get('ref','')))
    output['truncated']=len(output['items'])>limit
    output['items']=output['items'][:limit]
    output['coverage']='complete' if sources and all(s['coverage']=='complete' for s in output['sources']) and not output['truncated'] else 'partial'
    if role=='progress':
        # Defense in depth over adapter allowlists; never return source records,
        # command arguments, host locators, or unbounded free-form descriptions.
        forbidden={'argv','path','locator','root','root_path','run_dir','endpoint','host','host_id','device_id','source_record','summary','purpose','from_conv','to_conv'}
        def redact(value):
            if isinstance(value,dict):
                return {k:redact(v) for k,v in value.items() if k not in forbidden}
            if isinstance(value,list):
                return [redact(v) for v in value]
            if isinstance(value,str):
                import re
                if re.search(r'(?:^|[:@\s])[A-Za-z]:[\\/]|^[/\\]',value):
                    return 'withheld:path_value'
            return value
        output=redact(output)
    return output
