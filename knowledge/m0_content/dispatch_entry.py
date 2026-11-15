"""Register the synthetic research/content workflow on the existing Dispatcher."""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import sys

ROOT=Path(__file__).resolve().parents[2]
for item in (ROOT,ROOT/'dispatch',ROOT/'knowledge/m0_search'):
    if str(item) not in sys.path:sys.path.insert(0,str(item))
from sa_dispatch.cli import main as dispatch_main
from sa_dispatch.config import load_config
from sa_dispatch.models import Outcome
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.canonical import sha256_ref
from knowledge.m0_content.workflow_binding import WorkflowError,load_workflow_binding,search_modules,request_base

WRITES={'context.assemble','content.script_from_context','content.approval_record'}
OPERATIONS=WRITES|{'content.approval_status'}
PARAMS={
 'context.assemble':{'caller','domains','scopes','max_class','as_of','refs'},
 'content.script_from_context':{'trace_id','context_id','manifest_sha256','template_id'},
 'content.approval_record':{'trace_id','context_id','manifest_sha256','script_ref'},
 'content.approval_status':{'trace_id','context_id','manifest_sha256','script_ref','approval_ref'},
}


def validate_params(binding,operation,params,*,invocation=False):
    fields=PARAMS.get(operation,set())|{'workflow_binding'}
    if invocation and operation=='content.approval_record':fields=fields|{'control_approval'}
    if operation not in OPERATIONS or not isinstance(params,dict) or set(params)!=fields:
        raise WorkflowError('INVALID_WORKFLOW_FIELDS')
    if params['workflow_binding']!=binding.digest:
        raise WorkflowError('WORKFLOW_BINDING_CHANGED')
    clean={k:v for k,v in params.items() if k!='workflow_binding'}
    if operation=='context.assemble':
        _,_,search,_=search_modules()
        search.authorize(clean,binding.research.grant,'refs')
        expected=request_base(clean['as_of'])
        if any(clean[k]!=value for k,value in expected.items()):
            raise WorkflowError('WORKFLOW_AUTHORITY_MISMATCH')
        if not isinstance(clean['refs'],list) or not 1<=len(clean['refs'])<=100:
            raise WorkflowError('INVALID_CONTEXT_REFS')
        from datetime import datetime,timezone
        at=datetime.fromisoformat(clean['as_of'])
        if (at-datetime.now(timezone.utc)).total_seconds()>0:
            raise WorkflowError('FUTURE_CONTEXT_CLOCK')
    else:
        if (clean['trace_id'] not in binding.allowed_traces or
            not isinstance(clean['context_id'],str) or not re.fullmatch(r'ctx_[0-9a-f]{26}',clean['context_id']) or
            not isinstance(clean['manifest_sha256'],str) or not re.fullmatch('[0-9a-f]{64}',clean['manifest_sha256'])):
            raise WorkflowError('INVALID_CONTEXT_IDENTITY')
        if 'template_id' in clean and clean['template_id']!=binding.template_id:
            raise WorkflowError('TEMPLATE_NOT_BOUND')
        for key in ('script_ref','approval_ref'):
            if key in clean and (not isinstance(clean[key],str) or not re.fullmatch(r'mgmt/'+key.removesuffix('_ref')+r'/[A-Za-z0-9:_-]+@[1-9][0-9]*#[0-9a-f]{64}',clean[key])):
                raise WorkflowError('INVALID_CONTENT_REF')
    return clean


class WorkflowKind:
    version=1
    component='research_content'
    enabled=True
    allow_no_new=False
    def __init__(self,name,binding):
        self.name,self.binding=name,binding
        self.side_effect='idempotent_write' if name in WRITES else 'none'
    def validate(self,params):
        validate_params(self.binding,self.name,params)
        return params
    def scope(self,params):
        return {'workflow_binding':self.binding.digest,'catalog_instance':self.binding.research.instance,
                'content_instance':self.binding.content_instance}
    def request(self,task):
        if task.spec.access!='synthetic' or task.spec.egress!='none' or task.spec.max_attempts!=1:
            raise WorkflowError('WORKFLOW_REQUIRES_SYNTHETIC_SINGLE_ATTEMPT')
        validate_params(self.binding,self.name,task.spec.params)
        return {'schema_version':'0.1','request_id':task.task_id,'operation':self.name,
                'idempotency_key':task.task_id+':'+self.name+'@1' if self.name in WRITES else None,
                'payload':task.spec.params}
    def request_with_context(self,task,context):
        request=self.request(task)
        if self.name!='content.approval_record':return request
        from datetime import datetime,timezone
        approval=context.get('control_approval')
        expected={'task_id':task.task_id,'fingerprint':context.get('task_fingerprint'),
                  'scope':'script_ready','decision':'approved','actor':'synthetic-operator'}
        if not isinstance(approval,dict) or any(approval.get(k)!=v for k,v in expected.items()):
            raise WorkflowError('CONTROL_APPROVAL_REQUIRED')
        proof=approval.get('proof')
        if (not isinstance(proof,dict) or any(proof.get(k)!=v for k,v in expected.items()) or
            proof.get('synthetic') is not True or proof.get('human_reviewed') is not False or
            proof.get('publish_authorized') is not False or
            approval.get('proof_ref')!=sha256_ref(receipt_bytes(proof)) or
            approval.get('proof_ref')!=approval.get('evidence_ref')):
            raise WorkflowError('CONTROL_APPROVAL_PROOF_INVALID')
        expiry=datetime.fromisoformat(proof['expires_at'].replace('Z','+00:00'))
        row_expiry=datetime.fromisoformat(approval['expires_at'].replace('Z','+00:00'))
        if (expiry.tzinfo is None or row_expiry.tzinfo is None or
            not datetime.now(timezone.utc)<row_expiry<=expiry):
            raise WorkflowError('CONTROL_APPROVAL_EXPIRED')
        # The normal task API cannot supply this field. Only verified local
        # control evidence is injected into the immutable invocation.
        request['payload']={**request['payload'],'control_approval':{
            'approval_id':approval['approval_id'],'proof_hash':approval['proof_ref'],
            'task_id':task.task_id,'scope':'script_ready','actor':'synthetic-operator',
            'expires_at':row_expiry.isoformat()}}
        return request
    def result(self,response,task):
        if (response.get('schema_version')!='0.1' or response.get('ok') is not True or
            response.get('request_id')!=task.task_id):
            raise WorkflowError('UNBOUND_WORKFLOW_RESPONSE')
        data=response.get('data',{})
        if (data.get('workflow_binding')!=self.binding.digest or
            data.get('catalog_instance')!=self.binding.research.instance or
            data.get('content_instance')!=self.binding.content_instance or data.get('synthetic_only') is not True or
            data.get('receipt_content')!='references_only'):
            raise WorkflowError('UNBOUND_WORKFLOW_RECEIPT')
        denied={'body','source_record','snippet','script_text','argv','path','locator'}
        def inspect(value):
            if isinstance(value,dict):
                if set(value)&denied:raise WorkflowError('WORKFLOW_RECEIPT_HAS_BODY')
                for child in value.values():inspect(child)
            elif isinstance(value,list):
                for child in value:inspect(child)
        inspect(data)
        return data
    def lookup_request(self,task,original,request_hash):
        if self.name not in WRITES:raise WorkflowError('READ_OPERATION_NOT_RECONCILABLE')
        return {'schema_version':'0.1','request_id':'reconcile-'+task.task_id,'operation':'operation.lookup',
                'idempotency_key':None,'payload':{'task_id':task.task_id,'fence':task.fence,
                  'operation':original['operation'],'write_key':original['idempotency_key'],'request_hash':request_hash,
                  **self.instance()}}
    def instance(self):
        return {'binding_hash':self.binding.digest,'catalog_instance':self.binding.research.instance,
                'content_instance':self.binding.content_instance}
    def validate_lookup(self,task,original,data):
        if any(data.get(k)!=v for k,v in self.instance().items()):
            raise WorkflowError('LOOKUP_INSTANCE_MISMATCH')
        return True
    def rejection_outcome(self,response,task):
        if (response.get('schema_version')=='0.1' and response.get('request_id')==task.task_id and
            response.get('ok') is False and response.get('commit_state') in ('not_started','rolled_back') and
            response.get('workflow_binding')==self.binding.digest and
            response.get('error',{}).get('code') in PRECOMMIT_CODES):
            return Outcome('failed',{'code':response['error']['code'],'commit_state':response['commit_state']})
        return None


PRECOMMIT_CODES={'DEPENDENCY_REVOKED','DEPENDENCY_EXPIRED','DEPENDENCY_UNUSABLE',
                 'DEPENDENCY_GUARD_REQUIRED','DEPENDENCY_MANIFEST_MISMATCH','CONTEXT_MANIFEST_MISMATCH',
                 'WORKFLOW_BINDING_CHANGED','RESEARCH_BINDING_CHANGED','CONTENT_INSTANCE_CHANGED',
                 'CONTROL_APPROVAL_REQUIRED','CONTROL_APPROVAL_PROOF_INVALID','CONTROL_APPROVAL_EXPIRED',
                 'AS_OF_OUTSIDE_FRESHNESS_WINDOW','NOT_FOUND','ACCESS_DENIED','EXPIRED',
                 'CONTEXT_EXPIRED','CONTEXT_REVOKED'}


def configured(config):
    if config.execution_route not in (None, 'local_synthetic'):
        raise WorkflowError('WORKFLOW_EXECUTION_ROUTE_REQUIRED')
    component=config.components.get('research_content')
    if not config.synthetic or component is None:
        raise WorkflowError('SYNTHETIC_WORKFLOW_CONFIG_REQUIRED')
    argv=list(component.argv)
    if argv and Path(argv[0]).resolve()==Path(sys.executable).resolve():argv.pop(0)
    else:raise WorkflowError('WORKFLOW_EXECUTABLE_NOT_BOUND')
    if argv and argv[0]=='-B':argv.pop(0)
    if len(argv)!=3 or Path(argv[0]).resolve()!=Path(__file__).with_name('dispatch_component.py').resolve() or argv[1]!='--workflow-binding':
        raise WorkflowError('WORKFLOW_COMPONENT_NOT_BOUND')
    binding=load_workflow_binding(Path(argv[2]))
    spec=importlib.util.spec_from_file_location('m0_existing_research_entry',ROOT/'knowledge/m0_search/dispatch_entry.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    research=module.configured_kinds(config)
    if any(getattr(kind,'binding',None) is None or kind.binding.digest!=binding.research.digest for kind in research.values()):
        raise WorkflowError('RESEARCH_COMPONENT_BINDING_MISMATCH')
    return {'research':research,'research_content':{name:WorkflowKind(name,binding) for name in OPERATIONS}}


def main(argv=None):
    argv=sys.argv[1:] if argv is None else argv
    parser=argparse.ArgumentParser(add_help=False);parser.add_argument('--config',type=Path,required=True)
    try:
        selected,_=parser.parse_known_args(argv)
        config=load_config(selected.config,allowed_plugins={'research','research_content'})
        return dispatch_main(argv,plugins=configured(config))
    except (ValueError,OSError,TypeError,KeyError) as error:
        print(json.dumps({'schema':'sa_dispatch.v0','ok':False,'error':{'code':getattr(error,'code','WORKFLOW_REFUSED')}}))
        return 2


if __name__=='__main__':raise SystemExit(main())
