"""Bound synthetic write component. Only metadata receipts leave the subprocess."""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import re
import sys
ROOT=Path(__file__).resolve().parents[2]
for item in (ROOT,ROOT/'dispatch'):
    if str(item) not in sys.path:sys.path.insert(0,str(item))
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.canonical import sha256_ref
from knowledge.m0_content.workflow_binding import WorkflowError,load_workflow_binding,parse_json,search_modules,request_base
from knowledge.m0_content.workflow_guard import dependency_guard,require_guard
from knowledge.m0_content.dispatch_entry import validate_params,WRITES,OPERATIONS,PRECOMMIT_CODES


class PrecommitRejected(WorkflowError):
    pass


def projection(binding,result):
    identity={'workflow_binding':binding.digest,'catalog_instance':binding.research.instance,
              'content_instance':binding.content_instance}
    if any(key in result and result[key]!=value for key,value in identity.items()):
        raise WorkflowError('RESULT_INSTANCE_MISMATCH')
    return {**result,**identity,'synthetic_only':True,'receipt_content':'references_only'}


def envelope(request_id,data):
    return {'schema_version':'0.1','request_id':request_id,'ok':True,'data':data}


def invoke(binding,request):
    if not isinstance(request,dict) or set(request)!={'schema_version','request_id','operation','idempotency_key','payload'}:
        raise WorkflowError('INVALID_INVOCATION')
    if request['schema_version']!='0.1' or not isinstance(request['request_id'],str) or not re.fullmatch('[A-Za-z0-9_.:-]{1,160}',request['request_id']):
        raise WorkflowError('INVALID_INVOCATION')
    op=request['operation']
    from knowledge.m0_content import workflow_content
    _,_,_,research=search_modules()
    if op=='operation.lookup':
        payload=request['payload']
        if (request['idempotency_key'] is not None or not isinstance(payload,dict) or
            set(payload)!={'task_id','fence','operation','write_key','request_hash','binding_hash','catalog_instance','content_instance'} or payload['operation'] not in WRITES or
            type(payload['fence']) is not int or payload['fence']<1 or
            request['request_id']!='reconcile-'+payload['task_id'] or
            payload['write_key']!=payload['task_id']+':'+payload['operation']+'@1' or
            not re.fullmatch('sha256:[0-9a-f]{64}',payload['request_hash'])):
            raise WorkflowError('INVALID_LOOKUP')
        expected={'binding_hash':binding.digest,'catalog_instance':binding.research.instance,'content_instance':binding.content_instance}
        if any(payload.get(k)!=v for k,v in expected.items()):
            return envelope(request['request_id'],{'state':'instance_mismatch',**payload})
        if payload['operation']=='context.assemble':
            found=research.lookup(binding.research,payload['operation'],payload['write_key'],payload['request_hash'])
            found={'state':found['status'],'result':found.get('receipt'),
                   'current_usable':found.get('current_usable',False),'usable_reason':found.get('usable_reason','unknown')}
        else:
            with dependency_guard(binding) as guard:
                found=workflow_content.lookup_content(binding,payload['operation'],payload['write_key'],payload['request_hash'],guard)
        data={'state':found['state'],**payload}
        if found['state']=='committed':
            result=found['result']
            original=envelope(payload['task_id'],projection(binding,result))
            data.update(response=original,response_hash=sha256_ref(receipt_bytes(original)),
                        current_usable=found.get('current_usable') is True,
                        usable_reason=found.get('usable_reason',found.get('reason_code','unknown')))
        return envelope(request['request_id'],data)
    clean=validate_params(binding,op,request['payload'],invocation=True)
    key=request['idempotency_key']
    expected=request['request_id']+':'+op+'@1' if op in WRITES else None
    if key!=expected:raise WorkflowError('WRITE_KEY_MISMATCH')
    invocation_hash=sha256_ref(receipt_bytes(request))
    if op=='context.assemble':
        result=research.assemble(binding.research,clean,key,invocation_hash)
    else:
        with dependency_guard(binding) as guard:
            if op=='content.approval_status':
                result=workflow_content.approval_status(binding,clean,guard)
            else:
                prior=workflow_content.lookup_content(binding,op,key,invocation_hash,guard)
                if prior['state']=='committed':
                    # A historical success is not a new permission to use it.
                    result=prior['result']
                elif prior['state']=='conflict':
                    raise WorkflowError('WRITE_KEY_CONFLICT')
                else:
                    try:require_guard(guard,binding,clean['context_id'],clean['manifest_sha256'])
                    except WorkflowError as error:
                        if error.code in PRECOMMIT_CODES:raise PrecommitRejected(error.code) from None
                        raise
                    result=workflow_content.write_content(binding,op,clean,key,invocation_hash,guard)['result']
    # Explicit test-process-only crash injection after the durable commit, never
    # accepted through task params or an HTTP entry. Lookup is never interrupted.
    if op in WRITES and os.environ.get('M0_RESEARCH_CONTENT_TEST_CRASH')=='after_commit':os._exit(73)
    return envelope(request['request_id'],projection(binding,result))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['invoke'])
    parser.add_argument('--request',type=Path,required=True);parser.add_argument('--json',action='store_true')
    parser.add_argument('--workflow-binding',type=Path,required=True)
    args=parser.parse_args();request_id=None;binding=None
    try:
        with args.request.open('rb') as stream:raw=stream.read(131073)
        if len(raw)>131072:raise WorkflowError('REQUEST_TOO_LARGE')
        request=parse_json(raw);request_id=request.get('request_id') if isinstance(request,dict) else None
        binding=load_workflow_binding(args.workflow_binding)
        result=invoke(binding,request);code=0
    except Exception as error:
        result={'schema_version':'0.1','request_id':request_id,'ok':False,'error':{'code':getattr(error,'code','WORKFLOW_REFUSED')}};code=2
        if isinstance(error,PrecommitRejected) and binding is not None:
            result.update(commit_state='not_started',workflow_binding=binding.digest)
        elif (binding is not None and getattr(error,'commit_state',None)=='rolled_back' and
              getattr(error,'code',None) in PRECOMMIT_CODES):
            result.update(commit_state='rolled_back',workflow_binding=binding.digest)
    print(json.dumps(result,ensure_ascii=False,sort_keys=True));return code


if __name__=='__main__':raise SystemExit(main())
