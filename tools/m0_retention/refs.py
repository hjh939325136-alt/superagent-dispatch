"""Explicit reference graph. Unknown shapes freeze sweeping; strings are not refs."""
import re
from .common import HASH, MAX_BLOB, cas_path, parse, read

DIRECT={
 'reconciliations':('evidence_ref',), 'component_capabilities':('proof_ref',),
 'transport_artifact_grants':('content_hash',), 'control_approvals':('evidence_ref',),
 'control_reservations':('evidence_ref',),
 'control_handoffs':('authorization_ref','receipt_ref'),
 'host_attempts':('prompt_ref','report_ref'), 'host_events':('event_ref',),
 'claude_attempts':('prompt_ref','report_ref'),
 'evidence':('content_hash','artifact_hash')}
CHILDREN={
 'sa_dispatch.idempotency_proof.v1':('first_result_hash','second_result_hash','state_after_first_ref','state_after_second_ref'),
 'sa_dispatch.claude_report.v1':('prompt_ref','stdout_ref','init_ref','exit_ref','launch_ref'),
 'sa_dispatch.claude_terminal.v1':('prompt_ref','stdout_ref','init_ref','exit_ref','launch_ref'),
 'sa_dispatch.claude_launch.v1':('prompt_ref',),
 'sa_dispatch.claude_exit.v1':('prompt_ref',),
 'sa_dispatch.claude_run_usage.v1':('terminal_ref',),
 'sa_dispatch.host_run_usage.v1':('terminal_event_ref',),
 'sa_dispatch.reconciliation.v1':('lookup_ref',)}
LEAVES={'m0.retention_leaf.v1','sa_dispatch.claude_prompt.v1','sa_dispatch.control_approval.v1'}
RESULT_KEYS={'receipt','claude_report_ref','host_report_ref'}
FIXED=re.compile(r'^[^\s/]+/[^\s/]+/[^\s@]+@[^\s#]+#(?:sha256:)?([0-9a-f]{64})$')

class References:
    def __init__(self,cas,inventory):
        self.cas=cas;self.inventory=inventory;self.pins={};self.unknown=[];self.queue=[];self.documents=set()
    def doubt(self,code):
        if code not in self.unknown:self.unknown.append(code)
    def add(self,ref,origin,role='document'):
        if ref is None:return
        if not isinstance(ref,str):self.doubt('malformed_ref:'+origin);return
        if not HASH.fullmatch(ref):
            match=FIXED.fullmatch(ref)
            if not match:self.doubt('malformed_ref:'+origin);return
            ref='sha256:'+match[1]
        if role=='document' and ref not in self.documents:
            self.documents.add(ref);self.queue.append(ref)
        self.pins.setdefault(ref,[]).append(origin)
        if ref not in self.inventory:self.doubt('missing_ref:'+origin)
    def result(self,value,origin):
        if value is None:return
        if not isinstance(value,dict):self.doubt('unknown_result:'+origin);return
        # Only retained metadata contracts are understood. Unknown fields cannot
        # quietly hide references and therefore stop orphan collection.
        allowed=RESULT_KEYS|{'state','status','output','error','session_id','model','cost_unknown','run_id'}
        if set(value)-allowed:self.doubt('unknown_result_fields:'+origin)
        for key in RESULT_KEYS:
            if key in value:self.add(value[key],origin+'.'+key)
        if isinstance(value.get('output'),(dict,list)):self.doubt('opaque_result_output:'+origin)
    def outcome(self,value,origin):
        if not isinstance(value,dict) or set(value)-{'status','result','evidence'}:
            self.doubt('unknown_outcome:'+origin);return
        self.result(value.get('result'),origin+'.result')
        for row in self.array(value.get('evidence',[])):
            if not isinstance(row,dict) or set(row)-{'access','kind','content_hash','artifact_hash','ref'}:
                self.doubt('unknown_evidence:'+origin);continue
            for key in ('content_hash','artifact_hash'):self.add(row.get(key),origin+'.'+key)
            if row.get('ref') is not None:self.object_ref(row['ref'],origin+'.ref')
    @staticmethod
    def array(value):
        if not isinstance(value,list):raise ValueError('expected array')
        return value
    def object_ref(self,ref,origin):
        if not isinstance(ref,dict) or set(ref)!={'owner','kind','id','version','hash'} or any(
                not isinstance(val,str) or not val for val in ref.values()):
            self.doubt('malformed_ref:'+origin);return
        self.add(ref['hash'],origin)
    def row(self,table,row):
        origin=table+':'+str(row.get('task_id',row.get('seq',row.get('id','row'))))
        try:
            for field in DIRECT.get(table,()):self.add(row.get(field),origin+'.'+field)
            if table=='tasks':
                for ref in self.array(parse(row['input_refs_json'])):self.object_ref(ref,origin+'.input')
                self.result(parse(row['result_json']) if row['result_json'] else None,origin+'.result')
                if row['kind'] not in {'fixture.retention','fixture.benchmark','claude.run','host.run'}:self.doubt('unknown_task_kind:'+row['kind'])
            if table=='evidence' and row.get('ref_json'):
                ref=parse(row['ref_json'])
                if ref is not None:self.object_ref(ref,origin+'.ref')
            if table=='leases' and row.get('completion_json'):self.outcome(parse(row['completion_json']),origin+'.completion')
            if table=='late_results':self.outcome(parse(row['outcome_json']),origin+'.late')
            for field in ('refs_json','scope_refs_json','evidence_refs_json'):
                if field in row:
                    for ref in self.array(parse(row[field])):
                        if ref is None:self.doubt('malformed_ref:'+origin+'.'+field)
                        else:self.add(ref,origin+'.'+field)
        except (ValueError,TypeError,KeyError,AttributeError):self.doubt('malformed_record:'+origin)
    def close(self):
        while self.queue:
            ref=self.queue.pop()
            if ref not in self.inventory:continue
            try:value=parse(read(cas_path(self.cas,ref),MAX_BLOB))
            except (ValueError,UnicodeError):self.doubt('unknown_document:'+ref);continue
            if not isinstance(value,dict):self.doubt('unknown_document:'+ref);continue
            schema=value.get('schema')
            if schema is not None and not isinstance(schema,str):
                self.doubt('unknown_document:'+ref);continue
            if schema in CHILDREN:
                if any(key.endswith(('_ref','_refs','_hash','_hashes')) and key not in CHILDREN[schema]
                       for key in value):
                    self.doubt('unknown_document_fields:'+ref)
                for field in CHILDREN[schema]:
                    self.add(value.get(field),ref+'.'+field,'raw' if field in ('stdout_ref','init_ref') else 'document')
            elif schema not in LEAVES and not (value.get('synthetic') is True and set(value)<= {'synthetic','text'}):
                self.doubt('unknown_schema:'+str(schema))
        return self
