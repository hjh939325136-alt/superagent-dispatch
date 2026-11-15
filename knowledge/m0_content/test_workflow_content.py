"""Actual synthetic catalog → context → content; SQLite commits, not chain fixtures."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from knowledge.m0_content.contract import Contract, ContractError, digest, timestamp
from knowledge.m0_content.workflow_binding import (WorkflowError, initialize_binding, load_workflow_binding,
    request_base, search_modules)
from knowledge.m0_content.workflow_guard import dependency_guard
from knowledge.m0_content.workflow_content import (approval_status, initialize_workflow, lookup_content,
    write_content)
from tools.m0_manager.content_sources import content_query

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = 'content.script_from_context'
APPROVE = 'content.approval_record'


def setup(root):
    bound, catalog, search, workflow = search_modules()
    catalog.initialize(root/'research', operator_grant=catalog.OPERATOR)
    bound.initialize_binding(root/'research', root/'research-binding.json')
    research = bound.load_binding(root/'research-binding.json')
    with closing(Contract(root/'content.sqlite3', create=True)):
        pass
    initialize_workflow(root/'content.sqlite3')
    binding = initialize_binding(research.path, root/'content.sqlite3', root/'workflow.json')
    workflow.initialize_receipts(research, operator_grant=catalog.OPERATOR)
    base = request_base(timestamp())
    results = search.search(base | {'q': '剪辑 返工', 'limit': 50}, research.grant, catalog=research.catalog())
    control = search.search(base | {'q': 'control-07', 'limit': 50}, research.grant, catalog=research.catalog())
    refs = sorted({item['ref'] for item in results['results'] + control['results'] if item['status'] == 'current'})
    if not refs:
        raise AssertionError('Actual search returned no fixtures')
    receipt = workflow.assemble(research, base | {'refs': refs}, 'assemble-1', 'sha256:'+digest(base | {'refs': refs}))
    params = {'trace_id': binding.allowed_traces[0], 'context_id': receipt['context_id'],
              'manifest_sha256': receipt['manifest_sha256'], 'template_id': binding.template_id}
    return binding, params


def invoke(binding, op, params, key, request_hash=None):
    hashed = request_hash or 'sha256:'+digest({'operation': op, 'params': params, 'write_key': key})
    with dependency_guard(binding) as guard:
        return write_content(binding, op, params, key, hashed, guard)


WORKER = r'''
import json,os,sys,time
from pathlib import Path
from knowledge.m0_content.workflow_binding import load_workflow_binding
from knowledge.m0_content import workflow_content as writer
from knowledge.m0_content.test_workflow_content import invoke
binding=load_workflow_binding(sys.argv[1]); params=json.loads(sys.argv[2]); mode=sys.argv[4]
def hook(stage):
    if stage==mode:os._exit(23)
    if mode=='hold' and stage=='before_commit':
        Path(sys.argv[5]).write_text('ready')
        while not Path(sys.argv[6]).exists():time.sleep(.02)
writer._checkpoint=hook
result=invoke(binding,'content.script_from_context',params,sys.argv[3])
print(json.dumps(result))
'''


class WorkflowContentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='m0-workflow-content-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).absolute()
        self.binding, self.params = setup(self.root)

    def counts(self):
        with closing(sqlite3.connect(self.binding.content_path)) as db:
            return {name: db.execute('SELECT count(*) FROM '+name).fetchone()[0]
                    for name in ('artifacts', 'approvals', 'operation_receipts', 'content_events', 'external_contexts')}

    def hashes(self):
        return {str(path): (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
                for path in [self.binding.content_path, *self.binding.research.catalog_root.glob('*.sqlite3')]}

    def approve(self, script, key='approve-1'):
        params = {k:v for k,v in self.params.items() if k != 'template_id'} | {
            'script_ref': script['result']['script_ref'], 'control_approval': {
                'approval_id': key, 'task_id': key+'-task', 'scope': 'script_ready',
                'actor': 'synthetic-operator', 'proof_hash': 'sha256:'+digest(key),
                'expires_at': (datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()}}
        return invoke(self.binding, APPROVE, params, key), params

    def revoke(self):
        _, catalog, _, _ = search_modules()
        return self.binding.research.catalog().apply_batch('research-scatter', 'revoke-1', 'revoke_open',
                                                        operator_grant=catalog.OPERATOR)

    def query(self, *, bound=True):
        source = {'source_id': 'content', 'kind': 'content', 'locator': str(self.binding.content_path),
                  'source_instance': self.binding.content_instance, 'accepted_schema_versions': ['m0-content-synthetic-v1'],
                  'options': {'trace_ids': [self.params['trace_id']]}}
        if bound:
            source['options']['workflow_binding'] = str(self.binding.path)
        return content_query(source, 'content.status', {}, 'technical')

    def worker(self, key, mode='normal', extra=()):
        return subprocess.Popen([sys.executable, '-B', '-c', WORKER, str(self.binding.path), json.dumps(self.params),
                                 key, mode, *map(str,extra)], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def test_actual_search_refs_template_and_same_key_exact_receipt(self):
        first = invoke(self.binding, SCRIPT, self.params, 'script-1')
        before = self.counts()
        self.assertEqual(first, invoke(self.binding, SCRIPT, self.params, 'script-1'))
        self.assertEqual(before, self.counts())
        self.assertEqual(first['result']['script_version'], 1)
        self.assertEqual(first['result']['generation'], 'template_synthetic')
        self.assertFalse(first['result']['ai_generated'])
        self.assertEqual({ref.split('/')[0] for ref in first['result']['research_refs']},
                         {'research-scatter','research-content','signalhub'})
        self.assertEqual(self.query()['items'][0]['state'],'review_pending')
        self.assertIsNone(first['operation_receipt']['committed_fence'])
        with closing(Contract(self.binding.content_path)) as content:
            body = content.get(first['result']['context_ref'])['body']
            self.assertEqual(body['external_context']['refs'], first['result']['research_refs'])
            self.assertNotIn('text', body)
            self.assertNotIn('body', body['external_context'])

    def test_alias_receipt_is_durable_without_new_version(self):
        first = invoke(self.binding, SCRIPT, self.params, 'script-1')
        alias = invoke(self.binding, SCRIPT, self.params, 'script-2')
        self.assertEqual(first['result'], alias['result'])
        self.assertEqual(alias['operation_receipt']['reused_from'], 'script-1')
        self.assertEqual(self.counts(), {'artifacts': 2, 'approvals': 0, 'operation_receipts': 2,
                                        'content_events': 1, 'external_contexts': 1})
        with dependency_guard(self.binding) as guard:
            lookup = lookup_content(self.binding, SCRIPT, 'script-2', alias['request_hash'], guard)
        self.assertEqual(alias, lookup)

    def test_key_conflict_and_task_fields_guard_forgery_rejected(self):
        first = invoke(self.binding, SCRIPT, self.params, 'script-1')
        before = self.hashes()
        with self.assertRaises(WorkflowError):
            invoke(self.binding, SCRIPT, self.params, 'script-1', 'sha256:'+'0'*64)
        for extra in ({'path': 'other'}, {'body': 'untrusted'}, {'guard': {'usable': True}}):
            with self.assertRaises(WorkflowError):
                invoke(self.binding, SCRIPT, self.params | extra, 'malicious')
        with self.assertRaises(WorkflowError):
            write_content(self.binding, SCRIPT, self.params, 'forged', first['request_hash'], {'active': True})
        self.assertEqual(before, self.hashes())
        self.assertEqual(lookup_content(self.binding, SCRIPT, 'script-1', 'sha256:'+'0'*64), {'state':'conflict'})

    def test_lookup_different_binding_reports_instance_mismatch(self):
        first=invoke(self.binding,SCRIPT,self.params,'original')
        changed=replace(self.binding,digest='sha256:'+'0'*64)
        self.assertEqual(lookup_content(changed,SCRIPT,'original',first['request_hash']),{'state':'instance_mismatch'})

    def test_control_approval_is_bound_and_not_shared_across_tasks(self):
        script = invoke(self.binding, SCRIPT, self.params, 'script-1')
        first, params = self.approve(script)
        replay = invoke(self.binding, APPROVE, params, 'approve-1')
        self.assertEqual(first, replay)
        second, _ = self.approve(script, 'approve-2')
        self.assertNotEqual(first['result']['approval_ref'], second['result']['approval_ref'])
        self.assertEqual(self.counts()['approvals'], 2)
        with closing(Contract(self.binding.content_path)) as content:
            self.assertEqual(content.get(first['result']['approval_ref'])['body']['control_approval'], params['control_approval'])
        with self.assertRaises(WorkflowError):
            invoke(self.binding, APPROVE, params | {'control_approval': params['control_approval'] | {'actor':'human'}}, 'bad')
        with self.assertRaises(WorkflowError):
            invoke(self.binding, APPROVE, {k:v for k,v in params.items() if k!='control_approval'}, 'no-control')

    def test_wrong_manifest_and_closed_guard_leave_no_content_write(self):
        before=self.hashes()
        with self.assertRaises(ValueError):
            invoke(self.binding,SCRIPT,self.params|{'manifest_sha256':'0'*64},'wrong-manifest')
        with dependency_guard(self.binding) as guard:
            pass
        with self.assertRaises(WorkflowError):
            write_content(self.binding,SCRIPT,self.params,'closed','sha256:'+'0'*64,guard)
        self.assertEqual(before,self.hashes())

    def test_control_expiry_at_decision_and_before_commit_rolls_back(self):
        script=invoke(self.binding,SCRIPT,self.params,'script-1')
        _,params=self.approve(script)
        before=self.counts()
        expired=params|{'control_approval':params['control_approval']|{'expires_at':'2000-01-01T00:00:00+00:00'}}
        with self.assertRaises(WorkflowError) as caught:invoke(self.binding,APPROVE,expired,'expired')
        self.assertEqual(caught.exception.code,'CONTROL_APPROVAL_EXPIRED')
        soon=params|{'control_approval':params['control_approval']|{
            'expires_at':(datetime.now(timezone.utc)+timedelta(seconds=.5)).isoformat()}}
        def delay(stage):
            if stage=='before_commit':time.sleep(.65)
        with patch('knowledge.m0_content.workflow_content._checkpoint',side_effect=delay):
            with self.assertRaises(WorkflowError) as caught:invoke(self.binding,APPROVE,soon,'expired-before-commit')
        self.assertEqual(caught.exception.code,'CONTROL_APPROVAL_EXPIRED')
        self.assertEqual(before,self.counts())

    def test_committed_control_expiry_is_current_unusable_and_history_unchanged(self):
        script=invoke(self.binding,SCRIPT,self.params,'script-1')
        _,params=self.approve(script)
        soon=params|{'control_approval':params['control_approval']|{
            'expires_at':(datetime.now(timezone.utc)+timedelta(seconds=.7)).isoformat()}}
        committed=invoke(self.binding,APPROVE,soon,'soon')
        time.sleep(.8)
        before=self.hashes()
        replay=invoke(self.binding,APPROVE,soon,'soon')
        self.assertEqual(committed['result'],replay['result'])
        self.assertEqual(replay['effective_state'],'expired')
        self.assertFalse(replay['current_usable'])
        managed=self.query()['items'][0]['approvals']
        self.assertEqual(next(x for x in managed if x['ref']==committed['result']['approval_ref'])['state'],'expired')
        self.assertEqual(before,self.hashes())

    def test_prior_committed_between_lookup_and_begin_survives_control_expiry(self):
        script=invoke(self.binding,SCRIPT,self.params,'script-1')
        _,params=self.approve(script)
        deadline=datetime.now(timezone.utc)+timedelta(seconds=1)
        params=params|{'control_approval':params['control_approval']|{'expires_at':deadline.isoformat()}}
        committed=invoke(self.binding,APPROVE,params,'interleaved-approval')
        time.sleep(max(0,(deadline-datetime.now(timezone.utc)).total_seconds())+.05)
        before_counts,before_hashes=self.counts(),self.hashes()
        # Model only the two interleaving observations: this writer's initial
        # lookup was absent and its pre-lock authorization passed. The real DB
        # now has the other writer's committed key, and actual UTC expiry has
        # elapsed while the first writer would have waited for BEGIN IMMEDIATE.
        # Any attempt to take the new-write commit path must fail this test.
        with patch('knowledge.m0_content.workflow_content.lookup_content',return_value={'state':'absent'}) as lookup, \
             patch('knowledge.m0_content.workflow_content._control_expiry',
                   side_effect=[None,AssertionError('historical prior must bypass final write expiry')]) as expiry, \
             patch('knowledge.m0_content.workflow_content._checkpoint',
                   side_effect=AssertionError('historical prior must not enter new-write checkpoints')):
            with dependency_guard(self.binding) as guard:
                replay=write_content(self.binding,APPROVE,params,'interleaved-approval',committed['request_hash'],guard)
        self.assertEqual(lookup.call_count,1)
        self.assertEqual(expiry.call_count,1)
        self.assertEqual(replay['state'],'committed')
        self.assertEqual(replay['result'],committed['result'])
        self.assertEqual(replay['operation_receipt'],committed['operation_receipt'])
        self.assertEqual(replay['committed_at'],committed['committed_at'])
        self.assertEqual(replay['effective_state'],'expired')
        self.assertFalse(replay['current_usable'])
        self.assertNotIn('commit_state',replay)
        self.assertEqual(before_counts,self.counts())
        self.assertEqual(before_hashes,self.hashes())

    def test_stale_script_alias_cannot_reactivate_prior_approval(self):
        original=invoke(self.binding,SCRIPT,self.params,'original')
        approved,_=self.approve(original)
        _,_,_,workflow=search_modules()
        base=request_base(timestamp())
        context=workflow.assemble(self.binding.research,base|{'refs':original['result']['research_refs']},'new-context','sha256:'+digest(base))
        params=self.params|{'context_id':context['context_id'],'manifest_sha256':context['manifest_sha256']}
        updated=invoke(self.binding,SCRIPT,params,'updated')
        self.assertEqual(updated['result']['script_version'],2)
        before=self.counts()
        with self.assertRaises(WorkflowError):invoke(self.binding,SCRIPT,self.params,'stale-alias')
        self.assertEqual(before,self.counts())
        with dependency_guard(self.binding) as guard:
            old=lookup_content(self.binding,APPROVE,'approve-1',approved['request_hash'],guard)
        self.assertEqual(old['state'],'committed')
        self.assertFalse(old['current_usable'])
        self.assertEqual(old['effective_state'],'stale')

    def test_missing_external_registration_fails_closed(self):
        script=invoke(self.binding,SCRIPT,self.params,'script-1')
        approval,_=self.approve(script)
        with closing(sqlite3.connect(self.binding.content_path)) as db,db:
            db.execute('DELETE FROM external_contexts')
        with dependency_guard(self.binding) as guard:
            lookup=lookup_content(self.binding,APPROVE,'approve-1',approval['request_hash'],guard)
        self.assertEqual(lookup['state'],'committed')
        self.assertFalse(lookup['current_usable'])
        self.assertFalse(self.query()['items'][0]['approvals'][0]['usable'])

    def test_initialize_rejects_unmarked_database_without_changes(self):
        path=self.root/'unmarked.sqlite3'
        with closing(sqlite3.connect(path)) as db:
            db.execute('CREATE TABLE real_data(value TEXT)')
        before=path.read_bytes()
        with self.assertRaises((sqlite3.Error,WorkflowError)):
            initialize_workflow(path)
        self.assertEqual(before,path.read_bytes())

    def test_revocation_preserves_historical_receipt_but_invalidates_both_readers(self):
        script = invoke(self.binding, SCRIPT, self.params, 'script-1')
        approval, params = self.approve(script)
        self.assertTrue(self.query()['items'][0]['approvals'][0]['usable'])
        self.assertFalse(self.query(bound=False)['items'][0]['approvals'][0]['usable'])
        self.revoke()
        before = self.hashes()
        with dependency_guard(self.binding) as guard:
            historical = lookup_content(self.binding, APPROVE, 'approve-1', approval['request_hash'], guard)
            status = approval_status(self.binding, {k:v for k,v in params.items() if k!='control_approval'} |
                                     {'approval_ref':approval['result']['approval_ref']}, guard)
        managed = self.query()['items'][0]['approvals'][0]
        self.assertEqual(historical['state'], 'committed')
        self.assertEqual(historical['result'], approval['result'])
        self.assertEqual(historical['effective_state'], 'stale_by_revocation')
        self.assertFalse(status['usable'])
        self.assertEqual(managed['state'], 'stale_by_revocation')
        self.assertFalse(managed['usable'])
        with self.assertRaises(WorkflowError):
            invoke(self.binding, SCRIPT, self.params, 'script-new')
        self.assertEqual(before, self.hashes())
        with dependency_guard(self.binding) as guard:
            replay=write_content(self.binding,APPROVE,params,'approve-1',approval['request_hash'],guard)
        self.assertEqual(replay['result'],approval['result'])
        self.assertFalse(replay['current_usable'])

    def test_unbound_contract_never_marks_external_approval_current(self):
        script = invoke(self.binding, SCRIPT, self.params, 'script-1')
        approved, _ = self.approve(script)
        with closing(Contract(self.binding.content_path)) as content:
            with self.assertRaises(ContractError) as caught:
                content.verify_approval(approved['result']['approval_ref'],script['result']['script_ref'],'script_ready',1)
            self.assertEqual(caught.exception.code,'EXTERNAL_DEPENDENCY_UNVERIFIED')

    def test_killed_before_commit_rolls_back_all_and_after_commit_lookup_survives(self):
        before = self.counts()
        child = self.worker('crash-before', 'after_artifacts')
        out,err = child.communicate(timeout=20)
        self.assertEqual(child.returncode,23,(out,err))
        self.assertEqual(before,self.counts())
        child = self.worker('crash-after', 'after_commit')
        out,err = child.communicate(timeout=20)
        self.assertEqual(child.returncode,23,(out,err))
        hashed='sha256:'+digest({'operation':SCRIPT,'params':self.params,'write_key':'crash-after'})
        with dependency_guard(self.binding) as guard:
            result=lookup_content(self.binding,SCRIPT,'crash-after',hashed,guard)
        self.assertEqual(result['state'],'committed')
        self.assertEqual(self.counts()['artifacts'],2)

    def test_concurrent_same_semantics_different_keys_commit_alias(self):
        children=[self.worker(key) for key in ('race-1','race-2')]
        results=[]
        for child in children:
            out,err=child.communicate(timeout=25)
            self.assertEqual(child.returncode,0,(out,err))
            results.append(json.loads(out))
        self.assertEqual(results[0]['result'],results[1]['result'])
        self.assertEqual(self.counts()['artifacts'],2)
        self.assertEqual(self.counts()['operation_receipts'],2)

    def test_commit_holds_research_lock_until_then_revocation_invalidates(self):
        ready,release,revoked_write=self.root/'ready',self.root/'release',self.root/'revoker-written'
        revoked_done=self.root/'revoker-committed'
        child=self.worker('held','hold',(ready,release))
        try:
            end=time.monotonic()+15
            while not ready.exists() and child.poll() is None and time.monotonic()<end:
                time.sleep(.03)
            if not ready.exists():
                if child.poll() is None:child.kill()
                out,err=child.communicate(timeout=5)
                self.fail(f'content writer never held transaction: rc={child.returncode}, stdout={out!r}, stderr={err!r}')
            code="from knowledge.m0_content.workflow_binding import load_workflow_binding,search_modules;from pathlib import Path;import sys; b=load_workflow_binding(sys.argv[1]);c=search_modules()[1]; b.research.catalog().apply_batch('research-scatter','revoke-1','revoke_open',operator_grant=c.OPERATOR,_after_write=lambda count:Path(sys.argv[2]).write_text(str(count)));Path(sys.argv[3]).write_text('committed')"
            revoker=subprocess.Popen([sys.executable,'-B','-c',code,str(self.binding.path),str(revoked_write),str(revoked_done)],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                # Observe a real write inside the lifecycle transaction before
                # testing its blocked commit; interpreter startup is no proof.
                end=time.monotonic()+15
                while not revoked_write.exists() and revoker.poll() is None and time.monotonic()<end:
                    time.sleep(.02)
                if not revoked_write.exists():
                    if revoker.poll() is None:revoker.kill()
                    out,err=revoker.communicate(timeout=5)
                    self.fail(f'revoker never reached write transaction: rc={revoker.returncode}, stdout={out!r}, stderr={err!r}')
                try:
                    revoker.wait(timeout=.3)
                except subprocess.TimeoutExpired:
                    pass
                else:
                    out,err=revoker.communicate(timeout=5)
                    self.fail(f'revoker committed/exited before research guard release: rc={revoker.returncode}, stdout={out!r}, stderr={err!r}')
                self.assertEqual(revoked_write.read_text(),'1')
                self.assertFalse(revoked_done.exists())
                release.write_text('continue')
                out,err=child.communicate(timeout=20)
                self.assertEqual(child.returncode,0,(out,err))
                out,err=revoker.communicate(timeout=20)
                self.assertEqual(revoker.returncode,0,(out,err))
                self.assertEqual(revoked_done.read_text(),'committed')
            finally:
                if revoker.poll() is None:revoker.kill()
                revoker.communicate(timeout=5)
        finally:
            if child.poll() is None:child.kill()
            child.communicate(timeout=5)
        with self.assertRaises(WorkflowError):invoke(self.binding,SCRIPT,self.params,'after-revocation')
        print('lock_order_evidence='+json.dumps({'writer_ready':True,'revoker_after_write':True,
              'revoker_done_before_release':False,'revoker_done_after_release':True,
              'writer_rc':child.returncode,'revoker_rc':revoker.returncode,'new_write_after_revocation':'rejected'}))


if __name__ == '__main__':
    unittest.main()
