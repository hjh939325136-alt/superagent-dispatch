"""Exercise the exact public JSON CLI in separate local synthetic processes."""
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.control_cli import REQUEST_SCHEMA
from sa_dispatch.models import Outcome, TaskSpec
from sa_dispatch.store_sqlite import SqliteStore


class ControlCLITests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix='sa-control-cli-')
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.ledger, self.artifact_root = self.root/'ledger.sqlite3', self.root/'artifacts'
        self.store = SqliteStore(self.ledger, lease_ttl_seconds=30)
        self.artifacts = ArtifactStore(self.artifact_root)
        self.start = (datetime.now(timezone.utc)-timedelta(days=1)).isoformat()
        self.end = (datetime.now(timezone.utc)+timedelta(days=1)).isoformat()
        self.command_count = 0

    def request(self, operation, params):
        return {'schema':REQUEST_SCHEMA, 'synthetic':True, 'operation':operation, 'params':params}

    def invoke(self, operation, params=None, *, success=True, raw=None, ledger=None):
        self.command_count += 1
        path = self.root/('request-'+str(self.command_count)+'.json')
        path.write_text(raw if raw is not None else json.dumps(self.request(operation, params or {})), encoding='utf-8')
        run = subprocess.run([sys.executable, '-B', '-m', 'sa_dispatch.control_cli',
            '--ledger', str(ledger or self.ledger), '--artifacts', str(self.artifact_root), '--request', str(path)],
            cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True)
        result = json.loads(run.stdout)
        self.assertEqual(run.stderr, '')
        self.assertEqual(run.returncode == 0, success, result)
        self.assertEqual(result['ok'], success)
        self.assertEqual(result['messages_sent'], 0)
        return result

    def proof(self, value):
        return self.artifacts.put(json.dumps({'synthetic':True, **value},sort_keys=True).encode())

    def test_real_cli_budget_approval_settlement_and_handoff_flow(self):
        task = self.store.submit(TaskSpec('cli-work','fixture.read',1,{},(),('cpu',),'synthetic','none',2))[0]
        self.store.register_device('worker', ('cpu',))
        self.invoke('budget_upsert', {'budget_id':'day','period':'fixture-period','unit':'fixture_units',
            'limit_units':10,'starts_at':self.start,'ends_at':self.end})
        self.invoke('bind_task', {'task_id':task.task_id,'budget_id':'day','reserve_units':4,'approval_scope':'fixture.execute'})
        self.assertIsNone(self.store.claim('worker'))
        approval = self.proof({'task_id':task.task_id,'fingerprint':task.fingerprint,'scope':'fixture.execute',
                              'decision':'approved','actor':'reviewer','expires_at':self.end})
        decided = self.invoke('approval_decide', {'task_id':task.task_id,'scope':'fixture.execute','decision':'approved',
            'actor':'reviewer','evidence_ref':approval,'expires_at':self.end})
        self.assertTrue(decided['result']['approval_id'].startswith('ap_'))
        lease = self.store.claim('worker')
        self.assertIsNotNone(lease)
        self.store.complete(lease.lease_id,lease.fence,Outcome(),'worker')
        usage = self.proof({'task_id':task.task_id,'fence':lease.fence,'budget_id':'day',
                           'fingerprint':task.fingerprint,'actual_units':2,'final_usage':True})
        self.invoke('settle', {'task_id':task.task_id,'fence':lease.fence,'actual_units':2,
                              'evidence_ref':usage,'actor':'fixture-accountant'})
        auth = self.proof({'handoff_id':'cli-handoff','from_conv':'fixture-sender','to_conv':'fixture-recipient',
            'project_id':'fixture-project','scope':'takeover','scope_refs':[],'authorized_by':'fixture-user','expires_at':self.end})
        self.invoke('handoff_offer', {'handoff_id':'cli-handoff','from_conv':'fixture-sender','to_conv':'fixture-recipient',
            'project_id':'fixture-project','scope':'takeover','expires_at':self.end,'actor':'fixture-sender','authorization_ref':auth})
        self.invoke('handoff_transition', {'handoff_id':'cli-handoff','state':'accepted','actor':'fixture-recipient'})
        output = self.artifacts.put(b'only synthetic completed work')
        receipt = self.proof({'handoff_id':'cli-handoff','recipient':'fixture-recipient','decision':'completed','evidence_refs':[output]})
        self.invoke('handoff_transition', {'handoff_id':'cli-handoff','state':'completed','actor':'fixture-recipient','receipt_ref':receipt})
        status = self.invoke('status')['result']
        self.assertEqual((status['budgets'][0]['reserved_units'],status['budgets'][0]['used_units']), (0,2))
        self.assertEqual(status['handoffs'][0]['state'], 'completed')
        with closing(sqlite3.connect(self.ledger)) as conn:
            self.assertEqual(conn.execute('SELECT actual_units,state FROM control_reservations').fetchall(), [(2,'settled')])
            self.assertEqual(conn.execute('SELECT state,receipt_ref FROM control_handoffs').fetchall(), [('completed',receipt)])
        evidence = os.environ.get('SA_CONTROL_CLI_EVIDENCE_DIR')
        if evidence:
            folder=Path(evidence); folder.mkdir(parents=True,exist_ok=True)
            with (folder/'public-control-cli-flow.json').open('x',encoding='utf-8') as stream:
                json.dump({'synthetic_only':True,'subprocess_commands':self.command_count,'all_seven_operations':True,
                           'messages_sent':0,'database_verified':True,'policy':status},stream,indent=2)

    def test_missing_authorization_returns_structured_failure_and_no_handoff(self):
        result = self.invoke('handoff_offer', {'handoff_id':'denied','from_conv':'sender','to_conv':'recipient',
            'project_id':'fixture','scope':'instruct','expires_at':self.end,'actor':'sender'},success=False)
        self.assertEqual(result['error']['code'],'E_VALIDATION')
        self.assertEqual(self.invoke('status')['result']['handoffs'],[])

    def test_unknown_method_and_unexpected_params_are_rejected(self):
        for operation, params in (('__getattribute__',{}), ('status',{'anything':'denied'}), ('budget_upsert',{})):
            self.assertEqual(self.invoke(operation,params,success=False)['error']['code'],'E_CONTROL_REQUEST')

    def test_malformed_duplicate_oversized_json_and_synthetic_false_are_rejected(self):
        cases = ('{', '{"schema":"a","schema":"b"}', ' '*65537,
                 json.dumps({**self.request('status',{}),'synthetic':False}))
        before = self.ledger.read_bytes()
        for raw in cases:
            self.assertEqual(self.invoke('status',raw=raw,success=False)['error']['code'],'E_CONTROL_REQUEST')
        self.assertEqual(self.ledger.read_bytes(),before)

    def test_missing_or_non_synthetic_ledger_is_not_created_or_changed(self):
        missing=self.root/'missing.sqlite3'
        self.invoke('status',ledger=missing,success=False)
        self.assertFalse(missing.exists())
        self.store.submit(TaskSpec('real-denied','fixture.read',1,{},(),(),'local_only','none',1))
        before = self.ledger.read_bytes()
        result = self.invoke('status',success=False)
        self.assertIn('synthetic',result['error']['message'])
        self.assertEqual(self.ledger.read_bytes(),before)

    def test_status_commits_expiry_without_sending_a_message(self):
        self.invoke('handoff_offer',{'handoff_id':'expire','from_conv':'sender','to_conv':'recipient',
            'project_id':'fixture','scope':'read_status','expires_at':self.end,'actor':'sender'})
        with closing(sqlite3.connect(self.ledger)) as conn, conn:
            conn.execute("UPDATE control_handoffs SET expires_at='2000-01-01T00:00:00+00:00'")
        result=self.invoke('status')
        self.assertTrue(result['status_refreshes_handoff_expiry'])
        self.assertEqual(result['result']['handoffs'][0]['state'],'expired')
        with closing(sqlite3.connect(self.ledger)) as conn:
            self.assertEqual(conn.execute('SELECT state FROM control_handoffs').fetchone()[0],'expired')

    def test_normal_dispatch_cli_uses_the_same_handler(self):
        config = self.root/'dispatch.toml'
        config.write_text('\n'.join([
            'db_path = '+json.dumps(str(self.ledger)),
            'artifact_root = '+json.dumps(str(self.artifact_root)),
            'journal_dir = '+json.dumps(str(self.root/'journal')),
            'device_id = "fixture-cli"','capabilities = ["cpu"]','lease_ttl_seconds = 60',
            'component_timeout_seconds = 5','plugins = []','synthetic = true','[components]'])+'\n',encoding='utf-8')
        request=self.root/'normal.json'
        request.write_text(json.dumps(self.request('status',{})),encoding='utf-8')
        run=subprocess.run([sys.executable,'-B','-m','sa_dispatch','--config',str(config),
                            'control','--input',str(request)],cwd=Path(__file__).resolve().parents[1],capture_output=True,text=True)
        self.assertEqual(run.returncode,0,run.stderr+run.stdout)
        result=json.loads(run.stdout)
        self.assertTrue(result['ok'])
        self.assertEqual(result['data']['operation'],'status')
        self.assertEqual(result['data']['messages_sent'],0)


if __name__ == '__main__': unittest.main()
