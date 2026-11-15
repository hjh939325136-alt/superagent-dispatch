"""Independent v3 compatibility and complete synthetic host/control evidence."""
from dataclasses import replace
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import unittest
import uuid

import test_host_adapter as host_fixture
from sa_dispatch.artifacts import ArtifactError,ArtifactStore
from sa_dispatch.bundle import (FORMAT,V2_FORMAT,V2_TABLES,TABLES,export_bundle,import_bundle,
                                ledger,read_bundle)
from sa_dispatch.canonical import canonical_json,sha256_ref
from sa_dispatch.clock import timestamp
from sa_dispatch.config import Config
from sa_dispatch.control_policy import ControlPolicy
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.models import DispatchError,Evidence,Outcome,Ref,TaskSpec
from sa_dispatch.store_sqlite import SqliteStore


class BundleV3Tests(unittest.TestCase):
    def setUp(self):
        host_fixture.HostTests.setUp(self)
        self.config=Config(self.store.path,self.artifacts.root,self.root/'journal',
                           'host',('host.fixture',),10,5,(),{},True)
        self.policy=ControlPolicy(self.store,artifacts=self.artifacts)
        now=datetime.now(timezone.utc)
        self.start=timestamp(now-timedelta(minutes=1)); self.end=timestamp(now+timedelta(days=1))

    def proof(self,value):
        return self.artifacts.put(canonical_json({'synthetic':True,**value}))

    def target(self):
        config=replace(self.config,db_path=self.root/'target.db',artifact_root=self.root/'target-cas',
                       journal_dir=self.root/'target-journal')
        return config,SqliteStore(config.db_path,lease_ttl_seconds=10)

    def plain(self,key='plain'):
        return self.store.submit(TaskSpec(key,'fixture.read',1,{},(),('host.fixture',),
                                          'synthetic','none',2))[0]

    def complete_plain(self):
        task=self.plain(); lease=self.store.claim('host')
        ref=self.proof({'fixture_result':'complete'})
        self.store.complete(lease.lease_id,lease.fence,
                            Outcome(evidence=(Evidence('fixture',ref,'synthetic',artifact_hash=ref),)),'host')
        return task,ref

    def test_actual_v2_layout_and_0007_import_into_0008(self):
        task,ref=self.complete_plain()
        source=ledger(self.store.path)
        rows={name:source[name] for name in V2_TABLES}
        rows['schema_migrations']=[row for row in rows['schema_migrations'] if row['version']=='0007']
        self.assertEqual(len(rows['schema_migrations']),1)
        package=self.root/'actual-v2'; package.mkdir()
        files={}
        for table,records in rows.items():
            data=b''.join(receipt_bytes(record)+b'\n' for record in records)
            (package/(table+'.jsonl')).write_bytes(data); files[table+'.jsonl']=sha256_ref(data)
        ArtifactStore(package/'artifacts').put(self.artifacts.get(ref))
        manifest={'schema':V2_FORMAT,'files':files,'artifacts':[ref],
                  'import_policy':{'schedules':'disabled','devices':'excluded'},
                  'source_counts':{table:len(records) for table,records in rows.items()}}
        (package/'manifest.json').write_bytes(receipt_bytes(manifest))
        read,_,_=read_bundle(package)
        self.assertEqual(read['schema'],V2_FORMAT)
        config,target=self.target()
        report=import_bundle(config,package,target)
        restored=ledger(config.db_path)
        self.assertEqual(target.get_task(task.task_id).status,'succeeded')
        self.assertEqual({row['version'] for row in restored['schema_migrations']},{'0007','0008','0009','0010','0011'})
        self.assertTrue(report['target_migration_metadata_retained'])
        for table in set(TABLES)-set(V2_TABLES): self.assertEqual(restored[table],[],table)
        self.assertEqual(ArtifactStore(config.artifact_root).get(ref),self.artifacts.get(ref))

    def settled_host_control(self):
        prompt=self.proof({'text':'synthetic bundle audit'})
        task=self.store.submit(TaskSpec('host-bundle','host.synthetic',1,{'prompt_ref':prompt,'agent_id':'lead'},
            (Ref('dispatch','prompt',prompt[7:],'1',prompt),),('host.fixture',),'synthetic','none',2))[0]
        self.policy.budget_upsert('fixture-budget',period='fixture-day',unit='units',limit_units=5,
                                 starts_at=self.start,ends_at=self.end)
        approval=self.proof({'task_id':task.task_id,'fingerprint':task.fingerprint,'scope':'host.fixture',
                             'decision':'approved','actor':'reviewer','expires_at':self.end})
        approval_id=self.policy.approval_decide(task.task_id,scope='host.fixture',decision='approved',
            actor='reviewer',evidence_ref=approval,expires_at=self.end)
        self.policy.bind_task(task.task_id,budget_id='fixture-budget',reserve_units=3,
                              approval_scope='host.fixture',approval_id=approval_id)
        lease=self.store.claim('host'); report=self.adapter.run(lease)
        self.assertEqual(report['status'],'completed')
        report_ref=self.adapter.attempt(task.task_id,lease.fence)['report_ref']
        usage=self.proof({'task_id':task.task_id,'fence':lease.fence,'budget_id':'fixture-budget',
                         'fingerprint':task.fingerprint,'actual_units':2,'final_usage':True})
        self.policy.settle(task.task_id,lease.fence,actual_units=2,evidence_ref=usage,actor='fixture-meter')
        scope=self.artifacts.put(b'synthetic handoff scope')
        authorization=self.proof({'handoff_id':'handoff-one','from_conv':'sender','to_conv':'receiver',
            'project_id':'fixture-project','scope':'instruct','scope_refs':[scope],
            'authorized_by':'fixture-user','expires_at':self.end})
        self.policy.handoff_offer('handoff-one',from_conv='sender',to_conv='receiver',project_id='fixture-project',
            scope='instruct',scope_refs=[scope],expires_at=self.end,actor='sender',authorization_ref=authorization)
        self.policy.handoff_transition('handoff-one','accepted',actor='receiver')
        receipt=self.proof({'handoff_id':'handoff-one','recipient':'receiver','decision':'completed','evidence_refs':[report_ref]})
        self.policy.handoff_transition('handoff-one','completed',actor='receiver',receipt_ref=receipt)
        return {prompt,approval,report_ref,usage,scope,authorization,receipt}

    def test_v3_host_control_cas_survives_without_original_source(self):
        expected=self.settled_host_control()
        before=ledger(self.store.path)
        expected.update(row['event_ref'] for row in before['host_events'])
        package=self.root/'bundle-v3'; manifest=export_bundle(self.config,package)
        self.assertEqual(manifest['schema'],FORMAT)
        self.assertTrue(expected<=set(manifest['artifacts']))
        original={ref:self.artifacts.get(ref) for ref in manifest['artifacts']}
        self.store.path.rename(self.root/'source-hidden.db')
        self.artifacts.root.rename(self.root/'source-cas-hidden')
        config,target=self.target(); import_bundle(config,package,target)
        after=ledger(config.db_path)
        for table in TABLES: self.assertEqual(after[table],before[table],table)
        for ref,content in original.items(): self.assertEqual(ArtifactStore(config.artifact_root).get(ref),content)
        self.assertEqual(ControlPolicy(target,artifacts=ArtifactStore(config.artifact_root)).status()['handoffs'][0]['state'],'completed')

    def test_missing_host_event_blob_rejects_package(self):
        self.settled_host_control()
        package=self.root/'missing-host-event'; export_bundle(self.config,package)
        event_ref=ledger(self.store.path)['host_events'][0]['event_ref']
        ArtifactStore(package/'artifacts').path(event_ref).unlink()
        with self.assertRaises(ArtifactError): read_bundle(package)

    def test_terminal_task_with_active_budget_reservation_cannot_migrate(self):
        task=self.plain()
        self.policy.budget_upsert('budget',period='day',unit='units',limit_units=5,starts_at=self.start,ends_at=self.end)
        self.policy.bind_task(task.task_id,budget_id='budget',reserve_units=3)
        lease=self.store.claim('host'); output=self.proof({'fixture_result':'complete'})
        self.store.complete(lease.lease_id,lease.fence,
            Outcome(evidence=(Evidence('fixture',output,'synthetic',artifact_hash=output),)),'host')
        package=self.root/'blocked-reservation'
        with self.assertRaisesRegex(ArtifactError,'unsettled budget'): export_bundle(self.config,package)
        self.assertFalse(package.exists())
        _,target=self.target()
        with self.assertRaisesRegex(DispatchError,'unsettled budget'): target.import_snapshot(self.store.snapshot())
        self.assertEqual(target.list_tasks(),[])

    def test_active_handoff_cannot_migrate(self):
        self.policy.handoff_offer('pending',from_conv='sender',to_conv='receiver',project_id='fixture-project',
            scope='read_status',expires_at=self.end,actor='sender')
        for state in ('offered','accepted'):
            if state=='accepted': self.policy.handoff_transition('pending','accepted',actor='receiver')
            package=self.root/('blocked-'+state)
            with self.assertRaisesRegex(ArtifactError,'active handoffs'): export_bundle(self.config,package)
            self.assertFalse(package.exists())


if __name__=='__main__': unittest.main()
