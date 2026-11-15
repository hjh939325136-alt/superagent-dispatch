"""Contract/security tests for the real JSON manager entry, synthetic sources."""
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'dispatch'))
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.models import TaskSpec
from tools.m0_manager.manager import run
from tools.m0_manager.readonly import readonly_db, SourceError


class ManagerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.db=self.root/'ledger.sqlite3'
        self.store=SqliteStore(self.db,lease_ttl_seconds=10)
        self.task,_=self.store.submit(TaskSpec('synthetic-manager','synthetic.read',1,{},(),(),'synthetic','none',2))
        info=self.db.stat()
        self.source={'source_id':'synthetic-ledger','source_instance':'synthetic-instance-1','kind':'dispatch',
            'locator':str(self.db),'accepted_schema_versions':['0010','0011','0012'],'class':'internal','freshness_s':3600,
            'options':{'identity':{'device':info.st_dev,'inode':info.st_ino}}}
        self.binding=self.root/'binding.json'
        self.save()

    def tearDown(self):
        self.temp.cleanup()

    def save(self):
        self.binding.write_text(json.dumps({'version':1,'sources':[self.source]}),encoding='utf-8')

    def query(self,op='status',role='manager',**args):
        return run({'op':op,'role':role,'args':args},self.binding)

    def snapshot(self):
        return {p.name:(hashlib.sha256(p.read_bytes()).hexdigest(),p.stat().st_mtime_ns)
                for p in self.root.glob('ledger.sqlite3*') if p.is_file()}

    def test_one_hundred_queries_leave_db_and_wal_bytes_mtimes_events_unchanged(self):
        with closing(sqlite3.connect(self.db)) as writer:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('UPDATE tasks SET updated_at=updated_at');writer.commit()
            before=self.snapshot()
            count=writer.execute('SELECT COUNT(*) FROM unified_events').fetchone()[0]
            for _ in range(100):
                for op in ('status','handoff.list','attention.list','events.query'):
                    self.assertTrue(self.query(op)['ok'])
                self.assertTrue(self.query('trace.get',task_id=self.task.task_id)['ok'])
            # -shm is SQLite's coordination area; DB/-wal must not be modified.
            after=self.snapshot()
            for name in before:
                if not name.endswith('-shm'):self.assertEqual(before[name],after[name])
            self.assertEqual(count,writer.execute('SELECT COUNT(*) FROM unified_events').fetchone()[0])

    def test_authorizer_denies_writes_attach_pragma_and_extension(self):
        with readonly_db(self.db) as db:
            for sql in ('UPDATE tasks SET status="cancelled"','CREATE TABLE wrong(a)',
                        "ATTACH DATABASE ':memory:' AS extra",'PRAGMA user_version=99',
                        "SELECT load_extension('invalid')"):
                with self.subTest(sql=sql),self.assertRaises(sqlite3.DatabaseError):db.execute(sql)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0],1)

    def test_old_schema_is_unsupported_and_not_migrated(self):
        with closing(sqlite3.connect(self.db)) as db,db:
            db.execute("DELETE FROM schema_migrations WHERE version>='0010'")
        before=self.snapshot()
        result=self.query()
        self.assertEqual(result['sources'][0]['unknown'],['source_schema_unsupported'])
        self.assertEqual(before,self.snapshot())

    def test_routing_schema_requires_real_tables_and_remains_readonly(self):
        with closing(sqlite3.connect(self.db)) as db, db:
            db.execute('DROP TABLE route_audit')
        before = self.snapshot()
        self.assertEqual(self.query()['sources'][0]['unknown'], ['source_schema_unsupported'])
        self.assertEqual(before, self.snapshot())

    def test_mixed_ledger_queries_keep_mode_and_rows_unchanged(self):
        from sa_dispatch.artifacts import ArtifactStore
        from sa_dispatch.canonical import canonical_json
        from sa_dispatch.routing import activate_mixed, activation_manifest
        self.store.cancel(self.task.task_id, 'synthetic-manager')
        artifacts = ArtifactStore(self.root / 'cas')
        with closing(self.store._connect()) as conn:
            proof = activation_manifest(conn, actor='synthetic-manager', bindings={})
        activate_mixed(self.db, actor='synthetic-manager', bindings={}, artifacts=artifacts,
                       evidence_ref=artifacts.put(canonical_json(proof)))
        before = self.snapshot()
        for op, args in [('status', {}), ('task.get', {'task_id': self.task.task_id}),
                         ('trace.get', {'task_id': self.task.task_id})]:
            with self.subTest(op=op):
                result = self.query(op, **args)
                self.assertTrue(result['ok'])
                self.assertEqual(result['sources'][0]['schema_version'], '0012')
        self.assertEqual(before, self.snapshot())
        self.assertEqual(self.store.routing_mode(), 'mixed')

    def test_expired_handoff_is_effective_only(self):
        at=(datetime.now(timezone.utc)-timedelta(days=2)).isoformat()
        with closing(sqlite3.connect(self.db)) as db,db:
            db.execute('INSERT INTO control_handoffs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       ('h1','a','b','p','read_status','[]',None,None,at,'offered',None,'[]',at,at))
        before=self.snapshot()
        item=self.query('handoff.list')['items'][0]
        self.assertEqual((item['state'],item['effective_state']),('offered','expired'))
        self.assertTrue(any(i.get('rule')=='A6@1' for i in self.query('attention.list')['items']))
        self.assertEqual(before,self.snapshot())

    def test_source_missing_emits_attention_and_no_empty_success_claim(self):
        self.db.rename(self.root/'hidden.sqlite3')
        result=self.query('attention.list')
        self.assertEqual(result['sources'][0]['coverage'],'unavailable')
        self.assertTrue(any(r['rule']=='A10@1' for r in result['items']))
        self.assertEqual(result['coverage'],'partial')

    def test_instance_replaced_is_rejected(self):
        self.source['options']['identity']['inode']+=1;self.save()
        self.assertEqual(self.query()['sources'][0]['unknown'],['source_instance_changed'])

    def test_request_cannot_choose_files_sql_commands_or_access_grants(self):
        for key in ('path','sql','command','locator','class','options'):
            with self.subTest(key=key),self.assertRaises(SourceError) as caught:
                self.query(**{key:'injected'})
            self.assertEqual(caught.exception.code,'E_FIELD_NOT_ALLOWED')

    def test_role_boundary_and_unbound_source_fail_closed(self):
        for role in ('technical','progress'):
            with self.assertRaises(SourceError) as caught:self.query('knowledge.get',role,ref='x')
            self.assertEqual(caught.exception.code,'E_ROLE_NOT_ALLOWED')
        with self.assertRaises(SourceError):self.query(source_id='unknown')
        with self.assertRaises(SourceError):self.query('inventory.search',source_id='synthetic-ledger')

    def test_trace_current_task_and_historical_events(self):
        result=self.query('trace.get',role='progress',task_id=self.task.task_id)
        self.assertEqual(result['items'][0]['state_basis'],'current')
        self.assertEqual(result['events'][0]['state_basis'],'historical')
        raw=json.dumps(result)
        for denied in (str(self.root),'params_json','scope_json','argv','reason'):
            self.assertNotIn(denied,raw)

    def test_unknown_age_threshold_and_technical_filter(self):
        old=(datetime.now(timezone.utc)-timedelta(hours=2)).isoformat()
        with closing(sqlite3.connect(self.db)) as db,db:
            db.execute("UPDATE tasks SET status='unknown',updated_at=?",(old,))
        self.assertTrue(any(i.get('rule')=='A1@1' for i in self.query('attention.list')['items']))
        technical=self.query('attention.list','technical')
        self.assertFalse(any(i.get('rule')=='A1@1' for i in technical['items']))
        self.assertGreater(technical['withheld']['by_role'],0)

    def test_malformed_operation_is_structured_rejection(self):
        for field in ('op','role'):
            for value in ([],{},True,None):
                req={'op':'status','role':'manager','args':{}};req[field]=value
                with self.assertRaises(SourceError):run(req,self.binding)

    def test_unpinned_instance_rejected_and_wrong_trace_ref_refused(self):
        self.source['options'].pop('identity');self.save()
        self.assertEqual(self.query()['sources'][0]['unknown'],['source_identity_unpinned'])

    def test_progress_handoff_and_attention_hide_path_like_identifiers(self):
        at=(datetime.now(timezone.utc)-timedelta(days=2)).isoformat()
        with closing(sqlite3.connect(self.db)) as db,db:
            db.execute('INSERT INTO control_handoffs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       ('H:/private/handoff','a','b','H:/private/project','takeover','[]',None,None,at,'expired',None,'[]',at,at))
        for op in ('handoff.list','attention.list'):
            result=self.query(op,'progress')
            self.assertNotIn('H:/private',json.dumps(result))
        self.assertTrue(any(i.get('rule')=='A6@1' for i in self.query('attention.list')['items']))
        handoff=self.query('handoff.list','technical')['items'][0]
        self.assertNotIn('project_id',handoff);self.assertNotIn('scope',handoff)

    def test_trace_wrong_fixed_reference_not_ignored(self):
        result=self.query('trace.get',task_id=self.task.task_id,ref='incorrect-fixed-ref')
        self.assertEqual(result['items'],[])
        self.assertEqual(result['sources'][0]['unknown'],['source_reference_mismatch'])

    def test_linux_host_mount_refused_before_sqlite_open(self):
        fake=Path('/fixture-host-mount/synthetic.sqlite3')
        mounts='1 0 8:1 / / rw - ext4 /dev/root rw\n2 1 0:9 / /fixture-host-mount rw - 9p C: rw\n'
        with patch('tools.m0_manager.readonly.sys.platform','linux'), \
             patch('tools.m0_manager.readonly.local_file',return_value=fake), \
             patch.object(Path,'read_text',return_value=mounts), \
             patch('sqlite3.connect',side_effect=AssertionError('host SQLite opened')):
            with self.assertRaises(SourceError) as caught:
                with readonly_db(fake):pass
            self.assertEqual(caught.exception.code,'source_filesystem_not_local_sqlite')

    def test_non_synthetic_dispatch_source_is_not_consumed(self):
        with closing(sqlite3.connect(self.db)) as db,db:
            db.execute("UPDATE tasks SET access='local_only'")
        result=self.query()
        self.assertEqual(result['sources'][0]['unknown'],['source_scope_not_synthetic'])

    def test_actual_scheduler_template_error_kind_emits_a8(self):
        with closing(sqlite3.connect(self.db)) as db,db:
            db.execute('INSERT INTO schedule_events(schedule_id,version,kind,at,detail_code) VALUES(?,?,?,?,?)',
                       ('synthetic-bad-template',1,'schedule.template_error',datetime.now(timezone.utc).isoformat(),'invalid_template'))
        result=self.query('attention.list')
        self.assertTrue(any(row.get('rule')=='A8@1' and row['state']=='template_error' for row in result['items']))


if __name__=='__main__':unittest.main()
