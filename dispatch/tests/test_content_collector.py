"""Content collector integration with real synthetic producer tables."""
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

root=Path(__file__).resolve().parents[2]
if str(root) not in sys.path:sys.path.insert(0,str(root))
from knowledge.m0_content.contract import Contract
from sa_dispatch.artifacts import ArtifactError
from sa_dispatch.bundle import export_bundle
from sa_dispatch.collector import Collector, SourceBinding
from sa_dispatch.store_sqlite import SqliteStore


class ContentCollectorTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory(prefix='content-collector-')
        self.addCleanup(tmp.cleanup)
        self.source=Path(tmp.name)/'content.sqlite3'
        self.target=Path(tmp.name)/'ledger.sqlite3'
        SqliteStore(self.target,lease_ttl_seconds=10)
        with closing(Contract(self.source,create=True)) as content:
            instance=content.db.execute("SELECT value FROM meta WHERE key='instance'").fetchone()[0]
        self.binding=SourceBinding.enroll('content-fixture','content',self.source,instance,('m0-content-synthetic-v1',))
        self.collector=Collector(self.target)

    def put(self,ident,trace='synthetic-content-game'):
        with closing(Contract(self.source)) as content:
            return content.invoke('artifact.put',{'owner':'fixture','kind':'evidence','id':ident,'trace_id':trace,
                'body':{'text':'PRIVATE_SENTINEL 正文','path':'C:/private/argv.txt','argv':['private']}})['ref']

    def mutate(self,sql,args=()):
        with closing(sqlite3.connect(self.source)) as db, db:
            db.execute(sql,args)

    def rows(self,table):
        with closing(sqlite3.connect(self.target)) as db:
            db.row_factory=sqlite3.Row
            return [dict(row) for row in db.execute(f'SELECT * FROM {table}')]

    def sync(self,binding=None,**opts):
        return self.collector.sync(binding or self.binding,'content_events',**opts)

    def test_batches_source_trace_fixed_refs_and_readonly(self):
        refs=[self.put('first'),self.put('second','synthetic-content-ai-editing')]
        before=(self.source.read_bytes(),self.source.stat().st_mtime_ns)
        self.assertEqual(self.sync(limit=1)['projection_lag'],1)
        self.assertEqual(self.sync()['added'],1)
        self.assertEqual(self.sync()['added'],0)
        self.assertEqual(before,(self.source.read_bytes(),self.source.stat().st_mtime_ns))
        events=self.rows('unified_events')
        self.assertEqual([row['trace_id'] for row in events],['synthetic-content-game','synthetic-content-ai-editing'])
        self.assertEqual([row['source_id'] for row in events],['content_events:1','content_events:2'])
        self.assertEqual([json.loads(row['refs_json']) for row in events],[[ref] for ref in refs])
        self.assertTrue(all(row['task_id'] is None and row['attempt_epoch'] is None for row in events))
        self.assertNotIn('PRIVATE_SENTINEL',json.dumps(events))
        self.assertNotIn('argv',json.dumps(events))
        self.assertEqual(self.rows('collector_cursors')[0]['last_seq'],2)

    def test_fault_after_events_or_cursor_is_atomic(self):
        self.put('first')
        for checkpoint in ('events_inserted','cursor_updated'):
            def fail(stage):
                if stage==checkpoint:raise RuntimeError(stage)
            with self.subTest(checkpoint=checkpoint), self.assertRaisesRegex(RuntimeError,checkpoint):
                self.sync(fault=fail)
            self.assertEqual(self.rows('unified_events'),[])
            self.assertEqual(self.rows('collector_cursors'),[])
        self.assertEqual(self.sync()['added'],1)

    def test_anchor_rewrite_freezes(self):
        self.put('anchor');self.sync();self.put('second')
        self.mutate("UPDATE content_events SET at='2026-01-01T00:00:00+00:00' WHERE seq=1")
        self.assertEqual(self.sync()['kind'],'source_rewritten')
        self.assertEqual(self.sync()['state'],'frozen')
        self.assertEqual(len(self.rows('unified_events')),1)

    def test_anchor_deletion_freezes(self):
        self.put('anchor');self.sync();self.put('second')
        self.mutate('DELETE FROM content_events WHERE seq=1')
        self.assertEqual(self.sync()['kind'],'source_truncated')
        self.assertEqual(self.rows('collector_cursors')[0]['last_seq'],1)

    def test_instance_drift_freezes(self):
        self.put('anchor');self.sync()
        self.mutate("UPDATE meta SET value='replacement' WHERE key='instance'")
        self.assertEqual(self.sync()['kind'],'instance_changed')

    def test_source_identity_required(self):
        self.put('first')
        self.assertEqual(self.sync(replace(self.binding,file_identity='different'))['kind'],'instance_changed')
        self.assertEqual(self.rows('unified_events'),[])

    def test_exact_schema_and_synthetic_platform_gate(self):
        self.put('first')
        self.mutate("UPDATE meta SET value='1' WHERE key='platform_calls'")
        self.assertEqual(self.sync()['kind'],'schema_changed')
        self.mutate("UPDATE meta SET value='0' WHERE key='platform_calls'")
        self.mutate('ALTER TABLE content_events ADD COLUMN body TEXT')
        self.assertEqual(self.sync(replace(self.binding,source_id='content-second'))['kind'],'schema_changed')
        self.assertEqual(self.rows('unified_events'),[])

    def test_workflow_marker_is_explicitly_allowlisted(self):
        from knowledge.m0_content.workflow_content import initialize_workflow
        initialize_workflow(self.source)
        self.put('first')
        self.assertEqual(self.sync()['added'],1)
        self.mutate("UPDATE meta SET value='unknown-workflow' WHERE key='workflow_schema'")
        self.assertEqual(self.sync()['kind'],'schema_changed')

    def test_content_cannot_open_its_source_as_a_writable_target(self):
        collector=Collector(self.source)
        with patch.object(collector,'_connect',side_effect=AssertionError('source opened as target')):
            with self.assertRaisesRegex(ValueError,'separate'):
                collector.sync(self.binding,'content_events')

    def test_wal_source_refused_without_opening_sqlite(self):
        self.put('first')
        with closing(sqlite3.connect(self.source)) as db:
            self.assertEqual(db.execute('PRAGMA journal_mode=WAL').fetchone()[0],'wal')
        with patch('sa_dispatch.collector.readonly',side_effect=AssertionError('WAL SQLite opened')):
            self.assertEqual(self.sync()['kind'],'schema_changed')
        self.assertFalse(Path(str(self.source)+'-shm').exists())
        self.assertEqual(self.rows('unified_events'),[])

    def test_rejected_record_cannot_partially_commit_batch(self):
        first=self.put('first')
        second=self.put('second','synthetic-content-ai-editing')
        cases=[('kind','task.leased'),('operation','platform.submit'),('operation','approval.record'),
               ('metadata_json','{"body":"PRIVATE_SENTINEL"}'),('metadata_json','{"to_status":"succeeded"}'),
               ('refs_json',json.dumps([first])),('refs_json',json.dumps([second[:-64]+'0'*64])),
               ('refs_json',json.dumps(['C:/PRIVATE_SENTINEL/path'])),('trace_id','C:/PRIVATE_SENTINEL/path'),
               ('metadata_json',b'{}'),('metadata_json','{"decision":"hidden","decision":"observed"}'),
               ('at','1900-01-01T00:00:00+00:00')]
        with closing(sqlite3.connect(self.source)) as db:
            db.row_factory=sqlite3.Row
            original=dict(db.execute('SELECT * FROM content_events WHERE seq=2').fetchone())
        for index,(key,value) in enumerate(cases):
            with self.subTest(key=key,value=value):
                self.mutate(f'UPDATE content_events SET {key}=? WHERE seq=2',(value,))
                result=self.sync(replace(self.binding,source_id=f'case-{index}'))
                self.assertEqual(result['kind'],'redaction_rejected')
                self.assertEqual(result['last_seq'],0)
                self.mutate(f'UPDATE content_events SET {key}=? WHERE seq=2',(original[key],))
        self.assertEqual(self.rows('unified_events'),[])
        self.assertEqual(self.rows('collector_cursors'),[])
        self.assertNotIn('PRIVATE_SENTINEL',json.dumps(self.rows('collector_gaps')))

    def test_anchor_cannot_reference_rewritten_artifact(self):
        self.put('anchor');self.sync()
        self.mutate("UPDATE artifacts SET payload='{}'")
        self.assertEqual(self.sync()['kind'],'source_rewritten')

    def test_external_content_ref_does_not_become_dispatch_cas(self):
        self.put('first');self.sync()
        cfg=SimpleNamespace(db_path=self.target,artifact_root=self.target.parent/'cas')
        with self.assertRaises(ArtifactError):
            export_bundle(cfg,self.target.parent/'bundle')

    def test_publish_import_remains_a_fixture_observation(self):
        with closing(Contract(self.source)) as content:
            trace='synthetic-content-game'
            ref=content._put('webee','receipt','synthetic-receipt',
                {'verification_status':'unverified_import','observed_status':'published'},[],trace)
            content._event(trace,'publish.observe',[ref],'task.reconciled',{'to_status':'succeeded'})
        self.assertEqual(self.sync()['added'],1)
        row=self.rows('unified_events')[0]
        self.assertEqual((row['component'],row['kind'],row['task_id']),('content-fixture-publish','task.reconciled',None))
