from contextlib import closing
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.models import Evidence, Outcome, Ref, TaskSpec
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.unified_events import UnifiedEventStore, initialize_schema


class UnifiedEventsTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / 'ledger.sqlite3'
        self.clock = FakeClock()
        self.ledger = SqliteStore(self.path, clock=self.clock, lease_ttl_seconds=10)
        self.events = UnifiedEventStore(self.path, clock=self.clock)

    def test_three_sources_trace_order_idempotency_and_privacy(self):
        before = self.ledger.snapshot()
        at = timestamp(self.clock.now())
        row = {'task_id':'t_synthetic','from_status':None,'to_status':'queued',
               'fence':0,'actor':'program','at':at,'reason':'SECRET raw prompt'}
        first = self.events.ingest_ledger(row,source_id='ledger-1',trace_id='trace-1')
        self.assertEqual(first,self.events.ingest_ledger(row,source_id='ledger-1',trace_id='trace-1'))
        self.assertRegex(first,r'^[0-7][0-9A-HJKMNP-TV-Z]{25}$')
        self.clock.advance(1)
        body = {'schemaVersion':1,'at':timestamp(self.clock.now()),'event':'PreToolUse',
                'mode':'audit','decision':'allow','tool_input':'SECRET','restricted':'SECRET'}
        self.events.ingest_hook({'id':1,'body':json.dumps(body)},source_id='hook-1',trace_id='trace-1')
        self.clock.advance(1)
        self.events.ingest_handoff({'id':'handoff:one','status':'available_receipt_unconfirmed',
            'producerStatus':'completed','subject':'SECRET','updatedAt':timestamp(self.clock.now())},
            source_id='handoff-one-v1',trace_id='trace-1')
        rows = self.events.query('trace-1')
        self.assertEqual(['task.submitted','hook.observed','handoff.observed'],[r['kind'] for r in rows])
        self.assertNotIn('SECRET',json.dumps(rows))
        after = self.ledger.snapshot()
        for table in ('tasks', 'leases', 'events', 'evidence', 'late_results', 'devices'):
            self.assertEqual(before[table], after[table])

    def test_parse_error_safe_and_idempotent(self):
        first = self.events.ingest_hook({'body':'SECRET invalid json'},source_id='bad-1',trace_id='trace-1')
        self.clock.advance(60)
        self.assertEqual(first,self.events.ingest_hook({'body':'SECRET invalid json'},source_id='bad-1',trace_id='trace-1'))
        rows = self.events.query('trace-1')
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['kind'],'adapter.parse_error')
        self.assertNotIn('SECRET',json.dumps(rows))

    def test_raw_fields_rejected_and_conflicts_not_silenced(self):
        args = dict(source='test',source_id='1',trace_id='trace-1',actor='test',component='test',
                    kind='task.submitted',at=timestamp(self.clock.now()))
        with self.assertRaises(ValueError): self.events.append(**args,data_class='restricted')
        with self.assertRaises(ValueError): self.events.append(**args,metadata={'prompt':'SECRET'})
        self.events.append(**args)
        with self.assertRaises(ValueError): self.events.append(**{**args,'kind':'task.succeeded'})

    def test_completion_requires_handoff_evidence(self):
        self.events.ingest_handoff({'handoff_id':'h1','state':'completed','at':timestamp(self.clock.now())},
                                  source_id='h1-v1',trace_id='trace-1')
        self.assertEqual(self.events.query('trace-1')[0]['kind'],'adapter.parse_error')

    def test_actual_diagnostics_expose_missing_output(self):
        self.ledger.register_device('d',('cpu',))
        task,_ = self.ledger.submit(TaskSpec('key','test.read',1,{},(),('cpu',),'synthetic','none',2))
        lease = self.ledger.claim('d')
        self.ledger.complete(lease.lease_id,lease.fence,Outcome(),'d')
        self.assertEqual([task.task_id],self.events.diagnostics()['succeeded_without_output_refs'])
        task2,_ = self.ledger.submit(TaskSpec('key2','test.read',1,{},(),('cpu',),'synthetic','none',2))
        lease2 = self.ledger.claim('d')
        self.ledger.complete(lease2.lease_id,lease2.fence,Outcome(evidence=(Evidence('output','sha256:'+'a'*64,
            'synthetic',ref=Ref('dispatch','output','one','1','sha256:'+'a'*64)),)),'d')
        self.assertNotIn(task2.task_id,self.events.diagnostics()['succeeded_without_output_refs'])

    def test_schema_initialization_does_not_commit_caller_transaction(self):
        with closing(sqlite3.connect(':memory:')) as conn, conn:
            conn.execute('BEGIN')
            initialize_schema(conn)
            conn.rollback()
            self.assertIsNone(conn.execute("SELECT name FROM sqlite_master WHERE name='unified_events'").fetchone())


if __name__ == '__main__': unittest.main()
