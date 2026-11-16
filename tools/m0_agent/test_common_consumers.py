"""Shared 0013 consumers must preserve old isolation and metadata cursors."""
from contextlib import closing
from dataclasses import asdict, replace
import hashlib
import json
import sqlite3
import unittest

from sa_dispatch.collector import Collector, SourceBinding, canonical
from sa_dispatch.collector_cli import load_binding
from sa_dispatch.models import TaskSpec
from sa_dispatch.transport import CoordinatorAPI, TransportError
from .test_common_worker import CommonWorkerTests


class CommonConsumersTests(unittest.TestCase):
    def setUp(self):
        self.f = CommonWorkerTests()
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)

    def test_http_remains_scoped_with_real_external_result(self):
        f = self.f
        f.upgrade(); f.register(); f.tick()
        http = f.store('http_synthetic')
        http.register_device('http-worker', ['worker.compute'])
        token = 'fixture-' + 'a' * 40
        api = CoordinatorAPI(http, {'http-worker': hashlib.sha256(token.encode()).hexdigest()},
            synthetic_handoff=True, roles={'http-worker': ['worker']}, artifact_store=f.cas)
        self.assertEqual(api.invoke('/claim', {}, 'Bearer ' + token), {'lease': None})
        with self.assertRaises(TransportError):
            api.invoke('/status', {'task_id': f.status()['task_id']}, 'Bearer ' + token)
        task, _ = http.submit(TaskSpec('http-task', 'fixture.local', 1, {}, (),
                                      ('worker.compute',), 'synthetic', 'none', 1))
        # The preexisting handoff guard still refuses ungranted HTTP submissions.
        with self.assertRaises(TransportError) as error:
            api.invoke('/claim', {}, 'Bearer ' + token)
        self.assertEqual(error.exception.code, 'E_HANDOFF_NOT_READY')
        self.assertEqual(http.get_task(task.task_id)['status'], 'queued')

    def test_three_metadata_streams_require_explicit_schema_adoption(self):
        f = self.f
        old = SourceBinding.enroll('common-dispatch', 'dispatch', f.ledger,
                                  'common-instance', ('0012',))
        collector = Collector(f.ledger)
        streams = ('events', 'control_events', 'schedule_events')
        for stream in streams:
            collector.sync(old, stream)
        f.upgrade(); f.register(); f.tick()
        new = replace(old, accepted_schema_versions=('0013',), synthetic_only=False,
                      data_class='local_only', profile='common_metadata_0013')
        for stream in streams:
            self.assertEqual(collector.sync(new, stream)['state'], 'frozen')
            with closing(sqlite3.connect(f.ledger)) as db:
                last = db.execute('SELECT last_seq FROM collector_cursors WHERE source_id=? AND stream=?',
                                  (old.source_id, stream)).fetchone()[0]
            self.assertEqual(collector.adopt(new, stream, reason='accept_schema', actor='operator',
                                            resume_after=last)['epoch'], 2)
            collector.sync(new, stream)
            self.assertEqual(collector.sync(new, stream)['added'], 0)
        with closing(sqlite3.connect(f.ledger)) as db:
            rows = db.execute('SELECT * FROM unified_events').fetchall()
            self.assertNotIn(f.prompt, canonical(rows))
            self.assertNotIn('provider result', canonical(rows))
            self.assertEqual(db.execute('SELECT count(*) FROM collector_cursors WHERE epoch=2').fetchone()[0], 3)

    def test_v1_binding_cannot_enable_real_metadata_profile(self):
        f = self.f
        source = replace(SourceBinding.enroll('common', 'dispatch', f.ledger, 'instance', ('0013',)),
                         synthetic_only=False, data_class='local_only', profile='common_metadata_0013')
        path = f.root / 'binding.json'
        value = {'schema': 'sa_dispatch.collector_binding.v1', 'target': str(f.ledger),
                 'sources': [asdict(source)]}
        path.write_text(json.dumps(value), encoding='utf-8')
        with self.assertRaises(ValueError): load_binding(path)
        value['schema'] = 'sa_dispatch.collector_binding.v2'
        path.write_text(json.dumps(value), encoding='utf-8')
        _, sources, _ = load_binding(path)
        sources['common'].validate('events')

    def test_manager_explicit_metadata_profile_hides_prompt_and_output(self):
        from tools.m0_manager.dispatch_source import dispatch_query
        from tools.m0_manager.readonly import SourceError
        f = self.f
        f.upgrade(); f.register(); f.tick()
        info = f.ledger.stat()
        source = {'source_id': 'common-dispatch', 'source_instance': 'common-instance',
                  'kind': 'dispatch', 'locator': str(f.ledger), 'accepted_schema_versions': ['0013'],
                  'class': 'local_only', 'freshness_s': 3600,
                  'options': {'identity': {'device': info.st_dev, 'inode': info.st_ino}}}
        with self.assertRaises(SourceError): dispatch_query(source, 'status', {}, 'technical')
        source['options']['profile'] = 'common_metadata_0013'
        result = dispatch_query(source, 'trace.get', {'task_id': f.status()['task_id']}, 'technical')
        self.assertEqual(result['items'][0]['status'], 'succeeded')
        self.assertEqual(result['reservations'][0]['actual_units'], 1)
        self.assertNotIn(f.prompt, canonical(result))
        self.assertNotIn('provider result', canonical(result))


if __name__ == '__main__': unittest.main()
