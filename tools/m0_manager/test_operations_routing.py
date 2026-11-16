"""Read-only protocol boundaries for installed operations and handoff queries."""
import hashlib
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'dispatch'))
from tools.m0_agent import test_common_worker
from tools.m0_manager.manager import run, validate
from tools.m0_manager.readonly import SourceError


class OperationsRoutingTests(unittest.TestCase):
    def setUp(self):
        self.fixture = test_common_worker.CommonWorkerTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.upgrade()
        info = self.fixture.ledger.stat()
        self.source = {'source_id': 'common-dispatch', 'source_instance': 'fixture-instance',
                       'kind': 'dispatch', 'locator': str(self.fixture.ledger),
                       'accepted_schema_versions': ['0013'], 'class': 'local_only',
                       'freshness_s': 3600, 'options': {'profile': 'common_metadata_0013',
                            'identity': {'device': info.st_dev, 'inode': info.st_ino}}}
        self.binding = self.fixture.root / 'manager-binding.json'
        self.save()

    def save(self):
        self.binding.write_text(json.dumps({'version': 1, 'sources': [self.source]}), encoding='utf-8')

    def query(self, op, role='technical', **args):
        return run({'op': op, 'role': role, 'args': {'source_id': 'common-dispatch', **args}}, self.binding)

    def fingerprint(self):
        return hashlib.sha256(self.fixture.ledger.read_bytes()).hexdigest(), self.fixture.ledger.stat().st_mtime_ns

    def test_new_operations_require_one_explicit_source(self):
        for op, args in [('ops.snapshot', {}), ('handoff.get', {'handoff_id': 'x'}),
                         ('handoff.get', {'source_id': 'common-dispatch'})]:
            with self.subTest(op=op, args=args), self.assertRaises(SourceError) as caught:
                validate({'op': op, 'role': 'technical', 'args': args})
            self.assertEqual(caught.exception.code, 'E_ARGUMENT_REQUIRED')

    def test_arguments_cannot_choose_command_path_or_sql(self):
        for field in ('command', 'path', 'sql', 'health_path'):
            with self.subTest(field=field), self.assertRaises(SourceError) as caught:
                self.query('ops.snapshot', **{field: 'injected'})
            self.assertEqual(caught.exception.code, 'E_FIELD_NOT_ALLOWED')
        with self.assertRaises(SourceError):
            self.query('handoff.get', handoff_id='x' * 201)

    def test_uninstalled_fixture_is_not_presented_as_host_live_and_queries_do_not_write(self):
        before = self.fingerprint()
        for role in ('technical', 'manager', 'progress'):
            snapshot = self.query('ops.snapshot', role)
            self.assertEqual(snapshot['coverage'], 'partial')
            self.assertTrue(snapshot['sources'][0]['unknown'])
            handoff = self.query('handoff.get', role, handoff_id='missing-handoff')
            self.assertEqual(handoff['items'], [])
        self.assertEqual(before, self.fingerprint())

    def test_profile_refusal_happens_before_host_observation(self):
        self.source['options'].pop('profile')
        self.save()
        for op, args in [('ops.snapshot', {}), ('handoff.get', {'handoff_id': 'missing'})]:
            result = self.query(op, **args)
            self.assertEqual(result['sources'][0]['unknown'], ['source_scope_not_synthetic'])

    def test_changed_file_identity_is_not_live_or_a_valid_handoff_source(self):
        self.source['options']['identity']['inode'] += 1
        self.save()
        result = self.query('ops.snapshot')
        self.assertEqual(result['sources'][0]['unknown'], ['source_instance_changed'])


if __name__ == '__main__':
    unittest.main()
