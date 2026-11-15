"""Exercise the Collector CLI via subprocesses with engine-built synthetic fixtures."""

from contextlib import closing
from dataclasses import asdict
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from sa_dispatch.collector import SourceBinding
from sa_dispatch.store_sqlite import SqliteStore


class CollectorProtocolTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='collector-protocol-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / 'source.db'
        self.target = self.root / 'target.db'
        self.binding = self.root / 'binding.json'
        SqliteStore(self.target, lease_ttl_seconds=10)
        with closing(sqlite3.connect(self.source)) as connection, connection:
            connection.executescript('''
                CREATE TABLE collector_fixture_meta(key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE hook_events(seq INTEGER PRIMARY KEY, body TEXT);
            ''')
            connection.executemany('INSERT INTO collector_fixture_meta VALUES (?, ?)', [
                ('schema', 'fixture-v1'), ('instance', 'instance-one'), ('synthetic_only', 'true'),
            ])
        self.add_source_event(1)
        self.add_source_event(2)
        source_binding = SourceBinding.enroll(
            'synthetic-hook', 'fixture_hook', self.source, 'instance-one', ('fixture-v1',),
        )
        self.binding.write_text(json.dumps({
            'schema': 'sa_dispatch.collector_binding.v1',
            'target': str(self.target),
            'sources': [asdict(source_binding)],
        }), encoding='utf-8')
        self.sync_request = {'source_id': 'synthetic-hook', 'stream': 'hook_events'}

    def add_source_event(self, seq):
        body = {
            'schemaVersion': 1, 'at': '2026-11-15T00:00:00+00:00',
            'event': 'PreToolUse', 'mode': 'audit', 'decision': 'observed',
        }
        with closing(sqlite3.connect(self.source)) as connection, connection:
            connection.execute('INSERT INTO hook_events VALUES (?, ?)', (seq, json.dumps(body)))

    def call(self, command, request, *, binding=None, exit_code=0, via_manager=False):
        if not isinstance(request, bytes):
            request = json.dumps(request).encode('utf-8')
        arguments = (['tools.m0_manager', command, '--binding', str(binding or self.binding)]
                     if via_manager else ['sa_dispatch.collector_cli', '--binding',
                                          str(binding or self.binding), command])
        process = subprocess.run(
            [sys.executable, '-B', '-m', *arguments],
            input=request, capture_output=True, timeout=20,
            env=dict(os.environ, PYTHONPATH=os.pathsep.join(
                str(p) for p in Path(__file__).resolve().parents[1:3])),
        )
        self.assertEqual(process.returncode, exit_code, (process.stdout, process.stderr))
        self.assertEqual(process.stderr, b'')
        response = json.loads(process.stdout)
        self.assertEqual(response['schema_version'], '0.1')
        self.assertIs(response['ok'], exit_code == 0)
        return response

    def assert_rejected(self, command, request, error, *, binding=None, cause=None):
        response = self.call(command, request, binding=binding, exit_code=1)
        expected = {'schema_version': '0.1', 'ok': False, 'error': error}
        if cause is not None:
            expected['cause'] = cause
        self.assertEqual(response, expected)

    def cursor_and_event_count(self):
        with closing(sqlite3.connect(self.target.as_uri() + '?mode=ro', uri=True)) as connection:
            cursor = connection.execute('SELECT epoch, last_seq FROM collector_cursors').fetchone()
            event_count, = connection.execute('SELECT COUNT(*) FROM unified_events').fetchone()
            return cursor, event_count

    def test_legacy_and_versioned_sync_replay_same_events(self):
        legacy = self.call('sync', self.sync_request)
        self.assertTrue({'schema_version', 'ok', 'binding_sha256', 'result'} <= legacy.keys())
        self.assertRegex(legacy['binding_sha256'], r'^[0-9a-f]{64}$')
        self.assertEqual(legacy['result'], {
            'state': 'synced', 'added': 2, 'last_seq': 2, 'projection_lag': 0,
            'epoch': 1, 'redacted': 2,
        })
        replay = self.call('sync', self.sync_request | {'schema_version': '0.1'})
        self.assertEqual(replay, legacy | {'result': legacy['result'] | {'added': 0, 'redacted': 0}})
        self.assertEqual(self.cursor_and_event_count(), ((1, 2), 2))

    def test_invalid_sync_requests_leave_ledger_unchanged(self):
        self.call('sync', self.sync_request)
        self.add_source_event(3)
        before = self.target.read_bytes()
        for version in ('0.2', None, 0.1, True, ['0.1'], {'version': '0.1'}):
            with self.subTest(version=version):
                request = self.sync_request | {'schema_version': version}
                self.assert_rejected('sync', request, 'E_PROTOCOL_UNSUPPORTED')
                self.assert_rejected('sync', request, 'E_PROTOCOL_UNSUPPORTED',
                                     binding=self.root / 'missing-binding.json')

        for request, error, cause in (
            ([], 'E_FIELD_NOT_ALLOWED', None),
            ({'source_id': 'synthetic-hook'}, 'E_FIELD_NOT_ALLOWED', None),
            (self.sync_request | {'locator': str(self.root / 'injected.db')},
             'E_FIELD_NOT_ALLOWED', None),
            (self.sync_request | {'schema_version': '0.1', 'target': str(self.root / 'injected.db')},
             'E_FIELD_NOT_ALLOWED', None),
            (self.sync_request | {'source_id': []}, 'E_COLLECTOR_REFUSED', 'ValueError'),
            (self.sync_request | {'stream': True}, 'E_COLLECTOR_REFUSED', 'ValueError'),
            (b'{', 'E_COLLECTOR_REFUSED', 'JSONDecodeError'),
            (b'{"schema_version":"0.2","schema_version":"0.1"}',
             'E_COLLECTOR_REFUSED', 'ValueError'),
            (b' ' * 16385, 'E_COLLECTOR_REFUSED', 'ValueError'),
        ):
            with self.subTest(request=repr(request)[:180]):
                self.assert_rejected('sync', request, error, cause=cause)
        self.assertEqual(self.target.read_bytes(), before)
        self.assertEqual(self.cursor_and_event_count(), ((1, 2), 2))
        self.assertFalse((self.root / 'injected.db').exists())

    def test_manager_explicit_writer_route_preserves_protocol_and_exit_code(self):
        result = self.call('sync', self.sync_request | {'schema_version': '0.1'}, via_manager=True)
        self.assertEqual(result['result']['added'], 2)
        before = self.target.read_bytes()
        rejected = self.call('sync', self.sync_request | {'schema_version': '9.0'},
                             via_manager=True, exit_code=1)
        self.assertEqual(rejected['error'], 'E_PROTOCOL_UNSUPPORTED')
        self.assertEqual(self.target.read_bytes(), before)
        self.assertEqual(self.cursor_and_event_count(), ((1, 2), 2))

    def test_versioned_adoption_requires_explicit_valid_arguments(self):
        self.call('sync', self.sync_request)
        self.add_source_event(4)
        frozen = self.call('sync', self.sync_request | {'schema_version': '0.1'})
        self.assertEqual(frozen['result']['state'], 'frozen')
        request = self.sync_request | {
            'schema_version': '0.1', 'reason': 'accept_gap', 'actor': 'operator', 'resume_after': 4,
        }
        before = self.target.read_bytes()
        self.assert_rejected('adopt', self.sync_request, 'E_FIELD_NOT_ALLOWED')
        self.assert_rejected('adopt', request | {'schema_version': None}, 'E_PROTOCOL_UNSUPPORTED')
        for value in (True, -1, 4.0, '4', None):
            with self.subTest(resume_after=value):
                invalid = request | {'resume_after': value}
                self.assert_rejected('adopt', invalid, 'E_COLLECTOR_REFUSED', cause='ValueError')
                self.assert_rejected('adopt', invalid, 'E_COLLECTOR_REFUSED', cause='ValueError',
                                     binding=self.root / 'missing-binding.json')
        for invalid in ({'reason': 'free text'}, {'actor': []}):
            with self.subTest(invalid=invalid):
                self.assert_rejected('adopt', request | invalid, 'E_COLLECTOR_REFUSED', cause='ValueError')
        self.assertEqual(self.target.read_bytes(), before)
        self.assertEqual(self.cursor_and_event_count(), ((1, 2), 2))

        adoption = self.call('adopt', request)
        self.assertEqual(adoption['result'], {'state': 'adopted', 'epoch': 2, 'last_seq': 4})
        self.assertEqual(self.cursor_and_event_count(), ((2, 4), 2))


if __name__ == '__main__':
    unittest.main()
