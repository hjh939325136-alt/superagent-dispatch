"""Synthetic-only transactional receipts and real SQLite held-read races."""
from contextlib import closing
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from catalog import Catalog, CatalogError, OPERATOR, SOURCES, initialize
from dispatch_binding import initialize_binding, load_binding
from fixture_store import ROOT, fixture_id, read_json
from search import SearchError, record_ref, search
import workflow_catalog as workflow

NOW = '2026-11-15T08:00:00+00:00'
HASH = 'sha256:' + '1' * 64


class WorkflowCatalogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='m0-workflow-catalog-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).absolute()
        self.path = self.root / 'catalog'
        initialize(self.path, operator_grant=OPERATOR)
        initialize_binding(self.path, self.root / 'binding.json')
        self.binding = load_binding(self.root / 'binding.json')
        self.catalog = Catalog(self.path)
        self.request = read_json(ROOT / 'fixtures/three-source-request.json')
        self.base = {k: v for k, v in self.request.items() if k not in ('q', 'limit')}
        self.refs = [item['ref'] for item in search(self.request, self.binding.grant, self.catalog)['results'] if item['status'] == 'current']
        clock = patch.object(workflow, '_now', return_value=NOW)
        clock.start()
        self.addCleanup(clock.stop)

    def init(self):
        return workflow.initialize_receipts(self.binding, operator_grant=OPERATOR)

    def write(self, refs=None, key='write-1', request_hash=HASH, **kwargs):
        return workflow.assemble(self.binding, self.base | {'refs': self.refs if refs is None else refs}, key, request_hash, committed_fence=1, **kwargs)

    def ref(self, slug='control-07', domain='research-scatter'):
        ident = fixture_id(domain, slug)
        records, _ = self.catalog.load([domain])[domain]
        return record_ref(domain, next(row for row in records if row['id'] == ident))

    def files(self):
        return {p.name: (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns) for p in self.path.iterdir() if p.is_file()}

    def counts(self):
        with closing(sqlite3.connect(self.path / 'catalog.sqlite3')) as db:
            return {name: db.execute('SELECT count(*) FROM ' + name).fetchone()[0] for name in ('contexts', 'context_members', 'context_events', workflow.TABLE)}

    def assert_code(self, code, fn):
        with self.assertRaises((CatalogError, SearchError)) as caught:
            fn()
        self.assertEqual(caught.exception.code, code)

    def test_explicit_upgrade_only_reads_never_initialize(self):
        before = self.files()
        self.assert_code('WORKFLOW_SCHEMA_REQUIRED', lambda: workflow.lookup(self.binding, workflow.OPERATION, 'missing', HASH))
        self.assertEqual(before, self.files())
        self.assert_code('WORKFLOW_SCHEMA_REQUIRED', self.write)
        self.assertEqual(before, self.files())
        self.assert_code('FIXTURE_OPERATOR_REQUIRED', lambda: workflow.initialize_receipts(self.binding, operator_grant='wrong'))
        self.assertTrue(self.init()['created'])
        upgraded = self.files()
        self.assertFalse(self.init()['created'])
        self.assertEqual(upgraded, self.files())

    def test_exact_replay_conflict_and_new_key_alias_receipt(self):
        self.init()
        first = self.write()
        before = self.counts()
        replay = workflow.assemble(self.binding, self.base | {'refs': list(reversed(self.refs))}, 'write-1', HASH, committed_fence=2)
        self.assertEqual(first, replay)
        self.assertEqual(before, self.counts())
        self.assert_code('IDEMPOTENCY_CONFLICT', lambda: self.write(request_hash='sha256:' + '2' * 64))
        self.assert_code('IDEMPOTENCY_CONFLICT', lambda: self.write(self.refs[:1]))
        second = self.write(key='write-2', request_hash='sha256:' + '2' * 64)
        self.assertEqual(first['context_id'], second['context_id'])
        self.assertEqual(second['reused_from'], 'write-1')
        self.assertEqual(self.counts(), before | {workflow.TABLE: 2})
        self.assertEqual(workflow.lookup(self.binding, workflow.OPERATION, 'write-2', second['request_hash'])['receipt'], second)
        self.assertEqual(workflow.lookup(self.binding, workflow.OPERATION, 'missing', HASH)['status'], 'absent')
        self.assertEqual(workflow.lookup(self.binding, workflow.OPERATION, 'write-1', second['request_hash'])['status'], 'conflict')
        self.assertEqual(first['committed_fence'], 1)
        self.assertNotEqual(first['semantic_hash'], HASH)
        for field in ('body', 'snippet', 'source_record', 'title'):
            self.assertNotIn('"' + field + '"', json.dumps(first))

    def test_receipt_insert_failure_rolls_back_context_members_and_event(self):
        self.init()
        before = self.counts()
        with patch.object(workflow, '_insert_receipt', side_effect=RuntimeError('synthetic injected failure')):
            with self.assertRaises(RuntimeError):
                self.write()
        self.assertEqual(before, self.counts())
        self.assertEqual(workflow.lookup(self.binding, workflow.OPERATION, 'write-1', HASH)['status'], 'absent')

    def test_dispatch_task_operation_version_write_key(self):
        self.init()
        key = 't_' + '1' * 32 + ':context.assemble@1'
        receipt = self.write(key=key)
        self.assertEqual(receipt['write_key'], key)
        self.assertEqual(workflow.lookup(self.binding, workflow.OPERATION, key, HASH)['receipt'], receipt)
        for invalid in (key + ' ', key + '\n', '../' + key, 'x' * 202):
            self.assert_code('INVALID_WRITE_KEY', lambda: self.write(key=invalid))

    def test_committed_child_exit_before_ack_lookup_recovers_once(self):
        self.init()
        request = self.root / 'request.json'
        request.write_text(json.dumps(self.base | {'refs': self.refs}), encoding='utf-8')
        code = '''import json,os,sys
from dispatch_binding import load_binding
import workflow_catalog as w
w._now=lambda: sys.argv[3]
w.assemble(load_binding(sys.argv[1]),json.load(open(sys.argv[2])),'killed-after-commit',sys.argv[4],committed_fence=1)
os._exit(72)
'''
        result = subprocess.run([sys.executable, '-B', '-c', code, str(self.binding.path), str(request), NOW, HASH], cwd=ROOT, capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 72, result.stderr.decode(errors='replace'))
        lookup = workflow.lookup(self.binding, workflow.OPERATION, 'killed-after-commit', HASH)
        self.assertEqual(lookup['status'], 'committed')
        self.assertTrue(lookup['current_usable'])
        self.assertEqual(self.counts(), {'contexts': 1, 'context_members': len(self.refs), 'context_events': 1, workflow.TABLE: 1})

    def test_readonly_snapshot_reuses_transaction_and_closes(self):
        self.init()
        first = self.write()
        second = self.write(self.refs[:1], key='second')
        before = self.files()
        with workflow.catalog_snapshot(self.binding, self.base) as guard:
            self.assertTrue(guard.active)
            for receipt in (first, second):
                item = guard.inspect(receipt['context_id'], 'sha256:' + receipt['manifest_sha256'])
                self.assertTrue(item['usable'])
                self.assertEqual(item['refs'], receipt['refs'])
                self.assertEqual(item['decision_at'], NOW)
            with self.assertRaises(sqlite3.DatabaseError):
                guard._db.execute('DELETE FROM contexts')
            self.assert_code('CONTEXT_MANIFEST_MISMATCH', lambda: guard.inspect(first['context_id'], '0' * 64))
        self.assertFalse(guard.active)
        self.assert_code('SNAPSHOT_CLOSED', lambda: guard.inspect(first['context_id']))
        with workflow.context_snapshot(self.binding, self.base | {'context_id': first['context_id']}) as item:
            self.assertTrue(item['usable'])
        workflow.lookup(self.binding, workflow.OPERATION, 'write-1', HASH)
        self.assertEqual(before, self.files())

    def test_invalid_authority_request_fails_before_any_sql_open(self):
        attacks = [self.base | {'caller': self.base['caller'] | {'task_id': 'foreign'}},
                   self.base | {'domains': ['business']}, self.base | {'scopes': ['research:write']},
                   self.base | {'path': 'untrusted'}]
        with patch.object(workflow.sqlite3, 'connect', side_effect=AssertionError('SQL access before authorization')):
            for base in attacks:
                with self.subTest(base=base), self.assertRaises((CatalogError, SearchError)):
                    with workflow.catalog_snapshot(self.binding, base):
                        pass
            self.assert_code('INVALID_BINDING', lambda: workflow.lookup(replace(self.binding, grant='fixture-both'), workflow.OPERATION, 'write-1', HASH))
            with patch('search.read_json', return_value={}):
                self.assert_code('ACCESS_DENIED', lambda: workflow.lookup(self.binding, workflow.OPERATION, 'write-1', HASH))

    def test_revoked_context_is_metadata_history_not_current_or_reusable(self):
        self.init()
        first = self.write([self.ref()])
        self.catalog.apply_batch('research-scatter', 'revoke', 'revoke_open', operator_grant=OPERATOR)
        with workflow.context_snapshot(self.binding, self.base | {'context_id': first['context_id']}) as current:
            self.assertEqual(current['status'], 'stale_by_revocation')
            self.assertEqual(current['refs'], first['refs'])
            self.assertFalse(current['usable'])
        historical = workflow.lookup(self.binding, workflow.OPERATION, 'write-1', HASH)
        self.assertEqual(historical['receipt'], first)
        self.assertFalse(historical['current_usable'])
        self.assertEqual(historical['usable_reason'], 'stale_by_revocation')
        before = self.counts()
        with self.assertRaises((CatalogError, SearchError)):
            self.write(first['refs'], key='new-key')
        self.assertEqual(before, self.counts())
        with self.assertRaises((CatalogError, SearchError)):
            with workflow.context_snapshot(self.binding, self.base | {'max_class': 'public', 'context_id': first['context_id']}):
                pass
        self.catalog.rollback('research-scatter', 'revoke', operator_grant=OPERATOR)
        self.assertFalse(workflow.lookup(self.binding, workflow.OPERATION, 'write-1', HASH)['current_usable'])

    def test_current_scope_and_policy_are_rechecked_without_old_receipt_body(self):
        self.init()
        first = self.write([self.ref()])
        self.catalog.apply_batch('research-scatter', 'restrict', 'restrict_open', operator_grant=OPERATOR)
        with workflow.context_snapshot(self.binding, self.base | {'context_id': first['context_id']}) as current:
            self.assertEqual(current['status'], 'withheld')
            self.assertEqual(current['refs'], [])
        with workflow.catalog_snapshot(self.binding, self.base) as snapshot:
            with patch('search.read_json', return_value={}):
                self.assert_code('ACCESS_DENIED', lambda: snapshot.inspect(first['context_id']))

    def test_actual_now_freshness_and_expiry_cannot_use_old_as_of(self):
        self.init()
        for at in ('2026-11-16T00:00:00+00:00', '2026-11-10T00:00:00+00:00'):
            with self.subTest(at=at):
                self.assert_code('AS_OF_OUTSIDE_FRESHNESS_WINDOW', lambda: workflow.assemble(self.binding, self.base | {'as_of': at, 'refs': self.refs}, 'new-key', HASH))
        first = self.write([self.ref()])
        self.catalog.apply_batch('research-scatter', 'expire', 'expire_open', operator_grant=OPERATOR)
        with workflow.context_snapshot(self.binding, self.base | {'context_id': first['context_id']}) as current:
            self.assertEqual(current['status'], 'expired')
            self.assertFalse(current['usable'])
        with patch.object(workflow, '_now', return_value='2027-01-01T00:00:00+00:00'):
            history = workflow.lookup(self.binding, workflow.OPERATION, 'write-1', HASH)
            self.assertEqual(history['receipt'], first)
            self.assertFalse(history['current_usable'])
        with self.assertRaises((CatalogError, SearchError)):
            self.write(first['refs'], key='fresh-key')

    def test_wal_any_participating_database_refused_before_sqlite_open(self):
        self.init()
        for name in ('catalog', *SOURCES):
            path = self.path / (name + '.sqlite3')
            with closing(sqlite3.connect(path)) as db:
                self.assertEqual(db.execute('PRAGMA journal_mode=WAL').fetchone()[0], 'wal')
            try:
                before = self.files()
                with patch.object(workflow.sqlite3, 'connect', side_effect=AssertionError('WAL must be rejected before SQL')):
                    with self.assertRaises(CatalogError) as caught:
                        with workflow.catalog_snapshot(self.binding, self.base):
                            pass
                    self.assertEqual(caught.exception.code, 'UNSAFE_JOURNAL_MODE')
                self.assertEqual(before, self.files())
            finally:
                with closing(sqlite3.connect(path)) as db:
                    db.execute('PRAGMA journal_mode=DELETE')

    def test_foreign_mount_refused_before_sqlite_open(self):
        native = Path.read_text
        def read(path, *args, **kwargs):
            if str(path).replace('\\', '/') == '/proc/self/mountinfo':
                return f'1 0 1:0 / {self.root.anchor} rw - drvfs drvfs rw\n'
            return native(path, *args, **kwargs)
        with patch.object(workflow.sys, 'platform', 'linux'), patch.object(Path, 'read_text', read), patch.object(workflow.sqlite3, 'connect', side_effect=AssertionError('foreign SQL access')):
            self.assert_code('SOURCE_FILESYSTEM_NOT_LOCAL', lambda: workflow._sql_path(self.path / 'catalog.sqlite3'))

    def test_binding_instance_drift_fails_closed(self):
        self.init()
        data = json.loads(self.binding.path.read_text(encoding='utf-8'))
        data['catalog_instance'] = 'different-instance'
        self.binding.path.write_text(json.dumps(data), encoding='utf-8')
        with patch.object(workflow.sqlite3, 'connect', side_effect=AssertionError('SQL before binding check')):
            self.assert_code('BINDING_CHANGED', lambda: workflow.lookup(self.binding, workflow.OPERATION, 'write-1', HASH))

    def test_context_manifest_members_tampering_is_not_usable(self):
        self.init()
        first = self.write()
        with closing(sqlite3.connect(self.path / 'catalog.sqlite3')) as db, db:
            db.execute('DELETE FROM context_members WHERE ref=?', (first['refs'][0],))
        with workflow.catalog_snapshot(self.binding, self.base) as guard:
            self.assert_code('CONTEXT_MEMBERS_MISMATCH', lambda: guard.inspect(first['context_id']))

    def test_shared_guard_blocks_real_revocation_commit_then_reports_stale(self):
        self.init()
        first = self.write([self.ref()])
        prepared, finished = threading.Event(), threading.Event()
        failures = []
        def revoke():
            try:
                Catalog(self.path).apply_batch('research-scatter', 'concurrent-revoke', 'revoke_open', operator_grant=OPERATOR, _after_write=lambda _: prepared.set())
            except BaseException as error:
                failures.append(error)
            finally:
                finished.set()
        with workflow.catalog_snapshot(self.binding, self.base) as guard:
            self.assertTrue(guard.inspect(first['context_id'])['usable'])
            thread = threading.Thread(target=revoke)
            thread.start()
            self.assertTrue(prepared.wait(5))
            self.assertFalse(finished.wait(0.2), failures)
            self.assertTrue(guard.inspect(first['context_id'])['usable'])
        thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertFalse(workflow.lookup(self.binding, workflow.OPERATION, 'write-1', HASH)['current_usable'])

    def test_every_attached_source_shared_lock_blocks_direct_commit(self):
        self.init()
        for domain in SOURCES:
            prepared, finished = threading.Event(), threading.Event()
            failures = []
            def write():
                try:
                    with closing(sqlite3.connect(self.path / (domain + '.sqlite3'), timeout=5)) as db, db:
                        db.execute("UPDATE source_meta SET value=value WHERE key='instance'")
                        prepared.set()
                except BaseException as error:
                    failures.append(error)
                finally:
                    finished.set()
            with self.subTest(domain=domain), workflow.catalog_snapshot(self.binding, self.base):
                thread = threading.Thread(target=write)
                thread.start()
                self.assertTrue(prepared.wait(5))
                self.assertFalse(finished.wait(0.1), failures)
            thread.join(10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, [])


if __name__ == '__main__':
    unittest.main()
