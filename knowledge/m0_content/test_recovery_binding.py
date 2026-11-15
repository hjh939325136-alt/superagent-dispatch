"""Exercise recovery bindings on fresh synthetic copies in temporary directories."""
from contextlib import closing, redirect_stdout
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from knowledge.m0_content.contract import timestamp
from knowledge.m0_content.dispatch_component import invoke
from knowledge.m0_content.dispatch_entry import WorkflowKind, main as workflow_main
from knowledge.m0_content.recovery_dispatch import run_recovery_dispatch
from knowledge.m0_content.test_workflow_content import APPROVE, SCRIPT, setup
from knowledge.m0_content.workflow_binding import (WorkflowError, load_workflow_binding, request_base,
                                                   search_modules)
from knowledge.m0_content.workflow_content import lookup_content, write_content
from sa_dispatch.canonical import sha256_ref
from sa_dispatch.executor import receipt_bytes
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.invocation import WORKFLOW_PARAMS
from sa_dispatch.models import Evidence, Outcome, TaskSpec
from sa_dispatch.routing import activate_mixed, activation_manifest
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.unified_events import UnifiedEventStore
from tools.m0_backup.domain_binding import (HISTORICAL_READ_ONLY, MANIFEST_NAME, MANIFEST_SCHEMA,
    ORIGINAL_FILES, REHEARSAL_LOOKUP_ONLY, REQUIRED_FILES, RecoveryBindingError, canonical,
    write_recovery_bindings)


TASK_ID = 't_' + '1' * 32
APPROVAL_TASK_ID = 't_' + '2' * 32


def request(operation, task_id, params, *, write=True):
    return {'schema_version': '0.1', 'request_id': task_id, 'operation': operation,
            'idempotency_key': f'{task_id}:{operation}@1' if write else None, 'payload': params}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def ledger_snapshot(path):
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
        return (sha(path.read_bytes()), db.execute('PRAGMA user_version').fetchone(),
                sha('\n'.join(db.iterdump()).encode('utf-8')))


class RecoveryBindingTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix='m0-recovery-binding-')
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).absolute()
        self.source = self.root / 'source'
        self.source.mkdir()
        self.original, self.params = setup(self.source)
        self.recovered_root = self.root / 'recovered'
        self.recovered_root.mkdir()
        self.bound, _, _, self.research = search_modules()

    def stage(self, *, ledger=None):
        root = self.recovered_root
        shutil.copytree(self.original.research.catalog_root, root / 'catalog')
        shutil.copyfile(self.original.research.path, root / ORIGINAL_FILES['research'])
        shutil.copyfile(self.original.path, root / ORIGINAL_FILES['workflow'])
        shutil.copyfile(self.original.content_path, root / 'content.sqlite3')
        if ledger is not None:
            store = SqliteStore(self.source / 'dispatch.sqlite3', lease_ttl_seconds=90)
            artifacts = ArtifactStore(self.source / 'artifacts')
            if ledger == 'mixed':
                actor = 'synthetic-recovery-binding-test'
                with closing(store._connect()) as db:
                    document = activation_manifest(db, actor=actor, bindings={})
                activate_mixed(store.path, actor=actor, evidence_ref=artifacts.put(receipt_bytes(document)),
                               bindings={}, artifacts=artifacts)
            class FixtureKind:
                name, version, enabled, side_effect = 'research.search', 1, True, 'none'

                def validate(self, params):
                    return params

                def scope(self, params):
                    return {}

            store = SqliteStore(store.path, lease_ttl_seconds=90, route='local_synthetic' if ledger == 'mixed' else None,
                                kinds={'research.search': FixtureKind()})
            store.register_device('test', ())
            task, _ = store.submit(TaskSpec('recovery-test-read', 'research.search', 1, {}, (), (), 'synthetic', 'none', 1))
            lease = store.claim('test')
            response = {'test': 'synthetic-recovery-receipt'}
            receipt = artifacts.put(receipt_bytes(response))
            store.complete(lease.lease_id, lease.fence, Outcome(result={'receipt': receipt},
                evidence=(Evidence('component_response', receipt, 'synthetic', artifact_hash=receipt),)), 'test')
            UnifiedEventStore(store.path).project_task(store, task.task_id)
            shutil.copyfile(store.path, root / 'dispatch.sqlite3')
            shutil.copytree(artifacts.root, root / 'artifacts')
            return task.task_id, response
        else:
            (root / 'artifacts').mkdir()
            with closing(sqlite3.connect(root / 'dispatch.sqlite3')) as db:
                db.execute('CREATE TABLE synthetic_recovery_test (id TEXT)')

    def manifest(self, *, allow=(TASK_ID,), files=REQUIRED_FILES):
        root = self.recovered_root
        research = json.loads((root / ORIGINAL_FILES['research']).read_bytes())
        workflow = json.loads((root / ORIGINAL_FILES['workflow']).read_bytes())
        entries = {}
        artifact_files = {path.relative_to(root).as_posix() for path in (root / 'artifacts').rglob('*') if path.is_file()}
        for name in set(files) | artifact_files:
            raw = (root / name).read_bytes()
            entries[name] = {'sha256': sha(raw), 'bytes': len(raw)}
        manifest = {'schema': MANIFEST_SCHEMA, 'synthetic_only': True,
            'bindings': {'research_sha256': 'sha256:' + sha(canonical(research)),
                         'workflow_sha256': 'sha256:' + sha(canonical(workflow)),
                         'catalog_instance': research['catalog_instance'],
                         'content_instance': workflow['content_instance']},
            'allowed_unknown_task_ids': list(allow), 'files': entries}
        raw = canonical(manifest) + b'\n'
        (root / MANIFEST_NAME).write_bytes(raw)
        return sha(raw)

    def recover(self, mode=HISTORICAL_READ_ONLY):
        self.stage()
        return write_recovery_bindings(self.recovered_root, expected_manifest_sha256=self.manifest(), mode=mode)

    def writes(self):
        script_request = request(SCRIPT, TASK_ID, {'workflow_binding': self.original.digest, **self.params})
        script = invoke(self.original, script_request)['data']
        approval_params = {name: self.params[name] for name in ('trace_id', 'context_id', 'manifest_sha256')}
        approval_params.update(workflow_binding=self.original.digest, script_ref=script['script_ref'])
        expires = datetime.now(timezone.utc) + timedelta(minutes=30)
        approval = invoke(self.original, request(APPROVE, APPROVAL_TASK_ID, {**approval_params,
            'control_approval': {'approval_id': 'ap_recovery', 'proof_hash': 'sha256:' + 'a' * 64,
                'task_id': APPROVAL_TASK_ID, 'scope': 'script_ready', 'actor': 'synthetic-operator',
                'expires_at': expires.isoformat()}}))['data']
        return script_request, approval_params | {'approval_ref': approval['approval_ref']}

    def lookup_request(self, binding, original, task_id=TASK_ID):
        kind = WorkflowKind(original['operation'], binding)
        task = SimpleNamespace(task_id=task_id, fence=1)
        return kind.lookup_request(task, original, sha256_ref(receipt_bytes(original)))

    def test_source_moved_and_original_hashes_unchanged(self):
        _, approval = self.writes()
        research_path, workflow_path = self.recover()
        self.source.rename(self.root / 'source-unavailable')
        restored = load_workflow_binding(workflow_path)
        self.assertEqual(restored, load_workflow_binding(workflow_path))
        self.assertEqual(restored.research, self.bound.load_binding(research_path))
        self.assertEqual(restored.digest, self.original.digest)
        self.assertEqual(restored.research.digest, self.original.research.digest)
        self.assertEqual(restored.path, workflow_path)
        self.assertEqual(restored.research.path, research_path)
        self.assertEqual(restored.content_path, self.recovered_root / 'content.sqlite3')
        self.assertEqual(restored.research.catalog_root, self.recovered_root / 'catalog')
        self.assertEqual(restored.recovery_digest, restored.research.recovery_digest)
        self.assertEqual(restored.recovery_admission.mode, HISTORICAL_READ_ONLY)
        context = self.bound.execute_bound('research.context_get', request_base(timestamp()) | {
            'context_id': self.params['context_id'], 'source_binding': restored.research.digest}, restored.research)
        self.assertTrue(context['usable'])
        self.assertEqual(context['revocation_basis'], 'captured_snapshot')
        status = invoke(restored, request('content.approval_status', 'recovery-read', approval, write=False))['data']
        self.assertEqual(status['approval_ref'], approval['approval_ref'])
        self.assertEqual(status['post_snapshot_revocations'], 'unverified')

    def test_recovered_reads_keep_authorization_and_current_expiry_checks(self):
        _, approval = self.writes()
        _, workflow_path = self.recover()
        restored = load_workflow_binding(workflow_path)
        bad = request_base(timestamp()) | {'caller': {'agent': 'different'},
            'q': '剪辑', 'source_binding': restored.research.digest}
        with self.assertRaises(ValueError):
            self.bound.execute_bound('research.search', bad, restored.research)
        distant = (datetime.now(timezone.utc) + timedelta(days=3650)).isoformat()
        with patch.object(self.research, '_now', return_value=distant):
            status = invoke(restored, request('content.approval_status', 'read-expired', approval, write=False))['data']
        self.assertFalse(status['current_usable'])

    def test_only_explicit_task_ids_can_reconcile_and_writes_stay_denied(self):
        original, _ = self.writes()
        lookup = self.lookup_request(self.original, original)
        _, workflow_path = self.recover(REHEARSAL_LOOKUP_ONLY)
        restored = load_workflow_binding(workflow_path)
        self.assertEqual(restored.recovery_admission.allowed_unknown_task_ids, (TASK_ID,))
        self.assertEqual(self.lookup_request(restored, original), lookup)
        result = invoke(restored, lookup)['data']
        self.assertEqual(result['state'], 'committed')
        self.assertEqual(result['response_hash'], sha256_ref(receipt_bytes(invoke(self.original, original))))
        other_original = request(SCRIPT, APPROVAL_TASK_ID, original['payload'])
        other = self.lookup_request(self.original, other_original, APPROVAL_TASK_ID)
        for action in (lambda: invoke(restored, other),
                       lambda: self.lookup_request(restored, original, APPROVAL_TASK_ID),
                       lambda: lookup_content(restored, SCRIPT, other['payload']['write_key'], other['payload']['request_hash']),
                       lambda: self.research.lookup(restored.research, 'context.assemble',
                           f'{APPROVAL_TASK_ID}:context.assemble@1', lookup['payload']['request_hash'])):
            with self.assertRaisesRegex(RecoveryBindingError, 'RECOVERY_LOOKUP_NOT_ADMITTED'):
                action()
        for op in (SCRIPT, APPROVE, 'context.assemble'):
            params = dict.fromkeys(WORKFLOW_PARAMS[op]) | {'workflow_binding': restored.digest}
            kind = WorkflowKind(op, restored)
            with self.assertRaisesRegex(RecoveryBindingError, 'RECOVERY_DOMAIN_WRITE_DENIED'):
                kind.validate(params)
            with self.assertRaisesRegex(RecoveryBindingError, 'RECOVERY_DOMAIN_WRITE_DENIED'):
                invocation = params | ({'control_approval': {}} if op == APPROVE else {})
                invoke(restored, request(op, 'new-task', invocation))
        with self.assertRaisesRegex(RecoveryBindingError, 'RECOVERY_DOMAIN_WRITE_DENIED'):
            write_content(restored, SCRIPT, {}, 'new', 'sha256:' + 'a' * 64, None)
        with self.assertRaisesRegex(RecoveryBindingError, 'RECOVERY_DOMAIN_WRITE_DENIED'):
            self.bound.authorize_bound({}, restored.research, 'research.context_reassemble')

    def test_historical_mode_denies_lookup_even_with_admitted_manifest_id(self):
        original = request(SCRIPT, TASK_ID, {})
        lookup = self.lookup_request(self.original, original)
        _, workflow_path = self.recover()
        restored = load_workflow_binding(workflow_path)
        self.assertEqual(restored.recovery_admission.allowed_unknown_task_ids, ())
        for action in (lambda: self.lookup_request(restored, original), lambda: invoke(restored, lookup)):
            with self.assertRaisesRegex(RecoveryBindingError, 'RECOVERY_LOOKUP_NOT_ADMITTED'):
                action()

    def test_expected_manifest_pin_and_file_hashes_required_to_write(self):
        self.stage()
        expected = self.manifest()
        with self.assertRaisesRegex(RecoveryBindingError, 'RECOVERY_MANIFEST_CHANGED'):
            write_recovery_bindings(self.recovered_root, expected_manifest_sha256='0' * 64)
        extra = self.recovered_root / 'unlisted-config.json'
        extra.write_bytes(b'{}')
        with self.assertRaisesRegex(RecoveryBindingError, 'UNEXPECTED_RECOVERY_FILE'):
            write_recovery_bindings(self.recovered_root, expected_manifest_sha256=expected)
        extra.unlink()
        with (self.recovered_root / 'dispatch.sqlite3').open('ab') as db:
            db.write(b'mutation')
        with self.assertRaisesRegex(RecoveryBindingError, 'RECOVERY_FILE_HASH_MISMATCH'):
            write_recovery_bindings(self.recovered_root, expected_manifest_sha256=expected)
        self.assertFalse((self.recovered_root / 'research-binding.json').exists())

    def test_envelope_role_locator_edits_and_original_mutation_rejected(self):
        research_path, _ = self.recover()
        raw = research_path.read_bytes()
        for key, value in [('role', 'unknown'), ('locators', {'catalog_root': '../source/research'}),
                           ('locators', {'catalog_root': str(self.original.research.catalog_root)}),
                           ('locators', {'catalog_root': 'catalog', 'unknown': 'other'})]:
            with self.subTest(key=key, value=value):
                research_path.write_bytes(canonical(json.loads(raw) | {key: value}))
                with self.assertRaisesRegex(RecoveryBindingError, 'INVALID_RECOVERY_ENVELOPE'):
                    self.bound.load_binding(research_path)
        research_path.write_bytes(raw)
        original = self.recovered_root / ORIGINAL_FILES['research']
        original_raw = original.read_bytes()
        for invalid in (original_raw + b'\n', canonical(json.loads(original_raw) | {'catalog_instance': 'substitute'})):
            original.write_bytes(invalid)
            with self.assertRaisesRegex(RecoveryBindingError, 'ORIGINAL_BINDING_CHANGED'):
                self.bound.load_binding(research_path)

    def test_mixed_modes_and_stale_loaded_objects_rejected(self):
        research_path, workflow_path = self.recover()
        binding = load_workflow_binding(workflow_path)
        research_path.write_bytes(canonical(json.loads(research_path.read_bytes()) | {'mode': REHEARSAL_LOOKUP_ONLY}))
        with self.assertRaisesRegex(WorkflowError, 'RECOVERY_RESEARCH_BINDING_MISMATCH'):
            load_workflow_binding(workflow_path)
        with self.assertRaisesRegex(RecoveryBindingError, 'RECOVERY_BINDING_CHANGED'):
            binding.research.catalog()
        workflow_path.write_bytes(canonical(json.loads(workflow_path.read_bytes()) | {'mode': REHEARSAL_LOOKUP_ONLY}))
        fresh = load_workflow_binding(workflow_path)
        self.assertEqual(fresh.digest, binding.digest)
        self.assertNotEqual(fresh, binding)

    def test_missing_replaced_and_hardlinked_data_never_use_source(self):
        _, workflow_path = self.recover()
        target = self.recovered_root / 'content.sqlite3'
        target.unlink()
        with self.assertRaises(FileNotFoundError):
            load_workflow_binding(workflow_path)
        self.assertTrue(self.original.content_path.exists())
        os.link(self.original.content_path, target)
        with self.assertRaisesRegex(RecoveryBindingError, 'UNSAFE_RECOVERY_PATH'):
            load_workflow_binding(workflow_path)
        target.unlink()
        shutil.copyfile(self.original.content_path, target)
        with self.assertRaisesRegex(RecoveryBindingError, 'RECOVERY_FILE_CHANGED'):
            load_workflow_binding(workflow_path)

    def test_linked_catalog_rejected(self):
        research_path, _ = self.recover()
        copied = self.recovered_root / 'catalog'
        copied.rename(self.recovered_root / 'moved-catalog')
        try:
            copied.symlink_to(self.original.research.catalog_root, target_is_directory=True)
        except OSError as error:
            if os.name != 'nt':
                self.skipTest(f'symlink creation unavailable: {error}')
            result = subprocess.run([os.environ.get('COMSPEC', 'cmd.exe'), '/d', '/c', 'mklink', '/J',
                                      str(copied), str(self.original.research.catalog_root)], capture_output=True)
            if result.returncode != 0:
                self.skipTest('Windows junction creation unavailable')
        with self.assertRaisesRegex(RecoveryBindingError, 'UNSAFE_RECOVERY_PATH'):
            self.bound.load_binding(research_path)

    def test_wrong_content_instance_rejected_even_when_manifest_and_original_agree(self):
        self.stage()
        original = self.recovered_root / ORIGINAL_FILES['workflow']
        original.write_bytes(canonical(json.loads(original.read_bytes()) | {'content_instance': 'foreign'}))
        _, workflow_path = write_recovery_bindings(self.recovered_root, expected_manifest_sha256=self.manifest())
        with self.assertRaisesRegex(WorkflowError, 'CONTENT_INSTANCE_CHANGED'):
            load_workflow_binding(workflow_path)

    def test_foreign_historical_path_syntax_is_validated_without_dereferencing(self):
        self.stage()
        research_path = self.recovered_root / ORIGINAL_FILES['research']
        workflow_path = self.recovered_root / ORIGINAL_FILES['workflow']
        research = json.loads(research_path.read_bytes()) | {'catalog_root': 'Z:\\missing\\catalog'}
        research_path.write_bytes(canonical(research))
        workflow = json.loads(workflow_path.read_bytes()) | {'research_binding_sha256': 'sha256:' + sha(canonical(research)),
            'research_binding': '/missing/research-binding.json', 'content_db': '/missing/content.sqlite3'}
        workflow_path.write_bytes(canonical(workflow))
        _, active = write_recovery_bindings(self.recovered_root, expected_manifest_sha256=self.manifest())
        restored = load_workflow_binding(active)
        self.assertEqual(restored.digest, 'sha256:' + sha(canonical(workflow)))
        self.assertEqual(restored.research.catalog_root, self.recovered_root / 'catalog')
        result = self.bound.execute_bound('research.search', request_base(timestamp()) | {
            'q': '剪辑', 'source_binding': restored.research.digest}, restored.research)
        self.assertTrue(result['results'])

    def test_cli_rejects_mutating_commands_and_foreign_ledger_before_dispatch(self):
        task_id, response = self.stage(ledger='mixed')
        research, workflow = write_recovery_bindings(self.recovered_root, expected_manifest_sha256=self.manifest(allow=(task_id,)),
                                                     mode=REHEARSAL_LOOKUP_ONLY)
        import importlib.util
        spec = importlib.util.spec_from_file_location('m0_test_research_dispatch',
            Path(__file__).resolve().parents[1] / 'm0_search/dispatch_entry.py')
        research_entry = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(research_entry)
        root = self.recovered_root
        config = self.root / 'recovery.toml'
        research_argv = [sys.executable, '-B', str(Path(research_entry.__file__).with_name('dispatch_component.py')),
                         '--source-binding', str(research)]
        workflow_argv = [sys.executable, '-B', str(Path(__file__).with_name('dispatch_component.py')),
                         '--workflow-binding', str(workflow)]

        def configure(db, *, combined):
            components = f'\n[components.research]\nplatform="any"\nargv={json.dumps(research_argv)}\n'
            if combined:
                components += f'\n[components.research_content]\nplatform="any"\nargv={json.dumps(workflow_argv)}\n'
            config.write_text(f'db_path={json.dumps(str(db))}\n'
                f'artifact_root={json.dumps(str(root / "artifacts"))}\n'
                f'journal_dir={json.dumps(str(root / "journal"))}\n'
                f'device_id="test"\ncapabilities=[]\nlease_ttl_seconds=90\ncomponent_timeout_seconds=30\n'
                f'plugins={json.dumps(["research", "research_content"] if combined else ["research"])}\n'
                f'synthetic=true\nexecution_route="local_synthetic"\n{components}', encoding='utf-8')

        def cli(entry, *args):
            output = io.StringIO()
            with redirect_stdout(output):
                exit_code = entry(['--config', str(config), *args])
            return exit_code, json.loads(output.getvalue())

        db = root / 'dispatch.sqlite3'
        before = ledger_snapshot(db)
        for combined, entry in ((True, workflow_main), (False, research_entry.main)):
            configure(db, combined=combined)
            for operation in ('submit', 'run-once', 'retry', 'schedule-tick', 'control', 'device-register', 'recover'):
                with self.subTest(combined=combined, operation=operation):
                    code, result = cli(entry, operation)
                    self.assertEqual(code, 2)
                    self.assertEqual(result['error']['code'], 'RECOVERY_DISPATCH_COMMAND_DENIED')
            code, result = cli(entry, 'reconcile', APPROVAL_TASK_ID)
            self.assertEqual(code, 2)
            self.assertEqual(result['error']['code'], 'RECOVERY_LOOKUP_NOT_ADMITTED')
            if combined:
                code, result = cli(entry, 'reconcile', task_id)
                self.assertEqual(code, 2)
                self.assertEqual(result['error']['code'], 'RECOVERY_TASK_NOT_UNKNOWN')
            with patch('knowledge.m0_content.recovery_dispatch.SqliteStore',
                       side_effect=AssertionError('read operation constructed a write-capable store')):
                code, result = cli(entry, 'status')
                self.assertEqual(code, 0)
                self.assertEqual([task['task_id'] for task in result['data']['tasks']], [task_id])
                self.assertEqual(result['data']['revocation_basis'], 'captured_snapshot')
                code, result = cli(entry, 'status', task_id)
                self.assertEqual(code, 0)
                self.assertEqual(result['data']['task']['status'], 'succeeded')
                code, result = cli(entry, 'trace', task_id)
                self.assertEqual(code, 0)
                self.assertTrue(result['data']['events'])
                self.assertTrue(result['data']['ledger_events'])
                code, result = cli(entry, 'receipt', task_id)
                self.assertEqual(code, 0)
                self.assertEqual(result['data']['response'], response)
                code, result = cli(entry, 'schedule-status')
                self.assertEqual(code, 0)
                self.assertEqual(result['data']['schedules'], [])
                self.assertFalse(result['data']['execution_authorized'])
                self.assertFalse(result['data']['scheduler_started'])
                self.assertTrue(result['data']['stored_flags_are_historical'])
                code, result = cli(entry, 'schedule-status', task_id)
                self.assertEqual(code, 2)
                self.assertEqual(result['error']['code'], 'RECOVERY_DISPATCH_COMMAND_DENIED')
            foreign = self.root / 'foreign.sqlite3'
            configure(foreign, combined=combined)
            code, result = cli(entry, 'status')
            self.assertEqual(code, 2)
            self.assertEqual(result['error']['code'], 'RECOVERY_DISPATCH_PATH_MISMATCH')
            self.assertFalse(foreign.exists())
        self.assertEqual(ledger_snapshot(db), before)

    def test_cli_reads_copied_settled_isolated_ledger(self):
        task_id, response = self.stage(ledger='isolated')
        root = self.recovered_root
        _, workflow = write_recovery_bindings(root, expected_manifest_sha256=self.manifest(allow=()))
        self.source.rename(self.root / 'original-unavailable')
        restored = load_workflow_binding(workflow)
        db = root / 'dispatch.sqlite3'
        config = SimpleNamespace(db_path=db, artifact_root=root / 'artifacts', journal_dir=root / 'journal',
                                 synthetic=True, execution_route=None)
        before = ledger_snapshot(db)
        with patch('knowledge.m0_content.recovery_dispatch.SqliteStore',
                   side_effect=AssertionError('isolated observation constructed a write-capable store')):
            def observe(*args):
                return run_recovery_dispatch(config, {}, restored, 'workflow', args)

            self.assertEqual(observe('status', task_id)['task']['status'], 'succeeded')
            self.assertEqual(observe('receipt', task_id)['response'], response)
            trace = observe('trace', task_id)
            self.assertTrue(trace['events'])
            self.assertEqual(trace['ledger_events'][-1]['to_status'], 'succeeded')
            self.assertEqual(observe('schedule-status')['schedules'], [])
            config.execution_route = 'local_synthetic'
            with self.assertRaisesRegex(RecoveryBindingError, 'RECOVERY_DISPATCH_ROUTE_REQUIRED'):
                observe('status', task_id)
        self.assertEqual(ledger_snapshot(db), before)


if __name__ == '__main__':
    unittest.main()
