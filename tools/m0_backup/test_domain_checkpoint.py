"""Admission and integrity tests using the owned synthetic recovery fixture."""
from contextlib import closing
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from tools.m0_backup import domain_binding as archive
from tools.m0_backup import domain_checkpoint as checkpoint
from tools.m0_backup.run_domain_recovery_lab import run_child
from sa_dispatch.artifacts import ArtifactError
from sa_dispatch.bundle import ledger, portable


class DomainCheckpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        folder = tempfile.TemporaryDirectory(prefix='test-domain-checkpoint-')
        cls.addClassCleanup(folder.cleanup)
        cls.work = Path(folder.name).resolve()
        cls.source = cls.work / 'source'
        cls.source.mkdir()
        cls.stopped = run_child('--create', cls.source)
        cls.scenario = json.loads((cls.source / 'scenario.json').read_bytes())
        cls.package = cls.work / 'package'
        cls.anchor = checkpoint.capture_checkpoint(cls.source, cls.package, stopped_processes=[cls.stopped],
                                                   creator='checkpoint-tests', code_revision='working-tree')

    def setUp(self):
        folder = tempfile.TemporaryDirectory(dir=self.work)
        self.addCleanup(folder.cleanup)
        self.case = Path(folder.name)
        self.destination = self.case / 'restored'

    def _copy(self):
        return Path(shutil.copytree(self.package, self.case / 'package'))

    def _manifest(self, package, mutate):
        path = package / archive.MANIFEST_NAME
        manifest = json.loads(path.read_bytes())
        mutate(manifest)
        raw = archive.canonical(manifest) + b'\n'
        path.write_bytes(raw)
        return archive._digest(raw)

    def _repoint_dispatch_digest(self, package):
        name = 'dispatch.sqlite3'
        with closing(checkpoint._open_database(package / name)) as db:
            normalized = checkpoint._digest_image(db, name)
        raw = (package / name).read_bytes()

        def change(manifest):
            manifest['files'][name] = {'sha256': archive._digest(raw), 'bytes': len(raw)}
            manifest['databases'][name] = normalized

        return self._manifest(package, change)

    def test_restore_retains_images_holds_and_historical_device(self):
        from knowledge.m0_content.workflow_binding import load_workflow_binding
        fields = checkpoint.restore_checkpoint(self.package, self.destination, expected_manifest_sha256=self.anchor)
        scenario = self.scenario
        self.assertEqual(fields['allowed_unknown_task_ids'], [scenario['task']['task_id']])
        binding = load_workflow_binding(fields['workflow_binding'])
        self.assertEqual((binding.digest, binding.research.digest),
                         (scenario['workflow_sha256'], scenario['research_sha256']))
        self.assertEqual(binding.content_path, fields['content_db'])
        self.assertEqual(binding.recovery_admission.mode, archive.HISTORICAL_READ_ONLY)
        restored = ledger(fields['dispatch_db'], include_devices=True)
        original = ledger(self.source / 'ledger.sqlite3', include_devices=True)
        self.assertEqual(restored, original)
        self.assertTrue(restored['devices'])
        self.assertEqual(restored['control_budgets'][0]['reserved_units'], 2)
        self.assertEqual(restored['control_reservations'], scenario['holds'])
        self.assertTrue(all(row['status'] in ('unknown', 'succeeded') for row in restored['tasks']))
        with self.assertRaisesRegex(ArtifactError, 'settle queued, leased or unknown'):
            portable(restored)

    def test_invalid_manifest_anchor(self):
        for anchor in (None, 'sha256:' + self.anchor, '0' * 64):
            with self.subTest(anchor=anchor), self.assertRaises(archive.RecoveryBindingError):
                checkpoint.restore_checkpoint(self.package, self.destination, expected_manifest_sha256=anchor)
            self.assertFalse(self.destination.exists())

    def test_tampered_cas_rejected(self):
        package = self._copy()
        item = next((package / 'artifacts').glob('sha256/*/*'))
        item.write_bytes(b'corrupt')
        with self.assertRaisesRegex(archive.RecoveryBindingError, 'RECOVERY_FILE_HASH_MISMATCH'):
            checkpoint.restore_checkpoint(package, self.destination, expected_manifest_sha256=self.anchor)
        self.assertFalse(self.destination.exists())

    def test_path_escape_rejected_even_with_matching_anchor(self):
        package = self._copy()
        anchor = self._manifest(package, lambda manifest: manifest['files'].update({
            '../escape.json': {'sha256': '0' * 64, 'bytes': 0}}))
        with self.assertRaisesRegex(archive.RecoveryBindingError, 'INVALID_RECOVERY_FILES'):
            checkpoint.restore_checkpoint(package, self.destination, expected_manifest_sha256=anchor)
        self.assertFalse(self.destination.exists())

    def test_unmatched_budget_hold_rejected_even_with_matching_file_digests(self):
        package = self._copy()
        with closing(sqlite3.connect(package / 'dispatch.sqlite3')) as db, db:
            db.execute('UPDATE control_budgets SET reserved_units=reserved_units+1')
        anchor = self._repoint_dispatch_digest(package)
        with self.assertRaisesRegex(checkpoint.CheckpointError, 'held budget units disagree'):
            checkpoint.restore_checkpoint(package, self.destination, expected_manifest_sha256=anchor)
        self.assertFalse(self.destination.exists())

    def test_missing_intent_rejected_even_with_matching_file_digests(self):
        package = self._copy()
        with closing(sqlite3.connect(package / 'dispatch.sqlite3')) as db, db:
            db.execute("DELETE FROM evidence WHERE task_id=? AND kind='invocation_intent'",
                       (self.scenario['task']['task_id'],))
        anchor = self._repoint_dispatch_digest(package)
        with self.assertRaisesRegex(checkpoint.CheckpointError, 'lacks a supported anchored unknown attempt'):
            checkpoint.restore_checkpoint(package, self.destination, expected_manifest_sha256=anchor)
        self.assertFalse(self.destination.exists())

    def test_requires_owned_exited_process_evidence(self):
        with self.assertRaisesRegex(checkpoint.CheckpointError, 'subprocesses that have exited'):
            checkpoint.capture_checkpoint(self.source, self.destination, stopped_processes=[],
                                          creator='checkpoint-tests', code_revision='working-tree')
        self.assertFalse(self.destination.exists())

    def test_settled_pipeline_retains_content_approval_and_collector_history(self):
        fixture = self.case / 'settled-source'
        completed = subprocess.run([sys.executable, '-B', '-m', 'knowledge.m0_content.run_research_pipeline_lab',
                                    '--output', str(fixture)], cwd=checkpoint.ROOT, capture_output=True, timeout=150)
        self.assertEqual(completed.returncode, 0, completed.stderr.decode('utf-8', errors='replace'))
        package = self.case / 'settled-package'
        anchor = checkpoint.capture_checkpoint(fixture, package, stopped_processes=[completed],
                                               creator='checkpoint-tests', code_revision='working-tree')
        result = checkpoint.restore_checkpoint(package, self.destination, expected_manifest_sha256=anchor)
        self.assertEqual(result['allowed_unknown_task_ids'], [])
        before = ledger(fixture / 'ledger.sqlite3', include_devices=True)
        after = ledger(result['dispatch_db'], include_devices=True)
        self.assertEqual(after, before)
        self.assertTrue(after['control_approvals'])
        self.assertTrue(after['collector_cursors'])
        self.assertTrue(after['unified_events'])


if __name__ == '__main__':
    unittest.main()
