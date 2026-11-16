"""Ignored audit is retained but never admitted as completion authority."""
from contextlib import closing
import copy
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest

from tools.m0_backup import domain_binding as archive, domain_checkpoint as checkpoint
from tools.m0_backup import run_late_audit_recovery_lab as lab
from sa_dispatch.artifacts import ArtifactError, ArtifactStore
from sa_dispatch.bundle import ledger, portable
from sa_dispatch.canonical import canonical_json
from sa_dispatch.routing import activate_mixed, activation_manifest
from sa_dispatch.store_sqlite import SqliteStore


class LateAuditCheckpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        folder = tempfile.TemporaryDirectory(prefix='test-late-audit-checkpoint-')
        cls.addClassCleanup(folder.cleanup)
        cls.root = Path(folder.name).resolve()
        cls.source = cls.root / 'source'
        cls.source.mkdir()
        cls.stopped = lab.child('--create', cls.source)
        cls.scenario = json.loads((cls.source / 'scenario.json').read_bytes())
        cls.review = checkpoint.review_late_audit(cls.source)
        cls.package = cls.root / 'package'
        cls.anchor = checkpoint.capture_checkpoint(cls.source, cls.package, stopped_processes=[cls.stopped],
            creator='late-audit-tests', code_revision='working-tree', late_audit_review=cls.review)

    def setUp(self):
        self.case = Path(self.enterContext(tempfile.TemporaryDirectory(dir=self.root)))
        self.destination = self.case / 'restored'

    def package_copy(self, name='package'):
        return Path(shutil.copytree(self.package, self.case / name))

    def reanchor(self, package, change=lambda manifest: None):
        path = package / archive.MANIFEST_NAME
        manifest = json.loads(path.read_bytes())
        raw = (package / 'dispatch.sqlite3').read_bytes()
        manifest['files']['dispatch.sqlite3'] = dict(bytes=len(raw), sha256=archive._digest(raw))
        with closing(checkpoint._open_database(package / 'dispatch.sqlite3')) as db:
            manifest['databases']['dispatch.sqlite3'] = checkpoint._digest_image(db, 'dispatch.sqlite3')
        change(manifest)
        raw = archive.canonical(manifest) + b'\n'
        path.write_bytes(raw)
        return archive._digest(raw)

    def restore(self, package=None, anchor=None):
        return checkpoint.restore_checkpoint(package or self.package, self.destination,
                                             expected_manifest_sha256=anchor or self.anchor)

    def test_default_restore_preserves_every_ignored_row_and_cas_without_lookup_authority(self):
        restored = self.restore()
        before = ledger(self.source / 'ledger.sqlite3', include_devices=True)
        after = ledger(restored['dispatch_db'], include_devices=True)
        self.assertEqual(before, after)
        self.assertEqual(len(after['late_results']), 2)
        self.assertEqual({row['disposition'] for row in after['late_results']}, {'ignored'})
        unknown = next(t for t in after['tasks'] if t['task_id'] == self.scenario['task']['task_id'])
        self.assertEqual(unknown['status'], 'unknown')
        cas = ArtifactStore(restored['artifact_root'])
        for row in self.review['rows']:
            self.assertEqual(cas.get(row['cas_ref']), ArtifactStore(self.source / 'artifacts').get(row['cas_ref']))
        from knowledge.m0_content.workflow_binding import load_workflow_binding
        binding = load_workflow_binding(restored['workflow_binding'])
        self.assertEqual(binding.recovery_admission.allowed_unknown_task_ids, ())

    def test_capture_requires_exact_review_not_implicit_allowance(self):
        for review in (None, {}, dict(self.review, rows=[])):
            with self.subTest(review=review), self.assertRaises(ValueError):
                checkpoint.capture_checkpoint(self.source, self.destination, stopped_processes=[self.stopped],
                    creator='test', code_revision='working-tree', late_audit_review=review)
            self.assertFalse(self.destination.exists())

    def test_missing_and_stale_reviews_refused_even_with_recomputed_manifest(self):
        for index, alteration in enumerate(('missing', 'row_hash', 'extra_row')):
            package = self.package_copy(str(index))
            def change(manifest):
                if alteration == 'missing':
                    del manifest['late_audit_review']
                elif alteration == 'row_hash':
                    manifest['late_audit_review']['rows'][0]['row_sha256'] = '0' * 64
                else:
                    manifest['late_audit_review']['rows'].append(copy.deepcopy(self.review['rows'][0]))
            anchor = self.reanchor(package, change)
            with self.subTest(alteration=alteration), self.assertRaises(ValueError):
                self.restore(package, anchor)
            self.assertFalse(self.destination.exists())

    def test_missing_late_only_cas_not_hidden_by_recomputed_file_manifest(self):
        package = self.package_copy()
        ref = self.scenario['late_audit']['unknown_claim_receipt']
        path = ArtifactStore(package / 'artifacts').path(ref)
        relative = path.relative_to(package).as_posix()
        path.unlink()
        anchor = self.reanchor(package, lambda manifest: manifest['files'].pop(relative))
        with self.assertRaises((ValueError, OSError)):
            self.restore(package, anchor)
        self.assertFalse(self.destination.exists())

    def test_damaged_late_only_cas_rejected_even_with_matching_manifest_file_hash(self):
        package = self.package_copy()
        ref = self.scenario['late_audit']['unknown_claim_receipt']
        path = ArtifactStore(package / 'artifacts').path(ref)
        raw = path.read_bytes().replace(b'fake-success-not-authority', b'fake-success-corrupt-bytes')
        path.write_bytes(raw)
        relative = path.relative_to(package).as_posix()
        anchor = self.reanchor(package, lambda manifest: manifest['files'].__setitem__(relative,
            dict(bytes=len(raw), sha256=archive._digest(raw))))
        with self.assertRaises(ValueError):
            self.restore(package, anchor)
        self.assertFalse(self.destination.exists())

    def test_unsupported_late_rows_refused_after_matching_outer_and_review_hashes(self):
        for index, change in enumerate(('used', 'future', 'orphan', 'non_synthetic', 'extra_result')):
            package = self.package_copy(str(index))
            with closing(sqlite3.connect(package / 'dispatch.sqlite3')) as db, db:
                db.row_factory = sqlite3.Row
                row = dict(db.execute('SELECT * FROM late_results WHERE task_id=?',
                                      (self.scenario['task']['task_id'],)).fetchone())
                if change == 'used': row['disposition'] = 'used_for_reconcile'
                if change == 'future': row['fence'] = 99
                if change == 'orphan': row['task_id'] = 'missing-task'
                if change in ('non_synthetic', 'extra_result'):
                    value = json.loads(row['outcome_json'])
                    if change == 'non_synthetic': value['evidence'][0]['access'] = 'local_only'
                    else: value['result']['untrusted_extra'] = 'sha256:' + '0' * 64
                    row['outcome_json'] = canonical_json(value).decode()
                db.execute('UPDATE late_results SET task_id=:task_id,fence=:fence,disposition=:disposition,'
                           'outcome_json=:outcome_json WHERE id=:id', row)
            def rebind(manifest):
                item = next(r for r in manifest['late_audit_review']['rows'] if r['id'] == row['id'])
                item.update({key: row[key] for key in ('task_id', 'fence', 'disposition')})
                item['row_sha256'] = archive._digest(archive.canonical(row))
            anchor = self.reanchor(package, rebind)
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.restore(package, anchor)
            self.assertFalse(self.destination.exists())

    def test_ordinary_v7_still_rejects_terminal_ignored_audit(self):
        source = self.case / 'terminal'
        source.mkdir()
        store = SqliteStore(source / 'ledger.sqlite3', lease_ttl_seconds=1)
        cas = ArtifactStore(source / 'artifacts')
        with closing(store._connect()) as db:
            manifest = activation_manifest(db, actor='fixture', bindings={})
        activate_mixed(store.path, actor='fixture', evidence_ref=cas.put(canonical_json(manifest)),
                       bindings={}, artifacts=cas)
        lab.add_runtime_ignored(source)
        records = ledger(store.path)
        self.assertTrue(all(task['status'] == 'succeeded' for task in records['tasks']))
        with self.assertRaisesRegex(ArtifactError, 'late receipts'):
            portable(records)


if __name__ == '__main__':
    unittest.main()
