"""Admission and integrity tests using the owned synthetic recovery fixture."""
from contextlib import closing
from dataclasses import asdict, replace
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from tools.m0_backup import domain_binding as archive
from tools.m0_backup import domain_checkpoint as checkpoint
from tools.m0_backup.run_domain_recovery_lab import run_child
from sa_dispatch.artifacts import ArtifactError, ArtifactStore
from sa_dispatch.bundle import ledger, portable
from sa_dispatch.canonical import canonical_json
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.models import DispatchError, Outcome, Ref, TaskSpec
from sa_dispatch.routing import (HTTP_ROUTE, LOCAL_ROUTE, ROUTED_TABLES, activate_mixed,
                                 activation_manifest, binding_evidence, row_digest)
from sa_dispatch.scheduler import Scheduler
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch_hotspot import KINDS


READ_SPEC = TaskSpec('template', 'hotspot.topic_fetch', 1, {'op': 'topic.list', 'filters': {}},
                     (), ('fixture:read',), 'synthetic', 'none', 2)


def store_for(root, clock, route=None):
    return SqliteStore(root / 'ledger.sqlite3', clock=clock, lease_ttl_seconds=10, kinds=KINDS, route=route)


def add_schedule(scheduler, ident, *, spec=READ_SPEC, interval_seconds=10, **kwargs):
    scheduler.upsert(ident, interval_seconds=interval_seconds, task_template=spec,
                     next_fire_at=timestamp(scheduler.store.clock.now()), **kwargs)


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
        cls.schedule_ref = cls._add_http_history(cls.source)
        cls.package = cls.work / 'package'
        cls.anchor = checkpoint.capture_checkpoint(cls.source, cls.package, stopped_processes=[cls.stopped],
                                                   creator='checkpoint-tests', code_revision='working-tree')

    @staticmethod
    def _add_http_history(root):
        clock = FakeClock()
        store = store_for(root, clock, HTTP_ROUTE)
        store.register_device('recovery-http-program', ['fixture:read'])
        scheduler = Scheduler(store)
        add_schedule(scheduler, 'bounded-http', max_slots=4)
        for failures in range(3):
            for attempt in range(failures):
                with patch.object(store, 'submit', side_effect=sqlite3.OperationalError('database is locked')):
                    assert scheduler.tick() == []
                clock.advance(2 ** attempt)
            firing, = scheduler.tick()
            lease = store.claim('recovery-http-program', synthetic_handoff=True)
            assert lease.task_id == firing['task_id']
            store.complete(lease.lease_id, lease.fence, Outcome(result={'count': 0}), 'recovery-http-program')
            clock.advance(10)
        digest = ArtifactStore(root / 'artifacts').put(b'unfired scheduler input')
        ref = Ref('fixture', 'schedule', 'future', '1', digest)
        add_schedule(scheduler, 'template-only', spec=replace(READ_SPEC, input_refs=(ref,)), enabled=False)
        return digest

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
        self.assertEqual(len(restored['schedule_firings']), 3)
        self.assertTrue(all(row['outcome'] == 'created' and row['execution_route'] == HTTP_ROUTE
                            for row in restored['schedule_firings']))
        self.assertEqual([(row['failure_count'], row['rejection_code']) for row in restored['schedule_firings']],
                         [(0, None), (1, 'sqlite_busy'), (2, 'sqlite_busy')])
        enabled = next(row for row in restored['schedules'] if row['id'] == 'bounded-http')
        self.assertEqual((enabled['enabled'], enabled['max_slots']), (1, 4))
        self.assertEqual(ArtifactStore(fields['artifact_root']).get(self.schedule_ref), b'unfired scheduler input')
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

    def test_rejects_inconsistent_scheduler_rows_with_matching_file_digests(self):
        first_fire = "WHERE rowid=(SELECT MIN(rowid) FROM schedule_firings)"
        cases = (
            ('UPDATE schedule_firings SET fire_key=\'other\' ' + first_fire, 'fire key/version'),
            ('UPDATE schedule_firings SET version=8 ' + first_fire, 'fire key/version'),
            ('UPDATE schedule_firings SET task_id=(SELECT task_id FROM schedule_firings ORDER BY rowid DESC LIMIT 1) '
             + first_fire, 'terminal idempotency task'),
            ('UPDATE schedule_firings SET outcome=\'pending\' ' + first_fire, 'pending schedule reservation'),
            ('UPDATE schedule_firings SET outcome=\'completed\' ' + first_fire, 'invalid schedule firing outcome'),
            ('UPDATE schedule_firings SET task_template=json_set(task_template,\'$.params.filters.limit\', 5) '
             + first_fire, 'frozen schedule template'),
            ('UPDATE schedule_firings SET at=\'yesterday\' ' + first_fire, 'invalid scheduler timestamp'),
            ('UPDATE schedule_firings SET failure_count=1 ' + first_fire, 'invalid settled schedule attempt code'),
            ('UPDATE schedule_firings SET failure_count=1.5 ' + first_fire, 'invalid settled schedule attempt count'),
            ("DELETE FROM schedule_events WHERE kind='schedule.retry_scheduled'", 'count lacks retry audit'),
            ("UPDATE schedules SET max_catchup=1.5 WHERE id='bounded-http'", 'invalid schedule metadata'),
            ("UPDATE schedules SET max_slots=3 WHERE id='bounded-http'", 'lifetime slot bound'),
            ("UPDATE schedules SET last_fire_key=NULL WHERE id='bounded-http'", 'last fire key'),
            ("UPDATE schedules SET task_template=json_set(task_template,'$.kind','arbitrary.kind') "
             "WHERE id='template-only'", 'supported read-only task kind'),
            ('DELETE FROM transport_handoffs WHERE task_id=(SELECT task_id FROM schedule_firings LIMIT 1)',
             'valid handoff history'),
            ("INSERT INTO schedule_events VALUES(NULL,'missing',1,'schedule.exhausted',"
             "'2026-01-01T00:00:00.000000+00:00','max_slots_reached')", 'orphan schedule audit'),
        )
        package = self._copy()
        for statement, error in cases:
            with self.subTest(statement=statement):
                shutil.copyfile(self.package / 'dispatch.sqlite3', package / 'dispatch.sqlite3')
                with closing(sqlite3.connect(package / 'dispatch.sqlite3')) as db, db:
                    db.execute(statement)
                anchor = self._repoint_dispatch_digest(package)
                with self.assertRaisesRegex(checkpoint.CheckpointError, error):
                    checkpoint.restore_checkpoint(package, self.destination, expected_manifest_sha256=anchor)
                self.assertFalse(self.destination.exists())

    def test_template_only_missing_cas_rejected_even_with_matching_inventory_anchor(self):
        package = self._copy()
        path = ArtifactStore(package / 'artifacts').path(self.schedule_ref)
        path.unlink()
        anchor = self._manifest(package, lambda value: value['files'].pop(path.relative_to(package).as_posix()))
        with self.assertRaises((FileNotFoundError, archive.RecoveryBindingError, ArtifactError)):
            checkpoint.restore_checkpoint(package, self.destination, expected_manifest_sha256=anchor)
        self.assertFalse(self.destination.exists())

    def test_activation_retains_old_versions_local_rows_and_lifetime_slots(self):
        from knowledge.m0_content.run_research_pipeline_lab import prepare

        fixture = self.case / 'historical-source'
        fixture.mkdir()
        prepare(fixture)
        clock = FakeClock()
        old = store_for(fixture, clock)
        scheduler = Scheduler(old)
        cas = ArtifactStore(fixture / 'artifacts')
        digest = cas.put(b'retired frozen scheduler input')
        ref = Ref('fixture', 'schedule', 'retired', '1', digest)
        add_schedule(scheduler, 'bounded', interval_seconds=60, max_slots=2)
        with self.assertRaises(DispatchError):
            add_schedule(scheduler, 'orphan-template-audit', spec=replace(READ_SPEC, egress='summary'))
        first, = scheduler.tick()
        old.cancel(first['task_id'], 'fixture')
        add_schedule(scheduler, 'local', spec=replace(READ_SPEC, input_refs=(ref,)), max_slots=3)
        clock.advance(20)
        key = f'sched:local:v1:{timestamp(clock.now())}'
        conflict, _ = old.submit(replace(READ_SPEC, idem_key=key))
        old.cancel(conflict.task_id, 'fixture')
        self.assertEqual(scheduler.tick(), [])
        old_local = [row for row in scheduler.firings() if row['schedule_id'] == 'local']
        self.assertEqual([row['outcome'] for row in old_local], ['skipped_catchup', 'skipped_catchup', 'rejected'])
        self.assertEqual((old_local[-1]['rejection_code'], old_local[-1]['task_id']),
                         ('idempotency_conflict', conflict.task_id))
        clock.advance(10)
        add_schedule(scheduler, 'local', enabled=False)
        bindings = self._activation_bindings(old, cas, {
            'tasks': {conflict.task_id: LOCAL_ROUTE},
            'schedules': {'bounded': HTTP_ROUTE, 'local': LOCAL_ROUTE},
            'schedule_firings': {row['fire_key']: LOCAL_ROUTE for row in old_local},
        })
        with closing(old._connect()) as db:
            manifest = activation_manifest(db, actor='fixture', bindings=bindings)
        activate_mixed(old.path, actor='fixture', evidence_ref=cas.put(canonical_json(manifest)),
                       bindings=bindings, artifacts=cas, clock=clock)
        http = store_for(fixture, clock, HTTP_ROUTE)
        scheduler = Scheduler(http)
        add_schedule(scheduler, 'bounded', spec=replace(READ_SPEC, params={'op': 'topic.list', 'filters': {'limit': 2}}))
        second, = scheduler.tick()
        http.cancel(second['task_id'], 'fixture')
        package = self.case / 'historical-package'
        anchor = checkpoint.capture_checkpoint(fixture, package, stopped_processes=[self.stopped],
                                               creator='checkpoint-tests', code_revision='working-tree')
        fields = checkpoint.restore_checkpoint(package, self.destination, expected_manifest_sha256=anchor)
        before, after = (ledger(path, include_devices=True) for path in (old.path, fields['dispatch_db']))
        self.assertEqual(before, after)
        bounded = next(row for row in after['schedules'] if row['id'] == 'bounded')
        self.assertEqual((bounded['max_slots'], bounded['enabled'], bounded['version']), (2, 0, 2))
        self.assertEqual([(row['execution_route'], row['version']) for row in after['schedule_firings']
                          if row['schedule_id'] == 'bounded'], [(None, 1), (HTTP_ROUTE, 2)])
        self.assertTrue(all(row['execution_route'] == LOCAL_ROUTE for row in after['schedule_firings']
                            if row['schedule_id'] == 'local'))
        self.assertIn('orphan-template-audit', {row['schedule_id'] for row in after['schedule_events']})
        self.assertEqual(ArtifactStore(fields['artifact_root']).get(digest), b'retired frozen scheduler input')

    @staticmethod
    def _activation_bindings(store, artifacts, owners):
        bindings = {}
        with closing(store._connect()) as db:
            for table, entities in owners.items():
                bindings[table] = {}
                for key, route in entities.items():
                    row = db.execute(f'SELECT * FROM {table} WHERE {ROUTED_TABLES[table]}=?', (key,)).fetchone()
                    digest = row_digest(row)
                    fence = row['fence'] if table == 'tasks' else None
                    proof = binding_evidence(table, key, route, digest, 'sha256:' + 'a' * 64, fence=fence)
                    bindings[table][key] = {'execution_route': route, 'pre_state_hash': digest,
                                           'evidence_ref': artifacts.put(canonical_json(proof))}
                    if table == 'tasks':
                        bindings[table][key]['fence'] = fence
        return bindings

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
