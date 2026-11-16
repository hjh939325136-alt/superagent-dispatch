"""Native recovery using the real staged domains and a disposable dispatch ledger."""
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from dispatch.sa_dispatch.artifacts import ArtifactStore
from dispatch.sa_dispatch.store_sqlite import SqliteStore
from tools.m0_native_mixed import domains, recovery_set
from tools.m0_native_mixed.maintenance import MaintenanceError, digest


class Interrupted(BaseException):
    pass


class RecoveryTests(TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        config = self.base / 'etc/sa-dispatch'
        system = config.parent / 'systemd/system'
        data = self.base / 'var/lib/sa-dispatch'
        legacy = data.with_name('sa-dispatch-manager')
        domain = data.with_name('sa-dispatch-native') / 'domain'
        self.layout = SimpleNamespace(
            config_root=config, ledger=data / 'dispatch.sqlite3', artifacts=data / 'artifacts',
            domain=domain, legacy_bindings=legacy, http_journal=data / 'journal',
            service_files=(system / 'sa-dispatch.service',
                           system / 'sa-dispatch.service.d/10-m0-lab-port.conf',
                           system / 'sa-dispatch.service.d/native-mixed-guard.conf'))
        self.destination = config / '.mixed-maintenance/test-run/recovery'
        self.destination.parent.mkdir(parents=True)
        domain.parent.mkdir(parents=True)
        SqliteStore(self.layout.ledger, lease_ttl_seconds=30)
        self.layout.artifacts.mkdir()
        domains.stage_domains(domain, ledger=self.layout.ledger, artifacts=self.layout.artifacts,
                              source_root=domains.ROOT, executable=Path(sys.executable).resolve(),
                              device_id='local-evidence-fixture')
        self.http = config / 'coordinator.toml'
        self.http.write_text('\n'.join(f'{key} = {json.dumps(str(value))}' for key, value in (
            ('db_path', self.layout.ledger), ('artifact_root', self.layout.artifacts),
            ('journal_dir', self.layout.http_journal))), encoding='utf-8')
        self.credentials = config / 'identities.json'
        self.credentials.write_bytes(b'private fixture credential')
        unit, dropin, _ = self.layout.service_files
        unit.parent.mkdir(parents=True)
        unit.write_bytes(b'[Service]\nExecStart=/usr/bin/fixture --identities /etc/sa-dispatch/identities.json\n')
        dropin.parent.mkdir()
        dropin.write_bytes(b'[Service]\nWorkingDirectory=/var/lib/sa-dispatch\n')
        legacy.mkdir()
        for name in recovery_set.LEGACY:
            (legacy / name).write_bytes(b'{"synthetic":true}')
        self.late_ref = ArtifactStore(self.layout.artifacts).put(b'ignored outcome payload')
        self.journal = domain / 'journal' / ('l_' + '0' * 32 + '.jsonl')
        self.journal.write_bytes(b'{"fixture":"uncommitted journal evidence"}\n')
        with closing(sqlite3.connect(self.layout.ledger)) as db, db:
            db.execute("INSERT INTO late_results VALUES ('late','t_test',3,?, '2026-01-01','ignored')",
                       (json.dumps({'artifact': self.late_ref}),))
            db.execute('CREATE TABLE fixture_preservation(value BLOB)')
            db.execute('INSERT INTO fixture_preservation VALUES (?)', (b'original extra SQLite bytes',))
        self.plan = {'operation_id': 'test-run', 'native_root': str(config),
                     'anchor': {'source_files': {'fixture.py': 'fixture-source-hash'}},
                     'configs': [{'path': name, 'before_sha256': (
                         digest(self.http.read_bytes()) if name == self.http.name else None)}
                         for name in ('coordinator.toml', 'local.toml', *recovery_set.LEGACY)]}

    def capture(self):
        return recovery_set.capture(self.layout, self.plan, self.destination)

    def verify(self, receipt):
        return recovery_set.verify(self.layout, self.plan, self.destination, receipt)

    def test_all_six_images_and_original_aliases_with_metadata_only_credentials(self):
        original_read = recovery_set._read

        def check_reads(path, **kwargs):
            self.assertNotEqual(path, self.credentials)
            return original_read(path, **kwargs)

        with patch.object(recovery_set, '_read', side_effect=check_reads):
            receipt = self.capture()
            self.verify(receipt)
        manifest = json.loads((self.destination / recovery_set.MANIFEST).read_bytes())
        self.assertEqual(receipt['database_count'], 6)
        self.assertEqual(manifest['entries']['ledger/dispatch.sqlite3']['source'], str(self.layout.ledger))
        self.assertEqual(manifest['entries']['config/identities.json']['kind'], 'metadata')
        self.assertNotIn('sha256', manifest['entries']['config/identities.json'])
        self.assertFalse((self.destination / 'config/identities.json').exists())
        for name in ('local.toml', *recovery_set.LEGACY):
            self.assertEqual(manifest['entries'][f'config/{name}']['kind'], 'absent')
        self.assertEqual(manifest['entries']['http-journal']['kind'], 'absent')
        ledger = self.destination / 'ledger/dispatch.sqlite3'
        with closing(sqlite3.connect(ledger.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
            self.assertEqual(db.execute('SELECT outcome_json, disposition FROM late_results').fetchall(),
                             [(json.dumps({'artifact': self.late_ref}), 'ignored')])
            self.assertEqual(db.execute('SELECT value FROM fixture_preservation').fetchall(),
                             [(b'original extra SQLite bytes',)])
        ref = self.late_ref.removeprefix('sha256:')
        self.assertEqual((self.destination / 'cas/sha256' / ref[:2] / ref).read_bytes(),
                         b'ignored outcome payload')
        self.assertEqual((self.destination / 'domain/journal' / self.journal.name).read_bytes(),
                         self.journal.read_bytes())

        self.http.write_bytes(b'legitimate newly activated config')
        ArtifactStore(self.layout.artifacts).put(b'activation proof written after recovery')
        self.verify(receipt)
        with self.assertRaisesRegex(MaintenanceError, 'aliases|baseline|manifest|byte|changed'):
            self.http.write_bytes(b'journal_dir = ""\ndb_path = ""\nartifact_root = ""\n')
            self.capture()

    def interrupt(self):
        original_publish = recovery_set._publish
        targets = []

        def lose_power(path, raw):
            targets.append(path)
            if len(targets) == 3:
                path.with_name(path.name + recovery_set.PENDING).write_bytes(raw)
                raise Interrupted
            original_publish(path, raw)

        with patch.object(recovery_set, '_publish', side_effect=lose_power), self.assertRaises(Interrupted):
            self.capture()
        return targets

    def test_exact_partial_recovery_resumes_and_repeated_capture_is_inert(self):
        manifest, copied, pending = self.interrupt()
        manifest_before = manifest.stat().st_ino
        payload_before = copied.stat().st_ino
        receipt = self.capture()
        self.assertEqual((manifest.stat().st_ino, copied.stat().st_ino), (manifest_before, payload_before))
        self.assertTrue(pending.is_file())
        self.assertFalse(pending.with_name(pending.name + recovery_set.PENDING).exists())
        with patch.object(recovery_set, '_publish', side_effect=AssertionError('unnecessary file publication')):
            self.assertEqual(self.capture(), receipt)
        self.verify(receipt)

    def test_changed_source_and_unknown_partial_file_fail_without_replacement(self):
        manifest, copied, pending = self.interrupt()
        raw = manifest.read_bytes()
        unknown = self.destination / 'unknown.txt'
        unknown.write_bytes(b'keep')
        with self.assertRaisesRegex(MaintenanceError, 'unknown'):
            self.capture()
        self.assertEqual(unknown.read_bytes(), b'keep')
        unknown.unlink()
        self.journal.write_bytes(b'new journal data')
        with self.assertRaisesRegex(MaintenanceError, 'source set changed'):
            self.capture()
        self.assertEqual(manifest.read_bytes(), raw)
        self.assertTrue(copied.exists())
        self.assertFalse(pending.exists())

    def test_partial_bytes_and_completed_corruption_fail_closed(self):
        _, copied, pending = self.interrupt()
        temp = pending.with_name(pending.name + recovery_set.PENDING)
        expected = temp.read_bytes()
        self.assertGreater(len(expected), 0)
        temp.write_bytes(b'')
        with self.assertRaisesRegex(MaintenanceError, 'differ'):
            self.capture()
        self.assertEqual(temp.read_bytes(), b'')
        temp.write_bytes(expected)
        receipt = self.capture()
        copied.write_bytes(b'altered archive bytes')
        with self.assertRaisesRegex(MaintenanceError, 'hash differs|byte limit'):
            self.verify(receipt)

    def test_unknown_file_and_sidecars_refuse_capture(self):
        unknown = self.layout.artifacts / 'not-a-cas-shard'
        unknown.write_bytes(b'leave alone')
        with self.assertRaisesRegex(MaintenanceError, 'unknown'):
            self.capture()
        self.assertFalse(self.destination.exists())
        unknown.unlink()
        sidecar = self.layout.ledger.with_name(self.layout.ledger.name + '-wal')
        sidecar.write_bytes(b'potential dirty WAL')
        with self.assertRaisesRegex(MaintenanceError, 'sidecar'):
            self.capture()
        self.assertEqual(sidecar.read_bytes(), b'potential dirty WAL')
        self.assertFalse(self.destination.exists())

    def test_hardlinks_and_junction_or_symlink_are_rejected(self):
        external = self.base / 'external.json'
        os.link(self.credentials, external)
        with self.assertRaisesRegex(MaintenanceError, 'unlinked regular file'):
            self.capture()
        external.unlink()
        outside = self.base / 'outside'
        outside.mkdir()
        try:
            self.destination.symlink_to(outside, target_is_directory=True)
        except OSError as exc:
            if sys.platform != 'win32' or getattr(exc, 'winerror', None) != 1314:
                raise
            from _winapi import CreateJunction
            CreateJunction(str(outside), str(self.destination))
        with self.assertRaisesRegex(MaintenanceError, 'reparse|unlinked'):
            self.capture()
        self.assertEqual(list(outside.iterdir()), [])

    def test_bound_destination_and_service_secret_refused(self):
        with self.assertRaisesRegex(MaintenanceError, 'destination'):
            recovery_set.capture(self.layout, self.plan, self.base / 'outside')
        self.layout.service_files[0].write_bytes(b'[Service]\nEnvironment = SECRET=dont-copy\n')
        with self.assertRaisesRegex(MaintenanceError, 'service environment'):
            self.capture()
        self.assertFalse(self.destination.exists())

    def test_total_size_of_evidence_and_manifest_is_bounded(self):
        manifest, _ = recovery_set._source_snapshot(self.layout, self.plan, self.destination)
        total = manifest['total_bytes'] + len(recovery_set.canonical(manifest))
        with patch.object(recovery_set, 'LIMIT', total - 1), self.assertRaisesRegex(MaintenanceError, 'byte limit'):
            self.capture()
        self.assertFalse(self.destination.exists())
