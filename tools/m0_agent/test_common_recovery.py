"""Private archives and quarantined restores with native fixture domains."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from dispatch.sa_dispatch.artifacts import ArtifactStore
from dispatch.sa_dispatch.canonical import canonical_json
from dispatch.sa_dispatch.external_migration import upgrade, verify_schema
from dispatch.sa_dispatch.routing import activate_mixed, activation_manifest
from dispatch.sa_dispatch.store_sqlite import SqliteStore
from tools.m0_agent import common_recovery as recovery
from tools.m0_agent.deepseek_client import CONNECT_PROXY_HOST, CONNECT_PROXY_PORT
from tools.m0_native_mixed import domains
from tools.m0_native_mixed.maintenance import MaintenanceError


class CommonRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory())).resolve()
        data = self.root / 'var/lib/sa-dispatch'
        config = self.root / 'etc/sa-dispatch'
        self.layout = recovery.RecoveryLayout(
            config_root=config, ledger=data / 'dispatch.sqlite3', artifacts=data / 'artifacts',
            domain=data.with_name('sa-dispatch-native') / 'domain', http_journal=data / 'journal',
            legacy_bindings=data.with_name('sa-dispatch-manager'))
        self.archive = self.root / 'archive'
        self.quarantine = self.root / 'quarantine'
        config.mkdir(parents=True)
        self.layout.domain.parent.mkdir(parents=True)
        self.store = SqliteStore(self.layout.ledger, lease_ttl_seconds=30)
        self.cas = ArtifactStore(self.layout.artifacts)
        self.cas.put(b'private fixture result body')
        with closing(self.store._connect()) as conn:
            proof = self.cas.put(canonical_json(activation_manifest(conn, actor='tester', bindings={})))
        activate_mixed(self.layout.ledger, actor='tester', evidence_ref=proof, bindings={}, artifacts=self.cas)
        domains.stage_domains(self.layout.domain, ledger=self.layout.ledger, artifacts=self.layout.artifacts,
                              source_root=domains.ROOT, executable=Path(sys.executable).resolve(),
                              device_id='local-recovery-fixture')
        (config / 'coordinator.toml').write_text(f'db_path = {json.dumps(str(self.layout.ledger))}\n', encoding='utf-8')
        self.credential = config / 'identities.json'
        self.credential.write_bytes(b'PRIVATE FIXTURE TOKEN FILE')
        self.grant = config / recovery.GRANT_FILE
        self.grant.write_bytes(b'{"execution_mode":"live","max_tokens":100}')
        self.proxy = config / recovery.NETWORK_FILE
        self.proxy.write_bytes(canonical_json({'schema': 'superagent.provider-network.v1',
                                              'https_connect_proxy': {'host': CONNECT_PROXY_HOST, 'port': CONNECT_PROXY_PORT}}))
        self.guard = config / '.mixed-maintenance/guard.json'
        self.guard.parent.mkdir()
        self.guard.write_bytes(b'{"operation_id":"fixture"}')
        self.layout.http_journal.mkdir()
        self.journal = self.layout.http_journal / ('l_' + 'a' * 32 + '.jsonl')
        self.journal.write_bytes(b'{"metadata":"private journal evidence"}')

    def capture(self):
        return recovery.capture(self.layout, self.archive, offline=True)

    def test_private_capture_preserves_used_unknown_slot_and_restores_original_bytes(self):
        upgrade(self.layout.ledger, self.root / 'upgrade-recovery', offline=True)
        with closing(sqlite3.connect(self.layout.ledger)) as conn, conn:
            conn.execute('''INSERT INTO tasks(task_id,idem_key,fingerprint,kind,kind_version,params_json,
                input_refs_json,required_caps_json,scope_json,side_effect,access,egress,status,fence,attempts,
                max_attempts,created_at,updated_at,execution_route) VALUES('task','slot','fp','agent.run',1,
                '{}','[]','["agent.deepseek"]','{}','external','exportable','derived','unknown',1,1,1,?,?,
                'agent_external')''', ('2026-11-01', '2026-11-01'))
            conn.execute("INSERT INTO leases VALUES ('lease','task',1,'device','a','b','unknown',NULL)")
            conn.execute("INSERT INTO control_budgets VALUES ('budget','slot','requests',1,1,0,'a','b')")
            conn.execute("INSERT INTO control_reservations VALUES ('task',1,'budget','fp',1,NULL,'uncertain',NULL,'a',NULL,'b')")
        original = self.layout.ledger.read_bytes()
        read = recovery._read

        def without_credentials(path, **kwargs):
            self.assertNotEqual(path, self.credential)
            return read(path, **kwargs)

        with patch.object(recovery, '_read', side_effect=without_credentials):
            receipt = self.capture()
        self.assertEqual(receipt['ledger_version'], '0013')
        self.assertEqual(self.capture(), receipt)
        self.assertFalse((self.archive / 'config/identities.json').exists())
        self.assertEqual((self.archive / 'config' / recovery.GRANT_FILE).read_bytes(), self.grant.read_bytes())
        self.assertEqual((self.archive / recovery.NATIVE_GUARD).read_bytes(), self.guard.read_bytes())
        self.assertEqual((self.archive / 'config' / recovery.NETWORK_FILE).read_bytes(), self.proxy.read_bytes())
        self.assertEqual((self.archive / 'http-journal' / self.journal.name).read_bytes(), self.journal.read_bytes())
        self.assertEqual(recovery.verify(self.archive, expected_manifest_sha256=receipt['manifest_sha256']), receipt)
        self.quarantine.mkdir()
        restored = recovery.restore_to_quarantine(self.archive, self.quarantine,
                                                 expected_manifest_sha256=receipt['manifest_sha256'])
        self.assertTrue(restored['restore_verified'])
        marker = json.loads((self.quarantine / recovery.QUARANTINE_MARKER).read_bytes())
        for flag in ('credential_injection', 'timer_enabled', 'external_execution_enabled', 'services_started'):
            self.assertIs(marker[flag], False)
        ledger = self.quarantine / recovery.LEDGER_ALIAS
        self.assertEqual(ledger.read_bytes(), original)
        with closing(sqlite3.connect(ledger)) as conn:
            self.assertEqual(verify_schema(conn), '0013')
            self.assertEqual(conn.execute('SELECT status,fence,attempts FROM tasks').fetchone(), ('unknown', 1, 1))
            self.assertEqual(conn.execute('SELECT state,reserved_units FROM control_reservations').fetchone(), ('uncertain', 1))
        self.assertEqual(self.layout.ledger.read_bytes(), original)

    def test_refuses_sidecars_unknown_files_inline_credentials_and_unconfirmed_cold_capture(self):
        with self.assertRaisesRegex(MaintenanceError, 'explicitly confirmed'):
            recovery.capture(self.layout, self.archive)
        sidecar = self.layout.ledger.with_suffix('.sqlite3-wal')
        unknown = self.layout.config_root / 'unrelated-config.json'
        for path, message in ((sidecar, 'sidecar'), (unknown, 'unknown file')):
            path.write_bytes(b'fixture')
            with self.subTest(path=path), self.assertRaisesRegex(MaintenanceError, message):
                self.capture()
            path.unlink()
        self.grant.write_bytes(b'{"api_key":"fixture unarchivable"}')
        with self.assertRaisesRegex(MaintenanceError, 'credential-bearing field'):
            self.capture()
        self.assertFalse(self.archive.exists())

    def test_refuses_tampered_archive_or_nonempty_restore(self):
        receipt = self.capture()
        self.assertEqual(receipt['ledger_version'], '0012')
        self.quarantine.mkdir()
        (self.quarantine / 'existing').write_bytes(b'existing')
        with self.assertRaisesRegex(MaintenanceError, 'new or empty'):
            recovery.restore_to_quarantine(self.archive, self.quarantine)
        with self.assertRaisesRegex(MaintenanceError, 'hash differs from its receipt'):
            recovery.verify(self.archive, expected_manifest_sha256='0' * 64)
        path = self.archive / 'config' / recovery.GRANT_FILE
        path.write_bytes(b'{"execution_mode":"fixture tamper"}')
        with self.assertRaisesRegex(MaintenanceError, 'recovery file hash differs'):
            recovery.verify(self.archive)


if __name__ == '__main__':
    unittest.main()
