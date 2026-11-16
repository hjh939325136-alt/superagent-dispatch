"""Targeted install gate and Collector adoption tests with a temporary SQLite ledger."""

from contextlib import closing, nullcontext
from dataclasses import asdict
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dispatch.sa_dispatch.artifacts import ArtifactStore
from dispatch.sa_dispatch.canonical import canonical_json
from dispatch.sa_dispatch.collector import Collector, SourceBinding, file_identity
from dispatch.sa_dispatch.external_migration import upgrade
from dispatch.sa_dispatch.routing import activate_mixed, activation_manifest
from dispatch.sa_dispatch.store_sqlite import SqliteStore

from . import common_install as addon


class AddonInstallTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.manifest = 'a' * 64
        self.machine = 'b' * 32

    def test_source_manifest_rejects_changed_and_extra_files(self):
        raw = addon._json({'schema': addon.SOURCE_SCHEMA,
                           'files': {addon.ENTRY: sha256(b'reviewed').hexdigest()}})
        identity = sha256(raw).hexdigest()
        release = self.root / identity
        entry = release / addon.ENTRY
        entry.parent.mkdir(parents=True)
        entry.write_bytes(b'reviewed')
        (release / addon.SOURCE_MANIFEST).write_bytes(raw)
        with patch.object(addon, '_root_metadata'):
            addon._source_files(release, identity)
            entry.write_bytes(b'changed')
            with self.assertRaisesRegex(addon.InstallError, 'unlisted or changed'):
                addon._source_files(release, identity)
            entry.write_bytes(b'reviewed')
            (release / 'extra.py').write_bytes(b'foreign')
            with self.assertRaisesRegex(addon.InstallError, 'unlisted or changed'):
                addon._source_files(release, identity)

    def test_source_manifest_refuses_path_traversal(self):
        raw = addon._json({'schema': addon.SOURCE_SCHEMA,
                           'files': {addon.ENTRY: 'a' * 64, '../other.py': 'a' * 64}})
        identity = sha256(raw).hexdigest()
        release = self.root / identity
        release.mkdir()
        (release / addon.SOURCE_MANIFEST).write_bytes(raw)
        with patch.object(addon, '_root_metadata'), self.assertRaisesRegex(addon.InstallError, 'invalid release path'):
            addon._source_files(release, identity)

    def test_units_use_same_ledger_and_isolate_credential(self):
        release = self.root / self.manifest
        units = {path.name: raw.decode() for path, (raw, _) in addon._unit_bytes(
            release, self.machine, self.manifest).items()}
        http = units[addon.DROPIN.name]
        agent = units[addon.AGENT]
        timer = units[addon.TIMER]
        local = units[addon.LOCAL_ENTRY.name]
        for service in (http, agent):
            self.assertIn(f'flock --shared --nonblock --no-fork {addon.LEDGER.parent}', service)
            self.assertIn(f'guard --expected-machine {self.machine} --manifest-sha256 {self.manifest}', service)
        self.assertIn('ExecStartPre=\nExecStartPre=+', http)
        self.assertIn(f'serve --identities {addon.CONFIG / "identities.json"} --port 8876', http)
        self.assertIn('LoadCredential=provider_api_key\n', agent)
        self.assertIn('User=sa-agent-external\n', agent)
        self.assertNotIn('LoadCredential', http)
        self.assertNotIn('LoadCredential', local)
        self.assertIn('OnUnitInactiveSec=30s', timer)
        self.assertIn('local-run ', local)

    def test_pending_or_interrupted_guard_never_enables_timer(self):
        install = addon._Installer(self.root / self.manifest, self.machine, self.manifest)
        with patch.object(install, '_read_state', return_value={'phase': 'prepared'}), \
             patch.object(addon, '_systemctl') as command, \
             patch('tools.m0_native_mixed.maintenance._lock', return_value=nullcontext()):
            with self.assertRaisesRegex(addon.InstallError, 'guard is closed'):
                install.enable_timer()
            command.assert_not_called()
        with patch.object(install, '_read_state', return_value={'phase': 'installing'}):
            install.state_file = self.root / 'state.json'
            install.state_file.touch()
            with patch.object(addon, '_root_metadata'), \
                 patch('tools.m0_native_mixed.maintenance._lock', return_value=nullcontext()):
                with self.assertRaisesRegex(addon.InstallError, 'interrupted upgrade'):
                    install.install()

    def test_transient_local_component_has_private_writable_temp(self):
        install = addon._Installer(self.root / self.manifest, self.machine, self.manifest)
        with (patch.object(install, 'guard'),
              patch('tools.m0_native_mixed.native_adapter.directory_lock', return_value=nullcontext()),
              patch.object(addon.subprocess, 'run', return_value=SimpleNamespace(returncode=0)) as launched):
            self.assertEqual(install.local_run(['run-once']), 0)
        argv = launched.call_args.args[0]
        self.assertIn('--property=ProtectSystem=strict', argv)
        self.assertIn('--property=PrivateTmp=yes', argv)
        self.assertIn('--unit=' + addon.LOCAL, argv)

    def test_ready_guard_requires_unchanged_agent_configuration(self):
        install = addon._Installer(self.root / self.manifest, self.machine, self.manifest)
        state = {'phase': 'ready', 'pre_live_recovery': {'restore_verified': True},
                 'agent_config_hashes': {'agent-external.json': 'before'}}
        with patch.object(install, '_read_state', return_value=state), \
             patch.object(install, '_verify_installed'), \
             patch.object(addon, '_config_hashes', return_value={'agent-external.json': 'after'}):
            with self.assertRaisesRegex(addon.InstallError, 'configuration changed'):
                install.guard()

    def test_local_retarget_only_replaces_two_reviewed_paths(self):
        old_releases = self.root / 'native' / 'releases'
        old = old_releases / ('a' * 64)
        new = self.root / 'new' / 'releases' / ('b' * 64)
        unique = {
            'research': ('knowledge/m0_search/dispatch_component.py', '--source-binding', 'research-binding.json'),
            'research_content': ('knowledge/m0_content/dispatch_component.py', '--workflow-binding', 'workflow-binding.json'),
        }
        lines = [f'db_path = {json.dumps(str(addon.LEDGER))}',
                 f'artifact_root = {json.dumps(str(addon.CAS))}', 'execution_route = "local_synthetic"',
                 'synthetic = true', 'unknown_field = "preserve"']
        for name, (script, flag, binding) in unique.items():
            argv = ['/usr/bin/python3', '-B', str(old / script), flag, str(addon.DOMAIN / binding)]
            lines += [f'[components.{name}]', 'platform = "any"', 'argv = ' + json.dumps(argv)]
        original = '\n'.join(lines).encode()
        expected = '\n'.join(lines).encode()
        for script, _, _ in unique.values():
            expected = expected.replace(json.dumps(str(old / script)).encode(), json.dumps(str(new / script)).encode())
        with (patch.object(addon, 'OLD_RELEASES', old_releases), patch.object(addon, '_root_metadata'),
              patch.object(addon, '_trusted_local_python', return_value={'/usr/bin/python3', '/usr/bin/python3.13'})):
            self.assertEqual(addon._retarget_local(original, new), expected)
            self.assertEqual(addon._retarget_local(original.replace(b'/usr/bin/python3', b'/usr/bin/python3.13'), new),
                             expected.replace(b'/usr/bin/python3', b'/usr/bin/python3.13'))
            bad = original.replace(b'--source-binding', b'--unbounded')
            with self.assertRaisesRegex(addon.InstallError, 'invocation or domain binding changed'):
                addon._retarget_local(bad, new)
            with self.assertRaisesRegex(addon.InstallError, 'invocation or domain binding changed'):
                addon._retarget_local(original.replace(b'/usr/bin/python3', b'/usr/bin/python3.99'), new)

    def test_adopt_collector_and_manager_preserve_source_and_anchors(self):
        ledger = self.root / 'dispatch.sqlite3'
        config = self.root / 'config'
        config.mkdir()
        cas = ArtifactStore(self.root / 'artifacts')
        store = SqliteStore(ledger, lease_ttl_seconds=30)
        with closing(store._connect()) as conn:
            ref = cas.put(canonical_json(activation_manifest(conn, actor='fixture', bindings={})))
        activate_mixed(ledger, actor='fixture', evidence_ref=ref, bindings={}, artifacts=cas)
        old = SourceBinding.enroll('dispatch-fixture', 'dispatch', ledger, 'same-instance', ('0012',))
        path = config / 'collector-binding.json'
        source = asdict(old)
        del source['profile']
        path.write_bytes(addon._json({'schema': 'sa_dispatch.collector_binding.v1',
                                     'target': str(ledger), 'sources': [source]}))
        manager = {
            'source_id': old.source_id, 'source_instance': old.source_instance, 'kind': 'dispatch',
            'locator': str(ledger), 'accepted_schema_versions': ['0012'], 'class': 'internal',
            'options': {'identity': {'device': ledger.stat().st_dev, 'inode': ledger.stat().st_ino}}, 'freshness_s': 86400,
        }
        manager_path = config / 'manager-binding.json'
        manager_path.write_bytes(addon._json({'version': 1, 'sources': [manager]}))
        for stream in addon.STREAMS:
            self.assertEqual(Collector(ledger).sync(old, stream)['state'], 'synced')
        self.assertEqual(upgrade(ledger, self.root / 'recovery', offline=True)['schema_version'], '0013')
        with (patch.object(addon, 'LEDGER', ledger), patch.object(addon, 'CONFIG', config),
              patch.object(addon, '_root_metadata', side_effect=lambda path, **_: path.stat()),
              patch.object(addon, '_atomic', side_effect=lambda path, raw, **_: path.write_bytes(raw))):
            adoption = addon._adopt_collector()
            addon._retarget_manager(adoption)
        self.assertEqual(adoption['source_instance'], old.source_instance)
        self.assertEqual(file_identity(ledger), old.file_identity)
        self.assertEqual(set(adoption['streams']), set(addon.STREAMS))
        self.assertEqual({state['epoch'] for state in adoption['streams'].values()}, {2})
        new = json.loads(path.read_bytes())['sources'][0]
        self.assertEqual(new['profile'], 'common_metadata_0013')
        current_manager = json.loads(manager_path.read_bytes())['sources'][0]
        self.assertEqual(current_manager['options']['identity'], manager['options']['identity'])
        self.assertEqual(current_manager['options']['profile'], 'common_metadata_0013')
        self.assertEqual(current_manager['accepted_schema_versions'], ['0013'])
        with closing(sqlite3.connect(ledger)) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM collector_gaps WHERE kind='schema_changed' "
                                          "AND resolved_at IS NOT NULL").fetchone()[0], 3)


if __name__ == '__main__':
    unittest.main()
