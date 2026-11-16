"""Temporary fixed-layout bootstrap, actual frozen-source domain subprocess.

No systemd command, WSL mutation or host-root fixture is used by these tests.
"""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from dispatch.sa_dispatch.artifacts import ArtifactStore
from dispatch.sa_dispatch.store_sqlite import SqliteStore
from tools.m0_native_mixed.bootstrap import Bootstrap, guard_dropin, LOCAL_DEVICE_ID, MARKER
from tools.m0_native_mixed.domains import ROOT
from tools.m0_native_mixed.maintenance import MaintenanceError, canonical, digest, guard_check
from tools.m0_native_mixed.native_adapter import NativeLayout, local_entry_bytes, source_files, _tree


class ProcessLoss(BaseException): pass


class FakeSystemdBootstrap:
    def __init__(self, layout):
        self.layout = layout
        self.metadata = {}
        self.bad_owners = set()
        self.owned_new = []
        self.events = []
        self.current = {'ActiveState':'active', 'MainPID':'4242', 'NRestarts':'0'}

    def owner(self, path, role, *, mode=None):
        path = Path(path)
        if path in self.bad_owners: raise MaintenanceError('unowned fixture path')
        if path in self.metadata:
            actual_role, actual_mode = self.metadata[path]
            if actual_role != role or mode is not None and actual_mode != mode:
                raise MaintenanceError('fixture ownership/mode differs')

    def own_new(self, path, role, mode):
        self.owner(path, role)
        self.metadata[Path(path)] = (role, mode)
        self.owned_new.append(Path(path))

    def status(self): return dict(self.current)

    def validate_existing(self, layout):
        self.owner(layout.config_root / 'coordinator.toml', 'config')
        self.owner(layout.credentials, 'config')
        for path in (layout.artifacts, *layout.artifacts.rglob('*')):
            self.owner(path, 'runtime', mode=0o700 if path.is_dir() else 0o600)

    def reload(self): self.events.append('daemon-reload')

    def verify_integration(self):
        receipt = json.loads(self.layout.guard_manifest.read_bytes())
        for name, sha in receipt['files'].items():
            if digest(Path(name).read_bytes()) != sha: raise MaintenanceError('integration hash differs')
        entry = Path(receipt['local_entry'])
        if entry.read_bytes() != local_entry_bytes(self.layout): raise MaintenanceError('local guard differs')
        if self.layout.service_files[-1].read_bytes() != guard_dropin(self.layout):
            raise MaintenanceError('HTTP guard differs')


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix='native-bootstrap-')
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.source_sha = digest(canonical(source_files(NativeLayout(ROOT))))
        seed = NativeLayout.fixture(self.root, ROOT)
        release = seed.maintenance_lock.parent.parent / 'sa-dispatch-native/releases' / self.source_sha
        self.layout = replace(seed, source_root=release)
        # Before ANY mkdir/write, validate every direct and derived write target.
        self.layout.assert_fixture_contained(self.root)
        self.assertTrue(release.resolve().is_relative_to(self.root))
        l = self.layout
        for path in (l.config_root, l.ledger.parent, l.maintenance_lock.parent, l.domain.parent,
                     l.artifacts, *(p.parent for p in l.service_files)):
            self.assertTrue(path.resolve().is_relative_to(self.root))
            path.mkdir(parents=True, exist_ok=True)
        l.maintenance_lock.write_bytes(b'0')
        SqliteStore(l.ledger, lease_ttl_seconds=30)
        self.cas_ref = ArtifactStore(l.artifacts).put(b'preexisting reviewed legacy CAS')
        self.cas_before = {str(p):(p.read_bytes(),p.stat().st_mtime_ns) for p in l.artifacts.rglob('*') if p.is_file()}
        http = dict(db_path=str(l.ledger),artifact_root=str(l.artifacts),journal_dir=str(l.http_journal),
            device_id='coordinator',capabilities=[],plugins=['hotspot'],lease_ttl_seconds=30,
            component_timeout_seconds=20,synthetic=True)
        self.http_raw = ('\n'.join(f'{k} = {json.dumps(v)}' for k,v in http.items()) + '\n[components]\n').encode()
        (l.config_root / 'coordinator.toml').write_bytes(self.http_raw)
        l.credentials.write_bytes(b'credential-fixture-must-not-be-read')
        l.service_files[0].write_bytes(b'[Service]\nExecStartPre=/existing/preflight\nUser=sa-dispatch\n')
        l.service_files[1].write_bytes(b'[Service]\nEnvironment="PYTHONPATH=/opt/sa-dispatch/current"\n')
        self.platform = FakeSystemdBootstrap(l)
        self.platform.metadata[l.config_root / 'coordinator.toml'] = ('config',0o640)
        self.platform.metadata[l.credentials] = ('config',0o640)
        for path in (l.artifacts,*l.artifacts.rglob('*')):
            self.platform.metadata[path] = ('runtime',0o700 if path.is_dir() else 0o600)
        self.bootstrap = self.make_bootstrap()

    def make_bootstrap(self, **kwargs):
        return Bootstrap(ROOT, expected_source_sha256=self.source_sha,
            coordinator_before_sha256=digest(self.http_raw), layout=self.layout,
            platform=self.platform, fixture=True, **kwargs)

    def test_complete_bootstrap_keeps_live_service_and_config_bytes_and_legacy_cas(self):
        before = self.platform.status()
        result = self.bootstrap.run()
        self.assertEqual(result['phase'],'completed')
        self.assertEqual(self.platform.status(),before)
        self.assertEqual(self.platform.events,['daemon-reload'])
        self.assertEqual((self.layout.config_root / 'coordinator.toml').read_bytes(),self.http_raw)
        self.assertEqual({str(p):(p.read_bytes(),p.stat().st_mtime_ns) for p in self.layout.artifacts.rglob('*')
                          if p.is_file()},self.cas_before)
        self.assertFalse(any(p.is_relative_to(self.layout.artifacts) for p in self.platform.owned_new))
        self.assertEqual((self.layout.config_root / 'local.toml').read_bytes(),b'')
        self.assertEqual(self.platform.metadata[self.layout.config_root / 'local.toml'],('config',0o640))
        stage = json.loads((self.layout.domain / 'stage-manifest.json').read_bytes())
        self.assertEqual(stage['source_root'],str(self.layout.source_root))
        self.assertEqual(stage['root'],str(self.layout.domain))
        self.assertEqual(stage['state'],'staged')
        local = (self.layout.domain / 'local.toml').read_text(encoding='utf-8')
        self.assertEqual(tomllib.loads(local)['device_id'], LOCAL_DEVICE_ID)
        self.assertEqual(source_files(self.layout),source_files(NativeLayout(ROOT)))
        self.assertNotIn(b'ExecStartPre=\n',self.layout.service_files[-1].read_bytes())
        with self.assertRaises(MaintenanceError): guard_check(self.layout.config_root)
        record_before = self.bootstrap.record.read_bytes()
        self.assertEqual(self.make_bootstrap().run(),result)
        self.assertEqual(self.bootstrap.record.read_bytes(),record_before)
        self.assertEqual(self.platform.events,['daemon-reload'])

    def test_wrong_approved_config_hash_refuses_before_bootstrap_writes(self):
        (self.layout.config_root / 'coordinator.toml').write_bytes(b'foreign changed config')
        with self.assertRaises(MaintenanceError): self.bootstrap.run()
        self.assertFalse(self.bootstrap.record.exists())
        self.assertFalse(self.layout.domain.exists())
        self.assertEqual(self.platform.events,[])

    def test_unowned_parent_refuses_even_when_existing_bytes_match(self):
        self.platform.bad_owners.add(self.layout.config_root)
        with self.assertRaises(MaintenanceError): self.bootstrap.run()
        self.assertFalse(self.bootstrap.record.exists())
        self.assertFalse(self.layout.source_root.exists())

    def test_partial_source_resumes_only_matching_owned_bytes(self):
        def fail(label):
            if label.startswith('source_file:'): raise ProcessLoss()
        self.bootstrap.fault = fail
        with self.assertRaises(ProcessLoss): self.bootstrap.run()
        self.assertEqual(json.loads(self.bootstrap.record.read_bytes())['phase'],'intent')
        self.assertTrue((self.layout.source_root / MARKER).is_file())
        self.assertEqual(self.make_bootstrap().run()['phase'],'completed')

    def test_partial_domain_is_preserved_and_requires_reconciliation(self):
        def fail(label):
            if label == 'state:domain_started':
                self.layout.domain.mkdir()
                (self.layout.domain / 'stage-manifest.json').write_bytes(b'{"state":"incomplete"}')
                (self.layout.domain / 'partial.sqlite3').write_bytes(b'preserve partial bytes')
                raise ProcessLoss()
        self.bootstrap.fault = fail
        with self.assertRaises(ProcessLoss): self.bootstrap.run()
        before = _tree(self.layout.domain)
        with self.assertRaises(MaintenanceError): self.make_bootstrap().run()
        self.assertEqual(_tree(self.layout.domain),before)
        self.assertFalse(self.layout.service_files[-1].exists())
        self.assertEqual(self.platform.events,[])

    def test_complete_domain_before_journal_update_resumes_without_restaging(self):
        def fail(label):
            if label == 'domain_staged': raise ProcessLoss()
        self.bootstrap.fault = fail
        with patch('tools.m0_native_mixed.bootstrap.LOCAL_DEVICE_ID', 'existing-synthetic-worker'):
            with self.assertRaises(ProcessLoss): self.bootstrap.run()
        marker = (self.layout.domain / 'stage-manifest.json').read_bytes()
        local = self.layout.domain / 'local.toml'
        recorded = local.read_bytes()
        self.assertEqual(self.make_bootstrap().run()['phase'],'completed')
        self.assertEqual((self.layout.domain / 'stage-manifest.json').read_bytes(),marker)
        self.assertEqual(local.read_bytes(), recorded)
        self.assertEqual(tomllib.loads(recorded.decode('utf-8'))['device_id'], 'existing-synthetic-worker')

    def test_dropin_commit_gap_resumes_guarded_without_service_restart(self):
        def fail(label):
            if label == 'dropin_installed': raise ProcessLoss()
        self.bootstrap.fault = fail
        with self.assertRaises(ProcessLoss): self.bootstrap.run()
        self.assertTrue(self.layout.service_files[-1].exists())
        self.assertEqual(self.platform.events,[])
        with self.assertRaises(MaintenanceError): guard_check(self.layout.config_root)
        self.assertEqual(self.make_bootstrap().run()['phase'],'completed')
        self.assertEqual(self.platform.events,['daemon-reload'])
        self.assertEqual(self.platform.status()['MainPID'],'4242')

    def test_foreign_existing_local_config_is_not_overwritten(self):
        path = self.layout.config_root / 'local.toml'
        path.write_bytes(b'user-owned-existing-config')
        with self.assertRaises(MaintenanceError): self.bootstrap.run()
        self.assertEqual(path.read_bytes(),b'user-owned-existing-config')
        self.assertFalse(self.layout.service_files[-1].exists())
        self.assertEqual(self.platform.events,[])


if __name__ == '__main__': unittest.main()
