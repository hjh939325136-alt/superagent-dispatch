from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from tools.m0_installer import build_common, common


MANIFEST = 'a' * 64
ZIP_SHA = 'b' * 64
MACHINE = 'c' * 32
HOSTNAME = 'isolated-common-fixture'
PLAN = 'd' * 64
BRIDGE = 'tools/m0_native_mixed/common_entry.py'
GUARD = 'tools/m0_native_mixed/maintenance.py'
NATIVE = {BRIDGE: b'# fixture bridge\n', GUARD: b'# fixture guard\n'}
PAYLOAD = {**NATIVE, 'dispatch/sa_dispatch/__init__.py': b'# runtime fixture\n',
           'dispatch/sa_dispatch_hotspot/__init__.py': b'# plugin fixture\n',
           'tools/m0_installer/common_baseline.py': b'# baseline fixture\n'}


class Interrupted(RuntimeError):
    pass


class FakeHost:
    def __init__(self, paths):
        self.paths = paths
        self.owners = {}
        self.events = []
        self.account_record = None
        self.bridge_phase = None
        self.is_enabled = False
        self.fail_handoff = False
        self.lose_bootstrap_response = False
        self.lose_run_response = False
        self.complete_without_mixed_mode = False
        self.runtime_healthy = True

    def observe(self):
        return MACHINE, HOSTNAME

    def inode(self, path):
        info = path.lstat()
        return info.st_dev, info.st_ino

    def metadata(self, path):
        default = common.ROOT_DIR if path.is_dir() else common.ROOT_FILE
        return self.owners.get(self.inode(path), default)

    def own(self, path, metadata):
        self.owners[self.inode(path)] = metadata
        if path == self.paths.runtime and metadata[0] != 0:
            self.events.append('handoff_started')
            if self.fail_handoff:
                self.fail_handoff = False
                raise Interrupted()

    def verify_idle(self, *, fresh):
        self.events.append('fresh_preflight' if fresh else 'idle')

    def verify_offline(self, incoming, manifest):
        self.events.append('offline')

    def account(self):
        return self.account_record

    def create_account(self, operation):
        self.events.append('account')
        self.account_record = {'uid': 501, 'gid': 502, 'intent': operation}

    def reload(self):
        self.events.append('reload')

    def enabled(self):
        return self.is_enabled

    def enable(self):
        if self.bridge_phase != 'completed' or self.runtime_healthy is not True:
            raise AssertionError('enabled without native completion and current health')
        self.events.append('enable')
        self.is_enabled = True

    def source_files(self, incoming):
        return {name: common._sha((incoming / name).read_bytes()) for name in NATIVE}

    def baseline(self, incoming, manifest):
        p = self.paths
        self.events.append('baseline')
        unit = p.unit.read_text()
        if self.metadata(p.runtime) != common.PRIVATE_DIR or 'guard-check --native-root' not in unit:
            raise AssertionError('baseline requires protected runtime root and guarded service bytes')
        if '--schedules' in unit or '--port 8876' not in unit:
            raise AssertionError('HTTP unit has incorrect service flags')
        p.ledger.write_bytes(b'reserved fixture inode')
        self.own(p.ledger, common.PRIVATE_FILE)
        dev, ino = self.inode(p.ledger)
        receipt = {'schema': 'native_mixed.fresh_install.v1', 'phase': 'completed', 'deployment_manifest': manifest,
                   'ledger': str(p.ledger), 'ledger_identity': {'device': dev, 'inode': ino}}
        raw = common._json(receipt)
        p.fresh_receipt.write_bytes(raw)
        self.own(p.fresh_receipt, common.PRIVATE_FILE)
        return common._sha(raw)

    def bootstrap(self, incoming, manifest, source, coordinator):
        self.events.append('bootstrap')
        if self.metadata(self.paths.ledger) != (501, 502, 0o600):
            raise AssertionError('Bootstrap requires the journaled runtime permissions')
        release = self.paths.native / 'releases' / source
        if not release.exists():
            for name, expected in NATIVE.items():
                path = release / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(expected)
            marker = {'schema': 'native_mixed.frozen_source.v1', 'source_sha256': source, 'files': self.source_files(incoming)}
            (release / '.native-source.json').write_bytes(common._json(marker))
        if self.lose_bootstrap_response:
            self.lose_bootstrap_response = False
            raise Interrupted()
        return {'ok': True, 'phase': 'completed'}

    def bridge(self, release, action, manifest, proof, coordinator, operation, plan):
        self.events.append('bridge:' + action)
        if action == 'plan':
            self.bridge_phase = 'staged'
        elif action == 'run':
            if self.bridge_phase != 'staged':
                raise AssertionError('must not run a completed bridge twice')
            self.bridge_phase = 'completed'
            if self.lose_run_response:
                self.lose_run_response = False
                raise Interrupted()
        mode = 'mixed' if self.bridge_phase == 'completed' and not self.complete_without_mixed_mode else 'isolated'
        result = {'ok': True, 'phase': self.bridge_phase, 'plan_sha256': PLAN, 'ledger_mode': mode}
        if action == 'status' and self.bridge_phase == 'completed' and self.runtime_healthy is not None:
            result['runtime_healthy'] = self.runtime_healthy
        return result


class FixtureInstaller(common._Installer):
    def _current(self, *, create):
        # The Windows test account has no symlink creation privilege. Mock the
        # authorized selector only, while checking real package trees and writes.
        self._file(self.paths.http / 'current.fixture', self.http_release.name.encode(), create=create)


class InstallSequenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.paths = common._Paths(self.root)
        for parent in (self.paths.authority.parent, self.paths.unit.parent, self.paths.http.parent):
            parent.mkdir(parents=True, exist_ok=True)
        self.host = FakeHost(self.paths)

    def installer(self, *, machine=MACHINE, manifest=MANIFEST, zip_hash=ZIP_SHA):
        return FixtureInstaller(self.paths, self.host, PAYLOAD, manifest=manifest, zip_hash=zip_hash,
                                machine_id=machine, hostname=HOSTNAME)

    def phase(self):
        return common._decode(self.paths.state.read_bytes())['phase']

    def test_guard_before_baseline_and_readonly_after_completion(self):
        result = self.installer().run(install=True)
        events = self.host.events
        self.assertLess(events.index('reload'), events.index('baseline'))
        self.assertLess(events.index('baseline'), events.index('handoff_started'))
        self.assertLess(events.index('handoff_started'), events.index('bootstrap'))
        self.assertLess(events.index('bootstrap'), events.index('bridge:plan'))
        self.assertLess(events.index('bridge:run'), events.index('enable'))
        self.assertEqual(result['phase'], 'completed')
        self.assertIs(result['runtime_healthy'], True)
        self.assertEqual(result['operation_id'], 'common-' + MANIFEST[:24])
        self.assertEqual(self.host.metadata(self.paths.tokens), common.PRIVATE_FILE)
        tokens = common._decode(self.paths.tokens.read_bytes())['tokens']
        identities = self.paths.identities.read_bytes()
        for token in tokens.values():
            self.assertNotIn(token.encode(), identities)
            self.assertNotIn(token.encode(), common._json(result))
        self.assertNotIn(MACHINE.encode(), common._json(result))
        files = tuple(path for path in self.root.rglob('*') if path.is_file())
        before = [(path.read_bytes(), path.stat().st_mtime_ns) for path in files]
        self.host.events.clear()
        self.assertEqual(self.installer().run(install=False), result)
        self.assertEqual(self.installer().run(install=True), result)
        self.assertEqual(before, [(path.read_bytes(), path.stat().st_mtime_ns) for path in files])
        self.assertEqual(self.host.events, ['bridge:status', 'bridge:status'])

    def test_target_mismatch_and_missing_status_do_not_write(self):
        with self.assertRaisesRegex(common.InstallError, 'does not match operator selection'):
            self.installer(machine='e' * 32).run(install=True)
        with self.assertRaisesRegex(common.InstallError, 'directory is missing'):
            self.installer().run(install=False)
        self.assertFalse(self.paths.authority.exists())
        self.assertFalse(self.paths.etc.exists())

    def test_foreign_root_and_changed_package_are_refused(self):
        self.paths.runtime.mkdir()
        foreign = self.paths.runtime / 'foreign'
        foreign.write_bytes(b'foreign fixture')
        with self.assertRaisesRegex(common.InstallError, 'unrecognized preexisting'):
            self.installer().run(install=True)
        self.assertEqual(foreign.read_bytes(), b'foreign fixture')
        self.assertFalse(self.paths.state.exists())
        foreign.unlink()
        self.paths.runtime.rmdir()
        self.installer().run(install=True)
        with self.assertRaisesRegex(common.InstallError, 'different package or target'):
            self.installer(zip_hash='f' * 64).run(install=True)

    def test_resume_after_partial_handoff_keeps_baseline_and_finishes(self):
        self.host.fail_handoff = True
        with self.assertRaises(Interrupted):
            self.installer().run(install=True)
        self.assertEqual(self.phase(), 'baseline')
        self.assertEqual(self.host.metadata(self.paths.runtime), (501, 502, 0o700))
        self.assertEqual(self.host.metadata(self.paths.ledger), common.PRIVATE_FILE)
        old_inode = self.host.inode(self.paths.ledger)
        self.assertEqual(self.installer().run(install=True)['phase'], 'completed')
        self.assertEqual(self.host.inode(self.paths.ledger), old_inode)
        self.assertEqual(self.host.events.count('baseline'), 1)

    def test_resume_after_native_run_response_loss_never_runs_again(self):
        self.host.lose_run_response = True
        with self.assertRaises(Interrupted):
            self.installer().run(install=True)
        self.assertEqual(self.phase(), 'plan')
        self.assertFalse(self.host.is_enabled)
        self.assertEqual(self.installer().run(install=True)['phase'], 'completed')
        self.assertEqual(self.host.events.count('bridge:run'), 1)

    def test_mutated_frozen_release_denied_before_bridge_execution(self):
        self.host.lose_bootstrap_response = True
        with self.assertRaises(Interrupted):
            self.installer().run(install=True)
        state = common._decode(self.paths.state.read_bytes())
        marker = self.paths.native / 'releases' / state['source_sha256'] / GUARD
        marker.write_bytes(b'foreign guard fixture')
        with self.assertRaisesRegex(common.InstallError, 'unexpected installation file bytes|size limit'):
            self.installer().run(install=True)
        self.assertNotIn('bridge:plan', self.host.events)
        self.assertFalse(self.host.is_enabled)

    def test_completed_native_phase_with_isolated_ledger_does_not_enable(self):
        self.host.complete_without_mixed_mode = True
        with self.assertRaisesRegex(common.InstallError, 'native mixed maintenance or current Dispatch health is unverified'):
            self.installer().run(install=True)
        self.assertEqual(self.phase(), 'plan')
        self.assertFalse(self.host.is_enabled)

    def test_completed_native_phase_requires_current_health_before_enable(self):
        for health in (False, None):
            with self.subTest(runtime_healthy=health):
                self.host.runtime_healthy = health
                with self.assertRaisesRegex(common.InstallError, 'current Dispatch health is unverified'):
                    self.installer().run(install=True)
                self.assertEqual(self.phase(), 'plan')
                self.assertFalse(self.host.is_enabled)
        self.assertEqual(self.host.events.count('bridge:run'), 1)
        self.assertNotIn('enable', self.host.events)

    def test_stopped_completed_installation_refuses_readonly_status_and_install_rerun(self):
        self.installer().run(install=True)
        before = self.paths.state.read_bytes(), self.paths.state.stat().st_mtime_ns
        for health in (False, None):
            for install in (True, False):
                with self.subTest(runtime_healthy=health, install=install):
                    self.host.runtime_healthy = health
                    self.host.events.clear()
                    with self.assertRaisesRegex(common.InstallError, 'completed installation lacks verified current Dispatch health'):
                        self.installer().run(install=install)
                    self.assertEqual(self.host.events, ['bridge:status'])
                    self.assertEqual((self.paths.state.read_bytes(), self.paths.state.stat().st_mtime_ns), before)


class LaunchTests(unittest.TestCase):
    def test_package_launcher_and_bootstrap_dependencies_must_match_exact_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            workspace = Path(__file__).resolve().parents[2]
            required = build_common._required_paths(True)
            for name in (common.ENTRY, common.VERIFIER, common.TARGET_MODULE):
                required.add('payload/' + name)
            payload = {name: b'# fixture\n' for name in required}
            payload['payload/LICENSE'] = b'MIT License\n'
            for name in (common.ENTRY, common.VERIFIER, common.TARGET_MODULE):
                raw = (workspace / name).read_bytes()
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(raw)
                payload['payload/' + name] = raw
            manifest = build_common._json(build_common._manifest(payload, True))
            digest = common._sha(manifest)
            raw = build_common._zip_bytes(payload, manifest)
            path = root / ('superagent-common-' + digest[:12] + '.zip')
            path.write_bytes(raw)
            with patch.object(common, '_protected_source'):
                verified = common._verified_payload(path, digest, common._sha(raw), root)
                self.assertEqual(verified[common.ENTRY], payload['payload/' + common.ENTRY])
                (root / common.TARGET_MODULE).write_bytes(b'# foreign launch source\n')
                with self.assertRaisesRegex(common.InstallError, 'launch source differs'):
                    common._verified_payload(path, digest, common._sha(raw), root)

    def test_host_enables_only_fixed_service_without_start_or_timer(self):
        process = subprocess.CompletedProcess([], 0, stdout=b'', stderr=b'')
        with patch.object(common.subprocess, 'run', return_value=process) as run:
            common._Host(lambda: (MACHINE, HOSTNAME)).enable()
        self.assertEqual(run.call_args.args[0], ('/usr/bin/systemctl', 'enable', 'sa-dispatch.service'))

    def test_failed_native_status_names_the_actual_service_check(self):
        host = common._Host(lambda: (MACHINE, HOSTNAME))
        with patch.object(host, '_python', side_effect=common.InstallError('native health failed')) as run:
            with self.assertRaisesRegex(common.InstallError, 'inspect the current Dispatch service and guard'):
                host.bridge(Path('fixture'), 'status', MANIFEST, MANIFEST, MANIFEST, 'common-fixture', PLAN)
        self.assertEqual(run.call_args.args[2], 'status')


if __name__ == '__main__':
    unittest.main()
