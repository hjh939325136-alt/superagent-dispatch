"""Install only the fixed lab's inert native source/domain and restart guards.

This does not activate mixed mode, write the ledger, or start/stop a service.
Installing the drop-in closes future HTTP starts until Coordinator exposes its
reviewed operation. Interrupted domain initialization requires reconciliation;
no directory is deleted, reused as empty, or automatically restored.
"""
from contextlib import contextmanager
import argparse
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.m0_native_mixed.domains import ROOT, pin_source_files
from tools.m0_native_mixed.maintenance import MaintenanceError, canonical, digest, _safe, _read, _json, _write, _lock
from tools.m0_native_mixed.native_adapter import (
    NativeLayout, SystemdService, local_entry_bytes, source_files, target_guard_command, _tree,
)


MARKER = '.native-source.json'
LOCAL_DEVICE_ID = 'native-local-synthetic'
PHASES = ('intent', 'source_ready', 'domain_started', 'domain_ready', 'local_ready',
          'guard_prepared', 'restart_guard_installed', 'completed')


def guard_dropin(layout, *, deployment_manifest=None):
    target = ''
    if deployment_manifest is not None:
        target = f'ExecStartPre=+{target_guard_command(layout, deployment_manifest)}\n'
    return (f'[Service]\n{target}'
        f'ExecStartPre=+/usr/bin/python3 -B {layout.source_root}/tools/m0_native_mixed/maintenance.py '
        f'guard-check --native-root {layout.config_root}\n').encode()


class SystemdBootstrap:
    """Production ownership and reload operations; deliberately no start/stop."""
    def __init__(self, layout, *, deployment_manifest=None):
        self.service = SystemdService(layout, deployment_manifest=deployment_manifest)
        self.lab = self.service.lab

    def owner(self, path, role, *, mode=None):
        info = _safe(path).stat()
        if role == 'config':
            self.service.verify_config_file(path)
            if mode is not None and stat.S_IMODE(info.st_mode) != mode:
                raise MaintenanceError('new config metadata differs')
            return
        uid = 0 if role == 'root' else self.lab.account.pw_uid
        if info.st_uid != uid or stat.S_IMODE(info.st_mode) & 0o022:
            raise MaintenanceError('unowned or writable native bootstrap path: ' + str(path))
        if mode is not None and stat.S_IMODE(info.st_mode) != mode:
            raise MaintenanceError('native bootstrap permission mismatch: ' + str(path))

    def own_new(self, path, role, mode):
        _safe(path)
        uid = gid = 0
        if role == 'runtime': uid, gid = self.lab.account.pw_uid, self.lab.account.pw_gid
        if role == 'config': gid = self.lab.account.pw_gid
        os.chown(path, uid, gid)
        path.chmod(mode)
        self.owner(path, role, mode=mode)

    def status(self): return self.lab.status()

    def validate_existing(self, layout):
        if self.service.deployment_manifest is not None:
            self.lab.verify_target()
        current = self.lab.current()
        self.lab.validate(dict(service='sa-dispatch.service', ledger=str(layout.ledger),
            artifacts=str(layout.artifacts), old_release=str(current), release=str(current)))
        self.owner(layout.config_root / 'coordinator.toml', 'config')
        self.owner(layout.credentials, 'config')  # metadata only
        self.owner(layout.ledger, 'runtime', mode=0o600)
        for path in layout.service_files[:-1]: self.owner(path, 'root', mode=0o644)
        # Existing CAS is checked, never recursively chmod/chown'd.
        for path in (layout.artifacts, *layout.artifacts.rglob('*')):
            self.owner(path, 'runtime', mode=0o700 if path.is_dir() else 0o600)
        for name, sha in _tree(layout.artifacts).items():
            parts = name.split('/')
            if len(parts) != 3 or parts[0] != 'sha256' or parts[1] != sha[:2] or parts[2] != sha:
                raise MaintenanceError('legacy CAS has an unknown entry')

    def reload(self):
        subprocess.run(['systemctl', 'daemon-reload'], check=True, capture_output=True, timeout=20)

    def verify_integration(self): self.service.verify_guards()


class Bootstrap:
    def __init__(self, source_root, *, expected_source_sha256, coordinator_before_sha256,
                 layout=None, platform=None, fixture=False, fault=None, deployment_manifest=None):
        self.input = _safe(source_root)
        self.fixture = fixture
        self.fault = fault or (lambda phase: None)
        for value in (expected_source_sha256, coordinator_before_sha256):
            if not isinstance(value, str) or re.fullmatch('[0-9a-f]{64}', value) is None:
                raise MaintenanceError('explicit reviewed source and coordinator hashes required')
        self.source_sha = expected_source_sha256
        self.coordinator_sha = coordinator_before_sha256
        if not fixture:
            if (os.name != 'posix' or os.geteuid() != 0 or ROOT != self.input or
                    self.input.parent != Path('/opt/sa-dispatch-native/incoming') or
                    layout is not None or platform is not None):
                raise MaintenanceError('bootstrap requires the fixed root-owned Debian incoming source')
            layout = NativeLayout(Path('/opt/sa-dispatch-native/releases') / expected_source_sha256)
            platform = SystemdBootstrap(layout, deployment_manifest=deployment_manifest)
        elif layout is None or platform is None:
            raise MaintenanceError('fixture requires explicit bounded layout/platform')
        elif deployment_manifest is not None:
            raise MaintenanceError('deployment manifest requires the fixed production layout')
        self.layout, self.platform = layout, platform
        self.deployment_manifest = deployment_manifest
        self.base = layout.maintenance_lock.parent.parent / 'sa-dispatch-native'
        self.release = self.base / 'releases' / self.source_sha
        if layout.source_root != self.release:
            raise MaintenanceError('frozen release does not match the reviewed source digest')
        self.state_root = layout.domain.parent / 'bootstrap'
        self.record = self.state_root / (self.source_sha + '.json')
        self.local_entry = self.base / 'bin/local-dispatch'
        self.expected_files = source_files(NativeLayout(self.input))
        if digest(canonical(self.expected_files)) != self.source_sha:
            raise MaintenanceError('reviewed source digest differs before any bootstrap writes')
        if fixture:
            # Caller must run this check against its TemporaryDirectory before
            # creating any prerequisite; also enforce all derived write paths.
            roots = [p for p in layout.config_root.parents if p.name == 'etc']
            if len(roots) != 1: raise MaintenanceError('fixture root cannot be identified')
            self.fixture_root = roots[0].parent.resolve()
            layout.assert_fixture_contained(self.fixture_root)
            for path in (self.base, self.release, self.state_root, self.record, self.local_entry):
                if not path.resolve().is_relative_to(self.fixture_root):
                    raise MaintenanceError('derived bootstrap write path escapes fixture')

    def _parents(self, path):
        for parent in reversed(_safe(path).parents):
            if parent.exists(): self.platform.owner(parent, 'root')

    def _mkdir(self, path, *, mode=0o755):
        path = _safe(path)
        self._parents(path)
        if path.exists():
            if not path.is_dir(): raise MaintenanceError('bootstrap directory occupied by file')
            self.platform.owner(path, 'root')
            return
        path.mkdir()
        self.platform.own_new(path, 'root', mode)

    def _save(self, phase, **values):
        self.state.update(values, phase=phase)
        _write(self.record, canonical(self.state))
        self.platform.own_new(self.record, 'root', 0o600)
        self.fault('state:' + phase)

    def _publish(self, path, raw, *, role='root', mode=0o644):
        path = _safe(path)
        self._parents(path)
        if path.exists():
            self.platform.owner(path, role, mode=mode)
            if _read(path) != raw: raise MaintenanceError('existing bootstrap file differs: ' + str(path))
            return
        _write(path, raw, exclusive=True)
        self.platform.own_new(path, role, mode)

    def _source(self):
        marker = canonical(dict(schema='native_mixed.frozen_source.v1', source_sha256=self.source_sha,
                                files=self.expected_files))
        self._mkdir(self.base)
        self._mkdir(self.base / 'releases')
        if self.release.exists():
            self.platform.owner(self.release, 'root')
            if not (self.release / MARKER).exists():
                raise MaintenanceError('unowned or interrupted markerless release requires reconciliation')
            self.platform.owner(self.release / MARKER, 'root', mode=0o644)
            if _read(self.release / MARKER) != marker:
                raise MaintenanceError('frozen release ownership marker differs')
        else:
            self._mkdir(self.release)
            self._publish(self.release / MARKER, marker)
        self._verify_frozen(partial=True)
        for name, sha in self.expected_files.items():
            source, target = self.input / name, self.release / name
            self.platform.owner(source, 'root')
            raw = _read(source)
            if digest(raw) != sha: raise MaintenanceError('source changed during freezing')
            for parent in reversed(target.relative_to(self.release).parents):
                if parent != Path('.'): self._mkdir(self.release / parent)
            self._publish(target, raw)
            self.fault('source_file:' + name)
        self._verify_frozen()
        if source_files(NativeLayout(self.input)) != self.expected_files:
            raise MaintenanceError('source set changed during freezing')
        self._save('source_ready')

    def _verify_frozen(self, *, partial=False):
        marker = canonical(dict(schema='native_mixed.frozen_source.v1', source_sha256=self.source_sha,
                                files=self.expected_files))
        allowed = {**self.expected_files, MARKER:digest(marker)}
        observed = _tree(self.release)
        if (any(allowed.get(name) != sha for name,sha in observed.items()) or
                not partial and observed != allowed):
            raise MaintenanceError('frozen release contains foreign, missing or changed bytes')
        folders = {Path('.')}
        for name in allowed: folders.update(Path(name).parents)
        for path in (self.release,*self.release.rglob('*')):
            self.platform.owner(path,'root',mode=0o755 if path.is_dir() else 0o644)
            if path.is_dir() and path.relative_to(self.release) not in folders:
                raise MaintenanceError('unlisted directory in frozen release')

    def _domain_manifest(self, *, strict=True):
        manifest = _json(self.layout.domain / 'stage-manifest.json')
        if (manifest.get('state') != 'staged' or manifest.get('root') != str(self.layout.domain) or
                manifest.get('source_root') != str(self.release)):
            raise MaintenanceError('partial/foreign domain requires explicit reconciliation')
        # Bindings must be created at their final paths by the frozen source.
        from knowledge.m0_content.workflow_binding import load_workflow_binding
        binding = load_workflow_binding(self.layout.domain / 'workflow-binding.json')
        if (manifest['catalog_instance'] != binding.research.instance or
                manifest['content_instance'] != binding.content_instance):
            raise MaintenanceError('domain identities changed')
        if manifest['source_files'] != pin_source_files(self.release):
            raise MaintenanceError('staged domain source pin differs')
        if strict:
            expected = {**manifest['files'], 'stage-manifest.json':digest(_read(self.layout.domain / 'stage-manifest.json'))}
            if (_tree(self.layout.domain) != expected or
                    {p.relative_to(self.layout.domain).as_posix() for p in self.layout.domain.rglob('*')
                     if p.is_dir()} != {'research','journal'}):
                raise MaintenanceError('staged domain contains foreign or changed data')
        return manifest

    def _stage_domain(self):
        domain = self.layout.domain
        self._mkdir(domain.parent)
        if domain.exists():
            # A populated domain is accepted only after this bootstrap durably
            # recorded the start intent. Incomplete stage manifests never retry.
            if self.state['phase'] not in PHASES[2:]:
                raise MaintenanceError('existing domain is not owned by this bootstrap')
            manifest = self._domain_manifest()
        else:
            self._save('domain_started')
            script = ('from pathlib import Path; import sys; '
                'from tools.m0_native_mixed.domains import stage_domains; '
                'stage_domains(Path(sys.argv[1]),ledger=Path(sys.argv[2]),artifacts=Path(sys.argv[3]),'
                'source_root=Path(sys.argv[4]),executable=Path(sys.executable).resolve(),'
                'device_id=sys.argv[5])')
            executable = sys.executable if self.fixture else '/usr/bin/python3'
            subprocess.run([executable, '-B', '-c', script, str(domain), str(self.layout.ledger),
                str(self.layout.artifacts), str(self.release), LOCAL_DEVICE_ID], cwd=self.release,
                env={**os.environ, 'PYTHONPATH':str(self.release)}, check=True,
                capture_output=True, timeout=60)
            self.fault('domain_staged')
            manifest = self._domain_manifest()
        # stage_domains owns this complete new tree; never touch legacy CAS.
        for path in (domain, *domain.rglob('*')):
            _safe(path)
            self.platform.own_new(path, 'runtime', 0o700 if path.is_dir() else 0o600)
        self._save('domain_ready', domain_manifest_sha256=digest(canonical(manifest)),
                   domain_instances={key:manifest[key] for key in ('catalog_instance','content_instance')})

    def _local(self):
        self._mkdir(self.base / 'bin')
        self._publish(self.local_entry, local_entry_bytes(self.layout,
            deployment_manifest=self.deployment_manifest), mode=0o755)
        # An existing local config is never replaced even when its bytes happen
        # to match. Only a journaled bootstrap placeholder can be reused.
        local = self.layout.config_root / 'local.toml'
        if local.exists() and not self.state.get('local_placeholder_intent'):
            raise MaintenanceError('existing local configuration has no bootstrap ownership record')
        if not local.exists(): self._save('domain_ready', local_placeholder_intent=True)
        self._publish(local, b'', role='config', mode=0o640)
        self._save('local_ready')

    def _guards(self):
        self._mkdir(self.layout.service_files[-1].parent)
        intended = dict(self.state['intent']['service_files_before'])
        if any(digest(_read(Path(path))) != sha for path,sha in intended.items()):
            raise MaintenanceError('existing systemd integration changed during bootstrap')
        dropin = guard_dropin(self.layout, deployment_manifest=self.deployment_manifest)
        entry = local_entry_bytes(self.layout, deployment_manifest=self.deployment_manifest)
        intended[str(self.layout.service_files[-1])] = digest(dropin)
        receipt = canonical(dict(schema='native_mixed.guard.v1', source_sha256=self.source_sha,
            files=intended, local_entry=str(self.local_entry),
            local_entry_sha256=digest(entry)))
        # Record exact intended bytes before publishing any restart-affecting
        # file; each repeated publish verifies both ownership and bytes.
        self._save('guard_prepared', guard_receipt_sha256=digest(receipt),
                   restart_policy='closed_until_reviewed_coordinator_exposing')
        self._publish(self.layout.guard_manifest, receipt, mode=0o600)
        self._publish(self.layout.service_files[-1], dropin)
        self.fault('dropin_installed')
        self._save('restart_guard_installed', http_restart_now_guarded=True)
        self.platform.reload()
        self.platform.verify_integration()

    def run(self):
        l = self.layout
        self._parents(l.config_root / 'coordinator.toml')
        self._parents(self.input / MARKER)
        self.platform.owner(l.maintenance_lock,'root')
        for name in self.expected_files: self.platform.owner(self.input / name,'root')
        if digest(_read(l.config_root / 'coordinator.toml')) != self.coordinator_sha:
            raise MaintenanceError('existing coordinator differs from the caller-approved before hash')
        self.platform.validate_existing(l)
        with _lock(l.maintenance_lock):
            before_status = self.platform.status()
            # Validate identity again inside the shared maintenance exclusion.
            if digest(_read(l.config_root / 'coordinator.toml')) != self.coordinator_sha:
                raise MaintenanceError('coordinator changed while acquiring maintenance lock')
            self._mkdir(l.domain.parent)
            self._mkdir(self.state_root, mode=0o700)
            intent = dict(schema='native_mixed.bootstrap.v1', source_sha256=self.source_sha,
                coordinator_before_sha256=self.coordinator_sha, source=str(self.input), release=str(self.release),
                config_root=str(l.config_root), domain=str(l.domain),
                service_files_before={str(p):digest(_read(p)) for p in l.service_files[:-1]})
            if self.deployment_manifest is not None:
                intent['deployment_manifest'] = self.deployment_manifest
            if self.record.exists():
                self.platform.owner(self.record, 'root', mode=0o600)
                self.state = _json(self.record)
                if self.state.get('intent') != intent or self.state.get('phase') not in PHASES:
                    raise MaintenanceError('bootstrap journal differs from reviewed fixed targets')
            else:
                if any(p.exists() for p in (self.local_entry,l.guard_manifest,l.service_files[-1])):
                    raise MaintenanceError('existing guard integration lacks this bootstrap journal')
                self.state = dict(intent=intent)
                self._save('intent', original_service=before_status)
            phase = self.state['phase']
            if phase == 'completed':
                self._verify_frozen()
                manifest = self._domain_manifest(strict=False)
                if self.state['domain_instances'] != {key:manifest[key] for key in self.state['domain_instances']}:
                    raise MaintenanceError('completed domain identity changed')
                self.platform.verify_integration()
                return self.state
            if phase == 'intent': self._source()
            else:
                self._verify_frozen()
            if self.state['phase'] in ('source_ready','domain_started'): self._stage_domain()
            if self.state['phase'] == 'domain_ready': self._local()
            if self.state['phase'] in ('local_ready','guard_prepared','restart_guard_installed'): self._guards()
            after_status = self.platform.status()
            if after_status != before_status:
                raise MaintenanceError('service changed during bootstrap; no automatic restart attempted')
            if digest(_read(l.config_root / 'coordinator.toml')) != self.coordinator_sha:
                raise MaintenanceError('coordinator bytes changed during bootstrap')
            self._save('completed')
            return self.state


def main(argv=None):
    parser = argparse.ArgumentParser(description='Fixed Debian native mixed bootstrap; never starts/stops services')
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--source-sha256', required=True)
    parser.add_argument('--coordinator-before-sha256', required=True)
    parser.add_argument('--deployment-manifest')
    args = parser.parse_args(argv)
    try:
        result = Bootstrap(args.source_root, expected_source_sha256=args.source_sha256,
            coordinator_before_sha256=args.coordinator_before_sha256,
            deployment_manifest=args.deployment_manifest).run()
        print(json.dumps({'ok':True, 'phase':result['phase'], 'http_restart_now_guarded':True,
                          'started_or_stopped_services':False}))
        return 0
    except Exception as exc:
        print(json.dumps({'ok':False, 'error':type(exc).__name__, 'preserved_for_reconciliation':True}))
        return 1


if __name__ == '__main__': raise SystemExit(main())
