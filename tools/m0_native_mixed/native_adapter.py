"""Fixed Debian lab bridge for the forward-only mixed coordinator.

No discovery, restoration, credential loading or guard installation is performed.
Production callers use ``run`` so the existing deployment lock spans every phase.
The explicit fixture constructor is for temporary trees and injected services only.
"""
from contextlib import contextmanager, closing
from dataclasses import dataclass, replace
import copy
import json
import os
from pathlib import Path
import re
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import tomllib

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dispatch.sa_dispatch.artifacts import ArtifactStore
from dispatch.sa_dispatch.bundle import ledger, _settled
from dispatch.sa_dispatch.canonical import canonical_json
from dispatch.sa_dispatch.clock import Clock, FakeClock, timestamp
from dispatch.sa_dispatch.collector import Collector, SourceBinding, digest as collector_digest, enrich, source_info
from dispatch.sa_dispatch.collector_cli import load_binding as collector_binding
from dispatch.sa_dispatch.config import load_config
from dispatch.sa_dispatch.routing import activate_mixed, activation_manifest, binding_evidence, row_digest
from knowledge.m0_content.workflow_binding import load_workflow_binding
from tools.m0_manager.binding import load_binding as manager_binding
from tools.m0_deploy.install_target import InstallTargetError, validate_manifest_sha256
from tools.m0_native_mixed.domains import pin_source_files, ROOT
from tools.m0_native_mixed.fresh_install import (
    FRESH_MODE, WORKER_SOURCE_NAME, read_fresh_proof, verify_fresh_baseline, verify_fresh_streams,
)
from tools.m0_native_mixed.maintenance import MaintenanceError, canonical, digest, _safe, _read, _json, _write, _load, _lock, guard_check

STREAMS = ['events', 'control_events', 'schedule_events']
CONFIGS = {'coordinator.toml', 'local.toml', 'manager-binding.json', 'collector-binding.json'}


@dataclass(frozen=True)
class NativeLayout:
    source_root: Path
    config_root: Path = Path('/etc/sa-dispatch')
    ledger: Path = Path('/var/lib/sa-dispatch/dispatch.sqlite3')
    artifacts: Path = Path('/var/lib/sa-dispatch/artifacts')
    domain: Path = Path('/var/lib/sa-dispatch-native/domain')
    legacy_bindings: Path = Path('/var/lib/sa-dispatch-manager')
    maintenance_lock: Path = Path('/opt/sa-dispatch/.maintenance.lock')
    http_journal: Path = Path('/var/lib/sa-dispatch/journal')
    service_files: tuple = (Path('/etc/systemd/system/sa-dispatch.service'),
                           Path('/etc/systemd/system/sa-dispatch.service.d/10-m0-lab-port.conf'),
                           Path('/etc/systemd/system/sa-dispatch.service.d/native-mixed-guard.conf'))

    @property
    def credentials(self): return self.config_root / 'identities.json'
    @property
    def guard_manifest(self): return self.config_root / 'native-guard.json'
    @property
    def workers(self): return self.config_root / 'http-workers.json'

    @classmethod
    def fixture(cls, root, source_root):
        root = _safe(root)
        if not root.is_dir() or root == Path(root.anchor):
            raise MaintenanceError('existing bounded fixture directory required')
        base = cls(Path(source_root))
        result = replace(base, **{name: root / str(getattr(base, name)).lstrip('/\\')
            for name in ('config_root', 'ledger', 'artifacts', 'domain', 'legacy_bindings',
                         'maintenance_lock', 'http_journal')},
            service_files=tuple(root / str(p).lstrip('/\\') for p in base.service_files))
        result.assert_fixture_contained(root)
        return result

    def assert_fixture_contained(self, root):
        root = _safe(root).resolve()
        paths = [getattr(self, key) for key in ('config_root', 'ledger', 'artifacts', 'domain',
                 'legacy_bindings', 'maintenance_lock', 'http_journal')]
        paths.extend(self.service_files)
        paths.extend((self.credentials, self.guard_manifest, self.workers))
        if any(not p.resolve().is_relative_to(root) or p.resolve() == root for p in paths):
            raise MaintenanceError('fixture write path escapes temporary root')


def _tree(root):
    root = _safe(root)
    if not root.is_dir(): raise MaintenanceError('required owned tree missing')
    result = {}
    for path in sorted(root.rglob('*')):
        _safe(path)
        if path.is_file(): result[path.relative_to(root).as_posix()] = digest(_read(path))
    return result


def _identity(path):
    info = _safe(path).stat()
    return {'device': info.st_dev, 'inode': info.st_ino}


def source_files(layout):
    result = pin_source_files(layout.source_root)
    # Domain stager's pin set predates the native adapter itself.
    for tree in ('tools/m0_native_mixed', 'tools/m0_deploy'):
        for path in sorted((layout.source_root / tree).rglob('*.py')):
            result[path.relative_to(layout.source_root).as_posix()] = digest(_read(path))
    return result


@contextmanager
def directory_lock(directory, *, shared=False):
    """The same inode flock used by HTTP; not a substitute for stopping writers."""
    if os.name == 'nt':
        # Fixtures rely on their injected service; production never reaches this.
        yield
        return
    import fcntl
    fd = os.open(_safe(directory), os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(fd, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        yield
    finally: os.close(fd)


def _systemd_properties(output, scalar_keys):
    scalars, prestart = {}, []
    for line in output.splitlines():
        name, separator, value = line.partition('=')
        if not separator or not value.strip():
            raise MaintenanceError('malformed effective systemd property')
        if name == 'ExecStartPre':
            prestart.append(value)
        elif name in scalar_keys and name not in scalars:
            scalars[name] = value
        else:
            raise MaintenanceError('unexpected or duplicate effective systemd property')
    if set(scalars) != scalar_keys or not prestart:
        raise MaintenanceError('missing effective systemd property')
    return scalars, prestart


_SYSTEMD_COMMAND = re.compile(
    r'\{ path=([^\s;{}]+) ; argv\[\]=([^;{}]+?) ; ignore_errors=(yes|no)(?: ; [a-z_]+=[^;{}]+?)* \}'
)


def _systemd_commands(values):
    commands = []
    for value in values:
        remainder = value.strip()
        while remainder:
            match = _SYSTEMD_COMMAND.match(remainder)
            if match is None:
                raise MaintenanceError('malformed effective systemd command')
            executable, arguments, ignore = match.groups()
            if arguments.split()[:1] != [executable]:
                raise MaintenanceError('effective systemd command executable differs')
            commands.append((arguments, ignore == 'no'))
            remainder = remainder[match.end():].strip()
    return commands


def _verify_effective_systemd(layout, properties, formal_guard, target_guard):
    sandbox = {'User': 'sa-dispatch', 'Group': 'sa-dispatch', 'NoNewPrivileges': 'yes',
               'ProtectSystem': 'strict', 'UMask': '0077'}
    lines, prestart = _systemd_properties(properties, set(sandbox) | {'ExecStart'})
    effective = _systemd_commands(prestart)
    start = _systemd_commands([lines.pop('ExecStart')])
    flock = f'/usr/bin/flock --shared --nonblock --no-fork {layout.ledger.parent} '
    if ([required for command, required in effective if command == formal_guard] != [True] or
            len(start) != 1 or lines != sandbox or not start[0][1] or not start[0][0].startswith(flock)):
        raise MaintenanceError('effective systemd guard or sandbox differs')
    if target_guard is not None and [required for command, required in effective if command == target_guard] != [True]:
        raise MaintenanceError('effective systemd target guard differs')


class SystemdService:
    """Actual service implementation for the fixed lab or bound installation."""
    def __init__(self, layout, *, deployment_manifest=None):
        from tools.m0_deploy.lab import InstalledAdapter, LabAdapter
        self.lab = LabAdapter() if deployment_manifest is None else InstalledAdapter(deployment_manifest)
        self.layout = layout
        self.deployment_manifest = deployment_manifest

    def stop(self):
        # The singleton local entry uses this dedicated transient unit. Never
        # infer ownership from a process name or kill arbitrary Python workers.
        local = subprocess.check_output(['systemctl', 'show', 'sa-dispatch-local.service',
            '--property=LoadState', '--value'], text=True).strip()
        if local != 'not-found':
            subprocess.run(['systemctl', 'stop', 'sa-dispatch-local.service'], check=True,
                           capture_output=True, timeout=20)
        self.lab.stop()
    def start(self): self.lab.start()
    def health(self): return self.lab.health()

    def verify_stopped(self):
        state = self.lab.status()
        if state['MainPID'] != '0' or state['ActiveState'] not in ('inactive', 'failed'):
            raise MaintenanceError('HTTP writer still running')
        with socket.socket() as probe:
            if probe.connect_ex(('127.0.0.1', 8876)) == 0:
                raise MaintenanceError('HTTP port still occupied')
        protected = (self.layout.ledger.parent, self.layout.domain)
        for proc in Path('/proc').iterdir():
            if not proc.name.isdigit() or int(proc.name) == os.getpid(): continue
            try:
                status = (proc / 'status').read_text()
                uid = int(next(line for line in status.splitlines() if line.startswith('Uid:')).split()[1])
                if uid == self.lab.account.pw_uid:
                    raise MaintenanceError('untracked service-account process remains')
                for fd in (proc / 'fd').iterdir():
                    target = Path(os.readlink(fd))
                    if not any(target.is_relative_to(root) for root in protected): continue
                    flags = next(line.split()[1] for line in (proc / 'fdinfo' / fd.name).read_text().splitlines()
                                 if line.startswith('flags:'))
                    if int(flags, 8) & os.O_ACCMODE != os.O_RDONLY:
                        raise MaintenanceError('untracked writable descriptor remains')
            except (FileNotFoundError, ProcessLookupError): continue

    def verify_guards(self):
        if self.deployment_manifest is not None:
            self.lab.verify_target()
        layout = self.layout
        receipt = _json(layout.guard_manifest)
        expected = {'schema', 'source_sha256', 'files', 'local_entry', 'local_entry_sha256'}
        if set(receipt) != expected or receipt['schema'] != 'native_mixed.guard.v1':
            raise MaintenanceError('preinstalled guard integration receipt required')
        if receipt['source_sha256'] != digest(canonical(source_files(layout))):
            raise MaintenanceError('guard implementation source drift')
        if any((p.stat().st_uid != 0 or stat.S_IMODE(p.stat().st_mode) & 0o022)
               for p in (layout.guard_manifest, layout.source_root)):
            raise MaintenanceError('guard receipt/source ownership is unsafe')
        for name, sha in source_files(layout).items():
            path = layout.source_root / name
            if path.stat().st_uid != 0 or stat.S_IMODE(path.stat().st_mode) & 0o022:
                raise MaintenanceError('frozen executable source is writable by runtime')
            if name.startswith('dispatch/') and digest(_read(self.lab.current() / name.removeprefix('dispatch/'))) != sha:
                raise MaintenanceError('HTTP runtime differs from frozen component source')
        if set(receipt['files']) != {str(p) for p in layout.service_files}:
            raise MaintenanceError('guard service integration set differs')
        local = Path('/opt/sa-dispatch-native/bin/local-dispatch')
        if (receipt['local_entry'] != str(local) or
                _read(local) != local_entry_bytes(layout, deployment_manifest=self.deployment_manifest)):
            raise MaintenanceError('local writer guard entry differs')
        checks = {**receipt['files'], str(local): receipt['local_entry_sha256']}
        for name, sha in checks.items():
            path = _safe(name); info = path.stat()
            if info.st_uid != 0 or stat.S_IMODE(info.st_mode) & 0o022 or digest(_read(path)) != sha:
                raise MaintenanceError('guard entry permission or hash drift')
        guard = f'{layout.source_root}/tools/m0_native_mixed/maintenance.py'
        properties = subprocess.check_output(['systemctl', 'show', 'sa-dispatch.service',
            '--property=ExecStart,ExecStartPre,User,Group,NoNewPrivileges,ProtectSystem,UMask'], text=True)
        formal_guard = f'/usr/bin/python3 -B {guard} guard-check --native-root {layout.config_root}'
        target_guard = (target_guard_command(layout, self.deployment_manifest)
                        if self.deployment_manifest is not None else None)
        _verify_effective_systemd(layout, properties, formal_guard, target_guard)
        # Root executes guard-check (ExecStartPre=+...) because coordinator records are 0600.
        dropin = _read(layout.service_files[-1]).decode()
        required = f'ExecStartPre=+{formal_guard}'
        if required not in dropin.splitlines(): raise MaintenanceError('privileged fail-closed guard missing')
        if target_guard is not None and f'ExecStartPre=+{target_guard}' not in dropin.splitlines():
            raise MaintenanceError('privileged fail-closed target guard missing')

    def verify_permissions(self):
        for name in ('coordinator.toml', 'local.toml'):
            self.verify_config_file(self.layout.config_root / name)
        for root in (self.layout.domain, self.layout.artifacts):
            for path in (root, *root.rglob('*')):
                info = _safe(path).stat()
                if info.st_uid != self.lab.account.pw_uid or stat.S_IMODE(info.st_mode) & 0o077:
                    raise MaintenanceError('runtime data ownership/permissions differ')

    def verify_config_file(self, path):
        info = _safe(path).stat()
        mode = stat.S_IMODE(info.st_mode)
        root_managed = info.st_uid == 0 and info.st_gid == self.lab.account.pw_gid and mode == 0o640
        runtime_private = info.st_uid == self.lab.account.pw_uid and mode == 0o600
        if not (root_managed or runtime_private):
            raise MaintenanceError('config requires root:service 0640 or legacy runtime-owned 0600')

    def own_activation_artifacts(self, paths):
        for path in paths:
            for item in (path, path.parent, path.parent.parent):
                if not item.is_relative_to(self.layout.artifacts):
                    raise MaintenanceError('activation artifact outside fixed CAS')
                _safe(item)
                os.chown(item, self.lab.account.pw_uid, self.lab.account.pw_gid)
                item.chmod(0o700 if item.is_dir() else 0o600)


def target_guard_command(layout, deployment_manifest):
    manifest = validate_manifest_sha256(deployment_manifest)
    return (f'/usr/bin/python3 -B {layout.source_root}/tools/m0_native_mixed/native_adapter.py '
            f'verify-target --source-root {layout.source_root} --deployment-manifest {manifest}')


def local_entry_bytes(layout, *, deployment_manifest=None):
    target = ''
    if deployment_manifest is not None:
        target = f' --deployment-manifest {validate_manifest_sha256(deployment_manifest)}'
    return (f'#!/bin/sh\nexec /usr/bin/python3 -B {layout.source_root}/tools/m0_native_mixed/native_adapter.py '
            f'local-run --source-root {layout.source_root}{target} -- "$@"\n').encode()


class NativeAdapter:
    def __init__(self, layout, *, service=None, fixture=False, late_review=None,
                 late_review_sha256=None, deployment_manifest=None, fresh_install_sha256=None):
        self.layout = layout
        if fresh_install_sha256 is not None:
            validate_manifest_sha256(fresh_install_sha256)
            if not fixture and deployment_manifest is None:
                raise MaintenanceError('fresh installation requires the explicit deployment manifest')
        if not fixture:
            if (os.name != 'posix' or os.geteuid() != 0 or
                    layout != NativeLayout(layout.source_root) or
                    layout.source_root.parent != Path('/opt/sa-dispatch-native/releases') or
                    ROOT != layout.source_root):
                raise MaintenanceError('fixed root-owned Debian layout required')
            if service is not None: raise MaintenanceError('production service cannot be injected')
            service = SystemdService(layout, deployment_manifest=deployment_manifest)
        elif service is None:
            raise MaintenanceError('fixture requires explicit service')
        elif deployment_manifest is not None:
            raise MaintenanceError('deployment manifest requires the fixed production layout')
        elif fresh_install_sha256 is not None:
            root = layout.config_root.parent.parent
            if not root.is_absolute() or root == Path(root.anchor) or layout.config_root != root / 'etc/sa-dispatch':
                raise MaintenanceError('fresh fixture requires a bounded temporary layout')
            layout.assert_fixture_contained(root)
        self.service, self.fixture = service, fixture
        self.deployment_manifest = deployment_manifest
        self.fresh_install_sha256 = fresh_install_sha256
        self.cas = ArtifactStore(layout.artifacts)
        self._state_lock = None
        self._operation_lock = False
        self._candidate_hashes = None
        # Explicit operator admission; frozen inside the reviewed plan, never
        # inferred merely because an audit row is terminal or already present.
        self.late_review = copy.deepcopy(late_review)
        self.late_review_sha256 = late_review_sha256

    def rows(self): return ledger(self.layout.ledger, include_devices=True)

    def stage(self, coordinator, configs):
        """Parse the complete candidate set before freezing an immutable plan."""
        if set(configs) != CONFIGS or coordinator.adapter is not self:
            raise MaintenanceError('fixed candidate configuration set required')
        with _lock(self.layout.maintenance_lock):
            anchor = self.freeze()
            candidate = dict(anchor=anchor, configs=[dict(path=k, after_sha256=digest(v))
                                                    for k,v in configs.items()])
            with tempfile.TemporaryDirectory(prefix='.native-candidate-', dir=self.layout.config_root) as folder:
                root = Path(folder)
                for name, raw in configs.items(): _write(root / name, raw, exclusive=True)
                self.validate_configs(candidate, root=root, permissions=False)
            self._candidate_hashes = {item['path']:item['after_sha256'] for item in candidate['configs']}
            try: return coordinator.stage(configs, STREAMS)
            finally: self._candidate_hashes = None

    def run(self, coordinator, plan_hash):
        if coordinator.adapter is not self or coordinator.root != self.layout.config_root:
            raise MaintenanceError('coordinator/layout mismatch')
        with _lock(self.layout.maintenance_lock):
            if _json(coordinator.operation / 'state.json').get('phase') == 'completed':
                _, frozen, _ = _load(self.layout.config_root, coordinator.operation_id, plan_hash)
                if self._uses_fresh(frozen):
                    self._verify_fresh_evidence(frozen)
            self._operation_lock = True
            try: return coordinator.run(plan_hash)
            finally:
                self._release_state_lock()
                self._operation_lock = False

    def _release_state_lock(self):
        if self._state_lock:
            self._state_lock.__exit__(None, None, None)
            self._state_lock = None

    def _domain(self):
        manifest = _json(self.layout.domain / 'stage-manifest.json')
        binding = load_workflow_binding(self.layout.domain / 'workflow-binding.json')
        if (manifest['state'] != 'staged' or manifest['catalog_instance'] != binding.research.instance or
                manifest['content_instance'] != binding.content_instance or
                manifest['source_files'] != pin_source_files(self.layout.source_root)):
            raise MaintenanceError('domain instance or frozen source drift')
        return _tree(self.layout.domain)

    def _old_binding(self):
        target, sources, _ = collector_binding(self.layout.legacy_bindings / 'collector-binding.json')
        selected = [b for b in sources.values() if b.kind == 'dispatch' and Path(b.locator) == self.layout.ledger]
        if (target != self.layout.ledger or len(selected) != 1 or
                (self.fresh_install_sha256 is not None and len(sources) != 1)):
            raise MaintenanceError('exact original dispatch collector source required')
        return selected[0]

    def _uses_fresh(self, plan):
        anchor = plan['anchor']
        if self.fresh_install_sha256 is None:
            if 'collector_mode' in anchor or 'fresh_install_sha256' in anchor or 'fresh_files' in anchor:
                raise MaintenanceError('explicit fresh installation proof required for this plan')
            return False
        if (anchor.get('collector_mode') != FRESH_MODE or
                anchor.get('fresh_install_sha256') != self.fresh_install_sha256):
            raise MaintenanceError('frozen Collector mode or fresh installation hash differs')
        return True

    def _fresh_proof(self, binding, identity):
        return read_fresh_proof(self.layout, self.fresh_install_sha256, self.deployment_manifest,
                                binding, identity, fixture=self.fixture)

    def _verify_fresh_evidence(self, plan):
        if not self.fixture:
            self.service.lab.verify_target()
        anchor = plan['anchor']
        proof, hashes = self._fresh_proof(self._old_binding(), _identity(self.layout.ledger))
        if hashes != anchor.get('fresh_files') or digest(canonical(anchor['rows'])) != proof['baseline_rows_sha256']:
            raise MaintenanceError('frozen fresh installation evidence drift')

    def _fresh_continuation(self, plan, rows=None):
        anchor = plan['anchor']
        observed = verify_fresh_streams(self.layout.ledger, SourceBinding(**anchor['old_binding']))
        for name in (*STREAMS, 'unified_events', 'collector_cursors', 'collector_gaps'):
            expected = anchor['rows'][name]
            if observed[name] != expected or (rows is not None and rows[name] != expected):
                raise MaintenanceError('fresh Collector source or cursor drift')

    def freeze(self):
        old = self._old_binding()
        identity = _identity(self.layout.ledger)
        fresh = {}
        if self.fresh_install_sha256 is not None:
            self.service.verify_stopped()
            proof, hashes = self._fresh_proof(old, identity)
            rows = verify_fresh_baseline(self.layout.ledger, old)
            if digest(canonical(rows)) != proof['baseline_rows_sha256']:
                raise MaintenanceError('fresh ledger snapshot differs from the initialization proof')
            fresh = dict(collector_mode=FRESH_MODE, fresh_install_sha256=self.fresh_install_sha256,
                         fresh_files=hashes)
        else:
            rows = self.rows()
        self._admit_late(rows, self.late_review, self.late_review_sha256)
        if (rows['ledger_mode'] != [{'id':1, 'mode':'isolated'}] or
                max(r['version'] for r in rows['schema_migrations']) != '0012'):
            raise MaintenanceError('0012 isolated ledger required')
        cursors = {r['stream']:r for r in rows['collector_cursors'] if r['source_id'] == old.source_id}
        if set(cursors) != set(STREAMS): raise MaintenanceError('exact original stream set required')
        for stream, cursor in cursors.items():
            if (cursor['source_instance'] != old.source_instance or cursor['binding_sha256'] != old.fingerprint() or
                    cursor['schema_version'] != ('0012' if fresh else '0011') or
                    cursor['authority'] != 'local_owned'):
                raise MaintenanceError('old cursor authority differs')
        workers = _json(self.layout.workers)
        if (set(workers) != {'schema','devices'} or workers['schema'] != 'native_mixed.http_workers.v1' or
                not workers['devices'] or len({r['device_id'] for r in workers['devices']}) != len(workers['devices'])):
            raise MaintenanceError('explicit public HTTP worker provenance required')
        devices = {r['device_id']:r for r in rows['devices']}
        for worker in workers['devices']:
            if (set(worker) != {'device_id','row_sha256','source_path','source_sha256'} or
                    worker['device_id'] not in devices or row_digest(devices[worker['device_id']]) != worker['row_sha256']):
                raise MaintenanceError('HTTP worker row/provenance mismatch')
            origin = _safe(worker['source_path'])
            if origin == self.layout.credentials or 'identit' in origin.name.lower():
                raise MaintenanceError('credential sources are forbidden')
            if fresh and (origin != self.layout.config_root / WORKER_SOURCE_NAME or
                          worker['source_sha256'] != fresh['fresh_files'][WORKER_SOURCE_NAME]):
                raise MaintenanceError('fresh worker provenance is outside the fixed initialization record')
            if digest(_read(origin)) != worker['source_sha256']: raise MaintenanceError('worker public source drift')
        return dict(identity=identity, rows=rows, source_files=source_files(self.layout),
                    domain_files=self._domain(), cas_files=_tree(self.layout.artifacts),
                    legacy_files=_tree(self.layout.legacy_bindings), workers=workers,
                    workers_sha256=digest(_read(self.layout.workers)),
                    http_journal=_tree(self.layout.http_journal), old_binding=old.__dict__, cursors=cursors,
                    late_review=self.late_review, late_review_sha256=self.late_review_sha256, **fresh)

    def _admit_late(self, rows, review, approval):
        from tools.m0_deploy.probe import reviewed_late_refs, REVIEW_LIMIT
        if not rows['late_results']:
            if review is not None or approval is not None:
                raise MaintenanceError('late review has no corresponding audit rows')
            return
        if (not isinstance(review, dict) or not isinstance(approval, str) or
                not re.fullmatch('[0-9a-f]{64}', approval) or len(canonical(review)) > REVIEW_LIMIT or
                digest(canonical(review)) != approval):
            raise MaintenanceError('explicit exact late review document and SHA-256 required')
        binding = review.get('binding')
        fields = {'service','ledger','artifacts','old_release','release','release_manifest_sha256'}
        releases = self.layout.maintenance_lock.parent / 'releases'
        if (not isinstance(binding, dict) or set(binding) != fields or
                binding['service'] != 'sa-dispatch.service' or binding['ledger'] != str(self.layout.ledger) or
                binding['artifacts'] != str(self.layout.artifacts) or
                any(Path(binding[key]).parent != releases for key in ('old_release','release')) or
                not isinstance(binding['release_manifest_sha256'], str) or
                not re.fullmatch('[0-9a-f]{64}', binding['release_manifest_sha256'])):
            raise MaintenanceError('late review does not identify the fixed native installation')
        if not self.fixture:
            from tools.m0_deploy.deploy import verify_release, encoded
            current = self.service.lab.current()
            if (Path(binding['release']) != current or
                    digest(encoded(verify_release(current))) != binding['release_manifest_sha256']):
                raise MaintenanceError('late review runtime provenance differs')
        try:
            reviewed_late_refs(rows, self.cas, review)
        except (ValueError, OSError, KeyError, TypeError) as exc:
            raise MaintenanceError('late audit is outside the explicitly reviewed terminal profile') from exc

    def validate_plan(self, plan):
        anchor = plan['anchor']
        self.service.verify_guards()
        if (plan['native_root'] != str(self.layout.config_root) or
                {r['path'] for r in plan['configs']} != CONFIGS or plan['streams'] != STREAMS):
            raise MaintenanceError('fixed native config/stream set required')
        operation = self.layout.config_root / '.mixed-maintenance' / plan['operation_id']
        if not self.fixture and not operation.exists() and self._candidate_hashes != {
                item['path']:item['after_sha256'] for item in plan['configs']}:
            raise MaintenanceError('production plans must use adapter.stage candidate validation')
        if (_identity(self.layout.ledger) != anchor['identity'] or source_files(self.layout) != anchor['source_files'] or
                self._domain() != anchor['domain_files'] or _tree(self.layout.legacy_bindings) != anchor['legacy_files'] or
                digest(_read(self.layout.workers)) != anchor['workers_sha256']):
            raise MaintenanceError('frozen native identity/source/domain drift')
        if self._uses_fresh(plan):
            self._verify_fresh_evidence(plan)

    def stop_writers(self):
        if not self._operation_lock: raise MaintenanceError('whole-operation maintenance lock required')
        self.service.stop()
        self.service.verify_stopped()
        if self._state_lock is None:
            self._state_lock = directory_lock(self.layout.ledger.parent)
            self._state_lock.__enter__()

    def verify_quiescent(self, plan):
        self.service.verify_stopped()
        rows = self.rows(); _settled(rows)
        self._admit_late(rows, plan['anchor'].get('late_review'), plan['anchor'].get('late_review_sha256'))
        if (any(r['state'] in ('active','unknown') for r in rows['leases']) or
                any(r['enabled'] for r in rows['schedules']) or
                any(r['reserved_units'] for r in rows['control_budgets']) or
                any(r['state'] == 'reserved' for r in rows['control_reservations'])):
            raise MaintenanceError('unknown leases, enabled schedules or pending budgets')
        if rows['ledger_mode'][0]['mode'] == 'isolated' and rows != plan['anchor']['rows']:
            raise MaintenanceError('preactivation ledger changed')
        if _tree(self.layout.http_journal) != plan['anchor']['http_journal']:
            raise MaintenanceError('HTTP journal changed')
        observed = _tree(self.layout.artifacts)
        allowed = self._allowed_cas(plan)
        if (any(observed.get(k) != v for k,v in plan['anchor']['cas_files'].items()) or
                any(allowed.get(k) != v for k,v in observed.items())):
            raise MaintenanceError('unplanned CAS mutation')

    def _binding_documents(self, plan):
        result = {}
        for worker in plan['anchor']['workers']['devices']:
            result[worker['device_id']] = binding_evidence('devices', worker['device_id'], 'http_synthetic',
                worker['row_sha256'], 'sha256:' + worker['source_sha256'])
        return result

    def _allowed_cas(self, plan):
        allowed = dict(plan['anchor']['cas_files'])
        bindings = {'devices':{}}
        for identity, document in self._binding_documents(plan).items():
            sha = digest(canonical_json(document)); ref = 'sha256:' + sha
            allowed[self.cas.path(ref).relative_to(self.layout.artifacts).as_posix()] = sha
            bindings['devices'][identity] = dict(execution_route='http_synthetic',
                pre_state_hash=document['pre_state_hash'], evidence_ref=ref)
        manifest = dict(schema='sa_dispatch.mixed_activation.v1', actor=plan['operation_id'],
            ledger_hash='sha256:' + digest(canonical_json(plan['anchor']['rows'])),
            bindings_hash='sha256:' + digest(canonical_json(bindings)))
        sha = digest(canonical_json(manifest))
        allowed[self.cas.path('sha256:' + sha).relative_to(self.layout.artifacts).as_posix()] = sha
        return allowed

    def capture_recovery(self, plan, destination):
        from tools.m0_native_mixed import recovery_set
        self.verify_quiescent(plan)
        return recovery_set.capture(self.layout, plan, destination)

    def verify_recovery(self, plan, destination, receipt):
        from tools.m0_native_mixed import recovery_set
        recovery_set.verify(self.layout, plan, destination, receipt)

    def _proof_path(self, plan):
        return self.layout.config_root / '.mixed-maintenance' / plan['operation_id'] / 'activation-proof.json'

    def prepare_activation(self, plan):
        path = self._proof_path(plan)
        if path.exists():
            proof = _json(path)
            self.activation_evidence(plan, proof['ref'])
            return proof['ref']
        rows = self.rows()
        if rows != plan['anchor']['rows']: raise MaintenanceError('activation snapshot changed')
        bindings = {'devices': {}}
        for worker in plan['anchor']['workers']['devices']:
            row = next(r for r in rows['devices'] if r['device_id'] == worker['device_id'])
            proof = binding_evidence('devices', row['device_id'], 'http_synthetic', row_digest(row),
                                     'sha256:' + worker['source_sha256'])
            bindings['devices'][row['device_id']] = dict(execution_route='http_synthetic',
                pre_state_hash=row_digest(row), evidence_ref=self.cas.put(canonical_json(proof)))
        with closing(sqlite3.connect(self.layout.ledger)) as conn:
            conn.row_factory = sqlite3.Row
            ref = self.cas.put(canonical_json(activation_manifest(conn, actor=plan['operation_id'], bindings=bindings)))
        at = timestamp(Clock().now())
        rehearsal = path.with_name('activation-expected.sqlite3')
        if rehearsal.exists(): raise MaintenanceError('unowned partial activation rehearsal')
        with closing(sqlite3.connect(self.layout.ledger)) as src, closing(sqlite3.connect(rehearsal)) as dst:
            src.backup(dst)
        activate_mixed(rehearsal, actor=plan['operation_id'], evidence_ref=ref, bindings=bindings,
                       artifacts=self.cas, clock=FakeClock(at))
        expected = ledger(rehearsal, include_devices=True)
        if not self.fixture:
            fresh = self._allowed_cas(plan).keys() - plan['anchor']['cas_files'].keys()
            self.service.own_activation_artifacts([self.layout.artifacts / name for name in fresh])
        _write(path, canonical(dict(ref=ref, at=at, bindings=bindings, expected=expected,
                                   cas_files=_tree(self.layout.artifacts))), exclusive=True)
        return ref

    def activation_evidence(self, plan, proof_ref):
        proof = _json(self._proof_path(plan))
        expected_manifest = dict(schema='sa_dispatch.mixed_activation.v1', actor=plan['operation_id'],
            ledger_hash='sha256:' + digest(canonical_json(plan['anchor']['rows'])),
            bindings_hash='sha256:' + digest(canonical_json(proof['bindings'])))
        if proof['ref'] != proof_ref or json.loads(self.cas.get(proof_ref)) != expected_manifest:
            raise MaintenanceError('activation proof changed')
        rows = self.rows()
        if rows['ledger_mode'][0]['mode'] == 'isolated':
            if rows != plan['anchor']['rows']: raise MaintenanceError('preactivation drift')
            return None
        if self._normalized(plan, rows) != self._normalized(plan, proof['expected']):
            raise MaintenanceError('committed activation differs from exact expected snapshot')
        return {'proof_ref': proof_ref, 'at': proof['at'], 'snapshot_sha256': digest(canonical(proof['expected']))}

    def activate(self, plan, proof_ref):
        proof = _json(self._proof_path(plan))
        if self.activation_evidence(plan, proof_ref) is not None: raise MaintenanceError('second activation refused')
        activate_mixed(self.layout.ledger, actor=plan['operation_id'], evidence_ref=proof_ref,
                       bindings=proof['bindings'], artifacts=self.cas, clock=FakeClock(proof['at']))
        return self.activation_evidence(plan, proof_ref)

    def validate_configs(self, plan, *, root=None, permissions=True):
        root = root or self.layout.config_root
        for item in plan['configs']:
            if digest(_read(root / item['path'])) != item['after_sha256']:
                raise MaintenanceError('configuration hash mismatch')
        http = load_config(root / 'coordinator.toml')
        local = load_config(root / 'local.toml', allowed_plugins={'research','research_content'})
        staged = load_config(self.layout.domain / 'local.toml', allowed_plugins={'research','research_content'})
        if (local != staged or not http.synthetic or http.execution_route != 'http_synthetic' or http.components or
                http.db_path != self.layout.ledger or http.artifact_root != self.layout.artifacts or
                http.journal_dir != self.layout.http_journal or local.execution_route != 'local_synthetic' or
                local.device_id in {r['device_id'] for r in plan['anchor']['rows']['devices']} or
                http.device_id == local.device_id):
            raise MaintenanceError('HTTP/local route or device isolation mismatch')
        target, sources, _ = collector_binding(root / 'collector-binding.json')
        old = SourceBinding(**plan['anchor']['old_binding'])
        new = sources.get(old.source_id)
        if (target != self.layout.ledger or len(sources) != 1 or new is None or
                new != replace(old, accepted_schema_versions=['0012'])):
            raise MaintenanceError('collector source identity or schema changed unexpectedly')
        manager, _ = manager_binding(root / 'manager-binding.json')
        matches = [s for s in manager['sources'] if s['kind'] == 'dispatch']
        if (len(matches) != 1 or matches[0]['locator'] != str(self.layout.ledger) or
                matches[0]['source_id'] != old.source_id or matches[0]['source_instance'] != old.source_instance or
                matches[0]['accepted_schema_versions'] != ['0012'] or
                matches[0].get('options') != {'identity':plan['anchor']['identity']}):
            raise MaintenanceError('manager dispatch binding mismatch')
        if permissions: self.service.verify_permissions()

    def expose(self, plan):
        self.validate_configs(plan)
        self.service.verify_guards()
        guard_check(self.layout.config_root)
        self._release_state_lock()
        self.service.start()

    def health(self, plan):
        self.service.verify_guards()
        self.service.health()

    def _normalized(self, plan, rows):
        rows = copy.deepcopy(rows)
        # Only server heartbeat timestamps and verified per-stream adoption differ.
        for row in rows['devices']:
            if row['device_id'] in {w['device_id'] for w in plan['anchor']['workers']['devices']}:
                row.pop('last_seen_at', None)
        if self._uses_fresh(plan):
            self._fresh_continuation(plan, rows)
            return rows
        for stream in STREAMS:
            adopted = self._validate_cursor(plan, rows, stream)
            gaps = [r for r in rows['collector_gaps'] if r not in plan['anchor']['rows']['collector_gaps']
                    and r['source_id'] == plan['anchor']['old_binding']['source_id'] and r['stream'] == stream]
            if len(gaps) > 1 or (adopted and (len(gaps) != 1 or gaps[0]['resolved_at'] is None)):
                raise MaintenanceError('adoption cursor lacks its unique resolution audit')
        old_source = plan['anchor']['old_binding']['source_id']
        rows['collector_cursors'] = [r for r in rows['collector_cursors'] if r['source_id'] != old_source]
        original_gaps = plan['anchor']['rows']['collector_gaps']
        for gap in rows['collector_gaps']:
            if gap in original_gaps: continue
            if (gap['source_id'] != old_source or gap['stream'] not in STREAMS or gap['kind'] != 'schema_changed' or
                    gap['from_seq'] != plan['anchor']['cursors'][gap['stream']]['last_seq'] or
                    gap['to_seq'] != gap['from_seq']):
                raise MaintenanceError('unknown collector gap')
            if gap['resolved_at'] is not None and (gap['resolved_by'] != plan['operation_id'] or
                    json.loads(gap['resolution']) != dict(reason='accept_schema', resume_after=gap['from_seq'],
                        epoch=plan['anchor']['cursors'][gap['stream']]['epoch'] + 1)):
                raise MaintenanceError('unknown adoption resolution')
        if any(g not in rows['collector_gaps'] for g in original_gaps): raise MaintenanceError('historical gap changed')
        rows['collector_gaps'] = original_gaps
        return rows

    def _validate_cursor(self, plan, rows, stream):
        old = plan['anchor']['cursors'][stream]
        matches = [r for r in rows['collector_cursors'] if r['source_id'] == old['source_id'] and r['stream'] == stream]
        if len(matches) != 1: raise MaintenanceError('collector cursor missing/duplicated')
        current = matches[0]
        if current == old: return False
        expected = {**old, 'epoch':old['epoch'] + 1, 'schema_version':'0012'}
        with closing(sqlite3.connect(self.layout.ledger)) as conn:
            conn.row_factory = sqlite3.Row
            binding = replace(SourceBinding(**plan['anchor']['old_binding']), accepted_schema_versions=['0012'])
            _, expected['schema_sha256'] = source_info(conn, binding, stream)
            if old['last_seq']:
                row = conn.execute('SELECT rowid AS _collector_seq,* FROM ' + stream + ' WHERE rowid=?',
                                   (old['last_seq'],)).fetchone()
                if row is None or collector_digest(enrich(conn, binding, stream, row)) != old['anchor_sha256']:
                    raise MaintenanceError('original collector anchor changed')
        expected['updated_at'] = current['updated_at']
        if current != expected: raise MaintenanceError('unexpected collector cursor transition')
        return True

    def exposure_fingerprint(self, plan):
        proof = _json(self._proof_path(plan))
        if self._uses_fresh(plan):
            self._verify_fresh_evidence(plan)
        if _tree(self.layout.artifacts) != proof['cas_files']: raise MaintenanceError('unknown CAS write')
        return dict(rows=self._normalized(plan, self.rows()), identity=_identity(self.layout.ledger),
                    domain=self._domain(), journal=_tree(self.layout.http_journal), cas=proof['cas_files'])

    def adopt_stream(self, plan, stream):
        if stream not in STREAMS: raise MaintenanceError('unplanned stream')
        old = plan['anchor']['cursors'][stream]
        if self._uses_fresh(plan):
            path = self.layout.config_root / 'collector-binding.json'
            if digest(_read(path)) != next(item['after_sha256'] for item in plan['configs']
                                          if item['path'] == path.name):
                raise MaintenanceError('fresh Collector binding changed during continuation')
            with directory_lock(self.layout.ledger.parent, shared=True):
                self._verify_fresh_evidence(plan)
                self._fresh_continuation(plan)
            return dict(source_id=old['source_id'], source_instance=old['source_instance'], stream=stream,
                        epoch=old['epoch'], last_seq=old['last_seq'], anchor_sha256=old['anchor_sha256'],
                        reason='continue_same_schema')
        target, sources, _ = collector_binding(self.layout.config_root / 'collector-binding.json')
        binding = sources[old['source_id']]
        with directory_lock(self.layout.ledger.parent, shared=True):
            self._normalized(plan, self.rows())
            if not self._validate_cursor(plan, self.rows(), stream):
                writer = Collector(target)
                gap = writer.sync(binding, stream)
                if gap['state'] != 'frozen' or gap['kind'] != 'schema_changed':
                    raise MaintenanceError('expected explicit schema freeze')
                writer.adopt(binding, stream, reason='accept_schema', actor=plan['operation_id'],
                             resume_after=old['last_seq'])
            self._normalized(plan, self.rows())
        return dict(source_id=old['source_id'], source_instance=old['source_instance'], stream=stream,
                    epoch=old['epoch'] + 1, last_seq=old['last_seq'], anchor_sha256=old['anchor_sha256'],
                    reason='accept_schema')


def local_run(layout, args):
    """Owned CLI entry: closed guard + shared ledger-directory lock for full lifetime."""
    if not args or any(arg == '--config' or arg.startswith('--config=') for arg in args):
        raise MaintenanceError('fixed local configuration required')
    with directory_lock(layout.ledger.parent, shared=True):
        guard_check(layout.config_root)
        command = ['systemd-run', '--quiet', '--wait', '--pipe', '--collect',
            '--unit=sa-dispatch-local.service', '--property=User=sa-dispatch', '--property=Group=sa-dispatch',
            '--property=NoNewPrivileges=yes', '--property=ProtectSystem=strict', '--property=UMask=0077',
            '--property=WorkingDirectory=' + str(layout.source_root),
            '--property=ReadWritePaths=' + str(layout.ledger.parent) + ' ' + str(layout.domain),
            '--setenv=PYTHONPATH=' + str(layout.source_root / 'dispatch'),
            '/usr/bin/flock', '--shared', '--nonblock', '--no-fork', str(layout.ledger.parent),
            '/usr/bin/python3', '-B', str(layout.source_root / 'knowledge/m0_content/dispatch_entry.py'), '--config',
            str(layout.config_root / 'local.toml'), *args]
        return subprocess.run(command, cwd=layout.source_root, check=False).returncode


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['local-run', 'verify-target'])
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--deployment-manifest')
    args, rest = parser.parse_known_args(argv)
    layout = NativeLayout(args.source_root)
    if args.command == 'verify-target' and (args.deployment_manifest is None or rest):
        parser.error('verify-target requires an explicit deployment manifest and fixed source root')
    if rest[:1] == ['--']: rest = rest[1:]
    try:
        adapter = NativeAdapter(layout, deployment_manifest=args.deployment_manifest)
    except InstallTargetError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if args.command == 'verify-target':
        return 0
    adapter.service.verify_guards()
    return local_run(layout, rest)


if __name__ == '__main__': raise SystemExit(main())
