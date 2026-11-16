"""Install a verified SuperAgent common package on its explicitly selected fresh VM."""

import argparse
from contextlib import contextmanager
import hashlib
import io
import json
import os
from pathlib import Path
import re
import secrets
import socket
import stat
import subprocess
import sys
import tempfile
from types import ModuleType
import zipfile


SERVICE = 'sa-dispatch.service'
LOCAL_SERVICE = 'sa-dispatch-local.service'
ACCOUNT = 'sa-dispatch'
WORKER = 'common-http-synthetic'
DISPATCHER = 'common-http-dispatcher'
SCHEMA = 'superagent.common.install.v1'
TARGET_SCHEMA = 'superagent.common.target.v1'
SCOPE = 'synthetic_research_content_single_ledger'
ENTRY = 'tools/m0_installer/common.py'
VERIFIER = 'tools/m0_installer/build_common.py'
TARGET_MODULE = 'tools/m0_deploy/install_target.py'
LOCK_BYTES = b'superagent.common.install.v1\n'
PHASES = ('intent', 'prepared', 'baseline', 'handoff', 'bootstrap', 'plan', 'mixed_done', 'completed')
ROOT_DIR = (0, 0, 0o755)
PRIVATE_DIR = (0, 0, 0o700)
ROOT_FILE = (0, 0, 0o644)
PRIVATE_FILE = (0, 0, 0o600)
MAX_ARCHIVE = 18 * 1024 * 1024
RECORD_LIMIT = 64 * 1024


class InstallError(ValueError):
    """The verified package, target, or installation state was refused."""


def _require(condition, reason):
    if not condition:
        raise InstallError(reason)


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _hex(value, length=64):
    return isinstance(value, str) and re.fullmatch(f'[0-9a-f]{{{length}}}', value) is not None


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def _decode(raw):
    value = json.loads(raw)
    _require(isinstance(value, dict) and _json(value) == raw, 'noncanonical installation record')
    return value


def _lstat(path, *, directory=False, optional=False):
    try:
        info = path.lstat()
    except FileNotFoundError:
        if optional:
            return None
        raise
    is_type = stat.S_ISDIR if directory else stat.S_ISREG
    _require(is_type(info.st_mode) and not getattr(info, 'st_file_attributes', 0) & 0x400
             and (directory or info.st_nlink == 1), 'unsafe installation file or directory type')
    return info


def _exists(path):
    return os.path.lexists(path)


def _read(path, maximum):
    info = _lstat(path)
    _require(info.st_size <= maximum, 'installation file exceeds its size limit')
    flags = os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0)
    with os.fdopen(os.open(path, flags), 'rb') as stream:
        opened = os.fstat(stream.fileno())
        raw = stream.read(maximum + 1)
    _require((opened.st_dev, opened.st_ino) == (info.st_dev, info.st_ino)
             and len(raw) == info.st_size, 'installation file changed while reading')
    return raw


def _sync(path):
    if os.name == 'posix':
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _load_exact(path, name, raw):
    module = ModuleType(name)
    module.__file__ = str(path)
    exec(compile(raw, str(path), 'exec'), module.__dict__)
    return module


def _protected_source(path):
    for parent in reversed(path.parents):
        info = _lstat(parent, directory=True)
        _require(info.st_uid == info.st_gid == 0 and not info.st_mode & 0o022,
                 'installer must be launched from a protected source directory')
    info = _lstat(path)
    _require(info.st_uid == info.st_gid == 0 and not info.st_mode & 0o022,
             'installer must be launched from protected regular source files')


def _verified_payload(package, manifest_hash, zip_hash, source_root):
    """Bind the executing entry and its bootstrap modules to this exact ZIP."""
    _require(_hex(manifest_hash) and _hex(zip_hash), 'full expected package and ZIP SHA-256 are required')
    launch_files = {}
    for name in (ENTRY, VERIFIER, TARGET_MODULE):
        _protected_source(source_root / name)
        launch_files[name] = _read(source_root / name, RECORD_LIMIT)
    verifier = _load_exact(source_root / VERIFIER, '_common_package_verifier', launch_files[VERIFIER])
    checked = verifier.verify_archive(package, expected_manifest_sha256=manifest_hash, expected_zip_sha256=zip_hash)
    manifest = checked['manifest']
    _require(manifest['distribution'] == 'installer_candidate' and manifest['installer_entry'] == 'payload/' + ENTRY,
             'package does not declare the common installer entry')
    raw = _read(package, MAX_ARCHIVE)
    _require(_sha(raw) == zip_hash, 'package changed after read-only verification')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        payload = {name.removeprefix('payload/'): archive.read(name) for name in manifest['files']}
    _require(all(raw == payload.get(name) for name, raw in launch_files.items()), 'launch source differs from verified package')
    return payload


class _Paths:
    def __init__(self, root):
        self.authority = root / 'var/lib/sa-common-install'
        self.state = self.authority / 'state.json'
        self.lock = self.authority / '.lock'
        self.tokens = self.authority / 'http-local-tokens.json'
        self.etc = root / 'etc/sa-dispatch'
        self.target = self.etc / 'install-target.json'
        self.coordinator = self.etc / 'coordinator.toml'
        self.identities = self.etc / 'identities.json'
        self.fresh_receipt = self.etc / 'fresh-install.json'
        self.http = root / 'opt/sa-dispatch'
        self.native = root / 'opt/sa-dispatch-native'
        self.maintenance_lock = self.http / '.maintenance.lock'
        self.runtime = root / 'var/lib/sa-dispatch'
        self.ledger = self.runtime / 'dispatch.sqlite3'
        self.cas = self.runtime / 'artifacts'
        self.journal = self.runtime / 'journal'
        self.domain_parent = root / 'var/lib/sa-dispatch-native'
        self.legacy = root / 'var/lib/sa-dispatch-manager'
        self.unit = root / 'etc/systemd/system' / SERVICE
        self.dropin = self.unit.with_name(SERVICE + '.d') / '10-m0-lab-port.conf'

    def fresh_roots(self):
        return (self.etc, self.http, self.native, self.runtime, self.domain_parent, self.legacy,
                self.unit, self.dropin.parent)


class _Host:
    def __init__(self, observe):
        self.observe = observe

    def metadata(self, path):
        info = path.lstat()
        return (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode))

    def own(self, path, metadata):
        uid, gid, mode = metadata
        os.chown(path, uid, gid, follow_symlinks=False)
        path.chmod(mode)

    def _command(self, *args, cwd=None, timeout=30, missing_unit=False):
        result = subprocess.run(args, cwd=cwd, capture_output=True, timeout=timeout,
                                env={'PATH': '/usr/sbin:/usr/bin', 'LANG': 'C.UTF-8'})
        absent = missing_unit and b'LoadState=not-found' in result.stdout.splitlines()
        _require((result.returncode == 0 or absent) and len(result.stdout) <= RECORD_LIMIT,
                 'fixed installer subprocess failed')
        return result.stdout

    def _systemctl(self, *args, missing_unit=False):
        return self._command('/usr/bin/systemctl', *args, missing_unit=missing_unit)

    def _unit(self, name):
        raw = self._systemctl('show', name, '--property=LoadState,ActiveState,MainPID,UnitFileState', missing_unit=True)
        return dict(line.split('=', 1) for line in raw.decode('utf-8').splitlines())

    def verify_idle(self, *, fresh):
        for name in (SERVICE, LOCAL_SERVICE):
            state = self._unit(name)
            _require(state.get('ActiveState') in ('inactive', 'failed') and state.get('MainPID') == '0'
                     and (not fresh or state.get('LoadState') == 'not-found'), 'preexisting Dispatch service or writer')
        _require(self._unit('sa-dispatch.timer').get('LoadState') == 'not-found', 'preexisting Dispatch timer')
        with socket.socket() as probe:
            probe.settimeout(2)
            _require(probe.connect_ex(('127.0.0.1', 8876)) != 0, 'Dispatch listener already exists')

    def account(self):
        import grp
        import pwd

        try:
            user = pwd.getpwnam(ACCOUNT)
        except KeyError:
            try:
                grp.getgrnam(ACCOUNT)
            except KeyError:
                return None
            raise InstallError('unexpected preexisting Dispatch group') from None
        group = grp.getgrnam(ACCOUNT)
        _require(user.pw_uid > 0 and user.pw_gid == group.gr_gid > 0 and not group.gr_mem
                 and user.pw_dir == '/nonexistent' and user.pw_shell == '/usr/sbin/nologin',
                 'unexpected Dispatch account shape')
        return {'uid': user.pw_uid, 'gid': user.pw_gid, 'intent': user.pw_gecos}

    def create_account(self, operation):
        self._command('/usr/sbin/useradd', '--system', '--user-group', '--home-dir', '/nonexistent',
                      '--no-create-home', '--shell', '/usr/sbin/nologin', '--comment', operation, ACCOUNT)

    def reload(self):
        self._systemctl('daemon-reload')

    def enabled(self):
        return self._unit(SERVICE).get('UnitFileState') == 'enabled'

    def enable(self):
        self._systemctl('enable', SERVICE)

    def _python(self, root, *args, timeout=120):
        result = self._command('/usr/bin/python3', '-I', '-B', *args, cwd=root, timeout=timeout)
        document = json.loads(result)
        _require(isinstance(document, dict) and document.get('ok') is True, 'invalid fixed installer subprocess response')
        return document

    def source_files(self, incoming):
        script = ('import sys\nfrom pathlib import Path\n'
                  'root = Path(sys.argv[1])\nsys.path.insert(0, str(root))\n'
                  'from tools.m0_native_mixed.native_adapter import NativeLayout, source_files\n'
                  'from tools.m0_native_mixed.maintenance import canonical\n'
                  'print(canonical(dict(ok=True, files=source_files(NativeLayout(root)))).decode())\n')
        return self._python(incoming, '-c', script, str(incoming))['files']

    def verify_offline(self, incoming, manifest):
        script = ('import sys\nfrom pathlib import Path\n'
                  'root = Path(sys.argv[1])\nsys.path.insert(0, str(root))\n'
                  'from tools.m0_native_mixed.native_adapter import NativeLayout, SystemdService\n'
                  'SystemdService(NativeLayout(root), deployment_manifest=sys.argv[2]).verify_stopped()\n'
                  'print(\'{"ok":true}\')\n')
        self._python(incoming, '-c', script, str(incoming), manifest)

    def baseline(self, incoming, manifest):
        script = ('import sys\nfrom pathlib import Path\n'
                  'root = Path(sys.argv[1])\nsys.path.insert(0, str(root))\n'
                  'from tools.m0_installer.common_baseline import initialize\n'
                  'from tools.m0_native_mixed.native_adapter import NativeLayout\n'
                  'from tools.m0_native_mixed.maintenance import canonical\n'
                  'result = initialize(NativeLayout(root), deployment_manifest=sys.argv[2])\n'
                  'print(canonical(dict(ok=True, receipt_sha256=result["receipt_sha256"])).decode())\n')
        return self._python(incoming, '-c', script, str(incoming), manifest)['receipt_sha256']

    def bootstrap(self, incoming, manifest, source_sha, coordinator_sha):
        return self._python(incoming, str(incoming / 'tools/m0_native_mixed/bootstrap.py'),
                            '--source-root', str(incoming), '--source-sha256', source_sha,
                            '--coordinator-before-sha256', coordinator_sha, '--deployment-manifest', manifest)

    def bridge(self, release, action, manifest, proof, coordinator, operation, plan):
        args = ['--deployment-manifest', manifest, '--fresh-install-sha256', proof,
                '--coordinator-before-sha256', coordinator, '--operation-id', operation]
        if plan is not None:
            args += ['--plan-sha256', plan]
        try:
            return self._python(release, str(release / 'tools/m0_native_mixed/common_entry.py'), action, *args, timeout=240)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            if action != 'status':
                raise
            raise InstallError('native status verification refused; inspect the current Dispatch service and guard') from exc


class _Installer:
    def __init__(self, paths, host, payload, *, manifest, zip_hash, machine_id, hostname):
        self.paths, self.host, self.payload = paths, host, payload
        self.identity = {'manifest_sha256': manifest, 'zip_sha256': zip_hash,
                         'machine_id': machine_id, 'hostname': hostname}
        self.incoming = paths.native / 'incoming' / manifest
        self.http_release = paths.http / 'releases' / manifest
        self.operation = 'common-' + manifest[:24]
        self.account_intent = 'common-' + manifest
        values = {'db_path': str(paths.ledger), 'artifact_root': str(paths.cas), 'journal_dir': str(paths.journal),
                  'device_id': 'coordinator', 'capabilities': [], 'plugins': ['hotspot'], 'synthetic': True,
                  'lease_ttl_seconds': 30, 'component_timeout_seconds': 20}
        self.config = ('\n'.join(f'{name} = {json.dumps(value)}' for name, value in values.items()) +
                       '\n[components]\n').encode('utf-8')
        self.coordinator_hash = _sha(self.config)

    def _checked(self, path, metadata, *, directory=False):
        info = _lstat(path, directory=directory)
        _require(self.host.metadata(path) == metadata, 'unexpected installation file ownership or mode')
        return info

    def _parent_chain(self, path):
        for item in reversed(path.parents):
            _lstat(item, directory=True)
            uid, gid, mode = self.host.metadata(item)
            _require(uid == gid == 0 and not mode & 0o022, 'installation parent is writable or unowned')

    def _directory(self, path, metadata=ROOT_DIR, *, create):
        self._parent_chain(path)
        if not _exists(path):
            _require(create, 'required installation directory is missing')
            path.mkdir(mode=metadata[2])
            self.host.own(path, metadata)
            _sync(path.parent)
        self._checked(path, metadata, directory=True)

    def _atomic(self, path, raw, metadata, *, replace=False):
        self._parent_chain(path)
        pending = None
        try:
            with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.common-write-', delete=False) as stream:
                pending = Path(stream.name)
                stream.write(raw)
                self.host.own(pending, metadata)
                stream.flush()
                os.fsync(stream.fileno())
            if replace:
                self._checked(path, metadata)
                os.replace(pending, path)
            else:
                os.link(pending, path)
                pending.unlink()
            _sync(path.parent)
        finally:
            if pending is not None and _exists(pending):
                pending.unlink()

    def _file(self, path, raw, metadata=ROOT_FILE, *, create):
        self._parent_chain(path)
        if not _exists(path):
            _require(create, 'required installation file is missing')
            self._atomic(path, raw, metadata)
        self._checked(path, metadata)
        _require(_read(path, len(raw)) == raw, 'unexpected installation file bytes')

    def _tree(self, root, files, *, create):
        self._directory(root, create=create)
        directories = {Path('.')}
        for name in files:
            directories.update(Path(name).parents)
        for item in root.rglob('*'):
            relative = item.relative_to(root)
            _require(relative in directories or relative.as_posix() in files, 'foreign path inside frozen installation tree')
        for name in sorted(directories - {Path('.')}, key=lambda item: (len(item.parts), item.as_posix())):
            self._directory(root / name, create=create)
        for name, raw in sorted(files.items()):
            self._file(root / name, raw, create=create)

    @contextmanager
    def _locked(self, *, install):
        self._directory(self.paths.authority, PRIVATE_DIR, create=install)
        self._file(self.paths.lock, LOCK_BYTES, PRIVATE_FILE, create=install)
        with self.paths.lock.open('r+b') as stream:
            if os.name == 'nt':  # Only the bounded fixture can reach this branch.
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                yield
            finally:
                if os.name == 'nt':
                    stream.seek(0)
                    msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)

    def _observe(self):
        expected = self.identity['machine_id'], self.identity['hostname']
        _require(_hex(expected[0], 32) and expected[0] != '0' * 32 and isinstance(expected[1], str) and
                 re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.-]{0,63}', expected[1]) is not None,
                 'explicit expected machine ID and hostname are required')
        _require(self.host.observe() == expected, 'protected machine observation does not match operator selection')

    def _read_state(self):
        self._checked(self.paths.state, PRIVATE_FILE)
        state = _decode(_read(self.paths.state, RECORD_LIMIT))
        _require(state.get('schema') == SCHEMA and state.get('identity') == self.identity and state.get('phase') in PHASES,
                 'common install state belongs to a different package or target')
        return state

    def _save(self, phase=None, **changes):
        previous = _exists(self.paths.state)
        next_state = {**self.state, **changes}
        if phase is not None:
            next_state['phase'] = phase
        self._atomic(self.paths.state, _json(next_state), PRIVATE_FILE, replace=previous)
        self.state = next_state

    def _target(self, *, create):
        record = {'schema': TARGET_SCHEMA, 'scope': SCOPE, 'manifest_sha256': self.identity['manifest_sha256'],
                  'machine_id': self.identity['machine_id'], 'hostname': self.identity['hostname']}
        self._file(self.paths.target, _json(record), PRIVATE_FILE, create=create)

    def _account(self, *, create):
        account = self.host.account()
        if account is None:
            _require(create and 'account' not in self.state, 'journaled Dispatch account is missing')
            self.host.create_account(self.account_intent)
            account = self.host.account()
        _require(isinstance(account, dict) and set(account) == {'uid', 'gid', 'intent'}
                 and account['intent'] == self.account_intent
                 and all(type(account[key]) is int and account[key] > 0 for key in ('uid', 'gid')),
                 'invalid Dispatch account identity or creation marker')
        if 'account' in self.state:
            _require(account == self.state['account'], 'journaled Dispatch account identity changed')
        else:
            _require(create, 'Dispatch account does not have durable installer authority')
            self._save(account=account)
        return account

    def _credentials(self, group, *, create):
        path = self.paths.tokens
        if 'credential_id' not in self.state:
            _require(create and not _exists(path) and not _exists(self.paths.identities),
                     'existing local HTTP authorization lacks creation authority')
            self._save(credential_id=secrets.token_hex(16))
        identifier = self.state['credential_id']
        _require(_hex(identifier, 32), 'invalid local HTTP authorization identity')
        if not _exists(path):
            _require(create and 'tokens_sha256' not in self.state and not _exists(self.paths.identities),
                     'local HTTP material is missing or lacks authority')
            tokens = {name: secrets.token_urlsafe(32) for name in (DISPATCHER, WORKER)}
            self._atomic(path, _json({'credential_id': identifier, 'tokens': tokens}), PRIVATE_FILE)
        self._checked(path, PRIVATE_FILE)
        raw = _read(path, RECORD_LIMIT)
        envelope = _decode(raw)
        tokens = envelope.get('tokens')
        _require(set(envelope) == {'credential_id', 'tokens'} and envelope['credential_id'] == identifier and
                 isinstance(tokens, dict) and set(tokens) == {DISPATCHER, WORKER} and all(isinstance(value, str) and
                 re.fullmatch(r'[A-Za-z0-9_-]{43}', value) for value in tokens.values()), 'invalid local HTTP material')
        if 'tokens_sha256' in self.state:
            _require(_sha(raw) == self.state['tokens_sha256'], 'local HTTP material changed')
        else:
            _require(create, 'local HTTP material lacks its durable identity')
            self._save(tokens_sha256=_sha(raw))
        identities = {'devices': {
            DISPATCHER: {'token_sha256': _sha(tokens[DISPATCHER].encode()), 'roles': ['dispatcher']},
            WORKER: {'token_sha256': _sha(tokens[WORKER].encode()), 'roles': ['worker'],
                     'capabilities': ['component:mgmtcore']},
        }}
        self._file(self.paths.identities, _json(identities), (0, group, 0o640), create=create)

    def _release_files(self):
        runtime = {name.removeprefix('dispatch/'): raw for name, raw in self.payload.items()
                   if name.startswith(('dispatch/sa_dispatch/', 'dispatch/sa_dispatch_hotspot/'))}
        _require(runtime and all(len(Path(name).parts) == 2 for name in runtime), 'invalid fixed HTTP release closure')
        manifest = {'schema': 'm0_deploy.v1', 'kind': 'runtime-release', 'files': [
            {'path': name, 'size': len(raw), 'sha256': _sha(raw)} for name, raw in sorted(runtime.items())]}
        return {**runtime, 'release.json': _json(manifest) + b'\n'}

    def _current(self, *, create):
        path = self.paths.http / 'current'
        expected = self.http_release.relative_to(path.parent)
        if not _exists(path):
            _require(create, 'HTTP current release selector is missing')
            path.symlink_to(expected, target_is_directory=True)
            _sync(path.parent)
        _require(path.is_symlink() and Path(os.readlink(path)) == expected and self.host.metadata(path)[:2] == (0, 0),
                 'foreign HTTP current release selector')

    def _unit(self, *, create):
        p = self.paths
        release = p.native / 'releases' / self.state['source_sha256']
        maintenance = release / 'tools/m0_native_mixed/maintenance.py'
        unit = (f'[Unit]\nDescription=SuperAgent synthetic Dispatch coordinator\n'
                f'[Service]\nType=simple\nUser={ACCOUNT}\nGroup={ACCOUNT}\nWorkingDirectory={p.http}/current\n'
                f'NoNewPrivileges=yes\nProtectSystem=strict\nProtectHome=yes\nPrivateTmp=yes\nUMask=0077\n'
                f'ReadWritePaths={p.runtime}\nUnsetEnvironment=PYTHONHOME PYTHONSTARTUP\n'
                f'ExecStartPre=+/usr/bin/python3 -I -B {maintenance} guard-check --native-root {p.etc}\n'
                f'ExecStart=/usr/bin/flock --shared --nonblock --no-fork {p.runtime} '
                f'/usr/bin/python3 -P -s -B -m sa_dispatch --config {p.coordinator} '
                f'serve --identities {p.identities} --port 8876\n'
                '[Install]\nWantedBy=multi-user.target\n').encode('utf-8')
        dropin = f'[Service]\nEnvironment=PYTHONPATH={p.http}/current\n'.encode('utf-8')
        self._file(p.unit, unit, create=create)
        self._file(p.dropin, dropin, create=create)

    def _prepare(self, *, create, baseline_ready=False, after_plan=False):
        p = self.paths
        for directory in (p.etc, p.http, p.http / 'releases', p.native, p.native / 'incoming',
                          p.native / 'releases', p.dropin.parent):
            self._directory(directory, create=create)
        allowed_dropins = {p.dropin.name}
        if PHASES.index(self.state['phase']) >= PHASES.index('handoff'):
            allowed_dropins.add('native-mixed-guard.conf')
        _require({path.name for path in p.dropin.parent.iterdir()} <= allowed_dropins,
                 'foreign systemd drop-in blocks common installation')
        self._directory(p.legacy, PRIVATE_DIR, create=create)
        if not baseline_ready:
            for directory in (p.runtime, p.cas, p.journal):
                self._directory(directory, PRIVATE_DIR, create=create)
        self._target(create=create)
        account = self._account(create=create)
        self._credentials(account['gid'], create=create)
        self._file(p.maintenance_lock, LOCK_BYTES, PRIVATE_FILE, create=create)
        self._tree(self.incoming, self.payload, create=create)
        self._tree(self.http_release, self._release_files(), create=create)
        self._current(create=create)
        files = self.host.source_files(self.incoming)
        _require(isinstance(files, dict) and files and
                 all(name in self.payload and _sha(self.payload[name]) == sha for name, sha in files.items()) and
                 'tools/m0_native_mixed/common_entry.py' in files, 'invalid frozen native source map')
        source_sha = _sha(_json(files))
        marker = _json({'schema': 'native_mixed.frozen_source.v1', 'source_sha256': source_sha, 'files': files})
        self.native_files = {**{name: self.payload[name] for name in files}, '.native-source.json': marker}
        if 'source_sha256' in self.state:
            _require(source_sha == self.state['source_sha256'], 'frozen native source changed')
        else:
            _require(create, 'frozen native source lacks durable authority')
            self._save(source_sha256=source_sha)
        self._unit(create=create)
        if not after_plan:
            self._file(p.coordinator, self.config, (0, account['gid'], 0o640), create=create)

    def _identity_of(self, info):
        return {'device': info.st_dev, 'inode': info.st_ino}

    def _handoff_paths(self):
        p = self.paths
        return (p.runtime, p.ledger, p.cas, p.journal)

    def _record_baseline(self, proof):
        p = self.paths
        self._checked(p.fresh_receipt, PRIVATE_FILE)
        raw = _read(p.fresh_receipt, RECORD_LIMIT)
        value = _decode(raw)
        _require(_hex(proof) and _sha(raw) == proof and value.get('phase') == 'completed' and
                 value.get('schema') == 'native_mixed.fresh_install.v1' and
                 value.get('deployment_manifest') == self.identity['manifest_sha256'] and
                 value.get('ledger') == str(p.ledger), 'baseline receipt differs from its installer authority')
        user, group = (self.state['account'][key] for key in ('uid', 'gid'))
        handoff = []
        for path in self._handoff_paths():
            directory = path != p.ledger
            before = PRIVATE_DIR if directory else PRIVATE_FILE
            info = self._checked(path, before, directory=directory)
            handoff.append({'path': str(path), 'identity': self._identity_of(info), 'before': list(before),
                            'after': [user, group, before[2]]})
        _require(handoff[1]['identity'] == value.get('ledger_identity'), 'baseline ledger identity changed')
        self._save('baseline', fresh_install_sha256=proof, handoff=handoff)

    def _handoff(self, *, apply, pristine):
        p = self.paths
        self._checked(p.fresh_receipt, PRIVATE_FILE)
        _require(_sha(_read(p.fresh_receipt, RECORD_LIMIT)) == self.state.get('fresh_install_sha256'),
                 'fresh installation receipt changed')
        if pristine:
            _require(set(p.runtime.iterdir()) == {p.ledger, p.cas, p.journal} and not any(p.cas.iterdir())
                     and not any(p.journal.iterdir()), 'foreign state blocks runtime permission handoff')
        paths = self._handoff_paths()
        records = self.state.get('handoff')
        _require(isinstance(records, list) and len(records) == len(paths), 'missing runtime ownership journal')
        for item, path in zip(records, paths):
            directory = path != p.ledger
            before = PRIVATE_DIR if directory else PRIVATE_FILE
            after = (self.state['account']['uid'], self.state['account']['gid'], before[2])
            info = _lstat(path, directory=directory)
            _require(item == {'path': str(path), 'identity': self._identity_of(info), 'before': list(before),
                             'after': list(after)}, 'runtime ownership journal or inode differs')
            current = self.host.metadata(path)
            if apply and current == before:
                self.host.own(path, after)
            else:
                _require(current == after, 'runtime ownership differs from its journal')
            self._checked(path, after, directory=directory)

    def _bridge(self, action):
        source = self.state.get('source_sha256')
        proof = self.state.get('fresh_install_sha256')
        plan = self.state.get('plan_sha256')
        _require(_hex(source) and _hex(proof) and (action == 'plan' or _hex(plan)), 'missing fixed bridge authority')
        release = self.paths.native / 'releases' / source
        self._tree(release, self.native_files, create=False)
        return self.host.bridge(release, action, self.identity['manifest_sha256'], proof,
                                self.coordinator_hash, self.operation, None if action == 'plan' else plan)

    def _report(self):
        _require(self._mixed_complete(self._bridge('status')),
                 'completed installation lacks verified current Dispatch health; inspect the service and guard')
        _require(self.host.enabled(), 'completed Dispatch service enablement differs')
        return {'ok': True, 'schema': SCHEMA, 'phase': 'completed', 'scope': SCOPE,
                'manifest_sha256': self.identity['manifest_sha256'], 'zip_sha256': self.identity['zip_sha256'],
                'source_sha256': self.state['source_sha256'], 'fresh_install_sha256': self.state['fresh_install_sha256'],
                'plan_sha256': self.state['plan_sha256'], 'operation_id': self.operation,
                'listen': '127.0.0.1:8876', 'runtime_healthy': True}

    def _mixed_complete(self, status):
        return (status.get('phase') == 'completed' and status.get('ledger_mode') == 'mixed'
                and status.get('plan_sha256') == self.state['plan_sha256'] and status.get('runtime_healthy') is True)

    def run(self, *, install):
        self._observe()
        if install and not _exists(self.paths.state):
            _require(not any(_exists(path) for path in self.paths.fresh_roots()), 'unrecognized preexisting common installation')
        with self._locked(install=install):
            self._observe()
            if _exists(self.paths.state):
                self.state = self._read_state()
            else:
                _require(install and not any(_exists(path) for path in self.paths.fresh_roots())
                         and set(self.paths.authority.iterdir()) == {self.paths.lock} and self.host.account() is None,
                         'unrecognized preexisting common installation or account')
                self.host.verify_idle(fresh=True)
                self.state = {'schema': SCHEMA, 'identity': self.identity, 'phase': 'intent'}
                self._save()
            phase = self.state['phase']
            if phase == 'completed':
                self._prepare(create=False, baseline_ready=True, after_plan=True)
                self._handoff(apply=False, pristine=False)
                return self._report()
            if not install:
                return {'ok': True, 'schema': SCHEMA, 'phase': phase,
                        'manifest_sha256': self.identity['manifest_sha256'], 'operation_id': self.operation}
            if PHASES.index(phase) < PHASES.index('plan'):
                self.host.verify_idle(fresh=False)
            self._prepare(create=phase == 'intent', baseline_ready=PHASES.index(phase) >= PHASES.index('baseline'),
                          after_plan=PHASES.index(phase) >= PHASES.index('plan'))
            if PHASES.index(phase) >= PHASES.index('bootstrap'):
                self._handoff(apply=False, pristine=phase == 'bootstrap')
            manifest = self.identity['manifest_sha256']
            if self.state['phase'] == 'intent':
                self.host.reload()
                self._save('prepared')
            if self.state['phase'] == 'prepared':
                self._record_baseline(self.host.baseline(self.incoming, manifest))
            if self.state['phase'] == 'baseline':
                self.host.verify_offline(self.incoming, manifest)
                self._handoff(apply=True, pristine=True)
                self._save('handoff')
            if self.state['phase'] == 'handoff':
                self.host.verify_offline(self.incoming, manifest)
                self._handoff(apply=False, pristine=True)
                response = self.host.bootstrap(self.incoming, manifest, self.state['source_sha256'], self.coordinator_hash)
                _require(response.get('phase') == 'completed', 'native Bootstrap did not complete')
                self._save('bootstrap')
            if self.state['phase'] == 'bootstrap':
                plan = self._bridge('plan').get('plan_sha256')
                _require(_hex(plan), 'native bridge did not return its frozen plan hash')
                self._save('plan', plan_sha256=plan)
            if self.state['phase'] == 'plan':
                status = self._bridge('status')
                if status.get('phase') != 'completed':
                    _require(self._bridge('run').get('phase') == 'completed', 'native mixed maintenance did not complete')
                    status = self._bridge('status')
                _require(self._mixed_complete(status), 'native mixed maintenance or current Dispatch health is unverified')
                self._save('mixed_done')
            if self.state['phase'] == 'mixed_done':
                _require(self._mixed_complete(self._bridge('status')),
                         'cannot enable without native completion and verified current Dispatch health')
                if not self.host.enabled():
                    self.host.enable()
                report = self._report()
                self._save('completed')
                return report
            raise InstallError('unrecognized installer phase')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('command', choices=('install', 'status'))
    parser.add_argument('package', type=Path)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--zip-sha256', required=True)
    parser.add_argument('--expected-machine-id', required=True)
    parser.add_argument('--expected-hostname', required=True)
    args = parser.parse_args(argv)
    try:
        _require(sys.platform == 'linux' and os.geteuid() == 0 and sys.flags.isolated and sys.dont_write_bytecode,
                 'common installation requires Linux root and python3 -I -B')
        launch = Path(__file__).absolute()
        source_root = launch.parents[2]
        _require(launch == source_root / ENTRY, 'common installer must execute at its verified package path')
        payload = _verified_payload(args.package.absolute(), args.manifest_sha256, args.zip_sha256, source_root)
        target = _load_exact(source_root / TARGET_MODULE, '_common_install_target', payload[TARGET_MODULE])
        runner = _Installer(_Paths(Path('/')), _Host(target.observe_install_target), payload,
                            manifest=args.manifest_sha256, zip_hash=args.zip_sha256,
                            machine_id=args.expected_machine_id, hostname=args.expected_hostname)
        report = runner.run(install=args.command == 'install')
    except Exception as exc:
        # Subprocess output may include sensitive context. Report only the known
        # installer refusal string or exception type, never raw command output.
        reason = str(exc) if isinstance(exc, InstallError) else type(exc).__name__
        print(json.dumps({'ok': False, 'error': reason}), file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
