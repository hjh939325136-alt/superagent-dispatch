"""Explicit Debian 0012-to-0013 addon for the existing common Dispatch ledger."""

import argparse
from contextlib import closing
from hashlib import file_digest, sha256
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys
import tempfile
import tomllib


RELEASES = Path('/opt/sa-dispatch-common/releases')
AUTHORITY = Path('/var/lib/sa-dispatch-agent-maintenance')
CONFIG = Path('/etc/sa-dispatch')
SYSTEMD = Path('/etc/systemd/system')
LEDGER = Path('/var/lib/sa-dispatch/dispatch.sqlite3')
CAS = LEDGER.parent / 'artifacts'
DOMAIN = Path('/var/lib/sa-dispatch-native/domain')
MAINTENANCE_LOCK = Path('/opt/sa-dispatch/.maintenance.lock')
LOCAL_ENTRY = Path('/opt/sa-dispatch-native/bin/local-dispatch')
HTTP = 'sa-dispatch.service'
LOCAL = 'sa-dispatch-local.service'
ACCOUNT = 'sa-agent-external'
AGENT = ACCOUNT + '.service'
TIMER = ACCOUNT + '.timer'
SHARED = 'sa-dispatch-ledger'
SOURCE_MANIFEST = 'SOURCE_MANIFEST.json'
SOURCE_SCHEMA = 'superagent.common.external.source.v1'
STATE_SCHEMA = 'superagent.common.external.install.v1'
ENTRY = 'tools/m0_agent/common_install.py'
GUARD = CONFIG / '.mixed-maintenance/guard.json'
DROPINS = SYSTEMD / (HTTP + '.d')
DROPIN = DROPINS / 'zz-common-external.conf'
BOOT_MASK = Path('/run/systemd/generator.early') / HTTP
OLD_RELEASES = Path('/opt/sa-dispatch-native/releases')
SYSTEM_PYTHON = Path('/usr/bin/python3')
OLD_SERVICES = (SYSTEMD / HTTP, DROPINS / '10-m0-lab-port.conf', DROPINS / 'native-mixed-guard.conf', LOCAL_ENTRY)
BASE_CONFIGS = ('install-target.json', 'coordinator.toml', 'local.toml', 'manager-binding.json', 'collector-binding.json')
AGENT_CONFIGS = ('agent-external.json', 'provider-network.json')
STREAMS = ('events', 'schedule_events', 'control_events')
HEX = re.compile(r'[0-9a-f]{64}')
COMPONENT = re.compile(r'[A-Za-z0-9_.-]{1,128}')
ENV = {'PATH': '/usr/sbin:/usr/bin', 'LANG': 'C.UTF-8'}


class InstallError(ValueError):
    pass


def _require(condition, reason):
    if not condition:
        raise InstallError(reason)


def _json(data):
    return json.dumps(data, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('utf-8')


def _unique_fields(pairs):
    values = dict(pairs)
    _require(len(values) == len(pairs), 'duplicate metadata field')
    return values


def _root_metadata(path, *, directory=False, private=False):
    info = path.lstat()
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    _require(expected(info.st_mode) and info.st_uid == 0 and info.st_mode & 0o022 == 0
             and (directory or info.st_nlink == 1), 'unsafe root-managed installation path')
    if private:
        _require(info.st_gid == 0 and stat.S_IMODE(info.st_mode) == (0o700 if directory else 0o600),
                 'unsafe addon maintenance path')
    return info


def _root_parents(path):
    for parent in reversed(path.parents):
        _root_metadata(parent, directory=True)


def _root_bytes(path, *, limit=1024 * 1024, private=False):
    _root_metadata(path, private=private)
    with path.open('rb') as handle:
        raw = handle.read(limit + 1)
    _require(len(raw) <= limit, 'installation metadata file exceeds the byte limit')
    return raw


def _root_hash(path):
    _root_metadata(path)
    with path.open('rb') as handle:
        return file_digest(handle, 'sha256').hexdigest()


def _source_files(release, expected):
    _require(HEX.fullmatch(expected) and release.name == expected, 'expected source manifest digest is invalid')
    _root_parents(release)
    _root_metadata(release, directory=True)
    manifest = release / SOURCE_MANIFEST
    _require(_root_hash(manifest) == expected, 'source manifest SHA256 mismatch')
    value = json.loads(_root_bytes(manifest), object_pairs_hook=_unique_fields)
    _require(isinstance(value, dict) and set(value) == {'schema', 'files'} and value['schema'] == SOURCE_SCHEMA,
             'invalid source manifest schema')
    files = value['files']
    _require(isinstance(files, dict) and ENTRY in files and 1 <= len(files) <= 2000, 'invalid source manifest members')
    for name, digest in files.items():
        parts = name.split('/')
        _require(name != SOURCE_MANIFEST and all(part not in ('.', '..') and COMPONENT.fullmatch(part) for part in parts)
                 and isinstance(digest, str) and HEX.fullmatch(digest), 'invalid release path or hash')
    directories = {parent.as_posix() for name in files for parent in Path(name).parents}
    found = set()
    for folder, children, names in os.walk(release, followlinks=False):
        for child in children:
            path = Path(folder) / child
            _root_metadata(path, directory=True)
            _require(path.relative_to(release).as_posix() in directories, 'release contains an unlisted directory')
        for name in names:
            path = Path(folder) / name
            relative = path.relative_to(release).as_posix()
            if relative == SOURCE_MANIFEST:
                continue
            _require(relative in files and _root_hash(path) == files[relative], 'release file is unlisted or changed')
            found.add(relative)
    _require(found == set(files), 'source manifest has missing release files')


def _sync_parent(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic(path, raw, *, mode, gid=0):
    if path.exists() or path.is_symlink():
        _root_metadata(path)
    temp = None
    try:
        with tempfile.NamedTemporaryFile(prefix='.common-addon-', dir=path.parent, delete=False) as item:
            temp = Path(item.name)
            os.fchown(item.fileno(), 0, gid)
            os.fchmod(item.fileno(), mode)
            item.write(raw)
            item.flush()
            os.fsync(item.fileno())
        os.replace(temp, path)
        _sync_parent(path.parent)
    finally:
        if temp is not None:
            temp.unlink(missing_ok=True)


def _run(*args, check=True):
    return subprocess.run(args, env=ENV, capture_output=True, text=True, timeout=45, check=check)


def _systemctl(*args, check=True):
    return _run('/usr/bin/systemctl', *args, check=check)


def _property(unit, name):
    return _systemctl('show', unit, '--value', '--property=' + name, check=False).stdout.strip()


def _verify_inactive():
    for unit in (HTTP, LOCAL, TIMER, AGENT):
        state = _property(unit, 'ActiveState')
        _require(state in ('inactive', 'failed'), 'a known common writer or timer remains active')


def _stop_writers():
    for unit in (TIMER, AGENT, LOCAL, HTTP):
        if _property(unit, 'LoadState') != 'not-found':
            _systemctl('stop', unit)
    _verify_inactive()


def _verify_old_http_fragment():
    fragment = _property(HTTP, 'FragmentPath')
    accepted_mask = (fragment == str(BOOT_MASK) and _property(HTTP, 'LoadState') == 'masked'
                     and BOOT_MASK.is_symlink() and os.readlink(BOOT_MASK) == '/dev/null')
    _require(fragment == str(OLD_SERVICES[0]) or accepted_mask,
             'systemd coordinator is outside the fixed common service')


def _check_schema():
    from dispatch.sa_dispatch.external_migration import verify_schema
    from dispatch.sa_dispatch.routing import ledger_mode

    info = LEDGER.lstat()
    _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, 'common ledger must be a single-link regular file')
    with closing(sqlite3.connect(LEDGER.as_uri() + '?mode=ro', uri=True)) as conn, conn:
        conn.execute('BEGIN')
        _require(ledger_mode(conn) == 'mixed', 'addon requires the accepted mixed ledger')
        return verify_schema(conn), f'{info.st_dev}:{info.st_ino}'


def _config_hashes(names):
    hashes = {}
    for name in names:
        path = CONFIG / name
        try:
            hashes[name] = _root_hash(path)
        except FileNotFoundError:
            if name != 'provider-network.json':
                raise
            hashes[name] = None
    return hashes


def _valid_share(path, expected_uid, *, directory):
    info = path.lstat()
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    _require(kind(info.st_mode) and info.st_uid in (0, expected_uid)
             and (directory or info.st_nlink == 1), 'shared path is unsafe or belongs to an unrelated account')
    return info


def _accounts(*, create=False):
    import grp
    import pwd

    try:
        group = grp.getgrnam(SHARED)
    except KeyError:
        _require(create, 'common ledger group is missing')
        _run('/usr/sbin/groupadd', '--system', SHARED)
        group = grp.getgrnam(SHARED)
    try:
        account = pwd.getpwnam(ACCOUNT)
    except KeyError:
        _require(create, 'common agent account is missing')
        _run('/usr/sbin/useradd', '--system', '--no-create-home', '--home-dir', '/nonexistent',
             '--shell', '/usr/sbin/nologin', '--gid', SHARED, ACCOUNT)
        account = pwd.getpwnam(ACCOUNT)
    dispatch = pwd.getpwnam('sa-dispatch')
    _require(account.pw_uid not in (0, dispatch.pw_uid) and account.pw_gid == group.gr_gid != 0
             and account.pw_shell == '/usr/sbin/nologin' and account.pw_dir == '/nonexistent'
             and set(group.gr_mem) <= {ACCOUNT, 'sa-dispatch'}, 'common agent/system groups have unexpected membership')
    return dispatch.pw_uid, group.gr_gid


def _share_ledger():
    dispatch, shared = _accounts(create=True)
    paths = []
    for path in (LEDGER.parent, CAS, LEDGER, *CAS.rglob('*')):
        info = _valid_share(path, dispatch, directory=path in (LEDGER.parent, CAS) or path.is_dir())
        mode = 0o2770 if stat.S_ISDIR(info.st_mode) else 0o660 if path == LEDGER else 0o640
        paths.append((path, mode))
    for path, mode in paths:
        os.chown(path, -1, shared, follow_symlinks=False)
        os.chmod(path, mode)


def _unit_bytes(release, machine, manifest):
    store = LEDGER.parent
    args = f'--expected-machine {machine} --manifest-sha256 {manifest}'
    installer = f'/usr/bin/python3 -I -B {release / ENTRY}'
    guard = f'ExecStartPre=+{installer} guard {args}\n'
    lock = f'/usr/bin/flock --shared --nonblock --no-fork {store}'
    pythonpath = f'{release / "dispatch"}:{release}'
    http = (f'[Service]\nExecStartPre=\n{guard}ExecStart=\n'
            f'WorkingDirectory={release / "dispatch"}\nEnvironment=PYTHONPATH={pythonpath}\n'
            f'SupplementaryGroups={SHARED}\nUMask=0007\n'
            f'ExecStart={lock} /usr/bin/python3 -P -s -B -m sa_dispatch --config {CONFIG / "coordinator.toml"} '
            f'serve --identities {CONFIG / "identities.json"} --port 8876\n')
    agent = ('[Unit]\nDescription=Common Dispatch fixed DeepSeek slot\n'
             'Wants=network-online.target\nAfter=network-online.target\n'
             '[Service]\nType=oneshot\n'
             f'User={ACCOUNT}\nGroup={SHARED}\nWorkingDirectory={release}\n'
             'NoNewPrivileges=yes\nProtectSystem=strict\nProtectHome=yes\nPrivateTmp=yes\n'
             'RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6\nUMask=0007\nTimeoutStartSec=85\n'
             f'ReadWritePaths={store}\nLoadCredential=provider_api_key\n{guard}'
             f'ExecStart={lock} /usr/bin/python3 -I -B {release / "tools/m0_agent/common_runtime.py"} tick\n')
    timer = ('[Unit]\nDescription=Wake the common Dispatch single-slot agent\n'
             f'[Timer]\nOnBootSec=30s\nOnUnitInactiveSec=30s\nAccuracySec=2s\nUnit={AGENT}\n'
             '[Install]\nWantedBy=timers.target\n')
    local = f'#!/bin/sh\nexec {installer} local-run {args} -- "$@"\n'
    return {DROPIN: (http.encode(), 0o644), SYSTEMD / AGENT: (agent.encode(), 0o644),
            SYSTEMD / TIMER: (timer.encode(), 0o644), LOCAL_ENTRY: (local.encode(), 0o755)}

def _trusted_local_python():
    _root_parents(SYSTEM_PYTHON)
    alias = SYSTEM_PYTHON.lstat()
    _require(alias.st_uid == 0 and (stat.S_ISLNK(alias.st_mode) or stat.S_ISREG(alias.st_mode)),
             'system Python entry is unsafe')
    executable = SYSTEM_PYTHON.resolve(strict=True)
    _require(executable.parent == SYSTEM_PYTHON.parent and re.fullmatch(r'python3(?:\.[0-9]+)?', executable.name),
             'system Python must resolve to the packaged interpreter')
    info = _root_metadata(executable)
    _require(info.st_mode & stat.S_IXUSR, 'system Python is not executable')
    return {str(SYSTEM_PYTHON), str(executable)}


def _retarget_local(raw, release):
    from tools.m0_native_mixed.domains import COMPONENTS

    expected = tomllib.loads(raw.decode('utf-8'))
    _require(expected['execution_route'] == 'local_synthetic' and expected['synthetic'] is True
             and expected['db_path'] == str(LEDGER) and expected['artifact_root'] == str(CAS)
             and set(expected['components']) == set(COMPONENTS), 'local route is outside the common baseline')
    executables = _trusted_local_python()
    old_releases = set()
    for name, (script, flag) in COMPONENTS.items():
        argv = expected['components'][name]['argv']
        binding = DOMAIN / ('research-binding.json' if name == 'research' else 'workflow-binding.json')
        _require(len(argv) == 5 and argv[0] in executables and argv[1] == '-B' and argv[3:] == [flag, str(binding)],
                 'old local component invocation or domain binding changed')
        original = Path(argv[2])
        old = original.parents[len(Path(script).parts) - 1]
        _require(original == old / script and old.parent == OLD_RELEASES and HEX.fullmatch(old.name),
                 'old local script is outside the frozen native release')
        old_releases.add(old)
        replacement = release / script
        _root_metadata(replacement)
        original_toml = json.dumps(argv[2]).encode('utf-8')
        _require(raw.count(original_toml) == 1, 'local component path is not an exact immutable TOML value')
        argv[2] = str(replacement)
        raw = raw.replace(original_toml, json.dumps(argv[2]).encode('utf-8'), 1)
    _require(len(old_releases) == 1 and tomllib.loads(raw.decode('utf-8')) == expected,
             'retargeted local configuration changes unrelated fields')
    return raw


def _adopt_collector():
    from dataclasses import replace
    from dispatch.sa_dispatch.collector import Collector, file_identity
    from dispatch.sa_dispatch.collector_cli import load_binding

    path = CONFIG / 'collector-binding.json'
    payload = json.loads(_root_bytes(path), object_pairs_hook=_unique_fields)
    target, bindings, _ = load_binding(path)
    candidates = [entry for entry in bindings.values() if entry.kind == 'dispatch']
    _require(target == LEDGER and len(candidates) == 1, 'expected one common Dispatch Collector source')
    old = candidates[0]
    _require(Path(old.locator) == LEDGER and old.file_identity == file_identity(LEDGER)
             and old.synthetic_only is True and old.profile is None and '0012' in old.accepted_schema_versions,
             'old Collector source identity or baseline profile changed')
    new = replace(old, accepted_schema_versions=('0013',), data_class='local_only',
                  synthetic_only=False, profile='common_metadata_0013')
    payload['schema'] = 'sa_dispatch.collector_binding.v2'
    for source in payload['sources']:
        if source['source_id'] == old.source_id:
            source.update(accepted_schema_versions=['0013'], data_class='local_only',
                          synthetic_only=False, profile='common_metadata_0013')
    def cursors():
        with closing(sqlite3.connect(LEDGER.as_uri() + '?mode=ro', uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            return {row['stream']: dict(row) for row in conn.execute(
                'SELECT * FROM collector_cursors WHERE source_id=?', (old.source_id,))}
    before = cursors()
    _require(set(before) == set(STREAMS), 'common Collector cursors are missing or unexpected')
    collector = Collector(target)
    for stream in STREAMS:
        current = before[stream]
        old.validate(stream)
        new.validate(stream)
        _require(current['schema_version'] == '0012' and current['source_instance'] == old.source_instance
                 and current['binding_sha256'] == old.fingerprint() and current['authority'] == 'local_owned',
                 'old Collector cursor is outside the reviewed baseline')
        frozen = collector.sync(old, stream)
        _require((frozen.get('state'), frozen.get('kind'), frozen.get('last_seq'))
                 == ('frozen', 'schema_changed', current['last_seq']),
                 'Collector did not freeze at the original schema anchor')
        collector.adopt(new, stream, reason='accept_schema', actor='common-agent-upgrade',
                        resume_after=current['last_seq'])
    after = cursors()
    adopted = {}
    for stream, row in after.items():
        prior = before[stream]
        _require(row['schema_version'] == '0013' and row['binding_sha256'] == new.fingerprint()
                 and row['epoch'] == prior['epoch'] + 1 and all(row[key] == prior[key] for key in (
                     'last_seq', 'anchor_sha256', 'source_instance', 'authority', 'redacted_count')),
                 'Collector adoption shifted the original anchor')
        adopted[stream] = {key: row[key] for key in ('epoch', 'last_seq', 'anchor_sha256')}
    metadata = _root_metadata(path)
    _atomic(path, _json(payload), mode=stat.S_IMODE(metadata.st_mode), gid=metadata.st_gid)
    return {'source_id': old.source_id, 'source_instance': old.source_instance, 'streams': adopted}


def _retarget_manager(collector):
    from tools.m0_manager.binding import load_binding

    path = CONFIG / 'manager-binding.json'
    metadata = _root_metadata(path)
    payload, _ = load_binding(path)
    sources = [source for source in payload['sources'] if source['kind'] == 'dispatch']
    info = LEDGER.stat()
    identity = {'device': info.st_dev, 'inode': info.st_ino}
    _require(len(sources) == 1, 'expected one common Dispatch manager source')
    source = sources[0]
    _require(source['locator'] == str(LEDGER) and source['accepted_schema_versions'] == ['0012']
             and source['source_id'] == collector['source_id'] and source['source_instance'] == collector['source_instance']
             and source.get('options', {}).get('identity') == identity
             and 'profile' not in source['options'], 'old manager Dispatch source identity or profile changed')
    source.update(accepted_schema_versions=['0013'], **{'class': 'local_only'})
    source['options']['profile'] = 'common_metadata_0013'
    _atomic(path, _json(payload), mode=stat.S_IMODE(metadata.st_mode), gid=metadata.st_gid)


def _recover(name, expected_version):
    from tools.m0_agent.common_recovery import RecoveryLayout, capture, restore_to_quarantine

    archive = AUTHORITY / name
    receipt = capture(RecoveryLayout(), archive, offline=True)
    _require(receipt['ledger_version'] == expected_version, 'recovery ledger does not match the planned schema')
    quarantine = AUTHORITY / ('quarantine-' + name)
    restored = restore_to_quarantine(archive, quarantine, expected_manifest_sha256=receipt['manifest_sha256'])
    _require(restored['restore_verified'] is True and restored['external_execution_enabled'] is False,
             'cold recovery did not verify in the offline quarantine')
    return {'manifest_sha256': receipt['manifest_sha256'], 'restore_verified': True,
            'ledger_version': expected_version}


class _Installer:
    def __init__(self, release, machine, manifest):
        self.release, self.machine, self.manifest = release, machine, manifest
        self.state_file = AUTHORITY / 'state.json'

    def _base(self):
        return {'schema': STATE_SCHEMA, 'manifest_sha256': self.manifest, 'machine_id': self.machine}

    def _read_state(self):
        _root_metadata(AUTHORITY, directory=True, private=True)
        document = json.loads(_root_bytes(self.state_file, private=True), object_pairs_hook=_unique_fields)
        _require(isinstance(document, dict) and all(document.get(key) == value for key, value in self._base().items()),
                 'addon receipt belongs to a different manifest or machine')
        return document

    def _save(self, data, phase, **extra):
        record = {**data, **self._base(), **extra, 'phase': phase}
        _atomic(self.state_file, _json(record), mode=0o600)
        return record

    def _verify_installed(self, data):
        _require(_check_schema() == ('0013', data['ledger_identity']), 'installed common ledger identity changed')
        _require(_config_hashes(BASE_CONFIGS) == data['base_config_hashes'], 'base configuration drift blocks startup')
        checked = self._managed_files()
        _require(all(_root_hash(path) == checksum for path, checksum in checked.items()),
                 'guarded service or launcher bytes changed')
        _require(set(DROPINS.iterdir()) == {DROPIN, *OLD_SERVICES[1:3]}, 'common HTTP unit drop-ins changed')
        old_hashes = {path.name: _root_hash(path) for path in OLD_SERVICES[:3]}
        _require(old_hashes == data['original_unit_hashes'], 'underlying common unit or original drop-ins changed')
        _require(_root_hash(GUARD) == data['old_guard_closed_sha256'], 'old native start gate is open or changed')
        _require(data['pre_recovery']['restore_verified'] is True, 'prior common recovery was not tested')
        uid, group = _accounts()
        for path in (LEDGER.parent, CAS, LEDGER):
            directory = path != LEDGER
            info = _valid_share(path, uid, directory=directory)
            _require(info.st_gid == group and stat.S_IMODE(info.st_mode) == (0o2770 if directory else 0o660),
                     'common ledger or CAS is no longer shared through the dedicated group')

    def _managed_files(self):
        intended = _unit_bytes(self.release, self.machine, self.manifest)
        return {path: sha256(raw).hexdigest() for path, (raw, _) in intended.items()}

    def install(self):
        from tools.m0_deploy.install_target import validate_install_target
        from tools.m0_native_mixed.maintenance import _lock, guard_check
        from tools.m0_native_mixed.native_adapter import directory_lock
        from dispatch.sa_dispatch.external_migration import upgrade

        _root_metadata(MAINTENANCE_LOCK)
        with _lock(MAINTENANCE_LOCK):
            if self.state_file.exists():
                state = self._read_state()
                _require(state['phase'] in ('prepared', 'ready'),
                         'interrupted upgrade; preserve its receipt and review offline')
                self._verify_installed(state)
                return state
            _require(not AUTHORITY.exists() and not AUTHORITY.is_symlink(), 'unrecognized addon maintenance state')
            version, identity = _check_schema()
            _require(version == '0012', 'initial addon registration requires common 0012')
            target = json.loads(_root_bytes(CONFIG / 'install-target.json'), object_pairs_hook=_unique_fields)
            validate_install_target(target['manifest_sha256'])
            _require(guard_check(CONFIG)['phase'] == 'completed', 'old common activation is incomplete')
            _require(set(DROPINS.iterdir()) == set(OLD_SERVICES[1:3]), 'unrecognized HTTP service drop-in')
            _require(all(not path.exists() and not path.is_symlink() for path in (SYSTEMD / AGENT, SYSTEMD / TIMER)),
                     'common agent systemd entry already exists')
            _verify_old_http_fragment()
            local = CONFIG / 'local.toml'
            local_info = _root_metadata(local)
            local_raw = _retarget_local(_root_bytes(local), self.release)
            prior = [(file, _root_bytes(file), stat.S_IMODE(file.stat().st_mode)) for file in (*OLD_SERVICES, GUARD)]
            AUTHORITY.mkdir(mode=0o700)
            _sync_parent(AUTHORITY.parent)
            data = self._save({}, 'installing', ledger_identity=identity)
            service_archive = AUTHORITY / 'original-service'
            service_archive.mkdir(mode=0o700)
            for file, content, _ in prior:
                _atomic(service_archive / file.name, content, mode=0o600)
            _atomic(service_archive / 'locations.json', _json({
                file.name: {'path': str(file), 'mode': mode} for file, _, mode in prior
            }), mode=0o600)
            _stop_writers()
            _atomic(GUARD, _json({'phase': 'closed_for_common_external_upgrade'}), mode=0o600)
            pre = _recover('pre-0013', '0012')
            _verify_inactive()
            with directory_lock(LEDGER.parent):
                migration = upgrade(LEDGER, AUTHORITY / 'sqlite-upgrade', offline=True)
                _require(migration['schema_version'] == '0013' and migration['recovery_verified'] is True,
                         'ledger migration did not verify')
                collector = _adopt_collector()
                _atomic(local, local_raw, mode=stat.S_IMODE(local_info.st_mode), gid=local_info.st_gid)
                _retarget_manager(collector)
                _share_ledger()
                for path, (content, mode) in _unit_bytes(self.release, self.machine, self.manifest).items():
                    _atomic(path, content, mode=mode)
                _systemctl('daemon-reload')
                _verify_inactive()
                data = self._save(data, 'prepared', pre_recovery=pre, migration=migration, collector=collector,
                                  base_config_hashes=_config_hashes(BASE_CONFIGS),
                                  original_unit_hashes={path.name: sha256(raw).hexdigest() for path, raw, _ in prior
                                                        if path in OLD_SERVICES[:3]},
                                  old_guard_closed_sha256=_root_hash(GUARD))
            self._verify_installed(data)
            return data

    def seal(self):
        from sa_dispatch.artifacts import ArtifactStore
        from tools.m0_agent import common_runtime, common_worker
        from tools.m0_native_mixed.maintenance import _lock

        with _lock(MAINTENANCE_LOCK):
            data = self._read_state()
            if data['phase'] == 'ready':
                data = self.guard()
            else:
                _require(data['phase'] == 'prepared', 'common addon is not prepared or seal was interrupted')
                self._verify_installed(data)
                _verify_inactive()
                if _property(TIMER, 'UnitFileState'):
                    _require(_property(TIMER, 'UnitFileState') == 'disabled', 'external timer is already enabled')
                agent_hashes = _config_hashes(AGENT_CONFIGS)
                grant = common_runtime.read_config(CONFIG / 'agent-external.json')
                common_runtime.read_provider_network(CONFIG / 'provider-network.json')
                _require(grant.machine_id == self.machine, 'registered grant is bound to a different machine')
                status = common_worker.status(LEDGER, ArtifactStore(CAS), grant)
                _require(status['status'] in ('scheduled', 'queued') and not status['grant_expired']
                         and status['attempts'] == status['request_units_used'] == status['request_units_held'] == 0
                         and (status['status'] != 'queued' or status['approval_ref'] is not None),
                         'unspent registered grant is required before sealing')
                data = self._save(data, 'sealing', agent_config_hashes=agent_hashes)
                post = _recover('pre-live-0013', '0013')
                _require(agent_hashes == _config_hashes(AGENT_CONFIGS), 'agent configuration changed during recovery capture')
                data = self._save(data, 'ready', pre_live_recovery=post)
            self.guard()
            _systemctl('start', HTTP)
            return data

    def guard(self):
        data = self._read_state()
        _require(data['phase'] == 'ready' and data['pre_live_recovery']['restore_verified'] is True,
                 'common external restart guard is closed')
        self._verify_installed(data)
        _require(_config_hashes(AGENT_CONFIGS) == data['agent_config_hashes'],
                 'agent grant or provider network configuration changed')
        return data

    def enable_timer(self):
        from tools.m0_native_mixed.maintenance import _lock

        with _lock(MAINTENANCE_LOCK):
            data = self.guard()
            _require(_property(HTTP, 'ActiveState') == 'active', 'HTTP coordinator needs to be active first')
            _systemctl('enable', '--now', TIMER)
            _require(_property(TIMER, 'ActiveState') == 'active', 'agent timer could not be activated')
            return data | {'timer_enabled': True}

    def status(self):
        data = self._read_state()
        return self.guard() if data['phase'] == 'ready' else data

    def local_run(self, rest):
        from tools.m0_native_mixed.native_adapter import directory_lock

        _require(rest and all(arg != '--config' and not arg.startswith('--config=') for arg in rest),
                 'local route requires its fixed reviewed configuration')
        with directory_lock(LEDGER.parent, shared=True):
            self.guard()
            command = [
                '/usr/bin/systemd-run', '--quiet', '--wait', '--pipe', '--collect', '--unit=' + LOCAL,
                '--property=User=sa-dispatch', '--property=Group=sa-dispatch', '--property=SupplementaryGroups=' + SHARED,
                '--property=NoNewPrivileges=yes', '--property=ProtectSystem=strict', '--property=PrivateTmp=yes',
                '--property=UMask=0007',
                '--property=WorkingDirectory=' + str(self.release),
                '--property=ReadWritePaths=' + str(LEDGER.parent) + ' ' + str(DOMAIN),
                '--setenv=PYTHONPATH=' + str(self.release / 'dispatch'),
                '/usr/bin/flock', '--shared', '--nonblock', '--no-fork', str(LEDGER.parent),
                '/usr/bin/python3', '-B', str(self.release / 'knowledge/m0_content/dispatch_entry.py'),
                '--config', str(CONFIG / 'local.toml'), *rest,
            ]
            return subprocess.run(command, env=ENV, cwd=self.release, check=False).returncode


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('command', choices=('install', 'seal', 'guard', 'status', 'enable-timer', 'local-run'))
    parser.add_argument('--expected-machine', required=True)
    parser.add_argument('--manifest-sha256', required=True)
    args, rest = parser.parse_known_args(argv)
    if args.command != 'local-run' and rest:
        parser.error('unexpected arguments')
    try:
        _require(sys.platform == 'linux' and os.geteuid() == 0 and Path('/run/systemd/system').is_dir(),
                 'common addon requires Linux root under systemd')
        _require(re.fullmatch(r'[0-9a-f]{32}', args.expected_machine) and HEX.fullmatch(args.manifest_sha256),
                 'explicit machine ID and release manifest digest are required')
        release = RELEASES / args.manifest_sha256
        _require(Path(__file__).absolute() == release / ENTRY, 'addon must run from the selected frozen release')
        _source_files(release, args.manifest_sha256)
        _root_parents(AUTHORITY)
        _root_parents(CONFIG / 'install-target.json')
        _root_parents(DROPIN)
        sys.dont_write_bytecode = True
        sys.path[:0] = [str(release / 'dispatch'), str(release)]
        from tools.m0_deploy.install_target import observe_install_target

        machine, _ = observe_install_target()
        _require(machine == args.expected_machine, 'explicit machine ID differs from the Linux installation')
        installer = _Installer(release, machine, args.manifest_sha256)
        if args.command == 'local-run':
            return installer.local_run(rest[1:] if rest[:1] == ['--'] else rest)
        command = args.command.replace('-', '_')
        result = getattr(installer, command)()
    except Exception as error:
        result = {'ok': False, 'operation': args.command, 'error': type(error).__name__}
        if isinstance(error, InstallError):
            result['detail'] = str(error)
        print(json.dumps(result, sort_keys=True))
        return 1
    print(json.dumps({'ok': True, **result}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
