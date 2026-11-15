"""Local recovery configuration for the isolated synthetic research workflow."""
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import stat


MANIFEST_SCHEMA = 'm0.synthetic_domain_recovery.v1'
ENVELOPE_SCHEMA = 'm0.synthetic_recovery_binding.v1'
MANIFEST_NAME = 'recovery-manifest.json'
HISTORICAL_READ_ONLY = 'historical_read_only'
REHEARSAL_LOOKUP_ONLY = 'synthetic_rehearsal_lookup_only'
MODES = {HISTORICAL_READ_ONLY, REHEARSAL_LOOKUP_ONLY}
ROLES = {
    'research': {'catalog_root': 'catalog'},
    'workflow': {'research_binding': 'research-binding.json', 'content_db': 'content.sqlite3'},
}
CATALOG_FILES = ('catalog/catalog.json', 'catalog/catalog.sqlite3',
                 'catalog/research-scatter.sqlite3', 'catalog/research-content.sqlite3',
                 'catalog/signalhub.sqlite3')
ORIGINAL_FILES = {role: f'original-{role}-binding.json' for role in ROLES}
PINNED_FILES = (*CATALOG_FILES, 'content.sqlite3', 'dispatch.sqlite3')
REQUIRED_FILES = frozenset((*ORIGINAL_FILES.values(), *PINNED_FILES))
ARTIFACT = re.compile(r'artifacts/sha256/([0-9a-f]{2})/([0-9a-f]{64})')
READS = frozenset({'research.search', 'research.get', 'research.context_get', 'content.approval_status'})
HASH = re.compile('[0-9a-f]{64}')
HASH_REF = re.compile('sha256:[0-9a-f]{64}')
IDENTITY = re.compile('[A-Za-z0-9][A-Za-z0-9_.:-]{0,120}')
META_LIMIT = 65536
MANIFEST_LIMIT = 8 * 1024 * 1024


class RecoveryBindingError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class RecoveryAdmission:
    mode: str
    allowed_unknown_task_ids: tuple[str, ...]
    manifest_sha256: str
    catalog_instance: str
    content_instance: str
    revocation_basis: str = 'captured_snapshot'
    post_snapshot_revocations: str = 'unverified'

    def provenance(self):
        return {'revocation_basis': self.revocation_basis,
                'post_snapshot_revocations': self.post_snapshot_revocations}


@dataclass(frozen=True)
class RecoveryBinding:
    original: dict
    recovery_digest: str
    admission: RecoveryAdmission
    role_paths: dict[str, Path]
    file_identities: dict[str, dict[str, int]]


def require_recovery_operation(binding, operation, task_id=None):
    admission = getattr(binding, 'recovery_admission', None)
    if admission is None and getattr(binding, 'recovery_digest', None) is None:
        return
    if operation in READS:
        return
    if operation != 'operation.lookup':
        raise RecoveryBindingError('RECOVERY_DOMAIN_WRITE_DENIED')
    if (admission is None or admission.mode != REHEARSAL_LOOKUP_ONLY or
            task_id not in admission.allowed_unknown_task_ids):
        raise RecoveryBindingError('RECOVERY_LOOKUP_NOT_ADMITTED')


def lookup_task_id(operation, write_key):
    """Cross-check lower-level lookups with the dispatcher write-key convention."""
    suffix = ':' + operation + '@1'
    return write_key.removesuffix(suffix) if write_key.endswith(suffix) else None


def historical_path(value):
    """Validate archived path syntax without consulting any physical path."""
    if not isinstance(value, str) or '\x00' in value or value.startswith(('//', '\\\\')):
        return False
    path = PureWindowsPath(value) if re.match(r'^[A-Za-z]:', value) else PurePosixPath(value)
    return path.is_absolute() and '..' not in path.parts


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def _digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _matches(expression, value):
    return isinstance(value, str) and expression.fullmatch(value) is not None


def _stat(path, *, directory=False):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts or str(path).startswith(('\\\\', '//')):
        raise RecoveryBindingError('INVALID_RECOVERY_PATH')
    for part in (*reversed(path.parents), path):
        info = part.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise RecoveryBindingError('UNSAFE_RECOVERY_PATH')
    if directory:
        if not stat.S_ISDIR(info.st_mode):
            raise RecoveryBindingError('INVALID_RECOVERY_DIRECTORY')
    elif not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise RecoveryBindingError('UNSAFE_RECOVERY_PATH')
    return info


def _identity(info):
    return {'device': info.st_dev, 'inode': info.st_ino}


def _read(path, limit=None):
    before = _stat(path)
    if limit is not None and before.st_size > limit:
        raise RecoveryBindingError('RECOVERY_METADATA_TOO_LARGE')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        if _identity(before) != _identity(opened) or opened.st_nlink != 1:
            raise RecoveryBindingError('RECOVERY_FILE_CHANGED')
        if limit is None:
            digest = hashlib.sha256()
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(block)
            result = digest.hexdigest()
        else:
            result = stream.read(limit + 1)
            if len(result) > limit:
                raise RecoveryBindingError('RECOVERY_METADATA_TOO_LARGE')
        after = os.fstat(stream.fileno())
        if (opened.st_size, opened.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise RecoveryBindingError('RECOVERY_FILE_CHANGED')
    if _identity(before) != _identity(_stat(path)):
        raise RecoveryBindingError('RECOVERY_FILE_CHANGED')
    return result, _identity(before)


def _parse(raw):
    def unique(items):
        value = dict(items)
        if len(value) != len(items):
            raise RecoveryBindingError('DUPLICATE_RECOVERY_FIELD')
        return value

    def constant(_):
        raise RecoveryBindingError('INVALID_RECOVERY_JSON')

    value = json.loads(raw, object_pairs_hook=unique, parse_constant=constant)
    if not isinstance(value, dict):
        raise RecoveryBindingError('INVALID_RECOVERY_JSON')
    return value


def _manifest(root, expected):
    if not _matches(HASH, expected):
        raise RecoveryBindingError('EXPECTED_RECOVERY_MANIFEST_REQUIRED')
    raw, _ = _read(root / MANIFEST_NAME, MANIFEST_LIMIT)
    if _digest(raw) != expected:
        raise RecoveryBindingError('RECOVERY_MANIFEST_CHANGED')
    result = _parse(raw)
    bindings = result.get('bindings')
    allowed = result.get('allowed_unknown_task_ids')
    if (result.get('schema') != MANIFEST_SCHEMA or result.get('synthetic_only') is not True or
            not isinstance(bindings, dict) or set(bindings) != {
                'research_sha256', 'workflow_sha256', 'catalog_instance', 'content_instance'} or
            any(not _matches(HASH_REF, bindings[f'{role}_sha256']) for role in ROLES) or
            any(not _matches(IDENTITY, bindings[f'{kind}_instance']) for kind in ('catalog', 'content')) or
            not isinstance(allowed, list) or any(not _matches(IDENTITY, value) for value in allowed) or
            len(set(allowed)) != len(allowed)):
        raise RecoveryBindingError('INVALID_RECOVERY_MANIFEST')
    return result


def _original(root, manifest, role):
    relative = ORIGINAL_FILES[role]
    raw, _ = _read(root / relative, META_LIMIT)
    files = manifest.get('files')
    if not isinstance(files, dict) or files.get(relative) != {'sha256': _digest(raw), 'bytes': len(raw)}:
        raise RecoveryBindingError('ORIGINAL_BINDING_CHANGED')
    value = _parse(raw)
    binding = manifest['bindings']
    if ('sha256:' + _digest(canonical(value)) != binding[f'{role}_sha256'] or
            value.get('catalog_instance') != binding['catalog_instance']):
        raise RecoveryBindingError('ORIGINAL_BINDING_CHANGED')
    if role == 'workflow' and (value.get('content_instance') != binding['content_instance'] or
                               value.get('research_binding_sha256') != binding['research_sha256']):
        raise RecoveryBindingError('ORIGINAL_BINDING_CHANGED')
    return value


def _pins(root):
    _stat(root / 'catalog', directory=True)
    pins = {name: _identity(_stat(root / name)) for name in PINNED_FILES}
    for name in (*CATALOG_FILES[1:], 'content.sqlite3'):
        for suffix in ('-wal', '-shm', '-journal'):
            try:
                (root / (name + suffix)).lstat()
            except FileNotFoundError:
                continue
            raise RecoveryBindingError('UNEXPECTED_RECOVERY_SIDECAR')
    return pins


def _admission(manifest, mode, expected):
    allowed = tuple(manifest['allowed_unknown_task_ids']) if mode == REHEARSAL_LOOKUP_ONLY else ()
    if mode == REHEARSAL_LOOKUP_ONLY and not allowed:
        raise RecoveryBindingError('RECOVERY_LOOKUP_NOT_ADMITTED')
    return RecoveryAdmission(mode, allowed, expected, manifest['bindings']['catalog_instance'],
                             manifest['bindings']['content_instance'])


def load_recovery_binding(path, role):
    path = Path(path)
    if role not in ROLES or path.name != f'{role}-binding.json':
        raise RecoveryBindingError('INVALID_RECOVERY_ROLE')
    raw, _ = _read(path, META_LIMIT)
    envelope = _parse(raw)
    if (set(envelope) != {'schema', 'role', 'locators', 'expected_manifest_sha256', 'mode', 'pins'} or
            envelope['schema'] != ENVELOPE_SCHEMA or envelope['role'] != role or
            envelope['locators'] != ROLES[role] or not isinstance(envelope['mode'], str) or
            envelope['mode'] not in MODES):
        raise RecoveryBindingError('INVALID_RECOVERY_ENVELOPE')
    root = path.parent
    expected = envelope['expected_manifest_sha256']
    manifest = _manifest(root, expected)
    originals = {name: _original(root, manifest, name) for name in ROLES}
    pins = _pins(root)
    if canonical(pins) != canonical(envelope['pins']):
        raise RecoveryBindingError('RECOVERY_FILE_CHANGED')
    admission = _admission(manifest, envelope['mode'], expected)
    common = {key: envelope[key] for key in ('schema', 'expected_manifest_sha256', 'mode', 'pins')}
    recovery_digest = 'sha256:' + _digest(canonical(common))
    paths = {key: root / value for key, value in ROLES[role].items()}
    return RecoveryBinding(originals[role], recovery_digest, admission, paths, pins)


def check_recovery_binding_current(binding, role):
    digest = getattr(binding, 'recovery_digest', None)
    if digest is None:
        return None
    current = load_recovery_binding(binding.path, role)
    paths = ({'catalog_root': binding.catalog_root} if role == 'research' else
             {'research_binding': binding.research.path, 'content_db': binding.content_path})
    if (current.recovery_digest != digest or current.admission != binding.recovery_admission or
            current.role_paths != paths):
        raise RecoveryBindingError('RECOVERY_BINDING_CHANGED')
    return current


def check_recovery_runtime_paths(root, db_path, artifact_root, journal_dir):
    if (db_path, artifact_root, journal_dir) != (root / 'dispatch.sqlite3', root / 'artifacts', root / 'journal'):
        raise RecoveryBindingError('RECOVERY_DISPATCH_PATH_MISMATCH')
    _stat(db_path)
    _stat(artifact_root, directory=True)
    sha_root = artifact_root / 'sha256'
    for entry in artifact_root.iterdir():
        if entry != sha_root:
            raise RecoveryBindingError('UNEXPECTED_RECOVERY_FILE')
        _stat(entry, directory=True)
        for prefix in entry.iterdir():
            if not re.fullmatch('[0-9a-f]{2}', prefix.name):
                raise RecoveryBindingError('UNEXPECTED_RECOVERY_FILE')
            _stat(prefix, directory=True)
            for artifact in prefix.iterdir():
                if not HASH.fullmatch(artifact.name) or not artifact.name.startswith(prefix.name):
                    raise RecoveryBindingError('UNEXPECTED_RECOVERY_FILE')
                _stat(artifact)
    for suffix in ('-wal', '-shm', '-journal'):
        try:
            _stat(db_path.with_name(db_path.name + suffix))
        except FileNotFoundError:
            continue
        if suffix != '-journal':
            raise RecoveryBindingError('UNEXPECTED_RECOVERY_SIDECAR')


def _verify_package(root, manifest):
    files = manifest.get('files')
    if not isinstance(files, dict) or not REQUIRED_FILES <= files.keys():
        raise RecoveryBindingError('INVALID_RECOVERY_FILES')
    directories = {'catalog', 'artifacts'}
    for relative, entry in files.items():
        artifact = ARTIFACT.fullmatch(relative)
        if (relative not in REQUIRED_FILES and (artifact is None or artifact[1] != artifact[2][:2]) or
                not isinstance(entry, dict) or set(entry) != {'sha256', 'bytes'} or
                not _matches(HASH, entry['sha256']) or type(entry['bytes']) is not int or entry['bytes'] < 0 or
                artifact is not None and entry['sha256'] != artifact[2]):
            raise RecoveryBindingError('INVALID_RECOVERY_FILES')
        if artifact is not None:
            directories.update({'artifacts/sha256', f'artifacts/sha256/{artifact[1]}'})
    allowed = set(files) | directories | {MANIFEST_NAME}
    for directory in ('', *sorted(directories)):
        path = root / directory
        _stat(path, directory=True)
        if any(child.relative_to(root).as_posix() not in allowed for child in path.iterdir()):
            raise RecoveryBindingError('UNEXPECTED_RECOVERY_FILE')
    verified_pins = {}
    for relative, entry in files.items():
        path = root / relative
        if _stat(path).st_size != entry['bytes']:
            raise RecoveryBindingError('RECOVERY_FILE_HASH_MISMATCH')
        actual, identity = _read(path)
        if actual != entry['sha256']:
            raise RecoveryBindingError('RECOVERY_FILE_HASH_MISMATCH')
        if relative in PINNED_FILES:
            verified_pins[relative] = identity
    if verified_pins != _pins(root):
        raise RecoveryBindingError('RECOVERY_FILE_CHANGED')
    return verified_pins


def write_recovery_bindings(root, *, expected_manifest_sha256, mode=HISTORICAL_READ_ONLY):
    """Verify the externally pinned package before creating new runtime config."""
    if not isinstance(mode, str) or mode not in MODES:
        raise RecoveryBindingError('INVALID_RECOVERY_MODE')
    root = Path(root)
    _stat(root, directory=True)
    manifest = _manifest(root, expected_manifest_sha256)
    _admission(manifest, mode, expected_manifest_sha256)
    for role in ROLES:
        _original(root, manifest, role)
    pins = _verify_package(root, manifest)
    written = []
    try:
        for role, locators in ROLES.items():
            path = root / f'{role}-binding.json'
            payload = {'schema': ENVELOPE_SCHEMA, 'role': role, 'locators': locators,
                       'expected_manifest_sha256': expected_manifest_sha256, 'mode': mode, 'pins': pins}
            with path.open('xb') as stream:
                written.append(path)
                stream.write(canonical(payload) + b'\n')
                stream.flush()
                os.fsync(stream.fileno())
        return tuple(written)
    except BaseException:
        for path in written:
            path.unlink()
        raise
