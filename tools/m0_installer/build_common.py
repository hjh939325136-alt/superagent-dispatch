"""Build and verify the bounded common synthetic source package."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PureWindowsPath
import re
import stat
import subprocess
import tempfile
import zipfile


SCHEMA = 'superagent.common.package.v1'
SCOPE = 'synthetic_research_content_single_ledger'
MANIFEST = 'INSTALL_MANIFEST.json'
PAYLOAD = 'payload/'
LICENSE = 'dispatch/LICENSE'
INSTALLER = 'tools/m0_installer/common.py'
VERIFIER_SOURCES = frozenset({'tools/m0_installer/__init__.py', 'tools/m0_installer/build_common.py'})
BASELINE_SOURCES = frozenset({'tools/m0_installer/common_baseline.py'})
INSTALLER_SUPPORT = BASELINE_SOURCES | frozenset({'tools/m0_native_mixed/common_entry.py'})
MAX_FILES = 512
MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
MAX_MANIFEST_BYTES = 256 * 1024
MAX_ARCHIVE_BYTES = 18 * 1024 * 1024
HASH = re.compile(r'[0-9a-f]{64}')
PART = re.compile(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}')

# Match domains.pin_source_files and native_adapter.source_files without
# importing deployment modules or traversing local state in the source trees.
DOMAIN_TREES = ('dispatch/sa_dispatch', 'dispatch/sa_dispatch_hotspot',
                'knowledge/m0_search', 'knowledge/m0_content',
                'tools/m0_manager', 'tools/m0_backup')
NATIVE_TREES = ('tools/m0_native_mixed', 'tools/m0_deploy')
FIXTURES = frozenset({
    'knowledge/m0_search/fixtures/grants.json',
    'knowledge/m0_search/fixtures/research-scatter.json',
    'knowledge/m0_search/fixtures/research-content.json',
    'knowledge/m0_search/fixtures/signalhub.json',
    'knowledge/m0_content/fixture_authorization.json',
    'knowledge/m0_search/fixtures/search-request.json',
    'knowledge/m0_search/fixtures/three-source-request.json',
    'dispatch/fixtures/synthetic/topic_supply.json',
})
REQUIRED = frozenset({
    'dispatch/sa_dispatch/config.py', 'dispatch/sa_dispatch/schema.sql',
    'dispatch/sa_dispatch/collector_cli.py', 'dispatch/sa_dispatch/scheduler.py',
    'dispatch/sa_dispatch_hotspot/__init__.py',
    'knowledge/m0_search/dispatch_component.py', 'knowledge/m0_search/dispatch_entry.py',
    'knowledge/m0_content/dispatch_component.py', 'knowledge/m0_content/dispatch_entry.py',
    'tools/m0_native_mixed/native_adapter.py', 'tools/m0_native_mixed/domains.py',
    'tools/m0_native_mixed/bootstrap.py', 'tools/m0_native_mixed/maintenance.py',
    'tools/m0_deploy/deploy.py', 'tools/m0_deploy/install_target.py', 'tools/m0_manager/__main__.py',
    'tools/m0_manager/binding.py', 'tools/m0_backup/backup.py',
}) | FIXTURES | VERIFIER_SOURCES


class PackageError(ValueError):
    """Source or archive falls outside the package contract."""


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _json(document):
    return json.dumps(document, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def _source_allowed(name, include_installer):
    if name in FIXTURES or name in VERIFIER_SOURCES or name in BASELINE_SOURCES or (include_installer and name == INSTALLER):
        return True
    if '__pycache__' in name.split('/'):
        return False
    return ((name.endswith(('.py', '.sql')) and name.startswith(tuple(tree + '/' for tree in DOMAIN_TREES)))
            or (name.endswith('.py') and name.startswith(tuple(tree + '/' for tree in NATIVE_TREES))))


def _required_paths(include_installer):
    needed = REQUIRED | INSTALLER_SUPPORT | {INSTALLER} if include_installer else REQUIRED
    return {PAYLOAD + name for name in needed} | {PAYLOAD + 'LICENSE'}


def _archive_path(root, name):
    if len(name) > 512 or any(not PART.fullmatch(part) or part.endswith('.')
                             or PureWindowsPath(part).is_reserved() for part in name.split('/')):
        raise PackageError('Invalid package path')
    return root.joinpath(*name.split('/'))


def _unique_names(names):
    folded = {}
    files = set()
    dirs = set()
    for name in names:
        key = name.lower()
        if key in files or key in dirs:
            raise PackageError('Duplicate or conflicting package path')
        files.add(key)
        parts = name.split('/')
        for length in range(1, len(parts) + 1):
            prefix = '/'.join(parts[:length])
            prefix_key = prefix.lower()
            if folded.setdefault(prefix_key, prefix) != prefix:
                raise PackageError('Case-conflicting package path')
            if length < len(parts):
                if prefix_key in files:
                    raise PackageError('File used as a package directory')
                dirs.add(prefix_key)


def _regular_path(path, *, directory=False):
    if '..' in path.parts or str(path).startswith(('\\\\', '//')):
        raise PackageError('A local path without traversal is required')
    for part in (*reversed(path.parents), path):
        info = part.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise PackageError('Links and reparse points are forbidden')
        if part != path or directory:
            if not stat.S_ISDIR(info.st_mode):
                raise PackageError('Expected a directory')
        elif not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise PackageError('Expected a singly linked regular file')
    return info


def _identity(info):
    return info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _read(path, maximum):
    before = _regular_path(path)
    if before.st_size > maximum:
        raise PackageError('Package file exceeds size limit')
    flags = os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
    with os.fdopen(os.open(path, flags), 'rb') as stream:
        if _identity(os.fstat(stream.fileno())) != _identity(before):
            raise PackageError('Package file changed before reading')
        raw = stream.read(maximum + 1)
        if len(raw) != before.st_size or _identity(os.fstat(stream.fileno())) != _identity(before):
            raise PackageError('Package file changed during reading')
    if _identity(_regular_path(path)) != _identity(before):
        raise PackageError('Package file changed after reading')
    return raw


def _git(root, *args):
    # Explicit -C and an unmodified index location bind the file list to root.
    env = {key: value for key, value in os.environ.items() if not key.upper().startswith('GIT_')}
    try:
        return subprocess.run(['git', '-C', str(root), *args], env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.DEVNULL, check=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError) as error:
        raise PackageError('Cannot read the source checkout Git index') from error


def _source_paths(root, include_installer):
    _regular_path(root, directory=True)
    if _git(root, 'rev-parse', '--show-prefix').strip():
        raise PackageError('Source root must be the top of its Git checkout')
    tracked = _git(root, 'ls-files', '--stage', '-z', '--', *DOMAIN_TREES, *NATIVE_TREES,
                   *sorted(FIXTURES | VERIFIER_SOURCES | BASELINE_SOURCES | {LICENSE, INSTALLER}))
    sources = {}
    for record in tracked.split(b'\0'):
        if not record:
            continue
        metadata, raw_name = record.split(b'\t', 1)
        name = raw_name.decode('utf-8', errors='surrogateescape')
        if name != LICENSE and not _source_allowed(name, include_installer):
            continue
        mode, _, stage = metadata.split()
        if mode not in {b'100644', b'100755'} or stage != b'0':
            raise PackageError('Package source must be a tracked regular file without merge conflicts')
        archive_name = PAYLOAD + ('LICENSE' if name == LICENSE else name)
        if archive_name in sources:
            raise PackageError('Duplicate tracked package path')
        sources[archive_name] = _archive_path(root, name)
    _unique_names(sources)
    if not _required_paths(include_installer) <= sources.keys():
        raise PackageError('Required common source files are missing from the Git index')
    if len(sources) > MAX_FILES:
        raise PackageError('Package entry count exceeds limit')
    return sources


def _zip_bytes(payload, manifest):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w', compression=zipfile.ZIP_STORED, allowZip64=False) as archive:
        for name, data in [*sorted(payload.items()), (MANIFEST, manifest)]:
            entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.create_system = 3
            entry.external_attr = (stat.S_IFREG | 0o644) << 16
            archive.writestr(entry, data)
    return stream.getvalue()


def _manifest(payload, include_installer):
    return {'schema': SCHEMA, 'scope': SCOPE,
            'distribution': 'installer_candidate' if include_installer else 'source_candidate',
            'installer_entry': PAYLOAD + INSTALLER if include_installer else None,
            'files': {name: {'sha256': _sha(data), 'bytes': len(data)} for name, data in payload.items()}}


def _result(path, zip_raw, manifest_raw, manifest):
    return {'path': str(path), 'manifest_sha256': _sha(manifest_raw), 'zip_sha256': _sha(zip_raw),
            'files': len(manifest['files']), 'bytes': len(zip_raw),
            'payload_bytes': sum(value['bytes'] for value in manifest['files'].values()), 'manifest': manifest}


def _ensure_directory(directory):
    try:
        _regular_path(directory, directory=True)
    except FileNotFoundError:
        _ensure_directory(directory.parent)
        directory.mkdir(exist_ok=True)
        _regular_path(directory, directory=True)


def _publish_archive(path, raw):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.common-', delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(raw)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if _read(path, MAX_ARCHIVE_BYTES) != raw:
                raise PackageError('Destination already contains a different archive') from None
    finally:
        if temporary is not None:
            temporary.unlink()


def build(root, destination, *, include_installer=False):
    """Build a deterministic ZIP from explicitly scoped tracked working-tree files."""
    sources = _source_paths(Path(root).absolute(), include_installer)
    payload = {}
    available = MAX_PAYLOAD_BYTES
    for name, source in sorted(sources.items()):
        blob = _read(source, min(available, MAX_FILE_BYTES))
        payload[name] = blob
        available -= len(blob)
    if not payload[PAYLOAD + 'LICENSE'].startswith(b'MIT License'):
        raise PackageError('Expected the Dispatch MIT License')
    manifest = _manifest(payload, include_installer)
    manifest_raw = _json(manifest)
    if len(manifest_raw) > MAX_MANIFEST_BYTES:
        raise PackageError('Manifest exceeds size limit')
    zip_raw = _zip_bytes(payload, manifest_raw)
    if len(zip_raw) > MAX_ARCHIVE_BYTES:
        raise PackageError('Archive exceeds size limit')
    directory = Path(destination).absolute()
    _ensure_directory(directory)
    path = directory / f'superagent-common-{_sha(manifest_raw)[:12]}.zip'
    _publish_archive(path, zip_raw)
    return _result(path, zip_raw, manifest_raw, manifest)


def _entry_list(archive):
    entries = archive.infolist()
    if len(entries) > MAX_FILES + 1:
        raise PackageError('Archive entry count exceeds limit')
    for entry in entries:
        name = entry.orig_filename
        _archive_path(Path('.'), name)
        if (name != entry.filename or entry.flag_bits or entry.is_dir()
                or not stat.S_ISREG(entry.external_attr >> 16)
                or entry.compress_type != zipfile.ZIP_STORED or entry.compress_size != entry.file_size):
            raise PackageError('Archive contains an unsupported ZIP entry')
        maximum = MAX_MANIFEST_BYTES if name == MANIFEST else MAX_FILE_BYTES
        if not 0 <= entry.file_size <= maximum:
            raise PackageError('Archive entry exceeds size limit')
    names = [entry.filename for entry in entries]
    _unique_names(names)
    by_name = {entry.filename: entry for entry in entries}
    if MANIFEST not in by_name or sum(entry.file_size for entry in entries if entry.filename != MANIFEST) > MAX_PAYLOAD_BYTES:
        raise PackageError('Missing manifest or archive payload exceeds size limit')
    return by_name


def _load_manifest(raw, entries):
    manifest = json.loads(raw)
    if (not isinstance(manifest, dict) or set(manifest) != {'schema', 'scope', 'distribution', 'installer_entry', 'files'}
            or manifest['schema'] != SCHEMA or manifest['scope'] != SCOPE or _json(manifest) != raw):
        raise PackageError('Invalid or noncanonical package manifest')
    distribution = manifest['distribution']
    include_installer = distribution == 'installer_candidate'
    if distribution not in ('source_candidate', 'installer_candidate'):
        raise PackageError('Invalid package distribution')
    if manifest['installer_entry'] != (PAYLOAD + INSTALLER if include_installer else None):
        raise PackageError('Invalid package installer entry')
    files = manifest['files']
    if (not isinstance(files, dict) or set(files) != set(entries) - {MANIFEST}
            or not _required_paths(include_installer) <= files.keys()):
        raise PackageError('Archive and manifest file sets differ or required files are missing')
    for name, record in files.items():
        if (not name.startswith(PAYLOAD) or (name != PAYLOAD + 'LICENSE'
                and not _source_allowed(name.removeprefix(PAYLOAD), include_installer))):
            raise PackageError('Archive path is outside the common package source scope')
        if (not isinstance(record, dict) or set(record) != {'sha256', 'bytes'}
                or type(record['bytes']) is not int or record['bytes'] != entries[name].file_size
                or not isinstance(record['sha256'], str) or not HASH.fullmatch(record['sha256'])):
            raise PackageError('Invalid manifest file record')
    return manifest


def verify_archive(path, *, expected_manifest_sha256=None, expected_zip_sha256=None):
    """Verify the ZIP in memory without extracting or executing its entries."""
    path = Path(path).absolute()
    try:
        zip_raw = _read(path, MAX_ARCHIVE_BYTES)
        if expected_zip_sha256 is not None and _sha(zip_raw) != expected_zip_sha256:
            raise PackageError('Archive SHA256 mismatch')
        with zipfile.ZipFile(io.BytesIO(zip_raw)) as archive:
            entries = _entry_list(archive)
            manifest_raw = archive.read(entries[MANIFEST])
            manifest_sha = _sha(manifest_raw)
            if expected_manifest_sha256 is not None and manifest_sha != expected_manifest_sha256:
                raise PackageError('Manifest SHA256 mismatch')
            if path.name != f'superagent-common-{manifest_sha[:12]}.zip':
                raise PackageError('Archive filename does not match the manifest digest')
            manifest = _load_manifest(manifest_raw, entries)
            payload = {}
            for name, record in manifest['files'].items():
                data = archive.read(entries[name])
                if len(data) != record['bytes'] or _sha(data) != record['sha256']:
                    raise PackageError('Payload size or SHA256 mismatch')
                payload[name] = data
            if not payload[PAYLOAD + 'LICENSE'].startswith(b'MIT License'):
                raise PackageError('Expected the Dispatch MIT License')
            if _zip_bytes(payload, manifest_raw) != zip_raw:
                raise PackageError('Archive does not have the canonical ZIP representation')
    except PackageError:
        raise
    except (OSError, zipfile.BadZipFile, ValueError, RecursionError) as error:
        raise PackageError('Invalid or unreadable common package') from error
    return _result(path, zip_raw, manifest_raw, manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    builder = commands.add_parser('build')
    builder.add_argument('--root', type=Path, default=Path(__file__).absolute().parents[2])
    builder.add_argument('--destination', type=Path, required=True)
    builder.add_argument('--include-installer', action='store_true')
    verifier = commands.add_parser('verify')
    verifier.add_argument('archive', type=Path)
    verifier.add_argument('--manifest-sha256')
    verifier.add_argument('--zip-sha256')
    args = parser.parse_args()
    try:
        if args.command == 'build':
            result = build(args.root, args.destination, include_installer=args.include_installer)
        else:
            result = verify_archive(args.archive, expected_manifest_sha256=args.manifest_sha256,
                                    expected_zip_sha256=args.zip_sha256)
    except (OSError, PackageError) as error:
        parser.exit(1, f'{error}\n')
    print(json.dumps({key: value for key, value in result.items() if key != 'manifest'}, sort_keys=True))


if __name__ == '__main__':
    main()
