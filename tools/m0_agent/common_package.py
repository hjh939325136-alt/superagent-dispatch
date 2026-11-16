"""Build and verify a frozen source package for common external execution."""

import argparse
import io
import json
from pathlib import Path
import stat
import sys
import zipfile

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.m0_installer import build_common as baseline


SCHEMA = 'superagent.common.external.source.v1'
MANIFEST = 'SOURCE_MANIFEST.json'
LIMIT = 16 * 1024 * 1024
MAX_FILES = 512
AGENT_FILES = frozenset('tools/m0_agent/' + name + '.py' for name in (
    '__init__', 'common_install', 'common_lab', 'common_package', 'common_recovery', 'common_runtime',
    'common_worker', 'credential_channel', 'deepseek_client', 'external_preflight',
    'live_probe', 'provider_process', 'readonly_status', 'runtime', 'worker',
))
CAMPAIGN_FILES = frozenset('tools/m0_agent/' + name + '.py' for name in (
    'campaign_control', 'campaign_install', 'campaign_runtime', 'campaign_worker',
)) | {'dispatch/sa_dispatch/campaign_validation.py'}
COLLECTOR_FILES = frozenset(('tools/m0_agent/collector_install.py', 'tools/m0_agent/collector_runtime.py'))
BASE_ADDITIONS = AGENT_FILES | {'dispatch/sa_dispatch/agent_control.py', 'dispatch/sa_dispatch/external_migration.py',
                                'tools/m0_installer/credentials.py', 'tools/m0_installer/local_secret.py'}
ADDITIONS = BASE_ADDITIONS | CAMPAIGN_FILES | COLLECTOR_FILES
LEGACY_REQUIRED = (baseline.REQUIRED | baseline.INSTALLER_SUPPORT | {baseline.INSTALLER, baseline.LICENSE}
                   | BASE_ADDITIONS)
REQUIRED = LEGACY_REQUIRED | CAMPAIGN_FILES | COLLECTOR_FILES
PackageError = baseline.PackageError


def _allowed(name):
    return name == baseline.LICENSE or name in ADDITIONS or baseline._source_allowed(name, True)


def _name_set(names):
    members = set(names)
    if len(members) > MAX_FILES or not LEGACY_REQUIRED <= members:
        raise PackageError('Required common external source members are missing or excessive')
    if members & CAMPAIGN_FILES and not CAMPAIGN_FILES <= members:
        raise PackageError('Campaign source members must form a complete addon')
    if members & COLLECTOR_FILES and not (COLLECTOR_FILES | CAMPAIGN_FILES) <= members:
        raise PackageError('Collector source members require the complete Collector and campaign addons')
    for name in names:
        baseline._archive_path(Path('.'), name)
        if not _allowed(name):
            raise PackageError('Member falls outside common external source scope')
    baseline._unique_names([*names, MANIFEST])


def _zip_bytes(payload, manifest):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w', compression=zipfile.ZIP_STORED, allowZip64=False) as archive:
        for name, data in [*sorted(payload.items()), (MANIFEST, manifest)]:
            entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.create_system = 3
            entry.external_attr = (stat.S_IFREG | 0o644) << 16
            archive.writestr(entry, data)
    return stream.getvalue()


def _receipt(path, raw, manifest_raw, payload):
    return {'path': str(path), 'manifest_sha256': baseline._sha(manifest_raw),
            'zip_sha256': baseline._sha(raw), 'files': len(payload), 'bytes': len(raw),
            'payload_bytes': sum(map(len, payload.values()))}


def _manifest(raw, expected):
    if expected is not None and baseline._sha(raw) != expected:
        raise PackageError('Common external source manifest SHA256 mismatch')
    if len(raw) > baseline.MAX_MANIFEST_BYTES:
        raise PackageError('Common external source manifest exceeds size limit')
    document = json.loads(raw)
    if (not isinstance(document, dict) or set(document) != {'schema', 'files'}
            or document['schema'] != SCHEMA or baseline._json(document) != raw):
        raise PackageError('Invalid or noncanonical common external source manifest')
    files = document['files']
    if not isinstance(files, dict) or any(not isinstance(digest, str) or not baseline.HASH.fullmatch(digest)
                                          for digest in files.values()):
        raise PackageError('Invalid common external source manifest hashes')
    _name_set(files)
    return files


def _verified_payload(files, read):
    payload = {}
    available = LIMIT
    for name, digest in files.items():
        raw = read(name, min(baseline.MAX_FILE_BYTES, available))
        if len(raw) > available or len(raw) > baseline.MAX_FILE_BYTES or baseline._sha(raw) != digest:
            raise PackageError('Common external source member SHA256 or size mismatch')
        payload[name] = raw
        available -= len(raw)
    if not payload[baseline.LICENSE].startswith(b'MIT License'):
        raise PackageError('Expected the Dispatch MIT License')
    return payload


def build(root, destination):
    """Freeze the old package's source scope and the fixed external runtime additions."""
    root, destination = Path(root).absolute(), Path(destination).absolute()
    try:
        sources = {name.removeprefix(baseline.PAYLOAD): path for name, path in
                   baseline._source_paths(root, include_installer=True).items()}
        sources[baseline.LICENSE] = sources.pop('LICENSE')
        for name in ADDITIONS:
            # Explicit new source allowlist also covers pre-commit maintenance builds.
            sources[name] = baseline._archive_path(root, name)
        _name_set(sources)
        payload = {}
        available = LIMIT
        for name, path in sorted(sources.items()):
            payload[name] = baseline._read(path, min(available, baseline.MAX_FILE_BYTES))
            available -= len(payload[name])
        if not payload[baseline.LICENSE].startswith(b'MIT License'):
            raise PackageError('Expected the Dispatch MIT License')
        manifest = baseline._json({'schema': SCHEMA, 'files': {name: baseline._sha(data) for name, data in payload.items()}})
        _manifest(manifest, None)
        raw = _zip_bytes(payload, manifest)
        if len(raw) > LIMIT:
            raise PackageError('Common external archive exceeds 16 MiB')
        if destination.suffix != '.zip':
            destination /= f'superagent-common-external-{baseline._sha(manifest)[:12]}.zip'
        baseline._ensure_directory(destination.parent)
        baseline._publish_archive(destination, raw)
        return _receipt(destination, raw, manifest, payload)
    except PackageError:
        raise
    except (OSError, ValueError, RecursionError) as error:
        raise PackageError('Common external source package could not be built') from error


def _entries(archive):
    entries = archive.infolist()
    if len(entries) > MAX_FILES + 1:
        raise PackageError('Common external archive entry count exceeds limit')
    baseline._unique_names([entry.orig_filename for entry in entries])
    for entry in entries:
        name = entry.orig_filename
        baseline._archive_path(Path('.'), name)
        bound = baseline.MAX_MANIFEST_BYTES if name == MANIFEST else baseline.MAX_FILE_BYTES
        if (name != entry.filename or entry.flag_bits or entry.is_dir()
                or not stat.S_ISREG(entry.external_attr >> 16) or entry.compress_type != zipfile.ZIP_STORED
                or entry.compress_size != entry.file_size or not 0 <= entry.file_size <= bound):
            raise PackageError('Invalid common external ZIP member')
    if sum(entry.file_size for entry in entries) > LIMIT:
        raise PackageError('Common external ZIP payload exceeds size limit')
    return {entry.filename: entry for entry in entries}


def read_archive(path, *, expected_manifest_sha256=None, expected_zip_sha256=None):
    """Return only verified root-relative paths and their bounded payload bytes."""
    path = Path(path).absolute()
    try:
        raw = baseline._read(path, LIMIT)
        if expected_zip_sha256 is not None and baseline._sha(raw) != expected_zip_sha256:
            raise PackageError('Common external archive SHA256 mismatch')
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            entries = _entries(archive)
            if MANIFEST not in entries:
                raise PackageError('Common external source manifest is missing')
            manifest = archive.read(entries.pop(MANIFEST))
            files = _manifest(manifest, expected_manifest_sha256)
            if files.keys() != entries.keys():
                raise PackageError('Common external archive does not match its manifest file set')
            payload = _verified_payload(files, lambda name, _: archive.read(entries[name]))
            if _zip_bytes(payload, manifest) != raw:
                raise PackageError('Common external package does not have canonical ZIP encoding')
        return _receipt(path, raw, manifest, payload), {**payload, MANIFEST: manifest}
    except PackageError:
        raise
    except (OSError, zipfile.BadZipFile, ValueError, RecursionError) as error:
        raise PackageError('Common external archive could not be verified') from error


def verify_archive(path, *, expected_manifest_sha256=None, expected_zip_sha256=None):
    receipt, _ = read_archive(path, expected_manifest_sha256=expected_manifest_sha256,
                              expected_zip_sha256=expected_zip_sha256)
    return receipt


def _check_source_tree(root, names):
    files = set(names) | {MANIFEST}
    directories = {parent.as_posix() for name in files for parent in Path(name).parents if parent != Path('.')}
    pending, found = [root], set()
    while pending:
        directory = pending.pop()
        for path in directory.iterdir():
            alias = path.relative_to(root).as_posix()
            if alias in directories:
                baseline._regular_path(path, directory=True)
                pending.append(path)
            elif alias in files:
                found.add(alias)
            else:
                raise PackageError('Unexpected entry in the installed source release')
    if found != files:
        raise PackageError('Installed source release is incomplete')


def verify_source(root, *, expected_manifest_sha256):
    """Check an extracted, isolated source tree against its pinned manifest digest."""
    root = Path(root).absolute()
    try:
        if not isinstance(expected_manifest_sha256, str) or not baseline.HASH.fullmatch(expected_manifest_sha256):
            raise PackageError('A pinned common external source manifest SHA256 is required')
        raw = baseline._read(root / MANIFEST, baseline.MAX_MANIFEST_BYTES)
        files = _manifest(raw, expected_manifest_sha256)
        _check_source_tree(root, files)
        _verified_payload(files, lambda name, maximum: baseline._read(root / name, maximum))
    except PackageError:
        raise
    except (OSError, ValueError, RecursionError) as error:
        raise PackageError('Installed common external source could not be verified') from error
    return {'manifest_sha256': expected_manifest_sha256, 'files': len(files)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    commands = parser.add_subparsers(dest='command', required=True)
    builder = commands.add_parser('build')
    builder.add_argument('root', type=Path)
    builder.add_argument('destination', type=Path)
    verifier = commands.add_parser('verify')
    verifier.add_argument('archive', type=Path)
    verifier.add_argument('--manifest-sha256')
    verifier.add_argument('--zip-sha256')
    args = parser.parse_args(argv)
    try:
        result = (build(args.root, args.destination) if args.command == 'build' else
                  verify_archive(args.archive, expected_manifest_sha256=args.manifest_sha256,
                                 expected_zip_sha256=args.zip_sha256))
    except PackageError as error:
        parser.exit(2, f'{error}\n')
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
