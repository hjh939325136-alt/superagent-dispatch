#!/usr/bin/env python3
"""Export the reviewed source files to a new, standalone repository directory."""

import argparse
from hashlib import sha256
from pathlib import Path
import re
import stat
import sys
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


SOURCE_ROOT = Path(__file__).resolve().parents[1]

# Keep this list explicit. Adding a file to the development tree never exports it.
RELEASE_FILES = (
    '.github/workflows/check.yml',
    'LICENSE',
    'README.md',
    'config/dispatch.example.toml',
    'deploy/README.md',
    'deploy/backup-coordinator.sh',
    'deploy/check-coordinator.sh',
    'deploy/coordinator.example.toml',
    'deploy/identities.example.json',
    'deploy/install-coordinator.sh',
    'deploy/sa-dispatch.service',
    'deploy/smoke-coordinator.py',
    'fixtures/synthetic/topic_supply.json',
    'release/gitattributes.txt',
    'release/gitignore.txt',
    'release/stage.py',
    'sa_dispatch/__init__.py',
    'sa_dispatch/__main__.py',
    'sa_dispatch/adapters.py',
    'sa_dispatch/artifacts.py',
    'sa_dispatch/bundle.py',
    'sa_dispatch/canonical.py',
    'sa_dispatch/cli.py',
    'sa_dispatch/clock.py',
    'sa_dispatch/config.py',
    'sa_dispatch/executor.py',
    'sa_dispatch/models.py',
    'sa_dispatch/runner.py',
    'sa_dispatch/schema.sql',
    'sa_dispatch/selftest.py',
    'sa_dispatch/service.py',
    'sa_dispatch/states.py',
    'sa_dispatch/store_sqlite.py',
    'sa_dispatch/transport.py',
    'sa_dispatch_hotspot/__init__.py',
    'tests/contract/__init__.py',
    'tests/contract/test_store_sqlite.py',
    'tests/fakes/fake_component.py',
    'tests/test_adapters.py',
    'tests/test_artifacts.py',
    'tests/test_config_runner.py',
    'tests/test_dispatch_flow.py',
    'tests/test_fake_component.py',
    'tests/test_loopback_process.py',
    'tests/test_service.py',
    'tests/test_transport.py',
)

# These signatures supplement review of the allowlisted sources and sample data.
SENSITIVE_SIGNATURE = re.compile(
    rb'-----BEGIN (?:[A-Z0-9]+ )?PRIVATE KEY-----'
    rb'|\b(?:gh[pousr]_[a-zA-Z0-9]{30,}|github_pat_[a-zA-Z0-9_]{30,})\b'
    rb'|\bsk-(?:proj-|svcacct-)[a-zA-Z0-9_-]{30,}\b'
)


def read_release_file(relative):
    source_path = SOURCE_ROOT
    for part in Path(relative).parts:
        source_path /= part
        attributes = source_path.lstat()
        if stat.S_ISLNK(attributes.st_mode) or (
            getattr(attributes, 'st_file_attributes', 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise ValueError(f'Linked release input: {relative}')
    if not stat.S_ISREG(attributes.st_mode):
        raise ValueError(f'Expected a regular release file: {relative}')

    data = source_path.read_bytes()
    data.decode('utf-8')
    if b'\0' in data or SENSITIVE_SIGNATURE.search(data):
        raise ValueError(f'Unexpected content in release file: {relative}')
    return data.replace(b'\r\n', b'\n')


def release_bytes():
    files = {relative: read_release_file(relative) for relative in RELEASE_FILES}
    files['.gitattributes'] = files['release/gitattributes.txt']
    files['.gitignore'] = files['release/gitignore.txt']
    files['SHA256SUMS'] = ''.join(
        f'{sha256(data).hexdigest()}  {relative}\n'
        for relative, data in sorted(files.items())
    ).encode('utf-8')
    return files


def new_output(path):
    if path.exists() or path.is_symlink():
        raise ValueError(f'Output already exists: {path}')
    path = path.parent.resolve(strict=True) / path.name
    if path == SOURCE_ROOT or SOURCE_ROOT in path.parents:
        raise ValueError('Keep staged output outside the dispatch source directory.')
    return path


def stage(destination):
    destination = new_output(destination)
    files = release_bytes()
    destination.mkdir()
    for relative, data in files.items():
        path = destination / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('xb') as output:
            output.write(data)
    return destination, len(files)


def archive(destination):
    destination = new_output(destination)
    if destination.suffix.lower() != '.zip':
        raise ValueError('Give the source archive a .zip suffix.')
    checksum = new_output(destination.with_name(destination.name + '.sha256'))
    files = release_bytes()
    with ZipFile(destination, mode='x') as output:
        for path, data in sorted(files.items()):
            entry = ZipInfo(f'{destination.stem}/{path}')
            entry.create_system = 3
            mode = 0o755 if path.endswith('.sh') else 0o644
            entry.external_attr = (stat.S_IFREG | mode) << 16
            output.writestr(entry, data, compress_type=ZIP_DEFLATED, compresslevel=9)
    digest = sha256(destination.read_bytes()).hexdigest()
    with checksum.open('x', encoding='utf-8', newline='\n') as output:
        output.write(f'{digest}  {destination.name}\n')
    return destination, len(files)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--zip', action='store_true', help='write a source ZIP and SHA-256 file')
    parser.add_argument('destination', type=Path, help='a new output path with an existing parent')
    args = parser.parse_args()
    try:
        operation = archive if args.zip else stage
        destination, count = operation(args.destination)
    except (OSError, ValueError) as error:
        print(f'Release staging failed: {error}', file=sys.stderr)
        return 1
    print(f'Staged {count} files at {destination}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
