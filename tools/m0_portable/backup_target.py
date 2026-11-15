"""Run a synthetic backup using an explicit trusted-local target binding."""
import argparse
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

from tools.m0_backup import backup as backend


BINDING_SCHEMA = 'm0.portable_backup_target.v1'
BINDING_FIELDS = {'schema', 'target_root', 'quota_bytes', 'target_device', 'target_inode', 'pwsh'}
PLAN_FIELDS = {'schema', 'synthetic_only', 'source_root', 'op_id', 'files'}


def object_fields(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        backend.fail('E_FIELDS', 'duplicate JSON field')
    return result


def read_object(path, limit):
    value = json.loads(backend.regular_bytes(path, limit), object_pairs_hook=object_fields)
    if not isinstance(value, dict):
        backend.fail('E_FIELDS', 'expected a JSON object')
    return value


def local_absolute(value):
    if not isinstance(value, str) or not value or '\x00' in value:
        backend.fail('E_PATH', 'an explicit local absolute path is required')
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts or (os.name == 'nt' and not re.fullmatch('[A-Za-z]:', path.drive)):
        backend.fail('E_PATH', 'an explicit local absolute path is required')
    return path


def load_binding(path):
    value = read_object(path, 8192)
    if set(value) != BINDING_FIELDS or value['schema'] != BINDING_SCHEMA:
        backend.fail('E_BINDING', 'unknown binding schema or missing/unknown binding fields')
    quota = value['quota_bytes']
    if type(quota) is not int or not 0 < quota <= backend.MAX_BYTES:
        backend.fail('E_QUOTA', 'target quota must be within 200 MiB')
    root = local_absolute(value['target_root'])
    if root == root.parent:
        backend.fail('E_TARGET', 'target must be a dedicated directory below the volume root')
    info = backend.safe_stat(root)
    identity = (value['target_device'], value['target_inode'])
    if (not stat.S_ISDIR(info.st_mode) or any(type(number) is not int for number in identity)
            or identity != (info.st_dev, info.st_ino)):
        backend.fail('E_BINDING', 'target directory identity does not match the binding')
    pwsh = value['pwsh']
    if os.name == 'nt':
        pwsh = local_absolute(pwsh)
        if pwsh.name.casefold() != 'pwsh.exe' or not stat.S_ISREG(backend.safe_stat(pwsh).st_mode):
            backend.fail('E_BINDING', 'expected a trusted absolute pwsh.exe file')
    elif pwsh is not None:
        backend.fail('E_BINDING', 'pwsh must be null on this host')
    return root, quota, pwsh


def load_plan(path):
    value = read_object(path, 65536)
    fields = set(value)
    if fields not in (PLAN_FIELDS, PLAN_FIELDS | {'linux_devices'}):
        backend.fail('E_FIELDS', 'missing or unknown plan fields')
    local_absolute(value['source_root'])
    files = value['files']
    if not isinstance(files, list) or any(not isinstance(item, dict) or set(item) != {'path', 'kind'}
                                          or not all(isinstance(item[key], str) for key in item) for item in files):
        backend.fail('E_FIELDS', 'each selected file requires a string path and kind')
    return value


def run(command, binding, plan):
    root, quota, pwsh = load_binding(binding)
    selected = load_plan(plan)
    previous = os.environ.get('M0_BACKUP_PWSH')
    if pwsh is not None:
        os.environ['M0_BACKUP_PWSH'] = str(pwsh)
    try:
        if command == 'backup':
            return backend.backup(selected, root, quota)
        ready = backend.preflight(selected, root, quota)
        return {key: ready[key] for key in ('existing_bytes', 'required_bytes', 'quota_bytes', 'op_id')} | {
            'state': 'ready', 'writes': 0, 'archive_bytes': len(ready['archive']), 'devices': ready['manifest']['devices']}
    finally:
        if previous is None:
            os.environ.pop('M0_BACKUP_PWSH', None)
        else:
            os.environ['M0_BACKUP_PWSH'] = previous


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('preflight', 'backup'))
    parser.add_argument('--binding', required=True, type=Path, help='trusted operator JSON file')
    parser.add_argument('--plan', required=True, type=Path, help='explicit synthetic file selection JSON')
    args = parser.parse_args()
    try:
        result = run(args.command, args.binding, args.plan)
    except backend.BackupError as error:
        print(json.dumps({'state': error.state, 'code': error.code, 'message': str(error), 'partial': error.partial}), file=sys.stderr)
        return 2
    except (ValueError, TypeError, KeyError, OSError, subprocess.SubprocessError) as error:
        print(json.dumps({'state': 'rejected', 'code': 'E_INPUT_OR_PREFLIGHT', 'error_type': type(error).__name__}), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
