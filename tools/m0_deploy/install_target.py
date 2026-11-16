"""Read and validate the fixed machine binding for the common installation."""

import json
import os
from pathlib import Path
import re
import socket
import stat
import sys


INSTALL_TARGET = Path('/etc/sa-dispatch/install-target.json')
MACHINE_ID = Path('/etc/machine-id')
HOSTNAME = Path('/etc/hostname')
TARGET_SCHEMA = 'superagent.common.target.v1'
TARGET_SCOPE = 'synthetic_research_content_single_ledger'
TARGET_FIELDS = {'schema', 'scope', 'manifest_sha256', 'machine_id', 'hostname'}


class InstallTargetError(ValueError):
    pass


def validate_manifest_sha256(value):
    if not isinstance(value, str) or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise InstallTargetError('explicit full deployment manifest SHA256 required')
    return value


def _check_metadata(info, *, directory):
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    if (not expected_type(info.st_mode) or info.st_uid != 0 or info.st_gid != 0 or
            info.st_mode & 0o022 or (not directory and info.st_nlink != 1)):
        raise InstallTargetError('installation identity path ownership, type or permissions are unsafe')


def _read_protected(path, *, limit):
    """Walk from / using open descriptors so checked parents govern each lookup."""
    common_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
    directory_flags = common_flags | os.O_DIRECTORY
    root, *parents, name = path.parts
    try:
        directory = os.open(root, directory_flags)
        try:
            _check_metadata(os.fstat(directory), directory=True)
            for part in parents:
                child = os.open(part, directory_flags, dir_fd=directory)
                os.close(directory)
                directory = child
                _check_metadata(os.fstat(directory), directory=True)
            fd = os.open(name, common_flags | os.O_NONBLOCK, dir_fd=directory)
            try:
                _check_metadata(os.fstat(fd), directory=False)
                with os.fdopen(fd, 'rb', closefd=False) as stream:
                    contents = stream.read(limit + 1)
            finally:
                os.close(fd)
        finally:
            os.close(directory)
    except OSError as exc:
        raise InstallTargetError('cannot securely read the fixed installation identity path') from exc
    if len(contents) > limit:
        raise InstallTargetError('installation identity file exceeds its maximum size')
    return contents


def observe_install_target():
    """Return the current (machine_id, hostname) before an installer writes a binding."""
    if (sys.platform != 'linux' or os.geteuid() != 0 or
            not Path('/run/systemd/system').is_dir()):
        raise InstallTargetError('installation target requires Linux root under systemd')
    machine_id = _read_protected(MACHINE_ID, limit=64)
    if re.fullmatch(rb'[0-9a-f]{32}\n?', machine_id) is None or machine_id.rstrip(b'\n') == b'0' * 32:
        raise InstallTargetError('current machine-id is invalid')
    try:
        hostname = _read_protected(HOSTNAME, limit=256).decode('utf-8').rstrip('\n')
        current_hostname = socket.gethostname()
    except (UnicodeDecodeError, OSError) as exc:
        raise InstallTargetError('cannot read the current hostname') from exc
    if not hostname or len(hostname) > 64 or any(ord(character) < 33 for character in hostname):
        raise InstallTargetError('current hostname is invalid')
    if hostname != current_hostname:
        raise InstallTargetError('configured hostname differs from live hostname')
    return machine_id.decode('ascii').rstrip('\n'), hostname


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InstallTargetError('duplicate field in installation target binding')
        result[key] = value
    return result


def validate_install_target(deployment_manifest):
    """Validate a caller-selected package against its protected host binding."""
    expected = validate_manifest_sha256(deployment_manifest)
    machine_id, hostname = observe_install_target()
    contents = _read_protected(INSTALL_TARGET, limit=4096)
    try:
        target = json.loads(contents.decode('utf-8'), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise InstallTargetError('invalid installation target JSON') from exc
    if (not isinstance(target, dict) or set(target) != TARGET_FIELDS or
            any(type(value) is not str for value in target.values()) or
            target['schema'] != TARGET_SCHEMA or target['scope'] != TARGET_SCOPE):
        raise InstallTargetError('invalid installation target schema or scope')
    if target['machine_id'] != machine_id:
        raise InstallTargetError('installation target machine-id mismatch')
    if target['hostname'] != hostname:
        raise InstallTargetError('installation target hostname mismatch')
    if target['manifest_sha256'] != expected:
        raise InstallTargetError('installation target manifest SHA256 mismatch')
    return target
