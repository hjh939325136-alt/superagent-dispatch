"""File-only credential provisioning. Never puts a value in arguments or logs."""
import argparse
import json
import os
from pathlib import Path
import stat
import uuid
from contextlib import contextmanager


def regular(path):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('absolute path without traversal required')
    for part in (path, *path.parents):
        info = part.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise ValueError('credential links forbidden')
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or not 16 <= info.st_size <= 8192:
        raise ValueError('invalid credential file')
    return path, info


def protected(path):
    if os.name != 'posix':
        raise ValueError('credential import requires Linux permission verification')
    path, info = regular(path)
    if info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600:
        raise ValueError('credential must be root-owned mode 0600')
    for parent in path.parents:
        info = parent.stat()
        if info.st_uid != 0 or info.st_mode & 0o022:
            raise ValueError('credential parent must be root-owned and not writable by others')
    return path


@contextmanager
def provision_lock(destination):
    import fcntl
    lock = destination.parent / '.credential-import.lock'
    fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if info.st_uid != 0 or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
            raise ValueError('unsafe provisioning lock')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def import_key(source, destination):
    if os.name != 'posix' or os.geteuid() != 0:
        raise ValueError('root required')
    source = protected(source)
    destination = Path(destination)
    if not destination.is_absolute() or '..' in destination.parts:
        raise ValueError('invalid destination')
    # The installer creates the dedicated destination directory first.
    for parent in (destination.parent, *destination.parent.parents):
        info = parent.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022):
            raise ValueError('unsafe credential destination')
    with provision_lock(destination):
        return _import_locked(source, destination)


def _import_locked(source, destination):
    value = source.read_bytes()
    if b'\x00' in value or b'\n' in value or b'\r' in value:
        raise ValueError('credential must be a single line without terminator')
    if destination.exists() or destination.is_symlink():
        existing = protected(destination)
        if existing.read_bytes() != value:
            raise ValueError('existing credential differs; explicit rotation is required')
        return {'installed': True, 'reused': True, 'value_disclosed': False}
    temporary = destination.with_name('.credential-' + uuid.uuid4().hex)
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as handle:
        handle.write(value); handle.flush(); os.fsync(handle.fileno())
    # Root-owned parent and installer lock exclude other cooperating writers.
    if destination.exists() or destination.is_symlink():
        raise ValueError('credential appeared during provisioning')
    os.replace(temporary, destination)
    fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return {'installed': True, 'reused': False, 'value_disclosed': False}


def consume_systemd_credential():
    folder = os.environ.get('CREDENTIALS_DIRECTORY')
    if not folder:
        raise ValueError('systemd credential directory missing')
    path, info = regular(Path(folder) / 'provider_api_key')
    if (info.st_uid not in (0, os.geteuid()) or info.st_mode & 0o227 or
            (info.st_mode & 0o040 and info.st_gid not in (0, os.getegid()))):
        raise ValueError('systemd credential permissions are too broad')
    value = path.read_bytes()
    if not value or any(x in value for x in (b'\x00', b'\n', b'\r')):
        raise ValueError('invalid systemd credential')
    # Deliberately do not log the value, hash or prefix, and do not call a model.
    return {'credential_read': True, 'model_calls': 0, 'value_disclosed': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('import', 'probe'))
    parser.add_argument('--source')
    parser.add_argument('--destination')
    args = parser.parse_args()
    try:
        result = (import_key(args.source, args.destination) if args.command == 'import'
                  else consume_systemd_credential())
        print(json.dumps(result))
        return 0
    except (OSError, ValueError, TypeError):
        print(json.dumps({'ok': False, 'error': 'credential_operation_refused'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
