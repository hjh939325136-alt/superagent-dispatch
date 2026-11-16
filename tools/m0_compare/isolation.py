"""Fixed transient systemd isolation for the separate Hermes comparison process."""

import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys


HERMES_ROOT = PurePosixPath('/opt/sa-hermes-compare')
HERMES_COMMIT = '38c8a9c10fb3680beb6170a604cbec209fcb89a5'
CODE_ROOT = PurePosixPath('/opt/sa-compare-code')
BROKER_ROOT = PurePosixPath('/run/sa-compare-broker')
CHILD_BROKER = PurePosixPath('/run/sa-compare-ipc')
SOCKET_NAMES = ('provider.sock', 'tools.sock')
RUN_ID = re.compile(r'[a-z0-9][a-z0-9-]{0,23}')
ENTRY = re.compile(r'[a-z][a-z0-9_]{0,47}\.py')
ARG = re.compile(r'[A-Za-z0-9_.:/=@+-]{1,200}')
HIDDEN = ('/mnt', '/media', '/home', '/root', '/srv', '/var/tmp',
          '/etc/credstore', '/etc/credstore.encrypted', '/etc/sa-dispatch', '/etc/sa-dispatch-agent-campaign',
          '/etc/sa-dispatch-legacy-lab', '/opt/sa-dispatch', '/opt/sa-dispatch-common', '/opt/sa-dispatch-native',
          '/opt/sa-dispatch-campaign', '/opt/sa-dispatch-collector', '/opt/sa-dispatch-observer',
          '/var/lib/systemd/credential', '/var/lib/sa-dispatch', '/var/lib/sa-dispatch-native',
          '/var/lib/sa-dispatch-manager', '/var/lib/sa-common-install', '/var/lib/sa-dispatch-agent-maintenance',
          '/var/lib/sa-dispatch-agent-campaign', '/var/lib/sa-dispatch-collector', '/var/lib/sa-dispatch-soak',
          '/var/lib/sa-dispatch-composed')


class IsolationError(ValueError):
    pass


def _require(condition, message):
    if not condition:
        raise IsolationError(message)


def _plan(run_id, hermes_prefix, code_root, broker_directory, entry, args, sockets):
    _require(isinstance(run_id, str) and RUN_ID.fullmatch(run_id), 'invalid transient run identity')
    hermes, code, broker = map(PurePosixPath, (hermes_prefix, code_root, broker_directory))
    _require(hermes == HERMES_ROOT / HERMES_COMMIT
             and code == CODE_ROOT / run_id and broker == BROKER_ROOT / run_id,
             'comparison paths do not match the fixed roots')
    _require(isinstance(entry, str) and ENTRY.fullmatch(entry), 'entry must name a fixed script in the code root')
    _require(isinstance(args, (list, tuple)) and len(args) <= 20
             and all(isinstance(value, str) and ARG.fullmatch(value) for value in args),
             'invalid bounded transient argv')
    _require(isinstance(sockets, (list, tuple)) and all(name in SOCKET_NAMES for name in sockets)
             and len(set(sockets)) == len(sockets) and 'provider.sock' in sockets,
             'invalid broker socket selection')
    for argument in args:
        if '/' not in argument:
            continue
        value = argument.partition('=')[2] if argument.startswith('--') and '=' in argument else argument
        path = PurePosixPath(value)
        _require(path.is_absolute() and '..' not in path.parts and (
            any(path.is_relative_to(allowed) for allowed in (hermes, code, PurePosixPath('/tmp')))
            or path == broker or path in {CHILD_BROKER / name for name in sockets}),
            'transient argv path is outside approved locations')
    return hermes, code, broker


def unit_command(run_id, *, hermes_prefix, code_root, broker_directory, entry, args=(), sockets=SOCKET_NAMES):
    """Build a transient systemd command. The trusted launcher verifies native paths before executing it."""
    hermes, code, broker = _plan(run_id, hermes_prefix, code_root, broker_directory, entry, args, sockets)
    properties = {
        'Type': 'exec',
        'DynamicUser': 'yes',
        'User': 'sacmp-' + run_id,
        'WorkingDirectory': '/tmp',
        'PrivateNetwork': 'yes',
        'RestrictAddressFamilies': 'AF_UNIX',
        'PrivateDevices': 'yes',
        'ProtectSystem': 'strict',
        'ProtectHome': 'yes',
        'ProtectProc': 'invisible',
        'ProcSubset': 'pid',
        'NoNewPrivileges': 'yes',
        'CapabilityBoundingSet': '',
        'KeyringMode': 'private',
        'RestrictNamespaces': 'yes',
        'RestrictSUIDSGID': 'yes',
        'LockPersonality': 'yes',
        'ProtectClock': 'yes',
        'ProtectKernelTunables': 'yes',
        'ProtectKernelModules': 'yes',
        'ProtectKernelLogs': 'yes',
        'ProtectControlGroups': 'yes',
        'SystemCallArchitectures': 'native',
        'InaccessiblePaths': ' '.join('-' + path for path in HIDDEN),
        'TemporaryFileSystem': ('/opt:ro,nosuid,nodev /etc:ro,nosuid,nodev,noexec /var:ro,nosuid,nodev,noexec '
                                '/run:ro,nosuid,nodev,noexec /tmp:rw,nosuid,nodev,noexec,mode=1777,size=32M'),
        'BindReadOnlyPaths': ' '.join((str(hermes), str(code),
                                      *(f'{broker / name}:{CHILD_BROKER / name}' for name in sockets))),
        'UMask': '0077',
        'TasksMax': '64',
        'RuntimeMaxSec': '240s',
        'TimeoutStopSec': '5s',
    }
    argv = ['/usr/bin/systemd-run', '--quiet', '--wait', '--collect', '--pipe',
            f'--unit=sa-compare-hermes-{run_id}.service']
    argv.extend(f'--property={name}={value}' for name, value in properties.items())
    argv.extend(('--', '/usr/bin/env', '-i', 'PATH=/usr/bin:/bin', 'LANG=C.UTF-8', 'HOME=/tmp',
                 'TMPDIR=/tmp', 'XDG_CACHE_HOME=/tmp/cache', 'HERMES_HOME=/tmp/hermes',
                 str(hermes / 'venv/bin/python'), '-I', '-B', str(code / entry), *args))
    return argv


def _trusted(path, *, directory):
    for parent in (*reversed(path.parents), path):
        info = parent.lstat()
        is_directory = directory or parent != path
        kind = stat.S_ISDIR if is_directory else stat.S_ISREG
        _require(kind(info.st_mode) and info.st_uid == 0 and info.st_gid == 0 and not info.st_mode & 0o022
                 and (is_directory or info.st_nlink == 1), 'comparison source path is not root protected')
    return info


def _trusted_code_tree(path):
    count = total = 0
    for folder, directories, files in os.walk(path, followlinks=False):
        current = Path(folder)
        for name in directories:
            _trusted(current / name, directory=True)
            count += 1
            _require(count <= 64, 'comparison code root exceeds its file budget')
        for name in files:
            entry = current / name
            _require(entry.suffix in ('.py', '.json') and entry.name != 'benchmark-v1.json',
                     'comparison code root contains an unapproved file type')
            info = _trusted(entry, directory=False)
            count += 1
            total += info.st_size
            _require(count <= 64 and total <= 8 * 1024 * 1024, 'comparison code root exceeds its file budget')


def checked_unit_command(run_id, *, hermes_prefix, code_root, broker_directory, entry, args=(), sockets=SOCKET_NAMES):
    """Verify the Linux paths and socket permissions without launching a process."""
    _require(sys.platform == 'linux' and os.getuid() == os.geteuid() == 0, 'Linux root launcher required')
    import grp
    import pwd

    hermes, code, broker = _plan(run_id, hermes_prefix, code_root, broker_directory, entry, args, sockets)
    for lookup in (pwd.getpwnam, grp.getgrnam):
        try:
            lookup('sacmp-' + run_id)
        except KeyError:
            continue
        raise IsolationError('transient account name is already in use')

    interpreter = Path(hermes / 'venv/bin/python')
    _trusted(interpreter.parent, directory=True)
    _require(interpreter.lstat().st_uid == 0, 'Hermes interpreter is not root owned')
    real = interpreter.resolve(strict=True)
    _require(real.is_relative_to(Path(hermes)) or re.fullmatch(r'/usr/bin/python3(?:\.[0-9]+)?', str(real)),
             'Hermes interpreter resolves outside fixed executable roots')
    _require(_trusted(real, directory=False).st_mode & 0o111, 'Hermes interpreter is not executable')
    _trusted(Path(code / entry), directory=False)
    _trusted_code_tree(Path(code))

    private = _trusted(Path(BROKER_ROOT), directory=True)
    _require(stat.S_IMODE(private.st_mode) == 0o700, 'host broker root must be root-private')
    _trusted(Path(broker), directory=True)
    for name in sockets:
        info = Path(broker / name).lstat()
        _require(stat.S_ISSOCK(info.st_mode) and info.st_uid == info.st_gid == 0 and info.st_nlink == 1
                 and stat.S_IMODE(info.st_mode) == 0o666, 'broker socket ownership or mode differs')
    return unit_command(run_id, hermes_prefix=hermes, code_root=code, broker_directory=broker, entry=entry,
                        args=args, sockets=sockets)
