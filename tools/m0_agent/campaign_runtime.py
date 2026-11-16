"""Run an authorized campaign role against the existing Debian 0013 ledger."""

import argparse
from contextlib import contextmanager
from functools import partial
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys

if __package__ in (None, ''):
    source = Path(__file__).resolve().parents[2]
    sys.path[:0] = [str(source / 'dispatch'), str(source)]
    __package__ = 'tools.m0_agent'

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.models import DispatchError

from . import campaign_control, campaign_worker, common_runtime
from .provider_process import complete_bounded
from .runtime import read_systemd_key


CONFIG_DIR = Path('/etc/sa-dispatch-agent-campaign')
CONFIG = CONFIG_DIR / 'campaign.json'
LOCK = CONFIG_DIR / 'tick.lock'
ROLE_GROUP = 'sa-agent-campaign'
ACCOUNTS = {'producer': 'sa-agent-producer', 'reviewer': 'sa-agent-reviewer'}
SHARED_GROUP = common_runtime.SHARED_GROUP
LEDGER, CAS = common_runtime.LEDGER, common_runtime.CAS
MACHINE_ID = common_runtime.MACHINE_ID
NETWORK_CONFIG = common_runtime.NETWORK_CONFIG


def _fixed_metadata(info, *, mode, group, directory=False):
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if (not kind(info.st_mode) or info.st_uid != 0 or info.st_gid != group
            or stat.S_IMODE(info.st_mode) != mode or (not directory and info.st_nlink != 1)):
        raise ValueError('unsafe root-managed campaign path')


def _managed_path(path, *, mode, group, directory=False):
    _fixed_metadata(path.lstat(), mode=mode, group=group, directory=directory)


def read_config():
    for parent in CONFIG_DIR.parents:
        common_runtime._safe_system_path(parent, directory=True)
    common_runtime._safe_system_path(CONFIG_DIR, directory=True)
    common_runtime._safe_system_path(CONFIG, directory=False)
    config = campaign_control.validate_config(common_runtime._read_document(CONFIG))
    common_runtime._safe_system_path(MACHINE_ID, directory=False)
    if MACHINE_ID.read_text(encoding='ascii').strip() != config['machine_id']:
        raise ValueError('campaign is authorized for a different machine')
    return config


def verify_accounts():
    import grp
    import pwd

    role_group = grp.getgrnam(ROLE_GROUP)
    ledger_group = grp.getgrnam(SHARED_GROUP)
    accounts = {role: pwd.getpwnam(name) for role, name in ACCOUNTS.items()}
    dispatch = pwd.getpwnam('sa-dispatch').pw_uid
    external = pwd.getpwnam('sa-agent-external').pw_uid
    uids = {account.pw_uid for account in accounts.values()}
    role_names = set(ACCOUNTS.values())
    if (role_group.gr_gid in (0, ledger_group.gr_gid) or ledger_group.gr_gid == 0
            or role_group.gr_mem or len(uids) != len(ACCOUNTS) or uids & {0, dispatch, external}
            or any(account.pw_gid != role_group.gr_gid or account.pw_dir != '/nonexistent'
                   or account.pw_shell != '/usr/sbin/nologin' for account in accounts.values())
            or {entry.pw_name for entry in pwd.getpwall() if entry.pw_gid == role_group.gr_gid
                or entry.pw_uid in uids} != role_names):
        raise ValueError('campaign role accounts do not have isolated identities')
    return accounts, role_group.gr_gid, ledger_group.gr_gid, dispatch


def verify_execution(command, role=None):
    if sys.platform != 'linux':
        raise ValueError('campaign requires its Linux service identities')
    accounts, group, ledger_group, dispatch = verify_accounts()
    if command == 'tick':
        account = accounts[role]
        if (os.getuid() != account.pw_uid or os.geteuid() != account.pw_uid
                or os.getgid() != group or os.getegid() != group
                or set(os.getgroups()) - {group} != {ledger_group}):
            raise ValueError('campaign tick has incorrect role identity')
    elif os.getuid() != 0 or os.geteuid() != 0:
        raise ValueError('campaign operator command requires root')
    _managed_path(CONFIG_DIR, mode=0o750, group=group, directory=True)
    _managed_path(CONFIG, mode=0o640, group=group)
    for parent in LEDGER.parent.parents:
        common_runtime._safe_system_path(parent, directory=True)
    for path in (LEDGER.parent, CAS, LEDGER):
        info = path.lstat()
        directory = path != LEDGER
        kind = stat.S_ISDIR if directory else stat.S_ISREG
        mode = 0o2770 if directory else 0o660
        if (not kind(info.st_mode) or info.st_uid not in (0, dispatch) or info.st_gid != ledger_group
                or stat.S_IMODE(info.st_mode) != mode or (not directory and info.st_nlink != 1)):
            raise ValueError('campaign ledger or artifact path is not the approved common store')
    return group


@contextmanager
def tick_lock(group):
    import fcntl

    fd = os.open(LOCK, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        _fixed_metadata(os.fstat(fd), mode=0o440, group=group)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
        else:
            yield True
    finally:
        os.close(fd)


def run(command, role=None):
    group = verify_execution(command, role)
    config = read_config()
    artifacts = ArtifactStore(CAS)
    if command == 'tick':
        with tick_lock(group) as locked:
            if not locked:
                return {'status': 'busy', 'role': role}
            network = common_runtime.read_provider_network(NETWORK_CONFIG)
            if network is None:
                raise ValueError('approved common provider network configuration is missing')
            return campaign_worker.tick(
                LEDGER, artifacts, config, role=role, credential_reader=read_systemd_key,
                provider_call=partial(complete_bounded, https_connect_proxy=network),
            )
    return getattr(campaign_control, command)(LEDGER, artifacts, config)


def main(argv=None):
    from tools.m0_native_mixed.native_adapter import directory_lock

    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('command', choices=('register', 'tick', 'status', 'stop'))
    parser.add_argument('--role', choices=tuple(ACCOUNTS))
    args = parser.parse_args(argv)
    if (args.command == 'tick') != (args.role is not None):
        parser.error('--role is required for tick only')
    os.umask(0o007)
    try:
        # systemd holds the shared directory flock across the whole tick.
        if args.command == 'tick':
            result = run(args.command, args.role)
        else:
            with directory_lock(LEDGER.parent, shared=True):
                result = run(args.command)
    except (DispatchError, KeyError, OSError, TypeError, ValueError, RecursionError, sqlite3.Error) as error:
        print(json.dumps({'ok': False, 'operation': args.command,
                          'code': getattr(error, 'code', 'E_CAMPAIGN_RUNTIME')}))
        return 1
    print(json.dumps({'ok': True, **result}, ensure_ascii=True, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
