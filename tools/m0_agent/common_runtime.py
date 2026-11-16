"""Operator registration, systemd tick, and bounded status for the common agent."""

import argparse
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
from sa_dispatch.models import DispatchError, Ref

from . import common_install, common_worker
from .deepseek_client import allowed_connect_proxy
from .provider_process import complete_bounded
from .runtime import read_systemd_key
from .worker import OperatorGrant


CONFIG = Path('/etc/sa-dispatch/agent-external.json')
NETWORK_CONFIG = CONFIG.with_name('provider-network.json')
LEDGER = Path('/var/lib/sa-dispatch/dispatch.sqlite3')
CAS = LEDGER.parent / 'artifacts'
MACHINE_ID = Path('/etc/machine-id')
SHARED_GROUP = 'sa-dispatch-ledger'
SCHEMA = 'superagent.common.agent_external.v1'
NETWORK_SCHEMA = 'superagent.provider-network.v1'
CONFIG_LIMIT = 8 * 1024


def _unique_fields(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError('duplicate common agent configuration field')
        obj[key] = value
    return obj


def _read_document(path):
    with Path(path).open('rb') as config:
        raw = config.read(CONFIG_LIMIT + 1)
    if len(raw) > CONFIG_LIMIT:
        raise ValueError('common agent configuration too large')
    return json.loads(raw.decode('utf-8'), object_pairs_hook=_unique_fields)


def read_config(path):
    document = _read_document(path)
    if (not isinstance(document, dict)
            or set(document) != {'schema', 'grant', 'expires_at', 'ledger_identity', 'machine_id'}
            or document['schema'] != SCHEMA or not isinstance(document['grant'], dict)):
        raise ValueError('invalid common agent configuration')
    grant = document['grant']
    if set(grant) != set(OperatorGrant.__dataclass_fields__):
        raise ValueError('invalid common agent grant fields')
    return common_worker.CommonGrant(
        OperatorGrant(**(grant | {'prompt_ref': Ref(**grant['prompt_ref'])})),
        document['expires_at'], document['ledger_identity'], document['machine_id'],
    )


def _safe_system_path(path, *, directory):
    info = path.lstat()
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if (not kind(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022
            or (not directory and info.st_nlink != 1)):
        raise ValueError('unsafe common agent system path')


def read_provider_network(path):
    try:
        _safe_system_path(path, directory=False)
    except FileNotFoundError:
        return None
    document = _read_document(path)
    if (not isinstance(document, dict) or set(document) != {'schema', 'https_connect_proxy'}
            or document['schema'] != NETWORK_SCHEMA or not allowed_connect_proxy(document['https_connect_proxy'])):
        raise ValueError('invalid common agent CONNECT proxy configuration')
    return document['https_connect_proxy']


def _require_live_mode():
    if os.name != 'posix':
        raise ValueError('common agent CLI requires its Linux service account')
    if os.geteuid() == 0:
        common_install._root_metadata(common_install.AUTHORITY, directory=True, private=True)
        receipt = json.loads(common_install._root_bytes(common_install.AUTHORITY / 'state.json', private=True),
                             object_pairs_hook=_unique_fields)
        if receipt.get('schema') != common_install.STATE_SCHEMA or 'execution_mode' in receipt:
            raise ValueError('common agent runtime has no live execution authority')
    # The worker UID cannot read the private installer receipt. Its fixed,
    # root-controlled unit carries the offline gate without a second file.
    unit = common_install.SYSTEMD / common_install.AGENT
    common_install._root_parents(unit)
    common_install._root_metadata(unit)
    with unit.open('rb') as handle:
        marker = handle.readline(128).rstrip(b'\r\n')
    if marker == common_install.OFFLINE_MARKER.encode('ascii'):
        raise ValueError('common agent runtime has no live execution authority')


def _verify_service(command):
    if os.name != 'posix':
        raise ValueError('common agent CLI requires its Linux service account')
    import grp
    import pwd

    worker = pwd.getpwnam(common_worker.DEVICE_ID).pw_uid
    dispatch = pwd.getpwnam('sa-dispatch').pw_uid
    shared = grp.getgrnam(SHARED_GROUP).gr_gid
    uid = os.geteuid()
    if (worker in (0, dispatch) or dispatch == 0 or shared == 0
            or (command == 'register' and uid != 0)
            or (command == 'tick' and uid != worker)
            or (command == 'status' and uid not in (0, worker))
            or (uid != 0 and shared not in {os.getegid(), *os.getgroups()})):
        raise ValueError('common agent CLI has incorrect service ownership')
    for parent in {*CONFIG.parents, *LEDGER.parent.parents}:
        _safe_system_path(parent, directory=True)
    _safe_system_path(CONFIG, directory=False)
    for folder in (LEDGER.parent, CAS):
        info = folder.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid not in (0, dispatch)
                or info.st_gid != shared or info.st_mode & 0o2070 != 0o2070 or info.st_mode & 0o007):
            raise ValueError('common agent shared directory has incorrect ownership or permissions')
    info = LEDGER.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid not in (0, dispatch)
            or info.st_gid != shared or info.st_mode & 0o060 != 0o060 or info.st_mode & 0o007):
        raise ValueError('common agent common ledger has incorrect ownership or permissions')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('register', 'tick', 'status'))
    command = parser.parse_args(argv).command
    try:
        if command in ('register', 'tick'):
            _require_live_mode()
        _verify_service(command)
        config = read_config(CONFIG)
        if MACHINE_ID.read_text(encoding='ascii').strip() != config.machine_id:
            raise ValueError('grant is issued to a different Linux machine identity')
        artifacts = ArtifactStore(CAS)
        os.umask(0o007)
        if command == 'register':
            result = common_worker.register(LEDGER, artifacts, config)
        elif command == 'tick':
            network = read_provider_network(NETWORK_CONFIG)
            result = common_worker.tick(LEDGER, artifacts, config,
                                        credential_reader=read_systemd_key,
                                        provider_call=partial(complete_bounded, https_connect_proxy=network))
        else:
            result = common_worker.status(LEDGER, artifacts, config)
    except (DispatchError, KeyError, OSError, TypeError, ValueError, RecursionError, sqlite3.Error) as exc:
        print(json.dumps({'ok': False, 'error': 'common_agent_runtime_refused',
                          'operation': command, 'code': getattr(exc, 'code', 'E_RUNTIME_BOUNDARY')}))
        return 1
    print(json.dumps({'ok': True} | result, ensure_ascii=True, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
