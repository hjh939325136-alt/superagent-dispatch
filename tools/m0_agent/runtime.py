"""Single-grant R1 timer and read-only ledger status commands."""

import argparse
from contextlib import closing, contextmanager
from dataclasses import asdict
from functools import partial
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from tempfile import NamedTemporaryFile

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import canonical_json
from sa_dispatch.models import DispatchError, Ref, TaskRow
from sa_dispatch.routing import ledger_mode
from sa_dispatch.store_sqlite import SqliteStore

from tools.m0_installer.credentials import regular as regular_credential

from .deepseek_client import Completion, TokenUsage
from .provider_process import complete_bounded
from .worker import AgentKind, CAPABILITY, KIND, OperatorGrant, run_once


CONFIG_PATH = Path('/etc/superagent-r1/agent.json')
STATE_PATH = Path('/var/lib/superagent-r1')
DEVICE_ID = 'sa-r1'
CONFIG_SCHEMA = 'superagent.r1.agent.v1'
TICK_SCHEMA = 'superagent.r1.tick.v1'
DEADLINE_SECONDS = 45
LEASE_TTL_SECONDS = 120
CONFIG_LIMIT = 8 * 1024


def _unique_object(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError('duplicate R1 configuration field')
        obj[key] = value
    return obj


def read_config(path):
    with path.open('rb') as config:
        raw = config.read(CONFIG_LIMIT + 1)
    if len(raw) > CONFIG_LIMIT:
        raise ValueError('R1 configuration too large')
    data = json.loads(raw.decode('utf-8'), object_pairs_hook=_unique_object)
    if (not isinstance(data, dict) or set(data) != {'schema', 'execution_mode', 'grant'}
            or data['schema'] != CONFIG_SCHEMA or not isinstance(data['grant'], dict)
            or data['execution_mode'] not in ('simulation', 'live')):
        raise ValueError('invalid R1 configuration')
    grant = data['grant']
    if set(grant) != set(OperatorGrant.__dataclass_fields__) - {'execution_mode'}:
        raise ValueError('invalid R1 grant configuration')
    return OperatorGrant(**(grant | {'prompt_ref': Ref(**grant['prompt_ref']),
                                     'execution_mode': data['execution_mode']}))


def _verify_service(config, state):
    if os.name != 'posix':
        raise ValueError('R1 requires the Linux service user')
    import pwd

    uid = pwd.getpwnam(DEVICE_ID).pw_uid
    if uid == 0 or os.geteuid() != uid:
        raise ValueError('R1 requires the Linux service user')
    for parent in {*config.parents, *state.parents}:
        info = parent.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise ValueError('unsafe R1 system directory')
    info = config.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != 0
            or info.st_mode & 0o022):
        raise ValueError('unsafe R1 configuration file')
    for folder in (state, state / 'artifacts', state / 'journal'):
        info = folder.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != uid or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError('unsafe R1 state directory')
    try:
        info = (state / 'ledger.sqlite3').lstat()
    except FileNotFoundError:
        return
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != uid
            or stat.S_IMODE(info.st_mode) != 0o600):
        raise ValueError('unsafe R1 ledger file')


def read_systemd_key():
    folder = os.environ.get('CREDENTIALS_DIRECTORY')
    if not folder:
        raise ValueError('R1 systemd credential unavailable')
    path, info = regular_credential(Path(folder) / 'provider_api_key')
    if (info.st_uid not in (0, os.geteuid()) or info.st_mode & 0o227
            or (info.st_mode & 0o040 and info.st_gid not in (0, os.getegid()))):
        raise ValueError('R1 systemd credential permissions invalid')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as credential:
        if os.fstat(credential.fileno()) != info:
            raise ValueError('R1 systemd credential changed')
        raw = credential.read(8193)
    if len(raw) != info.st_size or any(separator in raw for separator in (b'\x00', b'\n', b'\r')):
        raise ValueError('R1 systemd credential invalid')
    try:
        return raw.decode('ascii')
    except UnicodeError:
        raise ValueError('R1 systemd credential invalid') from None


@contextmanager
def tick_lock(state):
    import fcntl

    descriptor = os.open(state / 'journal' / 'tick.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600):
            raise ValueError('unsafe R1 timer lock')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
        else:
            yield True
    finally:
        os.close(descriptor)


def _task_row(row):
    task = TaskRow(dict(row))
    for column in ('params', 'input_refs', 'required_caps', 'result'):
        encoded = task[column + '_json']
        task[column] = None if encoded is None else json.loads(encoded)
    return task


def _inspect_ledger(path, grant):
    """Check the dedicated R1 ledger without initializing or migrating it."""
    if not path.exists():
        return False, None
    template = canonical_json(asdict(grant.template()))
    with closing(sqlite3.connect(path.absolute().as_uri() + '?mode=ro', uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('BEGIN')
        if ledger_mode(conn) != 'isolated':
            raise DispatchError('R1 requires an isolated ledger')

        # An installer must reserve all writes for the singleton R1 runtime.
        # Both register_device and sweep otherwise operate on all isolated work.
        tasks = conn.execute('SELECT * FROM tasks LIMIT 2').fetchall()
        if len(tasks) > 1:
            raise DispatchError('unmanaged task in the R1 ledger')
        task = _task_row(tasks[0]) if tasks else None
        if task is not None and (
            task.execution_route is not None or task.side_effect != 'external'
            or canonical_json(asdict(task.spec)) != template or task.attempts not in (0, 1)
        ):
            raise DispatchError('unmanaged task in the R1 ledger')

        schedules = conn.execute('SELECT * FROM schedules LIMIT 2').fetchall()
        if len(schedules) > 1 or any(
            schedule['id'] != grant.grant_id or schedule['version'] != grant.version
            or schedule['execution_route'] is not None or schedule['max_slots'] != 1
            or canonical_json(json.loads(schedule['task_template'])) != template
            for schedule in schedules
        ):
            raise DispatchError('unmanaged schedule in the R1 ledger')

        firings = conn.execute('SELECT * FROM schedule_firings LIMIT 2').fetchall()
        if len(firings) > 1 or (firings and not schedules) or (task is not None and not firings) or any(
            firing['fire_key'] != grant.fire_key or firing['schedule_id'] != grant.grant_id
            or firing['version'] != grant.version or firing['scheduled_at'] != grant.scheduled_at
            or firing['execution_route'] is not None or firing['outcome'] not in ('pending', 'created')
            or canonical_json(json.loads(firing['task_template'])) != template
            or firing['outcome'] == 'created' and (task is None or firing['task_id'] != task.task_id)
            or firing['outcome'] == 'pending' and firing['task_id'] is not None
            for firing in firings
        ):
            raise DispatchError('unmanaged firing in the R1 ledger')

        devices = conn.execute('SELECT * FROM devices LIMIT 2').fetchall()
        if len(devices) > 1 or any(
            device['device_id'] != DEVICE_ID or device['execution_route'] is not None
            or json.loads(device['caps_json']) != [CAPABILITY] for device in devices
        ):
            raise DispatchError('unmanaged device in the R1 ledger')

        leases = conn.execute('SELECT * FROM leases LIMIT 2').fetchall()
        if len(leases) > 1 or any(
            task is None or lease['task_id'] != task.task_id or lease['device_id'] != DEVICE_ID
            or not devices or lease['fence'] != task.fence or task.attempts != 1 for lease in leases
        ):
            raise DispatchError('unmanaged lease in the R1 ledger')
        if task is not None and task.result is not None and (
            not isinstance(task.result, dict) or task.result.get('execution_mode') != grant.execution_mode
            or task.result.get('provider') != grant.provider
        ):
            raise DispatchError('agent result identity differs from grant')
        return True, task


def _summary(grant, task):
    result = (task.result or {}) if task is not None else {}
    summary = {
        'status': task.status if task is not None else 'not_submitted',
        'task_id': task.task_id if task is not None else None,
        'grant_id': grant.grant_id,
        'scheduled_at': grant.scheduled_at,
        'execution_mode': grant.execution_mode,
        'provider': grant.provider,
        'model': result.get('model', grant.model),
        'usage': result.get('usage'),
        'cost': None,
        'result_ref': result.get('result_ref'),
    }
    if grant.execution_mode == 'simulation':
        summary['real_calls'] = 0
    return summary


def query(state, grant):
    _require_explicit_mode(grant)
    exists, task = _inspect_ledger(state / 'ledger.sqlite3', grant)
    return _summary(grant, task) | {'ledger_exists': exists}


def _proof_identity(grant, summary):
    return {'schema': TICK_SCHEMA, 'credential_read': True, 'execution_mode': grant.execution_mode,
            'grant_id': grant.grant_id, 'grant_version': grant.version, 'scheduled_at': grant.scheduled_at,
            'task_id': summary['task_id'], 'status': summary['status']}


def _read_proof(state, grant, summary):
    try:
        with (state / 'journal' / 'last-tick.json').open('rb') as handle:
            raw = handle.read(CONFIG_LIMIT + 1)
        if len(raw) > CONFIG_LIMIT:
            return None
        proof = json.loads(raw.decode('utf-8'), object_pairs_hook=_unique_object)
    except (FileNotFoundError, ValueError, RecursionError):
        return None
    if not isinstance(proof, dict):
        return None
    invocation_id = proof.pop('invocation_id', None)
    pid = proof.pop('pid', None)
    try:
        matches = canonical_json(proof) == canonical_json(_proof_identity(grant, summary))
    except (ValueError, RecursionError):
        return None
    if (invocation_id is not None and (not isinstance(invocation_id, str)
            or not re.fullmatch(r'[0-9a-f]{32}', invocation_id)) or type(pid) is not int or pid <= 0
            or not matches):
        return None
    return proof | {'invocation_id': invocation_id, 'pid': pid}


def _write_proof(state, grant, summary):
    journal = state / 'journal'
    invocation_id = os.environ.get('INVOCATION_ID', '')
    proof = _proof_identity(grant, summary) | {
        'invocation_id': invocation_id if re.fullmatch(r'[0-9a-f]{32}', invocation_id) else None,
        'pid': os.getpid(),
    }
    temp = None
    try:
        with NamedTemporaryFile(dir=journal, prefix='.last-tick-', delete=False) as handle:
            temp = Path(handle.name)
            handle.write(canonical_json(proof))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, journal / 'last-tick.json')
        if os.name == 'posix':
            descriptor = os.open(journal, os.O_DIRECTORY | os.O_RDONLY | os.O_NOFOLLOW)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        if temp is not None:
            temp.unlink(missing_ok=True)


def check(state, grant):
    result = query(state, grant)
    if result['status'] == 'succeeded':
        ArtifactStore(state / 'artifacts').get(result['result_ref'])
    return result | {'check': 'configuration_ledger_and_artifact',
                     'credential_read_proof': _read_proof(state, grant, result)}


def _simulate(*, model, approved_text, api_key, max_tokens):
    return Completion('simulated-r1-completion', 'simulated-' + model, 'stop',
                      'Simulated R1 result. No provider request was sent.', TokenUsage(0, 0, 0, None, None))


def tick(state, grant, *, credential_reader=read_systemd_key, live_provider=complete_bounded,
         lock=tick_lock, clock=None):
    _require_explicit_mode(grant)
    with lock(state) as acquired:
        if not acquired:
            return _summary(grant, None) | {'status': 'busy'}
        ledger = state / 'ledger.sqlite3'
        _inspect_ledger(ledger, grant)
        key = credential_reader()
        if not isinstance(key, str) or not key:
            raise ValueError('R1 credential unavailable')
        store = SqliteStore(ledger, lease_ttl_seconds=LEASE_TTL_SECONDS, kinds={KIND: AgentKind()}, clock=clock)
        store.register_device(DEVICE_ID, [CAPABILITY])  # Also sweeps expired leases on this isolated ledger.
        provider = (_simulate if grant.execution_mode == 'simulation'
                    else partial(live_provider, deadline_seconds=DEADLINE_SECONDS))
        task = run_once(store, ArtifactStore(state / 'artifacts'), grant, device_id=DEVICE_ID,
                        api_key=key, provider_call=provider)
        summary = _summary(grant, task)
        _write_proof(state, grant, summary)
        return summary | {'credential_read': True}


def _require_explicit_mode(grant):
    if grant.execution_mode not in ('simulation', 'live'):
        raise ValueError('R1 requires an explicit simulation or live mode')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('tick', 'check', 'query'))
    args = parser.parse_args()
    try:
        _verify_service(CONFIG_PATH, STATE_PATH)
        grant = read_config(CONFIG_PATH)
        if args.command == 'tick':
            os.umask(0o077)
            result = tick(STATE_PATH, grant)
        elif args.command == 'check':
            result = check(STATE_PATH, grant)
        else:
            result = query(STATE_PATH, grant)
    except (DispatchError, KeyError, OSError, TypeError, ValueError, RecursionError, sqlite3.Error):
        print(json.dumps({'ok': False, 'error': 'r1_runtime_refused'}))
        return 1
    print(json.dumps({'ok': True} | result, ensure_ascii=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
