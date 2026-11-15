"""Trusted local synthetic ControlPolicy JSON-file entry point.

python -m sa_dispatch.control_cli --ledger ledger.sqlite3 --artifacts artifacts --request request.json

Request: {"schema":"sa_dispatch.control_request.v1","synthetic":true,
          "operation":"status","params":{}}
Operation names and params match the seven documented ControlPolicy methods.
No message/host invocation, claim or arbitrary method dispatch is exposed.
status() deliberately commits local handoff expiry updates as its API specifies.
"""
import argparse
from contextlib import closing
import inspect
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys

from .artifacts import ArtifactError, ArtifactStore
from .control_policy import ControlPolicy
from .models import DispatchError
from .store_sqlite import SqliteStore

REQUEST_SCHEMA = 'sa_dispatch.control_request.v1'
RESPONSE_SCHEMA = 'sa_dispatch.control_response.v1'
MAX_REQUEST_BYTES = 64 * 1024
OPERATIONS = frozenset(('budget_upsert', 'bind_task', 'approval_decide', 'settle',
                       'handoff_offer', 'handoff_transition', 'status'))


class ControlRequestError(DispatchError):
    code = 'E_CONTROL_REQUEST'


def _local_path(value, *, must_exist=False, regular=False):
    value = os.fspath(value)
    if value.startswith(('\\\\', '//')) or '://' in value:
        raise ControlRequestError('only explicit local filesystem paths are supported')
    path = Path(os.path.abspath(value))
    for component in reversed([path, *path.parents]):
        try:
            info = os.lstat(component)
        except FileNotFoundError:
            if must_exist: raise ControlRequestError('explicit local path does not exist') from None
            break
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise ControlRequestError('symlink and junction paths are not accepted by this local entry point')
    if must_exist and regular and not path.is_file():
        raise ControlRequestError('a regular local file is required')
    return path


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result: raise ControlRequestError('duplicate JSON object keys are not allowed')
        result[key] = value
    return result


def load_request(path):
    path = _local_path(path, must_exist=True, regular=True)
    with path.open('rb') as source:
        data = source.read(MAX_REQUEST_BYTES + 1)
    if len(data) > MAX_REQUEST_BYTES: raise ControlRequestError('control request exceeds 64 KiB')
    def invalid_constant(_): raise ControlRequestError('nonfinite JSON numbers are not allowed')
    try:
        return json.loads(data.decode('utf-8-sig'), object_pairs_hook=_unique_object, parse_constant=invalid_constant)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ControlRequestError('request must contain one UTF-8 JSON object') from exc


def validate_request(request):
    if not isinstance(request, dict) or set(request) != {'schema', 'synthetic', 'operation', 'params'}:
        raise ControlRequestError('request requires exactly schema, synthetic, operation and params')
    if request['schema'] != REQUEST_SCHEMA or request['synthetic'] is not True:
        raise ControlRequestError('explicit synthetic control_request.v1 envelope required')
    operation, params = request['operation'], request['params']
    if not isinstance(operation, str) or operation not in OPERATIONS:
        raise ControlRequestError('unsupported control operation')
    if not isinstance(params, dict): raise ControlRequestError('operation params must be an object')
    try:
        inspect.signature(getattr(ControlPolicy, operation)).bind(None, **params)
    except TypeError as exc:
        raise ControlRequestError('missing or unexpected operation parameters') from exc
    return operation, params


def _assert_synthetic_ledger(path):
    path = _local_path(path, must_exist=True, regular=True)
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as conn:
        conn.execute('PRAGMA query_only=ON')
        if conn.execute("SELECT 1 FROM tasks WHERE access IS NULL OR access!='synthetic' OR egress IS NULL OR egress!='none' LIMIT 1").fetchone():
            raise ControlRequestError('control CLI requires an exclusively synthetic no-egress ledger')
        if conn.execute("SELECT 1 FROM evidence WHERE access IS NULL OR access!='synthetic' LIMIT 1").fetchone():
            raise ControlRequestError('control CLI refuses non-synthetic evidence')
    return path


def execute_request(store, artifacts, request_dict):
    """Shared handler for the standalone and normal CLI; errors stay structured upstream.

    Actor names are operator-supplied fixture labels, never authenticated identity.
    The handler mutates only policy tables in the explicitly supplied local ledger.
    """
    operation, params = validate_request(request_dict)
    _assert_synthetic_ledger(store.path)
    if not isinstance(artifacts, ArtifactStore):
        raise ControlRequestError('an explicit local ArtifactStore is required')
    _local_path(artifacts.root)
    policy = ControlPolicy(store, artifacts=artifacts)
    result = getattr(policy, operation)(**params)
    if operation == 'approval_decide': result = {'approval_id': result}
    return {'operation': operation, 'result': result, 'scope': 'trusted_local_synthetic',
            'messages_sent': 0, 'status_refreshes_handoff_expiry': operation == 'status'}


class _Parser(argparse.ArgumentParser):
    def error(self, message): raise ControlRequestError(message)


def main(argv=None):
    parser = _Parser(description=__doc__)
    parser.add_argument('--ledger', required=True)
    parser.add_argument('--artifacts', required=True)
    parser.add_argument('--request', required=True)
    try:
        args = parser.parse_args(argv)
        request = load_request(args.request)
        validate_request(request)
        # Fail before SqliteStore can create/migrate an unintended or real ledger.
        ledger = _assert_synthetic_ledger(args.ledger)
        artifacts = ArtifactStore(_local_path(args.artifacts))
        store = SqliteStore(ledger, lease_ttl_seconds=60)
        response = {'schema': RESPONSE_SCHEMA, 'ok': True, **execute_request(store, artifacts, request)}
        code = 0
    except (DispatchError, ArtifactError, OSError, ValueError, TypeError, sqlite3.Error) as exc:
        response = {'schema': RESPONSE_SCHEMA, 'ok': False, 'messages_sent': 0,
                    'error': {'code': getattr(exc, 'code', 'E_CONTROL_IO'),
                              'message': str(exc), 'retryable': getattr(exc, 'retryable', False)}}
        code = getattr(exc, 'exit_code', 2)
    sys.stdout.buffer.write(json.dumps(response, ensure_ascii=False, sort_keys=True).encode('utf-8')+b'\n')
    return code


if __name__ == '__main__':
    raise SystemExit(main())
