"""Operate central policy metadata on an explicitly selected synthetic mixed ledger."""

import argparse
import inspect
import json
import sqlite3
import sys

from .central_control import AUTHORITY, CentralControl, _local_path
from .control_cli import ControlRequestError, load_request
from .models import DispatchError

REQUEST_SCHEMA = 'sa_dispatch.central_control_request.v1'
RESPONSE_SCHEMA = 'sa_dispatch.central_control_response.v1'
_OPERATIONS = {
    'status': CentralControl.status,
    'budget_upsert': CentralControl.budget_upsert,
    'handoff_offer': CentralControl.handoff_offer,
    'handoff_transition': CentralControl.handoff_transition,
    'handoff_expire': CentralControl.handoff_expire,
}


def _validate_request(request):
    if not isinstance(request, dict) or set(request) != {'schema', 'synthetic', 'operation', 'params'}:
        raise ControlRequestError('request requires exactly schema, synthetic, operation and params')
    if request['schema'] != REQUEST_SCHEMA or request['synthetic'] is not True:
        raise ControlRequestError('explicit synthetic central_control_request.v1 envelope required')
    operation, params = request['operation'], request['params']
    if not isinstance(operation, str) or operation not in _OPERATIONS:
        raise ControlRequestError('unsupported central control operation')
    if not isinstance(params, dict):
        raise ControlRequestError('operation params must be an object')
    try:
        inspect.signature(_OPERATIONS[operation]).bind(None, **params)
    except TypeError as exc:
        raise ControlRequestError('missing or unexpected operation parameters') from exc
    return operation, params


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ControlRequestError(message)


def main(argv=None):
    parser = _Parser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--central-maintenance', action='store_true', required=True)
    parser.add_argument('--expected-activation-ref', required=True)
    parser.add_argument('--ledger', required=True)
    parser.add_argument('--artifacts', required=True)
    parser.add_argument('--request', required=True)
    response = {'schema': RESPONSE_SCHEMA, 'messages_sent': 0}
    try:
        args = parser.parse_args(argv)
        operation, params = _validate_request(load_request(_local_path(args.request)))
        control = CentralControl(args.ledger, args.artifacts, args.expected_activation_ref, authority=AUTHORITY)
        result = _OPERATIONS[operation](control, **params)
        response.update(ok=True, operation=operation, result=result, scope=AUTHORITY,
                        status_refreshes_handoff_expiry=False)
        code = 0
    except (DispatchError, OSError, ValueError, TypeError, RecursionError, sqlite3.Error) as exc:
        response.update(ok=False, error={'code': getattr(exc, 'code', 'E_CONTROL_IO'),
                                        'message': str(exc), 'retryable': getattr(exc, 'retryable', False)})
        code = getattr(exc, 'exit_code', 2)
    sys.stdout.buffer.write(json.dumps(response, ensure_ascii=False, sort_keys=True).encode('utf-8') + b'\n')
    return code


if __name__ == '__main__':
    raise SystemExit(main())
