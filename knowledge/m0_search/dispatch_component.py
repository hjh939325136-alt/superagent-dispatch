"""Subprocess bridge for default fixtures or an explicitly bound fixture catalog."""
import argparse
import json
import sys
from pathlib import Path

from search import SearchError, search
from catalog import CatalogError
from dispatch_binding import OPERATIONS, execute_bound, load_binding, receipt_projection
import sqlite3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('invoke',))
    parser.add_argument('--request', type=Path, required=True)
    parser.add_argument('--json', action='store_true')
    parser.add_argument('--source-binding', type=Path)
    args = parser.parse_args()
    request_id = None
    try:
        request = json.loads(args.request.read_bytes())
        request_id = request.get('request_id')
        operations = OPERATIONS if args.source_binding else ('research.search',)
        if request.get('schema_version') != '0.1' or request.get('operation') not in operations or \
                not isinstance(request_id, str) or not request_id or request.get('idempotency_key') is not None:
            raise ValueError('invalid research invocation')
        if args.source_binding:
            binding = load_binding(args.source_binding)
            data = receipt_projection(execute_bound(request['operation'], request['payload'], binding), binding)
        else:
            data = search(request['payload'], 'fixture-both')
        result = {'schema_version': '0.1', 'request_id': request_id, 'ok': True, 'data': data}
        code = 0
    except (SearchError, CatalogError, KeyError, TypeError, ValueError, OSError, sqlite3.Error) as error:
        result = {'schema_version': '0.1', 'request_id': request_id, 'ok': False,
                  'error': {'code': getattr(error, 'code', 'INVALID_REQUEST')}}
        code = 2
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return code


if __name__ == '__main__':
    sys.exit(main())
