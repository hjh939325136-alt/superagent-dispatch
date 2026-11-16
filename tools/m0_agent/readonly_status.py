"""Expose the fixed R1 service status as one bounded JSON line."""

import json
import re
import sys

from . import runtime


SCHEMA_VERSION = 1
OUTPUT_LIMIT = 1024
TASK_STATES = frozenset({
    'queued', 'waiting_device', 'leased', 'succeeded', 'no_new', 'failed', 'unknown', 'cancelled',
})
TOKEN_COUNTS = ('prompt_tokens', 'completion_tokens', 'total_tokens')
CACHE_COUNTS = ('prompt_cache_hit_tokens', 'prompt_cache_miss_tokens')


def _usage(value):
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError
    counts = {field: value[field] for field in TOKEN_COUNTS}
    counts.update({field: value.get(field) for field in CACHE_COUNTS})
    for field, count in counts.items():
        if field in CACHE_COUNTS and count is None:
            continue
        if type(count) is not int or not 0 <= count < 2**31:
            raise ValueError
    return counts


def _status(summary, grant):
    task_id = summary['task_id']
    status = summary['status']
    ledger_exists = summary['ledger_exists']
    if type(ledger_exists) is not bool:
        raise ValueError
    if status == 'not_submitted':
        if task_id is not None:
            raise ValueError
    elif (status not in TASK_STATES or not ledger_exists or not isinstance(task_id, str)
          or re.fullmatch(r't_[0-9a-f]{32}', task_id) is None):
        raise ValueError

    result_ref = summary['result_ref']
    if result_ref is not None and (not isinstance(result_ref, str)
                                   or re.fullmatch(r'sha256:[0-9a-f]{64}', result_ref) is None):
        raise ValueError

    result = {
        'schema_version': SCHEMA_VERSION,
        'ok': True,
        'task_id': task_id,
        'status': status,
        'scheduled_at': grant.scheduled_at,
        'requested_model': grant.model,
        'execution_mode': grant.execution_mode,
        'usage': _usage(summary['usage']),
        'cost': None,
        'result_ref': result_ref,
        'ledger_exists': ledger_exists,
    }
    if grant.execution_mode == 'simulation':
        result['real_calls'] = 0
    return result


def _error(code):
    return json.dumps({'schema_version': SCHEMA_VERSION, 'ok': False, 'error': code})


def main():
    if len(sys.argv) != 1:
        print(_error('r1_status_invalid_arguments'))
        return 2
    try:
        runtime._verify_service(runtime.CONFIG_PATH, runtime.STATE_PATH)
        grant = runtime.read_config(runtime.CONFIG_PATH)
        summary = runtime.query(runtime.STATE_PATH, grant)
        output = json.dumps(_status(summary, grant), ensure_ascii=True)
        if len(output) + 1 > OUTPUT_LIMIT:
            raise ValueError
    except Exception:
        # The fixed remote-facing error contains no exception details.
        print(_error('r1_status_refused'))
        return 1
    print(output)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
