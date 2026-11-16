"""Bound the entire fixed provider call; credentials travel only over a pipe."""
from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.m0_agent.deepseek_client import (THINKING_MODES, Completion, DeepSeekError, TokenUsage,
                                            allowed_connect_proxy, complete_text)

INPUT_LIMIT = 72 * 1024
OUTPUT_LIMIT = 320 * 1024
REQUEST_FIELDS = {'model', 'approved_text', 'api_key', 'max_tokens'}
OPTIONAL_FIELDS = {'thinking', 'https_connect_proxy'}
ERROR_KINDS = frozenset(('invalid_request', 'response_too_large', 'incomplete_response',
    'timeout', 'connection_error', 'auth_rejected', 'rate_limited', 'redirect_rejected',
    'provider_error', 'invalid_response'))


def _argv():
    return [str(Path(sys.executable).resolve()), '-I', '-B', str(Path(__file__).resolve()), '--child']


def _child():
    try:
        raw = sys.stdin.buffer.read(INPUT_LIMIT + 1)
        if len(raw) > INPUT_LIMIT:
            raise DeepSeekError('invalid_request')
        request = json.loads(raw)
        if (not isinstance(request, dict)
                or not REQUEST_FIELDS <= set(request) <= REQUEST_FIELDS | OPTIONAL_FIELDS
                or ('thinking' in request and request['thinking'] not in THINKING_MODES)
                or ('https_connect_proxy' in request and not allowed_connect_proxy(request['https_connect_proxy']))):
            raise DeepSeekError('invalid_request')
        result = complete_text(**request)
        response = {'ok': True, 'completion': asdict(result)}
    except DeepSeekError as error:
        response = {'ok': False, 'kind': error.kind if error.kind in ERROR_KINDS else 'invalid_response',
                    'status': error.status, 'outcome_unknown': error.outcome_unknown}
    except Exception:
        response = {'ok': False, 'kind': 'invalid_response', 'status': None, 'outcome_unknown': True}
    # No traceback, raw request, provider body or credential diagnostics.
    encoded = json.dumps(response, ensure_ascii=False, allow_nan=False).encode('utf-8')
    if len(encoded) > OUTPUT_LIMIT:
        encoded = b'{"ok":false,"kind":"response_too_large","status":null,"outcome_unknown":true}'
    sys.stdout.buffer.write(encoded)
    sys.stdout.buffer.flush()


def complete_bounded(*, model, approved_text, api_key, max_tokens, deadline_seconds=45,
                     thinking=None, https_connect_proxy=None):
    """One child, one request. Timeout is ambiguous and never automatically retried."""
    if (type(deadline_seconds) not in (int, float) or not 0 < deadline_seconds <= 120
            or (thinking is not None and thinking not in THINKING_MODES)
            or (https_connect_proxy is not None and not allowed_connect_proxy(https_connect_proxy))):
        raise DeepSeekError('invalid_request')
    request = dict(model=model, approved_text=approved_text, api_key=api_key, max_tokens=max_tokens)
    if thinking is not None:
        request['thinking'] = thinking
    if https_connect_proxy is not None:
        request['https_connect_proxy'] = https_connect_proxy
    try:
        raw = json.dumps(request, ensure_ascii=False, allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, UnicodeError):
        raw = None
    if raw is None or len(raw) > INPUT_LIMIT:
        raise DeepSeekError('invalid_request')
    environment = {key: value for key, value in os.environ.items()
                   if key in ('SYSTEMROOT', 'WINDIR', 'TEMP', 'TMP', 'TMPDIR')}
    failed_to_start = False
    try:
        process = subprocess.Popen(_argv(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, env=environment, shell=False,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0) if os.name == 'nt' else 0)
    except OSError:
        failed_to_start = True
    if failed_to_start:
        raise DeepSeekError('process_start_failed')
    timed_out = False
    try:
        output, _ = process.communicate(raw, timeout=deadline_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        timed_out = True
    if timed_out:
        raise DeepSeekError('deadline_exceeded', outcome_unknown=True)
    if process.returncode != 0 or len(output) > OUTPUT_LIMIT:
        raise DeepSeekError('provider_process_failed', outcome_unknown=True)
    error = None
    try:
        envelope = json.loads(output)
        if envelope.get('ok') is False:
            if (set(envelope) != {'ok','kind','status','outcome_unknown'} or
                    envelope['kind'] not in ERROR_KINDS or type(envelope['outcome_unknown']) is not bool or
                    envelope['status'] is not None and (type(envelope['status']) is not int or
                                                        not 100 <= envelope['status'] <= 599)):
                raise ValueError()
            error = (envelope['kind'], envelope['status'], envelope['outcome_unknown'])
        elif envelope.get('ok') is True and set(envelope) == {'ok','completion'}:
            value = envelope['completion']
            if set(value) != {'response_id','model','finish_reason','text','usage'}:
                raise ValueError()
            usage = TokenUsage(**value.pop('usage'))
            result = Completion(**value, usage=usage)
        else:
            raise ValueError()
    except (ValueError, TypeError, KeyError, AttributeError):
        error = ('provider_process_failed', None, True)
    if error is not None:
        raise DeepSeekError(error[0], status=error[1], outcome_unknown=error[2])
    return result


if __name__ == '__main__':
    if sys.argv[1:] != ['--child']:
        raise SystemExit(2)
    _child()
