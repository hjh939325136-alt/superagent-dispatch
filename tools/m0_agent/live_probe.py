"""Explicit one-request DeepSeek probe with a durable local attempt record."""

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.m0_agent.deepseek_client import DeepSeekError
from tools.m0_agent.provider_process import ERROR_KINDS, complete_bounded
from tools.m0_installer.local_secret import normalize_key, read_protected_file


SCHEMA = 'superagent.deepseek_probe.v1'
EXPECTED = 'SUPERAGENT_OK'
PROMPT = 'Reply with exactly SUPERAGENT_OK and no other text.'
MAX_TOKENS = 32
RECORD_LIMIT = 4096
MODEL_PATTERN = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}')
FILE_PATTERN = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}')
PROVIDER_ERRORS = ERROR_KINDS | {'process_start_failed', 'deadline_exceeded', 'provider_process_failed'}
USAGE_FIELDS = {'prompt_tokens', 'completion_tokens', 'total_tokens',
                'prompt_cache_hit_tokens', 'prompt_cache_miss_tokens'}
RESPONSE_STATES = {'matched', 'unexpected_reply', 'truncated'}


def _record_path(path):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts or not FILE_PATTERN.fullmatch(path.name):
        raise ValueError('absolute probe record path required')
    if os.name == 'nt' and (path.is_reserved() or any(':' in part for part in path.parts[1:])):
        raise ValueError('invalid probe record path')
    return path


def _append(handle, record):
    handle.write(json.dumps(record, sort_keys=True, separators=(',', ':')).encode('ascii') + b'\n')
    handle.flush()
    os.fsync(handle.fileno())


def _persist_directory(path):
    if os.name == 'posix':
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _call(secret_file, model, credential_reader, provider):
    try:
        key = normalize_key(credential_reader(secret_file)).decode('ascii')
    except (OSError, TypeError, ValueError):
        return {'status': 'credential_refused'}
    try:
        response = provider(model=model, approved_text=PROMPT, api_key=key, max_tokens=MAX_TOKENS,
                            thinking='disabled')
        usage = asdict(response.usage)
        if not _valid_usage(usage):
            return {'status': 'unknown', 'error': 'invalid_response'}
        if response.truncated:
            status = 'truncated'
        elif response.text == EXPECTED:
            status = 'matched'
        else:
            status = 'unexpected_reply'
        return {'status': status, 'usage': usage,
                'response_sha256': hashlib.sha256(response.text.encode('utf-8')).hexdigest()}
    except DeepSeekError as error:
        kind = error.kind if isinstance(error.kind, str) and error.kind in PROVIDER_ERRORS else 'invalid_response'
        outcome_unknown = error.outcome_unknown or kind in {
            'deadline_exceeded', 'timeout', 'connection_error', 'provider_process_failed', 'invalid_response'}
        return {'status': 'unknown' if outcome_unknown else 'provider_refused', 'error': kind}
    except Exception:
        return {'status': 'unknown', 'error': 'provider_process_failed'}


def run_probe(secret_file, model, record_path, *, credential_reader=read_protected_file,
              provider=complete_bounded):
    """Reserve the path before reading a credential or invoking the provider."""
    path = _record_path(record_path)
    if not isinstance(model, str) or not MODEL_PATTERN.fullmatch(model):
        raise ValueError('explicit provider model required')
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_BINARY', 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, 'wb') as record:
        intent = {'schema': SCHEMA, 'model': model, 'status': 'intent'}
        _append(record, intent)
        _persist_directory(path.parent)
        outcome = _call(secret_file, model, credential_reader, provider)
        _append(record, outcome)
    return intent | outcome


def _valid_usage(usage):
    return (isinstance(usage, dict) and set(usage) == USAGE_FIELDS
            and all(type(usage[name]) is int and 0 <= usage[name] < 2**63
                    for name in ('prompt_tokens', 'completion_tokens', 'total_tokens'))
            and all(usage[name] is None or type(usage[name]) is int and 0 <= usage[name] < 2**63
                    for name in ('prompt_cache_hit_tokens', 'prompt_cache_miss_tokens')))


def _valid_outcome(value):
    if not isinstance(value, dict) or not isinstance(value.get('status'), str):
        return False
    status = value['status']
    if status == 'credential_refused':
        return set(value) == {'status'}
    if status in ('unknown', 'provider_refused'):
        return (set(value) == {'status', 'error'} and isinstance(value['error'], str)
                and value['error'] in PROVIDER_ERRORS)
    if status in RESPONSE_STATES:
        return (set(value) == {'status', 'usage', 'response_sha256'}
                and isinstance(value['response_sha256'], str)
                and re.fullmatch(r'[a-f0-9]{64}', value['response_sha256']) is not None
                and _valid_usage(value['usage']))
    return False


def read_status(record_path):
    """Read only bounded, validated metadata. An incomplete record stays reserved."""
    path = _record_path(record_path)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError('invalid probe record file')
    flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
    flags |= getattr(os, 'O_BINARY', 0)
    with os.fdopen(os.open(path, flags), 'rb') as record:
        before = os.fstat(record.fileno())
        if not os.path.samestat(info, before) or not stat.S_ISREG(before.st_mode):
            raise ValueError('probe record changed')
        raw = record.read(RECORD_LIMIT + 1)
    interrupted = {'schema': SCHEMA, 'status': 'unknown', 'error': 'incomplete_record'}
    if len(raw) > RECORD_LIMIT:
        return interrupted
    lines = raw.splitlines(keepends=True)
    try:
        intent = json.loads(lines[0]) if lines and lines[0].endswith(b'\n') else None
        if (not isinstance(intent, dict) or set(intent) != {'schema', 'model', 'status'}
                or intent['schema'] != SCHEMA or intent['status'] != 'intent'
                or not isinstance(intent['model'], str) or not MODEL_PATTERN.fullmatch(intent['model'])):
            return interrupted
        outcome = json.loads(lines[1]) if len(lines) == 2 and lines[1].endswith(b'\n') else None
        if _valid_outcome(outcome):
            return intent | outcome
        return interrupted | {'model': intent['model']}
    except (ValueError, RecursionError):
        return interrupted


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    commands = parser.add_subparsers(dest='command', required=True)
    run = commands.add_parser('run', allow_abbrev=False)
    run.add_argument('--secret-file', type=Path, required=True)
    run.add_argument('--model', required=True)
    run.add_argument('--record', type=Path, required=True)
    status = commands.add_parser('status', allow_abbrev=False)
    status.add_argument('--record', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        report = (run_probe(args.secret_file, args.model, args.record) if args.command == 'run'
                  else read_status(args.record))
    except FileExistsError:
        report = {'schema': SCHEMA, 'status': 'record_already_exists'}
    except FileNotFoundError:
        report = {'schema': SCHEMA, 'status': 'record_unavailable'}
    except (OSError, ValueError):
        report = {'schema': SCHEMA, 'status': 'record_refused'}
    print(json.dumps(report, sort_keys=True))
    readable = args.command == 'status' and report['status'] in (
        RESPONSE_STATES | {'unknown', 'provider_refused', 'credential_refused'})
    return 0 if report['status'] == 'matched' or readable else 1


if __name__ == '__main__':
    raise SystemExit(main())
