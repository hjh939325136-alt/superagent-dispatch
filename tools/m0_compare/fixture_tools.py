"""Isolated, bounded search and exact reads for the synthetic host comparison."""

from copy import deepcopy
from dataclasses import dataclass
from hashlib import sha256
import importlib
import json
from pathlib import Path
import re
import sqlite3
import sys
from threading import Lock
import unicodedata


ROOT = Path(__file__).resolve().parents[2] / 'knowledge' / 'm0_search'
DOMAINS = ('research-scatter', 'research-content')
CORPUS_PINS = {
    'research-scatter.json': 'f547c65a7faf6ae748272d8b0d00d4fbde21af2067603732c43e4d18333fb8db',
    'research-content.json': 'cc67496e095adcf8f70a01630515a02df8ad54342b2f6230e41b40b832c17b74',
    'grants.json': '7c4168047407b5971e31a247df5b60be5dc2df76a4bab6d54eb7c58fef0b6156',
}
MAX_CALLS = 32
MAX_RESPONSE_BYTES = 32 * 1024
ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,63}')
REF = re.compile(r'(research-scatter|research-content)/note/[0-9A-HJKMNP-TV-Z]{26}@[1-9][0-9]{0,8}#[a-f0-9]{64}')
INDEX_VERSION = re.compile(r'fixture-[a-f0-9]{64}')
_IMPORT_LOCK = Lock()


class FixtureToolError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _require(condition, code):
    if not condition:
        raise FixtureToolError(code)


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def _backend():
    # The legacy fixture scripts import their siblings as top-level modules.
    # Resolve only this repository-owned source and refuse a pre-existing alias
    # rather than calling whatever module the ambient interpreter found.
    names = ('fixture_store', 'signalhub_adapter', 'catalog', 'search')
    with _IMPORT_LOCK:
        sys.path.insert(0, str(ROOT))
        try:
            for name in names:
                module = importlib.import_module(name)
                _require(Path(module.__file__).resolve() == ROOT / (name + '.py'), 'FIXTURE_BACKEND_CONFLICT')
            return module
        finally:
            sys.path.remove(str(ROOT))


def _check_pins():
    for name, expected in CORPUS_PINS.items():
        path = ROOT / 'fixtures' / name
        _require(path.resolve() == path and path.is_file(), 'FIXTURE_CORPUS_CHANGED')
        try:
            with path.open('rb') as stream:
                raw = stream.read(64 * 1024 + 1)
        except OSError:
            raise FixtureToolError('FIXTURE_CORPUS_CHANGED') from None
        _require(len(raw) <= 64 * 1024 and sha256(raw).hexdigest() == expected, 'FIXTURE_CORPUS_CHANGED')


def _arguments(tool, arguments):
    _require(tool in ('knowledge.search', 'knowledge.get'), 'UNSUPPORTED_TOOL')
    _require(type(arguments) is dict, 'INVALID_REQUEST')
    domains = arguments.get('domains')
    _require(type(domains) is list and 1 <= len(domains) <= len(DOMAINS)
             and all(isinstance(domain, str) and domain in DOMAINS for domain in domains)
             and len(set(domains)) == len(domains), 'INVALID_REQUEST')
    if tool == 'knowledge.search':
        _require(arguments.keys() in ({'q', 'domains'}, {'q', 'domains', 'limit'}), 'INVALID_REQUEST')
        query, limit = arguments['q'], arguments.get('limit', 5)
        _require(isinstance(query, str) and 1 <= len(query) <= 200 and bool(query.strip())
                 and all(char in ' _-' or unicodedata.category(char)[0] in 'LMN' for char in query)
                 and type(limit) is int and 1 <= limit <= 10, 'INVALID_REQUEST')
        return {'q': query, 'domains': list(domains), 'limit': limit}
    _require(arguments.keys() == {'ref', 'domains'}, 'INVALID_REQUEST')
    ref = arguments['ref']
    match = REF.fullmatch(ref) if isinstance(ref, str) else None
    _require(match is not None and match[1] in domains, 'INVALID_REQUEST')
    return {'ref': ref, 'domains': list(domains)}


def _request(arguments):
    return {
        **arguments,
        'caller': {'agent': 'fixture-agent', 'task_id': 'fixture-task', 'purpose': 'fixture-review'},
        'scopes': ['research:read'],
        'max_class': 'internal',
        'as_of': '2026-11-15T00:00:00+08:00',
    }


def _failure(code, *, retryable=False, source=None):
    error = {'code': code, 'retryable': retryable}
    if source is not None:
        error['source'] = source
    return {'ok': False, 'error': error}


def _visible(row, domains):
    # Reuse the fixture server's visibility check and add an outbound fence.
    # Only its fixed result shape and the two authorized synthetic classes pass.
    keys = {'title', 'class', 'source_ref', 'observed_at', 'expires_at', 'human_reviewed',
            'human_locked', 'ref', 'status', 'snippet'}
    match = REF.fullmatch(row.get('ref', '')) if isinstance(row, dict) and isinstance(row.get('ref'), str) else None
    _require(match is not None and match[1] in domains and row.keys() == keys
             and row['class'] in ('public', 'internal') and row['status'] in ('current', 'expired')
             and row['human_reviewed'] is True and type(row['human_locked']) is bool
             and isinstance(row['snippet'], str) and len(row['snippet']) <= 240
             and isinstance(row['title'], str) and len(row['title']) <= 200
             and isinstance(row['source_ref'], str) and row['source_ref'].startswith(f'fixture:{match[1]}/'),
             'FIXTURE_OUTPUT_REFUSED')
    return row['ref']


@dataclass(frozen=True)
class _Run:
    experiment_id: str
    arm: str
    run_id: str
    scenario: str


class FixtureTools:
    """One in-process, broker-owned call sequence. A new instance has no replay history."""

    def __init__(self, *, experiment_id, arm, run_id, scenario='normal', audit=None):
        _require(all(isinstance(value, str) and ID.fullmatch(value) for value in (experiment_id, run_id))
                 and arm in ('current_worker', 'hermes')
                 and scenario in ('normal', 'recoverable', 'unrecoverable')
                 and (audit is None or callable(audit)), 'INVALID_RUN_BINDING')
        self._run = _Run(experiment_id, arm, run_id, scenario)
        self._audit = audit
        self._lock = Lock()
        self._replies = {}
        self._refs = set()
        self._transient_delivered = False

    def _call(self, tool, arguments):
        domains = arguments['domains']
        if (self._run.scenario == 'recoverable' and tool == 'knowledge.search'
                and not self._transient_delivered):
            return _failure('TEMPORARY_UNAVAILABLE', retryable=True), ()
        if self._run.scenario == 'unrecoverable' and 'research-content' in domains:
            return _failure('SOURCE_UNAVAILABLE', source='research-content'), ()
        backend = _backend()
        handler = backend.search if tool == 'knowledge.search' else backend.get
        try:
            result = handler(_request(arguments), 'fixture-both')
        except backend.SearchError as error:
            code = error.code if error.code in ('ACCESS_DENIED', 'NOT_FOUND', 'INVALID_REQUEST') else 'FIXTURE_UNAVAILABLE'
            return _failure(code), ()
        except (OSError, ValueError, sqlite3.Error):
            return _failure('FIXTURE_UNAVAILABLE'), ()
        if tool == 'knowledge.search':
            rows, withheld = result['results'], result['withheld']
            _require(isinstance(rows, list) and len(rows) <= arguments['limit']
                     and withheld.keys() == {'restricted', 'revoked', 'out_of_scope', 'unreviewed'}
                     and all(type(count) is int and 0 <= count <= 60 for count in withheld.values()),
                     'FIXTURE_OUTPUT_REFUSED')
            refs = tuple(_visible(row, domains) for row in rows)
        else:
            found = _visible(result['result'], domains)
            _require(found == arguments['ref'], 'FIXTURE_OUTPUT_REFUSED')
            refs = (found,)
        versions = result['index_versions']
        _require(versions and set(versions) <= set(domains)
                 and all(isinstance(version, str) and INDEX_VERSION.fullmatch(version) for version in versions.values()),
                 'FIXTURE_OUTPUT_REFUSED')
        visible = {'results': rows, 'withheld': withheld} if tool == 'knowledge.search' else {'result': result['result']}
        return {'ok': True, **visible, 'index_versions': versions}, refs

    def _check_refs(self):
        _check_pins()
        if not self._refs:
            return
        backend = _backend()
        for ref in sorted(self._refs):
            domains = [ref.split('/', 1)[0]]
            try:
                value = backend.get(_request({'ref': ref, 'domains': domains}), 'fixture-both')
                _require(_visible(value['result'], domains) == ref, 'REFERENCE_NOT_VISIBLE')
            except (backend.SearchError, OSError, ValueError, sqlite3.Error, KeyError, TypeError):
                raise FixtureToolError('REFERENCE_NOT_VISIBLE') from None
        _check_pins()

    def successful_refs(self):
        """Return every reference delivered, including text in search previews."""
        with self._lock:
            return tuple(sorted(self._refs))

    def revalidate_refs(self):
        """Recheck the grant and current source visibility before forwarding accumulated context."""
        with self._lock:
            try:
                self._check_refs()
            except FixtureToolError:
                return False
            return True

    def invoke(self, sequence, tool, arguments):
        """Serve a new zero-based sequence or replay the exact audited invocation."""
        _require(type(sequence) is int and 0 <= sequence < MAX_CALLS, 'INVALID_SEQUENCE')
        request = _arguments(tool, arguments)
        fingerprint = sha256(_encoded([tool, request])).hexdigest()
        with self._lock:
            _check_pins()
            if sequence in self._replies:
                saved, response = self._replies[sequence]
                _require(saved == fingerprint, 'SEQUENCE_CONFLICT')
                self._check_refs()
                return deepcopy(response)
            _require(sequence == len(self._replies), 'INVALID_SEQUENCE')
            response, refs = self._call(tool, request)
            _check_pins()
            _require(len(_encoded(response)) <= MAX_RESPONSE_BYTES, 'FIXTURE_OUTPUT_LIMIT')
            code = 'OK' if response['ok'] else response['error']['code']
            if self._audit is not None:
                event = {'experiment_id': self._run.experiment_id, 'arm': self._run.arm, 'run_id': self._run.run_id,
                         'scenario': self._run.scenario, 'sequence': sequence, 'tool': tool,
                         'request_sha256': fingerprint, 'response_sha256': sha256(_encoded(response)).hexdigest(),
                         'code': code, 'requested_ref': request.get('ref'), 'delivered_refs': list(refs)}
                try:
                    self._audit(event)
                except Exception:
                    raise FixtureToolError('AUDIT_REFUSED') from None
            self._replies[sequence] = (fingerprint, deepcopy(response))
            self._refs.update(refs)
            self._transient_delivered |= code == 'TEMPORARY_UNAVAILABLE'
            return deepcopy(response)
