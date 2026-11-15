"""Metadata-only reader for the two M0 content fixtures; never opens a writer."""
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib
import json
import re

from .binding import fixed_ref
from .readonly import SourceError, readonly_db

SCHEMA = 'm0-content-synthetic-v1'
TRACES = {'synthetic-content-game', 'synthetic-content-ai-editing'}
ROLES = {'technical', 'manager', 'progress'}
REF = re.compile(r'^([a-z][a-z0-9_-]*)/([a-z][a-z0-9_-]*)/([A-Za-z0-9:_-]+)@([1-9][0-9]*)#([0-9a-f]{64})$')
EVENTS = {'artifact.put', 'approval.record', 'publish.prepare', 'fixture.submit_unknown',
          'publish.observe', 'publish.metrics_import', 'publish.metrics_match',
          'strategy.activate', 'strategy.rollback', 'content.script_from_context'}
STATES = {'prepared', 'submitted_unknown', 'published', 'rejected', 'withdrawn'}


def _hash(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _time(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise SourceError('source_invalid_timestamp')
    return parsed.astimezone(timezone.utc)


class _Reader:
    def __init__(self, db, traces, guard=None):
        self.db, self.traces, self.cache = db, set(traces), {}
        self.guard = guard

    def external(self, ref):
        # Reuse the hash-bound mapping decoder, never its writer constructor.
        from knowledge.m0_content.contract import Contract, ContractError
        reader = object.__new__(Contract)
        reader.db = self.db
        try:
            mapping = reader.external_context(ref)
        except ContractError as exc:
            if exc.code == 'EXTERNAL_DEPENDENCY_UNVERIFIED':
                return {'usable': False, 'state': 'unverified', 'reason_code': exc.code}
            raise SourceError('content_hash_mismatch') from exc
        if mapping is None:
            return None
        if self.guard is None:
            return {'usable': False, 'state': 'unverified', 'reason_code': 'DEPENDENCY_GUARD_REQUIRED'}
        return self.guard.check_external(mapping)

    def artifact(self, ref, trace=None):
        match = REF.fullmatch(ref) if isinstance(ref, str) else None
        if not match:
            raise SourceError('content_reference_invalid')
        if ref not in self.cache:
            row = self.db.execute('SELECT * FROM artifacts WHERE ref=?', (ref,)).fetchone()
            if not row or row['trace_id'] not in self.traces:
                raise SourceError('content_reference_not_found')
            if len(row['payload']) > 1024 * 1024:
                raise SourceError('source_limit_exceeded')
            data = json.loads(row['payload'])
            owner, kind, ident, version, sha = match.groups()
            if (not isinstance(data, dict) or data.get('synthetic') is not True or
                    data.get('trace_id') != row['trace_id'] or data.get('class') != 'internal' or
                    [data.get('owner'), data.get('kind'), data.get('id'), data.get('version')] !=
                    [owner, kind, ident, int(version)] or
                    row['identity'] != '/'.join((owner, kind, ident)) or row['version'] != int(version) or
                    _hash(data) != sha or not isinstance(data.get('derived_from'), list) or
                    not isinstance(data.get('body'), dict)):
                raise SourceError('content_hash_mismatch')
            self.cache[ref] = data
        value = self.cache[ref]
        if trace is not None and value['trace_id'] != trace:
            raise SourceError('E_SOURCE_FORBIDDEN')
        return value

    def descriptor(self, ref, trace):
        data = self.artifact(ref, trace)
        identity = ref.split('@', 1)[0]
        head = self.db.execute('SELECT ref FROM heads WHERE identity=?', (identity,)).fetchone()
        if not head:
            raise SourceError('content_head_missing')
        # Resolve every dependency; a ref is evidence only after actual hash readback.
        for parent in data['derived_from']:
            self.artifact(parent)
        closure, changed = self.closure(ref)
        external = [value for dependency in closure if (value := self.external(dependency)) is not None]
        return {'record_type': 'artifact', 'ref': ref, 'trace_id': trace,
                **{key: data[key] for key in ('owner', 'kind', 'id', 'version', 'derived_from')},
                'sha256': ref.rsplit('#', 1)[1], 'readback_verified': True,
                'state': 'current_version' if head['ref'] == ref else 'historical_version',
                'dependency_current_usable': not changed and all(value.get('usable') is True for value in external),
                'dependency_checks': external,
                'state_basis': 'current' if head['ref'] == ref else 'historical', 'synthetic': True}

    def closure(self, ref):
        found, pending, changed = set(), [ref], []
        while pending:
            item = pending.pop()
            if item in found:
                continue
            if len(found) >= 500:
                raise SourceError('source_limit_exceeded')
            data = self.artifact(item)
            found.add(item)
            head = self.db.execute('SELECT ref FROM heads WHERE identity=?', (item.split('@', 1)[0],)).fetchone()
            if not head:
                raise SourceError('content_head_missing')
            self.artifact(head['ref'])
            if head['ref'] != item:
                changed.append({'approved_ref': item, 'current_ref': head['ref']})
            pending.extend(data['derived_from'])
        return sorted(found), changed

    def approvals(self, trace):
        result = []
        rows = self.db.execute('''SELECT a.* FROM approvals a JOIN artifacts b ON a.ref=b.ref
            WHERE b.trace_id=? ORDER BY a.ref LIMIT 501''', (trace,)).fetchall()
        if len(rows) > 500:
            raise SourceError('source_limit_exceeded')
        for row in rows:
            artifact = self.artifact(row['ref'], trace)
            body = artifact['body']
            subject = self.artifact(row['subject_ref'], trace)
            bound, changed = self.closure(row['subject_ref'])
            grant = body.get('authorization', {})
            expected_scope = {'script': 'script_ready', 'take': 'cut_review'}.get(subject['kind'])
            if (artifact['owner'] != 'mgmt' or artifact['kind'] != 'approval' or
                    body.get('human_reviewed') is not False or body.get('publish_authorized') is not False or
                    grant.get('grant_id') != 'm0-content-contract-fixture-v1' or
                    grant.get('kind') != 'synthetic_fixture' or grant.get('actor') != 'synthetic-fixture-not-human'):
                raise SourceError('content_fixture_authorization_invalid')
            valid_binding = (row['scope'] == expected_scope == body.get('scope') and
                             row['subject_ref'] == body.get('subject_ref') and
                             type(body.get('version_bound')) is int and
                             row['version_bound'] == body['version_bound'] == subject['version'] and
                             bound == json.loads(row['upstream_refs']) == body.get('upstream_refs') and
                             _hash(bound) == body.get('upstream_sha256') and
                             artifact['derived_from'] == bound)
            state = 'fixture_approved' if valid_binding and not changed else 'stale'
            external = [value for ref in bound if (value := self.external(ref)) is not None]
            invalid_external = next((value for value in external if value.get('usable') is not True), None)
            if invalid_external:
                state = invalid_external.get('state', 'unverified')
            control = body.get('control_approval')
            if control is not None and (not isinstance(control, dict) or not control.get('expires_at') or
                                       _time(control['expires_at']) <= datetime.now(timezone.utc)):
                state = 'expired'
            replacements = {item['approved_ref']: item['current_ref'] for item in changed}
            result.append({'record_type': 'approval', 'ref': row['ref'], 'trace_id': trace,
                           'subject_ref': row['subject_ref'], 'version_bound': row['version_bound'],
                           'upstream_fingerprint': body.get('upstream_sha256') if valid_binding else None,
                           'observed_head_fingerprint': _hash(sorted(replacements.get(ref, ref) for ref in bound)),
                           'changed_upstreams': changed,
                           'state': state, 'usable': state == 'fixture_approved',
                           'dependency_checks': external, 'state_basis': 'current', 'human_reviewed': False,
                           'publish_authorized': False, 'authorization_source': 'fixture://m0-content/contract-test-specification-v1',
                           'readback_verified': True})
        return result

    def preparations(self, trace, now):
        result = []
        rows = self.db.execute('SELECT * FROM preparations WHERE trace_id=? ORDER BY preparation_id LIMIT 501', (trace,)).fetchall()
        if len(rows) > 500:
            raise SourceError('source_limit_exceeded')
        for row in rows:
            self.artifact(row['ref'], trace)
            if row['state'] not in STATES or row['simulated_external_calls'] not in (0, 1):
                raise SourceError('content_state_invalid')
            if row['evidence_ref']:
                self.artifact(row['evidence_ref'])
            overdue = row['state'] == 'submitted_unknown' and now - _time(row['unknown_at']) >= timedelta(hours=24)
            result.append({'record_type': 'preparation', 'preparation_id': row['preparation_id'],
                           'ref': row['ref'], 'trace_id': trace, 'state': row['state'], 'state_basis': 'current',
                           'effect': 'external', 'automatic_retry_allowed': False,
                           'needs_human': overdue, 'unknown_at': row['unknown_at'],
                           'evidence_ref': row['evidence_ref'], 'platform_submit': 'unverified',
                           'platform_calls': 0, 'simulated_external_calls': row['simulated_external_calls'],
                           'verification_status': 'unverified_import', 'synthetic': True})
        return result


def content_query(source: dict, op: str, args: dict, role: str) -> dict:
    """Read only registered fixture metadata; refs are resolved, never executed."""
    if role not in ROLES or op not in {'content.status', 'trace.get'}:
        raise SourceError('E_ROLE_FORBIDDEN')
    allowed = {'source_id', 'trace_id', 'limit'} | ({'ref'} if op == 'trace.get' else set())
    if not isinstance(args, dict) or set(args) - allowed:
        raise SourceError('E_FIELD_NOT_ALLOWED')
    if args.get('source_id', source.get('source_id')) != source.get('source_id'):
        raise SourceError('E_SOURCE_FORBIDDEN')
    options = source.get('options', {})
    traces = options.get('trace_ids')
    if (source.get('kind') != 'content' or set(options) not in ({'trace_ids'}, {'trace_ids', 'workflow_binding'}) or
            not isinstance(traces, list) or not traces or len(traces) != len(set(traces)) or
            not set(traces) <= TRACES):
        raise SourceError('binding_invalid')
    trace = args.get('trace_id')
    if trace is not None and trace not in traces:
        raise SourceError('E_SOURCE_FORBIDDEN')
    if op == 'trace.get' and trace is None:
        raise SourceError('E_FIELD_NOT_ALLOWED')
    limit = args.get('limit', 100)
    if type(limit) is not int or not 1 <= limit <= 500:
        raise SourceError('E_FIELD_NOT_ALLOWED')
    instant = datetime.now(timezone.utc)
    observed = instant.isoformat()
    with ExitStack() as stack:
        guard = None
        if 'workflow_binding' in options:
            from knowledge.m0_content.workflow_binding import load_workflow_binding
            from knowledge.m0_content.workflow_guard import dependency_guard
            binding = load_workflow_binding(options['workflow_binding'])
            if (binding.content_path != Path(source['locator']).absolute() or
                    binding.content_instance != source.get('source_instance') or not set(traces) <= set(binding.allowed_traces)):
                raise SourceError('E_SOURCE_FORBIDDEN')
            guard = stack.enter_context(dependency_guard(binding))
        db = stack.enter_context(readonly_db(source['locator']))
        meta = dict(db.execute('SELECT key,value FROM meta'))
        if meta.get('schema') != SCHEMA or SCHEMA not in source.get('accepted_schema_versions', []):
            raise SourceError('source_schema_unsupported')
        if meta.get('instance') != source.get('source_instance'):
            raise SourceError('source_instance_changed')
        if meta.get('platform_calls') != '0':
            raise SourceError('content_fixture_platform_boundary_invalid')
        reader = _Reader(db, traces, guard)
        items, attention = [], []
        for selected in ([trace] if trace else traces):
            if not db.execute('SELECT 1 FROM artifacts WHERE trace_id=? LIMIT 1', (selected,)).fetchone():
                raise SourceError('content_trace_not_found')
            if args.get('ref') is not None:
                items.append(reader.descriptor(args['ref'], selected))
                continue
            approvals = reader.approvals(selected)
            preparations = reader.preparations(selected, instant)
            for entry in [item for item in approvals if not item['usable']] + [item for item in preparations if item['needs_human']]:
                state = 'approval_upstream_changed' if entry['record_type'] == 'approval' else 'submitted_unknown_over_24h'
                alert = {'rule': 'A11@1', 'subject_id': entry.get('preparation_id', entry['ref']),
                         'trace_id': selected, 'state': state, 'state_basis': 'current', 'as_of': observed,
                         'owner': 'manager', 'next_step': 'inspect_evidence', 'evidence_refs': [entry['ref']]}
                alert['ref'] = fixed_ref(source, 'attention', alert['subject_id'], alert)
                attention.append(alert)
            summary = {'record_type': 'content_trace', 'trace_id': selected,
                       'state': 'submitted_unknown' if any(x['state'] == 'submitted_unknown' for x in preparations)
                                else 'approval_stale' if any(not x['usable'] for x in approvals)
                                else 'review_pending' if not approvals else 'fixture_ready',
                       'state_basis': 'current', 'approvals': approvals, 'preparations': preparations,
                       'human_reviewed': False, 'publish_authorized': False, 'platform_submit': 'unverified',
                       'platform_calls': 0, 'automatic_retry_allowed': False, 'synthetic': True}
            summary['ref'] = fixed_ref(source, 'content-trace', selected, summary)
            items.append(summary)
            if op == 'trace.get':
                refs = db.execute('SELECT ref FROM artifacts WHERE trace_id=? ORDER BY rowid LIMIT 501', (selected,)).fetchall()
                if len(refs) > 500:
                    raise SourceError('source_limit_exceeded')
                items.extend(reader.descriptor(row['ref'], selected) for row in refs)
                events = db.execute('SELECT * FROM content_events WHERE trace_id=? ORDER BY seq LIMIT 501', (selected,)).fetchall()
                if len(events) > 500:
                    raise SourceError('source_limit_exceeded')
                for event in events:
                    refs = json.loads(event['refs_json'])
                    for ref in refs:
                        reader.artifact(ref)
                    items.append({'record_type': 'content_event', 'event_id': event['event_id'], 'seq': event['seq'],
                                  'trace_id': selected, 'operation': event['operation'] if event['operation'] in EVENTS else 'redacted',
                                  'at': _time(event['at']).isoformat(), 'refs': refs,
                                  'state_basis': 'historical', 'synthetic': True})
        partial = len(items) > limit
        return {'items': items[:limit], 'coverage': 'partial' if partial else 'complete',
                'unknown': ['limit_exceeded'] if partial else [], 'observed_at': observed,
                'source_instance': meta['instance'], 'schema_version': SCHEMA,
                'state_basis': 'current', 'attention': attention}
