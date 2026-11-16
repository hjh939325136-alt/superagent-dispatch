"""Validate the two supported ignored completion audit profiles."""

from datetime import datetime

from . import domain_binding as archive


SCHEMA = 'm0.ignored_late_audit_review.v1'
CLASSIFICATION = 'unknown_submitter_preserved_untrusted_audit_not_accepted'
RECEIPT_SCHEMAS = {
    'runtime_fixture': 'm0_deploy.synthetic_receipt.v1',
    'recovery_ignored_claim': 'm0.synthetic_ignored_result.v1',
}
ROW_FIELDS = {'id', 'task_id', 'fence', 'disposition', 'outcome_json', 'received_at'}


class LateAuditError(ValueError):
    """An ignored completion audit row lies outside the checkpoint profiles."""


def _require(condition, message):
    if not condition:
        raise LateAuditError(message)


def _object(raw):
    _require(isinstance(raw, (str, bytes)) and len(raw) <= archive.META_LIMIT,
             'invalid ignored late receipt JSON size or type')
    try:
        encoded = raw.encode('utf-8') if isinstance(raw, str) else raw
        _require(len(encoded) <= archive.META_LIMIT, 'invalid ignored late receipt JSON size')
        value = archive._parse(encoded)
        _require(archive.canonical(value) == encoded, 'ignored late receipt JSON must be canonical')
        return value
    except (ValueError, RecursionError) as error:
        raise LateAuditError('invalid canonical ignored late receipt JSON') from error


def _outcome(raw):
    value = _object(raw)
    _require(set(value) == {'status', 'result', 'evidence'} and value['status'] == 'succeeded' and
             isinstance(value['result'], dict) and set(value['result']) == {'receipt'} and
             isinstance(value['evidence'], list) and len(value['evidence']) == 1 and
             isinstance(value['evidence'][0], dict), 'unsupported ignored late receipt outcome')
    ref = value['result']['receipt']
    evidence = value['evidence'][0]
    profile = evidence.get('kind')
    _require(archive._matches(archive.HASH_REF, ref) and isinstance(profile, str) and
             profile in RECEIPT_SCHEMAS and
             evidence == {'kind': profile, 'content_hash': ref, 'access': 'synthetic',
                          'ref': None, 'artifact_hash': ref}, 'unsupported ignored late receipt evidence')
    return ref, profile


def _receipt(cas, ref, profile, task_id, fence):
    try:
        raw, _ = archive._read(cas.path(ref), archive.META_LIMIT)
    except (OSError, ValueError) as error:
        raise LateAuditError('cannot read ignored late receipt CAS object') from error
    _require('sha256:' + archive._digest(raw) == ref, 'ignored late receipt CAS hash mismatch')
    value = _object(raw)
    _require(set(value) == {'schema', 'synthetic', 'run_id', 'task_id', 'fence'} and
             value['schema'] == RECEIPT_SCHEMAS[profile] and value['synthetic'] is True and
             isinstance(value['run_id'], str) and bool(value['run_id'].strip()) and
             value['task_id'] == task_id and type(value['fence']) is int and value['fence'] == fence,
             'ignored late receipt CAS schema or task/fence mismatch')


def _runtime_attempt(task, lease, current, grants, fence, ref, cas):
    _require(task['side_effect'] == 'none' and task['status'] == 'succeeded' and
             fence < task['fence'] and lease['state'] == 'expired' and lease['completion_json'] is None and
             current is not None and current['state'] == 'completed',
             'ignored runtime receipt requires expired and separately completed fences')
    _require((task['task_id'], lease['lease_id'], ref) in grants,
             'ignored runtime receipt lacks its historical synthetic transport grant')
    current_ref, current_profile = _outcome(current['completion_json'])
    _require(current_profile == 'runtime_fixture' and current_ref != ref and
             _object(task['result_json']) == {'receipt': current_ref},
             'ignored runtime receipt requires a separate current receipt')
    _receipt(cas, current_ref, current_profile, task['task_id'], task['fence'])
    return current_ref


def inventory(records, tasks, allowed_unknown_task_ids, cas):
    """Return an exact review descriptor and explicit CAS roots for raw ignored audit."""
    if not records['late_results']:
        return None, set()
    allowed = set(allowed_unknown_task_ids)
    leases = {(row['task_id'], row['fence']): row for row in records['leases']}
    grants = {(row['task_id'], row['lease_id'], row['content_hash'])
              for row in records['transport_artifact_grants'] if row['access'] == 'synthetic'}
    rows, roots, seen = [], set(), set()
    for row in records['late_results']:
        _require(set(row) == ROW_FIELDS and archive._matches(archive.IDENTITY, row['id']) and
                 row['id'] not in seen and archive._matches(archive.IDENTITY, row['task_id']) and
                 type(row['fence']) is int and row['fence'] > 0 and row['disposition'] == 'ignored' and
                 isinstance(row['outcome_json'], str) and isinstance(row['received_at'], str) and
                 len(row['received_at']) <= 64, 'invalid ignored late receipt audit row')
        try:
            received = datetime.fromisoformat(row['received_at'])
        except ValueError as error:
            raise LateAuditError('invalid ignored late receipt timestamp') from error
        _require(received.utcoffset() is not None, 'ignored late receipt timestamp requires a timezone')
        task_id, fence = row['task_id'], row['fence']
        task, lease = tasks.get(task_id), leases.get((task_id, fence))
        _require(task is not None and lease is not None and task['access'] == 'synthetic' and
                 task['egress'] == 'none' and type(task['fence']) is int and fence <= task['fence'],
                 'ignored late receipt lacks a retained synthetic task and fence')
        ref, profile = _outcome(row['outcome_json'])
        if profile == 'runtime_fixture':
            current = leases.get((task_id, task['fence']))
            roots.add(_runtime_attempt(task, lease, current, grants, fence, ref, cas))
        else:
            _require(task_id in allowed and task['status'] == 'unknown' and fence == task['fence'] == 1 and
                     lease['state'] in ('unknown', 'expired') and lease['completion_json'] is None,
                     'ignored recovery claim requires a separately admitted unknown attempt')
        _receipt(cas, ref, profile, task_id, fence)
        rows.append({**{key: row[key] for key in ('id', 'task_id', 'fence', 'disposition')},
                     'row_sha256': archive._digest(archive.canonical(row)), 'profile': profile, 'cas_ref': ref})
        seen.add(row['id'])
        roots.add(ref)
    return {'schema': SCHEMA, 'classification': CLASSIFICATION,
            'rows': sorted(rows, key=lambda row: row['id'])}, roots
