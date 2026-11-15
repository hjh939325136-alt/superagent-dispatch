"""Canonical evidence for mixed local invocation and read-only reconciliation."""

from dataclasses import asdict
from datetime import datetime, timezone
import json

from .canonical import canonical_json, sha256_ref
from .models import DispatchError, Evidence, Outcome
from .routing import LOCAL_ROUTE, _digest


INTENT_KIND = 'invocation_intent'
INTENT_SCHEMA = 'sa_dispatch.invocation_intent.v1'
RECONCILIATION_SCHEMA = 'sa_dispatch.reconciliation.v2'
WORKFLOW_PROFILE = 'research_content.v1'
WORKFLOW_WRITES = {'context.assemble', 'content.script_from_context', 'content.approval_record'}
WORKFLOW_PARAMS = {
    'context.assemble': {'caller', 'domains', 'scopes', 'max_class', 'as_of', 'refs'},
    'content.script_from_context': {'trace_id', 'context_id', 'manifest_sha256', 'template_id'},
    'content.approval_record': {'trace_id', 'context_id', 'manifest_sha256', 'script_ref'},
    'content.approval_status': {'trace_id', 'context_id', 'manifest_sha256', 'script_ref', 'approval_ref'},
}
REQUEST_FIELDS = {'schema_version', 'request_id', 'operation', 'idempotency_key', 'payload'}
ENVELOPE_FIELDS = {'schema_version', 'request_id', 'ok', 'data'}


def receipt_bytes(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True,
                      separators=(',', ':')).encode('utf-8')


def read_object(artifacts, ref):
    if not _digest(ref):
        raise DispatchError('expected a SHA-256 evidence reference')
    try:
        raw = artifacts.get(ref)
        body = json.loads(raw)
        if not isinstance(body, dict) or sha256_ref(raw) != ref or receipt_bytes(body) != raw:
            raise ValueError('noncanonical evidence')
        return body
    except (OSError, ValueError, RecursionError) as exc:
        raise DispatchError('missing or invalid canonical evidence') from exc


def same(actual, expected, message):
    # Canonical comparison distinguishes booleans, integers and floats.
    if receipt_bytes(actual) != receipt_bytes(expected):
        raise DispatchError(message)


def _text(value):
    return type(value) is str and bool(value.strip())


def _at(value):
    if not _text(value):
        raise DispatchError('approval requires a timezone-aware timestamp')
    try:
        at = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise DispatchError('invalid historical approval timestamp') from exc
    if at.tzinfo is None:
        raise DispatchError('approval requires a timezone-aware timestamp')
    return at.astimezone(timezone.utc)


def _request_identity(task, request):
    if (not REQUEST_FIELDS <= request.keys() or request['schema_version'] != '0.1'
            or request['request_id'] != task.task_id or not _text(request['operation'])
            or not isinstance(request['payload'], dict)
            or (request['idempotency_key'] is not None and not _text(request['idempotency_key']))):
        raise DispatchError('invocation does not identify the task and operation')


def workflow_request(task, request, artifacts):
    """Validate the historical payload and its only defined child artifact."""
    scope = task.scope
    if (task.kind not in WORKFLOW_PARAMS or task.kind_version != 1 or task.max_attempts != 1
            or task.side_effect != ('idempotent_write' if task.kind in WORKFLOW_WRITES else 'none')
            or not isinstance(scope, dict) or set(scope) != {'workflow_binding', 'catalog_instance', 'content_instance'}
            or not _digest(scope['workflow_binding']) or not _text(scope['catalog_instance'])
            or not _text(scope['content_instance']) or scope['workflow_binding'] != task.params.get('workflow_binding')
            or set(task.params) != WORKFLOW_PARAMS[task.kind] | {'workflow_binding'}):
        raise DispatchError('workflow task binding is invalid')
    payload = dict(task.params)
    proof_ref = None
    if task.kind == 'content.approval_record':
        approval = request['payload'].get('control_approval')
        fields = {'approval_id', 'proof_hash', 'task_id', 'scope', 'actor', 'expires_at'}
        expected = {'task_id': task.task_id, 'scope': 'script_ready', 'actor': 'synthetic-operator'}
        if (not isinstance(approval, dict) or set(approval) != fields or not _text(approval['approval_id'])
                or any(approval[key] != value for key, value in expected.items())):
            raise DispatchError('historical approval does not bind to the invocation')
        proof_ref = approval['proof_hash']
        proof = read_object(artifacts, proof_ref)
        expected.update(schema='m0.synthetic_operator_approval.v1', fingerprint=task.fingerprint,
                        decision='approved', synthetic=True, human_reviewed=False, publish_authorized=False)
        same(proof, {**expected, 'expires_at': proof.get('expires_at')}, 'invalid historical approval proof')
        if _at(approval['expires_at']) > _at(proof['expires_at']):
            raise DispatchError('historical approval outlasts its proof')
        payload['control_approval'] = approval
    same(request, {'schema_version': '0.1', 'request_id': task.task_id, 'operation': task.kind,
                   'idempotency_key': task.task_id + ':' + task.kind + '@1' if task.kind in WORKFLOW_WRITES else None,
                   'payload': payload}, 'historical request differs from submitted task')
    return proof_ref


def validate_approval_row(task, request, row, anchor_at):
    approval = request['payload'].get('control_approval')
    if task.kind != 'content.approval_record':
        return
    expected = {'approval_id': approval['approval_id'], 'evidence_ref': approval['proof_hash'],
                'task_id': task.task_id, 'fingerprint': task.fingerprint, 'scope': approval['scope'],
                'actor': approval['actor'], 'decision': 'approved'}
    if (row is None or any(row[key] != value for key, value in expected.items())
            or _at(row['expires_at']) != _at(approval['expires_at'])
            or not _at(row['at']) <= _at(anchor_at) < _at(row['expires_at'])):
        raise DispatchError('approval was not valid at invocation')


def intent_document(task, lease, component, profile, request_ref, request):
    _request_identity(task, request)
    identity = asdict(task.spec)
    identity.pop('idem_key')
    identity['required_caps'] = sorted(set(identity['required_caps']))
    identity['input_refs'] = sorted(identity['input_refs'], key=lambda ref: tuple(ref[key] for key in
                                    ('owner', 'kind', 'id', 'version', 'hash')))
    identity.update(side_effect=task.side_effect, scope=task.scope)
    if (task.execution_route != LOCAL_ROUTE or task.access != 'synthetic' or task.egress != 'none'
            or type(lease['fence']) is not int or not 0 < lease['fence'] <= task.fence
            or lease['task_id'] != task.task_id or not _text(lease['lease_id']) or not _text(lease['device_id'])
            or not _text(component) or sha256_ref(canonical_json(identity)) != task.fingerprint or not _digest(request_ref)
            or not _text(task.idem_key) or not _text(task.kind) or type(task.kind_version) is not int
            or not isinstance(task.scope, dict) or profile not in (None, WORKFLOW_PROFILE)
            or (profile == WORKFLOW_PROFILE) != (task.kind in WORKFLOW_PARAMS)
            or (profile == WORKFLOW_PROFILE and component != 'research_content')):
        raise DispatchError('invalid mixed invocation identity')
    return {'schema': INTENT_SCHEMA, 'task_id': task.task_id, 'fingerprint': task.fingerprint,
            'idem_key': task.idem_key, 'kind': task.kind, 'kind_version': task.kind_version,
            'component': component, 'route': LOCAL_ROUTE, 'lease_id': lease['lease_id'],
            'device_id': lease['device_id'], 'fence': lease['fence'], 'scope': task.scope,
            'profile': profile, 'request_ref': request_ref, 'operation': request['operation'],
            'write_key': request['idempotency_key']}


def read_intent(task, lease, row, artifacts, *, approval_row):
    if (row['task_id'] != task.task_id or row['kind'] != INTENT_KIND or row['access'] != 'synthetic'
            or type(row['fence']) is not int or row['fence'] != lease['fence'] or row['ref_json'] is not None
            or row['artifact_hash'] != row['content_hash']
            or not _at(lease['granted_at']) <= _at(row['created_at']) < _at(lease['expires_at'])):
        raise DispatchError('invocation evidence row is not bound to the lease')
    intent = read_object(artifacts, row['artifact_hash'])
    request = read_object(artifacts, intent.get('request_ref'))
    same(intent, intent_document(task, lease, intent.get('component'), intent.get('profile'),
                                 intent['request_ref'], request), 'invocation intent binding mismatch')
    proof_ref = None
    if intent['profile'] == WORKFLOW_PROFILE:
        proof_ref = workflow_request(task, request, artifacts)
        if proof_ref is not None:
            approval = approval_row(request['payload']['control_approval']['approval_id'])
            validate_approval_row(task, request, approval, row['created_at'])
    return intent, request, proof_ref


def lookup_document(task, intent):
    if (intent['profile'] != WORKFLOW_PROFILE or intent['operation'] not in WORKFLOW_WRITES
            or task.side_effect != 'idempotent_write' or intent['fence'] != task.fence
            or not _text(intent['write_key'])):
        raise DispatchError('mixed reconciliation requires supported anchored write evidence')
    return {'schema_version': '0.1', 'request_id': 'reconcile-' + task.task_id,
            'operation': 'operation.lookup', 'idempotency_key': None,
            'payload': {'task_id': task.task_id, 'fence': task.fence, 'operation': intent['operation'],
                        'write_key': intent['write_key'], 'request_hash': intent['request_ref'],
                        'binding_hash': task.scope['workflow_binding'],
                        'catalog_instance': task.scope['catalog_instance'],
                        'content_instance': task.scope['content_instance']}}


def validate_lookup(task, intent, original, request, envelope):
    same(request, lookup_document(task, intent), 'invalid read-only lookup request')
    if (not isinstance(envelope, dict) or set(envelope) != ENVELOPE_FIELDS or envelope['ok'] is not True
            or envelope['schema_version'] != '0.1' or envelope['request_id'] != request['request_id']
            or not isinstance(envelope['data'], dict)):
        raise DispatchError('unverified lookup envelope')
    data = envelope['data']
    expected = {**request['payload'], 'state': 'committed'}
    same({key: data.get(key) for key in expected}, expected, 'unbound or incomplete lookup')
    if (set(data) != set(expected) | {'response', 'response_hash', 'current_usable', 'usable_reason'}
            or type(data['current_usable']) is not bool or not _text(data['usable_reason'])):
        raise DispatchError('invalid lookup result fields')
    response = data['response']
    if (not isinstance(response, dict) or set(response) != ENVELOPE_FIELDS or response['ok'] is not True
            or response['schema_version'] != '0.1' or response['request_id'] != task.task_id
            or not isinstance(response['data'], dict)
            or data['response_hash'] != sha256_ref(receipt_bytes(response))):
        raise DispatchError('unverified historical response')
    result = workflow_result_data(response, task, task.scope)
    if task.kind == 'content.approval_record':
        same(result.get('control_approval'), original['payload']['control_approval'],
             'historical result names a different approval')
    return data, response


def workflow_result_data(response, task, scope):
    if (response.get('schema_version') != '0.1' or response.get('ok') is not True
            or response.get('request_id') != task.task_id or not isinstance(response.get('data'), dict)):
        raise DispatchError('unbound workflow response')
    data = response['data']
    expected = {**scope, 'synthetic_only': True, 'receipt_content': 'references_only'}
    same({key: data.get(key) for key in expected}, expected, 'unbound workflow receipt')
    denied = {'body', 'source_record', 'snippet', 'script_text', 'argv', 'path', 'locator'}
    pending = [data]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            if set(value) & denied:
                raise DispatchError('workflow receipt includes restricted content')
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)
    return data


def workflow_outcome(task, response, lookup_ref, *, kind=None):
    result = response['data'] if kind is None else kind.result(response, task)
    return Outcome('succeeded', {**result, 'receipt': lookup_ref}, (
        Evidence('component_response', lookup_ref, task.access, artifact_hash=lookup_ref),))


def reconciliation_document(intent_ref, intent, lookup_request_ref, lookup_ref, usable, outcome):
    identity = {key: intent[key] for key in ('task_id', 'fence', 'idem_key', 'component', 'request_ref')}
    return {'schema': RECONCILIATION_SCHEMA, **identity, 'intent_ref': intent_ref,
            'lookup_request_ref': lookup_request_ref, 'lookup_ref': lookup_ref, 'current_usable': usable,
            'outcome_hash': sha256_ref(receipt_bytes(asdict(outcome)))}


def verify_reconciliation(task, anchor_ref, intent, original, proof, artifacts, *, kind=None):
    lookup_ref = proof.get('lookup_ref')
    lookup_request_ref = proof.get('lookup_request_ref')
    request = read_object(artifacts, lookup_request_ref)
    envelope = read_object(artifacts, lookup_ref)
    data, response = validate_lookup(task, intent, original, request, envelope)
    if kind is not None:
        same(kind.lookup_request(task, original, intent['request_ref']), request, 'kind lookup request mismatch')
        if kind.validate_lookup(task, original, data) is not True:
            raise DispatchError('kind rejected lookup evidence')
    outcome = workflow_outcome(task, response, lookup_ref, kind=kind)
    same(proof, reconciliation_document(anchor_ref, intent, lookup_request_ref, lookup_ref,
                                         data['current_usable'], outcome), 'reconciliation receipt binding mismatch')
    return outcome
