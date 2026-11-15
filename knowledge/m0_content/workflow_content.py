"""Atomic synthetic research-to-content writes and metadata-only reconciliation.

The trusted caller enters dependency_guard BEFORE this module opens content.
No request field can supply a path, script body, grant, or guard assertion.
"""
from contextlib import closing, contextmanager
from datetime import datetime
import json
from pathlib import Path
import re
import sqlite3

from knowledge.m0_content.contract import Contract, ContractError, canonical, digest, parse_ref, timestamp, ulid
from knowledge.m0_content.workflow_binding import WorkflowBinding, WorkflowError
from knowledge.m0_content.workflow_guard import WorkflowGuard, require_guard
from tools.m0_backup.domain_binding import check_recovery_binding_current, lookup_task_id, require_recovery_operation
from tools.m0_manager.readonly import local_sqlite_file, readonly_db

SCHEMA = 'm0.research_content_workflow.v1'
WRITES = {'content.script_from_context', 'content.approval_record'}
HASH = re.compile(r'^sha256:[0-9a-f]{64}$')
BARE_HASH = re.compile(r'^[0-9a-f]{64}$')
KEY = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.:@+\-]{0,299}$')
EXTERNAL_DDL = '''CREATE TABLE external_contexts(
 content_context_ref TEXT PRIMARY KEY REFERENCES artifacts(ref), workflow_binding TEXT NOT NULL,
 source_binding TEXT NOT NULL, catalog_instance TEXT NOT NULL, context_id TEXT NOT NULL,
 manifest_sha256 TEXT NOT NULL, refs_json TEXT NOT NULL, caller_json TEXT NOT NULL,
 domains_json TEXT NOT NULL, scopes_json TEXT NOT NULL, max_class TEXT NOT NULL,
 mapping_sha256 TEXT NOT NULL)'''
RECEIPT_DDL = '''CREATE TABLE operation_receipts(
 write_key TEXT PRIMARY KEY, operation TEXT NOT NULL, request_hash TEXT NOT NULL,
 semantic_hash TEXT NOT NULL, result_json TEXT NOT NULL, committed_at TEXT NOT NULL,
 committed_fence INTEGER, reused_from TEXT, decision_at TEXT NOT NULL)'''
MAPPING_FIELDS = ('workflow_binding', 'source_binding', 'catalog_instance', 'context_id', 'manifest_sha256',
                  'refs', 'caller', 'domains', 'scopes', 'max_class')


def _fail(code):
    raise WorkflowError(code)


def _checkpoint(stage):
    """No-op fault boundary for isolated process interruption tests."""


def _path(path):
    path = local_sqlite_file(path)
    if path.stat().st_nlink != 1:
        _fail('CONTENT_FILE_SHARED')
    return path


def _meta(db, binding=None, *, workflow=True):
    meta = dict(db.execute('SELECT key,value FROM meta'))
    if meta.get('schema') != 'm0-content-synthetic-v1' or meta.get('platform_calls') != '0':
        _fail('CONTENT_NOT_SYNTHETIC')
    if binding is not None and meta.get('instance') != binding.content_instance:
        _fail('CONTENT_INSTANCE_CHANGED')
    if workflow and meta.get('workflow_schema') != SCHEMA:
        _fail('CONTENT_WORKFLOW_NOT_INITIALIZED')
    return meta


def initialize_workflow(path):
    """Explicit additive initialization of an already marked synthetic Contract."""
    path = _path(path)
    with readonly_db(path) as db:
        _meta(db, workflow=False)
    with closing(Contract(path)) as content:
        content.db.execute('BEGIN IMMEDIATE')
        try:
            meta = _meta(content.db, workflow=False)
            if meta.get('workflow_schema') is not None:
                if meta['workflow_schema'] != SCHEMA:
                    _fail('CONTENT_WORKFLOW_SCHEMA_UNSUPPORTED')
                # Existing tables must really be present; never silently repair a damaged source.
                for name in ('external_contexts', 'operation_receipts'):
                    if not content.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone():
                        _fail('CONTENT_WORKFLOW_SCHEMA_INVALID')
            else:
                content.db.execute(EXTERNAL_DDL)
                content.db.execute(RECEIPT_DDL)
                content.db.execute('CREATE INDEX operation_receipts_semantic ON operation_receipts(operation,semantic_hash)')
                content.db.execute("INSERT INTO meta VALUES('workflow_schema',?)", (SCHEMA,))
            content.db.execute('COMMIT')
            return {'schema': SCHEMA, 'content_instance': meta['instance'], 'synthetic_only': True}
        except BaseException:
            if content.db.in_transaction:
                content.db.execute('ROLLBACK')
            raise


def _normalize(binding, operation, params):
    if not isinstance(binding, WorkflowBinding):
        _fail('INVALID_WORKFLOW_BINDING')
    common = {'trace_id', 'context_id', 'manifest_sha256'}
    fields = {'content.script_from_context': common | {'template_id'},
              'content.approval_record': common | {'script_ref', 'control_approval'},
              'content.approval_status': common | {'script_ref', 'approval_ref'}}
    if operation not in fields or not isinstance(params, dict) or set(params) != fields[operation]:
        _fail('INVALID_CONTENT_REQUEST')
    if (params['trace_id'] not in binding.allowed_traces or
            not isinstance(params['context_id'], str) or not KEY.fullmatch(params['context_id']) or
            not isinstance(params['manifest_sha256'], str) or not BARE_HASH.fullmatch(params['manifest_sha256'])):
        _fail('INVALID_CONTENT_REQUEST')
    for key in ('script_ref', 'approval_ref'):
        if key in params:
            parse_ref(params[key])
    if 'template_id' in params and params['template_id'] != binding.template_id:
        _fail('TEMPLATE_NOT_BOUND')
    if 'control_approval' in params:
        proof = params['control_approval']
        if (not isinstance(proof, dict) or set(proof) != {'approval_id', 'proof_hash', 'task_id', 'scope', 'actor', 'expires_at'} or
                proof.get('actor') != 'synthetic-operator' or proof.get('scope') != 'script_ready' or
                not isinstance(proof.get('proof_hash'), str) or not HASH.fullmatch(proof['proof_hash']) or
                any(not isinstance(proof.get(k), str) or not KEY.fullmatch(proof[k]) for k in ('approval_id', 'task_id'))):
            _fail('CONTROL_APPROVAL_REQUIRED')
        if timestamp(proof['expires_at']) != proof['expires_at']:
            _fail('CONTROL_APPROVAL_REQUIRED')
    return json.loads(canonical(params))


def semantic_hash(binding, operation, params):
    return 'sha256:' + digest({'operation': operation, 'version': 1, 'workflow_binding': binding.digest,
                               'params': _normalize(binding, operation, params)})


def _key(write_key, request_hash):
    if not isinstance(write_key, str) or not KEY.fullmatch(write_key):
        _fail('INVALID_WRITE_KEY')
    if not isinstance(request_hash, str) or not HASH.fullmatch(request_hash):
        _fail('INVALID_REQUEST_HASH')


def _guard_identity(binding, guard):
    if (not isinstance(guard, WorkflowGuard) or not guard.active or
            guard.binding.digest != binding.digest or guard.binding.content_instance != binding.content_instance or
            guard.binding.recovery_digest != binding.recovery_digest):
        _fail('DEPENDENCY_GUARD_REQUIRED')


def _mapping(binding, metadata):
    mapping = {key: metadata[key] for key in MAPPING_FIELDS if key != 'workflow_binding'}
    mapping['workflow_binding'] = binding.digest
    if (mapping['source_binding'] != binding.research.digest or
            mapping['catalog_instance'] != binding.research.instance or
            not isinstance(mapping['refs'], list) or not mapping['refs'] or len(set(mapping['refs'])) != len(mapping['refs'])):
        _fail('DEPENDENCY_BINDING_MISMATCH')
    mapping['refs'] = sorted(mapping['refs'])
    for field in ('domains', 'scopes'):
        mapping[field] = sorted(mapping[field])
    return json.loads(canonical(mapping))


def _external(content, mapping, trace_id):
    identity = ulid(['workflow-context', mapping['workflow_binding'], trace_id, mapping['context_id'], mapping['manifest_sha256']])
    body = {'workflow_binding': mapping['workflow_binding'], 'external_context': mapping,
            'external_dependency_sha256': 'sha256:' + digest(mapping), 'synthetic_only': True}
    head = content.db.execute('SELECT ref FROM heads WHERE identity=?', ('creative/context/' + identity,)).fetchone()
    if head:
        item = content.get(head['ref'])
        if item['body'] != body or item['derived_from'] or content.external_context(item['ref']) != mapping:
            _fail('CONTENT_CONTEXT_IDENTITY_CONFLICT')
        return item['ref']
    ref = content._put('creative', 'context', identity, body, [], trace_id)
    content.db.execute('INSERT INTO external_contexts VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', (
        ref, mapping['workflow_binding'], mapping['source_binding'], mapping['catalog_instance'],
        mapping['context_id'], mapping['manifest_sha256'], canonical(mapping['refs']), canonical(mapping['caller']),
        canonical(mapping['domains']), canonical(mapping['scopes']), mapping['max_class'], digest(mapping)))
    return ref


def _base_result(binding, params, mapping, context_ref, script_ref):
    return {'schema': 'm0.workflow_content_result.v1', 'workflow_binding': binding.digest,
            'content_instance': binding.content_instance, 'source_binding': mapping['source_binding'],
            'catalog_instance': mapping['catalog_instance'], 'trace_id': params['trace_id'],
            'context_id': mapping['context_id'], 'manifest_sha256': mapping['manifest_sha256'],
            'context_ref': context_ref, 'script_ref': script_ref, 'research_refs': mapping['refs'],
            'external_dependency_sha256': 'sha256:' + digest(mapping), 'synthetic_only': True,
            'generation': 'template_synthetic', 'ai_generated': False,
            'human_reviewed': False, 'publish_authorized': False, 'platform_calls': 0}


def _script(content, binding, params, mapping):
    template = binding.template
    if 'sha256:' + digest(template) != binding.template_sha256 or template.get('synthetic_only') is not True:
        _fail('TEMPLATE_HASH_MISMATCH')
    context_ref = _external(content, mapping, params['trace_id'])
    text = '\n'.join(line.format(source_count=len(mapping['refs']), manifest_sha256=mapping['manifest_sha256'])
                     for line in template['lines'])
    body = {'title': template['title'], 'text': text, 'template_id': binding.template_id,
            'template_sha256': binding.template_sha256, 'workflow_binding': binding.digest,
            'context_ref': context_ref, 'external_dependency_sha256': 'sha256:' + digest(mapping),
            'synthetic_only': True, 'generation': 'template_synthetic', 'ai_generated': False,
            'human_reviewed': False, 'publish_authorized': False}
    ident = ulid(['workflow-script', binding.digest, params['trace_id'], binding.template_id])
    script_ref = content._put('mgmt', 'script', ident, body, [context_ref], params['trace_id'])
    content._event(params['trace_id'], 'content.script_from_context', [context_ref, script_ref])
    return _base_result(binding, params, mapping, context_ref, script_ref) | {
        'template_id': binding.template_id, 'template_sha256': binding.template_sha256,
        'script_version': parse_ref(script_ref)[1]}


def _bound_script(content, binding, params, mapping):
    script = content.get(params['script_ref'])
    if (script['owner'] != 'mgmt' or script['kind'] != 'script' or script['trace_id'] != params['trace_id'] or
            script['body'].get('workflow_binding') != binding.digest or len(script['derived_from']) != 1):
        _fail('SCRIPT_NOT_BOUND')
    context_ref = script['derived_from'][0]
    if content.external_context(context_ref) != mapping or script['body'].get('context_ref') != context_ref:
        _fail('SCRIPT_CONTEXT_MISMATCH')
    content.closure(script['ref'], current=True)
    return script, context_ref


def _approve(content, binding, params, mapping):
    script, context_ref = _bound_script(content, binding, params, mapping)
    approved = content._approve({'subject_ref': script['ref'], 'scope': 'script_ready',
                                 'fixture_grant': 'm0-content-contract-fixture-v1'}, control_approval=params['control_approval'])
    return _base_result(binding, params, mapping, context_ref, script['ref']) | {
        'approval_ref': approved['ref'], 'version_bound': script['version'], 'scope': 'script_ready',
        'control_approval': params['control_approval'],
        'fixture_authorization_ref': 'fixture://m0-content/contract-test-specification-v1'}


def _verify_result(content, result):
    for field in ('context_ref', 'script_ref', 'approval_ref'):
        if field in result:
            content.get(result[field])


def _usability(content, result):
    try:
        _verify_result(content, result)
        if 'approval_ref' in result:
            content.verify_approval(result['approval_ref'], result['script_ref'], 'script_ready', result['version_bound'])
        else:
            content.closure(result['script_ref'], current=True)
        return {'current_usable': True, 'effective_state': 'fixture_current', 'reason_code': 'DEPENDENCY_CURRENT'}
    except ContractError as error:
        state = {'EXTERNAL_DEPENDENCY_REVOKED': 'stale_by_revocation', 'EXTERNAL_DEPENDENCY_EXPIRED': 'expired',
                 'CONTROL_APPROVAL_EXPIRED': 'expired',
                 'APPROVAL_STALE': 'stale', 'EXTERNAL_DEPENDENCY_UNVERIFIED': 'unverified'}.get(error.code, 'unverified')
        return {'current_usable': False, 'effective_state': state, 'reason_code': error.code}


def _receipt(binding, row, result):
    return {'write_key': row['write_key'], 'operation': row['operation'], 'request_hash': row['request_hash'],
            'semantic_hash': row['semantic_hash'], 'committed_fence': row['committed_fence'],
            'reused_from': row['reused_from'], 'committed_at': row['committed_at'], 'decision_at': row['decision_at'],
            'result_refs': [result[k] for k in ('context_ref', 'script_ref', 'approval_ref') if k in result],
            'instance': {'binding': binding.digest, 'catalog': binding.research.instance, 'content': binding.content_instance}}


def _control_expiry(params, at):
    proof = params.get('control_approval')
    if proof is not None and datetime.fromisoformat(proof['expires_at']) <= datetime.fromisoformat(at):
        _fail('CONTROL_APPROVAL_EXPIRED')


def write_content(binding, operation, params, write_key, request_hash, guard, *, committed_fence=None):
    require_recovery_operation(binding, operation)
    params = _normalize(binding, operation, params)
    if operation not in WRITES:
        _fail('CONTENT_OPERATION_NOT_WRITABLE')
    _key(write_key, request_hash)
    if committed_fence is not None and (type(committed_fence) is not int or committed_fence < 1):
        _fail('INVALID_COMMITTED_FENCE')
    semantic = semantic_hash(binding, operation, params)
    _guard_identity(binding, guard)
    # A committed key is historical fact; never turn a revoked dependency into a
    # claim that this write did not happen. No write connection is opened here.
    prior = lookup_content(binding, operation, write_key, request_hash, guard)
    if prior['state'] == 'committed':
        if prior['semantic_hash'] != semantic:
            _fail('IDEMPOTENCY_CONFLICT')
        return prior
    if prior['state'] != 'absent':
        _fail('IDEMPOTENCY_CONFLICT' if prior['state'] == 'conflict' else 'CONTENT_INSTANCE_CHANGED')
    metadata = require_guard(guard, binding, params['context_id'], params['manifest_sha256'])
    _control_expiry(params, metadata['decision_at'])
    mapping = _mapping(binding, metadata)
    path = _path(binding.content_path)
    with readonly_db(path) as db:
        _meta(db, binding)
    with closing(Contract(path, dependency_guard=guard)) as content:
        content.db.execute('BEGIN IMMEDIATE')
        try:
            _meta(content.db, binding)
            prior = content.db.execute('SELECT * FROM operation_receipts WHERE write_key=?', (write_key,)).fetchone()
            if prior and (prior['operation'] != operation or prior['request_hash'] != request_hash or prior['semantic_hash'] != semantic):
                _fail('IDEMPOTENCY_CONFLICT')
            if prior:
                # Another writer can commit this key between the read-only lookup
                # and BEGIN IMMEDIATE. It is already history, even if permission
                # expires while we waited. Do not mislabel it as rolled back.
                result = json.loads(prior['result_json'])
                _verify_result(content, result)
                content.db.execute('ROLLBACK')
                return {'state': 'committed', 'request_hash': request_hash, 'semantic_hash': semantic,
                        'result': result, 'committed_at': prior['committed_at'],
                        'operation_receipt': _receipt(binding, prior, result), **_usability(content, result)}
            same = prior or content.db.execute('SELECT * FROM operation_receipts WHERE operation=? AND semantic_hash=? ORDER BY committed_at,write_key LIMIT 1', (operation, semantic)).fetchone()
            if same:
                result = json.loads(same['result_json'])
                _verify_result(content, result)
                if not prior and not _usability(content, result)['current_usable']:
                    _fail('DEPENDENCY_UNUSABLE')
            else:
                result = (_script if operation == 'content.script_from_context' else _approve)(content, binding, params, mapping)
            _checkpoint('after_artifacts')
            committed = prior['committed_at'] if prior else timestamp()
            if not prior:
                content.db.execute('INSERT INTO operation_receipts VALUES(?,?,?,?,?,?,?,?,?)',
                    (write_key, operation, request_hash, semantic, canonical(result), committed, committed_fence,
                     same['write_key'] if same else None, metadata['decision_at']))
            # Expiry and guard activity are checked again while research locks are still held.
            require_guard(guard, binding, params['context_id'], params['manifest_sha256'])
            _checkpoint('before_commit')
            _control_expiry(params, timestamp())
            row = content.db.execute('SELECT * FROM operation_receipts WHERE write_key=?', (write_key,)).fetchone()
            content.db.execute('COMMIT')
        except BaseException as error:
            if content.db.in_transaction:
                content.db.execute('ROLLBACK')
                if isinstance(error, (WorkflowError, ContractError)):
                    error.commit_state = 'rolled_back'
            raise
        _checkpoint('after_commit')
        return {'state': 'committed', 'request_hash': request_hash, 'semantic_hash': semantic,
                'result': result, 'committed_at': committed, 'operation_receipt': _receipt(binding, row, result),
                **_usability(content, result)}


@contextmanager
def _read_view(binding, guard):
    """Reuse pure Contract read methods on an authorizer-protected RO connection.

    Contract.__init__ is deliberately not called; it opens a writer connection.
    This helper is not used by the manager adapter, which retains its own reader.
    """
    if not isinstance(binding, WorkflowBinding):
        _fail('INVALID_WORKFLOW_BINDING')
    recovered = check_recovery_binding_current(binding, 'workflow')
    identity = recovered.file_identities['content.sqlite3'] if recovered is not None else None
    with readonly_db(_path(binding.content_path), identity=identity) as db:
        _meta(db, binding)
        reader = object.__new__(Contract)
        reader.path, reader.db, reader.dependency_guard = binding.content_path, db, guard
        yield reader


def lookup_content(binding, operation, write_key, request_hash, guard=None):
    if operation not in WRITES:
        _fail('INVALID_CONTENT_OPERATION')
    _key(write_key, request_hash)
    require_recovery_operation(binding, 'operation.lookup', lookup_task_id(operation, write_key))
    if guard is not None:
        _guard_identity(binding, guard)
    with _read_view(binding, guard) as content:
        row = content.db.execute('SELECT * FROM operation_receipts WHERE write_key=?', (write_key,)).fetchone()
        if row is None:
            return {'state': 'absent'}
        if row['operation'] != operation or row['request_hash'] != request_hash:
            return {'state': 'conflict'}
        result = json.loads(row['result_json'])
        if any(result.get(key) != value for key,value in {
                'workflow_binding': binding.digest, 'catalog_instance': binding.research.instance,
                'content_instance': binding.content_instance}.items()):
            return {'state': 'instance_mismatch'}
        _verify_result(content, result)
        return {'state': 'committed', 'request_hash': row['request_hash'], 'semantic_hash': row['semantic_hash'],
                'result': result, 'committed_at': row['committed_at'], 'operation_receipt': _receipt(binding, row, result),
                **_usability(content, result)}


def approval_status(binding, params, guard):
    params = _normalize(binding, 'content.approval_status', params)
    _guard_identity(binding, guard)
    with _read_view(binding, guard) as content:
        script = content.get(params['script_ref'])
        approval = content.get(params['approval_ref'])
        if script['trace_id'] != params['trace_id'] or approval['trace_id'] != params['trace_id']:
            _fail('SCRIPT_NOT_BOUND')
        if len(script['derived_from']) != 1:
            _fail('SCRIPT_CONTEXT_MISMATCH')
        mapping = content.external_context(script['derived_from'][0])
        if mapping is None or any(mapping.get(key) != params[key] for key in ('context_id', 'manifest_sha256')) or mapping['workflow_binding'] != binding.digest:
            _fail('SCRIPT_CONTEXT_MISMATCH')
        result = {'script_ref': script['ref'], 'approval_ref': approval['ref'], 'version_bound': script['version']}
        current = _usability(content, result)
        provenance = binding.recovery_admission.provenance() if binding.recovery_admission is not None else {}
        return {**result, 'context_id': params['context_id'], 'manifest_sha256': params['manifest_sha256'],
                'workflow_binding': binding.digest, 'content_instance': binding.content_instance,
                **current, **provenance, 'usable': current['current_usable'], 'synthetic_only': True,
                'human_reviewed': False, 'publish_authorized': False, 'platform_calls': 0}
