"""Offline checkpoints for owned M0 research/content CLI fixtures."""
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import shutil
import sqlite3
from subprocess import CompletedProcess, Popen
import sys
from tempfile import TemporaryDirectory
from uuid import uuid4

from . import domain_binding as archive

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / 'dispatch') not in sys.path:
    sys.path.insert(0, str(ROOT / 'dispatch'))

from knowledge.m0_content import workflow_binding as workflow
from knowledge.m0_content import workflow_content as content
from knowledge.m0_content import contract
from sa_dispatch import bundle
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import sha256_ref
from sa_dispatch.collector import validate_records as validate_collector
from sa_dispatch.invocation import INTENT_KIND, WORKFLOW_PARAMS, WORKFLOW_WRITES, read_intent, workflow_request
from sa_dispatch.migrations import apply_m0_schema
from sa_dispatch.models import DispatchError
from sa_dispatch.routing import LOCAL_ROUTE, ROUTED_TABLES, ROUTES, validate_mixed_records


DATABASES = (*archive.CATALOG_FILES[1:], 'content.sqlite3', 'dispatch.sqlite3')
MARKER = archive.CATALOG_FILES[0]
LAYOUT = {name: name for name in archive.REQUIRED_FILES}
LAYOUT.update({name: name.replace('catalog/', 'research/') for name in archive.CATALOG_FILES})
LAYOUT.update({name: f'{role}-binding.json' for role, name in archive.ORIGINAL_FILES.items()})
LAYOUT['dispatch.sqlite3'] = 'ledger.sqlite3'
REVOCATION = {'revocation_basis': 'captured_snapshot', 'post_snapshot_revocations': 'unverified'}
QUIESCENCE = 'owned_offline_fixture_processes_exited'
FIXED_REF = re.compile(r'([^/#]+/[^/#]+/[^@/#]+@[1-9][0-9]*)#(?:sha256:)?([0-9a-f]{64})')


class CheckpointError(ValueError):
    """The fixture or package cannot be admitted into recovery."""


def _require(condition, message):
    if not condition:
        raise CheckpointError(message)


def _new_destination(path, source):
    path = Path(path)
    archive._stat(path.parent, directory=True)
    _require('..' not in path.parts, 'checkpoint destination must be a new directory')
    _require_absent(path)
    _require(path != source and not path.is_relative_to(source), 'destination must be outside its input root')
    return path


def _require_absent(path):
    try:
        path.lstat()
    except FileNotFoundError:
        return
    raise CheckpointError('checkpoint destination must be a new directory')


def _check_exited(processes):
    processes = tuple(processes)
    _require(processes, 'provide the owned fixture subprocesses that have exited')
    checked = []
    for process in processes:
        live_handle = isinstance(process, Popen)
        code = process.poll() if live_handle else getattr(process, 'returncode', None)
        _require(isinstance(process, (Popen, CompletedProcess)) and type(code) is int,
                 'owned fixture processes must finish before capture')
        checked.append({'kind': 'popen' if live_handle else 'completed_process',
                        'pid': process.pid if live_handle else None, 'returncode': code})
    return {'method': QUIESCENCE, 'checked_at': datetime.now(timezone.utc).isoformat(), 'processes': checked}


def _verify_quiescence(value):
    _require(isinstance(value, dict) and set(value) == {'method', 'checked_at', 'processes'} and
             value['method'] == QUIESCENCE and isinstance(value['checked_at'], str) and
             isinstance(value['processes'], list) and value['processes'], 'invalid checkpoint quiescence')
    try:
        checked_at = datetime.fromisoformat(value['checked_at'])
    except ValueError as error:
        raise CheckpointError('invalid checkpoint quiescence time') from error
    _require(checked_at.utcoffset() is not None, 'invalid checkpoint quiescence time')
    for process in value['processes']:
        _require(isinstance(process, dict) and set(process) == {'kind', 'pid', 'returncode'} and
                 type(process['returncode']) is int and
                 (process['kind'] == 'popen' and type(process['pid']) is int and process['pid'] > 0 or
                  process['kind'] == 'completed_process' and process['pid'] is None),
                 'invalid exited process evidence')


def _no_sidecars(path):
    for suffix in ('-journal', '-wal', '-shm'):
        sidecar = path.with_name(path.name + suffix)
        _require(not sidecar.exists() and not sidecar.is_symlink(), 'SQLite sidecar requires offline cleanup')


def _open_database(path):
    archive._stat(path)
    _no_sidecars(path)
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)
    try:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('PRAGMA trusted_schema=OFF')
        db.execute('BEGIN')
        return db
    except BaseException:
        db.close()
        raise


def _schema_objects(db):
    return {row['name']: tuple(row) for row in db.execute('SELECT name,type,tbl_name,sql FROM sqlite_schema')
            if not row['name'].startswith('sqlite_')}


@lru_cache(maxsize=1)
def _expected_schemas():
    _, catalog, _, workflow_catalog = workflow.search_modules()
    from fixture_store import SCHEMA as sources
    definitions = {
        'dispatch.sqlite3': (ROOT / 'dispatch/sa_dispatch/schema.sql').read_text(encoding='utf-8'),
        'content.sqlite3': contract.SCHEMA + content.EXTERNAL_DDL + ';' + content.RECEIPT_DDL + ';'
        'CREATE INDEX operation_receipts_semantic ON operation_receipts(operation,semantic_hash);',
        'catalog/catalog.sqlite3': catalog.CATALOG_SCHEMA + workflow_catalog.DDL + ';',
        'catalog/research-scatter.sqlite3': sources + catalog.SOURCE_EXTRA,
        'catalog/research-content.sqlite3': sources + catalog.SOURCE_EXTRA,
        'catalog/signalhub.sqlite3': '''CREATE TABLE source_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE reference_works(work_id TEXT PRIMARY KEY,version INTEGER NOT NULL,
          sha256 TEXT NOT NULL,payload TEXT NOT NULL);''',
    }
    expected = {}
    for name, ddl in definitions.items():
        with closing(sqlite3.connect(':memory:')) as db:
            db.row_factory = sqlite3.Row
            db.executescript(ddl)
            if name == 'dispatch.sqlite3':
                apply_m0_schema(db, '2000-01-01T00:00:00+00:00')
            expected[name] = _schema_objects(db)
    return expected


def _quoted(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def _digest_image(db, name):
    objects = _schema_objects(db)
    expected = _expected_schemas()[name]
    _require(all(objects.get(key) == record for key, record in expected.items()),
             'SQLite schema differs from the pinned fixture layout: ' + name)
    for key in objects.keys() - expected.keys():
        _, kind, _, sql = objects[key]
        _require(kind == 'table' and sql and sql.upper().startswith('CREATE TABLE '),
                 'unexpected SQLite schema object: ' + key)
        _require(db.execute(f'SELECT 1 FROM {_quoted(key)} LIMIT 1').fetchone() is None,
                 'unknown populated SQLite table: ' + key)
    _require([row[0] for row in db.execute('PRAGMA integrity_check')] == ['ok'] and
             db.execute('PRAGMA foreign_key_check').fetchone() is None,
             'SQLite integrity or foreign-key check failed: ' + name)
    digest = hashlib.sha256()
    digest.update(archive.canonical({pragma: db.execute('PRAGMA ' + pragma).fetchone()[0]
                                    for pragma in ('application_id', 'user_version', 'encoding')}))
    without_rowid = {row['name'] for row in db.execute('PRAGMA table_list') if row['wr']}
    for key in sorted(objects):
        digest.update(archive.canonical(objects[key]) + b'\n')
        if objects[key][1] != 'table':
            continue
        # Sort serialized rows so the digest does not depend on physical row order.
        columns = '*' if key in without_rowid or key not in expected else 'rowid,*'
        rows = (archive.canonical([{'blob': value.hex()} if isinstance(value, bytes) else value for value in row])
                for row in db.execute(f'SELECT {columns} FROM {_quoted(key)}'))
        for row in sorted(rows):
            digest.update(row + b'\n')
    return digest.hexdigest()


def _database_digests(paths):
    result = {}
    for name in DATABASES:
        with closing(_open_database(paths[name])) as db:
            result[name] = _digest_image(db, name)
    return result


def _backup_database(source, target):
    with closing(_open_database(source)) as db, closing(sqlite3.connect(target)) as copy:
        db.backup(copy)
        copy.execute('PRAGMA journal_mode=DELETE')


def _binding_objects(paths, *, live_root=None):
    bound, catalog, _, _ = workflow.search_modules()
    research_raw, workflow_raw = [archive._read(paths[archive.ORIGINAL_FILES[role]], archive.META_LIMIT)[0]
                                  for role in ('research', 'workflow')]
    research = archive._parse(research_raw)
    original = archive._parse(workflow_raw)
    _require(set(research) == bound.FIELDS and research['schema'] == bound.SCHEMA and
             research['synthetic_only'] is True and research['authorization_source'] == bound.AUTHORITY and
             research['grant_id'] == bound.GRANT and archive._matches(archive.IDENTITY, research['catalog_instance']) and
             archive._matches(archive.HASH, research['catalog_marker_sha256']), 'invalid original research binding')
    research_hash, workflow_hash = (sha256_ref(archive.canonical(value)) for value in (research, original))
    traces = original.get('allowed_traces')
    _require(set(original) == workflow.FIELDS and original['schema'] == workflow.SCHEMA and
             original['synthetic_only'] is True and original['authorization_source'] == workflow.AUTHORITY and
             original['catalog_instance'] == research['catalog_instance'] and
             original['research_binding_sha256'] == research_hash and
             archive._matches(archive.IDENTITY, original['content_instance']) and
             original['template'] == workflow.TEMPLATE and original['template_sha256'] == workflow.hashed(workflow.TEMPLATE) and
             isinstance(traces, list) and traces and all(isinstance(trace, str) for trace in traces) and
             len(traces) == len(set(traces)) and set(traces) <= set(workflow.TRACES), 'invalid original workflow binding')
    locators = (research['catalog_root'], original['research_binding'], original['content_db'])
    _require(all(archive.historical_path(value) for value in locators), 'invalid original historical path syntax')
    parser = PureWindowsPath if re.match(r'^[A-Za-z]:', locators[0]) else PurePosixPath
    historical_root = parser(locators[0]).parent
    expected = ('research', 'research-binding.json', 'content.sqlite3')
    _require(tuple(parser(value) for value in locators) == tuple(historical_root / name for name in expected),
             'original binding paths must belong to one prepared fixture')
    if live_root is not None:
        _require(tuple(Path(value) for value in locators) == tuple(live_root / name for name in expected),
                 'source catalog and content paths must be inside the prepared fixture')
    marker = archive._read(paths[MARKER], archive.META_LIMIT)[0]
    _require(archive._digest(marker) == research['catalog_marker_sha256'] and archive._parse(marker) == {
        'schema': catalog.MARKER, 'instance': research['catalog_instance'], 'synthetic_only': True,
        'sources': list(catalog.SOURCES), 'authorization_source': 'fixture://m0-research/lifecycle-specification-v1'},
        'catalog marker and original binding disagree')
    binding = bound.Binding(paths[archive.ORIGINAL_FILES['research']], paths[MARKER].parent,
                            research['catalog_instance'], research['catalog_marker_sha256'], research_hash)
    original_workflow = workflow.WorkflowBinding(paths[archive.ORIGINAL_FILES['workflow']], workflow_hash, binding,
        paths['content.sqlite3'], original['content_instance'], tuple(traces), workflow.TEMPLATE['id'],
        original['template_sha256'], workflow.TEMPLATE)
    identities = {'research_sha256': research_hash, 'workflow_sha256': workflow_hash,
                  'catalog_instance': binding.instance, 'content_instance': original_workflow.content_instance}
    return original_workflow, identities


def _ledger(paths):
    with closing(_open_database(paths['dispatch.sqlite3'])) as db:
        return bundle.snapshot_records(db)


def _ledger_admission(records, binding, cas):
    validate_collector(records['collector_cursors'], records['collector_gaps'])
    mode = records['ledger_mode'][0]['mode']
    _require(mode in ('mixed', 'isolated'), 'unsupported Dispatch ledger mode')
    bundle._validate_route_audit(records, mode == 'mixed')
    for table in ROUTED_TABLES:
        for row in records[table]:
            _require(row['execution_route'] is None or (mode == 'mixed' and row['execution_route'] in ROUTES),
                     'invalid Dispatch execution route')
    if mode == 'mixed':
        validate_mixed_records(records)
    tasks = {row['task_id']: row for row in records['tasks']}
    _require(all(row['access'] == 'synthetic' and row['egress'] == 'none' for row in tasks.values()) and
             all(row['access'] == 'synthetic' for row in records['evidence']) and
             all(row['class'] != 'local_only' for row in records['unified_events']),
             'checkpoint requires synthetic no-egress tasks and evidence')
    for table in ('schedules', 'schedule_firings', 'schedule_events', 'late_results'):
        _require(not records[table], 'checkpoint does not support populated ' + table)
    for table in ('leases', 'events', 'evidence', 'reconciliations', 'transport_handoffs',
                  'transport_artifact_grants', 'control_task_rules', 'control_reservations',
                  'control_approvals', 'host_attempts', 'host_events', 'claude_attempts'):
        _require(all(row['task_id'] in tasks for row in records[table]), 'orphan task reference in ' + table)
    _require(all(row['task_id'] is None or row['task_id'] in tasks or row['source'] != 'dispatch'
                 for row in records['unified_events']), 'orphan projected Dispatch task')
    _require(not any(row['state'] == 'active' for row in records['leases']), 'active leases prevent checkpoint')
    _require(not any(row['state'] in ('offered', 'accepted') for row in records['control_handoffs']),
             'active handoffs prevent checkpoint')
    _require(all(row['phase'] in ('completed', 'failed', 'cancelled', 'interrupted', 'orphan_terminal',
                                  'orphan_cancelled', 'not_started') for row in records['host_attempts']),
             'unfinished host attempts prevent checkpoint')
    _require(all(row['phase'] in ('succeeded', 'failed', 'interrupted') and row['report_ref']
                 for row in records['claude_attempts']), 'unfinished Claude attempts prevent checkpoint')
    leases = {(row['task_id'], row['fence']): row for row in records['leases']}
    approvals = {row['approval_id']: row for row in records['control_approvals']}
    scope = {'workflow_binding': binding.digest, 'catalog_instance': binding.research.instance,
             'content_instance': binding.content_instance}
    requests, allowed = {}, set()
    for row in records['evidence']:
        if row['kind'] != INTENT_KIND:
            continue
        key = row['task_id'], row['fence']
        _require(key not in requests and key in leases, 'invocation requires a unique valid lease')
        task = bundle._invocation_task(tasks[key[0]])
        intent, request, _ = read_intent(task, leases[key], row, cas, approval_row=approvals.get)
        requests[key] = (intent['request_ref'], request)
    for task in tasks.values():
        if task['kind'] in WORKFLOW_PARAMS:
            parsed = bundle._invocation_task(task)
            _require(parsed.scope == scope and parsed.params.get('workflow_binding') == binding.digest,
                     'Dispatch workflow binding or instance mismatch')
        if task['status'] in bundle.TERMINAL:
            continue
        task_id, fence = task['task_id'], task['fence']
        key = task_id, fence
        lease = leases.get(key)
        _require(task['status'] == 'unknown' and task['execution_route'] == LOCAL_ROUTE and task['kind'] in WORKFLOW_WRITES
                 and task['side_effect'] == 'idempotent_write' and task['kind_version'] == 1 and
                 task['attempts'] == task['max_attempts'] == fence == 1 and task['result_json'] is None and
                 lease and lease['state'] in ('unknown', 'expired') and lease['completion_json'] is None and
                 key in requests and all(item == key for item in leases if item[0] == task_id),
                 'nonterminal task lacks a supported anchored unknown attempt')
        _require(not any(item['task_id'] == task_id and item['kind'] in ('component_response', 'reconciliation_receipt')
                         for item in records['evidence']) and
                 not any(item['task_id'] == task_id and item['result'] in ('found', 'decided')
                         for item in records['reconciliations']), 'unknown attempt already has completion evidence')
        allowed.add(task_id)
    _budget_admission(records, tasks, leases, allowed, cas)
    return tasks, requests, sorted(allowed)


def _budget_admission(records, tasks, leases, allowed, cas):
    budgets = {row['budget_id']: row for row in records['control_budgets']}
    rules = {row['task_id']: row for row in records['control_task_rules']}
    holds = Counter()
    reservations = set()
    for row in records['control_reservations']:
        task_id, fence, budget_id = row['task_id'], row['fence'], row['budget_id']
        reservations.add((task_id, fence))
        _require((task_id, fence) in leases and budget_id in budgets and
                 row['fingerprint'] == tasks[task_id]['fingerprint'] and row['reserved_units'] >= 0,
                 'reservation does not match its task, lease or budget')
        if row['state'] == 'settled':
            _require(row['actual_units'] is not None and row['settled_at'] is not None and row['evidence_ref'],
                     'settled reservation lacks usage evidence')
            proof = archive._parse(cas.get(row['evidence_ref']))
            expected = {'synthetic': True, 'final_usage': True, **{key: row[key] for key in
                        ('task_id', 'fence', 'budget_id', 'fingerprint', 'actual_units')}}
            _require(all(type(proof.get(key)) is type(value) and proof[key] == value for key, value in expected.items()),
                     'settled usage evidence differs from reserved attempt')
            continue
        rule = rules.get(task_id)
        _require(row['state'] in ('reserved', 'uncertain') and task_id in allowed and
                 fence == tasks[task_id]['fence'] and rule and
                 (rule['budget_id'], rule['fingerprint'], rule['reserve_units']) ==
                 (budget_id, row['fingerprint'], row['reserved_units']) and
                 row['actual_units'] is None and row['settled_at'] is None and row['evidence_ref'] is None and
                 (row['uncertain_at'] is not None) == (row['state'] == 'uncertain'),
                 'unsettled reservation is not owned by an admitted attempt')
        holds[budget_id] += row['reserved_units']
    for ident in allowed & rules.keys():
        if rules[ident]['budget_id'] is not None:
            _require((ident, tasks[ident]['fence']) in reservations, 'admitted budgeted task lacks its reservation')
    for budget_id, row in budgets.items():
        values = [row[key] for key in ('reserved_units', 'used_units', 'limit_units')]
        _require(all(type(value) is int and value >= 0 for value in values) and row['unit'] and
                 holds[budget_id] == row['reserved_units'] and row['used_units'] + row['reserved_units'] <= row['limit_units'],
                 'held budget units disagree with reservation totals or exceed the limit')


class _CheckpointCAS(ArtifactStore):
    def __init__(self, root):
        archive._stat(root, directory=True)
        super().__init__(root)

    def get(self, ref):
        path = self.path(ref)
        archive._stat(path)
        return super().get(ref)


def _research_state(binding):
    _, catalog, _, workflow_catalog = workflow.search_modules()
    from fixture_store import canonical_hash, record_hash
    selected = binding.research.catalog()
    refs, contexts, receipts, revoked = set(), {}, [], {}
    with selected.transaction(catalog.SOURCES) as db:
        selected.load(catalog.SOURCES, connection=db)
        for domain in catalog.SOURCES:
            alias = catalog.ALIASES[domain]
            if domain == 'signalhub':
                rows = db.execute(f'SELECT work_id AS ref_id,version,sha256,payload FROM {alias}.reference_works')
            else:
                _require(db.execute(f'''SELECT 1 FROM {alias}.records r LEFT JOIN {alias}.record_versions v
                     ON v.ref_id=r.ref_id AND v.version=r.version WHERE v.ref_id IS NULL''').fetchone() is None,
                     'source head lacks a fixed historical version')
                rows = db.execute(f'SELECT ref_id,version,sha256,payload FROM {alias}.record_versions')
                for item in db.execute(f'''SELECT r.*,v.payload FROM {alias}.revocations r
                        LEFT JOIN {alias}.record_versions v ON v.ref_id=r.ref_id AND v.version=r.through_version
                        LEFT JOIN {alias}.import_batches b ON b.batch_id=r.batch_id WHERE b.batch_id IS NOT NULL'''):
                    _require(item['payload'] and archive._parse(item['payload'])['state'] in ('revoked', 'deleted'),
                             'revocation does not name its historical version')
                    key = (domain, item['ref_id'])
                    revoked[key] = max(revoked.get(key, 0), item['through_version'])
                _require(db.execute(f'''SELECT 1 FROM {alias}.revocations r LEFT JOIN {alias}.import_batches b
                         ON b.batch_id=r.batch_id WHERE b.batch_id IS NULL''').fetchone() is None,
                         'revocation does not name its synthetic import batch')
            for row in rows:
                value = archive._parse(row['payload'])
                _require(value.get('id') == row['ref_id'] and value.get('version') == row['version'] and
                         record_hash(value) == row['sha256'], 'historical source version hash mismatch')
                refs.add(catalog.fixed_ref(domain, value))
        for row in db.execute('SELECT * FROM contexts'):
            payload = archive._parse(row['payload'])
            context_id, digest = row['context_id'], row['sha256']
            _require(set(payload) == {'schema','caller','domains','refs','created_as_of'} and
                     payload['schema'] == 'm0-synthetic-context-v1' and canonical_hash(payload) == digest and
                     context_id == 'ctx_' + digest[:26] and isinstance(payload['refs'], list) and
                     payload['refs'] == sorted(set(payload['refs'])) and payload['refs'] and
                     set(payload['refs']) <= refs, 'invalid catalog context manifest or historical reference')
            members = [(*catalog.parse_ref(ref)[:3], ref) for ref in payload['refs']]
            actual = [tuple(value) for value in db.execute('SELECT domain,ref_id,version,ref FROM context_members '
                      'WHERE context_id=? ORDER BY ref', (context_id,))]
            _require(actual == members, 'context membership differs from the fixed manifest')
            stale = any(version <= revoked.get((domain, ident), 0) for domain, ident, version, _ in members)
            events = [item['op'] for item in db.execute('SELECT op FROM context_events WHERE context_id=? ORDER BY seq',
                                                      (context_id,))]
            expected_events = ['assembled', 'stale_by_revocation'] if stale else ['assembled']
            _require(events == expected_events and row['status'] == ('stale_by_revocation' if stale else 'current'),
                     'context revocation state differs from retained source history')
            contexts[context_id] = (digest, payload)
        for table in ('context_members', 'context_events'):
            _require(db.execute('SELECT 1 FROM ' + table + ' WHERE context_id NOT IN '
                                '(SELECT context_id FROM contexts)').fetchone() is None, 'orphan catalog context history')
        for row in db.execute('SELECT * FROM workflow_operation_receipts'):
            result = workflow_catalog._stored(binding.research, row)
            request = archive._parse(row['request_json'])
            _require(row['operation'] == workflow_catalog.OPERATION and
                     result['operation'] == row['operation'] and result['semantic_hash'] == row['semantic_hash'] and
                     result['committed_at'] == row['committed_at'] and result['state_basis'] == 'historical' and
                     result['status'] == 'committed' and result['synthetic_only'] is True and
                     result['catalog_instance'] == binding.research.instance and result['source_binding'] == binding.research.digest,
                     'catalog receipt metadata mismatch')
            manifest = _match_context(contexts, result)
            _require(result['refs'] == request['refs'] == manifest['refs'] and request['caller'] == manifest['caller'] and
                     request['domains'] == manifest['domains'] and request['as_of'] == manifest['created_as_of'],
                     'catalog receipt request differs from context manifest')
            receipts.append((dict(row), result, request))
    return refs, contexts, receipts


def _match_context(contexts, value):
    context = contexts.get(value['context_id'])
    _require(context is not None and value['manifest_sha256'] == context[0],
             'missing or mismatched catalog context manifest')
    return context[1]


def _content_state(binding, contexts):
    refs, receipts = set(), []
    with closing(_open_database(binding.content_path)) as db:
        content._meta(db, binding)
        reader = object.__new__(contract.Contract)
        reader.path, reader.db, reader.dependency_guard = binding.content_path, db, None
        for row in db.execute('SELECT ref, trace_id FROM artifacts'):
            artifact = reader.get(row['ref'])
            _require(artifact.get('synthetic') is True and artifact.get('trace_id') == row['trace_id'],
                     'content artifact is not a synthetic, trace-bound record')
            reader.closure(artifact['ref'])
            mapping = reader.external_context(artifact['ref'])
            if mapping is not None:
                manifest = _match_context(contexts, mapping)
                _require(mapping['workflow_binding'] == binding.digest and
                         mapping['source_binding'] == binding.research.digest and
                         mapping['catalog_instance'] == binding.research.instance and
                         mapping['refs'] == manifest['refs'] and mapping['caller'] == manifest['caller'] and
                         sorted(mapping['domains']) == sorted(manifest['domains']),
                         'external content context differs from catalog manifest or binding')
            refs.add(artifact['ref'])
        for row in db.execute('SELECT * FROM operation_receipts'):
            result = archive._parse(row['result_json'])
            content._verify_result(reader, result)
            manifest = _match_context(contexts, result)
            mapping = reader.external_context(result['context_ref'])
            script = reader.get(result['script_ref'])
            _require(result['workflow_binding'] == binding.digest and result['source_binding'] == binding.research.digest and
                     result['content_instance'] == binding.content_instance and result['catalog_instance'] == binding.research.instance and
                     result['research_refs'] == manifest['refs'] and result['synthetic_only'] is True and
                     result['human_reviewed'] is False and result['publish_authorized'] is False and
                     type(result['platform_calls']) is int and result['platform_calls'] == 0 and mapping and
                     result['external_dependency_sha256'] == 'sha256:' + contract.digest(mapping) and
                     mapping['context_id'] == result['context_id'] and mapping['manifest_sha256'] == result['manifest_sha256'] and
                     script['derived_from'] == [result['context_ref']] and script['trace_id'] == result['trace_id'] and
                     script['body']['workflow_binding'] == binding.digest,
                     'content operation receipt differs from its source or artifacts')
            operation = row['operation']
            _require(operation in content.WRITES, 'unsupported content receipt operation')
            if operation == 'content.approval_record':
                approval = db.execute('SELECT * FROM approvals WHERE ref=?', (result['approval_ref'],)).fetchone()
                artifact = reader.get(result['approval_ref'])
                _require(approval and approval['subject_ref'] == result['script_ref'] and
                         approval['scope'] == result['scope'] == 'script_ready' and
                         approval['version_bound'] == result['version_bound'] == script['version'] and
                         artifact['body']['control_approval'] == result['control_approval'],
                         'content approval receipt differs from its historical proof')
            receipts.append((dict(row), result, None))
    return refs, receipts


def _domain_receipts(binding, cas, tasks, requests, receipts):
    by_key = {row['write_key']: row for row, _, _ in receipts}
    _require(len(by_key) == len(receipts), 'duplicate domain receipt write key')
    roots = set()
    for row, result, catalog_request in receipts:
        operation = row['operation']
        ident, separator, suffix = row['write_key'].rpartition(':')
        _require(separator and suffix == operation + '@1' and ident in tasks,
                 'domain receipt does not identify its Dispatch task')
        task = bundle._invocation_task(tasks[ident])
        _require(task.kind == operation and task.fence == task.attempts == 1 and
                 row.get('committed_fence', result.get('committed_fence')) in (None, task.fence),
                 'domain receipt belongs to a different Dispatch operation or fence')
        anchored = requests.get((ident, task.fence))
        if anchored:
            request_ref, original = anchored
        else:
            # Older settled isolated writes did not record invocation-intent rows.
            payload = dict(task.params)
            if operation == 'content.approval_record':
                payload['control_approval'] = result['control_approval']
            original = {'schema_version': '0.1', 'request_id': ident, 'operation': operation,
                        'idempotency_key': row['write_key'], 'payload': payload}
            request_ref = sha256_ref(archive.canonical(original))
            proof = workflow_request(task, original, cas)
            if proof:
                roots.add(proof)
        _require(request_ref == row['request_hash'] and original['idempotency_key'] == row['write_key'],
                 'domain receipt original request hash differs from Dispatch')
        params = {key: value for key, value in original['payload'].items() if key != 'workflow_binding'}
        if catalog_request is not None:
            _, _, _, workflow_catalog = workflow.search_modules()
            _require(workflow_catalog._normalize(params, 'refs') == catalog_request,
                     'catalog receipt persisted the wrong original payload')
        else:
            _require(row['semantic_hash'] == content.semantic_hash(binding, operation, params) and
                     all(params[field] == result[field] for field in ('trace_id', 'context_id', 'manifest_sha256')) and
                     all(params[field] == result[field] for field in ('script_ref', 'control_approval') if field in params),
                     'content receipt persisted the wrong original payload')
        prior = result.get('reused_from') if catalog_request is not None else row.get('reused_from')
        if prior is not None:
            linked = by_key.get(prior)
            _require(linked and prior != row['write_key'] and linked['operation'] == operation and
                     linked['semantic_hash'] == row['semantic_hash'] and linked['committed_at'] <= row['committed_at'],
                     'invalid prior domain receipt reference')
    return roots


def _dispatch_refs(records, domain_refs):
    roots = set()

    def follow(ref):
        matched = FIXED_REF.fullmatch(ref) if isinstance(ref, str) else None
        _require(matched, 'invalid fixed Dispatch reference')
        identity, digest = matched.groups()
        if identity.split('/', 1)[0] in workflow.DOMAINS or identity.startswith(('creative/', 'mgmt/', 'webee/')):
            _require(f'{identity}#{digest}' in domain_refs, 'fixed Dispatch reference is missing from its domain image')
        else:
            roots.add('sha256:' + digest)

    for row in (*records['tasks'], *records['evidence']):
        if 'input_refs_json' in row:
            refs = archive._parse('{"refs":' + row['input_refs_json'] + '}')['refs']
        else:
            refs = [] if row['ref_json'] is None else [archive._parse(row['ref_json'])]
        for ref in refs:
            _require(set(ref) == {'owner', 'kind', 'id', 'version', 'hash'} and
                     archive._matches(archive.HASH_REF, ref['hash']), 'invalid Dispatch structured reference')
            follow(f"{ref['owner']}/{ref['kind']}/{ref['id']}@{ref['version']}#{ref['hash']}")
    for row in records['unified_events']:
        refs = archive._parse('{"refs":' + row['refs_json'] + '}')['refs']
        for ref in refs:
            follow(ref)
    return roots


def _inventory(paths, cas, *, live_root=None):
    binding, identities = _binding_objects(paths, live_root=live_root)
    records = _ledger(paths)
    tasks, requests, allowed = _ledger_admission(records, binding, cas)
    research_refs, contexts, catalog_receipts = _research_state(binding)
    content_refs, content_receipts = _content_state(binding, contexts)
    roots = _domain_receipts(binding, cas, tasks, requests, catalog_receipts + content_receipts)
    roots.update(_dispatch_refs(records, research_refs | content_refs))
    roots.update(bundle._artifact_roots(records, include_fixed_refs=False))
    refs = bundle._known_artifact_inventory(records, cas, roots)
    files = {cas.path(ref).relative_to(cas.root.parent).as_posix(): cas.get(ref) for ref in refs}
    return identities, allowed, files


def _paths(root, layout):
    return {key: root / name for key, name in layout.items()}


def _write_bytes(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as stream:
        stream.write(raw)


def _fixed_bytes(paths):
    return {name: archive._read(paths[name], archive.META_LIMIT)[0] for name in paths if name not in DATABASES}


def capture_checkpoint(source_root, destination, *, stopped_processes, creator, code_revision):
    """Capture an exclusively owned offline ``prepare`` fixture and return its manifest SHA-256.

    The caller owns the complete fixture and all its writers. Exited subprocess
    evidence and unchanged captures do not establish global writer exclusion.
    The output is a new directory. Keep the returned anchor as trusted metadata.
    """
    source = Path(source_root)
    archive._stat(source, directory=True)
    target = _new_destination(destination, source)
    quiescence = _check_exited(stopped_processes)
    _require(archive._matches(archive.IDENTITY, creator) and archive._matches(archive.IDENTITY, code_revision),
             'bounded checkpoint creator and code revision are required')
    paths = _paths(source, LAYOUT)
    with TemporaryDirectory(prefix='.m0-checkpoint-', dir=target.parent) as scratch:
        staged = Path(scratch) / 'payload'
        (staged / 'catalog').mkdir(parents=True)
        (staged / 'artifacts').mkdir()
        image_paths = _paths(staged, {key: key for key in LAYOUT})
        before = _database_digests(paths)
        fixed = _fixed_bytes(paths)
        source_inventory = _inventory(paths, _CheckpointCAS(source / 'artifacts'), live_root=source)
        for name in DATABASES:
            _backup_database(paths[name], image_paths[name])
        identities, allowed, cas_files = source_inventory
        for name, raw in {**fixed, **cas_files}.items():
            _write_bytes(staged / name, raw)
        _require(before == _database_digests(image_paths) == _database_digests(paths) and fixed == _fixed_bytes(paths) and
                 source_inventory == _inventory(paths, _CheckpointCAS(source / 'artifacts'), live_root=source) and
                 source_inventory == _inventory(image_paths, _CheckpointCAS(staged / 'artifacts')),
                 'source changed during capture or backup image differs from source')
        files = {}
        for name in sorted((*LAYOUT, *cas_files)):
            path = staged / name
            files[name] = {'bytes': archive._stat(path).st_size, 'sha256': archive._read(path)[0]}
        manifest = {'schema': archive.MANIFEST_SCHEMA, 'synthetic_only': True,
                    'checkpoint_id': uuid4().hex, 'created_at': datetime.now(timezone.utc).isoformat(),
                    'creator': creator, 'code_revision': code_revision, 'quiescence': quiescence,
                    'databases': before, 'revocation': REVOCATION, 'bindings': identities,
                    'allowed_unknown_task_ids': allowed, 'files': files}
        raw_manifest = archive.canonical(manifest) + b'\n'
        _write_bytes(staged / archive.MANIFEST_NAME, raw_manifest)
        anchor = archive._digest(raw_manifest)
        archive._verify_package(staged, archive._manifest(staged, anchor))
        _require_absent(target)
        staged.rename(target)
    return anchor


def restore_checkpoint(package_root, destination, *, expected_manifest_sha256, mode=archive.HISTORICAL_READ_ONLY):
    """Validate the anchored package, restore at a new directory, and mint runtime bindings."""
    package = Path(package_root)
    archive._stat(package, directory=True)
    target = _new_destination(destination, package)
    manifest = archive._manifest(package, expected_manifest_sha256)
    archive._verify_package(package, manifest)
    _verify_quiescence(manifest.get('quiescence'))
    _require(manifest.get('revocation') == REVOCATION, 'invalid checkpoint revocation provenance')
    with TemporaryDirectory(prefix='.m0-domain-restore-', dir=target.parent) as scratch:
        staged = Path(scratch) / 'payload'
        (staged / 'catalog').mkdir(parents=True)
        (staged / 'artifacts').mkdir()
        for name in (archive.MANIFEST_NAME, *manifest['files']):
            path = staged / name
            path.parent.mkdir(parents=True, exist_ok=True)
            archive._stat(package / name)
            shutil.copyfile(package / name, path)
        verified = archive._manifest(staged, expected_manifest_sha256)
        archive._verify_package(staged, verified)
        paths = _paths(staged, {name: name for name in LAYOUT})
        _require(_database_digests(paths) == verified.get('databases'), 'normalized SQLite image digest mismatch')
        identities, allowed, cas = _inventory(paths, _CheckpointCAS(staged / 'artifacts'))
        _require(identities == verified['bindings'] and allowed == verified['allowed_unknown_task_ids'] and
                 set(verified['files']) == set(LAYOUT) | set(cas),
                 'checkpoint manifest differs from admitted binding, task or CAS inventory')
        archive.write_recovery_bindings(staged, expected_manifest_sha256=expected_manifest_sha256, mode=mode)
        _require_absent(target)
        staged.rename(target)
    return {'dispatch_db': target / 'dispatch.sqlite3', 'content_db': target / 'content.sqlite3',
            'catalog_root': target / 'catalog', 'artifact_root': target / 'artifacts',
            'research_binding': target / 'research-binding.json', 'workflow_binding': target / 'workflow-binding.json',
            'manifest_sha256': expected_manifest_sha256, 'allowed_unknown_task_ids': allowed}
