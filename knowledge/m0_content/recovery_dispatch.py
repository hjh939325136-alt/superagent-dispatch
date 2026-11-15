"""Recovered checkout entry restricted to observation and authorized lookup."""
import json

from sa_dispatch.adapters import task_registry
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.executor import Executor
from sa_dispatch.routing import LOCAL_ROUTE, ledger_mode
from sa_dispatch.runner import ComponentRunner
from sa_dispatch.store_sqlite import SqliteStore
from tools.m0_backup.domain_binding import (IDENTITY, RecoveryBindingError,
    check_recovery_binding_current, check_recovery_runtime_paths, require_recovery_operation)
from tools.m0_manager.readonly import readonly_db


def _task(row):
    if row is None:
        raise RecoveryBindingError('RECOVERY_TASK_NOT_FOUND')
    task = dict(row)
    for name in ('params', 'input_refs', 'required_caps', 'scope', 'result'):
        raw = task[name + '_json']
        task[name] = json.loads(raw) if raw is not None else None
    return task


def _observe(db, action, task_id, artifacts):
    if action == 'schedule-status':
        tables = {'schedules': 'id', 'schedule_firings': 'scheduled_at,fire_key', 'schedule_events': 'seq'}
        rows = {name: [dict(row) for row in db.execute('SELECT * FROM ' + name + ' ORDER BY ' + order)]
                for name, order in tables.items()}
        return {'schedules': rows['schedules'], 'firings': rows['schedule_firings'],
                'events': rows['schedule_events'], 'scope': 'captured_ledger_all_routes',
                'stored_flags_are_historical': True, 'scheduler_started': False,
                'execution_authorized': False}
    if task_id is None:
        return {'tasks': [_task(row) for row in db.execute('SELECT * FROM tasks ORDER BY created_at,task_id')]}
    task = _task(db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone())
    if action == 'receipt':
        result = task['result'] or {}
        ref = result.get('receipt') or result.get('claude_report_ref') or result.get('host_report_ref')
        if ref is None:
            raise RecoveryBindingError('RECOVERY_RECEIPT_MISSING')
        return {'task_id': task_id, 'sha256': ref, 'response': json.loads(artifacts.get(ref))}
    ledger_events = [dict(row) for row in db.execute('SELECT * FROM events WHERE task_id=? ORDER BY seq', (task_id,))]
    if action == 'status':
        return {'task': task, 'events': ledger_events}
    events = [dict(row) for row in db.execute(
        'SELECT * FROM unified_events WHERE trace_id=? ORDER BY at,seq LIMIT 1000', (task_id,))]
    for event in events:
        event['refs'] = json.loads(event.pop('refs_json'))
        event['metadata'] = json.loads(event.pop('metadata_json'))
    reconciliations = [dict(row) for row in db.execute(
        'SELECT * FROM reconciliations WHERE task_id=? ORDER BY at,rowid', (task_id,))]
    return {'task_id': task_id, 'trace_id': task_id, 'events': events,
            'ledger_events': ledger_events, 'reconciliations': reconciliations}


def run_recovery_dispatch(config, plugins, binding, role, args):
    action = args[0] if args else None
    task_id = args[1] if len(args) == 2 else None
    if (action not in ('status', 'receipt', 'trace', 'reconcile', 'schedule-status') or len(args) > 2 or
            (action not in ('status', 'schedule-status') and task_id is None) or
            (action == 'schedule-status' and len(args) != 1) or
            (task_id is not None and not IDENTITY.fullmatch(task_id))):
        raise RecoveryBindingError('RECOVERY_DISPATCH_COMMAND_DENIED')
    if action == 'reconcile':
        if role != 'workflow':
            raise RecoveryBindingError('RECOVERY_LOOKUP_NOT_ADMITTED')
        require_recovery_operation(binding, 'operation.lookup', task_id)
    route = config.execution_route
    if not config.synthetic or route not in (None, LOCAL_ROUTE) or (action == 'reconcile' and route != LOCAL_ROUTE):
        raise RecoveryBindingError('RECOVERY_DISPATCH_ROUTE_REQUIRED')
    check_recovery_runtime_paths(binding.path.parent, config.db_path, config.artifact_root, config.journal_dir)
    current = check_recovery_binding_current(binding, role)
    ledger_identity = current.file_identities['dispatch.sqlite3']
    artifacts = ArtifactStore(config.artifact_root)
    with readonly_db(config.db_path, identity=ledger_identity) as db:
        expected_mode = 'mixed' if route == LOCAL_ROUTE else 'isolated'
        if ledger_mode(db) != expected_mode:
            raise RecoveryBindingError('RECOVERY_DISPATCH_ROUTE_REQUIRED')
        if action == 'reconcile':
            task = _task(db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone())
            if task['status'] != 'unknown':
                raise RecoveryBindingError('RECOVERY_TASK_NOT_UNKNOWN')
        else:
            data = _observe(db, action, task_id, artifacts)
    if action == 'reconcile':
        kinds = task_registry(config.plugins, plugins)
        store = SqliteStore(config.db_path, lease_ttl_seconds=config.lease_ttl_seconds, kinds=kinds,
                            component_config=config, route=LOCAL_ROUTE)
        task = store.get_task(task_id)
        if task.status != 'unknown':
            raise RecoveryBindingError('RECOVERY_TASK_NOT_UNKNOWN')
        data = Executor(config, store, ComponentRunner(config), kinds).reconcile(task)
    admission = binding.recovery_admission
    return {**data, **admission.provenance(), 'recovery_mode': admission.mode,
            'recovery_manifest_sha256': admission.manifest_sha256}
