"""Persistent execution boundaries and the offline mixed activation transaction."""
import json
import re
import sqlite3
from contextlib import closing
from pathlib import Path

from .canonical import canonical_json, sha256_ref
from .clock import Clock, timestamp
from .models import DispatchError
from .states import TERMINAL


HTTP_ROUTE = 'http_synthetic'
LOCAL_ROUTE = 'local_synthetic'
ROUTES = (HTTP_ROUTE, LOCAL_ROUTE)
ROUTED_TABLES = {'tasks': 'task_id', 'devices': 'device_id', 'schedules': 'id',
                 'schedule_firings': 'fire_key'}


class RouteRejected(DispatchError):
    code = 'E_EXECUTION_ROUTE'


def ledger_mode(conn):
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='ledger_mode' AND type='table'").fetchone():
        return 'isolated'
    row = conn.execute('SELECT mode FROM ledger_mode WHERE id=1').fetchone()
    if row is None or row[0] not in ('isolated', 'mixed'):
        raise RouteRejected('invalid persistent ledger mode')
    return row[0]


def row_digest(row):
    value = dict(row)
    value.pop('execution_route', None)
    return sha256_ref(canonical_json(value))


def _digest(value):
    return isinstance(value, str) and re.fullmatch(r'sha256:[0-9a-f]{64}', value) is not None


def audit(conn, action, table, identity, route, actor, at, evidence_ref, pre_state_hash=None, fence=None):
    conn.execute('''INSERT INTO route_audit(action,object_table,object_id,execution_route,
        actor,at,evidence_ref,pre_state_hash,fence) VALUES(?,?,?,?,?,?,?,?,?)''',
        (action, table, identity, route, actor, at, evidence_ref, pre_state_hash, fence))


def activation_manifest(conn, *, actor, bindings):
    from .bundle import snapshot_records
    return {'schema': 'sa_dispatch.mixed_activation.v1', 'actor': actor,
            'ledger_hash': sha256_ref(canonical_json(snapshot_records(conn))),
            'bindings_hash': sha256_ref(canonical_json(bindings))}


def binding_evidence(table, identity, route, pre_state_hash, source_hash, *, fence=None):
    """Build metadata-only evidence. The operator retains the source offline."""
    return {'schema': 'sa_dispatch.route_binding.v1', 'object_table': table, 'object_id': identity,
            'execution_route': route, 'pre_state_hash': pre_state_hash, 'fence': fence,
            'source_hash': source_hash}


def read_route_evidence(artifacts, ref):
    """Read only canonical metadata JSON, including when re-exporting prior evidence."""
    try:
        content = artifacts.get(ref)
        value = json.loads(content)
        if (sha256_ref(content) != ref or not isinstance(value, dict)
                or canonical_json(value) != content):
            raise ValueError('noncanonical route evidence')
    except (OSError, ValueError, RuntimeError, DispatchError) as exc:
        raise RouteRejected('route evidence is missing or invalid') from exc
    return value


def validate_binding_evidence(proof, table, identity, route, pre_state_hash, fence):
    source = proof.get('source_hash')
    expected = binding_evidence(table, identity, route, pre_state_hash, source, fence=fence)
    if not _digest(source) or canonical_json(proof) != canonical_json(expected):
        raise RouteRejected('source evidence does not identify the bound row and route')


def activate_mixed(path, *, actor, evidence_ref, bindings, artifacts, clock=None):
    """Apply an operator-prepared mapping after all old writers have stopped.

    The evidence digest identifies the maintenance record. Each binding also
    carries the exact current row digest and a separate evidence artifact.
    Task bindings require the current fence, including zero before first claim.
    The operator independently verifies the backup, stopped writers and offline sources.
    """
    if not isinstance(actor, str) or not actor.strip() or not _digest(evidence_ref):
        raise RouteRejected('activation requires an actor and maintenance evidence digest')
    if not isinstance(bindings, dict) or not set(bindings) <= ROUTED_TABLES.keys():
        raise RouteRejected('invalid activation bindings')
    from .bundle import snapshot_records
    from .migrations import apply_m0_schema
    at = timestamp((clock or Clock()).now())
    uri = Path(path).resolve().as_uri() + '?mode=rw'
    with closing(sqlite3.connect(uri, uri=True, timeout=10)) as conn, conn:
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('BEGIN IMMEDIATE')
        if ledger_mode(conn) != 'isolated':
            raise RouteRejected('mixed activation requires an isolated ledger')
        if read_route_evidence(artifacts, evidence_ref) != activation_manifest(conn, actor=actor, bindings=bindings):
            raise RouteRejected('maintenance evidence does not match the ledger and route bindings')
        apply_m0_schema(conn, at)
        if conn.execute("SELECT 1 FROM tasks WHERE access!='synthetic' OR egress!='none'").fetchone():
            raise RouteRejected('mixed activation requires synthetic tasks without egress')
        if conn.execute("SELECT 1 FROM leases WHERE state='active'").fetchone() or conn.execute(
                "SELECT 1 FROM tasks WHERE status='leased'").fetchone():
            raise RouteRejected('mixed activation requires all active leases to settle')
        for table, key in ROUTED_TABLES.items():
            table_bindings = bindings.get(table, {})
            if not isinstance(table_bindings, dict):
                raise RouteRejected('invalid activation mapping for ' + table)
            for identity, binding in table_bindings.items():
                required = {'execution_route', 'pre_state_hash', 'evidence_ref'}
                if table == 'tasks':
                    required.add('fence')
                if (not isinstance(identity, str) or not isinstance(binding, dict)
                        or set(binding) != required or binding['execution_route'] not in ROUTES
                        or not _digest(binding['evidence_ref'])):
                    raise RouteRejected('invalid activation binding for ' + table)
                row = conn.execute(f'SELECT * FROM {table} WHERE {key}=?', (identity,)).fetchone()
                fence = binding.get('fence')
                if (row is None or row['execution_route'] is not None or row_digest(row) != binding['pre_state_hash']
                        or (table == 'tasks' and (type(fence) is not int or fence != row['fence']))):
                    raise RouteRejected('activation binding does not match existing row')
                route = binding['execution_route']
                proof = read_route_evidence(artifacts, binding['evidence_ref'])
                validate_binding_evidence(proof, table, identity, route, binding['pre_state_hash'], fence)
                conn.execute(f'UPDATE {table} SET execution_route=? WHERE {key}=?', (route, identity))
                audit(conn, 'bind', table, identity, route, actor, at, binding['evidence_ref'],
                      binding['pre_state_hash'], fence)
        conn.execute('UPDATE schedules SET enabled=0 WHERE execution_route IS NULL')
        for row in conn.execute('SELECT * FROM devices WHERE execution_route IS NULL ORDER BY device_id'):
            audit(conn, 'disable', 'devices', row['device_id'], None, actor, at, None, row_digest(row))
        validate_mixed_records(snapshot_records(conn))
        audit(conn, 'activate', 'ledger_mode', '1', None, actor, at, evidence_ref)
        conn.execute("UPDATE ledger_mode SET mode='mixed' WHERE id=1")


def validate_mixed_records(records):
    """Apply the same route checks to a ledger snapshot and a portable layout."""
    tasks = {row['task_id']: row['execution_route'] for row in records['tasks']}
    if any(row['access'] != 'synthetic' or row['egress'] != 'none'
           or (row['execution_route'] is None and row['status'] not in TERMINAL)
           for row in records['tasks']):
        raise RouteRejected('mixed ledger has non-synthetic or unbound live tasks')
    if any(row['access'] != 'synthetic' for row in records['evidence']):
        raise RouteRejected('mixed ledger has non-synthetic evidence')

    schedules = {row['id']: row['execution_route'] for row in records['schedules']}
    if any(row['enabled'] and row['execution_route'] is None for row in records['schedules']):
        raise RouteRejected('mixed ledger has an unbound enabled schedule')
    for row in records['schedule_firings']:
        route, schedule, task = row['execution_route'], row['schedule_id'], row['task_id']
        if (schedule not in schedules or (task is not None and task not in tasks)
                or (row['outcome'] == 'pending' and route is None)
                or (route is not None and (schedules[schedule] != route
                                           or (task is not None and tasks[task] != route)))):
            raise RouteRejected('mixed ledger has an unbound pending or inconsistent schedule firing')

    for table, authority in (('transport_handoffs', 'handoff'), ('transport_artifact_grants', 'artifact')):
        for row in records[table]:
            if row['task_id'] not in tasks:
                raise RouteRejected('mixed ledger has orphan HTTP authority')
            if tasks[row['task_id']] == LOCAL_ROUTE:
                raise RouteRejected('local task has HTTP ' + authority + ' authority')

    lease_tasks = {row['lease_id']: row['task_id'] for row in records['leases']}
    if any(row['lease_id'] is not None and lease_tasks.get(row['lease_id']) != row['task_id']
           for row in records['transport_artifact_grants']):
        raise RouteRejected('HTTP artifact authority disagrees with its lease task')

    devices = {row['device_id']: row['execution_route'] for row in records.get('devices', [])}
    audited_devices = {row['object_id']: row['execution_route'] for row in records['route_audit']
                       if row['object_table'] == 'devices'}
    historical = {}
    for row in records['leases']:
        task, device = row['task_id'], row['device_id']
        if task not in tasks:
            raise RouteRejected('mixed ledger has orphan lease history')
        route = tasks[task]
        if route is None:
            continue
        if historical.setdefault(device, route) != route:
            raise RouteRejected('lease history binds one device to multiple routes')
    for device, route in historical.items():
        if devices.get(device) not in (None, route):
            raise RouteRejected('device ownership conflicts with routed lease history')
        if audited_devices.get(device) not in (None, route):
            raise RouteRejected('device audit disagrees with routed lease history')
