"""Transaction-local authority for the fixed common-agent provider request."""

from uuid import uuid4

from .canonical import canonical_json, sha256_ref
from .control_policy import _budget_config, _event, _write_budget
from .models import DispatchError


BUDGET_ID = 'agent-external:provider-request'
SCOPE = 'agent-external:deepseek:request'
OPERATOR = 'common-agent-operator'


def _budget(grant, expires_at):
    return _budget_config(BUDGET_ID, period='one-lifetime-slot', unit='provider_request',
                          limit_units=1, starts_at=grant.scheduled_at, ends_at=expires_at)


def register_budget(conn, grant, expires_at):
    config = _budget(grant, expires_at)
    if conn.execute('SELECT 1 FROM control_budgets WHERE budget_id=?', (BUDGET_ID,)).fetchone():
        raise DispatchError('common agent budget identifier is already in use')
    _write_budget(conn, config, None)


def read_budget(conn, grant, expires_at):
    row = conn.execute('SELECT * FROM control_budgets WHERE budget_id=?', (BUDGET_ID,)).fetchone()
    expected = _budget(grant, expires_at)
    if row is None or any(row[key] != value for key, value in expected.items()):
        raise DispatchError('common agent budget identity or limit changed')
    return row


def _binding(task, authority):
    return {
        'task_id': task['task_id'], 'fingerprint': task['fingerprint'],
        'execution_route': task['execution_route'], **authority,
    }


def _approval(task, authority):
    return {'schema': 'sa_dispatch.agent_external_approval.v1', 'decision': 'approved',
            'budget_id': BUDGET_ID, 'reserve_units': 1, 'scope': SCOPE,
            **_binding(task, authority)}


def record_approval(conn, task, authority, put_evidence, at):
    approval_id = 'a_' + uuid4().hex
    ref = put_evidence(canonical_json(_approval(task, authority)))
    conn.execute('''INSERT INTO control_approvals(approval_id,task_id,fingerprint,scope,decision,actor,
        evidence_ref,expires_at,at) VALUES(?,?,?,?,'approved',?,?,?,?)''',
        (approval_id, task['task_id'], task['fingerprint'], SCOPE, OPERATOR,
         ref, authority['expires_at'], at))
    changed = conn.execute('UPDATE control_task_rules SET approval_id=? WHERE task_id=? AND approval_id IS NULL',
                           (approval_id, task['task_id'])).rowcount
    if changed != 1:
        raise DispatchError('common agent approval requires an unapproved task rule')
    _event(conn, 'external-approval:'+task['task_id'], 'approval.decided', OPERATOR, at,
           task_id=task['task_id'], refs=(ref,))


def reserve_request(conn, task, authority, get_evidence, device, at):
    task_id, fingerprint = task['task_id'], task['fingerprint']
    if (not conn.in_transaction or task['fence'] != 0 or task['attempts'] != 0
            or task['status'] != 'queued' or device != authority['device_id']):
        raise DispatchError('common agent reservation requires its first claim transaction')
    rule = conn.execute('SELECT * FROM control_task_rules WHERE task_id=?', (task_id,)).fetchone()
    if (rule is None or (rule['fingerprint'], rule['budget_id'], rule['reserve_units'], rule['approval_scope'])
            != (fingerprint, BUDGET_ID, 1, SCOPE)):
        _event(conn, 'external-rule:'+task_id, 'permission_conflict', device, at,
               task_id=task_id, fence=1, code='policy_task_binding_mismatch')
        return False
    approval = conn.execute('''SELECT * FROM control_approvals WHERE task_id=? AND scope=?
        ORDER BY at DESC, rowid DESC LIMIT 1''', (task_id, SCOPE)).fetchone()
    expected = canonical_json(_approval(task, authority))
    if (approval is None or approval['approval_id'] != rule['approval_id']
            or approval['fingerprint'] != fingerprint or approval['decision'] != 'approved'
            or approval['actor'] != OPERATOR or approval['expires_at'] != authority['expires_at']
            or approval['expires_at'] <= at or approval['evidence_ref'] != sha256_ref(expected)):
        _event(conn, 'external-no-approval:'+task_id, 'approval.requested', device, at,
               task_id=task_id, fence=1, code='missing_stale_or_denied_approval')
        return False
    if get_evidence(approval['evidence_ref'], len(expected)) != expected:
        raise DispatchError('common agent approval evidence changed')
    if conn.execute('SELECT 1 FROM control_reservations WHERE budget_id=?', (BUDGET_ID,)).fetchone():
        raise DispatchError('common agent lifetime request already has a reservation')
    changed = conn.execute('''UPDATE control_budgets SET reserved_units=1
        WHERE budget_id=? AND period='one-lifetime-slot' AND unit='provider_request' AND limit_units=1
        AND reserved_units=0 AND used_units=0 AND starts_at<=? AND ends_at>?''', (BUDGET_ID, at, at)).rowcount
    if changed != 1:
        _event(conn, 'external-budget:'+task_id, 'budget.exceeded', device, at,
               task_id=task_id, fence=1, code='insufficient_remaining_units')
        return False
    conn.execute('''INSERT INTO control_reservations(task_id,fence,budget_id,fingerprint,
        reserved_units,state,created_at) VALUES(?,1,?,?,1,'reserved',?)''',
        (task_id, BUDGET_ID, fingerprint, at))
    _event(conn, 'reserved:'+task_id+':1', 'budget.reserved', device, at, task_id=task_id, fence=1)
    return True


def settle_request(conn, task, authority, result, put_evidence, at):
    task_id, fingerprint = task['task_id'], task['fingerprint']
    if task['fence'] != 1 or task['attempts'] != 1 or task['status'] != 'leased':
        raise DispatchError('common agent settlement requires the original live fence')
    receipt = {
        'schema': 'sa_dispatch.agent_external_usage.v1', **_binding(task, authority),
        'fence': 1, 'budget_id': BUDGET_ID, 'unit': 'provider_request',
        'actual_units': 1, 'final_usage': True, 'cost': None, 'response': result,
    }
    evidence = put_evidence(canonical_json(receipt))
    changed = conn.execute("""UPDATE control_reservations SET state='settled',actual_units=1,
        evidence_ref=?,settled_at=? WHERE task_id=? AND fence=1 AND budget_id=? AND fingerprint=?
        AND state='reserved' AND reserved_units=1""", (evidence, at, task_id, BUDGET_ID, fingerprint)).rowcount
    budget = conn.execute('''UPDATE control_budgets SET used_units=1,reserved_units=0
        WHERE budget_id=? AND unit='provider_request' AND limit_units=1 AND used_units=0 AND reserved_units=1''',
        (BUDGET_ID,)).rowcount
    if changed != 1 or budget != 1:
        raise DispatchError('common agent request reservation does not match its completion')
    _event(conn, 'settled:'+task_id+':1', 'budget.settled', authority['device_id'], at,
           task_id=task_id, fence=1, refs=(evidence,))
