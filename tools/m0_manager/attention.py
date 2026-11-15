"""Versioned, read-time local attention rules; no queue or automatic action."""
from datetime import timedelta
from .binding import instant, fixed_ref
from .dispatch_source import rows


def attention(db, available, source, observed):
    at = instant(observed)
    items, unknown, seen = [], [], set()
    options = source.get('options', {})
    def add(rule, identity, state, basis='current'):
        key = (rule, identity)
        if key in seen:
            return
        seen.add(key)
        data = {'rule': rule+'@1', 'subject_id': identity, 'state': state}
        items.append({**data, 'ref': fixed_ref(source, 'attention', identity, data),
                      'as_of': observed, 'state_basis': basis,
                      'owner': 'technical' if rule in ('A2','A5','A7','A8','A9','A10') else 'manager',
                      'next_step': 'inspect_evidence'})
    for row in rows(db, "SELECT task_id,updated_at FROM tasks WHERE status='unknown'"):
        if at-instant(row['updated_at']) >= timedelta(hours=1):
            add('A1', row['task_id'], 'unknown_over_1h')
    for table in ('host_attempts', 'claude_attempts'):
        if table not in available:
            unknown.append(table+':table_absent')
            continue
        for row in rows(db, f"SELECT task_id,fence,phase FROM {table} WHERE phase IN ('uncertain','orphan_unknown','supervisor_lost')"):
            add('A2', row['task_id']+':'+str(row['fence']), row['phase'])
    # Current reconciliation schema stores found/absent/mismatch/error/decided,
    # not 'still_unknown'. Infer repeated unsuccessful checks only alongside a
    # currently unknown task; keep that distinction explicit in the rule code.
    for row in rows(db, "SELECT task_id FROM tasks WHERE status='unknown'"):
        recent = rows(db, 'SELECT result FROM reconciliations WHERE task_id=? ORDER BY at DESC,rowid DESC LIMIT 2', (row['task_id'],))
        if len(recent) == 2 and all(r['result'] in ('absent','mismatch','error') for r in recent):
            add('A3', row['task_id'], 'repeated_unresolved_reconciliation')
    approval_rows = rows(db, """SELECT r.task_id,r.fingerprint,t.fingerprint AS current_fingerprint,r.approval_scope,a.fingerprint AS approved_fingerprint,
        a.scope,a.decision,a.expires_at FROM control_task_rules r JOIN tasks t ON t.task_id=r.task_id
        LEFT JOIN control_approvals a ON a.approval_id=r.approval_id
        WHERE r.approval_scope IS NOT NULL AND t.status NOT IN ('succeeded','no_new','failed','cancelled')""")
    for row in approval_rows:
        valid = (row['decision']=='approved' and row['fingerprint']==row['approved_fingerprint']==row['current_fingerprint'] and
                 row['scope']==row['approval_scope'] and row['expires_at'] and instant(row['expires_at']) > at)
        if not valid:
            add('A4', row['task_id'], 'approval_missing_or_invalid')
        elif instant(row['expires_at'])-at <= timedelta(hours=24):
            add('A4', row['task_id'], 'approval_expiring')
    for row in rows(db, "SELECT task_id,fence,uncertain_at,created_at FROM control_reservations WHERE state='uncertain'"):
        if at-instant(row['uncertain_at'] or row['created_at']) >= timedelta(hours=options.get('uncertain_hours', 1)):
            add('A5', row['task_id']+':'+str(row['fence']), 'reservation_uncertain')
    for row in rows(db, 'SELECT budget_id,limit_units,reserved_units,used_units,starts_at,ends_at FROM control_budgets'):
        if instant(row['starts_at']) <= at < instant(row['ends_at']) and (row['used_units']+row['reserved_units'])*10 >= row['limit_units']*9:
            add('A5', row['budget_id'], 'budget_near_limit')
    for row in rows(db, "SELECT task_id,at FROM control_events WHERE kind='budget.exceeded'"):
        # An old exceeded event is historical, not evidence that today's budget
        # is exceeded. Only match an active budget through its task rule.
        budget = db.execute('SELECT b.starts_at,b.ends_at FROM control_task_rules r JOIN control_budgets b ON b.budget_id=r.budget_id WHERE r.task_id=?',(row['task_id'],)).fetchone()
        if budget and instant(budget['starts_at']) <= instant(row['at']) <= at < instant(budget['ends_at']):
            add('A5', row['task_id'], 'budget_exceeded_this_period', 'historical')
    for row in rows(db, "SELECT handoff_id,expires_at FROM control_handoffs WHERE state IN ('offered','accepted','expired')"):
        if instant(row['expires_at']) <= at+timedelta(hours=24):
            add('A6', row['handoff_id'], 'handoff_expired' if instant(row['expires_at']) <= at else 'handoff_expiring')
    for row in rows(db, "SELECT f.fire_key,f.scheduled_at,f.outcome,s.interval_seconds FROM schedule_firings f JOIN schedules s ON s.id=f.schedule_id WHERE f.outcome IN ('pending','rejected')"):
        if row['outcome']=='rejected' or at-instant(row['scheduled_at']) >= timedelta(seconds=row['interval_seconds']*options.get('pending_ticks', 3)):
            add('A8', row['fire_key'], 'schedule_'+row['outcome'])
    for row in rows(db, "SELECT schedule_id,kind FROM schedule_events WHERE kind='schedule.template_error'"):
        add('A8', row['schedule_id'], 'template_error', 'historical')
    if 'collector_gaps' in available:
        for row in rows(db, 'SELECT gap_id,kind FROM collector_gaps WHERE resolved_at IS NULL'):
            add('A9', row['gap_id'], row['kind'])
    else:
        unknown.append('collector_gaps:table_absent')
    from .dispatch_source import projection
    for lag in projection(db, available, source):
        if lag['state']=='source_rebind_required':
            add('A9',lag['stream'],'source_rebind_required')
        if lag['lag'] is not None and lag['lag'] > options.get('projection_lag_limit',100):
            add('A9', lag['stream'], 'projection_lag')
    return items, unknown
