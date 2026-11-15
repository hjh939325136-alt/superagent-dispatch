"""Read the current task/control ledger without initializing or projecting it."""
import json
import re
from datetime import timedelta
from .binding import now, instant, token, fixed_ref
from .readonly import readonly_db, SourceError

SUPPORTED = {'0010', '0011'}
TASK_FIELDS = 'task_id,kind,kind_version,track,status,fence,attempts,side_effect,access,created_at,updated_at,fingerprint'


def rows(db, sql, params=()):
    return [dict(row) for row in db.execute(sql, params)]


def tables(db):
    return {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def schema(db, source):
    available = tables(db)
    if 'schema_migrations' not in available:
        raise SourceError('source_schema_unsupported')
    version = db.execute('SELECT MAX(version) FROM schema_migrations').fetchone()[0]
    if version not in SUPPORTED or version not in source['accepted_schema_versions']:
        raise SourceError('source_schema_unsupported')
    if db.execute("SELECT 1 FROM tasks WHERE access IS NOT 'synthetic' LIMIT 1").fetchone():
        raise SourceError('source_scope_not_synthetic')
    return version, available


def safe(row):
    # Queries below never select body/argv/params. Enforce a second metadata limit.
    def bounded(value):
        if not isinstance(value,str):return value
        if len(value)>300:return 'withheld'
        if re.search(r'(?:^|[:@\s])[A-Za-z]:[\\/]|^[/\\]',value):return 'withheld:path_value'
        return value
    return {key:bounded(value) for key,value in row.items()}


def record(source, kind, row, observed_at, *, historical=False):
    row = safe(row)
    identity = str(row.get('task_id') or row.get('handoff_id') or row.get('event_id') or row.get('seq') or kind)
    return {**row, 'ref': fixed_ref(source, kind, identity, row), 'as_of': observed_at,
            'state_basis': 'historical' if historical else 'current'}


def projection(db, available, source):
    result = []
    if 'collector_cursors' not in available:
        return result
    for stream in ('events', 'control_events', 'schedule_events'):
        if stream not in available:
            continue
        maximum = db.execute(f'SELECT COALESCE(MAX(seq),0) FROM {stream}').fetchone()[0]
        row = db.execute('SELECT last_seq,authority,source_instance FROM collector_cursors WHERE source_id=? AND stream=?',
                         (source['source_id'], stream)).fetchone()
        usable = row and row['authority']=='local_owned' and row['source_instance']==source['source_instance']
        result.append({'stream': stream, 'source_max_seq': maximum,
                       'last_seq': row[0] if row else None,
                       'lag': max(0, maximum-row[0]) if usable else None,
                       'state': 'observed' if usable else 'source_rebind_required' if row else 'not_configured'})
    return result


def dispatch_query(source, op, args, role):
    observed = now()
    options = source.get('options', {})
    identity=options.get('identity')
    if not isinstance(identity,dict) or set(identity)!={'device','inode'} or any(type(v) is not int for v in identity.values()):
        raise SourceError('source_identity_unpinned')
    with readonly_db(source['locator'], identity=options.get('identity')) as db:
        version, available = schema(db, source)
        result = {'items': [], 'coverage': 'complete', 'unknown': [],
                  'source_instance': source['source_instance'], 'schema_version': version,
                  'observed_at': observed, 'state_basis': 'current',
                  'projection_lag': projection(db, available, source)}
        limit = args.get('limit', 50)
        if op == 'status':
            counts = rows(db, 'SELECT status,COUNT(*) AS count FROM tasks GROUP BY status ORDER BY status')
            result['items'] = [{'kind': 'task_counts', 'counts': counts, 'state_basis': 'current', 'as_of': observed,
                                'runtime_liveness': 'not_observed'}]
        elif op in ('task.get', 'trace.get'):
            task_id = args['task_id']
            selected = rows(db, f'SELECT {TASK_FIELDS} FROM tasks WHERE task_id=?', (task_id,))
            result['items'] = [record(source, 'task', row, observed) for row in selected]
            if args.get('ref') is not None and (not result['items'] or args['ref']!=result['items'][0]['ref']):
                raise SourceError('source_reference_mismatch')
            if op == 'trace.get' and selected:
                events = rows(db, 'SELECT seq,task_id,from_status,to_status,fence,at FROM events WHERE task_id=? ORDER BY seq LIMIT ?', (task_id, limit))
                evidence = rows(db, 'SELECT evidence_id,task_id,fence,kind,content_hash,artifact_hash,access,created_at FROM evidence WHERE task_id=? ORDER BY created_at LIMIT ?', (task_id, limit))
                result['events'] = [record(source, 'event', row, observed, historical=True) for row in events]
                result['evidence'] = [record(source, 'evidence', row, observed, historical=True) for row in evidence]
                result['coverage'] = 'partial' if len(events) == limit or len(evidence) == limit else 'complete'
                if 'claude_attempts' in available:
                    attempts = rows(db, '''SELECT task_id,fence,origin_fence,session_id,model,phase,
                        report_ref,updated_at,execution_authority FROM claude_attempts
                        WHERE task_id=? ORDER BY fence LIMIT ?''', (task_id, limit))
                    result['attempts'] = [record(source, 'claude_attempt', {
                        **row, 'lifecycle_coverage':'attempt_snapshot_only',
                        'runtime_liveness':'not_observed', 'model_usage':'follow_report_ref'}, observed,
                        historical=row['execution_authority']=='historical_only') for row in attempts]
                    if len(attempts)==limit:result['coverage']='partial'
                if {'control_reservations','control_budgets'} <= available:
                    reservations = rows(db, '''SELECT r.task_id,r.fence,r.budget_id,b.unit,
                        r.reserved_units,r.actual_units,r.state,r.evidence_ref,r.created_at,r.settled_at
                        FROM control_reservations r JOIN control_budgets b USING(budget_id)
                        WHERE r.task_id=? ORDER BY r.fence LIMIT ?''', (task_id,limit))
                    result['reservations'] = [record(source,'reservation',row,observed) for row in reservations]
                    if len(reservations)==limit:result['coverage']='partial'
        elif op == 'events.query':
            selected = rows(db, 'SELECT seq,event_id,source,source_id,trace_id,task_id,attempt_epoch,component,kind,at,class FROM unified_events WHERE seq>? ORDER BY seq LIMIT ?', (args.get('after_seq', 0), limit))
            result['items'] = [record(source, 'event', row, observed, historical=True) for row in selected]
            result['state_basis'] = 'historical'
            result['coverage'] = 'partial' if len(selected) == limit else 'complete'
        elif op == 'handoff.list':
            selected = rows(db, 'SELECT handoff_id,project_id,scope,state,expires_at,created_at,updated_at FROM control_handoffs ORDER BY updated_at DESC,handoff_id LIMIT ?', (limit,))
            for row in selected:
                row['effective_state'] = 'expired' if row['state'] in ('offered', 'accepted') and instant(row['expires_at']) <= instant(observed) else row['state']
            if role in ('technical','progress'):
                selected=[{k:v for k,v in row.items() if k not in ('project_id','scope')} for row in selected]
            result['items'] = [record(source, 'handoff', row, observed) for row in selected]
            result['coverage'] = 'partial' if len(selected) == limit else 'complete'
        elif op == 'attention.list':
            from .attention import attention
            result['items'], result['unknown'] = attention(db, available, source, observed)
            if result['unknown']:
                result['coverage'] = 'partial'
            if len(result['items']) > limit:
                result['items'] = result['items'][:limit]
                result['coverage'] = 'partial'
        return result
