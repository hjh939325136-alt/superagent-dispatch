"""Count proven local synthetic Claude launches; never infer tokens or currency."""
from contextlib import closing
import json
from .canonical import canonical_json
from .control_policy import ControlPolicy
from .models import DispatchError

def settle_claude_runs(store,artifacts,task_id,*,actor='claude-settlement'):
    policy=ControlPolicy(store,artifacts=artifacts)
    with closing(store._connect()) as conn:
        task=dict(conn.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone())
        rows=[dict(r) for r in conn.execute('SELECT * FROM claude_attempts WHERE task_id=? ORDER BY fence',(task_id,))]
        reservations={r['fence']:dict(r) for r in conn.execute('SELECT r.*,b.unit FROM control_reservations r JOIN control_budgets b USING(budget_id) WHERE task_id=?',(task_id,))}
    if task['kind']!='claude.synthetic' or task['side_effect']!='none' or task['access']!='synthetic':
        raise DispatchError('Claude count settlement requires a synthetic read-only task')
    result={'unit':'claude_run','settled':[],'already_settled':[],'held':[],'report_refs':[], 'cost_unknown':not bool(rows), 'unproven_attempts':[],
        'observation':'not_observed' if not rows else 'attempts_observed'}
    by_fence={r['fence']:r for r in rows};terminals={};conflicts=set()
    for row in rows:
        if not row['report_ref']:
            result['cost_unknown']=True
            result['unproven_attempts'].append({'fence':row['fence'],'origin_fence':row['origin_fence'],'reason':'report_missing'})
            continue
        try:
            report=json.loads(artifacts.get(row['report_ref']))
            expected={'schema':'sa_dispatch.claude_report.v1','synthetic':True,
                **{k:row[k] for k in ('task_id','fence','origin_fence','session_id','request_id','model','prompt_ref')}}
            if row['execution_authority']!='local_owned' or any(type(report.get(k)) is not type(v) or report.get(k)!=v for k,v in expected.items()):raise ValueError('unbound report')
            if report.get('status') not in ('succeeded','failed','interrupted') or type(report.get('exit_code')) is not int:raise ValueError('unproven exit')
            origin=by_fence.get(row['origin_fence'])
            if not origin or origin['request_id']!=task_id+':'+str(origin['fence']) or origin['origin_fence']!=origin['fence'] or origin['execution_authority']!='local_owned' or origin['fence']>row['fence']:raise ValueError('missing origin')
            if any(origin[k]!=row[k] for k in ('session_id','request_id','model','prompt_ref','run_dir')):raise ValueError('foreign origin')
            exit_status=json.loads(artifacts.get(report['exit_ref']));launch=json.loads(artifacts.get(report['launch_ref']))
            launch_expected={'schema':'sa_dispatch.claude_launch.v1','synthetic':True,
                **{k:origin[k] for k in ('task_id','origin_fence','session_id','request_id','model','prompt_ref')}}
            if any(type(launch.get(k)) is not type(v) or launch.get(k)!=v for k,v in launch_expected.items()):raise ValueError('unbound launch')
            exit_expected={**launch_expected,'schema':'sa_dispatch.claude_exit.v1'}
            if any(type(exit_status.get(k)) is not type(v) or exit_status.get(k)!=v for k,v in exit_expected.items()):raise ValueError('unbound exit')
            for key in ('child_pid','supervisor_pid'):
                if type(launch.get(key)) is not int or launch[key]<=0 or launch[key]!=exit_status.get(key):raise ValueError('launch exit mismatch')
            if type(exit_status.get('exit_code')) is not int or exit_status['exit_code']!=report['exit_code']:raise ValueError('exit mismatch')
            artifacts.get(report['stdout_ref']);artifacts.get(report['prompt_ref'])
            if report.get('init_ref'):artifacts.get(report['init_ref'])
            terminal={'schema':'sa_dispatch.claude_terminal.v1','synthetic':True,
                **{k:origin[k] for k in ('task_id','origin_fence','session_id','request_id','model','prompt_ref')},
                **{k:report.get(k) for k in ('exit_ref','launch_ref','stdout_ref','init_ref')}}
            terminal_ref=artifacts.put(canonical_json(terminal))
            previous=terminals.get(origin['fence'])
            if previous and previous!=terminal_ref:
                conflicts.add(origin['fence']);raise ValueError('conflicting terminal proofs')
            terminals[origin['fence']]=terminal_ref
            result['report_refs'].append(row['report_ref'])
            result['cost_unknown'] |= report.get('cost_unknown') is not False or report.get('usage_known') is not True
        except (ValueError,KeyError,TypeError,OSError,DispatchError):
            # Missing evidence does not refund or fabricate zero usage.
            result['cost_unknown']=True
            result['unproven_attempts'].append({'fence':row['fence'],'origin_fence':row['origin_fence'],'reason':'terminal_evidence_unproven'})
            continue
    for origin in conflicts:
        terminals.pop(origin,None)
        for row in rows:
            if row['origin_fence']==origin:
                result['unproven_attempts'].append({'fence':row['fence'],'origin_fence':origin,'reason':'conflicting_terminal_proofs'})
        result['cost_unknown']=True
    for fence,reservation in reservations.items():
        row=by_fence.get(fence)
        terminal_ref=terminals.get(row['origin_fence']) if row else None
        if reservation['unit']!='claude_run' or row is None or row['execution_authority']!='local_owned' or not terminal_ref:
            if reservation['state']!='settled':result['held'].append({'fence':fence,'reason':'unit_or_launch_exit_unproven'})
            result['cost_unknown']=True
            continue
        origin=by_fence.get(row['origin_fence'])
        if not origin or any(origin[k]!=row[k] for k in ('session_id','request_id','model','prompt_ref','run_dir')):
            result['held'].append({'fence':fence,'reason':'observer_origin_unproven'});continue
        actual=1 if fence==row['origin_fence'] else 0
        proof={'schema':'sa_dispatch.claude_run_usage.v1','synthetic':True,'task_id':task_id,'fence':fence,
            'origin_fence':row['origin_fence'],'budget_id':reservation['budget_id'],'fingerprint':reservation['fingerprint'],
            'unit':'claude_run','actual_units':actual,'final_usage':True,'terminal_ref':terminal_ref}
        proof_ref=artifacts.put(canonical_json(proof))
        policy.settle(task_id,fence,actual_units=actual,evidence_ref=proof_ref,actor=actor)
        result['already_settled' if reservation['state']=='settled' else 'settled'].append({'fence':fence,'actual_units':actual,'evidence_ref':proof_ref})
    return result


def settle_task(store,artifacts,task_id,actor='claude-adapter'):
    """Explicit evidence-only settlement: no process, spawn, claim or source calls."""
    return settle_claude_runs(store,artifacts,task_id,actor=actor)
