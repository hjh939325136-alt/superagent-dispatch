"""Synthetic, trusted-local budget/approval/handoff policy.

No host or conversation is contacted. before_claim joins the caller's write
transaction, so reservation and the lease either both commit or both roll back.
"""
from contextlib import closing, contextmanager
from datetime import datetime, timedelta
import json
import re
from uuid import uuid4

from .clock import timestamp
from .models import DispatchError
from .routing import ROUTES, RouteRejected, ledger_mode
from .unified_events import _ulid

SCHEMA = """
CREATE TABLE IF NOT EXISTS control_budgets(
 budget_id TEXT PRIMARY KEY, period TEXT NOT NULL, unit TEXT NOT NULL,
 limit_units INTEGER NOT NULL CHECK(limit_units>=0), reserved_units INTEGER NOT NULL DEFAULT 0 CHECK(reserved_units>=0),
 used_units INTEGER NOT NULL DEFAULT 0 CHECK(used_units>=0), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS control_approvals(
 approval_id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(task_id),
 fingerprint TEXT NOT NULL, scope TEXT NOT NULL, decision TEXT NOT NULL CHECK(decision IN('approved','denied')),
 actor TEXT NOT NULL, evidence_ref TEXT NOT NULL, expires_at TEXT NOT NULL, at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS control_task_rules(
 task_id TEXT PRIMARY KEY REFERENCES tasks(task_id), fingerprint TEXT NOT NULL,
 budget_id TEXT REFERENCES control_budgets(budget_id), reserve_units INTEGER NOT NULL CHECK(reserve_units>=0),
 approval_scope TEXT, approval_id TEXT REFERENCES control_approvals(approval_id));
CREATE TABLE IF NOT EXISTS control_reservations(
 task_id TEXT NOT NULL REFERENCES tasks(task_id), fence INTEGER NOT NULL CHECK(fence>=1),
 budget_id TEXT NOT NULL REFERENCES control_budgets(budget_id), fingerprint TEXT NOT NULL,
 reserved_units INTEGER NOT NULL CHECK(reserved_units>=0), actual_units INTEGER CHECK(actual_units>=0),
 state TEXT NOT NULL CHECK(state IN('reserved','uncertain','settled')),
 evidence_ref TEXT, created_at TEXT NOT NULL, settled_at TEXT,
 PRIMARY KEY(task_id,fence));
CREATE TABLE IF NOT EXISTS control_handoffs(
 handoff_id TEXT PRIMARY KEY, from_conv TEXT NOT NULL, to_conv TEXT NOT NULL,
 project_id TEXT NOT NULL, scope TEXT NOT NULL CHECK(scope IN('read_status','instruct','takeover')), scope_refs_json TEXT NOT NULL,
 authorized_by TEXT, authorization_ref TEXT, expires_at TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN('offered','accepted','completed','rejected','expired')), receipt_ref TEXT, evidence_refs_json TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS control_events(
 seq INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE, event_key TEXT NOT NULL UNIQUE,
 kind TEXT NOT NULL, task_id TEXT, fence INTEGER, actor TEXT NOT NULL, at TEXT NOT NULL,
 refs_json TEXT NOT NULL, detail_code TEXT NOT NULL);
"""


def initialize_schema(conn):
    for statement in SCHEMA.split(';'):
        if statement.strip():
            conn.execute(statement)
    columns={row[1] for row in conn.execute('PRAGMA table_info(control_reservations)')}
    if 'uncertain_at' not in columns:
        conn.execute('ALTER TABLE control_reservations ADD COLUMN uncertain_at TEXT')
    conn.execute("""UPDATE control_reservations SET uncertain_at=COALESCE(
        (SELECT MIN(at) FROM control_events e WHERE e.task_id=control_reservations.task_id
          AND e.fence=control_reservations.fence AND e.kind='budget.uncertain'),created_at)
        WHERE state='uncertain' AND uncertain_at IS NULL""")


def _id(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:@/#-]{0,199}', value):
        raise DispatchError('bounded metadata identifier required')
    return value


def _units(value):
    if type(value) is not int or not 0 <= value <= 10 ** 12:
        raise DispatchError('nonnegative integer units required, maximum 10^12')
    return value


def _time(value):
    try:
        return timestamp(datetime.fromisoformat(value.replace('Z', '+00:00')))
    except (ValueError,TypeError,AttributeError) as error:
        raise DispatchError('timezone-aware timestamp required') from error


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', value):
        raise DispatchError('hashed artifact reference required')
    return value


def _event(conn, key, kind, actor, at, *, task_id=None, fence=None, refs=(), code='observed'):
    conn.execute('''INSERT OR IGNORE INTO control_events(event_id,event_key,kind,task_id,fence,actor,at,refs_json,detail_code)
        VALUES(?,?,?,?,?,?,?,?,?)''', (_ulid(at),key,kind,task_id,fence,actor,at,json.dumps(list(refs)),code))


def expire_reservation(conn,task_id,fence,actor,at):
    """Join sweep/mark_unknown transaction; uncertainty never refunds held units."""
    if not conn.in_transaction: raise DispatchError('reservation expiry requires the caller write transaction')
    _id(actor); at=_time(at)
    if type(fence) is not int or fence<1: raise DispatchError('positive attempt fence required')
    changed=conn.execute("""UPDATE control_reservations SET state='uncertain',uncertain_at=?
        WHERE task_id=? AND fence=? AND state='reserved'""",(at,task_id,fence)).rowcount
    if changed:
        _event(conn,'uncertain:'+task_id+':'+str(fence),'budget.uncertain',actor,at,
               task_id=task_id,fence=fence,code='reservation_retained')
    return bool(changed)


def before_claim(conn, task, next_fence, actor, at):
    """Return False to skip a candidate; caller MUST commit denial events.

    Call before mutating tasks/inserting leases, inside BEGIN IMMEDIATE. Never
    invoke this separately and then create a lease in another transaction.
    """
    if not conn.in_transaction:
        raise DispatchError('claim policy requires the caller write transaction')
    rule = conn.execute('SELECT * FROM control_task_rules WHERE task_id=?', (task['task_id'],)).fetchone()
    if rule is None:
        return True
    _id(actor)
    at = _time(at)
    if type(next_fence) is not int or next_fence != task['fence'] + 1:
        raise DispatchError('policy reservation must target the next attempt fence')
    key = task['task_id']+':'+str(next_fence)
    if task['access'] != 'synthetic' or task['egress'] != 'none' or rule['fingerprint'] != task['fingerprint']:
        _event(conn,'policy:'+key,'permission_conflict',actor,at,task_id=task['task_id'],
               fence=next_fence,code='policy_task_binding_mismatch')
        return False
    if rule['approval_scope']:
        approval = conn.execute('SELECT * FROM control_approvals WHERE approval_id=?', (rule['approval_id'],)).fetchone()
        latest = conn.execute('''SELECT approval_id FROM control_approvals
            WHERE task_id=? AND fingerprint=? AND scope=? ORDER BY at DESC,rowid DESC LIMIT 1''',
            (task['task_id'],task['fingerprint'],rule['approval_scope'])).fetchone()
        if not approval or approval['task_id'] != task['task_id'] or approval['fingerprint'] != task['fingerprint'] or \
                approval['scope'] != rule['approval_scope'] or approval['decision'] != 'approved' or approval['expires_at'] <= at or \
                not latest or latest['approval_id'] != approval['approval_id']:
            _event(conn,'approval:'+key,'approval.requested',actor,at,task_id=task['task_id'],
                   fence=next_fence,code='missing_stale_or_denied_approval')
            return False
    if not rule['budget_id']:
        return True
    prior = conn.execute('SELECT * FROM control_reservations WHERE task_id=? AND fence=?',
                         (task['task_id'],next_fence)).fetchone()
    if prior:
        if (prior['budget_id'],prior['fingerprint'],prior['reserved_units']) != \
                (rule['budget_id'],task['fingerprint'],rule['reserve_units']):
            raise DispatchError('attempt reservation cannot be rebound')
        return prior['state'] in ('reserved','uncertain')
    budget = conn.execute('SELECT * FROM control_budgets WHERE budget_id=?', (rule['budget_id'],)).fetchone()
    if not budget or not budget['starts_at'] <= at < budget['ends_at']:
        _event(conn,'budget-period:'+key,'budget.exceeded',actor,at,task_id=task['task_id'],fence=next_fence,code='budget_period_inactive')
        return False
    changed = conn.execute('''UPDATE control_budgets SET reserved_units=reserved_units+?
        WHERE budget_id=? AND used_units+reserved_units+?<=limit_units''',
        (rule['reserve_units'],rule['budget_id'],rule['reserve_units'])).rowcount
    if not changed:
        _event(conn,'budget-limit:'+key,'budget.exceeded',actor,at,task_id=task['task_id'],fence=next_fence,code='insufficient_remaining_units')
        return False
    conn.execute('''INSERT INTO control_reservations(task_id,fence,budget_id,fingerprint,reserved_units,
        actual_units,state,evidence_ref,created_at,settled_at) VALUES(?,?,?,?,?,NULL,'reserved',NULL,?,NULL)''',
                 (task['task_id'],next_fence,rule['budget_id'],task['fingerprint'],rule['reserve_units'],at))
    _event(conn,'reserved:'+key,'budget.reserved',actor,at,task_id=task['task_id'],fence=next_fence)
    return True


class ControlPolicy:
    def __init__(self, store, *, artifacts=None, authority=None):
        if authority not in (None, 'route_task'):
            raise RouteRejected('invalid control policy authority')
        self.store, self.artifacts = store, artifacts
        self._authority = authority
        with closing(store._connect()) as conn:
            conn.execute('BEGIN')
            if ledger_mode(conn) == 'mixed':
                self._require_route_authority()
                self._validate_mixed_schema(conn)
                return
        with store._tx() as conn:
            initialize_schema(conn)

    def _require_route_authority(self):
        if self._authority != 'route_task' or self.store.route not in ROUTES:
            raise RouteRejected('mixed control policy requires explicit route_task authority and a bound route')

    @staticmethod
    def _validate_mixed_schema(conn):
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        required = {
            'tasks': 'task_id execution_route access egress fingerprint status attempts',
            'control_budgets': 'budget_id period unit limit_units reserved_units used_units starts_at ends_at',
            'control_task_rules': 'task_id fingerprint budget_id reserve_units approval_scope approval_id',
            'control_approvals': 'approval_id task_id fingerprint scope decision actor evidence_ref expires_at at',
            'control_reservations': 'task_id fence budget_id fingerprint reserved_units actual_units state '
                                    'evidence_ref created_at settled_at uncertain_at',
            'control_handoffs': 'handoff_id from_conv to_conv project_id scope scope_refs_json authorized_by '
                                'authorization_ref expires_at state receipt_ref evidence_refs_json created_at updated_at',
            'control_events': 'seq event_id event_key kind task_id fence actor at refs_json detail_code',
        }
        for table, columns in required.items():
            present = {row[1] for row in conn.execute(f'PRAGMA table_info({table})')}
            if table not in tables or not set(columns.split()) <= present:
                raise RouteRejected('mixed control policy requires an initialized control schema')

    def _require_isolated(self, operation):
        if self.store.routing_mode() == 'mixed':
            raise RouteRejected(f'{operation} is restricted to an isolated control ledger')

    @contextmanager
    def _task_tx(self, task_id):
        # The locked check handles activation after a policy was constructed.
        if self.store.routing_mode() == 'mixed':
            self._require_route_authority()
        with self.store._tx(execution=True) as conn:
            mixed = ledger_mode(conn) == 'mixed'
            if mixed:
                self._require_route_authority()
                task = conn.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
                self.store.require_task_route(task, conn=conn)
                if task['access'] != 'synthetic' or task['egress'] != 'none':
                    raise DispatchError('policy supports synthetic no-egress tasks only')
            yield conn, mixed

    def _artifact(self, ref):
        _hash(ref)
        if self.artifacts is None:
            raise DispatchError('readable synthetic artifact store required')
        value = json.loads(self.artifacts.get(ref))
        if not isinstance(value, dict) or value.get('synthetic') is not True:
            raise DispatchError('synthetic proof object required')
        return value

    def _task(self, conn, task_id):
        row = conn.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()
        if not row or row['access'] != 'synthetic' or row['egress'] != 'none':
            raise DispatchError('policy supports synthetic no-egress tasks only')
        return row

    def budget_upsert(self, budget_id, *, period, unit, limit_units, starts_at, ends_at):
        self._require_isolated('budget_upsert')
        for value in (budget_id,period,unit): _id(value)
        _units(limit_units)
        start,end = _time(starts_at),_time(ends_at)
        if end <= start: raise DispatchError('budget period must be positive')
        with self.store._tx() as conn:
            old = conn.execute('SELECT * FROM control_budgets WHERE budget_id=?',(budget_id,)).fetchone()
            if old and (old['period'],old['unit'],old['starts_at'],old['ends_at']) != (period,unit,start,end):
                raise DispatchError('budget period identity is immutable; use a new budget id')
            if old and old['used_units']+old['reserved_units'] > limit_units:
                raise DispatchError('limit cannot erase already used or held units')
            conn.execute('''INSERT INTO control_budgets VALUES(?,?,?,?,0,0,?,?)
                ON CONFLICT(budget_id) DO UPDATE SET limit_units=excluded.limit_units''',
                (budget_id,period,unit,limit_units,start,end))

    def bind_task(self, task_id, *, budget_id=None, reserve_units=0, approval_scope=None, approval_id=None):
        _units(reserve_units)
        if budget_id is None and reserve_units: raise DispatchError('reserve units require a budget')
        if approval_scope: _id(approval_scope)
        if approval_id and not approval_scope: raise DispatchError('approval id requires a scope')
        with self._task_tx(task_id) as (conn, mixed):
            task = self._task(conn,task_id)
            if task['status'] not in ('queued','waiting_device') or task['attempts']:
                raise DispatchError('bind policy before the first attempt only')
            if mixed:
                if budget_id is not None:
                    _id(budget_id)
                if approval_scope is not None:
                    _id(approval_scope)
                if budget_id is None and approval_scope is None:
                    raise DispatchError('mixed control rules require a budget or approval scope')
                previous = conn.execute('''SELECT budget_id,reserve_units,approval_scope,approval_id
                    FROM control_task_rules WHERE task_id=?''', (task_id,)).fetchone()
                if previous and tuple(previous)[:3] != (budget_id, reserve_units, approval_scope):
                    raise DispatchError('existing mixed control requirements are immutable')
                if approval_id is None and previous:
                    approval_id = previous['approval_id']
                if approval_id is not None:
                    _id(approval_id)
                    approval = conn.execute('''SELECT task_id,fingerprint,scope FROM control_approvals
                        WHERE approval_id=?''', (approval_id,)).fetchone()
                    if not approval or tuple(approval) != (task_id, task['fingerprint'], approval_scope):
                        raise DispatchError('approval must identify the owning task, fingerprint and scope')
            conn.execute('''INSERT INTO control_task_rules VALUES(?,?,?,?,?,?) ON CONFLICT(task_id)
                DO UPDATE SET fingerprint=excluded.fingerprint,budget_id=excluded.budget_id,
                reserve_units=excluded.reserve_units,approval_scope=excluded.approval_scope,approval_id=excluded.approval_id''',
                (task_id,task['fingerprint'],budget_id,reserve_units,approval_scope,approval_id))

    def approval_decide(self, task_id, *, scope, decision, actor, evidence_ref, expires_at):
        _id(scope); _id(actor)
        if decision not in ('approved','denied'): raise DispatchError('invalid approval decision')
        expiry = _time(expires_at)
        if expiry <= self.store._now(): raise DispatchError('approval already expired')
        proof = self._artifact(evidence_ref)
        with self._task_tx(task_id) as (conn, _):
            task = self._task(conn,task_id)
            expected = {'task_id':task_id,'fingerprint':task['fingerprint'],'scope':scope,'decision':decision,'actor':actor}
            if any(proof.get(key)!=value for key,value in expected.items()) or _time(proof.get('expires_at')) < expiry:
                raise DispatchError('approval proof must bind task, fixed input fingerprint, scope, actor and decision')
            approval_id = 'ap_'+uuid4().hex
            conn.execute('INSERT INTO control_approvals VALUES(?,?,?,?,?,?,?,?,?)',
                         (approval_id,task_id,task['fingerprint'],scope,decision,actor,evidence_ref,expiry,self.store._now()))
            conn.execute('UPDATE control_task_rules SET approval_id=? WHERE task_id=? AND approval_scope=?',
                         (approval_id,task_id,scope))
            _event(conn,approval_id,'approval.decided',actor,self.store._now(),task_id=task_id,refs=(evidence_ref,),code=decision)
        return approval_id

    def mark_uncertain(self, task_id, fence, *, actor):
        _id(actor)
        if type(fence) is not int or fence<1: raise DispatchError('positive attempt fence required')
        with self._task_tx(task_id) as (conn, _):
            row=conn.execute('SELECT state FROM control_reservations WHERE task_id=? AND fence=?',(task_id,fence)).fetchone()
            if not row:
                raise DispatchError('attempt has no reservation')
            if row['state']=='settled': raise DispatchError('settled usage cannot become uncertain')
            expire_reservation(conn,task_id,fence,actor,self.store._now())

    def settle(self, task_id, fence, *, actual_units, evidence_ref, actor):
        _id(actor); _units(actual_units)
        if type(fence) is not int or fence<1: raise DispatchError('positive attempt fence required')
        proof = self._artifact(evidence_ref)
        with self._task_tx(task_id) as (conn, _):
            row = conn.execute('SELECT * FROM control_reservations WHERE task_id=? AND fence=?',(task_id,fence)).fetchone()
            if not row: raise DispatchError('attempt has no reservation')
            expected = {'task_id':task_id,'fence':fence,'budget_id':row['budget_id'],
                        'fingerprint':row['fingerprint'],'actual_units':actual_units,'final_usage':True}
            if any(type(proof.get(key)) is not type(value) or proof.get(key)!=value for key,value in expected.items()):
                raise DispatchError('final usage proof must bind the reserved task and attempt')
            if row['state']=='settled':
                if row['actual_units']!=actual_units or row['evidence_ref']!=evidence_ref:
                    raise DispatchError('settled usage is immutable')
                return
            conn.execute('''UPDATE control_budgets SET reserved_units=reserved_units-?,used_units=used_units+?
                WHERE budget_id=?''',(row['reserved_units'],actual_units,row['budget_id']))
            conn.execute("UPDATE control_reservations SET state='settled',actual_units=?,evidence_ref=?,settled_at=? WHERE task_id=? AND fence=?",
                         (actual_units,evidence_ref,self.store._now(),task_id,fence))
            _event(conn,'settled:'+task_id+':'+str(fence),'budget.settled',actor,self.store._now(),task_id=task_id,fence=fence,refs=(evidence_ref,))
            budget=conn.execute('SELECT * FROM control_budgets WHERE budget_id=?',(row['budget_id'],)).fetchone()
            if budget['used_units']+budget['reserved_units']>budget['limit_units']:
                _event(conn,'overrun:'+task_id+':'+str(fence),'budget.exceeded',actor,self.store._now(),task_id=task_id,fence=fence,refs=(evidence_ref,),code='actual_usage_overrun')

    def settle_host_run(self, task_id, run_id, terminal_event_ref, actor):
        """Count a proven synthetic host run once, never convert token/currency units.

        terminal_event_ref is a synthetic wrapper pointing to the original raw
        host event. Proofs use that stable raw reference, independent of which
        observer fence produced a summary report.
        """
        self._require_isolated('settle_host_run')
        _id(actor); _id(run_id)
        wrapper=self._artifact(terminal_event_ref)
        if wrapper.get('schema')!='sa_dispatch.host_terminal.v1' or wrapper.get('task_id')!=task_id or wrapper.get('run_id')!=run_id:
            raise DispatchError('terminal wrapper must bind this task and run')
        event_ref=_hash(wrapper.get('event_ref'))
        event=json.loads(self.artifacts.get(event_ref))
        types={'run.completed':'completed','run.failed':'failed','run.cancelled':'cancelled','run.interrupted':'interrupted'}
        if not isinstance(event,dict) or event.get('type') not in types or event.get('run_id')!=run_id or \
                event.get('status')!=types[event['type']] or type(event.get('seq')) is not int or event['seq']<1:
            raise DispatchError('readable matching terminal run event required')
        with self.store._tx() as conn:
            task=self._task(conn,task_id)
            if task['kind']!='host.synthetic' or task['side_effect']!='none':
                raise DispatchError('automatic host usage only supports synthetic read-only host tasks')
            attempts=[dict(row) for row in conn.execute('''SELECT * FROM host_attempts
                WHERE task_id=? AND run_id=? AND endpoint=? AND session_id=? ORDER BY fence''',
                (task_id,run_id,wrapper.get('endpoint'),wrapper.get('session_id')))]
            reservations={row['fence']:dict(row) for row in conn.execute('''SELECT r.*,b.unit
                FROM control_reservations r JOIN control_budgets b USING(budget_id) WHERE task_id=?''',(task_id,))}
        if not attempts:
            raise DispatchError('terminal event has no bound host attempt')
        initiators=[row for row in attempts if row['adopted_fence'] is None]
        if len(initiators)!=1 or initiators[0]['fence']!=attempts[0]['fence']:
            raise DispatchError('host run origin cannot be established unambiguously')
        origin=initiators[0]['fence']; known={row['fence'] for row in attempts}
        result={'run_id':run_id,'origin_fence':origin,'unit':'host_run',
                'settled':[],'already_settled':[],'held':[]}
        for attempt in attempts:
            fence=attempt['fence']; reservation=reservations.get(fence)
            if reservation is None:
                continue
            reason=None
            if reservation['unit']!='host_run': reason='unit_not_host_run'
            elif fence!=origin and (attempt['adopted_fence'] not in known or attempt['adopted_fence']>=fence):
                reason='observer_origin_unproven'
            if reason:
                if reservation['state']=='reserved': self.mark_uncertain(task_id,fence,actor=actor)
                if reservation['state']!='settled': result['held'].append({'fence':fence,'reason':reason})
                continue
            actual=1 if fence==origin else 0
            if reservation['state']=='settled':
                result['already_settled'].append({'fence':fence,'actual_units':reservation['actual_units'],
                    'matches_host_count':reservation['actual_units']==actual})
                continue
            proof={'schema':'sa_dispatch.host_run_usage.v1','synthetic':True,'task_id':task_id,'fence':fence,
                   'budget_id':reservation['budget_id'],'fingerprint':reservation['fingerprint'],
                   'unit':'host_run','actual_units':actual,'final_usage':True,'endpoint':wrapper['endpoint'],
                   'session_id':wrapper['session_id'],'run_id':run_id,'terminal_event_ref':event_ref}
            from .canonical import canonical_json
            proof_ref=self.artifacts.put(canonical_json(proof))
            self.settle(task_id,fence,actual_units=actual,evidence_ref=proof_ref,actor=actor)
            result['settled'].append({'fence':fence,'actual_units':actual,'evidence_ref':proof_ref})
        # Reservations lacking an attempt/run linkage are deliberately untouched.
        result['unlinked_fences']=[fence for fence,row in reservations.items() if fence not in known and row['state']!='settled']
        return result

    def handoff_offer(self, handoff_id, *, from_conv, to_conv, project_id, scope, expires_at,
                      actor, scope_refs=(), authorization_ref=None):
        self._require_isolated('handoff_offer')
        for value in (handoff_id,from_conv,to_conv,project_id,actor): _id(value)
        if actor!=from_conv or scope not in ('read_status','instruct','takeover'):
            raise DispatchError('handoff sender and scope required')
        expiry = _time(expires_at)
        if expiry<=self.store._now(): raise DispatchError('handoff already expired')
        refs=list(scope_refs)
        for ref in refs: _hash(ref)
        authorized_by=None
        if scope in ('instruct','takeover') or authorization_ref:
            proof=self._artifact(authorization_ref)
            expected={'handoff_id':handoff_id,'from_conv':from_conv,'to_conv':to_conv,'project_id':project_id,'scope':scope,'scope_refs':refs}
            if any(proof.get(key)!=value for key,value in expected.items()) or _time(proof['expires_at'])<expiry:
                raise DispatchError('handoff authorization does not cover this exact scope and expiry')
            authorized_by=_id(proof.get('authorized_by'))
        with self.store._tx() as conn:
            conn.execute('INSERT INTO control_handoffs VALUES(?,?,?,?,?,?,?,?,?,\'offered\',NULL,\'[]\',?,?)',
                (handoff_id,from_conv,to_conv,project_id,scope,json.dumps(refs),authorized_by,authorization_ref,expiry,self.store._now(),self.store._now()))
            _event(conn,'handoff:'+handoff_id+':offered','handoff.offered',actor,self.store._now(),refs=(authorization_ref,) if authorization_ref else ())

    def _expire(self, conn):
        now=self.store._now()
        for row in conn.execute("SELECT * FROM control_handoffs WHERE state IN ('offered','accepted') AND expires_at<=?",(now,)).fetchall():
            conn.execute("UPDATE control_handoffs SET state='expired',updated_at=? WHERE handoff_id=?",(now,row['handoff_id']))
            _event(conn,'handoff:'+row['handoff_id']+':expired','handoff.expired','policy',now,code='authorization_expired')

    def handoff_transition(self, handoff_id, state, *, actor, receipt_ref=None):
        self._require_isolated('handoff_transition')
        _id(actor)
        if state not in ('accepted','completed','rejected'): raise DispatchError('invalid handoff transition')
        proof=self._artifact(receipt_ref) if state=='completed' else None
        denied=None
        with self.store._tx() as conn:
            self._expire(conn)
            row=conn.execute('SELECT * FROM control_handoffs WHERE handoff_id=?',(handoff_id,)).fetchone()
            if not row or actor!=row['to_conv']: denied='only the intended recipient may acknowledge a handoff'
            elif row['state']==state and (state!='completed' or row['receipt_ref']==receipt_ref): pass
            elif row['state'] not in ('offered','accepted') or (state=='completed' and row['state']!='accepted'):
                denied='handoff state or authorization has expired'
            else:
                refs=[]
                if state=='completed':
                    if proof.get('handoff_id')!=handoff_id or proof.get('recipient')!=actor or proof.get('decision')!='completed':
                        raise DispatchError('completion receipt must bind handoff and recipient')
                    refs=proof.get('evidence_refs')
                    if not isinstance(refs,list) or not refs: raise DispatchError('completion output evidence required')
                    for ref in refs:
                        _hash(ref); self.artifacts.get(ref)
                conn.execute('UPDATE control_handoffs SET state=?,receipt_ref=?,evidence_refs_json=?,updated_at=? WHERE handoff_id=?',
                             (state,receipt_ref,json.dumps(refs),self.store._now(),handoff_id))
                _event(conn,'handoff:'+handoff_id+':'+state,'handoff.'+state,actor,self.store._now(),refs=(receipt_ref,) if receipt_ref else ())
        if denied: raise DispatchError(denied)

    def status(self, *, uncertain_after_hours=24):
        if type(uncertain_after_hours) is not int or not 0<=uncertain_after_hours<=8760:
            raise DispatchError('uncertainty threshold must be integer hours between 0 and 8760')
        cutoff=timestamp(self.store.clock.now()-timedelta(hours=uncertain_after_hours))
        with closing(self.store._connect()) as conn:
            conn.execute('BEGIN')
            if ledger_mode(conn) == 'mixed':
                self._require_route_authority()
                return self._mixed_status(conn, cutoff, uncertain_after_hours)
        with self.store._tx() as conn:
            self._expire(conn)
            result={name:[dict(row) for row in conn.execute('SELECT * FROM control_'+name)]
                    for name in ('budgets','task_rules','approvals','reservations','handoffs','events')}
            result['uncertain_after_hours']=uncertain_after_hours
            result['uncertain_over_threshold']=[dict(row) for row in conn.execute('''
                SELECT r.*,b.unit,b.period FROM control_reservations r JOIN control_budgets b USING(budget_id)
                WHERE r.state='uncertain' AND r.uncertain_at<? ORDER BY r.uncertain_at,r.task_id,r.fence''',(cutoff,))]
            return result

    def _mixed_status(self, conn, cutoff, uncertain_after_hours):
        route = self.store.route
        owned = ("SELECT task_id FROM tasks WHERE execution_route=? "
                 "AND access='synthetic' AND egress='none'")
        result = {
            name: [dict(row) for row in conn.execute(
                f'SELECT * FROM control_{name} WHERE task_id IN ({owned})', (route,))]
            for name in ('task_rules', 'approvals', 'reservations', 'events')
        }
        result['budgets'] = [dict(row) for row in conn.execute(f'''
            SELECT * FROM control_budgets WHERE budget_id IN (
                SELECT budget_id FROM control_task_rules WHERE task_id IN ({owned})
                UNION
                SELECT budget_id FROM control_reservations WHERE task_id IN ({owned}))''',
            (route, route))]
        result['uncertain_over_threshold'] = [dict(row) for row in conn.execute(f'''
            SELECT r.*,b.unit,b.period FROM control_reservations r JOIN control_budgets b USING(budget_id)
            WHERE r.task_id IN ({owned}) AND r.state='uncertain' AND r.uncertain_at<?
            ORDER BY r.uncertain_at,r.task_id,r.fence''', (route, cutoff))]
        result.update(scope='route_task', execution_route=route, budget_totals_scope='ledger',
                      handoffs=[], uncertain_after_hours=uncertain_after_hours)
        return result
