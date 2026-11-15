"""Transactional local ledger. No component databases or network access."""
import json
import sqlite3
import re
from contextlib import contextmanager, closing
from dataclasses import asdict, replace
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from .clock import Clock, timestamp
from .canonical import canonical_json, sha256_ref
from .models import (DispatchError, DuplicateInflight, Evidence, FenceRejected,
                     IdempotencyConflict, InvalidTransition, Lease, Outcome,
                     Ref, SweepReport, TaskRow, TaskSpec)
from .states import TERMINAL, validate_transition

def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)

class SqliteStore:
    TABLES = ('tasks', 'leases', 'events', 'evidence', 'late_results', 'devices')

    def __init__(self, path, *, clock=None, lease_ttl_seconds, kinds=None, component_config=None):
        if type(lease_ttl_seconds) is not int or lease_ttl_seconds <= 0:
            raise DispatchError('positive explicit lease TTL required')
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock or Clock()
        self.lease_ttl_seconds = lease_ttl_seconds
        self.kinds = kinds
        self.component_config = component_config
        with closing(self._connect()) as conn:
            conn.executescript(Path(__file__).with_name('schema.sql').read_text(encoding='utf-8'))
            conn.execute('BEGIN IMMEDIATE')
            from .migrations import apply_m0_schema
            apply_m0_schema(conn, self._now())
            conn.commit()

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys=ON')
        return conn

    @contextmanager
    def _tx(self):
        conn = self._connect()
        try:
            conn.execute('BEGIN IMMEDIATE')
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _now(self): return timestamp(self.clock.now())

    def _task(self, row):
        if row is None: raise DispatchError('task not found')
        data = dict(row)
        for key in ('params', 'input_refs', 'required_caps', 'scope', 'result'):
            value = data.get(key + '_json')
            data[key] = json.loads(value) if value is not None else None
        return TaskRow(data)

    def get_task(self, task_id):
        with closing(self._connect()) as c:
            return self._task(c.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone())

    def list_tasks(self):
        with closing(self._connect()) as c:
            return [self._task(r) for r in c.execute('SELECT * FROM tasks ORDER BY created_at,task_id')]

    def get_lease(self, lease_id):
        with closing(self._connect()) as c:
            row = c.execute('SELECT * FROM leases WHERE lease_id=?', (lease_id,)).fetchone()
            return TaskRow(dict(row)) if row else None

    def list_leases(self, task_id=None):
        with closing(self._connect()) as c:
            rows = c.execute('SELECT * FROM leases' + (' WHERE task_id=?' if task_id else ''), (task_id,) if task_id else ())
            return [TaskRow(dict(r)) for r in rows]

    def _event(self, c, task, old, new, reason, actor):
        c.execute('INSERT INTO events(task_id,from_status,to_status,fence,reason,actor,at) VALUES(?,?,?,?,?,?,?)',
                  (task['task_id'], old, new, task['fence'], reason, actor, self._now()))

    def _transition(self, c, task, new, reason, actor):
        validate_transition(task['status'], new)
        c.execute('UPDATE tasks SET status=?,updated_at=? WHERE task_id=?', (new, self._now(), task['task_id']))
        self._event(c, task, task['status'], new, reason, actor)

    def submit(self, spec, *, side_effect=None, scope=None):
        if isinstance(spec, dict): spec = TaskSpec.from_dict(spec)
        if not spec.idem_key or not spec.kind or type(spec.kind_version) is not int or spec.kind_version < 1:
            raise DispatchError('invalid task identity')
        if type(spec.max_attempts) is not int or spec.max_attempts < 1:
            raise DispatchError('invalid attempt limit')
        if spec.access not in ('synthetic','exportable','local_only') or spec.egress not in ('none','summary','derived'):
            raise DispatchError('invalid access/egress')
        if not isinstance(spec.params, dict) or any(not isinstance(v,str) or not v for v in spec.required_caps):
            raise DispatchError('invalid params/capabilities')
        if self.kinds is not None:
            kind = self.kinds.get(spec.kind)
            if kind is None or kind.version != spec.kind_version:
                err = DispatchError('unregistered kind'); err.code = 'E_KIND_UNKNOWN'; raise err
            if not kind.enabled:
                err = DispatchError('disabled kind'); err.code = 'E_KIND_DISABLED'; raise err
            spec = replace(spec, params=kind.validate(spec.params))
            side_effect, scope = kind.side_effect, kind.scope(spec.params)
        side_effect = side_effect or 'none'
        scope = {} if scope is None else scope
        if side_effect not in ('none','idempotent_write','external'): raise DispatchError('invalid effect')
        identity = asdict(spec)
        identity.pop('idem_key')
        identity['required_caps'] = sorted(set(spec.required_caps))
        identity['input_refs'] = sorted(identity['input_refs'], key=lambda r: (r['owner'],r['kind'],r['id'],r['version'],r['hash']))
        for ref in identity['input_refs']:
            if any(not isinstance(v,str) or not v for v in ref.values()) or ref['version'] == 'latest':
                raise DispatchError('fixed reference required')
        identity.update(side_effect=side_effect, scope=scope)
        try:
            fingerprint = sha256_ref(canonical_json(identity))
        except (TypeError, ValueError) as exc:
            raise DispatchError('unsupported task identity value') from exc
        with self._tx() as c:
            existing = c.execute('SELECT * FROM tasks WHERE idem_key=?', (spec.idem_key,)).fetchone()
            if existing:
                if existing['fingerprint'] != fingerprint: raise IdempotencyConflict('key already has different semantics')
                return self._task(existing), True
            if side_effect != 'none' and c.execute("SELECT 1 FROM tasks WHERE fingerprint=? AND status NOT IN('succeeded','no_new','failed','cancelled')", (fingerprint,)).fetchone():
                raise DuplicateInflight('identical write is already in flight')
            task_id, now = 't_' + uuid4().hex, self._now()
            row = dict(task_id=task_id, idem_key=spec.idem_key, fingerprint=fingerprint,
                       kind=spec.kind, kind_version=spec.kind_version, track=spec.track,
                       params_json=_json(spec.params), input_refs_json=_json(identity['input_refs']),
                       required_caps_json=_json(identity['required_caps']),scope_json=_json(scope),
                       side_effect=side_effect,access=spec.access,egress=spec.egress,status='queued',
                       fence=0,attempts=0,max_attempts=spec.max_attempts,parent_task_id=spec.parent_task_id,
                       result_json=None,created_at=now,updated_at=now)
            c.execute(f"INSERT INTO tasks({','.join(row)}) VALUES({','.join('?' for _ in row)})", tuple(row.values()))
            self._event(c,row,None,'queued','submitted','submit')
            return self._task(row), False

    def register_device(self, device_id, caps):
        if not isinstance(device_id,str) or not device_id or any(not isinstance(v,str) or not v for v in caps):
            raise DispatchError('invalid device')
        with self._tx() as c:
            c.execute('INSERT INTO devices(device_id,caps_json,last_seen_at) VALUES(?,?,?) ON CONFLICT(device_id) DO UPDATE SET caps_json=excluded.caps_json,last_seen_at=excluded.last_seen_at',
                      (device_id,_json(sorted(set(caps))),self._now()))
        return self.sweep()

    device_register = register_device

    def claim(self, device_id, caps=None, *, synthetic_handoff=False, kinds=None):
        with self._tx() as c:
            dev = c.execute('SELECT * FROM devices WHERE device_id=?',(device_id,)).fetchone()
            if not dev: raise DispatchError('device must be registered')
            allowed = set(json.loads(dev['caps_json']))
            if caps is not None: allowed &= set(caps)
            sql = "SELECT * FROM tasks WHERE status='queued'"
            if synthetic_handoff:
                sql += " AND access='synthetic' AND EXISTS (SELECT 1 FROM transport_handoffs h WHERE h.task_id=tasks.task_id)"
            for row in c.execute(sql + " ORDER BY created_at,task_id").fetchall():
                if kinds is not None and row['kind'] not in kinds: continue
                if not set(json.loads(row['required_caps_json'])) <= allowed: continue
                if row['attempts'] >= row['max_attempts']: continue
                from .control_policy import before_claim
                if not before_claim(c, row, row['fence']+1, device_id, self._now()): continue
                lease = Lease('l_'+uuid4().hex,row['task_id'],row['fence']+1,device_id,
                              timestamp(self.clock.now()+timedelta(seconds=self.lease_ttl_seconds)))
                c.execute("UPDATE tasks SET fence=fence+1,attempts=attempts+1 WHERE task_id=? AND status='queued'",(row['task_id'],))
                task = dict(row); task['fence'] = lease.fence
                self._transition(c,task,'leased','claimed',device_id)
                c.execute("INSERT INTO leases(lease_id,task_id,fence,device_id,granted_at,expires_at,state) VALUES(?,?,?,?,?,?,'active')",
                          (lease.lease_id,lease.task_id,lease.fence,device_id,self._now(),lease.expires_at))
                return lease
        return None

    def _active(self,c,lease_id,fence,device_id):
        lease = c.execute('SELECT * FROM leases WHERE lease_id=?',(lease_id,)).fetchone()
        task = c.execute('SELECT * FROM tasks WHERE task_id=?',(lease['task_id'],)).fetchone() if lease else None
        valid = bool(lease and task and lease['fence']==fence and lease['device_id']==device_id and
                     lease['state']=='active' and lease['expires_at']>self._now() and task['fence']==fence and task['status']=='leased')
        return lease, task, valid

    def renew(self,lease_id,fence,device_id):
        with self._tx() as c:
            lease,task,valid = self._active(c,lease_id,fence,device_id)
            if not valid: raise FenceRejected('lease is not current, owned and unexpired')
            expires=timestamp(self.clock.now()+timedelta(seconds=self.lease_ttl_seconds))
            c.execute('UPDATE leases SET expires_at=? WHERE lease_id=?',(expires,lease_id))
            return Lease(lease_id,task['task_id'],fence,device_id,expires)

    def _outcome(self,outcome):
        if isinstance(outcome,dict): outcome=Outcome.from_dict(outcome)
        if outcome.status not in ('succeeded','no_new','failed'): raise InvalidTransition('invalid completion status')
        return outcome, _json(asdict(outcome))

    def _record_evidence(self,c,task,outcome):
        for ev in outcome.evidence:
            if ev.access not in ('synthetic','exportable','local_only'): raise DispatchError('invalid evidence access')
            c.execute('INSERT OR IGNORE INTO evidence VALUES(?,?,?,?,?,?,?,?,?)',
                      ('e_'+uuid4().hex,task['task_id'],task['fence'],ev.kind,_json(asdict(ev.ref)) if ev.ref else None,
                       ev.content_hash,ev.artifact_hash,ev.access,self._now()))

    def complete(self,lease_id,fence,outcome,device_id):
        outcome, encoded = self._outcome(outcome)
        rejected=False
        with self._tx() as c:
            lease,task,valid=self._active(c,lease_id,fence,device_id)
            if lease and lease['state']=='completed' and lease['fence']==fence and lease['device_id']==device_id and lease['completion_json']==encoded:
                return
            if not valid:
                if task:
                    c.execute('INSERT INTO late_results VALUES(?,?,?,?,?,?)',
                              ('late_'+uuid4().hex,task['task_id'],fence,encoded,self._now(),'ignored'))
                rejected=True
            else:
                if outcome.status=='no_new':
                    kind=self.kinds.get(task['kind']) if self.kinds else None
                    if not getattr(kind,'allow_no_new',False) or not any(e.kind=='request_log' for e in outcome.evidence):
                        raise DispatchError('no_new requires explicit kind support and request evidence')
                self._record_evidence(c,task,outcome)
                self._transition(c,task,outcome.status,'completed',device_id)
                c.execute('UPDATE tasks SET result_json=? WHERE task_id=?',(_json(outcome.result),task['task_id']))
                c.execute("UPDATE leases SET state='completed',completion_json=? WHERE lease_id=?",(encoded,lease_id))
        if rejected: raise FenceRejected('completion is late or not owned')

    def mark_unknown(self,lease_id,fence,reason,device_id):
        with self._tx() as c:
            lease,task,valid=self._active(c,lease_id,fence,device_id)
            if not valid: raise FenceRejected('lease is not current, owned and unexpired')
            self._transition(c,task,'unknown',reason,device_id)
            c.execute("UPDATE leases SET state='unknown' WHERE lease_id=?",(lease_id,))
            from .control_policy import expire_reservation
            expire_reservation(c,task['task_id'],fence,device_id,self._now())

    def sweep(self, *, worker_ids=None):
        counts=asdict(SweepReport())
        with self._tx() as c:
            rows=c.execute("SELECT t.*,l.lease_id FROM tasks t JOIN leases l ON t.task_id=l.task_id WHERE t.status='leased' AND l.state='active' AND l.expires_at<=?",(self._now(),)).fetchall()
            for task in rows:
                status='unknown' if task['side_effect']!='none' else ('queued' if task['attempts']<task['max_attempts'] else 'failed')
                self._transition(c,task,status,'lease expired','sweep')
                c.execute("UPDATE leases SET state='expired' WHERE lease_id=?",(task['lease_id'],))
                from .control_policy import expire_reservation
                expire_reservation(c,task['task_id'],task['fence'],'sweep',self._now())
                counts['expired']+=1
                counts['requeued' if status=='queued' else status]+=1
            devices=[set(json.loads(r['caps_json'])) for r in c.execute('SELECT device_id,caps_json FROM devices')
                     if worker_ids is None or r['device_id'] in worker_ids]
            for task in c.execute("SELECT * FROM tasks WHERE status IN('queued','waiting_device')").fetchall():
                capable=any(set(json.loads(task['required_caps_json']))<=caps for caps in devices)
                status='queued' if capable else 'waiting_device'
                if status!=task['status']:
                    self._transition(c,task,status,'device eligibility','sweep'); counts[status]+=1
        return SweepReport(**counts)

    def register_capability(self, component, capability, *, lookup_by_write_key=False,
                            idempotency_proof='none', proof_ref=None, artifacts=None):
        """Register operator-reviewed capability evidence; no component is invoked here."""
        if not component or not capability or type(lookup_by_write_key) is not bool:
            raise DispatchError('component, versioned capability and lookup flag required')
        if idempotency_proof not in ('none', 'claimed', 'verified'):
            raise DispatchError('invalid idempotency proof state')
        implementation = None
        if idempotency_proof == 'verified':
            if artifacts is None or not proof_ref:
                raise DispatchError('verified capability requires a readable proof artifact')
            proof = json.loads(artifacts.get(proof_ref))
            if not isinstance(proof, dict):
                raise DispatchError('proof must be an object')
            from .component_proof import component_fingerprint
            if self.component_config is None or not self.component_config.synthetic:
                raise DispatchError('current synthetic component configuration required')
            implementation = component_fingerprint(self.component_config, component)
            required = {'schema': 'sa_dispatch.idempotency_proof.v1', 'component': component,
                        'capability': capability, 'same_write_key': True, 'calls': 2,
                        'records_after_first': 1, 'records_after_second': 1,
                        'component_fingerprint': implementation,
                        'generated_by': 'sa_dispatch.prove-idempotency.v1', 'scope': 'synthetic_only'}
            if any(type(proof.get(key)) is not type(value) or proof.get(key) != value
                   for key, value in required.items()):
                raise DispatchError('proof does not verify this versioned capability')
            first, second = proof.get('first_result_hash'), proof.get('second_result_hash')
            if not isinstance(first, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', first) or first != second:
                raise DispatchError('proof results do not match')
            for field in ('first_result_hash', 'second_result_hash', 'state_after_first_ref', 'state_after_second_ref'):
                artifacts.get(proof.get(field))
            if proof['state_after_first_ref'] != proof['state_after_second_ref']:
                raise DispatchError('proof state snapshots differ')
        with self._tx() as c:
            c.execute('INSERT INTO component_capabilities '
                      '(component,capability,lookup_by_write_key,idempotency_proof,proof_ref,updated_at,component_fingerprint) '
                      'VALUES(?,?,?,?,?,?,?) '
                      'ON CONFLICT(component,capability) DO UPDATE SET '
                      'lookup_by_write_key=excluded.lookup_by_write_key, '
                      'idempotency_proof=excluded.idempotency_proof, proof_ref=excluded.proof_ref, '
                      'updated_at=excluded.updated_at, component_fingerprint=excluded.component_fingerprint',
                      (component, capability, int(lookup_by_write_key), idempotency_proof, proof_ref, self._now(),implementation))

    def reconciliation_history(self, task_id):
        with closing(self._connect()) as c:
            return [dict(row) for row in c.execute(
                'SELECT * FROM reconciliations WHERE task_id=? ORDER BY at,rowid', (task_id,))]

    def resolve_unknown(self,task_id,verdict,actor,*,outcome=None,manual=False,
                        component=None,capability=None,evidence_ref=None,expected_fence=None,
                        artifacts=None):
        if not actor: raise DispatchError('actor required')
        with self._tx() as c:
            task=c.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()
            if not task or task['status']!='unknown': raise InvalidTransition('task is not unknown')
            if verdict != 'still_unknown' and (type(expected_fence) is not int or artifacts is None):
                raise DispatchError('state-changing reconciliation requires attempt fence and artifacts')
            if expected_fence is not None and task['fence'] != expected_fence:
                raise FenceRejected('reconciliation belongs to an older attempt')
            if evidence_ref is not None and (not isinstance(evidence_ref, str) or
                    not re.fullmatch(r'sha256:[0-9a-f]{64}', evidence_ref)):
                raise DispatchError('hashed reconciliation evidence required')
            method, result = ('human' if manual else 'lookup'), 'error'
            if verdict=='still_unknown':
                self._event(c,task,'unknown','unknown',verdict,actor)
            elif verdict in ('confirmed_done','done'):
                if outcome is None: raise DispatchError('confirmed result required')
                outcome,_=self._outcome(outcome)
                if outcome.status!='succeeded' or not outcome.evidence: raise DispatchError('confirmed completion evidence required')
                if not evidence_ref:
                    raise DispatchError('bound completion receipt required')
                receipt = json.loads(artifacts.get(evidence_ref))
                expected = {'schema': 'sa_dispatch.reconciliation.v1', 'task_id': task_id,
                            'fence': task['fence'], 'idem_key': task['idem_key'],
                            'outcome_hash': sha256_ref(_json(asdict(outcome)).encode('utf-8'))}
                if not isinstance(receipt, dict) or any(receipt.get(key) != value for key,value in expected.items()):
                    raise DispatchError('completion receipt does not bind to task, attempt and outcome')
                if manual:
                    if receipt.get('decision') != 'succeeded':
                        raise DispatchError('manual receipt requires a succeeded decision')
                else:
                    kind = self.kinds.get(task['kind']) if self.kinds else None
                    if kind is None or kind.version != task['kind_version'] or receipt.get('component') != kind.component:
                        raise DispatchError('lookup receipt requires the registered task component')
                for item in outcome.evidence:
                    artifacts.get(item.artifact_hash or item.content_hash)
                self._record_evidence(c,task,outcome)
                self._record_evidence(c, task, Outcome(evidence=(Evidence(
                    'reconciliation_receipt', evidence_ref, task['access'], artifact_hash=evidence_ref),)))
                self._transition(c,task,'succeeded',verdict,actor)
                c.execute('UPDATE tasks SET result_json=? WHERE task_id=?',(_json(outcome.result),task_id))
                result = 'decided' if manual else 'found'
            elif verdict in ('confirmed_not_done','not_done'):
                if task['side_effect']=='external':
                    raise DispatchError('external outcome requires completion or evidence-backed failure resolution')
                if task['side_effect']=='idempotent_write':
                    kind = self.kinds.get(task['kind']) if self.kinds else None
                    if kind is None or kind.version != task['kind_version']:
                        raise DispatchError('registered task kind required for idempotent retry')
                    expected = (kind.component, task['kind'] + '@' + str(task['kind_version']))
                    if not component or not capability or (component, capability) != expected:
                        raise DispatchError('capability must bind to the task kind and version')
                    proof = c.execute('SELECT * FROM component_capabilities WHERE component=? AND capability=?',
                                      (component, capability)).fetchone()
                    if not proof or proof['idempotency_proof'] != 'verified':
                        raise DispatchError('idempotent retry requires verified capability proof')
                    if artifacts is None:
                        raise DispatchError('retry requires proof artifact verification')
                    from .component_proof import component_fingerprint
                    if self.component_config is None or not self.component_config.synthetic or task['access'] != 'synthetic':
                        raise DispatchError('verified retry currently supports synthetic tasks only')
                    if proof['component_fingerprint'] != component_fingerprint(self.component_config, component):
                        raise DispatchError('capability proof belongs to a different component implementation')
                    proof_body = json.loads(artifacts.get(proof['proof_ref']))
                    for field in ('first_result_hash', 'second_result_hash', 'state_after_first_ref', 'state_after_second_ref'):
                        artifacts.get(proof_body.get(field))
                    evidence_ref = proof['proof_ref']
                    self._record_evidence(c, task, Outcome(evidence=(Evidence(
                        'idempotency_proof', evidence_ref, task['access'], artifact_hash=evidence_ref),)))
                method, result = 'proven_rerun', 'decided'
                self._transition(c,task,'queued' if task['attempts']<task['max_attempts'] else 'failed',verdict,actor)
            elif verdict == 'confirmed_failed':
                if not manual or not evidence_ref:
                    raise DispatchError('manual failure requires actor and hashed decision evidence')
                if artifacts is None:
                    raise DispatchError('manual failure requires a readable decision artifact')
                decision = json.loads(artifacts.get(evidence_ref))
                if not isinstance(decision, dict) or decision.get('task_id') != task_id or \
                        decision.get('fence') != task['fence'] or decision.get('decision') != 'failed':
                    raise DispatchError('decision must bind to this task and attempt')
                self._record_evidence(c, task, Outcome(evidence=(Evidence(
                    'manual_resolution', evidence_ref, task['access'], artifact_hash=evidence_ref),)))
                self._transition(c,task,'failed',verdict,actor)
                result = 'decided'
            else: raise DispatchError('invalid resolution')
            c.execute('INSERT INTO reconciliations VALUES(?,?,?,?,?,?,?,?)',
                      ('rec_' + uuid4().hex, task_id, task['fence'], method, result,
                       evidence_ref, actor, self._now()))

    def cancel(self,task_id,actor):
        with self._tx() as c:
            task=c.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()
            if not task or task['status'] not in ('queued','waiting_device'): raise InvalidTransition('cannot cancel task')
            self._transition(c,task,'cancelled','cancelled',actor)

    def events(self,task_id):
        with closing(self._connect()) as c:
            return [dict(r) for r in c.execute('SELECT * FROM events WHERE task_id=? ORDER BY seq',(task_id,))]

    def late_results(self,task_id):
        with closing(self._connect()) as c:
            return [dict(r) for r in c.execute('SELECT * FROM late_results WHERE task_id=?',(task_id,))]

    def snapshot(self):
        from .bundle import snapshot_records
        with self._tx() as c:
            return snapshot_records(c)

    def import_snapshot(self,snapshot):
        from .bundle import import_snapshot_records
        with self._tx() as c:
            return import_snapshot_records(c,snapshot)
