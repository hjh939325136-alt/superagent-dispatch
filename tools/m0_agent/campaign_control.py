"""Register and inspect immutable, fixed-window producer/reviewer campaigns."""

from contextlib import closing, contextmanager
from dataclasses import asdict
from datetime import datetime
import json
from uuid import uuid4

from sa_dispatch.campaign_validation import (
    CAPABILITY, DEVICES, KIND, OPERATOR, ROUTE, SCOPE, approval, budget_fields, budget_id,
    campaign_records, context, current_slot, handoff_id, offer, receipt, ref_for, review_text,
    schedule_id, schedule_template, slot_end, slot_start, task_spec, usage_receipt, validate_campaign_records,
    validate_config, validate_result,
)
from sa_dispatch.canonical import canonical_json, sha256_ref
from sa_dispatch.clock import Clock
from sa_dispatch.control_policy import (
    _event, _insert_handoff, _write_budget, _write_handoff_state, expire_reservation,
)
from sa_dispatch.external_migration import verify_schema
from sa_dispatch.models import DispatchError, Evidence, FenceRejected, IdempotencyConflict
from sa_dispatch.routing import RouteRejected, ledger_mode
from sa_dispatch.store_sqlite import SqliteStore

from .common_worker import LEASE_TTL_SECONDS, UNKNOWN_ERRORS, _CommonAgentStore, ledger_identity
from .deepseek_client import MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES
from .worker import AgentKind


def _encoded(value):
    return canonical_json(value).decode('utf-8')


class _CampaignStore(SqliteStore):
    _allowed_routes = (ROUTE,)
    _routed_evidence_access = 'local_only'
    _put = _CommonAgentStore._put
    _read = _CommonAgentStore._read

    def __init__(self, ledger, artifacts, config, *, clock=None):
        self.config = validate_config(config)
        self.artifacts = artifacts
        self.source_clock = clock or Clock()
        super().__init__(ledger, clock=self.source_clock, lease_ttl_seconds=LEASE_TTL_SECONDS,
                         kinds={KIND: AgentKind()}, route=ROUTE)
        with closing(self._connect()) as conn, conn:
            conn.execute('BEGIN')
            self._check_ledger(conn)
            if verify_schema(conn) != '0013':
                raise RouteRejected('agent campaign requires the installed 0013 schema')

    def _check_ledger(self, conn):
        if (ledger_mode(conn) != 'mixed' or ledger_identity(self.path) != self.config['ledger_identity']
                or conn.execute('SELECT MAX(version) FROM schema_migrations').fetchone()[0] != '0013'):
            raise RouteRejected('agent campaign ledger identity or schema version differs')

    @contextmanager
    def _tx(self, *, execution=False):
        with super()._tx(execution=execution) as conn:
            self._check_ledger(conn)
            # Record one lock-protected instant for all rows written by this transaction.
            now = self.source_clock.now()
            self.clock = _TransactionClock(now)
            try:
                yield conn
            finally:
                self.clock = self.source_clock

    def _enabled_kind(self, name, version):
        return name == KIND and version == 1

    def _requires_control(self, name):
        return name == KIND

    def _validate_submission_route(self, spec):
        expected = (task_spec(self.config, role, slot) for role in DEVICES
                    for slot in range(1, self.config['slots'] + 1))
        if spec not in expected:
            raise RouteRejected('agent campaign task must match a fixed authorization slot')

    def _read_text(self, ref, limit):
        try:
            text = self._read(ref, limit).decode('utf-8')
        except (OSError, UnicodeError, ValueError):
            raise DispatchError('agent campaign text is missing or invalid') from None
        if not text.strip():
            raise DispatchError('agent campaign text is empty')
        return text

    def _proof(self, value):
        return self._put(canonical_json(value))

    def _verify_proof(self, ref, value):
        expected = canonical_json(value)
        if ref != sha256_ref(expected) or self._read(ref, len(expected)) != expected:
            raise DispatchError('agent campaign proof does not match its authorized content')

    def _prompt(self, role):
        return self._read_text(self.config[role + '_prompt_ref']['hash'], MAX_REQUEST_BYTES - 4096)

    @staticmethod
    def _bound_text(text):
        if len(json.dumps(text, ensure_ascii=False).encode('utf-8')) > MAX_REQUEST_BYTES - 1024:
            raise DispatchError('agent campaign text exceeds the request bounds')
        return text

    def _load(self, conn):
        records = campaign_records(conn)
        owned = validate_campaign_records(records)
        schedules = {row['id']: row for row in records['schedules']}
        found = {}
        for role in DEVICES:
            sid = schedule_id(self.config, role)
            row = schedules.get(sid)
            if row is None:
                continue
            if sid not in owned['schedules'] or row['task_template'] != _encoded(schedule_template(self.config, role)):
                raise IdempotencyConflict('agent campaign authorization is immutable')
            found[role] = row
        return records, found

    def _task_for_slot(self, conn, role, slot):
        return conn.execute('SELECT * FROM tasks WHERE idem_key=?', (task_spec(self.config, role, slot).idem_key,)).fetchone()

    def _handoff(self, conn, slot):
        return conn.execute('SELECT * FROM control_handoffs WHERE handoff_id=?',
                            (handoff_id(self.config, slot),)).fetchone()

    def _paused(self, conn):
        return conn.execute('SELECT 1 FROM schedule_events WHERE schedule_id=? AND kind=?',
                            (schedule_id(self.config, 'producer'), 'campaign.paused')).fetchone() is not None

    def _advance(self, conn, now):
        conn.execute('UPDATE schedules SET last_tick_at=? WHERE id IN (?,?)',
                     (now, *(schedule_id(self.config, role) for role in DEVICES)))

    def _register(self):
        with self._tx(execution=True) as conn:
            _, schedules = self._load(conn)
            if schedules:
                return
            config, at = self.config, self._now()
            if at >= config['expires_at']:
                raise DispatchError('agent campaign authorization has expired')
            if conn.execute('SELECT 1 FROM control_budgets WHERE budget_id=?', (budget_id(config),)).fetchone():
                raise IdempotencyConflict('agent campaign authorization_id is already registered')
            for role in DEVICES:
                self._bound_text(self._prompt(role))
            self._proof(config)
            _write_budget(conn, budget_fields(config), None)
            for role, device in DEVICES.items():
                existing = conn.execute('SELECT * FROM devices WHERE device_id=?', (device,)).fetchone()
                if existing is None:
                    self._register_route_identity(conn, device)
                    conn.execute('INSERT INTO devices(device_id,caps_json,last_seen_at,execution_route) VALUES(?,?,?,?)',
                                 (device, json.dumps([CAPABILITY]), at, ROUTE))
                elif existing['execution_route'] != ROUTE or json.loads(existing['caps_json']) != [CAPABILITY]:
                    raise RouteRejected('agent campaign device identity has changed')
                conn.execute('''INSERT INTO schedules(id,interval_seconds,task_template,next_fire_at,enabled,
                    version,catchup,max_catchup,max_slots,last_tick_at,execution_route)
                    VALUES(?,?,?,?,1,1,'latest',1,?,?,?)''',
                    (schedule_id(config, role), config['interval_seconds'], _encoded(schedule_template(config, role)),
                     config['starts_at'], config['slots'], at, ROUTE))

    def _stop(self):
        with self._tx(execution=True) as conn:
            _, schedules = self._load(conn)
            if not schedules:
                raise RouteRejected('agent campaign must be registered before it can be paused')
            if self._paused(conn):
                return
            # The stop is effective immediately even if the wall clock moved backwards.
            now = max(self._now(), schedules['producer']['last_tick_at'])
            for role in DEVICES:
                sid = schedule_id(self.config, role)
                conn.execute('UPDATE schedules SET enabled=0,last_tick_at=? WHERE id=?', (now, sid))
                conn.execute('''INSERT INTO schedule_events(schedule_id,version,kind,at,detail_code)
                    VALUES(?,1,'campaign.paused',?,'operator_stop')''', (sid, now))

    def _sweep(self, conn, now):
        config = self.config
        changed = False
        for role, device in DEVICES.items():
            rows = conn.execute('''SELECT t.*, l.lease_id FROM tasks t
                JOIN schedule_firings f ON f.task_id=t.task_id
                JOIN leases l ON l.task_id=t.task_id AND l.fence=t.fence
                WHERE f.schedule_id=? AND t.status='leased' AND l.state='active'
                AND l.device_id=? AND l.expires_at<=?''', (schedule_id(config, role), device, now)).fetchall()
            for task in rows:
                self._transition(conn, task, 'unknown', 'lease expired', 'sweep')
                conn.execute("UPDATE leases SET state='expired' WHERE lease_id=?", (task['lease_id'],))
                if not expire_reservation(conn, task['task_id'], 1, 'sweep', now):
                    raise DispatchError('agent campaign expired lease lost its reservation')
                changed = True
        for slot in range(1, config['slots'] + 1):
            if slot_end(config, slot) > now:
                break
            handoff = self._handoff(conn, slot)
            if handoff and handoff['state'] in ('offered', 'accepted'):
                _write_handoff_state(conn, handoff['handoff_id'], 'expired', 'policy', now, event_required=True)
                changed = True
        return changed

    def _candidate(self, conn, role, schedules, now):
        if not schedules:
            raise RouteRejected('agent campaign requires operator registration')
        if now < schedules['producer']['last_tick_at']:
            return None
        slot = current_slot(self.config, now)
        if slot is None or not schedules[role]['enabled'] or self._paused(conn):
            return None
        if self._task_for_slot(conn, role, slot) is not None:
            return None
        if role == 'reviewer':
            row = self._handoff(conn, slot)
            if row is None or row['state'] != 'offered':
                return None
        return slot

    def prepare(self, role):
        with self._tx(execution=True) as conn:
            _, schedules = self._load(conn)
            at = self._now()
            candidate = self._candidate(conn, role, schedules, at)
            highwater = schedules['producer']['last_tick_at']
            if at >= highwater:
                recovered = self._sweep(conn, at)
                expired = highwater >= self.config['expires_at']
                if recovered or (at > highwater and not expired and not self._paused(conn)):
                    self._advance(conn, at)
            return candidate

    def _review_context(self, conn, slot, prompt):
        row = self._handoff(conn, slot)
        producer = self._task_for_slot(conn, 'producer', slot)
        result_ref = json.loads(producer['result_json'])['result_ref']
        offer_ref = row['authorization_ref']
        self._verify_proof(offer_ref, offer(self.config, slot, producer, result_ref))
        output = self._read_text(result_ref, MAX_RESPONSE_BYTES)
        return self._bound_text(review_text(prompt, output)), offer_ref, result_ref

    def _approve(self, conn, role, slot, task, context_ref, now):
        aid = 'a_' + uuid4().hex
        proof = approval(self.config, role, slot, task, context_ref)
        ref = self._proof(proof)
        conn.execute('''INSERT INTO control_approvals(approval_id,task_id,fingerprint,scope,decision,actor,
            evidence_ref,expires_at,at) VALUES(?,?,?,?,'approved',?,?,?,?)''',
            (aid, task['task_id'], task['fingerprint'], SCOPE, OPERATOR, ref, proof['expires_at'], now))
        updated = conn.execute('UPDATE control_task_rules SET approval_id=? WHERE task_id=? AND approval_id IS NULL',
                               (aid, task['task_id'])).rowcount
        if updated != 1:
            raise DispatchError('agent campaign approval requires a fresh task rule')
        _event(conn, 'campaign-approval:' + task['task_id'], 'approval.decided', OPERATOR, now,
               task_id=task['task_id'], fence=1, refs=(ref,), required=True)

    def _reserve(self, conn, task, device, now):
        bid = budget_id(self.config)
        changed = conn.execute('''UPDATE control_budgets SET reserved_units=reserved_units+1
            WHERE budget_id=? AND starts_at<=? AND ends_at>? AND used_units+reserved_units<limit_units''',
            (bid, now, now)).rowcount
        if changed != 1:
            raise RouteRejected('agent campaign request budget is exhausted')
        conn.execute('''INSERT INTO control_reservations(task_id,fence,budget_id,fingerprint,
            reserved_units,state,created_at) VALUES(?,1,?,?,1,'reserved',?)''',
            (task['task_id'], bid, task['fingerprint'], now))
        _event(conn, 'reserved:' + task['task_id'] + ':1', 'budget.reserved', device, now,
               task_id=task['task_id'], fence=1, required=True)

    def _record_firing(self, conn, role, slot, task, spec, now):
        sid = schedule_id(self.config, role)
        conn.execute('''INSERT INTO schedule_firings(fire_key,schedule_id,scheduled_at,task_template,
            task_id,version,outcome,at,execution_route) VALUES(?,?,?,?,?,1,'created',?,?)''',
            (spec.idem_key, sid, slot_start(self.config, slot), _encoded(asdict(spec)), task['task_id'], now, ROUTE))
        count = conn.execute('SELECT COUNT(*) FROM schedule_firings WHERE schedule_id=?', (sid,)).fetchone()[0]
        conn.execute('UPDATE schedules SET enabled=?,last_fire_key=?,next_fire_at=? WHERE id=?',
                     (int(count < self.config['slots']), spec.idem_key, slot_start(self.config, slot + 1), sid))

    def claim_slot(self, role, expected_slot, key):
        with self._tx(execution=True) as conn:
            _, schedules = self._load(conn)
            now = self._now()
            slot = self._candidate(conn, role, schedules, now)
            if slot is None or slot != expected_slot:
                return None
            self._verify_proof(ref_for(self.config), self.config)
            approved_text = self._bound_text(self._prompt(role))
            role_context_ref = None
            if role == 'reviewer':
                approved_text, offer_ref, output_ref = self._review_context(conn, slot, approved_text)
            if key in approved_text:
                raise DispatchError('agent campaign approved input matches credential material')
            spec = task_spec(self.config, role, slot)
            task, existing = self._submit(spec, conn=conn, control_rule={
                'budget_id': budget_id(self.config), 'reserve_units': 1, 'approval_scope': SCOPE})
            if existing:
                raise RouteRejected('agent campaign slot already contains an unrelated task')
            if role == 'reviewer':
                request_ref = self._put(approved_text.encode('utf-8'))
                role_context_ref = self._proof(context(self.config, slot, offer_ref, output_ref, request_ref))
                accepted_ref = self._proof(receipt(self.config, slot, task, offer_ref, role_context_ref, 'accepted'))
                hid = handoff_id(self.config, slot)
                _event(conn, 'campaign-context:' + hid, 'campaign.context_read', DEVICES[role], now,
                       task_id=task['task_id'], fence=1, refs=(accepted_ref, role_context_ref, request_ref), required=True)
                _write_handoff_state(conn, hid, 'accepted', DEVICES[role], now,
                                     receipt_ref=accepted_ref, event_required=True)
            self._approve(conn, role, slot, task, role_context_ref, now)
            self._reserve(conn, task, DEVICES[role], now)
            self._record_firing(conn, role, slot, task, spec, now)
            self._advance(conn, now)
            return self._grant_lease(conn, task, DEVICES[role]), approved_text

    def _claim_identity(self, conn, task, device):
        role = next((role for role, item in DEVICES.items() if item == device), None)
        if role is None or task['fence'] != 1:
            raise RouteRejected('agent campaign lease has the wrong role or fence')
        row = conn.execute('SELECT * FROM schedule_firings WHERE schedule_id=? AND task_id=?',
                           (schedule_id(self.config, role), task['task_id'])).fetchone()
        if row is None:
            raise RouteRejected('agent campaign lease is not bound to its campaign and role')
        slot = current_slot(self.config, row['scheduled_at'])
        if slot is None:
            raise RouteRejected('agent campaign lease has no authorized time slot')
        return role, slot

    def _active(self, conn, lease_id, fence, device_id):
        _, schedules = self._load(conn)
        lease, task, valid = super()._active(conn, lease_id, fence, device_id)
        if (not schedules or lease is None or task is None or lease['device_id'] != device_id
                or type(fence) is not int or fence != 1):
            raise RouteRejected('agent campaign lease has the wrong campaign or owner')
        self._claim_identity(conn, task, device_id)
        return lease, task, valid and self._now() >= schedules['producer']['last_tick_at']

    def mark_unknown(self, lease_id, fence, reason, device_id):
        if reason not in UNKNOWN_ERRORS:
            raise DispatchError('invalid campaign provider failure reason')
        with self._tx(execution=True) as conn:
            lease, task, valid = self._active(conn, lease_id, fence, device_id)
            if not valid:
                raise FenceRejected('campaign lease is not current, owned and unexpired')
            now = self._now()
            self._transition(conn, task, 'unknown', reason, device_id)
            conn.execute("UPDATE leases SET state='unknown' WHERE lease_id=?", (lease['lease_id'],))
            if not expire_reservation(conn, task['task_id'], fence, device_id, now):
                raise DispatchError('agent campaign unknown lease lost its reservation')
            self._advance(conn, now)

    def _outcome(self, outcome):
        outcome, encoded = super()._outcome(outcome)
        validate_result(self.config, outcome.status, outcome.result)
        ref = outcome.result.get('result_ref')
        expected = (Evidence('agent_result', ref, 'local_only', artifact_hash=ref),) if ref else ()
        if outcome.evidence != expected:
            raise DispatchError('campaign provider output requires exact local evidence')
        return outcome, encoded

    def _before_complete(self, conn, task, outcome, device_id):
        role, slot = self._claim_identity(conn, task, device_id)
        now = self._now()
        output_ref = outcome.result.get('result_ref')
        if output_ref:
            self._read_text(output_ref, MAX_RESPONSE_BYTES)
        context_ref = None
        if role == 'reviewer':
            row = self._handoff(conn, slot)
            accepted = conn.execute('SELECT refs_json FROM control_events WHERE event_key=?',
                                    ('campaign-context:' + row['handoff_id'],)).fetchone()
            accepted_ref, context_ref, request_ref = json.loads(accepted['refs_json'])
            producer = self._task_for_slot(conn, 'producer', slot)
            producer_ref = json.loads(producer['result_json'])['result_ref']
            self._verify_proof(context_ref, context(self.config, slot, row['authorization_ref'],
                                                    producer_ref, request_ref))
            self._verify_proof(accepted_ref, receipt(self.config, slot, task, row['authorization_ref'],
                                                     context_ref, 'accepted'))
            self._read_text(request_ref, MAX_REQUEST_BYTES)
        self._settle(conn, role, slot, task, outcome.result, context_ref, now)
        if now < slot_end(self.config, slot):
            if role == 'producer' and outcome.status == 'succeeded' and not self._paused(conn):
                self._offer(conn, slot, task, output_ref, now)
            elif role == 'reviewer':
                decision = 'completed' if outcome.status == 'succeeded' else 'rejected'
                proof = self._proof(receipt(self.config, slot, task, row['authorization_ref'], context_ref,
                                            decision, accepted_ref=accepted_ref, reviewer_output_ref=output_ref))
                _write_handoff_state(conn, row['handoff_id'], decision, device_id, now, receipt_ref=proof,
                                     refs=(output_ref,) if output_ref else (), event_required=True)
        self._advance(conn, now)

    def _settle(self, conn, role, slot, task, result, context_ref, now):
        proof = self._proof(usage_receipt(self.config, role, slot, task, result, context_ref))
        bid = budget_id(self.config)
        changed = conn.execute("""UPDATE control_reservations SET state='settled',actual_units=1,
            evidence_ref=?,settled_at=? WHERE task_id=? AND fence=1 AND budget_id=? AND fingerprint=?
            AND state='reserved' AND reserved_units=1""", (proof, now, task['task_id'], bid, task['fingerprint'])).rowcount
        budget = conn.execute('''UPDATE control_budgets SET used_units=used_units+1,reserved_units=reserved_units-1
            WHERE budget_id=? AND reserved_units>=1''', (bid,)).rowcount
        if changed != 1 or budget != 1:
            raise DispatchError('agent campaign completion lost its fenced reservation')
        _event(conn, 'settled:' + task['task_id'] + ':1', 'budget.settled', DEVICES[role], now,
               task_id=task['task_id'], fence=1, refs=(proof,), required=True)

    def _offer(self, conn, slot, task, output_ref, now):
        value = offer(self.config, slot, task, output_ref)
        proof = self._proof(value)
        _insert_handoff(conn, value['handoff_id'], from_conv=DEVICES['producer'], to_conv=DEVICES['reviewer'],
                        project_id=self.config['campaign_id'], scope='instruct', authorized_by=OPERATOR,
                        scope_refs=(ref_for(self.config), output_ref), authorization_ref=proof,
                        expires_at=value['expires_at'], at=now, event_required=True)

    def status(self):
        with closing(self._connect()) as conn, conn:
            conn.execute('BEGIN')
            self._check_ledger(conn)
            _, schedules = self._load(conn)
            return self._status(conn, schedules, self._now())

    def _status(self, conn, schedules, now):
        config = self.config
        budget = conn.execute('SELECT * FROM control_budgets WHERE budget_id=?',
                              (budget_id(config),)).fetchone() if schedules else None
        highwater = schedules['producer']['last_tick_at'] if schedules else None
        if not schedules:
            status = 'unregistered'
        elif self._paused(conn):
            status = 'paused'
        elif now < highwater:
            status = 'clock_rollback'
        elif now < config['starts_at']:
            status = 'scheduled'
        elif now >= config['expires_at']:
            status = 'expired'
        else:
            status = 'active'
        observed = max(now, highwater) if highwater else now
        slots = []
        for slot in range(1, config['slots'] + 1):
            start, end = slot_start(config, slot), slot_end(config, slot)
            row = self._handoff(conn, slot) if schedules else None
            handoff = None
            if row:
                handoff = {key: row[key] for key in ('handoff_id', 'state', 'authorization_ref', 'receipt_ref',
                                                    'expires_at', 'created_at', 'updated_at')}
                handoff['evidence_refs'] = json.loads(row['evidence_refs_json'])
                handoff['expiry_due'] = observed >= row['expires_at'] and row['state'] in ('offered', 'accepted')
                reading = conn.execute('SELECT refs_json FROM control_events WHERE event_key=?',
                                       ('campaign-context:' + row['handoff_id'],)).fetchone()
                if reading:
                    handoff['accepted_ref'], handoff['context_ref'], handoff['request_ref'] = json.loads(reading['refs_json'])
            view = dict(slot=slot, scheduled_at=start, expires_at=end, handoff=handoff)
            for role, device in DEVICES.items():
                task = self._task_for_slot(conn, role, slot) if schedules else None
                if task:
                    view[role] = self._task_status(conn, task, device)
                else:
                    empty = 'missed' if observed >= end else ('paused' if status == 'paused' else 'pending')
                    view[role] = dict(status=empty, task_id=None, device_id=device)
            slots.append(view)
        return dict(status=status, campaign_id=config['campaign_id'], authorization_id=config['authorization_id'],
                    config_ref=ref_for(config), ledger_identity=config['ledger_identity'], starts_at=config['starts_at'],
                    expires_at=config['expires_at'], interval_seconds=config['interval_seconds'], last_observed_at=highwater,
                    budget_limit_units=config['max_requests'], budget_unit='provider_request',
                    request_units_used=budget['used_units'] if budget else 0,
                    request_units_held=budget['reserved_units'] if budget else 0, slots=slots, cost=None)

    def _task_status(self, conn, task, device):
        tid = task['task_id']
        info = conn.execute('''SELECT a.evidence_ref AS approval_ref, r.state AS reservation_state,
            r.evidence_ref AS usage_evidence_ref, l.lease_id, l.state AS lease_state, l.granted_at,
            t.created_at,t.updated_at FROM tasks t JOIN control_task_rules rule ON rule.task_id=t.task_id
            JOIN control_approvals a ON a.approval_id=rule.approval_id
            JOIN control_reservations r ON r.task_id=t.task_id JOIN leases l ON l.task_id=t.task_id
            WHERE t.task_id=?''', (tid,)).fetchone()
        return dict(info) | dict(status=task['status'], task_id=tid, device_id=device,
                                 fence=task['fence'], attempts=task['attempts']) | (
                                     json.loads(task['result_json']) if task['result_json'] else {})


class _TransactionClock:
    def __init__(self, now: datetime):
        self.instant = now

    def now(self):
        return self.instant


def register(ledger, artifacts, config, *, clock=None):
    store = _CampaignStore(ledger, artifacts, config, clock=clock)
    store._register()
    return store.status()


def status(ledger, artifacts, config, *, clock=None):
    return _CampaignStore(ledger, artifacts, config, clock=clock).status()


def stop(ledger, artifacts, config, *, clock=None):
    store = _CampaignStore(ledger, artifacts, config, clock=clock)
    store._stop()
    return store.status()
