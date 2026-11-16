"""One fixed public or synthetic DeepSeek slot on the common Dispatch ledger."""

from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
import sqlite3

from sa_dispatch.agent_control import (BUDGET_ID, SCOPE, read_budget, record_approval,
                                       register_budget, reserve_request, settle_request)
from sa_dispatch.campaign_validation import campaign_records, validate_campaign_records
from sa_dispatch.canonical import canonical_json, sha256_ref
from sa_dispatch.clock import timestamp
from sa_dispatch.control_policy import expire_reservation
from sa_dispatch.external_migration import verify_schema
from sa_dispatch.models import DispatchError, Evidence, FenceRejected, Outcome, SweepReport
from sa_dispatch.routing import EXTERNAL_ROUTE, RouteRejected, ledger_mode
from sa_dispatch.store_sqlite import SqliteStore

from .deepseek_client import DeepSeekError, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, TokenUsage
from .provider_process import ERROR_KINDS
from .worker import AgentKind, CAPABILITY, KIND, OperatorGrant


DEVICE_ID = 'sa-agent-external'
DEADLINE_SECONDS = 45
LEASE_TTL_SECONDS = 120
THINKING = 'disabled'
DEFINITE_HTTP_ERRORS = {
    'auth_rejected': (401, 403),
    'rate_limited': (429,),
    'provider_error': (400, 402, 404, 413, 422),
}
UNKNOWN_ERRORS = ERROR_KINDS | {'deadline_exceeded', 'provider_process_failed', 'process_start_failed'}


def ledger_identity(path):
    info = Path(path).stat()
    return f'{info.st_dev}:{info.st_ino}'


@dataclass(frozen=True)
class CommonGrant:
    grant: OperatorGrant
    expires_at: str
    ledger_identity: str
    machine_id: str

    def __post_init__(self):
        try:
            expiry = timestamp(datetime.fromisoformat(self.expires_at))
        except (TypeError, ValueError, OverflowError):
            raise DispatchError('invalid common agent grant expiry') from None
        if (not isinstance(self.grant, OperatorGrant) or self.grant.execution_mode != 'live'
                or self.expires_at != expiry or self.expires_at <= self.grant.scheduled_at
                or not re.fullmatch(r'[0-9]{1,20}:[0-9]{1,20}', self.ledger_identity)
                or not re.fullmatch(r'[0-9a-f]{32}', self.machine_id)):
            raise DispatchError('invalid common agent live grant')
        for field in ('owner', 'id', 'version'):
            if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}', getattr(self.grant.prompt_ref, field)):
                raise DispatchError('common agent prompt requires a bounded fixed reference')

    def authority(self):
        return {'grant': self.grant.params(), 'expires_at': self.expires_at,
                'device_id': DEVICE_ID, 'ledger_identity': self.ledger_identity,
                'machine_id': self.machine_id, 'thinking': THINKING}


def _ref(value):
    return isinstance(value, str) and re.fullmatch(r'sha256:[0-9a-f]{64}', value)


class _CommonAgentStore(SqliteStore):
    _allowed_routes = (EXTERNAL_ROUTE,)
    _routed_evidence_access = 'local_only'

    def __init__(self, ledger, artifacts, config, *, clock=None):
        self.artifacts = artifacts
        self.config = config
        self.grant = config.grant
        self.template = self.grant.template()
        self.encoded_template = canonical_json(asdict(self.template)).decode('utf-8')
        self.next_at = timestamp(datetime.fromisoformat(self.grant.scheduled_at) + timedelta(seconds=1))
        self.authority = config.authority()
        self.approval_rule = {'budget_id': BUDGET_ID, 'reserve_units': 1, 'approval_scope': SCOPE}
        super().__init__(ledger, clock=clock, lease_ttl_seconds=LEASE_TTL_SECONDS,
                         kinds={KIND: AgentKind()}, route=EXTERNAL_ROUTE)
        with closing(self._connect()) as conn, conn:
            conn.execute('BEGIN')
            if verify_schema(conn) != '0013':
                raise RouteRejected('common agent requires the installed 0013 schema')
            self._require_common_ledger(conn)

    def _require_common_ledger(self, conn):
        if (ledger_mode(conn) != 'mixed' or ledger_identity(self.path) != self.config.ledger_identity
                or conn.execute('SELECT MAX(version) FROM schema_migrations').fetchone()[0] != '0013'):
            raise RouteRejected('common agent ledger identity or schema version differs')

    @contextmanager
    def _tx(self, *, execution=False):
        with super()._tx(execution=execution) as conn:
            self._require_common_ledger(conn)
            yield conn

    def _enabled_kind(self, name, version):
        return name == KIND and version == self.template.kind_version

    def _requires_control(self, name):
        return name == KIND

    def _validate_submission_route(self, spec):
        if spec != self.template:
            raise RouteRejected('common agent task must match the fixed grant template')

    def submit(self, *args, **kwargs):
        raise RouteRejected('common agent tasks require fixed slot registration')

    def register_device(self, *args, **kwargs):
        raise RouteRejected('common agent device requires fixed slot registration')

    device_register = register_device

    def renew(self, *args, **kwargs):
        raise RouteRejected('common agent lease has a fixed deadline')

    def cancel(self, *args, **kwargs):
        raise RouteRejected('common agent slot is immutable')

    def resolve_unknown(self, *args, **kwargs):
        raise RouteRejected('common agent unknown request retains its lifetime slot')

    def _put(self, content):
        ref = self.artifacts.put(content)
        if os.name == 'posix':
            path = self.artifacts.path(ref)
            # The registering operator and the service UID share this CAS.
            with path.open('rb') as handle:
                if os.fstat(handle.fileno()).st_uid == os.geteuid():
                    os.fchmod(handle.fileno(), 0o640)
                    os.fsync(handle.fileno())
            for parent in (path.parent, path.parent.parent, self.artifacts.root):
                fd = os.open(parent, os.O_DIRECTORY | os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        return ref

    def _read(self, ref, max_bytes):
        with self.artifacts.path(ref).open('rb') as handle:
            content = handle.read(max_bytes + 1)
        if len(content) > max_bytes or sha256_ref(content) != ref:
            raise DispatchError('common agent artifact does not match its approved content')
        return content

    def prompt(self):
        prompt = self._read(self.grant.prompt_ref.hash, MAX_REQUEST_BYTES).decode('utf-8')
        # Leave room for the fixed JSON envelope in the provider byte limit.
        if not prompt.strip() or len(json.dumps(prompt, ensure_ascii=False).encode('utf-8')) > MAX_REQUEST_BYTES - 1024:
            raise DispatchError('common agent prompt exceeds the request bounds')
        return prompt

    def _verify_task(self, row):
        task = self._task(row)
        scope = self.kinds[KIND].scope(self.grant.params())
        identity = asdict(self.template)
        del identity['idem_key']
        expected = sha256_ref(canonical_json(identity | {'side_effect': 'external', 'scope': scope}))
        if (task.execution_route != EXTERNAL_ROUTE or task.spec != self.template
                or task.side_effect != 'external' or task.scope != scope or task.fingerprint != expected
                or task.fence != task.attempts or task.attempts not in (0, 1)):
            raise RouteRejected('common agent task drifted from its fixed authorization')
        return task

    def _slot(self, conn):
        schedule = conn.execute('SELECT * FROM schedules WHERE id=?', (self.grant.grant_id,)).fetchone()
        device = conn.execute('SELECT * FROM devices WHERE device_id=?', (DEVICE_ID,)).fetchone()
        task = conn.execute('SELECT * FROM tasks WHERE idem_key=?', (self.grant.fire_key,)).fetchone()
        firing = conn.execute('SELECT * FROM schedule_firings WHERE fire_key=?', (self.grant.fire_key,)).fetchone()
        selected = {'schedules': schedule, 'devices': device, 'tasks': task, 'schedule_firings': firing}
        records = campaign_records(conn)
        campaign_owned = validate_campaign_records(records)
        key = {'schedules': 'id', 'devices': 'device_id', 'tasks': 'task_id', 'schedule_firings': 'fire_key'}
        for table, row in selected.items():
            count = sum(item[key[table]] not in campaign_owned[table] for item in records[table])
            if count != int(row is not None) or (row is not None and row['execution_route'] != EXTERNAL_ROUTE):
                raise RouteRejected('common agent slot, task or device identifier is already bound')
        if schedule is None:
            if any(row is not None for row in (device, task, firing)):
                raise RouteRejected('common agent registration is incomplete')
            return None, None
        if device is None or json.loads(device['caps_json']) != [CAPABILITY]:
            raise RouteRejected('common agent device capability has changed')
        read_budget(conn, self.grant, self.config.expires_at)
        expected = {
            'interval_seconds': 1, 'task_template': self.encoded_template, 'version': self.grant.version,
            'catchup': 'latest', 'max_catchup': 1, 'max_slots': 1, 'enabled': int(firing is None),
            'last_fire_key': self.grant.fire_key if firing is not None else None,
            'next_fire_at': self.next_at if firing is not None else self.grant.scheduled_at,
        }
        if any(schedule[key] != value for key, value in expected.items()):
            raise RouteRejected('common agent schedule identity or lifetime slot changed')
        if firing is None and task is None:
            return schedule, None
        if firing is None or task is None or (
                firing['schedule_id'], firing['scheduled_at'], firing['version'],
                firing['task_template'], firing['outcome'], firing['task_id']) != (
                self.grant.grant_id, self.grant.scheduled_at, self.grant.version,
                self.encoded_template, 'created', task['task_id']):
            raise RouteRejected('common agent firing does not identify its immutable task')
        self._verify_task(task)
        return schedule, task

    def _materialize(self, conn):
        at = self._now()
        if not self.grant.scheduled_at <= at < self.config.expires_at:
            return None
        task, existing = self._submit(self.template, control_rule=self.approval_rule, conn=conn)
        if existing:
            raise RouteRejected('common agent task exists without its lifetime slot')
        record_approval(conn, task, self.authority, self._put, at)
        conn.execute('''INSERT INTO schedule_firings(fire_key,schedule_id,scheduled_at,task_template,
            task_id,version,outcome,at,execution_route) VALUES(?,?,?,?,?,?,'created',?,?)''',
            (self.grant.fire_key, self.grant.grant_id, self.grant.scheduled_at, self.encoded_template,
             task.task_id, self.grant.version, at, EXTERNAL_ROUTE))
        conn.execute('UPDATE schedules SET enabled=0, last_fire_key=?, next_fire_at=?, last_tick_at=? WHERE id=?',
                     (self.grant.fire_key, self.next_at, at, self.grant.grant_id))
        conn.execute('''INSERT INTO schedule_events(schedule_id,version,kind,at,detail_code)
            VALUES(?,?,'schedule.exhausted',?,'max_slots_reached')''', (self.grant.grant_id, self.grant.version, at))
        return task.task_id

    def register(self):
        with self._tx(execution=True) as conn:
            schedule, task = self._slot(conn)
            if schedule is None:
                at = self._now()
                if at >= self.config.expires_at:
                    raise DispatchError('common agent grant has expired')
                self.prompt()
                register_budget(conn, self.grant, self.config.expires_at)
                self._register_route_identity(conn, DEVICE_ID)
                conn.execute('INSERT INTO devices(device_id,caps_json,last_seen_at,execution_route) VALUES(?,?,?,?)',
                             (DEVICE_ID, json.dumps([CAPABILITY]), at, EXTERNAL_ROUTE))
                conn.execute('''INSERT INTO schedules(id,interval_seconds,task_template,next_fire_at,
                    enabled,version,catchup,max_catchup,max_slots,execution_route)
                    VALUES(?,1,?,?,1,?,'latest',1,1,?)''',
                    (self.grant.grant_id, self.encoded_template, self.grant.scheduled_at,
                     self.grant.version, EXTERNAL_ROUTE))
            if task is None:
                self._materialize(conn)

    def due_task_id(self):
        with self._tx(execution=True) as conn:
            schedule, task = self._slot(conn)
            if schedule is None:
                raise RouteRejected('common agent slot requires operator registration')
            return task['task_id'] if task is not None else self._materialize(conn)

    def claim(self, device_id, caps=None, *, synthetic_handoff=False, kinds=None, task_id=None):
        if (device_id != DEVICE_ID or caps != (CAPABILITY,) or kinds != (KIND,)
                or synthetic_handoff is not False or not isinstance(task_id, str) or not task_id):
            raise RouteRejected('common agent claim requires its device, kind, capability and exact task')
        with self._tx(execution=True) as conn:
            _, task = self._slot(conn)
            if task is None or task['task_id'] != task_id:
                raise RouteRejected('common agent claim does not match the registered task')
            if task['status'] != 'queued':
                return None
            if reserve_request(conn, task, self.authority, self._read, device_id, self._now()):
                return self._grant_lease(conn, task, device_id)
        return None

    def _active(self, conn, lease_id, fence, device_id):
        if device_id != DEVICE_ID or type(fence) is not int or fence != 1:
            raise RouteRejected('common agent lease requires its fixed device and first fence')
        lease, task, valid = super()._active(conn, lease_id, fence, device_id)
        _, registered = self._slot(conn)
        if (task is None or registered is None or task['task_id'] != registered['task_id']
                or lease['device_id'] != DEVICE_ID or lease['fence'] != 1):
            raise RouteRejected('common agent lease does not match the registered slot')
        return lease, task, valid

    def mark_unknown(self, lease_id, fence, reason, device_id):
        if reason not in UNKNOWN_ERRORS:
            raise RouteRejected('invalid common agent failure reason')
        return super().mark_unknown(lease_id, fence, reason, device_id)

    def sweep(self, *, task_id=None):
        if not isinstance(task_id, str) or not task_id:
            raise RouteRejected('common agent sweep requires the registered task')
        with self._tx(execution=True) as conn:
            _, task = self._slot(conn)
            if task is None or task['task_id'] != task_id:
                raise RouteRejected('common agent sweep does not match the registered task')
            lease = conn.execute("""SELECT * FROM leases WHERE task_id=? AND fence=?
                AND device_id=? AND state='active' AND expires_at<=?""",
                (task_id, task['fence'], DEVICE_ID, self._now())).fetchone()
            if task['status'] == 'leased' and lease is not None:
                self._transition(conn, task, 'unknown', 'lease expired', 'sweep')
                conn.execute("UPDATE leases SET state='expired' WHERE lease_id=?", (lease['lease_id'],))
                expire_reservation(conn, task_id, task['fence'], 'sweep', self._now())
                return SweepReport(expired=1, unknown=1)
        return SweepReport()

    def _outcome(self, outcome):
        outcome, encoded = super()._outcome(outcome)
        result = outcome.result
        if (not isinstance(result, dict) or result.get('execution_mode') != 'live'
                or result.get('provider') != self.grant.provider or result.get('cost') is not None
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}', result.get('model', ''))):
            raise DispatchError('common agent result has invalid provider metadata')
        fields = {'execution_mode', 'provider', 'model', 'cost'}
        if set(result) == fields | {'http_status', 'error'}:
            if (outcome.status != 'failed' or outcome.evidence or result['model'] != self.grant.model
                    or result['http_status'] not in DEFINITE_HTTP_ERRORS.get(result['error'], ())):
                raise DispatchError('common agent failure requires definitive HTTP response metadata')
        else:
            usage = result.get('usage')
            result_ref = result.get('result_ref')
            response_id = result.get('response_id', '')
            truncated = result.get('truncated')
            if (set(result) != fields | {'response_id', 'finish_reason', 'truncated', 'usage', 'result_ref'}
                    or type(truncated) is not bool or result['finish_reason'] != ('length' if truncated else 'stop')
                    or outcome.status != ('failed' if truncated else 'succeeded')
                    or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}', response_id)
                    or not isinstance(usage, dict) or set(usage) != set(TokenUsage.__dataclass_fields__)
                    or not all(type(usage[key]) is int and 0 <= usage[key] <= 2**31-1
                               for key in ('prompt_tokens', 'completion_tokens', 'total_tokens'))
                    or any(value is not None and (type(value) is not int or not 0 <= value <= usage['prompt_tokens'])
                           for key, value in usage.items() if key.startswith('prompt_cache_'))
                    or usage['prompt_tokens'] + usage['completion_tokens'] != usage['total_tokens']
                    or not _ref(result_ref)
                    or outcome.evidence != (Evidence('agent_result', result_ref, 'local_only', artifact_hash=result_ref),)):
                raise DispatchError('common agent result evidence or usage is invalid')
        return outcome, encoded

    def _before_complete(self, conn, task, outcome, device_id):
        if outcome.evidence:
            self._read(outcome.result['result_ref'], MAX_RESPONSE_BYTES)
        settle_request(conn, task, self.authority, outcome.result, self._put, self._now())

    def status(self):
        with closing(self._connect()) as conn, conn:
            conn.execute('BEGIN')
            self._require_common_ledger(conn)
            schedule, task = self._slot(conn)
            budget = read_budget(conn, self.grant, self.config.expires_at) if schedule is not None else None
            status = task['status'] if task is not None else ('scheduled' if schedule is not None else 'unregistered')
            task_id, fence = (task['task_id'], task['fence']) if task is not None else (None, None)
            result, proof, reservation, lease = {}, None, None, None
            if task is not None:
                proof = conn.execute('''SELECT a.evidence_ref FROM control_task_rules r
                    JOIN control_approvals a ON r.approval_id=a.approval_id WHERE r.task_id=?''', (task_id,)).fetchone()
                reservation = conn.execute('SELECT state,evidence_ref FROM control_reservations WHERE task_id=? AND fence=?',
                                           (task_id, fence)).fetchone()
                lease = conn.execute('SELECT lease_id,state FROM leases WHERE task_id=? AND fence=? AND device_id=?',
                                     (task_id, fence, DEVICE_ID)).fetchone()
                if task['result_json'] is not None:
                    result = json.loads(task['result_json'])
                    if not isinstance(result, dict):
                        raise DispatchError('common agent ledger result has invalid metadata')
                    result_ref = result.get('result_ref')
                    evidence = (Evidence('agent_result', result_ref, 'local_only', artifact_hash=result_ref),) if result_ref else ()
                    self._outcome(Outcome(status, result, evidence))
            return {
                'status': status, 'task_id': task_id,
                'execution_route': EXTERNAL_ROUTE, 'execution_mode': 'live', 'provider': self.grant.provider,
                'grant_id': self.grant.grant_id, 'grant_version': self.grant.version,
                'device_id': DEVICE_ID, 'ledger_identity': self.config.ledger_identity,
                'scheduled_at': self.grant.scheduled_at, 'expires_at': self.config.expires_at,
                'grant_expired': self._now() >= self.config.expires_at,
                'fence': fence,
                'attempts': task['attempts'] if task is not None else 0,
                'budget_unit': 'provider_request', 'budget_limit_units': 1,
                'request_units_used': budget['used_units'] if budget is not None else 0,
                'request_units_held': budget['reserved_units'] if budget is not None else 0,
                'approval_ref': proof['evidence_ref'] if proof is not None and _ref(proof['evidence_ref']) else None,
                'reservation_state': reservation['state'] if reservation is not None else None,
                'usage_evidence_ref': (reservation['evidence_ref'] if reservation is not None
                                       and _ref(reservation['evidence_ref']) else None),
                'lease_id': lease['lease_id'] if lease is not None else None,
                'lease_state': lease['state'] if lease is not None else None,
                'requested_model': self.grant.model, 'model': result.get('model'),
                'response_id': result.get('response_id'), 'result_ref': result.get('result_ref'),
                'finish_reason': result.get('finish_reason'), 'usage': result.get('usage'),
                'error': result.get('error'), 'http_status': result.get('http_status'), 'cost': None,
            }


def register(ledger, artifacts, config, *, clock=None):
    store = _CommonAgentStore(ledger, artifacts, config, clock=clock)
    store.register()
    return store.status()


def status(ledger, artifacts, config, *, clock=None):
    return _CommonAgentStore(ledger, artifacts, config, clock=clock).status()


def tick(ledger, artifacts, config, *, credential_reader, provider_call, clock=None):
    store = _CommonAgentStore(ledger, artifacts, config, clock=clock)
    task_id = store.due_task_id()
    if task_id is None:
        return store.status()
    store.sweep(task_id=task_id)
    summary = store.status()
    if summary['status'] != 'queued' or summary['grant_expired']:
        return summary
    prompt = store.prompt()
    key = credential_reader()
    if not isinstance(key, str) or not re.fullmatch(r'[A-Za-z0-9._~+/=-]{1,1024}', key):
        raise DispatchError('common agent credential unavailable or invalid')
    if key in prompt:
        raise DispatchError('common agent approved prompt matches credential material')
    lease = store.claim(DEVICE_ID, (CAPABILITY,), kinds=(KIND,), task_id=task_id)
    if lease is None:
        return store.status()

    metadata = {'execution_mode': 'live', 'provider': config.grant.provider, 'cost': None}
    try:
        completion = provider_call(model=config.grant.model, approved_text=prompt, api_key=key,
                                   max_tokens=config.grant.max_tokens, deadline_seconds=DEADLINE_SECONDS,
                                   thinking=THINKING)
    except DeepSeekError as error:
        if error.outcome_unknown or error.status not in DEFINITE_HTTP_ERRORS.get(error.kind, ()):
            reason = error.kind if error.kind in UNKNOWN_ERRORS else 'invalid_response'
            try:
                store.mark_unknown(lease.lease_id, lease.fence, reason, DEVICE_ID)
            except FenceRejected:
                store.sweep(task_id=task_id)
            return store.status()
        outcome = Outcome('failed', metadata | {'model': config.grant.model,
                                                'error': error.kind, 'http_status': error.status})
    else:
        fields = (completion.response_id, completion.model, completion.text)
        if any(key in field for field in fields):
            store.mark_unknown(lease.lease_id, lease.fence, 'invalid_response', DEVICE_ID)
            return store.status()
        ref = store._put(completion.text.encode('utf-8'))
        outcome = Outcome('failed' if completion.truncated else 'succeeded', metadata | {
            'model': completion.model, 'response_id': completion.response_id,
            'finish_reason': completion.finish_reason, 'truncated': completion.truncated,
            'usage': asdict(completion.usage), 'result_ref': ref,
        }, (Evidence('agent_result', ref, 'local_only', artifact_hash=ref),))
    try:
        try:
            store.complete(lease.lease_id, lease.fence, outcome, DEVICE_ID)
        except sqlite3.OperationalError:
            # Retry only the same completion. Its lease and canonical receipt are the idempotency key.
            store.complete(lease.lease_id, lease.fence, outcome, DEVICE_ID)
    except FenceRejected:
        store.sweep(task_id=task_id)
    return store.status()
