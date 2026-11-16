"""Synchronous, single-slot worker for an operator-approved DeepSeek call.

The caller supplies a trusted grant and must enforce a hard wall deadline before
using the real transport. An expired external lease is recovered by ledger sweep.
"""

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import json
import re

from sa_dispatch.canonical import canonical_json, sha256_ref
from sa_dispatch.clock import timestamp
from sa_dispatch.models import DispatchError, Evidence, Outcome, Ref, TaskSpec
from sa_dispatch.routing import RouteRejected
from sa_dispatch.scheduler import Scheduler

from .deepseek_client import DeepSeekError, MAX_OUTPUT_TOKENS, MAX_REQUEST_BYTES


KIND = 'agent.run'
CAPABILITY = 'agent.deepseek'


@dataclass(frozen=True)
class OperatorGrant:
    grant_id: str
    version: int
    prompt_ref: Ref
    source: str
    provider: str
    model: str
    max_tokens: int
    scheduled_at: str
    egress: str
    # None preserves the pre-R1 direct-call contract. It never implies simulation.
    execution_mode: str | None = None

    def __post_init__(self):
        ref = self.prompt_ref
        if (
            not isinstance(self.grant_id, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', self.grant_id)
            or type(self.version) is not int or not 1 <= self.version < 2**63
            or not isinstance(ref, Ref) or ref.kind != 'agent_prompt'
            or any(not isinstance(value, str) or not value for value in asdict(ref).values())
            or ref.version == 'latest' or not re.fullmatch(r'sha256:[0-9a-f]{64}', ref.hash)
            or self.source not in ('synthetic', 'public') or self.egress != 'derived'
            or self.execution_mode not in (None, 'simulation', 'live')
            or self.provider != 'deepseek' or not isinstance(self.model, str)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}', self.model)
            or type(self.max_tokens) is not int or not 1 <= self.max_tokens <= MAX_OUTPUT_TOKENS
        ):
            raise DispatchError('invalid agent operator grant')
        try:
            instant = datetime.fromisoformat(self.scheduled_at)
            canonical = timestamp(instant)
            timestamp(instant + timedelta(seconds=1))
        except (TypeError, ValueError, OverflowError):
            raise DispatchError('invalid agent schedule instant') from None
        if self.scheduled_at != canonical:
            raise DispatchError('canonical UTC schedule instant required')

    @property
    def fire_key(self):
        return f'sched:{self.grant_id}:v{self.version}:{self.scheduled_at}'

    def params(self):
        params = asdict(self)
        if self.execution_mode is None:
            del params['execution_mode']
        return params

    def template(self):
        return TaskSpec(self.fire_key, KIND, 1, self.params(), (self.prompt_ref,),
                        (CAPABILITY,), 'exportable', self.egress, 1)


class AgentKind:
    name = KIND
    version = 1
    enabled = True
    side_effect = 'external'
    dedicated_runner = True

    def validate(self, params):
        try:
            if 'execution_mode' in params and params['execution_mode'] is None:
                raise DispatchError('legacy mode must omit the execution_mode field')
            return OperatorGrant(**(params | {'prompt_ref': Ref(**params['prompt_ref'])})).params()
        except (KeyError, TypeError):
            raise DispatchError('invalid agent task parameters') from None

    def scope(self, params):
        return {'grant_id': params['grant_id'], 'grant_version': params['version']}


class AgentScheduler(Scheduler):
    """Restrict the standard scheduler to exactly one immutable grant."""

    def __init__(self, store, grant):
        self.grant = grant
        self.template = grant.template()
        self.encoded = canonical_json(asdict(self.template)).decode('utf-8')
        self.next_at = timestamp(datetime.fromisoformat(grant.scheduled_at) + timedelta(seconds=1))
        super().__init__(store)
        self._validate_template(self.template)

    def _require_scheduler_route(self, mode):
        if mode != 'isolated' or self.store.route is not None:
            raise RouteRejected('agent requires an isolated ledger')

    def _validate_template(self, template):
        spec = TaskSpec.from_dict(template) if isinstance(template, dict) else template
        kind = self.store.kinds.get(KIND) if self.store.kinds is not None else None
        if (type(kind) is not AgentKind or not kind.enabled or kind.side_effect != 'external'
                or kind.version != self.template.kind_version or not isinstance(spec, TaskSpec)
                or canonical_json(asdict(spec)) != canonical_json(asdict(self.template))
                or kind.validate(spec.params) != self.template.params):
            raise DispatchError('agent template does not match the operator grant')
        return spec

    def upsert(self, *args, **kwargs):
        raise DispatchError('agent schedule is immutable')

    def _ensure_schedule(self):
        grant = self.grant
        with self.store._tx(execution=True) as conn:
            schedule = self._schedule_for_upsert(conn, grant.grant_id)
            if schedule is None:
                conn.execute('''INSERT INTO schedules(id, interval_seconds, task_template, next_fire_at,
                    enabled, version, catchup, max_catchup, max_slots, execution_route)
                    VALUES(?,1,?,?,1,?,'latest',1,1,NULL)''',
                             (grant.grant_id, self.encoded, grant.scheduled_at, grant.version))
                schedule = self._schedule_for_upsert(conn, grant.grant_id)
            rows = conn.execute('SELECT * FROM schedule_firings WHERE schedule_id=?',
                                (grant.grant_id,)).fetchall()
            if (schedule['version'] != grant.version or schedule['max_slots'] != 1
                    or schedule['interval_seconds'] != 1 or schedule['catchup'] != 'latest'
                    or schedule['max_catchup'] != 1 or len(rows) > 1):
                raise DispatchError('agent schedule budget or identity changed')
            self._validate_template(json.loads(schedule['task_template']))
            if rows:
                firing = rows[0]
                if (schedule['enabled'] or schedule['next_fire_at'] != self.next_at
                        or schedule['last_fire_key'] != grant.fire_key or firing['execution_route'] is not None
                        or firing['fire_key'] != grant.fire_key or firing['version'] != grant.version
                        or firing['scheduled_at'] != grant.scheduled_at
                        or firing['outcome'] not in ('pending', 'created')):
                    raise DispatchError('agent firing identity changed')
                self._validate_template(json.loads(firing['task_template']))
            elif (schedule['next_fire_at'] != grant.scheduled_at or schedule['last_fire_key'] is not None
                  or not schedule['enabled']):
                raise DispatchError('agent schedule state changed')

    def tick(self, *, limit=1, synthetic_handoff=False):
        if type(limit) is not int or limit != 1 or synthetic_handoff is not False:
            raise DispatchError('agent supports one isolated firing')
        self._ensure_schedule()
        super().tick(limit=1, schedule_id=self.grant.grant_id)
        with self.store._tx(execution=True) as conn:
            row = conn.execute('SELECT * FROM schedule_firings WHERE fire_key=? AND schedule_id=?',
                               (self.grant.fire_key, self.grant.grant_id)).fetchone()
            if row is None or row['outcome'] == 'pending':
                return None
            if (row['outcome'] != 'created' or not row['task_id'] or row['execution_route'] is not None
                    or row['version'] != self.grant.version or row['scheduled_at'] != self.grant.scheduled_at):
                raise DispatchError('agent firing was rejected or changed')
            self._validate_template(json.loads(row['task_template']))
            return row['task_id']

    def _verify_task(self, task):
        self._validate_template(task.spec)
        if task.execution_route is not None or task.side_effect != 'external':
            raise DispatchError('agent task lacks isolated external side-effect policy')


def run_once(store, artifacts, grant, *, device_id, api_key, provider_call):
    """Run the due grant at most once and return the ledger task (or None)."""
    scheduler = AgentScheduler(store, grant)
    task_id = scheduler.tick()
    if task_id is None:
        return None
    task = store.get_task(task_id)
    scheduler._verify_task(task)
    if task.status != 'queued':
        return task

    try:
        with artifacts.path(grant.prompt_ref.hash).open('rb') as prompt_file:
            prompt_bytes = prompt_file.read(MAX_REQUEST_BYTES + 1)
        if len(prompt_bytes) > MAX_REQUEST_BYTES or sha256_ref(prompt_bytes) != grant.prompt_ref.hash:
            raise ValueError
        prompt = prompt_bytes.decode('utf-8')
    except (OSError, ValueError):
        raise DispatchError('approved agent prompt is unavailable or invalid') from None

    lease = store.claim(device_id, caps=(CAPABILITY,), kinds=(KIND,), task_id=task_id)
    if lease is None:
        return store.get_task(task_id)
    try:
        completion = provider_call(model=grant.model, approved_text=prompt, api_key=api_key,
                                   max_tokens=grant.max_tokens)
    except DeepSeekError as error:
        if error.outcome_unknown:
            store.mark_unknown(lease.lease_id, lease.fence, error.kind, device_id)
            return store.get_task(task_id)
        identity = {} if grant.execution_mode is None else {'execution_mode': grant.execution_mode, 'provider': grant.provider}
        outcome = Outcome('failed', {'error': error.kind, 'http_status': error.status, 'cost': None, **identity})
    else:
        artifact_hash = artifacts.put(completion.text.encode('utf-8'))
        identity = {} if grant.execution_mode is None else {'execution_mode': grant.execution_mode, 'provider': grant.provider}
        outcome = Outcome('failed' if completion.truncated else 'succeeded', {
            'response_id': completion.response_id, 'model': completion.model,
            **identity,
            'finish_reason': completion.finish_reason, 'truncated': completion.truncated,
            'usage': asdict(completion.usage), 'cost': None, 'result_ref': artifact_hash,
        }, (Evidence('agent_result', artifact_hash, 'local_only', artifact_hash=artifact_hash),))
    store.complete(lease.lease_id, lease.fence, outcome, device_id)
    return store.get_task(task_id)
