"""Business independent immutable boundary objects."""
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

SideEffect = Literal['none', 'idempotent_write', 'external']
Access = Literal['synthetic', 'exportable', 'local_only']
Status = Literal['queued', 'waiting_device', 'leased', 'succeeded', 'no_new', 'failed', 'unknown', 'cancelled']

class DispatchError(Exception):
    code = 'E_VALIDATION'
    exit_code = 2
    retryable = False

class FenceRejected(DispatchError):
    code = 'E_FENCE_REJECTED'
    exit_code = 4

class IdempotencyConflict(DispatchError):
    code = 'E_IDEMPOTENCY_CONFLICT'
    exit_code = 3

class DuplicateInflight(DispatchError):
    code = 'E_DUPLICATE_INFLIGHT'
    exit_code = 3

class InvalidTransition(DispatchError):
    code = 'E_INVALID_TRANSITION'

@dataclass(frozen=True)
class Ref:
    owner: str
    kind: str
    id: str
    version: str
    hash: str

@dataclass(frozen=True)
class TaskSpec:
    idem_key: str
    kind: str
    kind_version: int
    params: dict
    input_refs: tuple[Ref, ...]
    required_caps: tuple[str, ...]
    access: Access
    egress: str
    max_attempts: int
    track: str | None = None
    parent_task_id: str | None = None

    @classmethod
    def from_dict(cls, data):
        value = dict(data)
        value['input_refs'] = tuple(r if isinstance(r, Ref) else Ref(**r) for r in value.get('input_refs', ()))
        value['required_caps'] = tuple(value.get('required_caps', ()))
        return cls(**value)

@dataclass(frozen=True)
class Lease:
    lease_id: str
    task_id: str
    fence: int
    device_id: str
    expires_at: str

@dataclass(frozen=True)
class Evidence:
    kind: str
    content_hash: str
    access: Access
    ref: Ref | None = None
    artifact_hash: str | None = None

@dataclass(frozen=True)
class Outcome:
    status: str = 'succeeded'
    result: dict = field(default_factory=dict)
    evidence: tuple[Evidence, ...] = ()

    @classmethod
    def from_dict(cls, data):
        value = dict(data)
        items = []
        for item in value.get('evidence', ()):
            if isinstance(item, Evidence): items.append(item); continue
            item = dict(item)
            if item.get('ref') is not None and not isinstance(item['ref'], Ref): item['ref'] = Ref(**item['ref'])
            items.append(Evidence(**item))
        value['evidence'] = tuple(items)
        return cls(**value)

class TaskRow(dict):
    @property
    def spec(self):
        return TaskSpec.from_dict({name: self[name] for name in TaskSpec.__dataclass_fields__})

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def to_dict(self):
        return dict(self)

@dataclass(frozen=True)
class SweepReport:
    expired: int = 0
    requeued: int = 0
    unknown: int = 0
    failed: int = 0
    waiting_device: int = 0
    queued: int = 0

    def to_dict(self):
        return asdict(self)
