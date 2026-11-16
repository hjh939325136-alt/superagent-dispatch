"""Run at most one authorized provider request for the requested campaign role."""

from dataclasses import asdict
import re
import sqlite3

from sa_dispatch.campaign_validation import DEFINITE_HTTP_ERRORS, DEVICES, validate_result
from sa_dispatch.canonical import sha256_ref
from sa_dispatch.models import DispatchError, Evidence, FenceRejected, Outcome

from .campaign_control import _CampaignStore
from .common_worker import DEADLINE_SECONDS, THINKING, UNKNOWN_ERRORS
from .deepseek_client import Completion, DeepSeekError, MAX_RESPONSE_BYTES


def _unknown(store, role, lease, kind):
    reason = kind if isinstance(kind, str) and kind in UNKNOWN_ERRORS else 'invalid_response'
    try:
        store.mark_unknown(lease.lease_id, lease.fence, reason, DEVICES[role])
    except FenceRejected:
        store.prepare(role)
    return store.status()


def _provider_outcome(store, completion, key, base):
    if not isinstance(completion, Completion):
        raise DispatchError('invalid campaign provider response')
    fields = (completion.response_id, completion.model, completion.text)
    if any(not isinstance(field, str) or key in field for field in fields):
        raise DispatchError('invalid campaign provider response')
    content = completion.text.encode('utf-8')
    if len(content) > MAX_RESPONSE_BYTES or not completion.text.strip():
        raise DispatchError('invalid campaign provider output bounds')
    ref = sha256_ref(content)
    status = 'failed' if completion.truncated else 'succeeded'
    result = base | {
        'model': completion.model, 'response_id': completion.response_id,
        'finish_reason': completion.finish_reason, 'truncated': completion.truncated,
        'usage': asdict(completion.usage), 'result_ref': ref,
    }
    validate_result(store.config, status, result)
    store._put(content)
    return Outcome(status, result, (Evidence('agent_result', ref, 'local_only', artifact_hash=ref),))


def tick(ledger, artifacts, config, role, credential_reader, provider_call, clock=None):
    if not isinstance(role, str) or role not in DEVICES:
        raise DispatchError('invalid agent campaign role')
    store = _CampaignStore(ledger, artifacts, config, clock=clock)
    slot = store.prepare(role)
    if slot is None:
        return store.status()
    key = credential_reader()
    if not isinstance(key, str) or not re.fullmatch(r'[A-Za-z0-9._~+/=-]{1,1024}', key):
        raise DispatchError('agent campaign credential unavailable or invalid')
    claimed = store.claim_slot(role, slot, key)
    if claimed is None:
        return store.status()
    lease, approved_text = claimed

    base = {'execution_mode': 'live', 'provider': 'deepseek', 'cost': None}
    try:
        completion = provider_call(model=store.config['model'], approved_text=approved_text, api_key=key,
                                   max_tokens=store.config['max_tokens'], deadline_seconds=DEADLINE_SECONDS,
                                   thinking=THINKING)
    except DeepSeekError as error:
        if (error.outcome_unknown or not isinstance(error.kind, str) or type(error.status) is not int
                or error.status not in DEFINITE_HTTP_ERRORS.get(error.kind, ())):
            return _unknown(store, role, lease, error.kind)
        outcome = Outcome('failed', base | {'model': store.config['model'], 'error': error.kind,
                                            'http_status': error.status})
    except Exception:
        # A provider adapter exception cannot establish whether a request was sent.
        return _unknown(store, role, lease, 'provider_process_failed')
    else:
        try:
            outcome = _provider_outcome(store, completion, key, base)
        except (DispatchError, ValueError, TypeError, OSError):
            return _unknown(store, role, lease, 'invalid_response')
    try:
        try:
            store.complete(lease.lease_id, lease.fence, outcome, DEVICES[role])
        except sqlite3.OperationalError:
            store.complete(lease.lease_id, lease.fence, outcome, DEVICES[role])
    except FenceRejected:
        store.prepare(role)
    return store.status()
