"""Account-balance stop rule for the explicitly approved, small host comparison.

Balance deltas are account-wide, not attributable invoices. This is a stop rule
with headroom, not an upstream atomic monetary authorization.
"""

from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile


CAPS = {'CNY': Decimal('1'), 'USD': Decimal('0.15')}
SCHEMA = 'superagent.compare.money.v1'
APPROVAL = 'comparison-public-fixture-v1'
USAGE_FIELDS = ('prompt_tokens', 'completion_tokens', 'total_tokens')


class MoneyRefused(ValueError):
    pass


def _require(ok):
    if not ok:
        raise MoneyRefused('comparison_money_gate_closed')


def _balance(response):
    _require(type(response) is dict and response.get('is_available') is True)
    rows = response.get('balance_infos')
    _require(type(rows) is list and len(rows) == 1 and type(rows[0]) is dict)
    currency = rows[0].get('currency')
    _require(currency in CAPS)
    try:
        value = Decimal(rows[0]['total_balance'])
        _require(value.is_finite() and value >= 0)
    except (InvalidOperation, KeyError, TypeError):
        raise MoneyRefused('comparison_money_gate_closed') from None
    return currency, value


def _usage(response):
    usage = response.get('usage') if type(response) is dict else None
    _require(type(usage) is dict and all(type(usage.get(key)) is int and 0 <= usage[key] < 2**31
                                        for key in USAGE_FIELDS)
             and usage['total_tokens'] == usage['prompt_tokens'] + usage['completion_tokens'])
    return {key: usage[key] for key in USAGE_FIELDS}


def _write(path, value):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    fd, name = tempfile.mkstemp(prefix='.money-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


class MoneyGuard:
    """Caller holds one exclusive process lock for this entire operation."""

    def __init__(self, path, *, balance, forward, credential_id):
        self.path, self.balance, self.forward, self.credential_id = Path(path), balance, forward, credential_id

    def _load(self, current):
        currency, amount = current
        if self.path.exists():
            value = json.loads(self.path.read_bytes())
            _require(value['schema'] == SCHEMA and value['approval'] == APPROVAL
                     and value['credential_id'] == self.credential_id and value['currency'] == currency
                     and value['cap'] == str(CAPS[currency]) and value['state'] == 'ready')
            return value
        value = {'schema': SCHEMA, 'approval': APPROVAL, 'credential_id': self.credential_id,
                 'currency': currency, 'cap': str(CAPS[currency]), 'baseline': str(amount),
                 'last_balance': str(amount), 'observed_drop': '0', 'state': 'ready', 'calls': [],
                 'attribution': 'account_wide_delta_not_exclusive_request_cost'}
        _write(self.path, value)
        return value

    def perform(self, arm, payload):
        _require(arm in ('worker', 'hermes') and type(payload) is dict)
        before = _balance(self.balance())
        state = self._load(before)
        baseline, cap = Decimal(state['baseline']), CAPS[before[0]]
        # An increase indicates a top-up, grant change or account mismatch:
        # do not let it erase spend or silently extend this experiment.
        if before[1] > Decimal(state['last_balance']):
            state['state'] = 'closed_balance_increase'
            _write(self.path, state)
            raise MoneyRefused('comparison_money_gate_closed')
        spent = max(Decimal(state['observed_drop']), baseline - before[1])
        if spent >= cap * Decimal('0.8') or before[1] < cap * Decimal('0.2'):
            state.update(state='closed_budget', observed_drop=str(spent), last_balance=str(before[1]))
            _write(self.path, state)
            raise MoneyRefused('comparison_money_gate_closed')
        _require(sum(item['arm'] == arm for item in state['calls']) < 20)
        raw = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
        _require(len(raw) <= 128 * 1024 and payload.get('model') == 'deepseek-flash'
                 and type(payload.get('max_tokens')) is int and 1 <= payload['max_tokens'] <= 512
                 and payload.get('thinking') == {'type': 'disabled'} and payload.get('stream') is False)
        item = {'arm': arm, 'payload_sha256': sha256(raw).hexdigest(), 'before': str(before[1]), 'state': 'sent'}
        state['calls'].append(item)
        state.update(state='inflight', last_balance=str(before[1]), observed_drop=str(spent))
        _write(self.path, state)
        try:
            response = self.forward(payload)
            # Keep only metadata, never prompts/completions or the credential.
            usage = _usage(response)
            item['usage'] = usage
            _require(usage['completion_tokens'] <= payload['max_tokens'])
            after = _balance(self.balance())
            _require(after[0] == before[0] and after[1] <= before[1])
            spent = max(spent, baseline - after[1])
            item.update(state='returned', after=str(after[1]))
            state.update(state='closed_budget' if spent >= cap * Decimal('0.8') else 'ready',
                         last_balance=str(after[1]), observed_drop=str(spent))
            _write(self.path, state)
            return response
        except BaseException:
            state['state'] = 'closed_unknown'
            _write(self.path, state)
            raise
