"""Bound the comparison's provider requests and synthetic tool receipts."""

from copy import deepcopy
import json
import re
from threading import Lock

from dispatch.sa_dispatch.artifacts import ArtifactStore
from dispatch.sa_dispatch.canonical import canonical_json, sha256_ref
from dispatch.sa_dispatch.models import IdempotencyConflict

from .fixture_tools import FixtureToolError, FixtureTools
from .hermes_host import MAX_TOOL_INPUT_BYTES, tool_schemas
from .request_ledger import COUNT_SCHEMA, LIMITS, MAX_OUTPUT_PER_CALL, RequestLedger, RequestLedgerError


MODEL = 'deepseek-flash'
MAX_PAYLOAD_BYTES = 128 * 1024
MAX_TEXT_BYTES = 32 * 1024
MAX_TOOLS_PER_TURN = 4
ARM_NAMES = {'worker': 'worker', 'current_worker': 'worker', 'hermes': 'hermes'}
FIXTURE_ARMS = {'worker': 'current_worker', 'hermes': 'hermes'}
TOOL_NAMES = {'knowledge_search': 'knowledge.search', 'knowledge_get': 'knowledge.get'}
ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}')
SHA = re.compile(r'[a-f0-9]{64}')


class BrokerError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _check(condition, code='REQUEST_REFUSED'):
    if not condition:
        raise BrokerError(code)


def _integer(value, lower, upper):
    return type(value) is int and lower <= value <= upper


def _text(value, *, limit=MAX_TEXT_BYTES, empty=False):
    _check(isinstance(value, str) and (empty or bool(value.strip())) and len(value.encode('utf-8')) <= limit)
    return value


def _identifier(value):
    _check(isinstance(value, str) and ID.fullmatch(value))
    return value


def _strict_json(text, *, limit):
    _text(text, limit=limit)

    def pairs(items):
        doc = dict(items)
        _check(len(items) == len(doc))
        return doc

    value = json.loads(text, object_pairs_hook=pairs)
    _check(type(value) is dict)
    canonical_json(value)
    return value


def _tool_calls(value):
    _check(type(value) is list and 1 <= len(value) <= MAX_TOOLS_PER_TURN, 'RESPONSE_REFUSED')
    calls = []
    seen = set()
    required = {'id', 'type', 'function'}
    for position, call in enumerate(value):
        _check(type(call) is dict and required <= set(call) <= required | {'index'} and call['type'] == 'function'
               and ('index' not in call or (type(call['index']) is int and call['index'] == position)))
        identifier = _identifier(call['id'])
        function = call['function']
        _check(identifier not in seen and type(function) is dict and set(function) == {'name', 'arguments'}
               and function['name'] in TOOL_NAMES)
        args = _strict_json(function['arguments'], limit=MAX_TOOL_INPUT_BYTES)
        calls.append({'id': identifier, 'type': 'function',
                      'function': {'name': function['name'], 'arguments': canonical_json(args).decode('utf-8')}})
        seen.add(identifier)
    return calls


def _assistant(message, *, allow_hermes_pad=False):
    _check(type(message) is dict and message.get('role') == 'assistant'
           and set(message) <= {'role', 'content', 'reasoning_content', 'tool_calls'})
    calls = message.get('tool_calls')
    reasoning = message.get('reasoning_content')
    # Pinned Hermes pads DeepSeek tool-call history with exactly one ASCII
    # space. Consume this wire marker locally; our upstream thinking is off.
    _check(reasoning in (None, '') or (allow_hermes_pad and calls is not None and reasoning == ' '))
    content = message.get('content')
    if calls is None:
        return {'role': 'assistant', 'content': _text(content)}
    return {'role': 'assistant', 'content': _text('' if content is None else content, empty=True),
            'tool_calls': _tool_calls(calls)}


def _message(message, *, allow_hermes_pad=False):
    _check(type(message) is dict)
    role = message.get('role')
    if role == 'assistant':
        return _assistant(message, allow_hermes_pad=allow_hermes_pad)
    if role == 'tool':
        _check({'role', 'tool_call_id', 'content'} <= set(message) <= {'role', 'name', 'tool_call_id', 'content'}
               and message.get('name') in (None, *TOOL_NAMES))
        result = _strict_json(message['content'], limit=MAX_TEXT_BYTES)
        _check(canonical_json(result).decode('utf-8') == message['content'])
        return dict(message, tool_call_id=_identifier(message['tool_call_id']))
    _check(role in ('system', 'user') and set(message) == {'role', 'content'})
    return {'role': role, 'content': _text(message['content'])}


def _sanitize(payload, tools_enabled, *, allow_hermes_pad=False):
    _check(type(payload) is dict and {'model', 'messages', 'max_tokens'} <= set(payload)
           <= {'model', 'messages', 'max_tokens', 'tools', 'stream', 'think', 'thinking'}
           and payload['model'] == MODEL
           and payload.get('stream', False) is False and payload.get('think', False) is False
           and payload.get('thinking', {'type': 'disabled'}) == {'type': 'disabled'}
           and _integer(payload['max_tokens'], 1, MAX_OUTPUT_PER_CALL)
           and type(payload['messages']) is list and 1 <= len(payload['messages']) <= 48)
    expected = [{'type': 'function', 'function': schema} for schema in tool_schemas()] if tools_enabled else []
    actual = payload.get('tools', [])
    _check(type(actual) is list and len(actual) == len(expected)
           and all(any(slot == tool for slot in actual) for tool in expected), 'TOOL_SCHEMA_REFUSED')
    clean = {'model': MODEL, 'messages': [_message(item, allow_hermes_pad=allow_hermes_pad)
                                         for item in payload['messages']],
             'max_tokens': payload['max_tokens'], 'stream': False, 'thinking': {'type': 'disabled'}}
    if expected:
        clean['tools'] = expected
    encoded = canonical_json(clean)
    _check(len(encoded) <= MAX_PAYLOAD_BYTES)
    return clean, encoded


def _provider_response(raw, *, tools_enabled):
    _check(type(raw) is dict and raw.get('object') == 'chat.completion' and raw.get('model') == MODEL
           and _integer(raw.get('created'), 0, 2**63 - 1)
           and type(raw.get('choices')) is list and len(raw['choices']) == 1, 'RESPONSE_REFUSED')
    choice = raw['choices'][0]
    _check(type(choice) is dict and type(choice.get('index')) is int and choice['index'] == 0)
    finish, message = choice['finish_reason'], _assistant(choice['message'])
    _check(finish in ('stop', 'tool_calls', 'length')
           and (finish == 'tool_calls') == ('tool_calls' in message)
           and (finish != 'tool_calls' or tools_enabled), 'RESPONSE_REFUSED')
    usage = raw.get('usage')
    _check(type(usage) is dict and all(_integer(usage.get(key), 0, 2**31 - 1)
           for key in ('prompt_tokens', 'completion_tokens', 'total_tokens'))
           and usage['total_tokens'] == usage['prompt_tokens'] + usage['completion_tokens'], 'USAGE_UNVERIFIED')
    counts = {key: usage[key] for key in ('prompt_tokens', 'completion_tokens', 'total_tokens')}
    response = {'id': _identifier(raw['id']), 'object': 'chat.completion', 'created': raw['created'],
                'model': MODEL, 'choices': [{'index': 0, 'message': message, 'finish_reason': finish}], 'usage': counts}
    return response, {'input_tokens': counts['prompt_tokens'], 'output_tokens': counts['completion_tokens']}


def _model_reply(state, *, response=None, code=None):
    result = dict(state=state, response=response)
    if code is not None:
        result['code'] = code
    return result


def _settled_reply(state, response):
    if response['choices'][0]['finish_reason'] == 'length':
        return _model_reply(state, code='OUTPUT_TRUNCATED')
    return _model_reply(state, response=deepcopy(response))


def _tool_failure(code):
    return {'ok': False, 'error': {'code': code, 'retryable': False}}


class Broker:
    """Trusted construction point. A fresh process cannot resume old dialogue."""

    def __init__(self, ledger, artifacts, *, forward, count_proof, verify_count_proof,
                 mode='disabled', authorize_live=None):
        _check(isinstance(ledger, RequestLedger) and isinstance(artifacts, ArtifactStore)
               and mode in ('disabled', 'fixture', 'live')
               and all(callable(fn) for fn in (forward, count_proof, verify_count_proof))
               and (mode != 'live' or callable(authorize_live)), 'BROKER_CONFIGURATION_REFUSED')
        status = ledger.status()
        _check(status['model'] == MODEL, 'BROKER_CONFIGURATION_REFUSED')
        self.ledger, self.artifacts = ledger, artifacts
        self.experiment_id = status['experiment_id']
        self.mode, self.authorize_live = mode, authorize_live
        self.forward, self.count_proof, self.verify_count_proof = forward, count_proof, verify_count_proof
        self._runs, self._lock = {}, Lock()

    def new_run(self, *, arm, run_id, approved_prompt, tools_enabled, scenario='normal', audit=None):
        _check(arm in ARM_NAMES and type(tools_enabled) is bool and (audit is None or callable(audit)))
        arm = ARM_NAMES[arm]
        prompt = _text(approved_prompt)
        with self._lock:
            requests = self.ledger.status(arm)['arms'][arm]['requests']
            _check((arm, run_id) not in self._runs and all(row['run_id'] != run_id for row in requests),
                   'RUN_REPLAY_UNAVAILABLE')
            run = BrokerRun(self, arm, run_id, prompt, tools_enabled, scenario, audit)
            self._runs[(arm, run_id)] = run
            return run


class BrokerRun:
    """One task's approved prompt, transcript, tool provenance, and request sequence."""

    def __init__(self, broker, arm, run_id, prompt, tools_enabled, scenario, audit):
        self.broker, self.arm, self.run_id = broker, arm, run_id
        self.prompt, self.tools_enabled, self.scenario, self.audit = prompt, tools_enabled, scenario, audit
        self._lock = Lock()
        self._attempts, self._tools, self._tool_calls = {}, {}, {}
        self._fixture_seq = 0
        self._audit_sequence = None
        self.fixture = FixtureTools(experiment_id=broker.experiment_id, arm=FIXTURE_ARMS[arm], run_id=run_id,
                                    scenario=scenario, audit=self._fixture_audit if audit is not None else None)

    def _fixture_audit(self, event):
        self.audit(dict(event, sequence=self._audit_sequence))

    def _authorize(self):
        broker = self.broker
        _check(broker.mode != 'disabled', 'FORWARDING_DISABLED')
        if broker.mode == 'live':
            _check(broker.authorize_live(broker.experiment_id, self.arm, self.run_id) is True,
                   'LIVE_ACCOUNTING_NOT_AUTHORIZED')

    def _permission(self):
        _check(self.fixture.revalidate_refs() is True, 'REFERENCE_NOT_VISIBLE')

    def _next_request(self, sequence, messages):
        _check(sequence == len(self._attempts), 'SEQUENCE_CONFLICT')
        if sequence == 0:
            base = messages[1:] if messages[0]['role'] == 'system' else messages
            _check(base == [{'role': 'user', 'content': self.prompt}], 'PROMPT_BINDING_REFUSED')
            return
        prior = self._attempts[sequence - 1]
        _check(prior.get('response') is not None, 'REQUEST_OUTCOME_UNKNOWN')
        step = prior['response']['choices'][0]
        _check(step['finish_reason'] == 'tool_calls', 'RUN_ALREADY_FINISHED')
        assistant = step['message']
        prefix = prior['messages'] + [assistant]
        calls = {item['id']: item['function']['name'] for item in assistant['tool_calls']}
        tail = messages[len(prefix):]
        _check(messages[:len(prefix)] == prefix and len(tail) == len(calls), 'TRANSCRIPT_BINDING_REFUSED')
        remaining = set(calls)
        for item in tail:
            cid = item.get('tool_call_id')
            expected = self._tool_calls.get((sequence - 1, cid))
            _check(item['role'] == 'tool' and cid in remaining and expected is not None
                   and item.get('name', calls[cid]) == calls[cid] and item['content'] == expected,
                   'TRANSCRIPT_BINDING_REFUSED')
            remaining.remove(cid)

    def _count(self, exact, digest, sequence):
        # The ledger field names refer to local reservation limits. A live
        # estimator has no authority to attest the provider's tokenizer.
        scope = 'authorized_live_estimate' if self.broker.mode == 'live' else 'synthetic_fixture'
        candidate = self.broker.count_proof(exact, model=MODEL, scope=scope)
        _check(type(candidate) is dict and set(candidate) == {'input_upper_bound', 'method_sha256'}
               and _integer(candidate['input_upper_bound'], 1, LIMITS['input_tokens'])
               and isinstance(candidate['method_sha256'], str) and SHA.fullmatch(candidate['method_sha256']),
               'COUNT_PROOF_REFUSED')
        proof = dict(schema=COUNT_SCHEMA, model=MODEL, payload_hash=digest, **candidate)
        _check(self.broker.verify_count_proof(deepcopy(proof), exact, scope=scope, arm=self.arm, run_id=self.run_id,
                                             sequence=sequence) is True, 'COUNT_PROOF_REFUSED')
        return proof['input_upper_bound'], self.broker.artifacts.put(canonical_json(proof))

    def _reserve(self, sequence, digest, count, proof, maximum):
        return self.broker.ledger.reserve(arm=self.arm, run_id=self.run_id, sequence=sequence,
                                          payload_hash=digest, input_upper_bound=count,
                                          count_proof_ref=proof, max_output_tokens=maximum)

    def _unknown(self, record, reason):
        try:
            self.broker.ledger.mark_unknown(record['lease_id'], record['fence'], reason=reason)
        except Exception:
            # A stopped process or failed journal commit leaves the reservation
            # leased and still held. Restart recovery is explicit in the ledger.
            pass
        return _model_reply('unknown', code='REQUEST_OUTCOME_UNKNOWN')

    def _complete(self, sequence, payload):
        self._authorize()
        _check(_integer(sequence, 0, LIMITS['provider_requests'] - 1), 'SEQUENCE_CONFLICT')
        clean, exact = _sanitize(payload, self.tools_enabled, allow_hermes_pad=self.arm == 'hermes')
        digest = sha256_ref(exact)
        self._permission()
        prior = self._attempts.get(sequence)
        if prior is not None:
            _check(prior['digest'] == digest, 'SEQUENCE_CONFLICT')
            reserved = self._reserve(sequence, digest, prior['count'], prior['proof'], clean['max_tokens'])
            if reserved['state'] != 'cached':
                return _model_reply(reserved['state'], code='REQUEST_OUTCOME_UNKNOWN'
                                    if reserved['state'] == 'unknown' else 'REQUEST_INFLIGHT')
            _check(prior.get('response') is not None, 'CACHE_REFUSED')
            expected = canonical_json(prior['response'])
            with self.broker.artifacts.path(reserved['response_ref']).open('rb') as artifact:
                _check(artifact.read(len(expected) + 1) == expected, 'CACHE_REFUSED')
            self._permission()
            self._authorize()
            return _settled_reply('cached', prior['response'])
        self._next_request(sequence, clean['messages'])
        count, proof = self._count(exact, digest, sequence)
        self._authorize()
        self._permission()
        reserved = self._reserve(sequence, digest, count, proof, clean['max_tokens'])
        _check(reserved['state'] == 'reserved', 'RESERVATION_REFUSED')
        record = {**reserved, 'digest': digest, 'count': count, 'proof': proof, 'messages': clean['messages']}
        self._attempts[sequence] = record
        try:
            self._authorize()
            self.broker.ledger.mark_sent(record['lease_id'], record['fence'])
            self._permission()
            self._authorize()
            outcome = self.broker.forward(deepcopy(clean))
        except Exception:
            return self._unknown(record, 'provider_transport_error')
        try:
            response, usage = _provider_response(outcome, tools_enabled=self.tools_enabled)
            _check(usage['input_tokens'] <= count and usage['output_tokens'] <= clean['max_tokens'], 'BUDGET_OVERRUN')
            ref = self.broker.artifacts.put(canonical_json(response))
            self.broker.ledger.settle(record['lease_id'], record['fence'], response_ref=ref, usage=usage)
        except BrokerError as error:
            return self._unknown(record, 'budget_overrun' if error.code == 'BUDGET_OVERRUN' else 'unverified_usage')
        except Exception:
            return self._unknown(record, 'unverified_usage')
        record['response'] = response
        self._permission()
        self._authorize()
        return _settled_reply('settled', response)

    def complete(self, sequence, payload):
        if not self._lock.acquire(blocking=False):
            return _model_reply('inflight', code='REQUEST_INFLIGHT')
        try:
            return self._complete(sequence, payload)
        except BrokerError as error:
            return _model_reply('rejected', code=error.code)
        except IdempotencyConflict:
            return _model_reply('rejected', code='SEQUENCE_CONFLICT')
        except RequestLedgerError as error:
            code = {'E_COMPARE_BUDGET_EXHAUSTED': 'BUDGET_EXHAUSTED',
                    'E_COMPARE_ARM_BLOCKED': 'ARM_BLOCKED'}.get(error.code, 'LEDGER_REFUSED')
            return _model_reply('rejected', code=code)
        except Exception:
            return _model_reply('rejected', code='REQUEST_REFUSED')
        finally:
            self._lock.release()

    def _tool_audit_denial(self, sequence, name, digest, reply):
        if self.audit is not None:
            self.audit({'experiment_id': self.broker.experiment_id, 'arm': FIXTURE_ARMS[self.arm],
                        'run_id': self.run_id, 'scenario': self.scenario, 'sequence': sequence, 'tool': TOOL_NAMES[name],
                        'request_sha256': digest[7:], 'response_sha256': sha256_ref(canonical_json(reply))[7:],
                        'code': reply['error']['code'], 'requested_ref': None, 'delivered_refs': []})

    def _tool(self, sequence, name, args):
        self._authorize()
        self._permission()
        _check(self.tools_enabled and name in TOOL_NAMES and _integer(sequence, 0, 31) and type(args) is dict,
               'TOOL_CALL_REFUSED')
        encoded = canonical_json(args)
        _check(len(encoded) <= MAX_TOOL_INPUT_BYTES, 'TOOL_CALL_REFUSED')
        digest = sha256_ref(canonical_json([name, args]))
        if sequence in self._tools:
            previous, reply = self._tools[sequence]
            _check(previous == digest, 'SEQUENCE_CONFLICT')
            return deepcopy(reply)
        _check(sequence == len(self._tools) and self._attempts, 'SEQUENCE_CONFLICT')
        step = len(self._attempts) - 1
        record = self._attempts[step].get('response')
        _check(record is not None, 'TOOL_CALL_REFUSED')
        calls = record['choices'][0]['message'].get('tool_calls', ())
        keys = [(step, call['id']) for call in calls if call['function']['name'] == name
                and call['function']['arguments'].encode('utf-8') == encoded
                and (step, call['id']) not in self._tool_calls]
        if not keys:
            reply = _tool_failure('TOOL_CALL_REFUSED')
            self._tool_audit_denial(sequence, name, digest, reply)
            self._tools[sequence] = (digest, deepcopy(reply))
            return deepcopy(reply)
        self._audit_sequence = sequence
        try:
            reply = self.fixture.invoke(self._fixture_seq, TOOL_NAMES[name], args)
        except FixtureToolError as error:
            code = error.code if error.code in ('INVALID_REQUEST', 'REFERENCE_NOT_VISIBLE') else 'FIXTURE_UNAVAILABLE'
            reply = _tool_failure(code)
            self._tool_audit_denial(sequence, name, digest, reply)
        else:
            self._fixture_seq += 1
        finally:
            self._audit_sequence = None
        self._tools[sequence] = (digest, deepcopy(reply))
        self._tool_calls[keys[0]] = canonical_json(reply).decode('utf-8')
        return deepcopy(reply)

    def tool(self, sequence, name, args):
        with self._lock:
            try:
                return self._tool(sequence, name, args)
            except BrokerError as error:
                return _tool_failure(error.code)
            except Exception:
                return _tool_failure('TOOL_CHANNEL_UNAVAILABLE')
