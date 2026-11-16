"""Transactional external-request accounting for one isolated comparison experiment."""

from contextlib import closing, contextmanager
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat

from dispatch.sa_dispatch.artifacts import ArtifactStore
from dispatch.sa_dispatch.canonical import canonical_json, sha256_ref
from dispatch.sa_dispatch.clock import Clock
from dispatch.sa_dispatch.control_policy import _budget_config, _event, _write_budget, expire_reservation
from dispatch.sa_dispatch.models import Evidence, FenceRejected, IdempotencyConflict, Outcome, TaskSpec
from dispatch.sa_dispatch.routing import ledger_mode
from dispatch.sa_dispatch.store_sqlite import SqliteStore


ARMS = ('worker', 'hermes')
LIMITS = {'provider_requests': 20, 'input_tokens': 32_000, 'output_tokens': 8_000}
MAX_OUTPUT_PER_CALL = 512
MAX_ARTIFACT = 256 * 1024
KIND = 'comparison.provider_request'
ACTOR = 'comparison-broker'
GENESIS_KEY = 'compare:experiment_registered'
EXPERIMENT_SCHEMA = 'superagent.compare.experiment.v1'
COUNT_SCHEMA = 'superagent.compare.count_proof.v1'
RESERVE_SCHEMA = 'superagent.compare.request_reservation.v1'
USAGE_SCHEMA = 'superagent.compare.request_usage.v1'
TRUSTED_RECEIPT_SCHEMA = 'superagent.compare.trusted_provider_receipt.v1'
END = '9999-12-31T23:59:59.999999+00:00'
ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,63}')
REF = re.compile(r'sha256:[a-f0-9]{64}')
SHA = re.compile(r'[a-f0-9]{64}')
LEASE_ID = re.compile(r'l_[a-f0-9]{32}')
TASK_ID = re.compile(r't_[a-f0-9]{32}')
UNKNOWN_REASONS = ('provider_outcome_unknown', 'process_interrupted', 'lease_expired', 'unverified_usage',
                   'provider_transport_error', 'budget_overrun')


class RequestLedgerError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _require(condition, code='E_COMPARE_INTEGRITY'):
    if not condition:
        raise RequestLedgerError(code)


def _matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _digest_field(value):
    _require(_matches(REF, value), 'E_COMPARE_INVALID_REF')
    return value


def _arm(value):
    _require(value in ARMS, 'E_COMPARE_INVALID_ARM')
    return value


def _number(value, minimum, maximum):
    return type(value) is int and minimum <= value <= maximum


def _document(raw):
    value = json.loads(raw)
    _require(type(value) is dict and canonical_json(value) == raw)
    return value


def _identity(path):
    info = path.lstat()
    _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and path.resolve() == path,
             'E_COMPARE_UNSAFE_LEDGER')
    return info.st_dev, info.st_ino


def _event_key(task_id, event):
    return f'compare:{event}:{task_id}:1'


class _Store(SqliteStore):
    """Open an existing isolated store without applying migrations on read."""

    def __init__(self, path, clock, lease_ttl_seconds):
        self.path = path
        self.clock = clock
        self.lease_ttl_seconds = lease_ttl_seconds
        self._route = None
        self.kinds = None
        self.component_config = None


class RequestLedger:
    def __init__(self, ledger_path, artifacts, experiment_id, *, count_method_sha256, model='deepseek-flash', create=False,
                 clock=None, lease_ttl_seconds=120):
        _require(_matches(ID, experiment_id) and _matches(ID, model) and isinstance(artifacts, ArtifactStore)
                 and _matches(SHA, count_method_sha256) and type(create) is bool
                 and _number(lease_ttl_seconds, 1, 300), 'E_COMPARE_INVALID_EXPERIMENT')
        path = Path(ledger_path).absolute()
        _require(path.resolve() == path and path.parent.is_dir(), 'E_COMPARE_UNSAFE_LEDGER')
        self._store = _Store(path, clock or Clock(), lease_ttl_seconds)
        self._artifacts = artifacts
        self._experiment = experiment_id
        self._model = model
        # Identify the approved counting policy; this is not a claim that the
        # policy proves a commercial provider's remote token-count upper bound.
        self._count_method_sha256 = count_method_sha256
        if not os.path.lexists(path):
            _require(create, 'E_COMPARE_LEDGER_MISSING')
            self._initialize()
        self._file_identity = _identity(path)
        with self._read() as conn:
            self._validate(conn)

    def _config(self):
        return {'schema': EXPERIMENT_SCHEMA, 'experiment_id': self._experiment, 'model': self._model,
                'count_method_sha256': self._count_method_sha256,
                'lease_ttl_seconds': self._store.lease_ttl_seconds, 'arms': list(ARMS),
                'limits_per_arm': LIMITS, 'max_output_per_call': MAX_OUTPUT_PER_CALL}

    def _initialize(self):
        path, store = self._store.path, self._store
        # Reserve a fresh pathname without ever attaching to an existing store.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        os.close(fd)
        SqliteStore(path, clock=store.clock, lease_ttl_seconds=store.lease_ttl_seconds)
        with store._tx(execution=True) as conn:
            now = store._now()
            config_ref = self._artifacts.put(canonical_json(self._config()))
            _event(conn, GENESIS_KEY, 'compare.experiment_registered', ACTOR, now, refs=(config_ref,), required=True)
            for arm in ARMS:
                for unit, limit in LIMITS.items():
                    config = _budget_config(self._budget_id(arm, unit), period='experiment_lifetime', unit=unit,
                                            limit_units=limit, starts_at=now, ends_at=END)
                    _write_budget(conn, config, None)

    @contextmanager
    def _read(self):
        _require(_identity(self._store.path) == self._file_identity, 'E_COMPARE_LEDGER_IDENTITY')
        with closing(self._store._connect()) as conn:
            conn.execute('BEGIN')
            _require(_identity(self._store.path) == self._file_identity, 'E_COMPARE_LEDGER_IDENTITY')
            yield conn

    @contextmanager
    def _write(self):
        _require(_identity(self._store.path) == self._file_identity, 'E_COMPARE_LEDGER_IDENTITY')
        with self._store._tx(execution=True) as conn:
            _require(_identity(self._store.path) == self._file_identity, 'E_COMPARE_LEDGER_IDENTITY')
            yield conn, self._validate(conn)

    def _artifact(self, ref, *, limit=MAX_ARTIFACT):
        path = self._artifacts.path(_digest_field(ref))
        with path.open('rb') as handle:
            raw = handle.read(limit + 1)
        _require(len(raw) <= limit and sha256_ref(raw) == ref, 'E_COMPARE_ARTIFACT_INVALID')
        return raw

    def _validate(self, conn):
        _require(ledger_mode(conn) == 'isolated' and conn.execute(
            'SELECT MAX(version) FROM schema_migrations').fetchone()[0] == '0012', 'E_COMPARE_NOT_ISOLATED')
        genesis = conn.execute('SELECT * FROM control_events WHERE event_key=?', (GENESIS_KEY,)).fetchone()
        _require(genesis is not None and (genesis['kind'], genesis['actor'], genesis['task_id'], genesis['fence']) ==
                 ('compare.experiment_registered', ACTOR, None, None), 'E_COMPARE_NOT_EXPERIMENT')
        refs = json.loads(genesis['refs_json'])
        _require(type(refs) is list and len(refs) == 1 and
                 self._artifact(refs[0], limit=4096) == canonical_json(self._config()), 'E_COMPARE_EXPERIMENT_MISMATCH')
        return genesis['at']

    def _budget_id(self, arm, unit):
        return f'compare:{self._experiment}:{arm}:{unit}'

    def _track(self, arm):
        return f'compare:{self._experiment}:{arm}'

    def _task_key(self, arm, run_id, sequence):
        return f'{self._track(arm)}:{run_id}:{sequence}'

    def _vector(self, input_tokens, output_tokens):
        return {'provider_requests': 1, 'input_tokens': input_tokens, 'output_tokens': output_tokens}

    def _budget_rows(self, conn, arm, starts_at):
        budgets = {}
        for unit, limit in LIMITS.items():
            budget_id = self._budget_id(arm, unit)
            row = conn.execute('SELECT * FROM control_budgets WHERE budget_id=?', (budget_id,)).fetchone()
            _require(row is not None and (row['budget_id'], row['unit'], row['period'], row['starts_at'],
                                          row['ends_at'], row['limit_units']) ==
                     (budget_id, unit, 'experiment_lifetime', starts_at, END, limit)
                     and 0 <= row['used_units'] and 0 <= row['reserved_units']
                     and row['used_units'] + row['reserved_units'] <= limit)
            budgets[unit] = row
        return budgets

    def _event_ref(self, conn, task_id, event, *, optional=False):
        row = conn.execute('SELECT * FROM control_events WHERE event_key=?', (_event_key(task_id, event),)).fetchone()
        if optional and row is None:
            return None
        _require(row is not None and (row['kind'], row['task_id'], row['fence'], row['actor']) ==
                 ('compare.request_' + event, task_id, 1, ACTOR))
        refs = json.loads(row['refs_json'])
        _require(type(refs) is list and len(refs) == 1)
        return _digest_field(refs[0])

    def _reservation(self, conn, task):
        task_id = task['task_id']
        ref = self._event_ref(conn, task_id, 'reserved')
        document = _document(self._artifact(ref, limit=8192))
        params = json.loads(task['params_json'])
        arm = params['arm']
        vector = self._vector(params['input_upper_bound'], params['max_output_tokens'])
        held = conn.execute('SELECT * FROM control_reservations WHERE task_id=? AND fence=1', (task_id,)).fetchone()
        _require(document == {**params, 'schema': RESERVE_SCHEMA, 'task_id': task_id, 'fence': 1, 'vector': vector}
                 and held is not None and (held['budget_id'], held['fingerprint'], held['reserved_units']) ==
                 (self._budget_id(arm, 'provider_requests'), task['fingerprint'], 1))
        return ref, document, held

    def _usage(self, conn, task, reserved, held):
        ref = self._event_ref(conn, task['task_id'], 'settled')
        document = _document(self._artifact(ref, limit=8192))
        vector = document['actual']
        response_ref = _digest_field(document['response_ref'])
        expected = {'schema': USAGE_SCHEMA, 'reservation_ref': self._event_ref(conn, task['task_id'], 'reserved'),
                    'task_id': task['task_id'], 'fence': 1, 'response_ref': response_ref, 'actual': vector}
        reconciled = self._event_ref(conn, task['task_id'], 'reconciled', optional=True)
        if reconciled is not None:
            expected['reconciliation_ref'] = reconciled
            self._artifact(reconciled, limit=8192)
        _require(document == expected)
        _require(set(vector) == set(LIMITS) and vector['provider_requests'] == 1
                 and all(_number(actual, 0, reserved['vector'][unit]) for unit, actual in vector.items())
                 and (held['state'], held['actual_units'], held['evidence_ref']) == ('settled', 1, ref)
                 and json.loads(task['result_json']) == {'response_ref': response_ref, 'usage_ref': ref})
        return ref, document

    def _tasks(self, conn, arm):
        rows = conn.execute('SELECT * FROM tasks WHERE track=? ORDER BY created_at,task_id LIMIT ?',
                            (self._track(arm), LIMITS['provider_requests'] + 1)).fetchall()
        _require(len(rows) <= LIMITS['provider_requests'])
        return rows

    def _arm_snapshot(self, conn, arm, starts_at):
        budgets = self._budget_rows(conn, arm, starts_at)
        used, reserved = dict.fromkeys(LIMITS, 0), dict.fromkeys(LIMITS, 0)
        requests = []
        for task in self._tasks(conn, arm):
            _require((task['kind'], task['kind_version'], task['execution_route'], task['access'],
                      task['egress'], task['side_effect'], task['attempts'], task['max_attempts'], task['fence']) ==
                     (KIND, 1, None, 'exportable', 'derived', 'external', 1, 1, 1))
            reservation_ref, intent, held = self._reservation(conn, task)
            _require(intent['arm'] == arm and intent['experiment_id'] == self._experiment
                     and intent['model'] == self._model)
            state = task['status']
            lease = conn.execute('SELECT * FROM leases WHERE task_id=? LIMIT 2', (task['task_id'],)).fetchall()
            expected_lease = {'succeeded': 'completed', 'leased': 'active', 'unknown': 'unknown'}
            _require(state in expected_lease and len(lease) == 1
                     and (lease[0]['fence'], lease[0]['device_id'], lease[0]['state']) ==
                     (1, ACTOR, expected_lease[state]))
            sent = self._event_ref(conn, task['task_id'], 'sent', optional=True)
            _require(sent in (None, reservation_ref) and (state != 'succeeded' or sent == reservation_ref))
            entry = {'task_id': task['task_id'], 'run_id': intent['run_id'], 'sequence': intent['sequence'],
                     'state': state, 'fence': 1, 'reservation_ref': reservation_ref}
            if state == 'succeeded':
                ref, receipt = self._usage(conn, task, intent, held)
                vector, target = receipt['actual'], used
                entry.update(response_ref=receipt['response_ref'], usage_ref=ref)
                if 'reconciliation_ref' in receipt:
                    entry['reconciliation_ref'] = receipt['reconciliation_ref']
            else:
                _require(state in ('leased', 'unknown') and held['state'] == (
                    'uncertain' if state == 'unknown' else 'reserved'))
                if state == 'unknown':
                    _require(self._event_ref(conn, task['task_id'], 'unknown') == reservation_ref)
                vector, target = intent['vector'], reserved
            for unit, amount in vector.items():
                target[unit] += amount
            requests.append(entry)
        _require(sum(row['state'] in ('leased', 'unknown') for row in requests) <= 1)
        for unit, row in budgets.items():
            _require((used[unit], reserved[unit]) == (row['used_units'], row['reserved_units']))
        states = {row['state'] for row in requests}
        return {'state': 'closed_unknown' if 'unknown' in states else 'inflight' if 'leased' in states else 'ready',
                'budgets': {key: {'budget_id': row['budget_id'], 'limit_units': row['limit_units'],
                                  'used_units': row['used_units'], 'held_units': row['reserved_units']}
                            for key, row in budgets.items()},
                'requests': requests}

    def status(self, arm=None):
        """Read and reconcile the existing receipts and budget vectors."""
        arms = ARMS if arm is None else (_arm(arm),)
        with self._read() as conn:
            starts_at = self._validate(conn)
            return {'experiment_id': self._experiment, 'model': self._model,
                    'arms': {member: self._arm_snapshot(conn, member, starts_at) for member in arms}}

    def reserve(self, *, arm, run_id, sequence, payload_hash, input_upper_bound, count_proof_ref, max_output_tokens):
        _arm(arm)
        _require(_matches(ID, run_id) and _number(sequence, 0, 2**31 - 1)
                 and _number(input_upper_bound, 1, LIMITS['input_tokens'])
                 and _number(max_output_tokens, 1, MAX_OUTPUT_PER_CALL), 'E_COMPARE_INVALID_RESERVATION')
        _digest_field(payload_hash)
        proof = _document(self._artifact(count_proof_ref, limit=4096))
        _require(proof == {'schema': COUNT_SCHEMA, 'payload_hash': payload_hash, 'model': self._model,
                           'input_upper_bound': input_upper_bound, 'method_sha256': self._count_method_sha256}
                 and type(proof['input_upper_bound']) is int,
                 'E_COMPARE_COUNT_PROOF')
        params = {'experiment_id': self._experiment, 'arm': arm, 'run_id': run_id, 'sequence': sequence,
                  'payload_hash': payload_hash, 'model': self._model, 'input_upper_bound': input_upper_bound,
                  'count_proof_ref': count_proof_ref, 'max_output_tokens': max_output_tokens}
        store = self._store
        key = self._task_key(arm, run_id, sequence)
        with self._write() as (conn, starts_at):
            existing = conn.execute('SELECT * FROM tasks WHERE idem_key=?', (key,)).fetchone()
            now = store._now()
            state = self._arm_snapshot(conn, arm, starts_at)
            if existing is not None:
                if json.loads(existing['params_json']) != params:
                    raise IdempotencyConflict('comparison request sequence has different content')
                row = next(item for item in state['requests'] if item['task_id'] == existing['task_id'])
                if row['state'] == 'succeeded':
                    self._artifact(row['response_ref'])
                    return {'state': 'cached', 'task_id': row['task_id'], 'fence': 1,
                            'response_ref': row['response_ref'], 'usage_ref': row['usage_ref']}
                return {'state': 'unknown' if row['state'] == 'unknown' else 'inflight',
                        'task_id': row['task_id'], 'fence': 1}
            _require(state['state'] == 'ready', 'E_COMPARE_ARM_BLOCKED')
            _require(starts_at <= now < END, 'E_COMPARE_CLOCK_OUTSIDE_EXPERIMENT')
            vector = self._vector(input_upper_bound, max_output_tokens)
            for unit, held in vector.items():
                changed = conn.execute('''UPDATE control_budgets SET reserved_units=reserved_units+?
                    WHERE budget_id=? AND used_units+reserved_units+?<=limit_units''',
                    (held, self._budget_id(arm, unit), held)).rowcount
                _require(changed == 1, 'E_COMPARE_BUDGET_EXHAUSTED')
            spec = TaskSpec(key, KIND, 1, params, (), (), 'exportable', 'derived', 1, track=self._track(arm))
            task, _ = store._submit(spec, side_effect='external', scope={'experiment_id': self._experiment, 'arm': arm},
                                    conn=conn)
            lease = store._grant_lease(conn, task, ACTOR)
            reservation_ref = self._artifacts.put(canonical_json(
                {**params, 'schema': RESERVE_SCHEMA, 'task_id': task['task_id'], 'fence': 1, 'vector': vector}))
            conn.execute('''INSERT INTO control_reservations(task_id,fence,budget_id,fingerprint,reserved_units,
                state,created_at) VALUES(?,1,?,?,1,'reserved',?)''',
                (task['task_id'], self._budget_id(arm, 'provider_requests'), task['fingerprint'], now))
            _event(conn, _event_key(task['task_id'], 'reserved'), 'compare.request_reserved', ACTOR, now,
                   task_id=task['task_id'], fence=1, refs=(reservation_ref,), required=True)
            return {'state': 'reserved', 'task_id': lease.task_id, 'lease_id': lease.lease_id, 'fence': lease.fence,
                    'expires_at': lease.expires_at, 'reservation_ref': reservation_ref}

    def _owned(self, conn, lease_id, fence):
        if not _matches(LEASE_ID, lease_id) or type(fence) is not int or fence != 1:
            raise FenceRejected('comparison lease/fence is invalid')
        lease, task, _ = self._store._active(conn, lease_id, fence, ACTOR)
        if (lease is None or task is None or (lease['device_id'], lease['fence'], task['fence'], task['kind']) !=
                (ACTOR, fence, fence, KIND)):
            raise FenceRejected('comparison lease/fence is not owned')
        params = json.loads(task['params_json'])
        _require(params['experiment_id'] == self._experiment and task['track'] == self._track(_arm(params['arm'])))
        return lease, task

    def _unexpired(self, lease, task):
        if not (lease['state'] == 'active' and task['status'] == 'leased'
                and lease['granted_at'] <= self._store._now() < lease['expires_at']):
            raise FenceRejected('comparison lease is not current and unexpired')

    def mark_sent(self, lease_id, fence):
        """Spend the only permission to hand this reservation to the transport."""
        with self._write() as (conn, _):
            lease, task = self._owned(conn, lease_id, fence)
            self._unexpired(lease, task)
            tid = task['task_id']
            _require(self._event_ref(conn, tid, 'sent', optional=True) is None, 'E_COMPARE_ALREADY_SENT')
            ref, _, held = self._reservation(conn, task)
            _require(held['state'] == 'reserved')
            self._unexpired(lease, task)
            _event(conn, _event_key(tid, 'sent'), 'compare.request_sent', ACTOR, self._store._now(),
                   task_id=tid, fence=1, refs=(ref,), required=True)
            return {'state': 'sent', 'task_id': tid, 'fence': 1}

    def settle(self, lease_id, fence, *, response_ref, usage):
        _require(type(usage) is dict and set(usage) == {'input_tokens', 'output_tokens'}
                 and all(type(value) is int and value >= 0 for value in usage.values()), 'E_COMPARE_USAGE_INVALID')
        actual = self._vector(usage['input_tokens'], usage['output_tokens'])
        with self._write() as (conn, starts_at):
            lease, task = self._owned(conn, lease_id, fence)
            if task['status'] != 'succeeded':
                self._unexpired(lease, task)
            tid = task['task_id']
            reserve_ref, reservation, held = self._reservation(conn, task)
            _require(self._event_ref(conn, tid, 'sent', optional=True) == reserve_ref, 'E_COMPARE_NOT_SENT')
            _require(all(value <= reservation['vector'][unit] for unit, value in actual.items()),
                     'E_COMPARE_USAGE_EXCEEDS_RESERVED')
            self._artifact(response_ref)
            receipt = {'schema': USAGE_SCHEMA, 'reservation_ref': reserve_ref, 'task_id': tid, 'fence': 1,
                       'response_ref': response_ref, 'actual': actual}
            if task['status'] == 'succeeded':
                old_ref, old = self._usage(conn, task, reservation, held)
                if old != receipt:
                    raise IdempotencyConflict('comparison usage receipt is immutable')
                return {'state': 'cached', 'task_id': tid, 'fence': 1, 'response_ref': response_ref, 'usage_ref': old_ref}
            _require(held['state'] == 'reserved')
            self._unexpired(lease, task)
            return self._commit_settlement(conn, starts_at, lease, task, reservation, receipt)

    def _commit_settlement(self, conn, starts_at, lease, task, reservation, receipt):
        store = self._store
        tid, now = task['task_id'], store._now()
        reconciled = receipt.get('reconciliation_ref')
        reservation_state, lease_state = ('uncertain', 'unknown') if reconciled else ('reserved', 'active')
        self._budget_rows(conn, reservation['arm'], starts_at)
        usage_ref = self._artifacts.put(canonical_json(receipt))
        for unit, value in receipt['actual'].items():
            funded = reservation['vector'][unit]
            changed = conn.execute('''UPDATE control_budgets SET reserved_units=reserved_units-?,
                used_units=used_units+? WHERE budget_id=? AND reserved_units>=?''',
                (funded, value, self._budget_id(reservation['arm'], unit), funded)).rowcount
            _require(changed == 1)
        changed = conn.execute('''UPDATE control_reservations SET state='settled',actual_units=1,
            evidence_ref=?,settled_at=? WHERE task_id=? AND fence=1 AND state=?''',
            (usage_ref, now, tid, reservation_state)).rowcount
        _require(changed == 1)
        response_ref = receipt['response_ref']
        result = {'response_ref': response_ref, 'usage_ref': usage_ref}
        outcome, encoded = store._outcome(Outcome('succeeded', result,
            (Evidence('comparison_response', response_ref, 'local_only', artifact_hash=response_ref),)))
        store._record_evidence(conn, task, outcome)
        store._transition(conn, task, 'succeeded',
                          'trusted_provider_response_reconciled' if reconciled else 'provider_result_committed', ACTOR)
        conn.execute('UPDATE tasks SET result_json=? WHERE task_id=?',
                     (canonical_json(result).decode('utf-8'), tid))
        changed = conn.execute("""UPDATE leases SET state='completed',completion_json=?
            WHERE lease_id=? AND state=? AND fence=1""", (encoded, lease['lease_id'], lease_state)).rowcount
        _require(changed == 1)
        _event(conn, _event_key(tid, 'settled'), 'compare.request_settled', ACTOR, now,
               task_id=tid, fence=1, refs=(usage_ref,), required=True)
        if reconciled:
            _event(conn, _event_key(tid, 'reconciled'), 'compare.request_reconciled', ACTOR, now,
                   task_id=tid, fence=1, refs=(reconciled,), required=True)
        return {'state': 'settled', 'task_id': tid, 'fence': 1, **result}

    def _trusted_response(self, evidence_ref, reservation, response_ref, usage):
        # This entry point is for an operator's private, independently captured
        # bridge evidence. No model or tool client may supply these artefacts.
        from .broker import _provider_response
        from .money_guard import APPROVAL as BRIDGE_APPROVAL, SCHEMA as BRIDGE_SCHEMA

        proof = _document(self._artifact(evidence_ref, limit=8192))
        names = ('experiment_id', 'arm', 'run_id', 'sequence', 'task_id', 'fence', 'payload_hash', 'model')
        expected = {key: reservation[key] for key in names}
        _require(proof == {**expected, 'schema': TRUSTED_RECEIPT_SCHEMA, 'source': 'trusted_windows_bridge',
                           'bridge_receipt_ref': proof.get('bridge_receipt_ref'),
                           'bridge_call_index': proof.get('bridge_call_index'),
                           'raw_response_ref': proof.get('raw_response_ref'),
                           'response_ref': response_ref, 'usage': usage}
                 and type(proof['fence']) is int and type(proof['sequence']) is int,
                 'E_COMPARE_RECONCILIATION_PROOF')
        bridge = json.loads(self._artifact(proof['bridge_receipt_ref']))
        _require(type(bridge) is dict and bridge.get('schema') == BRIDGE_SCHEMA
                 and bridge.get('approval') == BRIDGE_APPROVAL and type(bridge.get('calls')) is list
                 and _matches(SHA, bridge.get('credential_id'))
                 and 1 <= len(bridge['calls']) <= 2 * LIMITS['provider_requests']
                 and _number(proof['bridge_call_index'], 0, len(bridge['calls']) - 1),
                 'E_COMPARE_RECONCILIATION_PROOF')
        record = bridge['calls'][proof['bridge_call_index']]
        counts = {'prompt_tokens': usage['input_tokens'], 'completion_tokens': usage['output_tokens'],
                  'total_tokens': usage['input_tokens'] + usage['output_tokens']}
        matching = [item for item in bridge['calls'] if type(item) is dict and
                    (item.get('arm'), item.get('payload_sha256')) ==
                    (reservation['arm'], reservation['payload_hash'].removeprefix('sha256:'))]
        _require(len(matching) == 1 and matching[0] is record and record.get('state') == 'returned'
                 and type(record.get('usage')) is dict and all(type(record['usage'].get(key)) is int
                     and record['usage'][key] == value for key, value in counts.items()),
                 'E_COMPARE_RECONCILIATION_PROOF')
        raw = _document(self._artifact(proof['raw_response_ref']))
        try:
            normalized, actual_usage = _provider_response(raw, tools_enabled=reservation['arm'] == 'hermes')
        except (KeyError, TypeError, ValueError):
            raise RequestLedgerError('E_COMPARE_RECONCILIATION_PROOF') from None
        _require(actual_usage == usage and self._artifact(response_ref) == canonical_json(normalized),
                 'E_COMPARE_RECONCILIATION_PROOF')
        # Ignore the bridge file's revision hash here. A later snapshot of the
        # same account and call index must not fund another task.
        call_identity = canonical_json([bridge['approval'], bridge['credential_id'], proof['bridge_call_index']])
        return 'compare:trusted_bridge_call:' + sha256(call_identity).hexdigest()

    def reconcile_known_response(self, task_id, *, response_ref, usage, evidence_ref):
        """Settle an already-sent result using operator-attested bridge evidence.

        The original bridge receipt did not record a response digest. The
        operator must independently match the saved raw response to its call.
        """
        _require(_matches(TASK_ID, task_id), 'E_COMPARE_RECONCILIATION_TASK')
        _require(type(usage) is dict and set(usage) == {'input_tokens', 'output_tokens'}
                 and all(type(value) is int and value >= 0 for value in usage.values()), 'E_COMPARE_USAGE_INVALID')
        actual = self._vector(usage['input_tokens'], usage['output_tokens'])
        with self._write() as (conn, starts_at):
            leases = conn.execute('SELECT lease_id,fence FROM leases WHERE task_id=? LIMIT 2', (task_id,)).fetchall()
            if len(leases) != 1 or leases[0]['fence'] != 1:
                raise FenceRejected('comparison reconciliation requires the original fence')
            lease, task = self._owned(conn, leases[0]['lease_id'], 1)
            reserve_ref, reservation, held = self._reservation(conn, task)
            _require(self._event_ref(conn, task_id, 'sent', optional=True) == reserve_ref, 'E_COMPARE_NOT_SENT')
            event = conn.execute('SELECT detail_code FROM control_events WHERE event_key=?',
                                 (_event_key(task_id, 'unknown'),)).fetchone()
            _require(event is not None and event['detail_code'] == 'unverified_usage'
                     and self._event_ref(conn, task_id, 'unknown') == reserve_ref,
                     'E_COMPARE_RECONCILIATION_REASON')
            _require(all(value <= reservation['vector'][unit] for unit, value in actual.items()),
                     'E_COMPARE_USAGE_EXCEEDS_RESERVED')
            receipt = {'schema': USAGE_SCHEMA, 'reservation_ref': reserve_ref, 'task_id': task_id, 'fence': 1,
                       'response_ref': response_ref, 'actual': actual, 'reconciliation_ref': evidence_ref}
            if task['status'] == 'succeeded':
                usage_ref, previous = self._usage(conn, task, reservation, held)
                if previous != receipt:
                    raise IdempotencyConflict('comparison reconciliation receipt is immutable')
                return {'state': 'cached', 'task_id': task_id, 'fence': 1,
                        'response_ref': response_ref, 'usage_ref': usage_ref}
            if (task['status'], lease['state'], held['state']) != ('unknown', 'unknown', 'uncertain'):
                raise FenceRejected('comparison reconciliation requires the recorded unknown fence')
            self._arm_snapshot(conn, reservation['arm'], starts_at)
            call_key = self._trusted_response(evidence_ref, reservation, response_ref, usage)
            _require(conn.execute('SELECT 1 FROM control_events WHERE event_key=?', (call_key,)).fetchone() is None,
                     'E_COMPARE_BRIDGE_RECEIPT_USED')
            _event(conn, call_key, 'compare.bridge_call_reconciled', ACTOR, self._store._now(),
                   task_id=task_id, fence=1, refs=(evidence_ref,), required=True)
            return self._commit_settlement(conn, starts_at, lease, task, reservation, receipt)

    def _mark_unknown(self, conn, lease, task, reason):
        tid = task['task_id']
        if task['status'] == 'unknown' and lease['state'] == 'unknown':
            return {'state': 'unknown', 'task_id': tid, 'fence': 1}
        if task['status'] != 'leased' or lease['state'] != 'active':
            raise FenceRejected('comparison lease cannot become unknown')
        now = self._store._now()
        reference, _, held = self._reservation(conn, task)
        _require(held['state'] == 'reserved' and expire_reservation(conn, tid, 1, ACTOR, now))
        self._store._transition(conn, task, 'unknown', reason, ACTOR)
        conn.execute("UPDATE leases SET state='unknown' WHERE lease_id=?", (lease['lease_id'],))
        _event(conn, _event_key(tid, 'unknown'), 'compare.request_unknown', ACTOR, now,
               task_id=tid, fence=1, refs=(reference,), code=reason, required=True)
        return {'state': 'unknown', 'task_id': tid, 'fence': 1}

    def mark_unknown(self, lease_id, fence, *, reason='provider_outcome_unknown'):
        _require(reason in UNKNOWN_REASONS, 'E_COMPARE_INVALID_UNKNOWN_REASON')
        with self._write() as (conn, _):
            return self._mark_unknown(conn, *self._owned(conn, lease_id, fence), reason)

    def recover_unknown(self, arm):
        """Explicit restart recovery. Even an unsent reservation is never replayed."""
        _arm(arm)
        with self._write() as (conn, _):
            rows = conn.execute('''SELECT l.lease_id,l.fence FROM leases l JOIN tasks t USING(task_id)
                WHERE t.track=? AND t.status='leased' AND l.state='active' LIMIT 2''',
                (self._track(arm),)).fetchall()
            _require(len(rows) <= 1)
            if not rows:
                return None
            lease, task = self._owned(conn, rows[0]['lease_id'], rows[0]['fence'])
            return self._mark_unknown(conn, lease, task, 'process_interrupted')
