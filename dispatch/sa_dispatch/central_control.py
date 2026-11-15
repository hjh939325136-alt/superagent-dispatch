"""Trusted local maintenance for an existing synthetic mixed ledger."""

from contextlib import closing, contextmanager
from datetime import timedelta
import json
import os
from pathlib import Path
import sqlite3
import stat

from .canonical import canonical_json, sha256_ref
from .clock import Clock, timestamp
from .control_policy import (
    _budget_config, _hash, _id, _insert_handoff, _time, _write_budget,
    _write_handoff_state, validate_mixed_control_schema,
)
from .models import DispatchError, IdempotencyConflict
from .routing import RouteRejected, ledger_mode, read_route_evidence

AUTHORITY = 'trusted_local_synthetic_central'
AUTHORIZATION_SCHEMA = 'sa_dispatch.central_handoff_authorization.v1'
RECEIPT_SCHEMA = 'sa_dispatch.central_handoff_receipt.v1'
_ACTIVE = ('offered', 'accepted')
_TRANSITIONS = {'offered': ('accepted', 'rejected'), 'accepted': ('completed', 'rejected')}
_BUDGET_KEYS = ('budget_id', 'period', 'unit', 'limit_units', 'starts_at', 'ends_at')
_HANDOFF_KEYS = ('handoff_id', 'from_conv', 'to_conv', 'project_id', 'scope', 'expires_at')


def budget_config_hash(budget_id, *, period, unit, limit_units, starts_at, ends_at):
    config = _budget_config(budget_id, period=period, unit=unit, limit_units=limit_units,
                            starts_at=starts_at, ends_at=ends_at)
    return sha256_ref(canonical_json(config))


def _offer(handoff_id, *, from_conv, to_conv, project_id, scope, expires_at, scope_refs):
    for value in (handoff_id, from_conv, to_conv, project_id):
        _id(value)
    if scope not in ('read_status', 'instruct', 'takeover'):
        raise DispatchError('invalid handoff scope')
    if not isinstance(scope_refs, (list, tuple)):
        raise DispatchError('handoff scope references must be an array')
    return dict(handoff_id=handoff_id, from_conv=from_conv, to_conv=to_conv, project_id=project_id,
                scope=scope, expires_at=_time(expires_at), scope_refs=[_hash(ref) for ref in scope_refs])


def handoff_offer_hash(handoff_id, *, from_conv, to_conv, project_id, scope, expires_at, scope_refs=()):
    offer = _offer(handoff_id, from_conv=from_conv, to_conv=to_conv, project_id=project_id,
                   scope=scope, expires_at=expires_at, scope_refs=scope_refs)
    return sha256_ref(canonical_json(offer))


def _budget_view(row):
    config = {key: row[key] for key in _BUDGET_KEYS}
    return dict(row, config_hash=budget_config_hash(**config))


def _offer_hash_from_row(row):
    offer = {key: row[key] for key in _HANDOFF_KEYS}
    return handoff_offer_hash(**offer, scope_refs=json.loads(row['scope_refs_json']))


def _expiry_due(row, now):
    return row['state'] in _ACTIVE and row['expires_at'] <= now


def _handoff_view(row, now):
    return dict(row, offer_hash=_offer_hash_from_row(row), expiry_due=_expiry_due(row, now))


def _local_path(value):
    value = os.fspath(value)
    if value.startswith(('\\\\', '//')) or '://' in value:
        raise RouteRejected('explicit local filesystem paths are required')
    path = Path(os.path.abspath(value))
    if os.name == 'nt':
        import ctypes
        drive_type = ctypes.windll.kernel32.GetDriveTypeW(str(path.anchor))
        if drive_type not in (2, 3, 6) or any(':' in part for part in path.parts[1:]):
            raise RouteRejected('local disk paths are required')
    return path


def _local_stat(path, *, directory=False):
    for component in reversed(path.parents):
        info = component.lstat()
        if not stat.S_ISDIR(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise RouteRejected('path components must be local directories without links or junctions')
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
        raise RouteRejected('links and junctions are not accepted')
    if directory:
        if not stat.S_ISDIR(info.st_mode):
            raise RouteRejected('an existing local artifact directory is required')
    elif not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise RouteRejected('a regular local file without hard links is required')
    return info


def _identity(info):
    return info.st_dev, info.st_ino


def _same_file(handle, identity):
    if _identity(os.fstat(handle.fileno())) != identity:
        raise RouteRejected('maintenance path changed while opening the file')


def _strict_read_object(pairs):
    value = dict(pairs)
    if len(value) != len(pairs):
        raise ValueError('duplicate keys in proof')
    return value


class _LocalArtifacts:
    """Read verified bytes without creating CAS files or following links."""

    def __init__(self, path):
        self.path = _local_path(path)
        self.identity = _identity(_local_stat(self.path, directory=True))

    def validate_path(self):
        if _identity(_local_stat(self.path, directory=True)) != self.identity:
            raise RouteRejected('maintenance artifact root changed')

    def get(self, ref):
        digest = _hash(ref)[7:]
        self.validate_path()
        path = self.path / 'sha256' / digest[:2] / digest
        identity = _identity(_local_stat(path))
        with path.open('rb') as source:
            _same_file(source, identity)
            content = source.read()
        if sha256_ref(content) != ref:
            raise DispatchError('artifact contents do not match their reference')
        return content


def _control_writes_only(action, table, column, database, source):
    if action == sqlite3.SQLITE_INSERT:
        allowed = table in ('control_budgets', 'control_handoffs', 'control_events')
    elif action == sqlite3.SQLITE_UPDATE:
        allowed = ((table == 'control_budgets' and column == 'limit_units')
                   or (table == 'control_handoffs'
                       and column in ('state', 'receipt_ref', 'evidence_refs_json', 'updated_at')))
    elif action == sqlite3.SQLITE_DELETE:
        allowed = False
    else:
        return sqlite3.SQLITE_OK
    return sqlite3.SQLITE_OK if allowed and database == 'main' and source is None else sqlite3.SQLITE_DENY


class CentralControl:
    """Maintain central policy metadata with explicitly supplied local authority."""

    def __init__(self, ledger, artifacts, expected_activation_ref, *, authority=None, clock=None):
        if authority != AUTHORITY:
            raise RouteRejected('explicit trusted local synthetic central maintenance authority required')
        self._activation_ref = _hash(expected_activation_ref)
        self._ledger = _local_path(ledger)
        self._ledger_identity = _identity(_local_stat(self._ledger))
        self._artifacts = _LocalArtifacts(artifacts)
        self._clock = clock or Clock()
        with self._transaction(write=False):
            pass

    def _validate_paths(self):
        if _identity(_local_stat(self._ledger)) != self._ledger_identity:
            raise RouteRejected('maintenance ledger file changed')
        self._artifacts.validate_path()
        with self._ledger.open('rb') as source:
            _same_file(source, self._ledger_identity)
            header = source.read(20)
        # Read-only SQLite can create WAL sidecars. The dispatch store uses a
        # rollback journal, allowing ordinary locking without those writes.
        if header[:16] != b'SQLite format 3\x00' or header[18:20] != b'\x01\x01':
            raise RouteRejected('an existing SQLite ledger in rollback journal mode is required')
        journal = self._ledger.with_name(self._ledger.name + '-journal')
        try:
            _local_stat(journal)
        except FileNotFoundError:
            pass

    def _validate_locked(self, conn):
        self._validate_paths()
        if ledger_mode(conn) != 'mixed':
            raise RouteRejected('central maintenance requires an activated mixed ledger')
        validate_mixed_control_schema(conn)
        activations = conn.execute("""SELECT object_table,object_id,execution_route,evidence_ref,pre_state_hash,fence,actor
            FROM route_audit WHERE action='activate' LIMIT 2""").fetchall()
        if (len(activations) != 1
                or tuple(activations[0])[:-1] != ('ledger_mode', '1', None, self._activation_ref, None, None)):
            raise RouteRejected('unique expected mixed activation record required')
        proof = read_route_evidence(self._artifacts, self._activation_ref)
        if (set(proof) != {'schema', 'actor', 'ledger_hash', 'bindings_hash'}
                or proof['schema'] != 'sa_dispatch.mixed_activation.v1'
                or proof['actor'] != activations[0]['actor']
                or not isinstance(proof['actor'], str) or not proof['actor'].strip()):
            raise RouteRejected('invalid mixed activation document')
        _hash(proof['ledger_hash'])
        _hash(proof['bindings_hash'])
        if conn.execute("SELECT 1 FROM tasks WHERE access IS NOT 'synthetic' OR egress IS NOT 'none' LIMIT 1").fetchone():
            raise RouteRejected('central maintenance requires synthetic no-egress tasks')
        if conn.execute("SELECT 1 FROM evidence WHERE access IS NOT 'synthetic' LIMIT 1").fetchone():
            raise RouteRejected('central maintenance requires synthetic evidence')

    @contextmanager
    def _transaction(self, *, write):
        self._validate_paths()
        uri = self._ledger.as_uri() + ('?mode=rw' if write else '?mode=ro')
        with closing(sqlite3.connect(uri, uri=True, timeout=10)) as conn, conn:
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA foreign_keys=ON')
            if not write:
                conn.execute('PRAGMA query_only=ON')
            conn.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
            self._validate_locked(conn)
            if write:
                conn.set_authorizer(_control_writes_only)
            yield conn

    def status(self, *, uncertain_after_hours=24):
        if type(uncertain_after_hours) is not int or not 0 <= uncertain_after_hours <= 8760:
            raise DispatchError('uncertainty threshold must be integer hours between 0 and 8760')
        with self._transaction(write=False) as conn:
            observed_at = self._clock.now()
            now = timestamp(observed_at)
            cutoff = timestamp(observed_at - timedelta(hours=uncertain_after_hours))
            result = {name: [dict(row) for row in conn.execute('SELECT * FROM control_'+name)]
                      for name in ('task_rules', 'approvals', 'reservations', 'events')}
            result['budgets'] = [_budget_view(row) for row in conn.execute(
                'SELECT * FROM control_budgets ORDER BY budget_id')]
            result['handoffs'] = [_handoff_view(row, now) for row in conn.execute(
                'SELECT * FROM control_handoffs ORDER BY handoff_id')]
            result['uncertain_over_threshold'] = [dict(row) for row in conn.execute('''
                SELECT r.*,b.unit,b.period FROM control_reservations r JOIN control_budgets b USING(budget_id)
                WHERE r.state='uncertain' AND r.uncertain_at<? ORDER BY r.uncertain_at,r.task_id,r.fence''', (cutoff,))]
            return dict(result, uncertain_after_hours=uncertain_after_hours, scope=AUTHORITY,
                        budget_totals_scope='ledger', messages_sent=0)

    def budget_upsert(self, budget_id, *, period, unit, limit_units, starts_at, ends_at, expected_config_hash):
        config = _budget_config(budget_id, period=period, unit=unit, limit_units=limit_units,
                                starts_at=starts_at, ends_at=ends_at)
        if expected_config_hash is not None:
            _hash(expected_config_hash)
        with self._transaction(write=True) as conn:
            row = conn.execute('SELECT * FROM control_budgets WHERE budget_id=?', (budget_id,)).fetchone()
            current = _budget_view(row) if row else None
            if (current['config_hash'] if current else None) != expected_config_hash:
                raise IdempotencyConflict('budget configuration changed or already exists')
            if current and all(current[key] == config[key] for key in _BUDGET_KEYS):
                return current
            if _write_budget(conn, config, row) != 1:
                raise DispatchError('budget change was not recorded')
            return _budget_view(conn.execute('SELECT * FROM control_budgets WHERE budget_id=?', (budget_id,)).fetchone())

    def _synthetic_proof(self, ref):
        try:
            proof = json.loads(self._artifacts.get(ref), object_pairs_hook=_strict_read_object)
        except (OSError, ValueError, RecursionError) as exc:
            raise DispatchError('readable synthetic proof required') from exc
        if not isinstance(proof, dict) or proof.get('synthetic') is not True:
            raise DispatchError('readable synthetic proof required')
        return proof

    def _authorized_by(self, authorization_ref, offer_hash):
        proof = self._synthetic_proof(authorization_ref)
        if (set(proof) != {'schema', 'synthetic', 'offer_hash', 'authorized_by'}
                or proof['schema'] != AUTHORIZATION_SCHEMA or proof['offer_hash'] != offer_hash):
            raise DispatchError('handoff authorization must bind the exact offer')
        return _id(proof['authorized_by'])

    def _validate_original_offer(self, row):
        offer_hash = _offer_hash_from_row(row)
        if row['authorized_by'] != self._authorized_by(row['authorization_ref'], offer_hash):
            raise DispatchError('stored handoff authorization does not match the offer')
        return offer_hash

    def _decision_evidence(self, receipt_ref, offer_hash, recipient, state):
        proof = self._synthetic_proof(receipt_ref)
        expected = dict(schema=RECEIPT_SCHEMA, synthetic=True, offer_hash=offer_hash,
                        recipient=recipient, decision=state)
        evidence_refs = proof.get('evidence_refs') if state == 'completed' else []
        if state == 'completed':
            if not isinstance(evidence_refs, list) or not evidence_refs:
                raise DispatchError('completion output evidence required')
            expected['evidence_refs'] = evidence_refs
        if proof != expected:
            raise DispatchError('handoff receipt must bind the exact offer, recipient and decision')
        for ref in evidence_refs:
            try:
                self._artifacts.get(ref)
            except OSError as exc:
                raise DispatchError('completion output evidence must be readable') from exc
        return evidence_refs

    @staticmethod
    def _handoff(conn, handoff_id):
        return conn.execute('SELECT * FROM control_handoffs WHERE handoff_id=?', (handoff_id,)).fetchone()

    def handoff_offer(self, handoff_id, *, from_conv, to_conv, project_id, scope, expires_at,
                      actor, scope_refs=(), authorization_ref):
        with self._transaction(write=True) as conn:
            now = timestamp(self._clock.now())
            offer = _offer(handoff_id, from_conv=from_conv, to_conv=to_conv, project_id=project_id,
                           scope=scope, expires_at=expires_at, scope_refs=scope_refs)
            if _id(actor) != from_conv:
                raise DispatchError('only the sender may offer a handoff')
            offer_hash = sha256_ref(canonical_json(offer))
            authorized_by = self._authorized_by(authorization_ref, offer_hash)
            row = self._handoff(conn, handoff_id)
            if row:
                if (offer_hash, authorization_ref, authorized_by) != (
                        _offer_hash_from_row(row), row['authorization_ref'], row['authorized_by']):
                    raise IdempotencyConflict('existing handoff offer and authorization are immutable')
                return _handoff_view(row, now)
            if offer['expires_at'] <= now:
                raise DispatchError('handoff already expired')
            _insert_handoff(conn, **offer, authorized_by=authorized_by, authorization_ref=authorization_ref,
                            at=now, event_required=True)
            return _handoff_view(self._handoff(conn, handoff_id), now)

    def handoff_transition(self, handoff_id, state, *, actor, receipt_ref):
        with self._transaction(write=True) as conn:
            now = timestamp(self._clock.now())
            row = self._handoff(conn, _id(handoff_id))
            if not row or _id(actor) != row['to_conv']:
                raise DispatchError('only the intended recipient may acknowledge an existing handoff')
            if _expiry_due(row, now):
                raise DispatchError('handoff authorization has expired')
            if state not in ('accepted', 'completed', 'rejected'):
                raise DispatchError('invalid handoff decision')
            if row['state'] != state and state not in _TRANSITIONS.get(row['state'], ()):
                raise DispatchError('invalid handoff transition from current state')
            offer_hash = self._validate_original_offer(row)
            refs = self._decision_evidence(receipt_ref, offer_hash, actor, state)
            if row['state'] == state:
                if row['receipt_ref'] != receipt_ref or json.loads(row['evidence_refs_json']) != refs:
                    raise IdempotencyConflict('handoff decision receipt and outputs are immutable')
                return _handoff_view(row, now)
            _write_handoff_state(conn, handoff_id, state, actor, now, receipt_ref=receipt_ref, refs=refs,
                                 event_required=True)
            return _handoff_view(self._handoff(conn, handoff_id), now)

    def handoff_expire(self, handoff_id):
        """Record expiry of the named handoff, including pre-activation offers."""
        with self._transaction(write=True) as conn:
            now = timestamp(self._clock.now())
            row = self._handoff(conn, _id(handoff_id))
            if not row:
                raise DispatchError('existing handoff required')
            if row['state'] != 'expired':
                if not _expiry_due(row, now):
                    raise DispatchError('an active overdue handoff is required')
                _write_handoff_state(conn, handoff_id, 'expired', 'policy', now, event_required=True)
                row = self._handoff(conn, handoff_id)
            return _handoff_view(row, now)
