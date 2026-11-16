"""Durable, forward-only mixed activation coordinator.

This is not a general backup/restore API. The trusted fixed-layout adapter must
prove all native writers stopped, preserve the full recovery set (including
ignored audit/CAS, domain identities, journals and binding state), and validate
exact identities and state. No automatic restore or deletion is implemented.

Install the guard at every HTTP/local start entry before maintenance. A missing
guard ALWAYS denies startup; a legacy isolated bootstrap needs an explicit,
separately reviewed installation policy. The maintenance process alone may stop
writers while the guard is closed. Expose must be idempotent and health must check
actual service identity. Adoption must be idempotent using each actual cursor.
New records/configs default to mode 0600. The native installer must explicitly
arrange intended new-file ownership and guard readability, or run ExecStartPre
with an appropriately privileged helper. Permission errors always deny startup.
"""
from contextlib import contextmanager
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
from typing import Protocol
import uuid


class MaintenanceError(ValueError):
    pass


class Adapter(Protocol):
    def freeze(self) -> dict: ...
    def validate_plan(self, plan: dict) -> None: ...
    def stop_writers(self) -> None: ...
    def verify_quiescent(self, plan: dict) -> None: ...
    def capture_recovery(self, plan: dict, destination: Path) -> dict: ...
    def verify_recovery(self, plan: dict, destination: Path, receipt: dict) -> None: ...
    def prepare_activation(self, plan: dict) -> str: ...
    def activation_evidence(self, plan: dict, proof_ref: str) -> dict | None: ...
    def activate(self, plan: dict, proof_ref: str) -> dict: ...
    def validate_configs(self, plan: dict) -> None: ...
    def expose(self, plan: dict) -> None: ...
    def health(self, plan: dict) -> None: ...
    def exposure_fingerprint(self, plan: dict) -> dict: ...
    def adopt_stream(self, plan: dict, stream: str) -> dict: ...


PHASES = {'staged', 'guarded', 'quiescent', 'checkpointed', 'proof_ready',
          'activated', 'configuring', 'configured', 'exposing', 'healthy',
          'adopting', 'completed', 'needs_reconciliation'}
EXPOSED = {'exposing', 'healthy', 'adopting'}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _safe(path):
    if '..' in Path(path).parts:
        raise MaintenanceError('parent traversal refused')
    path = Path(path).absolute()
    for part in (*reversed(path.parents), path):
        if part.is_symlink():
            raise MaintenanceError('linked path refused: ' + str(part))
        if part.exists():
            info = part.lstat()
            if getattr(info, 'st_reparse_tag', 0) or (stat.S_ISREG(info.st_mode) and info.st_nlink != 1):
                raise MaintenanceError('linked/reparse path refused: ' + str(part))
            if not stat.S_ISREG(info.st_mode) and not stat.S_ISDIR(info.st_mode):
                raise MaintenanceError('special path refused: ' + str(part))
    return path


def _target(root, name):
    if (not isinstance(name, str) or '\\' in name or ':' in name or
            not name or PurePosixPath(name).is_absolute() or
            any(part in ('', '.', '..') for part in name.split('/')) or
            name.split('/')[0] == '.mixed-maintenance'):
        raise MaintenanceError('invalid bounded configuration path')
    return _safe(root / name)


def _read(path):
    path = _safe(path)
    if not path.is_file():
        raise MaintenanceError('required regular file missing: ' + str(path))
    return path.read_bytes()


def _json(path):
    try:
        value = json.loads(_read(path))
        if not isinstance(value, dict):
            raise ValueError('expected object')
        return value
    except (ValueError, OSError) as exc:
        raise MaintenanceError('invalid durable record: ' + str(path)) from exc


def _sync_dir(path):
    if os.name != 'nt':
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _write(path, raw, *, exclusive=False):
    path = _safe(path)
    if not path.parent.is_dir():
        raise MaintenanceError('parent directory must already exist')
    if exclusive and path.exists():
        raise MaintenanceError('immutable record already exists')
    old_info = path.stat() if path.exists() else None
    mode = stat.S_IMODE(old_info.st_mode) if old_info else 0o600
    temp = path.with_name('.' + path.name + '.' + uuid.uuid4().hex + '.tmp')
    # Temp files are retained on unexpected failure; never remove unknown paths.
    with temp.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    if old_info is not None and os.name != 'nt':
        os.chown(temp, old_info.st_uid, old_info.st_gid)
    os.chmod(temp, mode)
    _safe(path)
    if exclusive and path.exists():
        raise MaintenanceError('immutable record appeared during write')
    os.replace(temp, path)
    _sync_dir(path.parent)


def _hash_or_none(path):
    _safe(path)
    return digest(_read(path)) if path.exists() else None


def _load(root, operation_id, expected):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', operation_id):
        raise MaintenanceError('invalid operation ID')
    operation = _safe(root / '.mixed-maintenance' / operation_id)
    raw = _read(operation / 'plan.json')
    if digest(raw) != expected:
        raise MaintenanceError('immutable plan hash mismatch')
    plan = _json(operation / 'plan.json')
    if (plan.get('version') != 1 or plan.get('operation_id') != operation_id or
            plan.get('native_root') != str(root) or not isinstance(plan.get('configs'), list) or
            not isinstance(plan.get('streams'), list)):
        raise MaintenanceError('invalid operation plan')
    seen = set()
    for index, item in enumerate(plan['configs']):
        target = _target(root, item['path'])
        if str(target).casefold() in seen or item['blob'] != f'config-{index}.bin':
            raise MaintenanceError('duplicate/unbounded configuration')
        seen.add(str(target).casefold())
        if digest(_read(operation / item['blob'])) != item['after_sha256']:
            raise MaintenanceError('frozen intended configuration changed')
    state = _json(operation / 'state.json')
    if state.get('plan_sha256') != expected or state.get('phase') not in PHASES:
        raise MaintenanceError('missing/corrupt operation phase')
    return operation, plan, state


def guard_check(native_root):
    """Read-only start gate; raises on every missing, partial or corrupt record."""
    root = _safe(native_root)
    guard = _json(root / '.mixed-maintenance' / 'guard.json')
    operation, plan, state = _load(root, guard['operation_id'], guard['plan_sha256'])
    if state['phase'] not in EXPOSED | {'completed'}:
        raise MaintenanceError('mixed maintenance restart guard is closed')
    if not state.get('activation') or not state.get('proof_ref') or not state.get('recovery'):
        raise MaintenanceError('missing activation/recovery authority')
    for item in plan['configs']:
        if _hash_or_none(_target(root, item['path'])) != item['after_sha256']:
            raise MaintenanceError('configuration drift blocks startup')
    return {'allowed': True, 'operation_id': plan['operation_id'], 'phase': state['phase']}


@contextmanager
def _lock(path):
    _safe(path)
    with path.open('r+b') as stream:
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            if os.name == 'nt':
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


class Coordinator:
    def __init__(self, native_root, operation_id, adapter, *, fault=None):
        self.root = _safe(native_root)
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', operation_id):
            raise MaintenanceError('invalid operation ID')
        self.operation_id, self.adapter = operation_id, adapter
        self.base = _safe(self.root / '.mixed-maintenance')
        self.operation = _safe(self.base / operation_id)
        self.fault = fault or (lambda label: None)

    def stage(self, configs, streams):
        """Freeze exact intended bytes and adapter anchor. No live file changes."""
        if not configs or not isinstance(configs, dict):
            raise MaintenanceError('nonempty fixed configuration set required')
        if (not isinstance(streams, list) or len(set(streams)) != len(streams) or
                any(not isinstance(s, str) or not s for s in streams)):
            raise MaintenanceError('explicit unique collector streams required')
        items = []
        for index, (name, raw) in enumerate(sorted(configs.items())):
            target = _target(self.root, name)
            if not isinstance(raw, bytes) or not target.parent.is_dir():
                raise MaintenanceError('config bytes and existing parent required')
            items.append(dict(path=name, before_sha256=_hash_or_none(target),
                              after_sha256=digest(raw), blob=f'config-{index}.bin'))
        plan = dict(version=1, operation_id=self.operation_id, native_root=str(self.root),
                    anchor=self.adapter.freeze(), configs=items, streams=streams)
        self.adapter.validate_plan(plan)
        self.base.mkdir(exist_ok=True)
        _safe(self.base)
        self.operation.mkdir()  # Never reuse an incomplete or foreign staging directory.
        for item in items:
            _write(self.operation / item['blob'], configs[item['path']], exclusive=True)
        raw = canonical(plan)
        plan_hash = digest(raw)
        _write(self.operation / 'plan.json', raw, exclusive=True)
        _write(self.operation / 'state.json', canonical(dict(phase='staged', plan_sha256=plan_hash)), exclusive=True)
        if not (self.base / 'lock').exists():
            try:
                with (self.base / 'lock').open('xb') as stream:
                    stream.write(b'0'); stream.flush(); os.fsync(stream.fileno())
            except FileExistsError:
                pass
        _sync_dir(self.base)
        return plan_hash

    def _save(self, state, phase, **updates):
        state.update(updates, phase=phase)
        _write(self.operation / 'state.json', canonical(state))
        self.fault('state:' + phase)

    def _guard(self, plan_hash, phase):
        path = self.base / 'guard.json'
        intended = dict(operation_id=self.operation_id, plan_sha256=plan_hash)
        if path.exists():
            current = _json(path)
            if current != intended:
                raise MaintenanceError('another operation owns restart guard')
        else:
            if phase != 'staged':
                raise MaintenanceError('durable restart guard disappeared')
            _write(path, canonical(intended), exclusive=True)
        self.fault('guard_installed')

    def run(self, expected_plan_sha256):
        with _lock(self.base / 'lock'):
            _, plan, state = _load(self.root, self.operation_id, expected_plan_sha256)
            # Completed repeats perform no adapter calls, restarts or filesystem writes.
            if state['phase'] == 'completed':
                guard_check(self.root)
                return state
            if state['phase'] == 'needs_reconciliation':
                raise MaintenanceError('operation requires explicit reconciliation')
            try:
                self._guard(expected_plan_sha256, state['phase'])
                self.adapter.validate_plan(plan)
                original_phase = state['phase']
                if original_phase == 'staged':
                    self._save(state, 'guarded')
                # Close gate before stop on a recovered exposed operation.
                if original_phase in EXPOSED:
                    self._save(state, 'configured', recovering_exposure=True)
                self.adapter.stop_writers()
                self.fault('writers_stopped')
                self.adapter.verify_quiescent(plan)
                if original_phase in EXPOSED or state.get('recovering_exposure'):
                    if self.adapter.exposure_fingerprint(plan) != state.get('exposure_baseline'):
                        raise MaintenanceError('new/unknown writes after service exposure')
                for item in plan['configs']:
                    current = _hash_or_none(_target(self.root, item['path']))
                    allowed = ({item['before_sha256'], item['after_sha256']}
                               if state['phase'] in {'configuring', 'configured'} else
                               {item['before_sha256']})
                    if current not in allowed:
                        raise MaintenanceError('configuration changed outside reviewed operation')
                    if (original_phase in EXPOSED or state.get('recovering_exposure')) and current != item['after_sha256']:
                        raise MaintenanceError('configuration drift after service exposure')
                if state['phase'] in {'guarded', 'quiescent'}:
                    self._save(state, 'quiescent')
                    receipt = self.adapter.capture_recovery(plan, _safe(self.operation / 'recovery'))
                    self.fault('recovery_captured')
                    self.adapter.verify_recovery(plan, _safe(self.operation / 'recovery'), receipt)
                    self._save(state, 'checkpointed', recovery=receipt)
                self.adapter.verify_recovery(plan, _safe(self.operation / 'recovery'), state['recovery'])
                if state['phase'] == 'checkpointed':
                    proof = self.adapter.prepare_activation(plan)
                    if not isinstance(proof, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', proof):
                        raise MaintenanceError('invalid activation proof reference')
                    self.fault('proof_written')
                    self._save(state, 'proof_ready', proof_ref=proof)
                evidence = self.adapter.activation_evidence(plan, state['proof_ref'])
                if evidence is None:
                    if state['phase'] != 'proof_ready':
                        raise MaintenanceError('committed activation evidence missing')
                    evidence = self.adapter.activate(plan, state['proof_ref'])
                    self.fault('activation_committed')
                    observed = self.adapter.activation_evidence(plan, state['proof_ref'])
                    if not evidence or observed != evidence:
                        raise MaintenanceError('activation readback mismatch')
                if state.get('activation') is not None and state['activation'] != evidence:
                    raise MaintenanceError('committed activation identity changed')
                if state['phase'] == 'proof_ready':
                    self._save(state, 'activated', activation=evidence)
                if state['phase'] in {'activated', 'configuring'}:
                    self._save(state, 'configuring')
                    for index, item in enumerate(plan['configs']):
                        target = _target(self.root, item['path'])
                        if _hash_or_none(target) != item['after_sha256']:
                            if _hash_or_none(target) != item['before_sha256']:
                                raise MaintenanceError('config changed immediately before replacement')
                            _write(target, _read(self.operation / item['blob']))
                        self.fault('config_replaced:' + str(index))
                        if _hash_or_none(target) != item['after_sha256']:
                            raise MaintenanceError('configuration replacement did not persist')
                    self._save(state, 'configured')
                self.adapter.validate_configs(plan)
                baseline = self.adapter.exposure_fingerprint(plan)
                self._save(state, 'exposing', exposure_baseline=baseline, recovering_exposure=False)
                guard_check(self.root)
                self.fault('before_expose')
                self.adapter.expose(plan)
                self.fault('after_expose')
                self.adapter.health(plan)
                if self.adapter.exposure_fingerprint(plan) != baseline:
                    raise MaintenanceError('new/unknown writes while exposing service')
                self._save(state, 'healthy')
                adopted = state.get('adopted', {})
                for stream in plan['streams']:
                    # Adapter verifies completed adoption again; never trusts a local bit.
                    receipt = self.adapter.adopt_stream(plan, stream)
                    self.fault('stream_adopted:' + stream)
                    if stream in adopted and adopted[stream] != receipt:
                        raise MaintenanceError('collector adoption receipt drift')
                    adopted[stream] = receipt
                    self._save(state, 'adopting', adopted=adopted)
                self.adapter.health(plan)
                self.adapter.validate_configs(plan)
                if self.adapter.exposure_fingerprint(plan) != baseline:
                    raise MaintenanceError('new/unknown writes during collector adoption')
                self._save(state, 'completed')
                return state
            except Exception as exc:
                # BaseException models process loss: durable intent remains recoverable.
                # Ordinary failures close startup first, stop writers, preserve every byte.
                try:
                    self._save(state, 'needs_reconciliation', error_type=type(exc).__name__)
                except Exception:
                    # A failed atomic journal write must not retain an open gate.
                    # Invalidate the already-owned guard in place (no allocation
                    # or arbitrary deletion). If storage itself fails, propagate
                    # the error; stopping all writers remains unconditional.
                    guard = _safe(self.base / 'guard.json')
                    with guard.open('wb') as stream:
                        stream.write(b'{}')
                        stream.flush()
                        os.fsync(stream.fileno())
                    _sync_dir(guard.parent)
                    raise
                finally:
                    self.adapter.stop_writers()
                raise


def main(argv=None):
    parser = argparse.ArgumentParser(description='Fail-closed native mixed startup guard')
    parser.add_argument('command', choices=['guard-check'])
    parser.add_argument('--native-root', required=True)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(guard_check(Path(args.native_root)), sort_keys=True))
        return 0
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print(json.dumps({'allowed': False, 'error': type(exc).__name__}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
