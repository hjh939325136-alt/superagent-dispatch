"""Versioned synthetic coordinator upgrades with durable conservative recovery.

The engine accepts an explicit service adapter. The CLI uses only the fixed lab
adapter in lab.py. It does not install packages, alter identities, or run tasks.
"""
import argparse
from contextlib import contextmanager, closing
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
from uuid import uuid4

from .probe import REVIEW_LIMIT, fingerprint, historical_fingerprint, migration_identity

FORMAT = 'm0_deploy.v1'
LIMIT = 16 * 1024 * 1024
PACKAGES = ('sa_dispatch', 'sa_dispatch_hotspot')


class DeployError(ValueError):
    pass


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def regular(path, *, directory=False):
    path = Path(path).absolute()
    for item in (path, *path.parents):
        if not item.exists() and not item.is_symlink():
            continue
        info = item.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise DeployError('links and reparse points are not accepted')
    mode = path.lstat().st_mode
    if not (stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode)):
        raise DeployError('unexpected filesystem object type')
    return path


def safe_tree(root):
    regular(root, directory=True)
    for parent, dirs, files in os.walk(root, followlinks=False):
        for name in dirs:
            regular(Path(parent) / name, directory=True)
        for name in files:
            regular(Path(parent) / name)


def tree_inventory(root):
    """Full bounded tree identity, including currently unreferenced CAS objects."""
    root = Path(root).absolute()
    safe_tree(root)
    result, total = [], 0
    for path in sorted(root.rglob('*')):
        if path.is_file():
            size = path.stat().st_size
            total += size
            if total > LIMIT:
                raise DeployError('maintenance tree exceeds size cap')
            result.append({'path': path.relative_to(root).as_posix(), 'size': size,
                           'sha256': digest(regular(path).read_bytes())})
    return result


def contained_target(root, target):
    """Check existing *and missing* destination ancestors before any creation."""
    root, target = Path(root).absolute(), Path(target).absolute()
    regular(root, directory=True)
    if not target.is_relative_to(root) or target == root or '..' in target.parts:
        raise DeployError('restore destination escapes its explicit root')
    for item in (target, *target.parents):
        if item.exists() or item.is_symlink():
            regular(item, directory=item != target)
    if not target.resolve().is_relative_to(root):
        raise DeployError('resolved restore destination escapes its root')
    return target


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name('.' + path.name + '.' + uuid4().hex)
    with temporary.open('xb') as stream:
        stream.write(encoded(value) + b'\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    sync_dir(path.parent)


def sync_dir(path):
    if os.name == 'posix':
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


@contextmanager
def lock(path):
    # flock is released by the kernel after process termination, unlike a pidfile.
    if os.name != 'posix':
        raise DeployError('apply/recover require Linux advisory locks')
    import fcntl
    with Path(path).open('a+b') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise DeployError('another maintenance process owns the lock') from None
        yield


def stage(source, destination):
    source, destination = Path(source).absolute(), Path(destination).absolute()
    regular(source, directory=True)
    regular(destination.parent, directory=True)
    files = []
    for package in PACKAGES:
        regular(source / package, directory=True)
        for path in sorted((source / package).iterdir()):
            if path.suffix not in ('.py', '.sql'):
                continue
            regular(path)
            content = path.read_bytes()
            files.append((path.relative_to(source).as_posix(), content))
    if not files or sum(len(content) for _, content in files) > LIMIT:
        raise DeployError('empty or oversized runtime source')
    manifest = {'schema': FORMAT, 'kind': 'runtime-release', 'files': [
        {'path': name, 'size': len(content), 'sha256': digest(content)} for name, content in files]}
    if destination.exists():
        old = verify_release(destination)
        if old != manifest:
            raise DeployError('release identifier already contains different source')
        return manifest
    staging = Path(tempfile.mkdtemp(prefix='.release-', dir=destination.parent))
    try:
        for name, content in files:
            target = staging / name
            target.parent.mkdir(exist_ok=True)
            target.write_bytes(content)
            target.chmod(0o644)
        for package in PACKAGES:
            (staging / package).chmod(0o755)
        atomic_json(staging / 'release.json', manifest)
        (staging / 'release.json').chmod(0o644)
        staging.chmod(0o755)
        verify_release(staging)
        os.rename(staging, destination)
        sync_dir(destination.parent)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return manifest


def _relative(name):
    if not isinstance(name, str) or '\\' in name or not name or name.startswith('/') or any(
            part in ('', '.', '..') for part in name.split('/')) or ':' in name:
        raise DeployError('invalid relative manifest path')
    return Path(name)


def verify_files(root, manifest, extra=()):
    root = regular(root, directory=True)
    expected = set(extra)
    total = 0
    for row in manifest['files']:
        name = row['path']
        if name in expected:
            raise DeployError('duplicate manifest path')
        expected.add(name)
        path = regular(root / _relative(name))
        total += path.stat().st_size
        if total > LIMIT or path.stat().st_size != row['size'] or digest(path.read_bytes()) != row['sha256']:
            raise DeployError('manifest size or SHA mismatch')
    safe_tree(root)
    actual = {path.relative_to(root).as_posix() for path in root.rglob('*') if path.is_file()}
    if actual != expected:
        raise DeployError('unlisted or missing manifest file')
    return total


def verify_release(path):
    path = regular(path, directory=True)
    manifest = json.loads(regular(path / 'release.json').read_text())
    if manifest.get('schema') != FORMAT or manifest.get('kind') != 'runtime-release':
        raise DeployError('unexpected release manifest')
    for row in manifest['files']:
        relative = _relative(row['path'])
        if len(relative.parts) != 2 or relative.parts[0] not in PACKAGES or relative.suffix not in ('.py', '.sql'):
            raise DeployError('release file is not in the runtime allowlist')
    verify_files(path, manifest, extra=('release.json',))
    return manifest


def run_probe(release, ledger, artifacts, action='inspect', *, user=None, review=None):
    command = [sys.executable, '-B', str(Path(__file__).with_name('probe.py')),
               action, '--release', str(release), '--ledger', str(ledger), '--artifacts', str(artifacts)]
    review_bytes = None
    if review is not None:
        review_bytes = encoded(review)
        if len(review_bytes) > REVIEW_LIMIT:
            raise DeployError('late review exceeds size cap')
        command.append('--late-review-stdin')
    if user:
        command = ['/usr/sbin/runuser', '-u', user, '--', *command]
    environment = dict(os.environ)
    environment.pop('PYTHONPATH', None)
    environment.pop('PYTHONHOME', None)
    result = subprocess.run(command, input=review_bytes, capture_output=True, timeout=30, env=environment)
    if result.returncode:
        raise DeployError('candidate probe failed: ' + action)
    try:
        response = json.loads(result.stdout)
        if response.get('ok') is not True:
            raise ValueError()
        return response['result']
    except (ValueError, KeyError):
        raise DeployError('invalid candidate probe response') from None


def snapshot(release, ledger, artifacts, destination, *, user=None, review=None):
    ledger, artifacts, destination = Path(ledger), Path(artifacts), Path(destination)
    regular(ledger)
    safe_tree(artifacts)
    if destination.exists() or destination.is_symlink():
        raise DeployError('snapshot destination must not exist')
    regular(destination.parent, directory=True)
    temporary = Path(tempfile.mkdtemp(prefix='.snapshot-', dir=destination.parent))
    try:
        db = temporary / 'ledger.sqlite3'
        with closing(sqlite3.connect(ledger.absolute().as_uri() + '?mode=ro', uri=True)) as source:
            with closing(sqlite3.connect(db)) as target:
                source.backup(target)
                target.execute('PRAGMA journal_mode=DELETE')
        # Probe only the consistent snapshot; all authentication files are excluded.
        temporary.chmod(0o755)
        db.chmod(0o644)
        inspected = run_probe(release, db, artifacts, user=user, review=review)
        files = [{'path': db.name, 'size': db.stat().st_size, 'sha256': digest(db.read_bytes())}]
        total = files[0]['size'] + inspected['blob_bytes']
        if total > LIMIT:
            raise DeployError('snapshot exceeds maintenance size cap')
        for blob in inspected['blobs']:
            ref = blob['ref']
            if not re.fullmatch('sha256:[0-9a-f]{64}', ref):
                raise DeployError('invalid CAS reference')
            name = 'artifacts/sha256/' + ref[7:9] + '/' + ref[7:]
            source_path = regular(artifacts / name.removeprefix('artifacts/'))
            content = source_path.read_bytes()
            if digest(content) != ref[7:] or len(content) != blob['size']:
                raise DeployError('CAS changed during snapshot')
            target = temporary / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            files.append({'path': name, 'size': len(content), 'sha256': digest(content)})
        (temporary / 'artifacts').mkdir(exist_ok=True)
        manifest = {'schema': FORMAT, 'kind': 'state-snapshot', 'files': files,
                    'tables': inspected['tables'], 'synthetic': True, 'settled': True}
        if review is not None:
            manifest['late_review_sha256'] = digest(encoded(review))
        atomic_json(temporary / 'snapshot.json', manifest)
        verify_files(temporary, manifest, extra=('snapshot.json',))
        os.rename(temporary, destination)
        sync_dir(destination.parent)
        return manifest
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def state_write(operation, phase, **fields):
    path = Path(operation) / 'state.json'
    state = json.loads(path.read_text()) if path.exists() else {'schema': FORMAT, 'history': []}
    state.update(fields)
    state['phase'] = phase
    state['history'].append(phase)
    atomic_json(path, state)
    return state


def validate_review_binding(review, context):
    binding = {key: context[key] for key in
               ('service', 'ledger', 'artifacts', 'old_release', 'release', 'release_manifest_sha256')}
    if not isinstance(review, dict) or review.get('binding') != binding or len(encoded(review)) > REVIEW_LIMIT:
        raise DeployError('late review does not identify this source and candidate')
    return digest(encoded(review))


def read_review(path, approval):
    if path is None and approval is None:
        return None
    if path is None or approval is None:
        raise DeployError('explicit late review document and approved hash are required')
    with regular(path).open('rb') as stream:
        raw = stream.read(REVIEW_LIMIT + 1)
    if len(raw) > REVIEW_LIMIT:
        raise DeployError('late review exceeds size cap')
    review = json.loads(raw)
    if not isinstance(review, dict) or digest(encoded(review)) != approval:
        raise DeployError('approved late review hash differs')
    return review


def prepare(release, ledger, artifacts, operation, old_release, adapter, *, review=None):
    release, operation = Path(release).absolute(), Path(operation).absolute()
    manifest = verify_release(release)
    context = {'release': str(release), 'old_release': str(Path(old_release).absolute()),
               'release_manifest_sha256': digest(encoded(manifest)), 'ledger': str(Path(ledger).absolute()),
               'artifacts': str(Path(artifacts).absolute()), 'service': adapter.identity}
    review_options = {}
    if review is not None:
        review_hash = validate_review_binding(review, context)
        review_options['review'] = review
    if operation.exists():
        raise DeployError('choose a new operation directory')
    regular(operation.parent, directory=True)
    operation.mkdir(mode=0o755)
    state_write(operation, 'preparing')
    try:
        state_manifest = snapshot(release, ledger, artifacts, operation / 'snapshot', user=adapter.user, **review_options)
        clone = operation / 'rehearsal'
        shutil.copytree(operation / 'snapshot', clone)
        adapter.own_rehearsal(clone)
        migrated = adapter.probe(release, clone / 'ledger.sqlite3', clone / 'artifacts', 'migrate', **review_options)
        before = state_manifest['tables']
        if historical_fingerprint(clone / 'ledger.sqlite3', operation / 'snapshot/ledger.sqlite3', before) != before:
            raise DeployError('migration changed historical rows')
        smoke = adapter.probe(release, clone / 'ledger.sqlite3', clone / 'artifacts', 'smoke')
        plan = {'schema': FORMAT, **context,
                'old_release_files': tree_inventory(old_release),
                'source_artifacts_state': tree_inventory(artifacts),
                'snapshot_sha256': digest(encoded(state_manifest)),
                'snapshot_bytes': sum(row['size'] for row in state_manifest['files']),
                'release_bytes': sum(row['size'] for row in manifest['files']),
                'rehearsal_tables': migrated['tables'],
                'migration_identity': migration_identity(clone / 'ledger.sqlite3'), 'smoke': smoke,
                'task_writes': 0}
        if review is not None:
            plan.update(late_review=review, late_review_sha256=review_hash)
        atomic_json(operation / 'plan.json', plan)
        return state_write(operation, 'prepared', plan_sha256=digest(encoded(plan)), plan=plan)
    except Exception as exc:
        state_write(operation, 'preparation_failed', error=type(exc).__name__)
        raise


def load_verified(operation, approval):
    operation = regular(operation, directory=True)
    state = json.loads(regular(operation / 'state.json').read_text())
    plan = json.loads(regular(operation / 'plan.json').read_text())
    if digest(encoded(plan)) != approval or state.get('plan_sha256') != approval:
        raise DeployError('explicit reviewed plan hash is required')
    if digest(encoded(verify_release(plan['release']))) != plan['release_manifest_sha256']:
        raise DeployError('candidate source changed after rehearsal')
    snap = operation / 'snapshot'
    manifest = json.loads(regular(snap / 'snapshot.json').read_text())
    if digest(encoded(manifest)) != plan['snapshot_sha256']:
        raise DeployError('snapshot manifest changed')
    if 'late_review' in plan:
        review_hash = validate_review_binding(plan['late_review'], plan)
        if review_hash != plan.get('late_review_sha256') or review_hash != manifest.get('late_review_sha256'):
            raise DeployError('late receipt review binding changed')
    elif 'late_review_sha256' in plan or 'late_review_sha256' in manifest:
        raise DeployError('late receipt review is missing')
    verify_files(snap, manifest, extra=('snapshot.json',))
    return state, plan, manifest


def _restore_file(source, target, *, root):
    target = contained_target(root, target)
    previous = target.stat() if target.exists() else None
    temporary = target.with_name('.' + target.name + '.restore-' + uuid4().hex)
    contained_target(root, temporary)
    shutil.copyfile(source, temporary)
    if previous:
        os.chmod(temporary, stat.S_IMODE(previous.st_mode))
        if os.name == 'posix':
            os.chown(temporary, previous.st_uid, previous.st_gid)
    with temporary.open('r+b') as stream:
        os.fsync(stream.fileno())
    os.replace(temporary, target)
    sync_dir(target.parent)


def _rollback(operation, plan, manifest, adapter):
    state_write(operation, 'rollback_stop_intent')
    adapter.stop()
    with adapter.state_lock():
        state = json.loads((Path(operation) / 'state.json').read_text())
        try:
            if tree_inventory(plan['old_release']) != plan['old_release_files']:
                raise DeployError('old release changed after plan review')
            expected = state.get('exposed_state')
            if expected:
                live = {'tables': fingerprint(plan['ledger']), 'artifacts': tree_inventory(plan['artifacts'])}
                if live != expected:
                    # The new process may have accepted work after startup. Retain
                    # its full state, plus the old snapshot; never erase history.
                    raise DeployError('ledger or CAS changed after service exposure')
        except Exception as exc:
            state_write(operation, 'needs_reconciliation', error=type(exc).__name__,
                        service_stopped=True, original_snapshot_preserved=True, current_state_preserved=True)
            raise deploy_reconciliation_error() from None
        # Service is stopped and exposure has just been checked. If this process
        # dies during replacement, recovery can safely repeat the same restore.
        state_write(operation, 'rollback_restore_intent', exposed_state=None)
        db = Path(plan['ledger'])
        regular(db)
        for suffix in ('-wal', '-shm', '-journal'):
            sidecar = Path(str(db) + suffix)
            if sidecar.exists():
                regular(sidecar).unlink()
        _restore_file(Path(operation) / 'snapshot/ledger.sqlite3', db, root=db.parent)
        for row in manifest['files']:
            if not row['path'].startswith('artifacts/'):
                continue
            target = Path(plan['artifacts']) / row['path'].removeprefix('artifacts/')
            contained_target(plan['artifacts'], target)
            if not target.exists() or digest(regular(target).read_bytes()) != row['sha256']:
                target.parent.mkdir(parents=True, exist_ok=True)
                contained_target(plan['artifacts'], target)
                _restore_file(Path(operation) / 'snapshot' / row['path'], target, root=plan['artifacts'])
                adapter.own_restored(target)
        if fingerprint(db) != manifest['tables']:
            raise DeployError('restored ledger differs from snapshot')
        adapter.select(plan['old_release'])
        state_write(operation, 'rollback_start_intent', exposed_state={
            'tables': fingerprint(plan['ledger']), 'artifacts': tree_inventory(plan['artifacts'])})
    adapter.start()
    health = adapter.health()
    return state_write(operation, 'rolled_back', restored_tables=manifest['tables'], health=health)


def deploy_reconciliation_error():
    return DeployError('needs_reconciliation: retained current state and original snapshot; service stopped')


def apply(operation, approval, adapter, *, fault=None, recovery_only=False):
    with adapter.maintenance_lock():
        state, plan, manifest = load_verified(operation, approval)
        adapter.validate(plan)
        if state['phase'] == 'needs_reconciliation':
            raise deploy_reconciliation_error()
        if state['phase'] in ('completed', 'rolled_back', 'refused_before_migration'):
            return {**state, 'repeat': True}
        if state['phase'] in ('stop_intent', 'stale_snapshot'):
            if tree_inventory(plan['old_release']) != plan['old_release_files']:
                adapter.stop()
                state_write(operation, 'needs_reconciliation', service_stopped=True,
                            original_snapshot_preserved=True, current_state_preserved=True,
                            error='old_release_drift')
                raise deploy_reconciliation_error()
            adapter.select(plan['old_release'])
            adapter.start()
            return state_write(operation, 'refused_before_migration', recovery='no migration had started')
        if state['phase'] != 'prepared':
            return _rollback(operation, plan, manifest, adapter)
        if recovery_only:
            return {**state, 'recovery_noop': True}
        if 'migration_identity' not in plan:
            raise DeployError('legacy prepared plan lacks migration identities; prepare a new reviewed plan')
        if tree_inventory(plan['old_release']) != plan['old_release_files']:
            raise DeployError('old release changed after plan review')
        review_options = {'review': plan['late_review']} if 'late_review' in plan else {}
        try:
            state_write(operation, 'stop_intent')
            adapter.stop()
            with adapter.state_lock():
                if (fingerprint(plan['ledger']) != manifest['tables'] or
                        tree_inventory(plan['artifacts']) != plan['source_artifacts_state']):
                    # No migration occurred: do not replace legitimately newer data.
                    state_write(operation, 'stale_snapshot')
                    raise DeployError('live ledger changed since reviewed snapshot')
                adapter.probe(plan['release'], plan['ledger'], plan['artifacts'], 'inspect', **review_options)
                state_write(operation, 'migrate_intent')
                adapter.probe(plan['release'], plan['ledger'], plan['artifacts'], 'migrate', **review_options)
                if fault:
                    fault('after_migrate')
                if historical_fingerprint(plan['ledger'], Path(operation) / 'snapshot/ledger.sqlite3', manifest['tables']) != manifest['tables']:
                    raise DeployError('migration altered historical rows')
                if 'migration_identity' in plan and migration_identity(plan['ledger']) != plan['migration_identity']:
                    raise DeployError('migration identities differ from reviewed rehearsal')
                adapter.select(plan['release'])
                state_write(operation, 'start_intent', exposed_state={
                    'tables': fingerprint(plan['ledger']), 'artifacts': tree_inventory(plan['artifacts'])})
            adapter.start()
            health = adapter.health()
            if fault:
                fault('after_health')
            return state_write(operation, 'completed', health=health)
        except Exception as exc:
            current = json.loads((Path(operation) / 'state.json').read_text())
            if current['phase'] in ('stop_intent', 'stale_snapshot'):
                adapter.start()
                state_write(operation, 'refused_before_migration', error=type(exc).__name__)
                raise
            state_write(operation, 'failure', error=type(exc).__name__)
            try:
                return _rollback(operation, plan, manifest, adapter)
            except Exception as rollback_error:
                latest = json.loads((Path(operation) / 'state.json').read_text())
                if latest['phase'] == 'needs_reconciliation':
                    raise deploy_reconciliation_error() from None
                state_write(operation, 'rollback_failed', error=type(rollback_error).__name__)
                raise DeployError('rollback incomplete; service remains stopped or unhealthy') from None


def main():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers(dest='action', required=True)
    staging = subs.add_parser('stage')
    staging.add_argument('--source', required=True)
    staging.add_argument('--destination', required=True)
    args = parser.parse_args()
    try:
        result = stage(args.source, args.destination)
        print(json.dumps({'ok': True, 'manifest_sha256': digest(encoded(result)), 'manifest': result}))
        return 0
    except Exception as exc:
        print(json.dumps({'ok': False, 'error': type(exc).__name__}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
