"""Fixed-layout systemd adapter for the already authorized synthetic Debian lab."""
import argparse
from contextlib import contextmanager
import http.client
import json
import os
from pathlib import Path
import pwd
import re
import subprocess
import time
import tomllib

from . import deploy
from .install_target import InstallTargetError, _read_protected, observe_install_target, validate_install_target

ROOT = Path('/opt/sa-dispatch')
OPERATIONS = Path('/var/lib/sa-dispatch-upgrades')
LEDGER = Path('/var/lib/sa-dispatch/dispatch.sqlite3')
ARTIFACTS = Path('/var/lib/sa-dispatch/artifacts')
SERVICE = 'sa-dispatch.service'
PORT = 8876
LEGACY_LAB_HOSTNAME = Path('/etc/sa-dispatch-legacy-lab/hostname')


class LabAdapter:
    user = 'sa-dispatch'
    identity = SERVICE

    def __init__(self):
        try:
            expected = _read_protected(LEGACY_LAB_HOSTNAME, limit=256).decode('utf-8').rstrip('\n')
            _, actual = observe_install_target()
        except (InstallTargetError, UnicodeError) as exc:
            raise deploy.DeployError('legacy adapter is lab-only and requires a protected hostname binding') from exc
        if expected != actual:
            raise deploy.DeployError('unexpected machine; this adapter is lab-only')
        self.account = pwd.getpwnam(self.user)

    def status(self):
        output = subprocess.check_output(['systemctl', 'show', SERVICE,
            '--property=ActiveState,SubState,MainPID,NRestarts,User,Group,WorkingDirectory'], text=True)
        return dict(line.split('=', 1) for line in output.splitlines())

    def current(self):
        path = ROOT / 'current'
        if not path.is_symlink():
            raise deploy.DeployError('current must be the owned release symlink')
        target = path.resolve()
        if target.parent != ROOT / 'releases':
            raise deploy.DeployError('current release escapes the release directory')
        deploy.regular(target, directory=True)
        return target

    def validate(self, plan):
        if plan['service'] != SERVICE or Path(plan['ledger']) != LEDGER or Path(plan['artifacts']) != ARTIFACTS:
            raise deploy.DeployError('plan does not identify the fixed lab')
        releases = {Path(plan['old_release']), Path(plan['release'])}
        if any(path.parent != ROOT / 'releases' for path in releases) or self.current() not in releases:
            raise deploy.DeployError('unrecognized current or requested release')
        config = tomllib.loads(deploy.regular('/etc/sa-dispatch/coordinator.toml').read_text())
        if (config.get('synthetic') is not True or config.get('components') or
                Path(config.get('db_path', '')) != LEDGER or
                Path(config.get('artifact_root', '')) != ARTIFACTS):
            raise deploy.DeployError('live coordinator is not the expected synthetic configuration')
        state = self.status()
        if state['User'] != self.user or state['Group'] != self.user or state['WorkingDirectory'] != str(ROOT / 'current'):
            raise deploy.DeployError('unexpected service owner or working directory')

    def stop(self):
        subprocess.run(['systemctl', 'stop', SERVICE], check=True, capture_output=True, timeout=20)
        status = self.status()
        if status['ActiveState'] not in ('inactive', 'failed') or status['MainPID'] != '0':
            raise deploy.DeployError('dedicated service did not stop')

    def start(self):
        subprocess.run(['systemctl', 'start', SERVICE], check=True, capture_output=True, timeout=20)

    def select(self, release):
        release = Path(release)
        if release.parent != ROOT / 'releases':
            raise deploy.DeployError('release selection escapes lab root')
        deploy.regular(release, directory=True)
        temporary = ROOT / ('.current-' + os.urandom(8).hex())
        temporary.symlink_to(release.relative_to(ROOT))
        os.replace(temporary, ROOT / 'current')
        deploy.sync_dir(ROOT)

    def own_rehearsal(self, path):
        deploy.safe_tree(path)
        for item in (path, *path.rglob('*')):
            os.chown(item, self.account.pw_uid, self.account.pw_gid)
            item.chmod(0o700 if item.is_dir() else 0o600)

    def own_restored(self, path):
        os.chown(path, self.account.pw_uid, self.account.pw_gid)
        path.chmod(0o600)

    def probe(self, release, ledger, artifacts, action, *, review=None):
        return deploy.run_probe(release, ledger, artifacts, action, user=self.user, review=review)

    def maintenance_lock(self):
        return deploy.lock(ROOT / '.maintenance.lock')

    @contextmanager
    def state_lock(self):
        import fcntl
        fd = os.open(LEDGER.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(fd)

    def health(self):
        checks = []
        for _ in range(3):
            time.sleep(1)
            state = self.status()
            if state['ActiveState'] != 'active' or state['SubState'] != 'running' or state['MainPID'] == '0':
                raise deploy.DeployError('service is not active')
            process = Path('/proc') / state['MainPID']
            if (process / 'cwd').resolve() != self.current() or (process / 'exe').resolve() != Path('/usr/bin/python3').resolve():
                raise deploy.DeployError('service executable or release identity mismatch')
            address = '0100007F:' + format(PORT, '04X')
            inodes = {line.split()[9] for line in Path('/proc/net/tcp').read_text().splitlines()[1:]
                      if line.split()[1] == address and line.split()[3] == '0A'}
            owned = {os.readlink(item) for item in (process / 'fd').iterdir() if item.is_symlink()}
            if not any('socket:[' + inode + ']' in owned for inode in inodes):
                raise deploy.DeployError('expected loopback socket is not owned by service')
            conn = http.client.HTTPConnection('127.0.0.1', PORT, timeout=3)
            conn.request('POST', '/health', b'{}', headers={'Content-Type': 'application/json'})
            response = conn.getresponse()
            status = response.status
            response.read()
            conn.close()
            if status != 401:
                raise deploy.DeployError('expected unauthenticated health rejection')
            checks.append({'pid': state['MainPID'], 'n_restarts': state['NRestarts'], 'status': status})
        if len({row['pid'] for row in checks}) != 1:
            raise deploy.DeployError('service restarted during health verification')
        return {'checks': checks, 'release': str(self.current()), 'listen': '127.0.0.1:8876',
                'credentials_read': False, 'task_writes': 0}


class InstalledAdapter(LabAdapter):
    """Fixed systemd operations on an explicitly bound common installation."""

    def __init__(self, deployment_manifest):
        self.deployment_manifest = deployment_manifest
        self.verify_target()
        self.account = pwd.getpwnam(self.user)

    def verify_target(self):
        validate_install_target(self.deployment_manifest)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('prepare', 'apply', 'recover', 'health'))
    parser.add_argument('--release')
    parser.add_argument('--operation')
    parser.add_argument('--plan-sha256')
    parser.add_argument('--late-review')
    parser.add_argument('--late-review-sha256')
    parser.add_argument('--deployment-manifest',
                        help='pinned common installation manifest SHA256; legacy labs use a private hostname binding')
    args = parser.parse_args()
    try:
        if args.action != 'prepare' and (args.late_review is not None or args.late_review_sha256 is not None):
            raise deploy.DeployError('late review is only supplied when preparing an operation')
        adapter = LabAdapter() if args.deployment_manifest is None else InstalledAdapter(args.deployment_manifest)
        if args.action == 'health':
            result = adapter.health()
        else:
            if not args.operation or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,63}', args.operation):
                raise deploy.DeployError('explicit simple operation ID required')
            OPERATIONS.mkdir(mode=0o755, exist_ok=True)
            deploy.regular(OPERATIONS, directory=True)
            operation = OPERATIONS / args.operation
            if args.action == 'prepare':
                if not args.release or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,63}', args.release):
                    raise deploy.DeployError('explicit installed release ID required')
                review = deploy.read_review(args.late_review, args.late_review_sha256)
                release = ROOT / 'releases' / args.release
                plan = {'service': SERVICE, 'ledger': str(LEDGER), 'artifacts': str(ARTIFACTS),
                        'old_release': str(adapter.current()), 'release': str(release)}
                adapter.validate(plan)
                with adapter.maintenance_lock():
                    result = deploy.prepare(release, LEDGER, ARTIFACTS, operation, adapter.current(), adapter, review=review)
            else:
                if not args.plan_sha256:
                    raise deploy.DeployError('explicit reviewed plan hash required')
                result = deploy.apply(operation, args.plan_sha256, adapter, recovery_only=args.action == 'recover')
        print(json.dumps({'ok': True, 'result': result}, sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({'ok': False, 'error': type(exc).__name__}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
