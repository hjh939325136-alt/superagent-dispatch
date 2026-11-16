"""Fresh R1 installation with one authorized scheduled agent task; no disk actions."""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
import time
import tempfile

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from credentials import import_key, protected
    from trial import atomic, fixed, safe, run
else:
    from .credentials import import_key, protected
    # trial is retained unchanged as the already-frozen rehearsal entry point.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from .trial import atomic, fixed, safe, run

BASE = Path('/opt/superagent-r1')
CONFIG = Path('/etc/superagent-r1')
DATA = Path('/var/lib/superagent-r1')
SERVICE = 'superagent-r1-agent.service'
TIMER = 'superagent-r1-agent.timer'
SCHEMA = 'superagent.r1.package.v1'
PROMPT = 'Synthetic server verification: reply with a short checklist for checking a scheduled task. Do not access tools, websites, or private information.'


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def verify(package, expected):
    if not isinstance(expected, str) or re.fullmatch('[0-9a-f]{64}', expected) is None:
        raise ValueError('explicit package digest required')
    package = safe(Path(package))
    raw = safe(package / 'INSTALL_MANIFEST.json').read_bytes()
    if sha(raw) != expected:
        raise ValueError('package digest mismatch')
    manifest = json.loads(raw)
    if manifest.get('schema') != SCHEMA or manifest.get('scope') != 'r1_single_scheduled_agent':
        raise ValueError('package scope differs')
    files = manifest.get('files')
    if not isinstance(files, dict) or not 1 <= len(files) <= 1000:
        raise ValueError('invalid package file count')
    actual = {p.relative_to(package).as_posix() for p in package.rglob('*') if safe(p).is_file()}
    if actual != set(files) | {'INSTALL_MANIFEST.json'}:
        raise ValueError('package file set differs')
    total = 0
    for name, digest in files.items():
        p = PurePosixPath(name)
        if (str(p) != name or p.is_absolute() or '..' in p.parts or '\\' in name
                or not name.startswith(('payload/', 'installer/'))):
            raise ValueError('package path outside approved roots')
        path = safe(package / name)
        total += path.stat().st_size
        if total > 8 * 1024 * 1024 or sha(path.read_bytes()) != digest:
            raise ValueError('package content invalid or too large')
    required = {'installer/r1.py', 'installer/credentials.py', 'installer/trial.py',
                'payload/tools/m0_agent/runtime.py', 'payload/tools/m0_agent/worker.py',
                'payload/tools/m0_agent/provider_process.py', 'payload/tools/m0_installer/credentials.py',
                'payload/tools/m0_agent/readonly_status.py',
                'payload/dispatch/sa_dispatch/store_sqlite.py'}
    if not required <= files.keys():
        raise ValueError('package runtime closure incomplete')
    return manifest


def make_config(identity):
    prompt_hash = 'sha256:' + sha(PROMPT.encode())
    return {'schema':'superagent.r1.agent.v1', 'execution_mode':identity['execution_mode'],
        'grant':{'grant_id':'r1-install-' + identity['manifest_sha256'][:12], 'version':1,
            'prompt_ref':{'owner':'operator','kind':'agent_prompt','id':'r1-install-check','version':'1','hash':prompt_hash},
            'source':'synthetic','provider':'deepseek','model':identity['model'],'max_tokens':64,
            'scheduled_at':identity['scheduled_at'],'egress':'derived'}}


def service_bytes(release, execution_mode):
    # Network is denied entirely in simulation; live code fixes one TLS endpoint.
    network = 'IPAddressDeny=any\n' if execution_mode == 'simulation' else ''
    return f'''[Unit]
Description=SuperAgent R1 single authorized scheduled task
After=network-online.target
Wants=network-online.target
[Service]
Type=oneshot
User=sa-r1
Group=sa-r1
WorkingDirectory={release}/payload
Environment=PYTHONPATH={release}/payload/dispatch:{release}/payload
LoadCredential=provider_api_key:{CONFIG}/provider.key
ExecStart=/usr/bin/python3 -P -B -m tools.m0_agent.runtime tick
TimeoutStartSec=70
KillMode=control-group
UMask=0077
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths={DATA}
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
LimitCORE=0
MemoryMax=256M
TasksMax=32
{network}'''.encode()


TIMER_BYTES = f'''[Unit]
Description=SuperAgent R1 authorized schedule poll
[Timer]
OnBootSec=5
OnUnitInactiveSec=5
AccuracySec=1
Unit={SERVICE}
[Install]
WantedBy=timers.target
'''.encode()


def owned(path, uid, gid, mode):
    info = safe(path).stat()
    if (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) != (uid, gid, mode):
        raise ValueError('unexpected installation ownership or permissions')


def make_directory(path, uid, gid, mode):
    safe(path)
    if path.exists():
        owned(path, uid, gid, mode)
    else:
        # Parent is root-controlled. Publish only a fully configured directory;
        # interrupted staging is retained outside the final installation path.
        staged = Path(tempfile.mkdtemp(prefix='.' + path.name + '-stage-', dir=path.parent))
        os.chown(staged, uid, gid)
        staged.chmod(mode)
        os.rename(staged, path)


def runtime_command(release, command):
    return ('runuser','-u','sa-r1','--','/usr/bin/env','-i','PATH=/usr/bin:/bin',
        'PYTHONPATH='+str(release/'payload/dispatch')+':'+str(release/'payload'),
        '/usr/bin/python3','-P','-B','-m','tools.m0_agent.runtime',command)


def readonly_entry_bytes(release):
    """Fixed root entry for a separately provisioned forced-command SSH account."""
    if release.parent != BASE / 'releases' or re.fullmatch('[0-9a-f]{64}', release.name) is None:
        raise ValueError('readonly entry requires a fixed R1 release')
    return f'''#!/bin/sh
if [ "$#" -ne 0 ]; then
    printf '%s\\n' '{{"schema_version":1,"ok":false,"error":"r1_status_invalid_arguments"}}'
    exit 2
fi
if [ "$(/usr/bin/id -u)" != 0 ]; then
    printf '%s\\n' '{{"schema_version":1,"ok":false,"error":"r1_status_refused"}}'
    exit 1
fi
exec /usr/bin/env -i PATH=/usr/bin:/bin /usr/bin/timeout --kill-after=2s 15s /usr/sbin/runuser -u sa-r1 -- /usr/bin/env -i PATH=/usr/bin:/bin PYTHONPATH={release}/payload/dispatch:{release}/payload /usr/bin/python3 -P -B -m tools.m0_agent.readonly_status
'''.encode('ascii')


def validate_key(source):
    # R1's stricter preflight precedes the immutable credential import.
    source = protected(source)
    value = source.read_bytes()
    if re.fullmatch(rb'[A-Za-z0-9._~+/=-]{16,1024}', value) is None:
        raise ValueError('R1 credential format invalid')


def seed_prompt():
    # All writes below the service-owned state root execute as that account.
    # Root never follows service-controlled paths to chown a file.
    script = '''import hashlib,os,pathlib,stat,sys,tempfile
root=pathlib.Path(sys.argv[1]); content=sys.argv[2].encode(); digest=hashlib.sha256(content).hexdigest()
os.umask(0o077)
for folder in (root/'artifacts',root/'journal',root/'artifacts/sha256',root/'artifacts/sha256'/digest[:2]):
    folder.mkdir(mode=0o700,exist_ok=True)
    info=folder.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o700: raise ValueError('unsafe state directory')
path=root/'artifacts/sha256'/digest[:2]/digest
if path.exists() or path.is_symlink():
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as f:
        info=os.fstat(f.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_nlink!=1 or stat.S_IMODE(info.st_mode)!=0o600 or f.read(len(content)+1)!=content: raise ValueError('prompt differs')
else:
    with tempfile.NamedTemporaryFile(dir=path.parent,prefix='.prompt-',delete=False) as f:
        staged=f.name;f.write(content);f.flush();os.fsync(f.fileno())
    os.replace(staged,path)
    fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)
'''
    run('runuser','-u','sa-r1','--','/usr/bin/python3','-I','-B','-c',script,str(DATA),PROMPT)


def check():
    identity = json.loads(safe(BASE / 'install-intent.json').read_bytes())
    release = BASE / 'releases' / identity['manifest_sha256']
    verify(release, identity['manifest_sha256'])
    entry = BASE / 'bin' / 'manager-readonly'
    owned(entry.parent, 0, 0, 0o755)
    owned(entry, 0, 0, 0o755)
    if entry.read_bytes() != readonly_entry_bytes(release):
        raise ValueError('readonly entry changed')
    configuration = json.loads(safe(CONFIG / 'agent.json').read_bytes())
    protected(CONFIG / 'provider.key')
    if configuration != make_config(identity):
        raise ValueError('runtime grant changed from installation identity')
    if safe(Path('/etc/systemd/system') / SERVICE).read_bytes() != service_bytes(release, identity['execution_mode']):
        raise ValueError('runtime service changed')
    if safe(Path('/etc/systemd/system') / TIMER).read_bytes() != TIMER_BYTES:
        raise ValueError('runtime timer changed')
    state = run('systemctl','show',TIMER,'--property=ActiveState','--value')
    if state != 'active':
        raise ValueError('R1 timer not active')
    health = dict(line.split('=',1) for line in run('systemctl','show',SERVICE,
        '--property=Result,ExecMainStatus,NeedDaemonReload,FragmentPath').splitlines())
    if (health.get('Result') != 'success' or health.get('ExecMainStatus') != '0'
            or health.get('NeedDaemonReload') != 'no'
            or health.get('FragmentPath') != '/etc/systemd/system/' + SERVICE):
        raise ValueError('R1 service health differs')
    raw = run(*runtime_command(release,'check'))
    report = json.loads(raw)
    grant=configuration['grant']
    proof=report.get('credential_read_proof')
    if (report.get('ledger_exists') is not True or report.get('status') != 'succeeded'
            or report.get('execution_mode') != identity['execution_mode']
            or report.get('grant_id') != grant['grant_id'] or not report.get('task_id')
            or not report.get('result_ref') or not isinstance(proof,dict)
            or proof.get('credential_read') is not True or proof.get('task_id') != report['task_id']
            or proof.get('status') != report['status'] or proof.get('execution_mode') != identity['execution_mode']
            or proof.get('grant_id') != grant['grant_id']
            or not isinstance(proof.get('invocation_id'),str)
            or re.fullmatch('[0-9a-f]{32}',proof['invocation_id']) is None):
        raise ValueError('scheduled runtime or credential proof not ready')
    if identity['execution_mode']=='simulation' and (report.get('real_calls') != 0
            or report.get('provider') != 'deepseek'
            or report.get('model') != 'simulated-' + identity['model']):
        raise ValueError('simulation proof is not explicit')
    if identity['execution_mode']=='live' and report.get('provider') != 'deepseek':
        raise ValueError('live provider proof differs')
    return {'schema':'superagent.r1.install_check.v1','manifest_sha256':identity['manifest_sha256'],
        'execution_mode':identity['execution_mode'], 'timer_active':True,'runtime':report,
        'full_system_ready':False,'physical_device_verified':False}


def install(package, expected, secret_file, execution_mode, model):
    if os.name != 'posix' or os.geteuid() != 0 or not Path('/run/systemd/system').is_dir():
        raise ValueError('running Linux systemd root required')
    if execution_mode not in ('simulation','live') or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,127}',model or ''):
        raise ValueError('explicit mode and model required')
    manifest = verify(package, expected)
    validate_key(secret_file)
    for path in (BASE,CONFIG,DATA,Path('/etc/systemd/system')/SERVICE,Path('/etc/systemd/system')/TIMER):safe(path)
    if not BASE.exists() and any(p.exists() for p in (CONFIG,DATA,Path('/etc/systemd/system')/SERVICE,Path('/etc/systemd/system')/TIMER)):
        raise ValueError('unrecognized existing R1 state')
    if BASE.exists() and not (BASE/'install-intent.json').exists() and any(p.name!='.install.lock' for p in BASE.iterdir()):
        raise ValueError('unrecognized installation root')
    make_directory(BASE,0,0,0o755)
    import fcntl,grp,pwd
    with (BASE/'.install.lock').open('a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        intent = BASE/'install-intent.json'
        if intent.exists():
            identity=json.loads(safe(intent).read_bytes())
            if (identity.get('manifest_sha256'),identity.get('execution_mode'),identity.get('model')) != (expected,execution_mode,model):
                raise ValueError('different package/mode/model requires explicit upgrade')
        else:
            identity={'schema':'superagent.r1.install_intent.v1','manifest_sha256':expected,
                'execution_mode':execution_mode,'model':model,
                'scheduled_at':(datetime.now(timezone.utc)+timedelta(seconds=10)).isoformat(timespec='microseconds')}
            fixed(intent,encoded(identity),0o644)
        releases=BASE/'releases';make_directory(releases,0,0,0o755)
        release=releases/expected
        if not release.exists():
            staged=Path(tempfile.mkdtemp(prefix='.release-stage-',dir=releases))
            staged.chmod(0o755)
            for name in sorted([*manifest['files'],'INSTALL_MANIFEST.json']):
                target=staged/name;target.parent.mkdir(parents=True,exist_ok=True,mode=0o755)
                fixed(target,(package/name).read_bytes(),0o644)
            verify(staged,expected)
            os.rename(staged,release)
        owned(release,0,0,0o755)
        verify(release,expected)
        for path in release.rglob('*'):
            owned(path,0,0,0o755 if path.is_dir() else 0o644)
        make_directory(BASE/'bin',0,0,0o755)
        fixed(BASE/'bin'/'manager-readonly',readonly_entry_bytes(release),0o755)
        owned(BASE/'bin'/'manager-readonly',0,0,0o755)
        try:account=pwd.getpwnam('sa-r1')
        except KeyError:
            run('useradd','--system','--user-group','--home-dir',str(DATA),'--shell','/usr/sbin/nologin','sa-r1')
            account=pwd.getpwnam('sa-r1')
        if account.pw_dir!=str(DATA) or account.pw_shell!='/usr/sbin/nologin' or grp.getgrgid(account.pw_gid).gr_name!='sa-r1':
            raise ValueError('R1 service account differs')
        make_directory(CONFIG,0,account.pw_gid,0o750)
        make_directory(DATA,account.pw_uid,account.pw_gid,0o700)
        import_key(secret_file,CONFIG/'provider.key')
        config=CONFIG/'agent.json';fixed(config,encoded(make_config(identity)),0o640)
        os.chown(config,0,account.pw_gid)
        # Seed only the fixed approved synthetic input; no model output exists yet.
        seed_prompt()
        fixed(Path('/etc/systemd/system')/SERVICE,service_bytes(release,execution_mode),0o644)
        fixed(Path('/etc/systemd/system')/TIMER,TIMER_BYTES,0o644)
        for name in (SERVICE,TIMER):owned(Path('/etc/systemd/system')/name,0,0,0o644)
        run('systemctl','daemon-reload')
        run('systemctl','enable','--now',TIMER)
        # Startup is owned by the timer; installer does not issue a duplicate task.
        for _ in range(100):
            try:
                report=check()
                atomic(BASE/'last-install-check.json',encoded(report),0o600)
                return report
            except (OSError,ValueError,KeyError):time.sleep(1)
        raise ValueError('R1 runtime not ready; all state retained')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['install','check'])
    parser.add_argument('--package',type=Path);parser.add_argument('--manifest-sha256')
    parser.add_argument('--secret-file',type=Path);parser.add_argument('--mode',choices=['simulation','live'])
    parser.add_argument('--model')
    args=parser.parse_args()
    try:
        report=check() if args.command=='check' else install(args.package,args.manifest_sha256,args.secret_file,args.mode,args.model)
        print(json.dumps(report));return 0
    except Exception as error:
        print(json.dumps({'ok':False,'error':type(error).__name__,'details':'R1 refused; state retained; no secret output'}));return 1


if __name__=='__main__':raise SystemExit(main())
