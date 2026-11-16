"""Unattended fresh-guest trial installer. Never upgrades an existing deployment."""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sqlite3
import subprocess
import sys
import time

from credentials import consume_systemd_credential, import_key, protected

BASE = Path('/opt/superagent-trial')
CONFIG = Path('/etc/superagent-trial')
DATA = Path('/var/lib/superagent-trial')
UNIT = 'superagent-trial.service'
PORT = 18776


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def safe(path):
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('unsafe path')
    for part in (path, *path.parents):
        if part.is_symlink():
            raise ValueError('symlink path refused')
        if part.exists() and part.is_file() and part.stat().st_nlink != 1:
            raise ValueError('hardlink path refused')
    return path


def atomic(path, raw, mode=0o600):
    safe(path)
    tmp = path.with_name('.' + path.name + '-' + secrets.token_hex(8))
    with tmp.open('xb') as handle:
        handle.write(raw); handle.flush(); os.fsync(handle.fileno())
    tmp.chmod(mode)
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fixed(path, raw, mode=0o600):
    safe(path)
    if path.exists():
        if path.read_bytes() != raw:
            raise ValueError('existing installation file differs')
    else:
        atomic(path, raw, mode)


def verify_package(package, expected):
    safe(package)
    raw = safe(package / 'INSTALL_MANIFEST.json').read_bytes()
    if sha(raw) != expected:
        raise ValueError('package manifest does not match trusted digest')
    manifest = json.loads(raw)
    if manifest['scope'] != 'synthetic_installer_trial_not_full_M0':
        raise ValueError('wrong package scope')
    names = set()
    for path in package.rglob('*'):
        safe(path)
        if path.is_file():
            names.add(path.relative_to(package).as_posix())
    if names != set(manifest['files']) | {'INSTALL_MANIFEST.json'}:
        raise ValueError('package file set differs')
    for name, digest in manifest['files'].items():
        if name.startswith('/') or '..' in Path(name).parts or '\\' in name:
            raise ValueError('unbounded package path')
        if sha(safe(package / name).read_bytes()) != digest:
            raise ValueError('package content differs')
    return manifest


def run(*args):
    # Errors never print command output: it could contain deployment details.
    result = subprocess.run(args, capture_output=True, timeout=30)
    if result.returncode:
        raise ValueError('required system operation failed')
    return result.stdout.decode().strip()


def service():
    proof = consume_systemd_credential()
    proof.update(schema='superagent.installer_startup.v1', invocation_id=os.environ.get('INVOCATION_ID'),
                 scope='synthetic_installer_trial_not_full_M0', pid=os.getpid())
    atomic(DATA / 'startup-proof.json', (json.dumps(proof)+'\n').encode())
    release = safe(BASE / 'installed.json')
    state = json.loads(release.read_bytes())
    os.chdir(BASE / 'releases' / state['manifest_sha256'] / 'payload' / 'dispatch')
    os.execv('/usr/bin/python3', ['/usr/bin/python3','-B','-m','sa_dispatch',
        '--config',str(CONFIG/'coordinator.toml'),'serve','--identities',str(CONFIG/'identities.json'),
        '--port',str(PORT)])


def check():
    installed = json.loads(safe(BASE / 'installed.json').read_bytes())
    verify_package(BASE/'releases'/installed['manifest_sha256'], installed['manifest_sha256'])
    fields = dict(line.split('=',1) for line in run('systemctl','show',UNIT,
        '--property=ActiveState,SubState,MainPID,InvocationID,User,Group').splitlines())
    proof = json.loads(safe(DATA/'startup-proof.json').read_bytes())
    assert fields['ActiveState']=='active' and fields['SubState']=='running'
    assert fields['User']==fields['Group']=='sa-trial'
    assert proof['credential_read'] and proof['invocation_id']==fields['InvocationID']
    assert str(proof['pid'])==fields['MainPID']
    conn=http.client.HTTPConnection('127.0.0.1',PORT,timeout=3)
    conn.request('POST','/health',b'{}',{'Content-Type':'application/json'})
    response=conn.getresponse(); code=response.status; response.read(); conn.close()
    assert code==401
    db=sqlite3.connect((DATA/'ledger.sqlite3').as_uri()+'?mode=ro',uri=True)
    try:
        assert db.execute('PRAGMA quick_check').fetchone()[0]=='ok'
        mode=db.execute('SELECT mode FROM ledger_mode').fetchone()[0]
        tasks=db.execute('SELECT count(*) FROM tasks').fetchone()[0]
    finally:
        db.close()
    report={'schema':'superagent.installer_check.v1','ok':True,'scope':installed['scope'],
        'manifest_sha256':installed['manifest_sha256'],'active':True,'mode':mode,'tasks':tasks,
        'credential_read':True,'model_calls':0,'value_disclosed':False,'anonymous_health_status':code,
        'full_M0_ready':False}
    atomic(DATA/'self-check.json',(json.dumps(report)+'\n').encode())
    return report


def install(package, expected, secret_file):
    if os.geteuid()!=0 or not Path('/run/systemd/system').is_dir() or sys.version_info<(3,11):
        raise ValueError('running Linux systemd root and Python3.11+ required')
    manifest=verify_package(package,expected)
    protected(secret_file)
    for path in (BASE,CONFIG,DATA,Path('/etc/systemd/system')/UNIT):safe(path)
    if BASE.exists() and not (BASE/'install-intent.json').exists():
        if BASE.stat().st_uid!=0 or any(p.name!='.install.lock' for p in BASE.iterdir()):
            raise ValueError('unrecognized trial installation root')
    if not BASE.exists() and any(p.exists() for p in (CONFIG,DATA,Path('/etc/systemd/system')/UNIT)):
        raise ValueError('existing trial state has no installation identity')
    BASE.mkdir(mode=0o755,exist_ok=True)
    import fcntl
    with (BASE/'.install.lock').open('a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        identity={'scope':manifest['scope'],'manifest_sha256':expected}
        fixed(BASE/'install-intent.json',json.dumps(identity,sort_keys=True).encode(),0o644)
        # No in-place upgrade: only this exact package can resume.
        releases=BASE/'releases';releases.mkdir(mode=0o755,exist_ok=True)
        release=releases/expected;release.mkdir(mode=0o755,exist_ok=True)
        for name in sorted([*manifest['files'],'INSTALL_MANIFEST.json']):
            target=release/name;safe(target);target.parent.mkdir(parents=True,exist_ok=True,mode=0o755)
            fixed(target,(package/name).read_bytes(),0o644)
        verify_package(release,expected)
        import pwd,grp
        try:
            account=pwd.getpwnam('sa-trial')
        except KeyError:
            run('useradd','--system','--user-group','--home-dir',str(DATA),'--shell','/usr/sbin/nologin','sa-trial')
            account=pwd.getpwnam('sa-trial')
        if account.pw_dir!=str(DATA) or account.pw_shell!='/usr/sbin/nologin' or grp.getgrgid(account.pw_gid).gr_name!='sa-trial':
            raise ValueError('unexpected trial account')
        CONFIG.mkdir(mode=0o750,exist_ok=True);os.chown(CONFIG,0,account.pw_gid)
        DATA.mkdir(mode=0o700,exist_ok=True);os.chown(DATA,account.pw_uid,account.pw_gid)
        import_key(secret_file,CONFIG/'provider.key')
        configuration={'db_path':str(DATA/'ledger.sqlite3'),'artifact_root':str(DATA/'artifacts'),
            'journal_dir':str(DATA/'journal'),'device_id':'trial-coordinator','capabilities':[],
            'plugins':['hotspot'],'synthetic':True,'lease_ttl_seconds':60,'component_timeout_seconds':10,'components':{}}
        fixed(CONFIG/'coordinator.toml',('\n'.join(k+' = '+json.dumps(v) for k,v in configuration.items())+'\n').encode(),0o640)
        auth=CONFIG/'identities.json'
        if not auth.exists():
            token=secrets.token_bytes(32)
            fixed(auth,json.dumps({'devices':{'trial-dispatcher':{'token_sha256':sha(token),'roles':['dispatcher']}}}).encode(),0o640)
        for path in (CONFIG/'coordinator.toml',auth):os.chown(path,0,account.pw_gid)
        installed=json.dumps(identity,sort_keys=True).encode()
        fixed(BASE/'installed.json',installed,0o644)
        script=release/'installer/trial.py'
        unit=f'''[Unit]
Description=SuperAgent isolated installation trial (not full M0)
After=local-fs.target
[Service]
Type=exec
User=sa-trial
Group=sa-trial
StateDirectory=superagent-trial
StateDirectoryMode=0700
LoadCredential=provider_api_key:{CONFIG}/provider.key
ExecStart=/usr/bin/python3 -B {script} service
Restart=on-failure
RestartSec=5
UMask=0077
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
RestrictAddressFamilies=AF_UNIX AF_INET
IPAddressDeny=any
IPAddressAllow=localhost
[Install]
WantedBy=multi-user.target
'''
        fixed(Path('/etc/systemd/system')/UNIT,unit.encode(),0o644)
        run('systemctl','daemon-reload')
        run('systemctl','enable','--now',UNIT)
        for _ in range(20):
            try:return check()
            except (OSError,ValueError,AssertionError,sqlite3.Error):time.sleep(.25)
        raise ValueError('trial failed self-check; retained all state')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('install','check','service'))
    parser.add_argument('--package',type=Path)
    parser.add_argument('--manifest-sha256')
    parser.add_argument('--secret-file',type=Path)
    args=parser.parse_args()
    try:
        if args.command=='service':return service()
        report=check() if args.command=='check' else install(args.package,args.manifest_sha256,args.secret_file)
        print(json.dumps(report));return 0
    except Exception as error:
        print(json.dumps({'ok':False,'error':type(error).__name__,'details':'trial refused; state preserved; no secret output'}))
        return 1


if __name__=='__main__':raise SystemExit(main())
