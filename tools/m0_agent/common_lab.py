"""Explicit bounded acceptance steps for the isolated common Debian installation.

Only prepare registers one public instructional prompt; it never calls a provider.
HTTP and research probes retain their synthetic labels and do not access business data.
"""
import argparse
import base64
from contextlib import closing
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import hashlib
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / 'dispatch'), str(ROOT)]
from sa_dispatch.agent_control import BUDGET_ID
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.clock import timestamp
from sa_dispatch.collector import Collector
from sa_dispatch.collector_cli import load_binding
from sa_dispatch.external_migration import verify_schema
from sa_dispatch.models import Ref, TaskSpec
from tools.m0_agent.common_runtime import CONFIG, LEDGER, CAS, SCHEMA, _safe_system_path, read_config
from tools.m0_agent.common_worker import ledger_identity, status
from tools.m0_agent.worker import OperatorGrant

PROMPT = ('用中文写三条简短的 Linux 服务上线检查建议，分别说明：启动后检查健康状态；'
          '重启后确认任务不会重复调用付费 API；进程异常退出时保留不确定状态，先核对回执。'
          '仅输出建议，不执行命令，不声称任何具体机器已经通过验收。总计不超过120个汉字。')


def check(value, reason):
    if not value: raise ValueError(reason)


def write_new(path, value, mode=0o640):
    import grp
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, mode)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    os.chown(path, 0, grp.getgrnam('sa-dispatch-ledger').gr_gid)


def prepare(machine, grant_id=None):
    import grp
    check(os.geteuid() == 0, 'root required')
    check(Path('/etc/machine-id').read_text().strip() == machine, 'machine identity mismatch')
    with closing(sqlite3.connect(LEDGER.as_uri() + '?mode=ro', uri=True)) as db:
        check(verify_schema(db) == '0013', '0013 required')
        registered = db.execute('SELECT 1 FROM control_budgets WHERE budget_id=?', (BUDGET_ID,)).fetchone()
    if os.path.lexists(CONFIG):
        _safe_system_path(CONFIG, directory=False)
        existing = read_config(CONFIG)
        check(existing.machine_id == machine and (grant_id is None or existing.grant.grant_id == grant_id),
              'existing grant differs')
        return status(LEDGER, ArtifactStore(CAS), existing)
    check(registered is None, 'registered grant configuration is missing; recover the original configuration')
    check(grant_id is not None, 'explicit --grant-id required when preparing a new live grant')
    check(not Path('/etc/sa-dispatch/provider-network.json').exists(), 'unexpected existing network config')
    now = datetime.now(timezone.utc)
    artifacts = ArtifactStore(CAS)
    prompt = PROMPT.encode('utf-8')
    digest = 'sha256:' + hashlib.sha256(prompt).hexdigest()
    grant = OperatorGrant(grant_id, 1,
        Ref('operator', 'agent_prompt', 'linux-service-checklist', '1', digest),
        'public', 'deepseek', 'deepseek-flash', 256, timestamp(now), 'derived', 'live')
    artifacts.put(prompt)
    # ArtifactStore deliberately creates private files; publish only this approved prompt.
    blob = artifacts.path(digest)
    check(blob.is_file(), 'prompt CAS layout differs')
    group = grp.getgrnam('sa-dispatch-ledger').gr_gid
    os.chown(blob.parent, 0, group); os.chmod(blob.parent, 0o2770)
    os.chown(blob, 0, group); os.chmod(blob, 0o640)
    document = {'schema': SCHEMA, 'grant': asdict(grant), 'machine_id': machine,
                'ledger_identity': ledger_identity(LEDGER), 'expires_at': timestamp(now + timedelta(hours=8))}
    write_new(CONFIG, document)
    write_new(Path('/etc/sa-dispatch/provider-network.json'),
              {'schema': 'superagent.provider-network.v1',
               'https_connect_proxy': {'host': '10.0.2.2', 'port': 7897}})
    return {'prepared': True, 'prompt_ref': digest, 'request_limit': 1, 'max_tokens': 256}


def http_probe():
    tokens = json.loads(Path('/var/lib/sa-common-install/http-local-tokens.json').read_bytes())['tokens']
    def post(endpoint, data, worker=False, denied=False):
        name = 'common-http-synthetic' if worker else 'common-http-dispatcher'
        with closing(HTTPConnection('127.0.0.1', 8876, timeout=8)) as conn:
            conn.request('POST', endpoint, json.dumps(data),
                         {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + tokens[name]})
            response = conn.getresponse(); body = json.loads(response.read(262144))
        if denied:
            check(response.status in (403, 404), 'foreign route was exposed')
            return body['error']['code']
        check(response.status == 200 and body.get('ok'), 'HTTP acceptance refused')
        return body['data']
    spec = TaskSpec('common-external-http-001', 'hotspot.topic_fetch', 1,
                    {'op': 'topic.list', 'filters': {}}, (), ('component:mgmtcore',), 'synthetic', 'none', 1)
    submitted = post('/submit', {'spec': asdict(spec), 'inputs': {}})
    task = submitted
    if task['status'] == 'queued':
        lease = post('/claim', {}, True)['lease']
        check(lease and lease['task_id'] == task['task_id'], 'unexpected HTTP task')
        args = {key: lease[key] for key in ('lease_id', 'fence')}
        output = b'{"synthetic":true,"component":"HTTP acceptance","count":0}'
        digest = 'sha256:' + hashlib.sha256(output).hexdigest()
        post('/artifact/put', {**args, 'hash': digest, 'content_base64': base64.b64encode(output).decode()}, True)
        post('/complete', {**args, 'outcome': {'status': 'succeeded', 'result': {'count': 0},
             'evidence': [{'kind': 'fixture_result', 'content_hash': digest, 'access': 'synthetic',
                           'artifact_hash': digest}]}}, True)
    own = post('/status', {'task_id': task['task_id']}, True)
    check(own['status'] == 'succeeded', 'HTTP task not complete')
    agent = status(LEDGER, ArtifactStore(CAS), read_config(CONFIG))
    denial = post('/status', {'task_id': agent['task_id']}, True, True)
    check(post('/claim', {}, True)['lease'] is None, 'HTTP claimed another route')
    return {'http': own, 'external_status_denied': denial}


def local_probe(machine, manifest):
    base = ['/usr/bin/python3', '-I', '-B', str(ROOT / 'tools/m0_agent/common_install.py'),
            'local-run', '--expected-machine', machine, '--manifest-sha256', manifest, '--']
    def call(args, payload=None):
        run = subprocess.run([*base, *args], input=json.dumps(payload).encode() if payload else b'',
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=45)
        check(run.returncode == 0, 'guarded local subprocess refused: ' + run.stdout[-1000:].decode(errors='replace'))
        body = json.loads(run.stdout)
        check(body.get('ok'), 'local CLI refused')
        return body['data']
    call(['device-register'])
    params = json.loads((ROOT / 'knowledge/m0_search/fixtures/three-source-request.json').read_bytes())
    spec = TaskSpec('common-external-research-001', 'research.search', 1, params, (),
                    ('research:read', 'signalhub:refs'), 'synthetic', 'none', 1)
    row = call(['submit'], asdict(spec))['task']
    if row['status'] == 'queued': call(['run-once'])
    final = call(['status', row['task_id']])['task']
    check(final['status'] == 'succeeded', 'local research did not succeed')
    return {'task_id': final['task_id'], 'status': final['status'], 'route': final['execution_route'],
            'result': final['result']}


def collect():
    target, sources, _ = load_binding(Path('/etc/sa-dispatch/collector-binding.json'))
    binding = sources['common-dispatch']; collector = Collector(target)
    result = {stream: collector.sync(binding, stream)
              for stream in ('events', 'control_events', 'schedule_events')}
    check(all(row['state'] == 'synced' for row in result.values()), 'collector frozen')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('prepare', 'http', 'local', 'collect', 'status'))
    parser.add_argument('--machine', required=True)
    parser.add_argument('--manifest')
    parser.add_argument('--grant-id', help='explicit operator grant ID for initial prepare')
    args = parser.parse_args()
    if args.grant_id is not None and args.command != 'prepare':
        parser.error('--grant-id is only used by prepare')
    check(os.geteuid() == 0 and Path('/etc/machine-id').read_text().strip() == args.machine,
          'explicit root-owned lab target required')
    if args.command == 'prepare': result = prepare(args.machine, args.grant_id)
    elif args.command == 'http': result = http_probe()
    elif args.command == 'local': result = local_probe(args.machine, args.manifest)
    elif args.command == 'collect': result = collect()
    else: result = status(LEDGER, ArtifactStore(CAS), read_config(CONFIG))
    print(json.dumps(result, sort_keys=True, ensure_ascii=True))


if __name__ == '__main__': main()
