"""Public CLI/service scheduling and real multi-process failure tests."""
from dataclasses import asdict
from contextlib import closing
from datetime import datetime, timezone, timedelta
import hashlib
import base64
import json
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
import urllib.error

from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.models import TaskSpec
from sa_dispatch.scheduler import Scheduler
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch_hotspot import KINDS

ROOT = Path(__file__).resolve().parents[1]


class SchedulerProcessTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.db = self.root / 'ledger.sqlite3'
        self.config = self.root / 'dispatch.toml'
        self.identities = self.root / 'identities.json'
        self.token = secrets.token_urlsafe(32)
        self.identities.write_text(json.dumps({'devices': {'worker': {
            'token_sha256': hashlib.sha256(self.token.encode()).hexdigest(),
            'roles': ['worker'], 'capabilities': ['component:mgmtcore']}}}), encoding='utf-8')
        values = {'db_path': str(self.db), 'artifact_root': str(self.root / 'artifacts'),
                  'journal_dir': str(self.root / 'journal'), 'device_id': 'coordinator',
                  'capabilities': [], 'plugins': ['hotspot'], 'lease_ttl_seconds': 2,
                  'component_timeout_seconds': 1, 'synthetic': True}
        self.config.write_text('\n'.join(key + ' = ' + json.dumps(value) for key, value in values.items())
                               + '\n[components]\n', encoding='utf-8')
        self.spec = TaskSpec('template', 'hotspot.topic_fetch', 1, {'op': 'topic.list', 'filters': {}},
                             (), ('component:mgmtcore',), 'synthetic', 'none', 3)

    def cli(self, *args):
        result = subprocess.run([sys.executable, '-B', '-m', 'sa_dispatch', '--config', str(self.config), *args],
                                cwd=ROOT, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return json.loads(result.stdout)['data']

    def upsert(self, interval=60):
        request = self.root / 'schedule.json'
        request.write_text(json.dumps({'schedule_id': 'public-read', 'interval_seconds': interval,
            'task_template': asdict(self.spec), 'next_fire_at': timestamp(datetime.now(timezone.utc)),
            'catchup': 'latest', 'max_catchup': 3}), encoding='utf-8')
        return self.cli('schedule-upsert', '--input', str(request))

    def test_cli_bound_omission_preserves_and_explicit_null_removes(self):
        request=self.root/'bounded.json'
        value={'schedule_id':'bounded','interval_seconds':60,'task_template':asdict(self.spec),
               'next_fire_at':timestamp(datetime.now(timezone.utc)),'max_slots':3}
        request.write_text(json.dumps(value),encoding='utf-8')
        self.cli('schedule-upsert','--input',str(request))
        value.pop('max_slots');value['enabled']=False
        request.write_text(json.dumps(value),encoding='utf-8')
        self.cli('schedule-upsert','--input',str(request))
        store=SqliteStore(self.db,lease_ttl_seconds=2,kinds=KINDS)
        self.assertEqual(Scheduler(store).status()['schedules'][0]['max_slots'],3)
        value['max_slots']=None
        request.write_text(json.dumps(value),encoding='utf-8')
        self.cli('schedule-upsert','--input',str(request))
        state=Scheduler(store).status()
        self.assertIsNone(state['schedules'][0]['max_slots'])
        self.assertEqual(sum(e['kind']=='schedule.bound_removed' for e in state['events']),1)

    def start(self):
        logfile = self.root / ('service-' + secrets.token_hex(4) + '.log')
        stream = logfile.open('wb')
        process = subprocess.Popen([sys.executable, '-B', '-m', 'sa_dispatch', '--config', str(self.config),
            'serve', '--identities', str(self.identities), '--port', '0', '--schedules'],
            cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT)
        stream.close()
        self.addCleanup(self.stop, process)
        deadline = time.monotonic() + 15
        prefix = 'Coordinator listening on 127.0.0.1:'
        while time.monotonic() < deadline and process.poll() is None:
            for line in logfile.read_text(errors='replace').splitlines():
                if line.startswith(prefix):
                    return process, int(line[len(prefix):])
            time.sleep(0.03)
        self.fail(logfile.read_text(errors='replace'))

    @staticmethod
    def stop(process):
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)

    def invoke(self, port, route, payload):
        request = urllib.request.Request(f'http://127.0.0.1:{port}{route}',
            data=json.dumps(payload).encode(), headers={'Content-Type':'application/json',
            'Authorization':'Bearer ' + self.token})
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.load(response)['data']

    def complete(self, port, lease):
        payload = {'lease_id': lease['lease_id'], 'fence': lease['fence']}
        receipt = json.dumps({'synthetic':True,'task_id':lease['task_id'],'epoch':lease['fence']}, sort_keys=True).encode()
        digest = 'sha256:' + hashlib.sha256(receipt).hexdigest()
        self.invoke(port, '/artifact/put', dict(payload, hash=digest,
                    content_base64=base64.b64encode(receipt).decode()))
        self.invoke(port, '/complete', dict(payload, outcome={'status':'succeeded', 'result':{'receipt':digest},
            'evidence':[{'kind':'synthetic_receipt','content_hash':digest,'artifact_hash':digest,'access':'synthetic'}]}))
        return {'sha256':digest,'content':json.loads(receipt)}

    def evidence(self, name, data):
        destination = os.environ.get('SA_SCHEDULER_EVIDENCE_DIR')
        if destination:
            folder = Path(destination)
            folder.mkdir(parents=True, exist_ok=True)
            payload = {'schema':'sa-dispatch.scheduler-service-evidence.v1','synthetic_only':True,
                       'observed_at':timestamp(datetime.now(timezone.utc)), **data}
            with (folder / (name + '.json')).open('x', encoding='utf-8') as stream:
                json.dump(payload, stream, indent=2)

    def test_public_service_three_slots_with_receipts(self):
        self.upsert(interval=1)
        process, port = self.start()
        receipts = []
        blocked_until_handoff = 0
        deadline = time.monotonic() + 10
        while len(receipts) < 3 and time.monotonic() < deadline:
            try:
                lease = self.invoke(port, '/claim', {})['lease']
            except urllib.error.HTTPError as error:
                body = json.load(error)
                if error.code != 409 or body['error']['code'] != 'E_HANDOFF_NOT_READY':
                    raise
                blocked_until_handoff += 1
                time.sleep(.01)
                continue
            if lease:
                receipts.append(self.complete(port, lease))
            else:
                time.sleep(.03)
        self.stop(process)
        status = self.cli('schedule-status')
        tasks = self.cli('status')['tasks']
        self.assertEqual(len(receipts), 3)
        self.assertEqual(len(tasks), 3)
        self.assertEqual(len({row['fire_key'] for row in status['firings']}), 3)
        self.assertTrue(all(task['status'] == 'succeeded' for task in tasks))
        traces = [self.cli('trace', task['task_id']) for task in tasks]
        for trace in traces:
            self.assertTrue(any(event['kind'] == 'task.succeeded' and event['refs'] for event in trace['events']))
        self.assertEqual(self.cli('trace', tasks[0]['task_id']), traces[0])
        self.evidence('public-three-slots', {'entrypoints':['schedule-upsert','serve --schedules','HTTP claim/artifact/complete'],
                      'manual_task_submissions':0,'handoff_pending_responses':blocked_until_handoff,
                      'firings':status['firings'],'tasks':tasks,'receipts':receipts,'traces':traces})

    def test_normal_cli_and_coordinator_kill_recover_lease(self):
        self.assertEqual(self.upsert()['version'], 1)
        process, port = self.start()
        lease = self.invoke(port, '/claim', {})['lease']
        self.assertIsNotNone(lease)
        process.kill(); process.wait(timeout=10)
        time.sleep(2.1)
        _, port = self.start()
        recovered = self.invoke(port, '/claim', {})['lease']
        self.assertEqual(recovered['task_id'], lease['task_id'])
        self.assertEqual(recovered['fence'], 2)
        receipt = self.complete(port, recovered)
        state = self.cli('schedule-status')
        self.assertEqual(len(state['firings']), 1)
        self.assertEqual(state['firings'][0]['outcome'], 'created')
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(conn.execute('''SELECT count(*) FROM schedule_firings f
                LEFT JOIN tasks t ON t.task_id=f.task_id WHERE f.outcome='created' AND t.task_id IS NULL''').fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT status,attempts FROM tasks").fetchone(), ('succeeded',2))
        self.evidence('coordinator-kill-restart', {'first_lease':lease,'recovered_lease':recovered,
                      'receipt':receipt,'orphan_created_fires':0,'firings':state['firings']})

    def test_two_processes_tick_100_times_at_barrier(self):
        clock = FakeClock()
        store = SqliteStore(self.db, clock=clock, lease_ttl_seconds=10, kinds=KINDS)
        scheduler = Scheduler(store)
        scheduler.upsert('race', interval_seconds=10, task_template=self.spec,
                         next_fire_at=timestamp(clock.now()), catchup='all', max_catchup=3)
        gate = self.root / 'gate'
        code = '''import sys,time
from pathlib import Path
from sa_dispatch.clock import FakeClock
from sa_dispatch.scheduler import Scheduler
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch_hotspot import KINDS
s=Scheduler(SqliteStore(sys.argv[1],clock=FakeClock('2026-01-01T00:00:20Z'),lease_ttl_seconds=10,kinds=KINDS))
Path(sys.argv[3]).touch()
while not Path(sys.argv[2]).exists(): time.sleep(.01)
for _ in range(100): s.tick()
'''
        ready = [self.root / ('ready-' + str(index)) for index in range(2)]
        children = [subprocess.Popen([sys.executable,'-B','-c',code,str(self.db),str(gate),str(marker)], cwd=ROOT,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE) for marker in ready]
        for child in children:
            self.addCleanup(self.stop, child)
        deadline = time.monotonic() + 10
        while not all(marker.exists() for marker in ready) and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(all(marker.exists() for marker in ready))
        gate.touch()
        for child in children:
            output, errors = child.communicate(timeout=20)
            self.assertEqual(child.returncode, 0, errors.decode())
        self.assertEqual(len(store.list_tasks()), 3)
        self.assertEqual(len(scheduler.firings()), 3)
        self.evidence('two-process-race', {'processes':2,'ticks_per_process':100,'tasks':3,
                      'firings':scheduler.firings()})

    def test_process_death_after_task_commit_recovers_pending_fire(self):
        self.upsert()
        code = '''import os,sys
from sa_dispatch.scheduler import Scheduler
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch_hotspot import KINDS
store=SqliteStore(sys.argv[1],lease_ttl_seconds=1,kinds=KINDS)
real=store.submit
def crash(spec):
    real(spec)
    os._exit(71)
store.submit=crash
Scheduler(store).tick(synthetic_handoff=True)
'''
        died = subprocess.run([sys.executable,'-B','-c',code,str(self.db)],cwd=ROOT,timeout=10)
        self.assertEqual(died.returncode, 71)
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM tasks').fetchone()[0], 1)
            self.assertEqual(conn.execute('SELECT outcome FROM schedule_firings').fetchone()[0], 'pending')
        _, port = self.start()
        self.assertIsNotNone(self.invoke(port, '/claim', {})['lease'])
        state = self.cli('schedule-status')
        self.assertEqual(state['firings'][0]['outcome'], 'created')
        self.assertEqual(len(self.cli('status')['tasks']), 1)
        self.evidence('submit-commit-process-death', {'crash_exit_code':71,'tasks_after_recovery':1,
                      'firings':state['firings']})

    def test_two_bad_pending_slots_after_process_death_do_not_block_healthy(self):
        store = SqliteStore(self.db, lease_ttl_seconds=2, kinds=KINDS)
        scheduler = Scheduler(store)
        scheduler.upsert('two-bad',interval_seconds=10,task_template=self.spec,
                         next_fire_at=timestamp(datetime.now(timezone.utc)-timedelta(seconds=10)),
                         catchup='all',max_catchup=2)
        code = '''import os,sys
from sa_dispatch.scheduler import Scheduler
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch_hotspot import KINDS
store=SqliteStore(sys.argv[1],lease_ttl_seconds=2,kinds=KINDS)
store.submit=lambda spec: os._exit(71)
Scheduler(store).tick(limit=2)
'''
        died = subprocess.run([sys.executable,'-B','-c',code,str(self.db)],cwd=ROOT,timeout=10)
        self.assertEqual(died.returncode,71)
        with store._tx() as conn:
            rows=conn.execute('SELECT fire_key,task_template FROM schedule_firings').fetchall()
            self.assertEqual(len(rows),2)
            for row in rows:
                template=json.loads(row['task_template']); template['kind']='fixture.unavailable'
                conn.execute('UPDATE schedule_firings SET task_template=? WHERE fire_key=?',
                             (json.dumps(template),row['fire_key']))
            conn.execute('UPDATE schedules SET enabled=0')
        self.upsert()
        process,port=self.start()
        lease=self.invoke(port,'/claim',{})['lease']
        self.assertIsNotNone(lease)
        self.complete(port,lease)
        self.stop(process)
        state=self.cli('schedule-status')
        bad=[row for row in state['firings'] if row['schedule_id']=='two-bad']
        self.assertTrue(all(row['outcome']=='rejected' and row['rejection_code']=='invalid_reserved_template' for row in bad))
        with store._tx() as conn:
            counts={
                'created_orphans':conn.execute("SELECT count(*) FROM schedule_firings f LEFT JOIN tasks t ON t.task_id=f.task_id WHERE f.outcome='created' AND t.task_id IS NULL").fetchone()[0],
                'tasks_without_fires':conn.execute("SELECT count(*) FROM tasks t LEFT JOIN schedule_firings f ON f.task_id=t.task_id WHERE t.idem_key LIKE 'sched:%' AND f.fire_key IS NULL").fetchone()[0],
                'pending':conn.execute("SELECT count(*) FROM schedule_firings WHERE outcome='pending'").fetchone()[0]}
        self.assertEqual(set(counts.values()),{0})
        self.evidence('bad-pending-process-recovery',{'crash_exit_code':71,'bad_slots':2,
            'healthy_task':lease['task_id'],'audit_counts':counts,'firings':state['firings']})


if __name__ == '__main__':
    unittest.main()
