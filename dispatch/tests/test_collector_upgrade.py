"""Exercise a real 0011-to-0012 migration and Collector CLI cursor adoption."""
from contextlib import closing
from dataclasses import asdict
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import canonical_json
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.collector import SourceBinding
from sa_dispatch.models import TaskSpec
from sa_dispatch.routing import HTTP_ROUTE, LOCAL_ROUTE, activate_mixed, activation_manifest
from sa_dispatch.scheduler import Scheduler
from sa_dispatch.store_sqlite import SqliteStore


class ReadKind:
    name = 'fixture.read'
    version = 1
    enabled = True
    side_effect = 'none'

    def validate(self, params):
        return params

    def scope(self, params):
        return {}


class CollectorUpgradeTests(unittest.TestCase):
    streams = ('events', 'control_events', 'schedule_events')
    source_id = 'dispatch-fixture'
    instance = 'fixture-one'
    actor = 'fixture-operator'

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='collector-upgrade-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.path = self.root / 'ledger.db'
        self.clock = FakeClock()
        self.kinds = {ReadKind.name: ReadKind()}

    def store(self, route=None):
        return SqliteStore(self.path, lease_ttl_seconds=10, clock=self.clock, kinds=self.kinds, route=route)

    def rows(self, sql):
        with closing(sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            return [dict(row) for row in connection.execute(sql)]

    def create_0011(self):
        # Run the production migration prefix, stopping before the first 0012 DDL.
        with patch('sa_dispatch.migrations.apply_routing_schema') as route_migration:
            self.store()
        route_migration.assert_called_once()
        self.assertEqual(self.rows('SELECT MAX(version) AS version FROM schema_migrations'), [{'version': '0011'}])
        for table in ('tasks', 'devices', 'schedules', 'schedule_firings'):
            self.assertNotIn('execution_route', [column['name'] for column in self.rows(f'PRAGMA table_info({table})')])
        self.assertEqual(self.rows("SELECT name FROM sqlite_schema WHERE name IN ('ledger_mode','route_audit')"), [])

        # Seed old rows through the actual 0011 schema. Current producer APIs
        # require the route column, so they are used only after migration.
        at = timestamp(self.clock.now())
        with closing(sqlite3.connect(self.path)) as connection, connection:
            connection.execute('''INSERT INTO tasks(task_id,idem_key,fingerprint,kind,kind_version,params_json,
              input_refs_json,required_caps_json,scope_json,side_effect,access,egress,status,fence,attempts,
              max_attempts,created_at,updated_at) VALUES('old-task','old-key',?,'fixture.read',1,'{}',
              '[]','[]','{}','none','synthetic','none','succeeded',1,1,2,?,?)''',
                               ('sha256:' + 'a' * 64, at, at))
            connection.executemany('''INSERT INTO events(task_id,from_status,to_status,fence,reason,actor,at)
              VALUES('old-task',?,?,?,?, 'fixture',?)''', [
                (None, 'queued', 0, 'submitted', at), ('queued', 'leased', 1, 'claimed', at),
                ('leased', 'succeeded', 1, 'completed', at),
            ])
            connection.execute('''INSERT INTO leases(lease_id,task_id,fence,device_id,granted_at,expires_at,state)
              VALUES('old-lease','old-task',1,'old-worker',?,'2026-01-01T00:00:10.000000+00:00','completed')''', (at,))
            connection.execute('''INSERT INTO control_events(event_id,event_key,kind,task_id,fence,actor,at,refs_json,detail_code)
              VALUES('old-event','old-event-key','budget.exceeded','old-task',1,'fixture',?,'[]','fixture-detail')''', (at,))
            connection.execute('''INSERT INTO schedule_events(schedule_id,version,kind,at,detail_code)
              VALUES('old-schedule',1,'schedule.exhausted',?,'max_slots_reached')''', (at,))

    def collector_binding(self, version):
        binding = SourceBinding.enroll(self.source_id, 'dispatch', self.path, self.instance, (version,))
        path = self.root / f'collector-{version}.json'
        path.write_text(json.dumps({'schema': 'sa_dispatch.collector_binding.v1', 'target': str(self.path),
                                    'sources': [asdict(binding)]}), encoding='utf-8')
        return path

    def manager_binding(self):
        info = self.path.stat()
        path = self.root / 'manager.json'
        path.write_text(json.dumps({'version': 1, 'sources': [{
            'source_id': self.source_id, 'source_instance': self.instance, 'kind': 'dispatch',
            'locator': str(self.path), 'accepted_schema_versions': ['0012'], 'class': 'internal',
            'freshness_s': 3600, 'options': {'identity': {'device': info.st_dev, 'inode': info.st_ino}},
        }]}), encoding='utf-8')
        return path

    def cli(self, module, binding, command, request, *, exit_code=0):
        workspace = Path(__file__).resolve().parents[2]
        child = subprocess.run(
            [sys.executable, '-X', 'utf8', '-B', '-m', module, '--binding', str(binding), *command],
            input=json.dumps({'schema_version': '0.1', **request}), text=True, encoding='utf-8',
            capture_output=True, timeout=20, env=dict(os.environ, PYTHONPATH=os.pathsep.join(
                map(str, (workspace / 'dispatch', workspace)))),
        )
        if os.environ.get('SA_COLLECTOR_UPGRADE_STDOUT') == '1':
            print(child.stdout, end='')
        self.assertEqual(child.returncode, exit_code, (child.stdout, child.stderr))
        self.assertEqual(child.stderr, '')
        response = json.loads(child.stdout)
        self.assertEqual(response['schema_version'], '0.1')
        self.assertIs(response['ok'], exit_code == 0)
        return response

    def collector(self, binding, command, stream, *, exit_code=0, **fields):
        return self.cli('sa_dispatch.collector_cli', binding, [command],
                        {'source_id': self.source_id, 'stream': stream, **fields}, exit_code=exit_code)

    def activate_and_produce(self):
        isolated = self.store()
        self.assertEqual(self.rows('SELECT mode FROM ledger_mode'), [{'mode': 'isolated'}])
        self.assertEqual(self.rows('SELECT MAX(version) AS version FROM schema_migrations'), [{'version': '0012'}])
        self.assertIn('execution_route', [row['name'] for row in self.rows('PRAGMA table_info(tasks)')])
        artifacts = ArtifactStore(self.root / 'cas')
        with closing(isolated._connect()) as connection:
            proof = activation_manifest(connection, actor=self.actor, bindings={})
        activate_mixed(self.path, actor=self.actor, evidence_ref=artifacts.put(canonical_json(proof)),
                       bindings={}, artifacts=artifacts, clock=self.clock)
        self.assertEqual(self.rows('SELECT mode FROM ledger_mode'), [{'mode': 'mixed'}])
        self.clock.advance(10)
        spec = TaskSpec('new-task', ReadKind.name, 1, {}, (), ('cpu',), 'synthetic', 'none', 2)
        local = self.store(LOCAL_ROUTE)
        local.register_device('fixture-local', ['cpu'])
        local.submit(spec, control_rule={'approval_scope': 'fixture-approval'})
        self.assertIsNone(local.claim('fixture-local'))
        scheduler = Scheduler(self.store(HTTP_ROUTE))
        scheduler.upsert('new-schedule', interval_seconds=10, task_template=spec,
                         next_fire_at=timestamp(self.clock.now()), max_slots=1)
        self.assertEqual(len(scheduler.tick(synthetic_handoff=True)), 1)

    def test_schema_upgrade_freezes_then_adopts_each_stream_without_replay_or_loss(self):
        self.create_0011()
        old_binding = self.collector_binding('0011')
        old_task = self.rows("SELECT * FROM tasks WHERE task_id='old-task'")
        old_lease = self.rows("SELECT * FROM leases WHERE lease_id='old-lease'")
        anchors = dict(zip(self.streams, (3, 1, 1)))
        for stream, anchor in anchors.items():
            result = self.collector(old_binding, 'sync', stream)['result']
            self.assertEqual((result['state'], result['added'], result['last_seq'], result['epoch']),
                             ('synced', anchor, anchor, 1))
        old_events = self.rows('SELECT * FROM unified_events ORDER BY seq')
        self.assertEqual(len(old_events), 5)
        cursor_query = 'SELECT * FROM collector_cursors ORDER BY stream'
        old_cursors = self.rows(cursor_query)
        self.assertEqual({row['schema_version'] for row in old_cursors}, {'0011'})

        self.activate_and_produce()
        new_binding = self.collector_binding('0012')
        for stream, anchor in anchors.items():
            result = self.collector(old_binding, 'sync', stream)['result']
            self.assertEqual((result['state'], result['kind'], result['last_seq']),
                             ('frozen', 'schema_changed', anchor))
            self.assertEqual(self.collector(new_binding, 'sync', stream)['result'], {
                'state': 'frozen', 'kind': 'schema_changed', 'last_seq': anchor,
            })
        self.assertEqual(self.rows(cursor_query), old_cursors)

        before = self.path.read_bytes()
        adoption = {'reason': 'accept_schema', 'actor': self.actor, 'resume_after': anchors['events']}
        denied = {'schema_version': '0.1', 'ok': False, 'error': 'E_COLLECTOR_REFUSED', 'cause': 'ValueError'}
        self.assertEqual(self.collector(old_binding, 'adopt', 'events', exit_code=1, **adoption), denied)
        self.assertEqual(self.collector(new_binding, 'adopt', 'events', exit_code=1,
                                        **(adoption | {'resume_after': 1000})), denied)
        manager = self.manager_binding()
        response = self.cli('tools.m0_manager', manager, [], {'op': 'attention.list', 'role': 'manager', 'args': {}})
        self.assertEqual(sum(item['rule'] == 'A9@1' and item['state'] == 'schema_changed'
                             for item in response['items']), 3)
        self.assertEqual({row['stream']: row['lag'] for row in response['sources'][0]['projection_lag']},
                         {'events': 2, 'control_events': 1, 'schedule_events': 1})
        response = self.cli('tools.m0_manager', manager, [], {'op': 'adopt', 'role': 'manager', 'args': adoption}, exit_code=2)
        self.assertEqual(response['error'], 'E_OPERATION_NOT_ALLOWED')
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.rows(cursor_query), old_cursors)
        self.assertEqual(self.rows('SELECT * FROM unified_events ORDER BY seq'), old_events)

        new_kinds = {'events': ['task.submitted', 'task.submitted'], 'control_events': ['approval.requested'],
                     'schedule_events': ['schedule.exhausted']}
        for stream, anchor in anchors.items():
            with self.subTest(stream=stream):
                result = self.collector(new_binding, 'adopt', stream, **(adoption | {'resume_after': anchor}))['result']
                self.assertEqual(result, {'state': 'adopted', 'epoch': 2, 'last_seq': anchor})
                cursor = next(row for row in self.rows(cursor_query) if row['stream'] == stream)
                prior = next(row for row in old_cursors if row['stream'] == stream)
                self.assertEqual(cursor['anchor_sha256'], prior['anchor_sha256'])
                self.assertEqual(cursor['schema_version'], '0012')
                self.assertEqual(cursor['last_seq'], anchor)
                gap, = self.rows(f"SELECT * FROM collector_gaps WHERE stream='{stream}'")
                self.assertEqual((gap['kind'], gap['resolved_by']), ('schema_changed', self.actor))
                self.assertIsNotNone(gap['resolved_at'])
                self.assertEqual(json.loads(gap['resolution']),
                                 {'reason': 'accept_schema', 'resume_after': anchor, 'epoch': 2})
                count = len(new_kinds[stream])
                result = self.collector(new_binding, 'sync', stream)['result']
                self.assertEqual((result['added'], result['last_seq'], result['epoch'], result['projection_lag']),
                                 (count, anchor + count, 2, 0))
                replay = self.collector(new_binding, 'sync', stream)['result']
                self.assertEqual(replay, result | {'added': 0, 'redacted': 0})

        all_events = self.rows('SELECT * FROM unified_events ORDER BY seq')
        self.assertEqual(all_events[:len(old_events)], old_events)
        self.assertEqual([(row['source'], row['source_id'], row['kind']) for row in all_events[len(old_events):]], [
            (f'{self.source_id}@{self.instance}:e2', f'{stream}:{anchors[stream] + index}', kind)
            for stream, kinds in new_kinds.items() for index, kind in enumerate(kinds, 1)
        ])
        self.assertEqual(self.rows("SELECT * FROM tasks WHERE task_id='old-task'"),
                         [old_task[0] | {'execution_route': None}])
        self.assertEqual(self.rows("SELECT * FROM leases WHERE lease_id='old-lease'"), old_lease)


if __name__ == '__main__':
    unittest.main()
