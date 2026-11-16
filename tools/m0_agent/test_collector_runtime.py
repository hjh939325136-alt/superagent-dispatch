"""Exercise bounded collection and failure gates against a real 0013 SQLite ledger."""

from contextlib import ExitStack, closing, contextmanager
from dataclasses import asdict, replace
import json
from hashlib import sha256
import os
from pathlib import Path
import stat
import sys
import unittest
from unittest.mock import patch

from sa_dispatch.collector import Collector, SourceBinding
from sa_dispatch.models import TaskSpec

from . import collector_runtime as runtime
from . import test_common_worker


class CollectorRuntimeTests(unittest.TestCase):
    MACHINE = 'c' * 32
    BOOT = '01010101-0202-0303-0404-050505050505'

    def setUp(self):
        fixture = test_common_worker.CommonWorkerTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.upgrade()
        self.fixture = fixture
        self.clock = fixture.clock
        self.root = fixture.root
        self.ledger = fixture.ledger
        self.health_dir = self.root / 'health'
        common, campaign = self.root / 'common', self.root / 'campaign'
        for directory in (self.health_dir, common, campaign):
            directory.mkdir()
        self.paths = dict(LEDGER=self.ledger, BINDING=self.root / 'collector-binding.json',
                          MAINTENANCE_LOCK=self.root / 'maintenance.lock',
                          NATIVE_GUARD=self.root / 'native-guard.json',
                          HEALTH_DIR=self.health_dir, PIN=self.health_dir / 'collector-runtime.json',
                          HEALTH=self.health_dir / 'health.json', COMMON_AUTHORITY=common,
                          CAMPAIGN_AUTHORITY=campaign, MACHINE_ID=self.root / 'machine-id',
                          BOOT_ID=self.root / 'boot-id')
        self.paths['MACHINE_ID'].write_text(self.MACHINE, encoding='ascii')
        self.paths['BOOT_ID'].write_text(self.BOOT, encoding='ascii')
        self.paths['MAINTENANCE_LOCK'].touch()
        self.write(self.paths['NATIVE_GUARD'], {'phase': 'closed_for_common_external_upgrade'})
        binding = SourceBinding.enroll('common-dispatch', 'dispatch', self.ledger, 'fixture', ('0013',))
        binding = replace(binding, profile='common_metadata_0013', synthetic_only=False, data_class='local_only')
        self.write(self.paths['BINDING'], dict(schema='sa_dispatch.collector_binding.v2', target=str(self.ledger),
                                              sources=[asdict(binding)]))
        self.locks = []
        stack = self.enterContext(ExitStack())
        stack.enter_context(patch.multiple(runtime, **self.paths))
        stack.enter_context(patch.object(runtime, '_require_root'))
        stack.enter_context(patch.object(runtime.common_install, '_root_parents'))
        stack.enter_context(patch.object(runtime.common_install, '_root_metadata', side_effect=self.checked_path))
        stack.enter_context(patch.object(runtime, '_shared_info', side_effect=self.checked_path))
        stack.enter_context(patch.object(runtime, '_flock', self.track_lock))
        stack.enter_context(patch.object(runtime, '_installed_guard', side_effect=self.inside_locks))
        stack.enter_context(patch.object(runtime.common_install, '_atomic', side_effect=self.atomic))
        for flag in ('O_NOFOLLOW', 'O_NONBLOCK'):
            if not hasattr(os, flag):
                stack.enter_context(patch.object(os, flag, 0, create=True))
        self.pin = runtime.build_pin(min_free_bytes=1)
        self.write(self.paths['PIN'], self.pin)
        self.common_state = dict(schema=runtime.common_install.STATE_SCHEMA, machine_id=self.MACHINE,
                                 phase='ready', manifest_sha256='a' * 64,
                                 ledger_identity=self.pin['ledger_identity'],
                                 pre_live_recovery={'restore_verified': True},
                                 old_guard_closed_sha256=sha256(self.paths['NATIVE_GUARD'].read_bytes()).hexdigest(),
                                 base_config_hashes={'collector-binding.json': self.pin['binding_raw_sha256']})
        self.campaign_state = dict(schema=runtime.campaign_install.SCHEMA, machine_id=self.MACHINE,
                                   phase='stopped', manifest_sha256='b' * 64, prior_manifest_sha256='a' * 64,
                                   ledger_identity=self.pin['ledger_identity'])
        self.write(common / 'state.json', self.common_state)
        self.write(campaign / 'state.json', self.campaign_state)
        self.block = None

    @staticmethod
    def checked_path(path, *, directory=False, private=False):
        info = path.lstat()
        expected = stat.S_ISDIR if directory else stat.S_ISREG
        if not expected(info.st_mode) or (not directory and info.st_nlink != 1):
            raise ValueError('unsafe path')
        return info

    @staticmethod
    def write(path, data):
        path.write_bytes(json.dumps(data, separators=(',', ':')).encode('utf-8'))

    @staticmethod
    def atomic(path, content, *, mode):
        temporary = path.with_suffix('.tmp')
        temporary.write_bytes(content)
        temporary.replace(path)

    @contextmanager
    def track_lock(self, path, *, directory):
        self.locks.append((path, directory))
        try:
            if self.block == path:
                code = 'ledger_lock_busy' if directory else 'maintenance_lock_busy'
                raise runtime._Refused(code)
            yield
        finally:
            self.locks.pop()

    def inside_locks(self):
        self.assertEqual(self.locks, [(self.paths['MAINTENANCE_LOCK'], False), (self.ledger.parent, True)])

    def rows(self, table):
        with closing(self.fixture.local._connect()) as conn:
            return [dict(row) for row in conn.execute('SELECT * FROM ' + table)]

    def tick(self):
        return runtime.tick(clock=self.clock)

    def test_incremental_noop_and_bounded_batches(self):
        self.assertTrue(runtime.status(clock=self.clock)['stale'])
        real = Collector.sync

        def sync(store, binding, stream, *, limit):
            self.inside_locks()
            self.assertEqual(limit, 100)
            return real(store, binding, stream, limit=limit)

        with patch.object(Collector, 'sync', sync):
            first = self.tick()
            self.assertEqual((first['status'], first['streams']['events']['added']), ('ok', 1))
            self.assertFalse(first['stale'])
            self.assertEqual(self.tick()['streams']['events']['added'], 0)
            initial = len(self.rows('unified_events'))
            task, _ = self.fixture.local.submit(TaskSpec('extra', 'fixture.local', 1, {}, (),
                                                          ('worker.compute',), 'synthetic', 'none', 1))
            for _ in range(103):
                with closing(self.fixture.local._write_connect()) as conn, conn:
                    conn.execute('''INSERT INTO events(task_id,from_status,to_status,fence,reason,actor,at)
                        VALUES(?,'queued','queued',0,'body sentinel','fixture',?)''', (task.task_id, first['attempt_at']))
            batch = self.tick()
            self.assertEqual((batch['streams']['events']['added'], batch['streams']['events']['projection_lag']), (100, 4))
            finish = self.tick()
            self.assertEqual((finish['streams']['events']['added'], finish['streams']['events']['projection_lag']), (4, 0))
            self.assertEqual(len(self.rows('unified_events')) - initial, 104)
            self.assertNotIn('body sentinel', self.paths['HEALTH'].read_text())

    def test_frozen_stream_stays_frozen_and_others_advance(self):
        self.fixture.register()
        self.fixture.tick()
        self.tick()
        with closing(self.fixture.local._write_connect()) as conn, conn:
            conn.execute("UPDATE control_events SET detail_code='raw fixture explanation' WHERE seq=3")
        self.fixture.local.submit(TaskSpec('incremental', 'fixture.local', 1, {}, (),
                                          ('worker.compute',), 'synthetic', 'none', 1))
        health = self.tick()
        self.assertEqual((health['status'], health['code']), ('degraded', 'stream_frozen'))
        self.assertEqual(health['streams']['events']['added'], 1)
        self.assertEqual(health['streams']['control_events']['code'], 'source_rewritten')
        gap, = self.rows('collector_gaps')
        self.assertIsNone(gap['resolved_at'])
        self.assertEqual(self.tick()['streams']['control_events']['state'], 'frozen')
        self.assertEqual(self.rows('collector_gaps'), [gap])

    def test_binding_drift_and_capacity_never_write_ledger(self):
        before = self.ledger.read_bytes()
        self.write(self.paths['PIN'], self.pin | {'max_ledger_bytes': 1})
        health = self.tick()
        self.assertEqual((health['status'], health['code']), ('blocked', 'capacity_limit'))
        self.assertGreater(health['metrics']['ledger_bytes'], 1)
        self.assertEqual(self.ledger.read_bytes(), before)
        self.paths['HEALTH'].unlink()
        self.write(self.paths['PIN'], self.pin)
        binding = self.paths['BINDING']
        binding.write_bytes(binding.read_bytes() + b'\n')
        self.assertEqual(self.tick()['code'], 'identity_or_guard_invalid')
        self.assertEqual(self.ledger.read_bytes(), before)

    def test_lock_contention_records_health_only(self):
        before = self.ledger.read_bytes()
        for path, code in ((self.paths['MAINTENANCE_LOCK'], 'maintenance_lock_busy'),
                           (self.ledger.parent, 'ledger_lock_busy')):
            self.block = path
            report = self.tick()
            self.assertEqual((report['status'], report['code'], report['busy']), ('busy', code, True))
            self.assertEqual(self.ledger.read_bytes(), before)
        self.block = None
        self.assertEqual(self.tick()['status'], 'ok')

    def test_persistent_pending_refuses_writes_but_aborted_is_terminal(self):
        before = self.ledger.read_bytes()
        common = self.paths['COMMON_AUTHORITY'] / 'state.json'
        campaign = self.paths['CAMPAIGN_AUTHORITY'] / 'state.json'
        self.write(common, self.common_state | {'phase': 'prepared'})
        self.assertEqual(self.tick()['code'], 'common_not_ready')
        self.write(common, self.common_state)
        native = self.paths['NATIVE_GUARD']
        closed = native.read_bytes()
        self.write(native, {'phase': 'pending'})
        self.assertEqual(self.tick()['code'], 'identity_or_guard_invalid')
        native.write_bytes(closed)
        for phase in ('prepared', 'upgrading', 'rotating', 'aborting'):
            self.write(campaign, self.campaign_state | {'phase': phase})
            self.assertEqual(self.tick()['code'], 'campaign_maintenance_pending')
        self.write(campaign, self.campaign_state | {'phase': 'paused'})
        rotation = campaign.with_name('rotate.json')
        receipt = dict(schema=runtime.campaign_install.ROTATION_SCHEMA, machine_id=self.MACHINE)
        self.write(rotation, receipt | {'phase': 'aborting'})
        self.assertEqual(self.tick()['code'], 'campaign_maintenance_pending')
        self.assertEqual(self.ledger.read_bytes(), before)
        self.write(rotation, receipt | {'phase': 'aborted'})
        self.assertEqual(self.tick()['status'], 'ok')

    def test_common_only_works_and_lost_campaign_authority_fails_closed(self):
        authority = self.paths['CAMPAIGN_AUTHORITY']
        (authority / 'state.json').unlink()
        authority.rmdir()
        self.assertEqual(self.tick()['status'], 'ok')
        with closing(self.fixture.local._write_connect()) as conn, conn:
            conn.execute("""INSERT INTO control_budgets VALUES
                ('agent-campaign:authorization:fixture','campaign-lifetime','provider_request',2,0,0,'start','end')""")
        before = self.ledger.read_bytes()
        self.assertEqual(self.tick()['code'], 'identity_or_guard_invalid')
        self.assertEqual(self.ledger.read_bytes(), before)

    def test_status_readonly_and_stale_by_age_reboot_or_rollback(self):
        first = self.tick()
        raw = self.paths['HEALTH'].read_bytes()
        self.clock.advance(179)
        self.assertFalse(runtime.status(clock=self.clock)['stale'])
        self.clock.advance(1)
        self.assertTrue(runtime.status(clock=self.clock)['stale'])
        self.clock.advance(-181)
        self.assertTrue(runtime.status(clock=self.clock)['stale'])
        self.clock.advance(2)
        self.paths['BOOT_ID'].write_text('00000000-0000-0000-0000-000000000000', encoding='ascii')
        self.assertTrue(runtime.status(clock=self.clock)['stale'])
        self.assertEqual(self.paths['HEALTH'].read_bytes(), raw)
        self.assertEqual(first['metrics_scope'], 'ledger_and_sqlite_sidecars')


class CollectorLockTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform.startswith('linux'), 'native flock requires Linux')
    def test_nonblocking_real_exclusive_lock_and_shared_directory_lock(self):
        import fcntl
        from tempfile import TemporaryDirectory

        directory = Path(self.enterContext(TemporaryDirectory()))
        path = directory / 'maintenance.lock'
        path.touch()
        with patch.object(runtime.common_install, '_root_metadata', side_effect=lambda p: p.lstat()), \
                patch.object(runtime, '_shared_info', side_effect=lambda p, directory: p.lstat()):
            with path.open('rb') as held:
                fcntl.flock(held, fcntl.LOCK_EX)
                with self.assertRaisesRegex(runtime._Refused, 'maintenance_lock_busy'):
                    with runtime._flock(path, directory=False):
                        self.fail('exclusive lock was acquired while already held')
            descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_SH)
                with runtime._flock(directory, directory=True):
                    pass
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                with self.assertRaisesRegex(runtime._Refused, 'ledger_lock_busy'):
                    with runtime._flock(directory, directory=True):
                        self.fail('shared lock was acquired under exclusive lock')
            finally:
                os.close(descriptor)


if __name__ == '__main__':
    unittest.main()
