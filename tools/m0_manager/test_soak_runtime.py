"""Exercise the observation recorder with simulated elapsed time and a fake probe."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager, nullcontext
from datetime import timedelta
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import sys
from threading import Event, Lock
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from dispatch.sa_dispatch.clock import FakeClock, timestamp

from . import soak_runtime as soak


NATIVE_LOCK = soak._lock


class SoakRuntimeTests(unittest.TestCase):
    BOOT = '01010101-0202-0303-0404-050505050505'
    NEW_BOOT = '09090909-0808-0707-0606-050505050505'

    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.record = self.root / 'record.json'
        self.pin_path = self.root / 'pin.json'
        self.clock = FakeClock()
        self.monotonic_ms = 1_000_000
        self.boot = self.BOOT
        self.pin = dict(schema=soak.PIN_SCHEMA, machine_id='a' * 32, ledger_identity='1:2',
                        observer_manifest_sha256='b' * 64, binding_sha256='c' * 64, source_id='common-dispatch',
                        source_instance='fixture', duration_seconds=86400, interval_seconds=300, max_gap_seconds=600)
        self.pin_path.write_text(json.dumps(self.pin), encoding='utf-8')
        stack = self.enterContext(ExitStack())
        stack.enter_context(patch.multiple(soak, AUTHORITY=self.root, PIN=self.pin_path,
                                           RECORD=self.record, LOCK=self.root / 'soak.lock'))
        stack.enter_context(patch.object(soak, '_root'))
        stack.enter_context(patch.object(soak, '_lock', side_effect=lambda **_: nullcontext()))
        stack.enter_context(patch.object(soak, '_protected', side_effect=self.read))
        stack.enter_context(patch.object(soak.common_install, '_atomic', side_effect=self.atomic))
        stack.enter_context(patch.object(soak, '_scope', return_value=(self.pin, self.root / ('b' * 64))))
        stack.enter_context(patch.object(soak, '_boot', side_effect=lambda: self.boot))
        stack.enter_context(patch.object(soak, '_boottime', side_effect=lambda: self.monotonic_ms))
        self.guard = stack.enter_context(patch.object(soak, '_guard'))
        self.query = stack.enter_context(patch.object(soak, '_manager_query', side_effect=self.output))

    @staticmethod
    def read(path, max_bytes, *, private=False):
        result = path.read_bytes()
        if len(result) > max_bytes:
            raise ValueError('fixture exceeds read limit')
        return result

    @staticmethod
    def atomic(path, raw, *, mode):
        temporary = path.with_suffix('.tmp')
        temporary.write_bytes(raw)
        temporary.replace(path)

    def output(self, *args):
        observed = timestamp(self.clock.now())
        item = dict(schema=soak.OPS_SCHEMA, kind='operational_snapshot', non_atomic=True,
                    observation_started_at=observed, observation_completed_at=observed,
                    state_basis='current', coverage='complete', attention=[],
                    observations={
                        'ledger': {'state': 'observed'},
                        'boot': {'state': 'observed', 'boot_id': self.boot},
                        'systemd': {'state': 'observed', 'units': {
                            unit: dict(state='observed', active_state='active' if unit == soak.COORDINATOR else 'inactive')
                            for unit in soak.UNITS}},
                        'collector': dict(state='ok', freshness='fresh', success_boot_id=self.boot,
                                          streams={name: {'projection_lag': 0} for name in soak.STREAMS}),
                        'campaign': {'state': 'intentional_idle', 'private_body': 'DO_NOT_PERSIST_FIXTURE'},
                    })
        return dict(ok=True, op='ops.snapshot', coverage='complete', items=[item],
                    sources=[{'source_id': self.pin['source_id'], 'source_instance': self.pin['source_instance'],
                              'schema_version': '0013'}])

    def advance(self, seconds, *, boottime=None):
        self.clock.advance(seconds)
        self.monotonic_ms += (seconds if boottime is None else boottime) * 1000

    def start(self):
        return soak.start(clock=self.clock)

    def tick(self):
        return soak.tick(clock=self.clock)

    def status(self):
        return soak.status(clock=self.clock)

    def before_final_tick(self):
        first = self.start()
        for _ in range(143):
            self.advance(600)
            self.assertEqual(self.tick()['state'], 'running')
        self.advance(600)
        return first

    def test_pass_requires_full_wall_and_monotonic_window_then_terminal_is_readonly(self):
        first = self.start()
        self.assertEqual(first['sample_count'], 1)
        self.assertEqual(first['deadline_at'], timestamp(self.clock.now() + timedelta(days=1)))
        self.assertEqual(self.start(), first)
        self.assertEqual(self.tick(), first)
        self.query.assert_called_once()
        # The maximum permitted gap yields exactly 145 observations in the pinned window.
        for _ in range(143):
            self.advance(600)
            self.assertEqual(self.tick()['state'], 'running')
        self.advance(600)
        final = self.tick()
        self.assertEqual((final['state'], final['reason'], final['sample_count']), ('passed', 'window_passed', 145))
        self.assertEqual((final['elapsed_seconds'], final['boot_elapsed_seconds']), (86400, 86400))
        self.assertEqual(final['idle_campaign_samples'], 145)
        raw = self.record.read_bytes()
        self.assertLess(len(raw), soak.MAX_RECORD_BYTES)
        self.assertNotIn(b'DO_NOT_PERSIST_FIXTURE', raw)
        self.assertEqual(sha256(self.pin_path.read_bytes()).hexdigest(), json.loads(raw)['pin_sha256'])
        calls = self.query.call_count
        self.advance(86400)
        self.assertEqual(self.tick(), final)
        self.assertEqual(self.status(), final)
        with self.assertRaisesRegex(soak.SoakRefused, 'record_terminal'):
            self.start()
        self.assertEqual(self.query.call_count, calls)
        self.assertEqual(self.record.read_bytes(), raw)

    def test_full_wall_time_without_boot_elapsed_cannot_pass(self):
        self.start()
        self.advance(86400, boottime=300)
        view = self.tick()
        self.assertEqual(view['state'], 'running')
        self.assertEqual((view['elapsed_seconds'], view['boot_elapsed_seconds'], view['gap_count']), (86400, 300, 1))

    def test_bad_initial_probe_refuses_without_record_or_guard(self):
        self.query.side_effect = PermissionError('secret detail')
        with self.assertRaisesRegex(soak.SoakRefused, 'initial_observation_unavailable'):
            self.start()
        self.assertFalse(self.record.exists())
        self.assertEqual(self.status()['state'], 'not_started')
        self.guard.assert_not_called()

    def test_bad_probe_and_missed_window_remain_visible_and_nonpassing(self):
        self.start()
        self.advance(601)
        self.query.side_effect = subprocess.TimeoutExpired('fixture', 12)
        bad = self.tick()
        self.assertEqual((bad['bad_samples'], bad['gap_count'], bad['latest']['coverage']), (1, 1, 'unknown'))
        self.query.side_effect = self.output
        self.advance(85799)
        view = self.tick()
        self.assertEqual((view['state'], view['reason']), ('failed', 'bad_observations'))
        self.assertGreater(view['max_gap_seconds'], 600)

    def test_reboot_and_clock_rollback_end_the_observation(self):
        for reboot in (False, True):
            with self.subTest(reboot=reboot):
                self.start()
                if reboot:
                    self.advance(300)
                    self.boot = self.NEW_BOOT
                else:
                    self.advance(-1)
                view = self.tick()
                self.assertEqual(view['state'], 'failed' if reboot else 'clock_invalid')
                self.assertEqual(view['reason'], 'boot_changed' if reboot else 'clock_rollback')
                self.assertEqual(view['reboot_count'], int(reboot))
                self.assertEqual(self.status(), view)
                self.record.unlink()

    def test_pause_guard_and_readonly_overdue_diagnostics(self):
        self.start()
        raw = self.record.read_bytes()
        self.advance(601)
        self.assertTrue(self.status()['sampling_overdue'])
        self.guard.side_effect = ValueError('guard closed')
        with self.assertRaisesRegex(ValueError, 'guard closed'):
            self.tick()
        self.assertEqual(self.record.read_bytes(), raw)
        paused = soak.pause(clock=self.clock)
        self.assertEqual((paused['state'], paused['reason']), ('paused', 'operator_paused'))
        self.assertEqual(self.tick(), paused)
        with self.assertRaisesRegex(soak.SoakRefused, 'record_terminal'):
            self.start()

    def test_binding_scope_drift_creates_bad_sample_without_manager_query(self):
        self.start()
        self.advance(300)
        with patch.object(soak, '_scope', return_value=(self.pin | {'ledger_identity': '1:3'}, self.root)):
            result = self.tick()
        self.assertEqual(result['bad_samples'], 1)
        self.assertEqual(result['latest']['coverage'], 'unknown')
        self.query.assert_called_once()

    def test_pin_hash_change_refuses_record_and_nonproduction_duration_is_rejected(self):
        self.start()
        self.pin_path.write_text(json.dumps(self.pin) + '\n', encoding='utf-8')
        with self.assertRaises(soak.SoakRefused):
            self.status()
        self.pin_path.write_text(json.dumps(self.pin | {'duration_seconds': 300}), encoding='utf-8')
        with self.assertRaises(soak.SoakRefused):
            self.status()

    def test_gate_rechecked_before_final_pass_commit(self):
        self.before_final_tick()
        raw = self.record.read_bytes()
        closed = Event()

        def query(*args):
            closed.set()
            return self.output(*args)

        def guard():
            if closed.is_set():
                raise ValueError('guard closed during snapshot')

        self.query.side_effect = query
        self.guard.side_effect = guard
        with self.assertRaisesRegex(ValueError, 'guard closed during snapshot'):
            self.tick()
        self.assertEqual(self.record.read_bytes(), raw)
        self.assertEqual(self.status()['state'], 'running')

    @staticmethod
    def serial_lock(waiting):
        mutex = Lock()

        @contextmanager
        def lock(*, wait_seconds=0):
            if wait_seconds:
                waiting.set()
                acquired = mutex.acquire(timeout=2)
            else:
                acquired = mutex.acquire(blocking=False)
            if not acquired:
                raise soak.SoakRefused('record_busy')
            try:
                yield
            finally:
                mutex.release()

        return lock

    def test_tick_that_already_holds_record_lock_can_finish_before_pause_is_accepted(self):
        self.before_final_tick()
        probing, release, waiting, accepted = (Event() for _ in range(4))

        def query(*args):
            probing.set()
            self.assertTrue(release.wait(2))
            return self.output(*args)

        self.query.side_effect = query
        with patch.object(soak, '_lock', self.serial_lock(waiting)), ThreadPoolExecutor(max_workers=2) as pool:
            ticks = pool.submit(self.tick)
            self.assertTrue(probing.wait(2))
            pause = pool.submit(soak.pause, clock=self.clock, before_commit=accepted.set)
            self.assertTrue(waiting.wait(2))
            self.assertFalse(accepted.is_set())
            release.set()
            self.assertEqual(ticks.result(timeout=2)['state'], 'passed')
            self.assertEqual(pause.result(timeout=2)['state'], 'passed')
            self.assertTrue(accepted.is_set())

    def test_accepted_pause_keeps_a_later_tick_from_passing_and_is_retryable(self):
        self.before_final_tick()
        waiting, accepted, release = (Event() for _ in range(3))

        def gate():
            accepted.set()
            self.assertTrue(release.wait(2))
            raise OSError('interrupted after installer accepted pause')

        with patch.object(soak, '_lock', self.serial_lock(waiting)), ThreadPoolExecutor(max_workers=2) as pool:
            pause = pool.submit(soak.pause, clock=self.clock, before_commit=gate)
            self.assertTrue(accepted.wait(2))
            with self.assertRaisesRegex(soak.SoakRefused, 'record_busy'):
                self.tick()
            release.set()
            with self.assertRaisesRegex(OSError, 'interrupted'):
                pause.result(timeout=2)
            self.guard.side_effect = ValueError('persisted pausing gate closed')
            with self.assertRaisesRegex(ValueError, 'persisted pausing'):
                self.tick()
            self.assertEqual(soak.pause(clock=self.clock)['state'], 'paused')
            self.assertEqual(self.tick()['state'], 'paused')

    @unittest.skipUnless(sys.platform.startswith('linux'), 'native flock requires Linux')
    def test_native_lock_acquires_after_waiting_for_inflight_tick(self):
        path = soak.LOCK
        path.touch()
        waiting = Event()

        def acquire():
            waiting.set()
            with NATIVE_LOCK(wait_seconds=1):
                return True

        with patch.object(soak.common_install, '_root_metadata', side_effect=lambda p, **_: p.lstat()), \
                ThreadPoolExecutor(max_workers=1) as pool:
            with NATIVE_LOCK():
                waiter = pool.submit(acquire)
                self.assertTrue(waiting.wait(1))
                with self.assertRaisesRegex(soak.SoakRefused, 'record_busy'):
                    with NATIVE_LOCK():
                        self.fail('contended nonblocking lock acquired')
            self.assertTrue(waiter.result(timeout=2))


if __name__ == '__main__':
    unittest.main()
