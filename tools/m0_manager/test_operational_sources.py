"""Exercise the common 0013 snapshot with protected-file and systemd fixtures."""

from contextlib import ExitStack, closing
from dataclasses import asdict
from datetime import timedelta
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

from sa_dispatch.clock import timestamp
from sa_dispatch.campaign_validation import SCHEMA as CAMPAIGN_SCHEMA

from tools.m0_agent import campaign_control, test_common_worker

from . import operational_sources as ops


class OperationalSnapshotTests(unittest.TestCase):
    MACHINE = 'f' * 32
    BOOT = '01010101-0202-0303-0404-050505050505'
    OLD_BOOT = '02020202-0303-0404-0505-060606060606'

    def setUp(self):
        self.fixture = test_common_worker.CommonWorkerTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.upgrade()
        self.clock = self.fixture.clock
        self.ledger = self.fixture.ledger
        root = self.fixture.root
        self.paths = {key: root / (key.lower() + '.json') for key in
                      ('COMMON_STATE', 'CAMPAIGN_STATE', 'CAMPAIGN_CONFIG', 'COLLECTOR_STATE',
                       'COLLECTOR_PIN', 'COLLECTOR_HEALTH', 'MACHINE_ID', 'BOOT_ID')}
        self.paths['LEDGER'] = self.ledger
        self.paths['MACHINE_ID'].write_text(self.MACHINE, encoding='ascii')
        self.paths['BOOT_ID'].write_text(self.BOOT, encoding='ascii')
        self.config = dict(
            schema=CAMPAIGN_SCHEMA, campaign_id='one', authorization_id='one-auth', version=1, machine_id=self.MACHINE,
            ledger_identity=self.fixture.config.ledger_identity, starts_at=self.observed(),
            expires_at=timestamp(self.clock.now() + timedelta(seconds=60)), interval_seconds=60,
            slots=1, max_requests=2, model='approved-model', max_tokens=20,
            producer_prompt_ref=asdict(self.fixture.config.grant.prompt_ref),
            reviewer_prompt_ref=asdict(self.fixture.config.grant.prompt_ref))
        campaign_control.register(self.ledger, self.fixture.cas, self.config, clock=self.clock)
        campaign_raw = self.write('CAMPAIGN_CONFIG', self.config)
        self.campaign = dict(schema='superagent.agent_campaign.install.v1', machine_id=self.MACHINE,
                             ledger_identity=self.config['ledger_identity'], phase='enabled',
                             campaign_id='one', config_sha256=sha256(campaign_raw).hexdigest())
        self.write('CAMPAIGN_STATE', self.campaign)
        self.common = dict(schema='superagent.common.external.install.v1', machine_id=self.MACHINE,
                           ledger_identity=self.config['ledger_identity'], phase='ready',
                           pre_live_recovery={'restore_verified': True},
                           collector={'source_id': 'common-dispatch', 'source_instance': 'fixture'})
        self.write('COMMON_STATE', self.common)
        self.limits = dict(stale_after_seconds=180, max_ledger_bytes=33554432, min_free_bytes=268435456)
        pin = dict(schema='superagent.common.collector_runtime.v1', machine_id=self.MACHINE,
                   ledger_identity=self.config['ledger_identity'], binding_raw_sha256='0' * 64,
                   binding_sha256='1' * 64, **self.limits)
        pin_raw = self.write('COLLECTOR_PIN', pin)
        self.collector = {key: pin[key] for key in
                          ('machine_id', 'ledger_identity', 'binding_raw_sha256', 'binding_sha256')}
        self.collector.update(schema='superagent.common.collector_install.v1', phase='enabled',
                              pin_sha256=sha256(pin_raw).hexdigest())
        self.write('COLLECTOR_STATE', self.collector)
        self.health = dict(schema='superagent.common.collector_health.v1', status='ok', code='synced',
                           boot_id=self.BOOT, attempt_at=self.observed(), success_boot_id=self.BOOT,
                           success_at=self.observed(), stale=False, limits=self.limits,
                           metrics_scope='ledger_and_sqlite_sidecars',
                           metrics=dict(ledger_bytes=1000, sidecar_bytes=0, free_bytes=2**30),
                           streams={key: dict(last_seq=4, projection_lag=0, state='synced', code='synced')
                                    for key in ops.STREAMS})
        self.write('COLLECTOR_HEALTH', self.health)
        info = self.ledger.stat()
        self.source = {'source_id': 'common-dispatch', 'source_instance': 'fixture',
                       'class': 'local_only', 'locator': str(self.ledger),
                       'options': {'profile': 'common_metadata_0013',
                                   'identity': {'device': info.st_dev, 'inode': info.st_ino}}}
        stack = self.enterContext(ExitStack())
        stack.enter_context(patch.multiple(ops, **self.paths))
        stack.enter_context(patch.object(ops.sys, 'platform', 'linux'))
        stack.enter_context(patch.object(ops, 'now', side_effect=self.observed))
        self.protected = stack.enter_context(patch.object(ops, '_protected', side_effect=self.read))
        self.systemd = stack.enter_context(patch.object(ops.subprocess, 'run', side_effect=self.systemctl))
        self.inactive_campaign_timers = False

    @staticmethod
    def read(path, max_bytes, *, private=False):
        with path.open('rb') as stream:
            content = stream.read(max_bytes + 1)
        if len(content) > max_bytes:
            raise ValueError('oversize test receipt')
        return content

    def write(self, label, value):
        raw = json.dumps(value, separators=(',', ':')).encode('utf-8')
        self.paths[label].write_bytes(raw)
        return raw

    def observed(self):
        return timestamp(self.clock.now())

    def systemctl(self, argv, **kwargs):
        self.assertEqual(argv[:2], ['/usr/bin/systemctl', 'show'])
        self.assertEqual(tuple(argv[argv.index('--') + 1:]), ops.UNITS)
        self.assertEqual(kwargs['timeout'], 3)
        blocks = []
        for name in ops.UNITS:
            enabled = name.endswith('.timer') or name == ops.COORDINATOR
            inactive = (name in ops.CAMPAIGN_TIMERS and self.inactive_campaign_timers) or not enabled
            status = 'inactive' if inactive else 'active'
            sub_state = 'dead' if inactive else 'waiting' if name.endswith('.timer') else 'running'
            blocks.append(f'Id={name}\nLoadState=loaded\nActiveState={status}\nSubState={sub_state}\n'
                          f'UnitFileState={"enabled" if enabled else "static"}')
        return subprocess.CompletedProcess(argv, 0, '\n\n'.join(blocks).encode('ascii'))

    def snapshot(self, source=None):
        with closing(self.fixture.local._connect()) as conn:
            conn.execute('BEGIN')
            return ops.operational_snapshot(conn, source or self.source, self.observed())

    def test_fresh_scope_is_readonly_and_stale_is_computed_from_current_boot_time(self):
        before = self.ledger.read_bytes()
        current = self.snapshot()
        self.assertEqual((current['coverage'], current['state_basis'], current['non_atomic']), ('complete', 'current', True))
        self.assertEqual(current['attention'], [])
        self.assertEqual(current['observations']['boot']['boot_id'], self.BOOT)
        self.assertEqual(current['observations']['collector']['freshness'], 'fresh')
        self.assertEqual(current['observations']['campaign']['state'], 'active')
        self.clock.advance(180)
        stale = self.snapshot()
        self.assertEqual(stale['observations']['collector']['stale_reason'], 'success_overdue')
        self.assertEqual(stale['observations']['campaign']['state'], 'intentional_idle')
        self.assertEqual(stale['observations']['campaign']['authorization_state'], 'expired')
        self.assertEqual(self.ledger.read_bytes(), before)

    def test_old_boot_unavailable_boot_and_bad_health_are_distinct(self):
        self.write('COLLECTOR_HEALTH', self.health | {'boot_id': self.OLD_BOOT, 'success_boot_id': self.OLD_BOOT})
        report = self.snapshot()
        collected = report['observations']['collector']
        self.assertEqual((collected['freshness'], collected['attempt_boot_id'], collected['stale_reason']),
                         ('stale', self.OLD_BOOT, 'boot_changed'))
        self.paths['BOOT_ID'].unlink()
        report = self.snapshot()
        self.assertIn('boot', report['unknown'])
        self.assertEqual(report['observations']['collector']['freshness'], 'unknown')
        self.write('COLLECTOR_HEALTH', self.health | {'success_at': 'PRIVATE DATA'})
        report = self.snapshot()
        self.assertEqual(report['observations']['collector']['state'], 'unknown')
        self.assertNotIn('PRIVATE DATA', json.dumps(report))

    def test_successful_health_requires_consistent_streams_capacity_and_attempt(self):
        metrics = self.health['metrics']
        streams = self.health['streams']
        cases = (
            {'streams': streams | {'control_events': streams['control_events'] |
                                   {'state': 'frozen', 'code': 'source_rewritten'}}},
            {'metrics': metrics | {'sidecar_bytes': self.limits['max_ledger_bytes'] - metrics['ledger_bytes']}},
            {'metrics': metrics | {'free_bytes': self.limits['min_free_bytes']}},
            {'metrics': metrics | {'ledger_bytes': None}},
            {'success_boot_id': self.OLD_BOOT},
            {'success_at': timestamp(self.clock.now() - timedelta(seconds=1))},
        )
        for extra in cases:
            with self.subTest(fields=tuple(extra)):
                self.write('COLLECTOR_HEALTH', self.health | extra)
                report = self.snapshot()
                self.assertEqual(report['observations']['collector']['state'], 'unknown')
                self.assertIn('collector', report['unknown'])
        self.write('COLLECTOR_HEALTH', self.health | {
            'streams': streams | {'events': streams['events'] | {'projection_lag': 51}}})
        collected = self.snapshot()['observations']['collector']
        self.assertEqual(collected['state'], 'ok')
        self.assertEqual(collected['streams']['events']['projection_lag'], 51)

    def test_historical_or_replaced_binding_never_probes_host_runtime(self):
        archive = str(Path(self.ledger).with_name('quarantine.sqlite'))
        wrong_identity = self.source['options']['identity'] | {'inode': -1}
        for source in (self.source | {'locator': archive},
                       self.source | {'options': self.source['options'] | {'identity': wrong_identity}}):
            report = self.snapshot(source)
            self.assertEqual(report['state_basis'], 'unknown')
            self.assertEqual(report['unknown'], ['ledger'])
            self.assertEqual(report['observations']['systemd'], dict(state='unknown', observed_at=None))
        self.systemd.assert_not_called()
        self.protected.assert_not_called()

    def test_pause_expiry_and_pending_do_not_impute_campaign_failure(self):
        self.inactive_campaign_timers = True
        active = self.snapshot()
        self.assertIn('campaign_timer_inactive', {item['code'] for item in active['attention']})
        self.write('CAMPAIGN_STATE', self.campaign | {'phase': 'paused'})
        paused = self.snapshot()
        self.assertEqual(paused['observations']['campaign']['state'], 'intentional_idle')
        self.assertNotIn('campaign_timer_inactive', {item['code'] for item in paused['attention']})
        self.write('CAMPAIGN_STATE', self.campaign)
        self.clock.advance(65)
        expired = self.snapshot()
        self.assertEqual(expired['observations']['campaign']['authorization_state'], 'expired')
        self.assertNotIn('campaign_timer_inactive', {item['code'] for item in expired['attention']})
        rotation = self.paths['CAMPAIGN_STATE'].with_name('rotate.json')
        rotation.write_text(json.dumps(dict(schema='superagent.agent_campaign.rotation.v1',
                                            machine_id=self.MACHINE, phase='aborting')), encoding='utf-8')
        pending = self.snapshot()
        self.assertEqual(pending['observations']['campaign']['state'], 'maintenance_pending')
        self.assertIn('campaign_maintenance_pending', {item['code'] for item in pending['attention']})

    def test_missing_inaccessible_health_and_unloaded_units_are_unknown(self):
        self.paths['COLLECTOR_HEALTH'].unlink()
        output = b'Id=sa-dispatch.service\nLoadState=not-found\nActiveState=inactive\nSubState=dead\nUnitFileState=\n'
        self.systemd.side_effect = None
        self.systemd.return_value = subprocess.CompletedProcess([], 1, output)
        report = self.snapshot()
        self.assertEqual(report['observations']['systemd']['state'], 'unknown')
        self.assertEqual(report['observations']['collector']['state'], 'unknown')
        self.assertNotIn('coordinator_unavailable', {item['code'] for item in report['attention']})

        def inaccessible(path, limit, *, private=False):
            if path == self.paths['COLLECTOR_HEALTH']:
                raise PermissionError('opaque test location')
            return self.read(path, limit, private=private)

        self.protected.side_effect = inaccessible
        report = self.snapshot()
        self.assertIn('collector', report['unknown'])
        self.assertNotIn('opaque test location', json.dumps(report))

    def test_collected_transient_local_unit_is_observed_but_missing_permanent_unit_is_unknown(self):
        def local_collected(argv, **kwargs):
            original = self.systemctl(argv, **kwargs)
            local_loaded = (f'Id={ops.LOCAL}\nLoadState=loaded\nActiveState=inactive\nSubState=dead\n'
                            'UnitFileState=static').encode('ascii')
            local_absent = local_loaded.replace(b'LoadState=loaded', b'LoadState=not-found').replace(
                b'UnitFileState=static', b'UnitFileState=')
            return subprocess.CompletedProcess(argv, 1, original.stdout.replace(local_loaded, local_absent))

        self.systemd.side_effect = local_collected
        report = self.snapshot()
        units = report['observations']['systemd']['units']
        self.assertEqual(report['coverage'], 'complete')
        self.assertEqual(units[ops.LOCAL], dict(state='observed', load_state='not-found', active_state='inactive',
                                              sub_state='dead', unit_file_state='', availability='on_demand_not_running'))

        def permanent_missing(argv, **kwargs):
            original = local_collected(argv, **kwargs)
            loaded = b'Id=sa-dispatch-collector.timer\nLoadState=loaded\nActiveState=active\nSubState=waiting\nUnitFileState=enabled'
            missing = b'Id=sa-dispatch-collector.timer\nLoadState=not-found\nActiveState=inactive\nSubState=dead\nUnitFileState='
            return subprocess.CompletedProcess(argv, 1, original.stdout.replace(loaded, missing))

        self.systemd.side_effect = permanent_missing
        report = self.snapshot()
        self.assertEqual(report['coverage'], 'partial')
        self.assertEqual(report['observations']['systemd']['units'][ops.COLLECTOR_TIMER], {'state': 'unknown'})


if __name__ == '__main__':
    unittest.main()
