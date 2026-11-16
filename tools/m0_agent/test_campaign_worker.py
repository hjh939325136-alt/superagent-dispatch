"""Campaign transaction, handoff, and process-loss tests on mixed SQLite."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import asdict, replace
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

from sa_dispatch.bundle import snapshot_records
from sa_dispatch.campaign_validation import DEVICES, SCHEMA, validate_campaign_records
from sa_dispatch.clock import timestamp
from sa_dispatch import collector
from sa_dispatch.models import DispatchError
from sa_dispatch.routing import RouteRejected
from sa_dispatch.transport import CoordinatorAPI, TransportError

from . import campaign_control, campaign_worker, test_common_worker
from .deepseek_client import DeepSeekError


class CampaignWorkerTests(unittest.TestCase):
    def setUp(self):
        self.f = test_common_worker.CommonWorkerTests()
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.f.upgrade()
        start = self.f.clock.now()
        refs = {}
        for role in DEVICES:
            prompt = f'Authored public {role} prompt.'
            refs[role + '_prompt_ref'] = asdict(self.f.config.grant.prompt_ref) | {
                'id': role, 'hash': self.f.cas.put(prompt.encode('utf-8'))}
        self.config = dict(schema=SCHEMA, campaign_id='test-campaign', authorization_id='test-authorization',
                           version=1, machine_id='a' * 32, ledger_identity=self.f.config.ledger_identity,
                           starts_at=timestamp(start), expires_at=timestamp(start + timedelta(seconds=540)),
                           interval_seconds=180, slots=3, max_requests=6, model='approved-model',
                           max_tokens=80, **refs)
        self.args = self.f.ledger, self.f.cas, self.config
        self.clock = self.f.clock
        self.provider = Mock(return_value=self.f.reply)
        self.credential = Mock(return_value='fixture-secret-not-for-output')

    def register(self):
        return campaign_control.register(*self.args, clock=self.clock)

    def status(self):
        return campaign_control.status(*self.args, clock=self.clock)

    def tick(self, role, **overrides):
        options = dict(credential_reader=self.credential, provider_call=self.provider, clock=self.clock)
        return campaign_worker.tick(*self.args, role, **(options | overrides))

    def records(self):
        with closing(self.f.local._connect()) as conn:
            conn.execute('BEGIN')
            return snapshot_records(conn)

    def test_three_slots_preserve_already_spent_common_and_http_isolation(self):
        self.f.register()
        self.f.tick()
        common_before = self.f.status()
        self.assertEqual(common_before['request_units_used'], 1)
        self.assertEqual(self.register()['status'], 'active')
        for index in range(3):
            self.tick('reviewer')
            summary = self.tick('producer')
            self.assertEqual(summary['slots'][index]['handoff']['state'], 'offered')
            summary = self.tick('reviewer')
            pair = summary['slots'][index]
            self.assertEqual((pair['producer']['status'], pair['reviewer']['status'], pair['handoff']['state']),
                             ('succeeded', 'succeeded', 'completed'))
            self.assertEqual((pair['producer']['device_id'], pair['reviewer']['device_id']),
                             tuple(DEVICES.values()))
            self.assertIn('Producer data (JSON)', self.provider.call_args.kwargs['approved_text'])
            self.assertIn(self.f.reply.text, self.provider.call_args.kwargs['approved_text'])
            self.clock.advance(180)
        self.assertEqual(self.tick('producer')['status'], 'expired')
        final = self.tick('reviewer')
        self.assertEqual((final['request_units_used'], final['request_units_held'], self.provider.call_count), (6, 0, 6))
        idle = self.records()
        self.clock.advance(10)
        self.tick('producer')
        self.tick('reviewer')
        self.assertEqual(idle, self.records())
        self.assertEqual(self.f.tick(), common_before)
        owned = validate_campaign_records(self.records(), self.f.cas.get)
        self.assertEqual({key: len(value) for key, value in owned.items()},
                         dict(schedules=2, devices=2, tasks=6, schedule_firings=6))
        http = self.f.store('http_synthetic')
        http.register_device('http-worker', ['worker.compute'])
        token = 'fixture-' + 'x' * 40
        api = CoordinatorAPI(http, {'http-worker': hashlib.sha256(token.encode()).hexdigest()},
                             synthetic_handoff=True, roles={'http-worker': ['worker']}, artifact_store=self.f.cas)
        self.assertEqual(api.invoke('/claim', {}, 'Bearer ' + token), {'lease': None})
        with self.assertRaises(TransportError):
            api.invoke('/status', {'task_id': final['slots'][0]['producer']['task_id']}, 'Bearer ' + token)

    def test_pause_expiry_missed_window_rollback_and_readonly_status(self):
        self.register()
        before = self.records()
        self.clock.advance(195)
        self.assertEqual(self.status()['slots'][0]['producer']['status'], 'missed')
        self.assertEqual(before, self.records())
        summary = self.tick('producer')
        self.assertEqual(summary['slots'][1]['producer']['status'], 'succeeded')
        self.clock.advance(-160)
        self.assertEqual(self.tick('producer')['status'], 'clock_rollback')
        self.assertEqual(campaign_control.stop(*self.args, clock=self.clock)['status'], 'paused')
        paused_records = self.records()
        self.assertEqual(campaign_control.stop(*self.args, clock=self.clock)['status'], 'paused')
        self.assertEqual(paused_records, self.records())
        self.clock.advance(600)
        self.assertEqual(self.tick('reviewer')['slots'][1]['handoff']['state'], 'expired')
        self.assertEqual(self.tick('producer')['request_units_used'], 1)
        self.provider.assert_called_once()

    def test_configuration_immutable_credentials_and_stale_cas_fail_closed(self):
        self.assertEqual(self.status()['status'], 'unregistered')
        self.assertEqual(self.register(), self.register())
        for changed in (self.config | {'max_tokens': 81}, self.config | {'campaign_id': 'other-campaign'}):
            with self.assertRaises(DispatchError):
                campaign_control.register(*self.args[:2], changed, clock=self.clock)
        with self.assertRaises(DispatchError):
            self.tick('third-role')
        with self.assertRaises(DispatchError):
            self.tick('producer', credential_reader=lambda: '')
        with self.assertRaises(DispatchError):
            self.tick('producer', credential_reader=lambda: 'Authored')
        self.assertEqual(self.status()['request_units_held'], 0)
        self.provider.assert_not_called()
        output = self.tick('producer')['slots'][0]['producer']['result_ref']
        self.f.cas.path(output).write_bytes(b'mutated producer output')
        with self.assertRaises(DispatchError):
            self.tick('reviewer')
        self.assertEqual(self.status()['slots'][0]['handoff']['state'], 'offered')
        self.assertEqual(self.status()['request_units_held'], 0)
        self.provider.assert_called_once()
        with self.assertRaises(DispatchError):
            validate_campaign_records(self.records(), self.f.cas.get)

    def test_concurrent_claims_and_role_fence_reject(self):
        self.register()
        with ThreadPoolExecutor(max_workers=4) as pool:
            output = list(pool.map(lambda _: self.tick('producer'), range(4)))
        self.assertEqual(len({entry['slots'][0]['producer']['task_id'] for entry in output}), 1)
        self.provider.assert_called_once()
        store = campaign_control._CampaignStore(*self.args, clock=self.clock)
        lease = self.status()['slots'][0]['producer']['lease_id']
        with self.assertRaises(RouteRejected):
            store.mark_unknown(lease, 1, 'invalid_response', DEVICES['reviewer'])

    def test_reviewer_http_failure_cannot_complete_handoff(self):
        self.register()
        self.tick('producer')
        failure = Mock(side_effect=DeepSeekError('rate_limited', status=429))
        summary = self.tick('reviewer', provider_call=failure)
        self.assertEqual(summary['slots'][0]['reviewer']['status'], 'failed')
        self.assertEqual(summary['slots'][0]['handoff']['state'], 'rejected')
        self.assertEqual(summary['request_units_used'], 2)
        self.tick('reviewer', provider_call=failure)
        failure.assert_called_once()
        validate_campaign_records(self.records(), self.f.cas.get)

    def test_collector_replays_frozen_suffix_from_pinned_anchor(self):
        self.f.register()
        self.f.tick()
        binding = replace(collector.SourceBinding.enroll('common', 'dispatch', self.f.ledger, 'fixture', ('0013',)),
                          synthetic_only=False, data_class='local_only', profile='common_metadata_0013')
        reader = collector.Collector(self.f.ledger, clock=self.clock)
        anchor = reader.sync(binding, 'control_events')
        self.assertEqual(anchor['last_seq'], 3)
        self.register()
        self.tick('producer')
        self.tick('reviewer')
        campaign_control.stop(*self.args, clock=self.clock)
        with patch.object(collector, '_KINDS', collector._KINDS - {'campaign.context_read', 'campaign.paused'}):
            frozen = reader.sync(binding, 'control_events')
        self.assertEqual((frozen['kind'], frozen['last_seq']), ('redaction_rejected', 3))
        original_gap, = self.records()['collector_gaps']
        self.assertEqual(reader.sync(binding, 'control_events')['state'], 'frozen')
        adopted = reader.adopt(binding, 'control_events', reason='accept_redaction',
                               actor='operator', resume_after=anchor['last_seq'])
        self.assertEqual(adopted, dict(state='adopted', epoch=2, last_seq=3))
        replayed = reader.sync(binding, 'control_events')
        self.assertEqual((replayed['state'], replayed['projection_lag']), ('synced', 0))
        self.assertEqual(reader.sync(binding, 'control_events')['added'], 0)
        self.assertEqual(reader.sync(binding, 'schedule_events')['state'], 'synced')
        records = self.records()
        resolved, = records['collector_gaps']
        self.assertEqual(resolved['gap_id'], original_gap['gap_id'])
        self.assertEqual(json.loads(resolved['resolution']), dict(reason='accept_redaction', resume_after=3, epoch=2))
        projected = records['unified_events']
        events = [row for row in projected if row['source_id'].startswith('control_events:')]
        self.assertEqual({row['source_id'] for row in events},
                         {f'control_events:{seq}' for seq in range(1, replayed['last_seq'] + 1)})
        self.assertEqual(len(events), replayed['last_seq'])
        reading, = (row for row in events if row['kind'] == 'campaign.context_read')
        self.assertEqual(len(json.loads(reading['refs_json'])), 3)
        self.assertEqual(sum(row['kind'] == 'campaign.paused' for row in projected), 2)
        for text in (self.f.reply.text, 'Authored public reviewer prompt.', 'observed', 'operator_stop'):
            self.assertNotIn(text, collector.canonical(projected))

        source = next(row for row in records['control_events'] if row['kind'] == 'campaign.context_read')
        source['_collector_seq'] = source['seq']
        with self.assertRaises(ValueError):
            collector.project(replace(binding, profile=None), 'control_events', source, source['at'])
        with self.assertRaises(ValueError):
            collector.project(binding, 'control_events', source | {'refs_json': '["arbitrary body"]'}, source['at'])

    def test_offer_and_receiver_claim_rollback_are_atomic(self):
        self.register()
        with patch.object(campaign_control._CampaignStore, '_offer', side_effect=RuntimeError('interrupt')):
            with self.assertRaises(RuntimeError):
                self.tick('producer')
        interrupted = self.status()
        self.assertEqual(interrupted['slots'][0]['producer']['status'], 'leased')
        self.assertIsNone(interrupted['slots'][0]['handoff'])
        self.assertEqual((interrupted['request_units_used'], interrupted['request_units_held']), (0, 1))
        self.clock.advance(180)
        self.tick('producer')
        with patch.object(campaign_control._CampaignStore, '_grant_lease', side_effect=RuntimeError('interrupt')):
            with self.assertRaises(RuntimeError):
                self.tick('reviewer')
        interrupted = self.status()
        self.assertEqual(interrupted['slots'][1]['handoff']['state'], 'offered')
        self.assertIsNone(interrupted['slots'][1]['reviewer']['task_id'])
        self.assertEqual((interrupted['request_units_used'], interrupted['request_units_held']), (1, 1))
        self.assertEqual(self.tick('reviewer')['slots'][1]['handoff']['state'], 'completed')
        self.assertEqual(self.provider.call_count, 3)

    def test_actual_reviewer_process_exit_holds_budget_without_replay(self):
        self.register()
        self.tick('producer')
        path = self.f.root / 'campaign.json'
        path.write_text(json.dumps(self.config), encoding='utf-8')
        marker = self.f.root / 'provider-attempt'
        code = '''
import json,os,sys
from pathlib import Path
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.clock import FakeClock
from tools.m0_agent.campaign_worker import tick
def provider(**kwargs):
    with Path(sys.argv[4]).open('xb') as stream:
        stream.write(b'one reviewer attempt'); stream.flush(); os.fsync(stream.fileno())
    os._exit(72)
config=json.loads(Path(sys.argv[3]).read_text(encoding='utf-8'))
tick(Path(sys.argv[1]),ArtifactStore(sys.argv[2]),config,'reviewer',
     lambda:'fixture-not-a-real-key',provider,FakeClock())
'''
        root = Path(__file__).resolve().parents[2]
        env = dict(os.environ, PYTHONPATH=os.pathsep.join((str(root / 'dispatch'), str(root))))
        result = subprocess.run([sys.executable, '-B', '-c', code, str(self.f.ledger), str(self.f.cas.root),
                                 str(path), str(marker)], env=env, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 72, result.stderr.decode(errors='replace'))
        self.assertEqual(self.status()['slots'][0]['handoff']['state'], 'accepted')
        self.clock.advance(121)
        prohibited = Mock(side_effect=AssertionError('must not repeat ambiguous provider request'))
        for _ in range(2):
            summary = self.tick('reviewer', credential_reader=prohibited, provider_call=prohibited)
            reviewer = summary['slots'][0]['reviewer']
            self.assertEqual((reviewer['status'], reviewer['reservation_state']), ('unknown', 'uncertain'))
            self.assertEqual((summary['request_units_used'], summary['request_units_held']), (1, 1))
        prohibited.assert_not_called()
        self.assertEqual(marker.read_bytes(), b'one reviewer attempt')


if __name__ == '__main__':
    unittest.main()
