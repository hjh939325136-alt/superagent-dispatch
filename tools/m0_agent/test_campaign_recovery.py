"""Campaign recovery requires the actual handoff proof and consumed request state."""
from contextlib import closing
from dataclasses import asdict
from datetime import timedelta
import json
import sqlite3
import unittest

from sa_dispatch.bundle import snapshot_records
from sa_dispatch.canonical import canonical_json
from sa_dispatch.clock import FakeClock, timestamp
from sa_dispatch.external_migration import upgrade
from sa_dispatch.models import Ref
from sa_dispatch.routing import validate_mixed_records
from tools.m0_agent import campaign_control, campaign_worker, common_recovery
from tools.m0_agent import test_common_recovery as recovery_fixture
from tools.m0_agent.common_worker import ledger_identity
from tools.m0_agent.deepseek_client import Completion, TokenUsage
from tools.m0_native_mixed.maintenance import MaintenanceError


class CampaignRecoveryTests(unittest.TestCase):
    def setUp(self):
        recovery_fixture.CommonRecoveryTests.setUp(self)
        upgrade(self.layout.ledger, self.root / 'upgrade-campaign', offline=True)
        self.clock = FakeClock()
        def prompt(role):
            ref = self.cas.put(('Authored ' + role + ' instructions.').encode())
            return asdict(Ref('operator', 'agent_prompt', role, '1', ref))
        self.config = dict(
            schema='superagent.agent_campaign.v1', campaign_id='recovery-case',
            authorization_id='recovery-authority', version=1, machine_id='a' * 32,
            ledger_identity=ledger_identity(self.layout.ledger), starts_at=timestamp(self.clock.now()),
            expires_at=timestamp(self.clock.now() + timedelta(minutes=3)), interval_seconds=180,
            slots=1, max_requests=2, model='fixture-model', max_tokens=64,
            producer_prompt_ref=prompt('producer'), reviewer_prompt_ref=prompt('reviewer'))
        campaign_control.register(self.layout.ledger, self.cas, self.config, clock=self.clock)

    def tick(self, role):
        return campaign_worker.tick(
            self.layout.ledger, self.cas, self.config, role=role, clock=self.clock,
            credential_reader=lambda: 'fixture-credential',
            provider_call=lambda **_: Completion('response-' + role, 'fixture-model', 'stop',
                                                'Reviewed service check.', TokenUsage(5, 4, 9, 0, 5)))

    def test_completed_handoff_survives_quarantine_and_has_no_execution_authority(self):
        self.tick('producer')
        self.tick('reviewer')
        with closing(sqlite3.connect(self.layout.ledger)) as conn:
            conn.row_factory = sqlite3.Row
            validate_mixed_records(snapshot_records(conn), allow_external=True)
        receipt = common_recovery.capture(self.layout, self.archive, offline=True)
        restored = common_recovery.restore_to_quarantine(
            self.archive, self.quarantine, expected_manifest_sha256=receipt['manifest_sha256'])
        self.assertTrue(restored['restore_verified'])
        self.assertFalse(restored['external_execution_enabled'])
        with closing(sqlite3.connect(self.quarantine / 'ledger/dispatch.sqlite3')) as conn:
            self.assertEqual(conn.execute('SELECT state FROM control_handoffs').fetchone(), ('completed',))
            self.assertEqual(conn.execute('SELECT used_units,reserved_units FROM control_budgets').fetchone(), (2, 0))
            self.assertEqual(conn.execute('SELECT SUM(attempts) FROM tasks').fetchone(), (2,))

    def test_self_consistent_manifest_cannot_drop_referenced_context(self):
        self.tick('producer')
        self.tick('reviewer')
        common_recovery.capture(self.layout, self.archive, offline=True)
        with closing(sqlite3.connect(self.layout.ledger)) as conn:
            ref = json.loads(conn.execute("SELECT refs_json FROM control_events WHERE event_key GLOB 'campaign-context:*'").fetchone()[0])[1]
        alias = self.cas.path(ref).relative_to(self.cas.root).as_posix()
        member = 'cas/' + alias
        path = self.archive / member
        path.unlink()
        manifest_path = self.archive / common_recovery.MANIFEST
        manifest = json.loads(manifest_path.read_bytes())
        entry = manifest['entries'].pop(member)
        manifest['file_count'] -= 1
        manifest['total_bytes'] -= entry['bytes']
        manifest_path.write_bytes(canonical_json(manifest))
        with self.assertRaises(MaintenanceError):
            common_recovery.verify(self.archive)


if __name__ == '__main__':
    unittest.main()
