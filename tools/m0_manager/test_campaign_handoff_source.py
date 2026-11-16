"""Exact Manager handoff evidence checks against a temporary campaign ledger."""

from contextlib import closing
import json
import sqlite3
import unittest
from unittest.mock import patch

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.campaign_validation import handoff_id
from sa_dispatch.clock import timestamp

from .campaign_handoff_source import read_handoff
from .readonly import SourceError, readonly_db


class CampaignHandoffSourceTests(unittest.TestCase):
    def setUp(self):
        from tools.m0_agent.test_campaign_worker import CampaignWorkerTests

        fixture = CampaignWorkerTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        fixture.register()
        for _ in range(3):
            fixture.tick('producer')
            fixture.tick('reviewer')
            fixture.clock.advance(180)
        self.ledger = fixture.f.ledger
        self.identity = handoff_id(fixture.config, 2)
        self.reviewer = fixture.status()['slots'][1]['reviewer']['task_id']

    def query(self, identity=None):
        with readonly_db(self.ledger) as db:
            return read_handoff(db, source_id='fixture-common', handoff_id=identity or self.identity,
                                observed_at=timestamp(self.fixture.clock.now()))

    def mutate(self, sql, params):
        with closing(sqlite3.connect(self.ledger)) as conn, conn:
            conn.execute(sql, params)

    def test_three_completed_chains_are_exact_bounded_and_read_only(self):
        before = self.ledger.read_bytes()
        with patch.object(ArtifactStore, 'get', side_effect=AssertionError('unexpected artifact read')):
            for index, expected in enumerate(self.fixture.status()['slots'], 1):
                identity = handoff_id(self.fixture.config, index)
                row = self.query(identity)
                self.assertEqual((row['handoff_id'], row['slot'], row['state']), (identity, index, 'completed'))
                self.assertEqual(row['context_ref'], expected['handoff']['context_ref'])
                self.assertEqual(row['accepted_ref'], expected['handoff']['accepted_ref'])
                self.assertEqual(row['terminal_receipt_ref'], expected['handoff']['receipt_ref'])
                self.assertEqual(row['reviewer']['output_ref'], expected['reviewer']['result_ref'])
                self.assertEqual(row['reviewer']['reservation']['usage_ref'],
                                 expected['reviewer']['usage_evidence_ref'])
                self.assertEqual(row['budget']['used_units'], 6)
                self.assertEqual(row['proof_basis'], 'ledger_hash_linkage')
                self.assertEqual(row['content_integrity'], 'not_checked')
                serialized = json.dumps(row)
                for private in ('prompt_ref', 'request_ref', 'result_json', 'params_json', 'fixture-secret',
                                'Authored public', 'from_conv', 'to_conv'):
                    self.assertNotIn(private, serialized)
        self.assertEqual(self.ledger.read_bytes(), before)

    def test_refuses_wrong_recipient(self):
        self.mutate("UPDATE control_handoffs SET to_conv='other-recipient' WHERE handoff_id=?", (self.identity,))
        with self.assertRaisesRegex(SourceError, 'source_campaign_handoff_unproven'):
            self.query()

    def test_refuses_missing_context_link(self):
        self.mutate('DELETE FROM control_events WHERE event_key=?', ('campaign-context:' + self.identity,))
        with self.assertRaisesRegex(SourceError, 'source_campaign_handoff_unproven'):
            self.query()

    def test_refuses_unknown_reviewer_even_with_completed_handoff_row(self):
        self.mutate("UPDATE tasks SET status='unknown' WHERE task_id=?", (self.reviewer,))
        with self.assertRaisesRegex(SourceError, 'source_campaign_handoff_unproven'):
            self.query()

    def test_refuses_wrong_context_fence(self):
        self.mutate('UPDATE control_events SET fence=2 WHERE event_key=?', ('campaign-context:' + self.identity,))
        with self.assertRaisesRegex(SourceError, 'source_campaign_handoff_unproven'):
            self.query()

    def test_missing_identity_does_not_fall_back_to_a_different_handoff(self):
        self.assertIsNone(self.query(handoff_id(self.fixture.config, 4)))
        self.assertIsNone(self.query('synthetic-index:one'))


if __name__ == '__main__':
    unittest.main()
