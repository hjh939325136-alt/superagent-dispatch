"""Authorized fixture reads, deterministic faults, and broker-visible audit boundaries."""

from hashlib import sha256
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from . import fixture_tools as fixture


DOMAINS = ['research-scatter', 'research-content']
QUERY = {'q': '剪辑 返工', 'domains': DOMAINS}
RESTRICTED = 'research-scatter/note/1TKW8H659WK91W9FWWSA8C7VYQ@1#40c7fc2e1740dddbc0623ba72c338692e2cd02dd639c48be1d1072ff77e657d4'
REVOKED = 'research-content/note/3PDARVCGPTHYN3YTDRZFPNCVKG@1#aa1062b8a56e9e188a40744e60503d6e2a945de4ef3c408eaf452216cbdb57d0'


class FixtureToolsTests(unittest.TestCase):
    def service(self, scenario='normal', arm='hermes', audit=None):
        return fixture.FixtureTools(experiment_id='fixture-e1', arm=arm, run_id='T3-R1', scenario=scenario, audit=audit)

    def test_exact_visible_version_and_real_restricted_revoked_denials(self):
        events = []
        service = self.service(audit=events.append)
        search = service.invoke(0, 'knowledge.search', QUERY)
        self.assertEqual(search['withheld']['restricted'], 1)
        self.assertEqual(search['withheld']['revoked'], 1)
        current = [item for item in search['results'] if item['status'] == 'current']
        self.assertEqual(len(current), 2)
        reference = next(item['ref'] for item in current if item['ref'].startswith('research-content/'))
        self.assertIn('@2#', reference)
        result = service.invoke(1, 'knowledge.get', {'ref': reference, 'domains': DOMAINS})
        self.assertEqual(result['result']['ref'], reference)
        for seq, ref in enumerate((reference.replace('@2#', '@1#'), RESTRICTED, REVOKED), 2):
            response = service.invoke(seq, 'knowledge.get', {'ref': ref, 'domains': DOMAINS})
            self.assertEqual(response, {'ok': False, 'error': {'code': 'NOT_FOUND', 'retryable': False}})
            self.assertEqual((events[seq]['code'], events[seq]['requested_ref'], events[seq]['delivered_refs']),
                             ('NOT_FOUND', ref, []))
        self.assertEqual(set(service.successful_refs()), {item['ref'] for item in search['results']})
        self.assertTrue(service.revalidate_refs())
        self.assertNotIn('snippet', json.dumps(events))
        self.assertNotIn('剪辑', json.dumps(events, ensure_ascii=False))

    def test_same_fault_per_immutable_arm_and_repeated_seq_does_not_repeat_audit(self):
        for arm in ('hermes', 'current_worker'):
            events = []
            service = self.service('recoverable', arm=arm, audit=events.append)
            first = service.invoke(0, 'knowledge.search', QUERY)
            self.assertEqual(first['error']['code'], 'TEMPORARY_UNAVAILABLE')
            self.assertTrue(first['error']['retryable'])
            self.assertEqual(service.invoke(0, 'knowledge.search', QUERY | {'limit': 5}), first)
            second = service.invoke(1, 'knowledge.search', QUERY)
            self.assertTrue(second['ok'])
            second['results'].clear()
            self.assertTrue(service.invoke(1, 'knowledge.search', QUERY)['results'])
            self.assertEqual([entry['sequence'] for entry in events], [0, 1])
            self.assertTrue(all(entry['arm'] == arm and entry['experiment_id'] == 'fixture-e1' for entry in events))
            with self.assertRaisesRegex(fixture.FixtureToolError, 'SEQUENCE_CONFLICT'):
                service.invoke(1, 'knowledge.search', QUERY | {'q': '其他查询'})

    def test_permanent_source_fault_allows_narrowing_to_healthy_source(self):
        service = self.service('unrecoverable')
        for seq in (0, 1):
            response = service.invoke(seq, 'knowledge.search', QUERY)
            self.assertEqual(response['error'],
                             {'code': 'SOURCE_UNAVAILABLE', 'retryable': False, 'source': 'research-content'})
        response = service.invoke(2, 'knowledge.search', QUERY | {'domains': ['research-scatter']})
        self.assertEqual(len(response['results']), 1)
        ref = response['results'][0]['ref']
        self.assertEqual(service.invoke(3, 'knowledge.get', {'ref': ref, 'domains': ['research-scatter']})['result']['ref'], ref)

    def test_cumulative_refs_recheck_visibility_and_cached_read_fails_closed(self):
        service = self.service()
        row = service.invoke(0, 'knowledge.search', QUERY)['results'][0]
        backend = fixture._backend()
        with patch.object(backend, 'get', side_effect=backend.SearchError('NOT_FOUND', 'private body')):
            self.assertFalse(service.revalidate_refs())
            with self.assertRaisesRegex(fixture.FixtureToolError, 'REFERENCE_NOT_VISIBLE'):
                service.invoke(0, 'knowledge.search', QUERY)
        self.assertIn(row['ref'], service.successful_refs())
        with patch.dict(fixture.CORPUS_PINS, {'grants.json': '0' * 64}):
            self.assertFalse(service.revalidate_refs())
            with self.assertRaisesRegex(fixture.FixtureToolError, 'FIXTURE_CORPUS_CHANGED'):
                service.invoke(1, 'knowledge.search', QUERY)

    def test_rejects_non_allowlisted_inputs_and_invalid_sequences(self):
        service = self.service()
        for sequence in (True, 1, -1, 32, '0'):
            with self.assertRaisesRegex(fixture.FixtureToolError, 'INVALID_SEQUENCE'):
                service.invoke(sequence, 'knowledge.search', QUERY)
        for extra in ({'q': 'https://example.invalid'}, {'q': '../fixture'}, {'q': 'SELECT * FROM records'},
                      {'path': 'some.json'}, {'grant': 'fixture-three-source'}, {'as_of': '2026-11-30'},
                      {'caller': 'someone'}, {'domains': ['signalhub']}, {'limit': True}):
            with self.assertRaisesRegex(fixture.FixtureToolError, 'INVALID_REQUEST'):
                service.invoke(0, 'knowledge.search', QUERY | extra)
        with self.assertRaisesRegex(fixture.FixtureToolError, 'UNSUPPORTED_TOOL'):
            service.invoke(0, 'read_file', QUERY)
        self.assertEqual(service.successful_refs(), ())
        self.assertTrue(service.invoke(0, 'knowledge.search', QUERY)['ok'])

    def test_audit_failure_does_not_expose_reply_or_consume_fault(self):
        sink = Mock(side_effect=[RuntimeError('private diagnostic'), None])
        service = self.service('recoverable', audit=sink)
        with self.assertRaisesRegex(fixture.FixtureToolError, 'AUDIT_REFUSED'):
            service.invoke(0, 'knowledge.search', QUERY)
        self.assertEqual(service.successful_refs(), ())
        self.assertEqual(service.invoke(0, 'knowledge.search', QUERY)['error']['code'], 'TEMPORARY_UNAVAILABLE')
        self.assertEqual(sink.call_args_list[0], sink.call_args_list[1])

    def test_code_and_benchmark_pin_same_corpus_when_local_candidate_exists(self):
        path = Path(__file__).resolve().parents[2] / 'state/hermes-comparison-candidate/benchmark-v1.json'
        benchmark = json.loads(path.read_text(encoding='utf-8')) if path.is_file() else None
        fixture._check_pins()
        for name, expected in fixture.CORPUS_PINS.items():
            local = fixture.ROOT / 'fixtures' / name
            self.assertEqual(sha256(local.read_bytes()).hexdigest(), expected)
            if benchmark is not None:
                self.assertEqual(benchmark['corpus']['file_sha256'][f'knowledge/m0_search/fixtures/{name}'], expected)


if __name__ == '__main__':
    unittest.main()
