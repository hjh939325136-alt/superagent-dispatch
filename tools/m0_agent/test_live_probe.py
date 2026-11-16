"""Exercise the one-request probe with a synthetic credential and provider."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
import hashlib
from io import StringIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from unittest.mock import Mock, patch

from tools.m0_agent import live_probe
from tools.m0_agent.deepseek_client import Completion, DeepSeekError, TokenUsage


class LiveProbeTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory(prefix='sa-probe-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.record = self.root / 'attempt.jsonl'
        self.secret_file = self.root / 'credential.key'
        self.key = b'synthetic-credential-for-test'
        self.model = 'deepseek-flash'
        self.reader = Mock(return_value=b'\xef\xbb\xbf' + self.key + b'\r\n')
        self.usage = TokenUsage(10, 2, 12, None, None)
        self.provider = Mock(return_value=Completion('private-response-id', 'provider-metadata', 'stop',
                                                    live_probe.EXPECTED, self.usage))

    def probe(self):
        return live_probe.run_probe(self.secret_file, self.model, self.record,
                                    credential_reader=self.reader, provider=self.provider)

    def test_fixed_request_and_redacted_result(self):
        result = self.probe()
        self.reader.assert_called_once_with(self.secret_file)
        self.provider.assert_called_once_with(model=self.model, approved_text=live_probe.PROMPT,
                                             api_key=self.key.decode(), max_tokens=32, thinking='disabled')
        self.assertEqual(result['status'], 'matched')
        self.assertEqual(result['usage']['total_tokens'], 12)
        self.assertEqual(result['response_sha256'], hashlib.sha256(b'SUPERAGENT_OK').hexdigest())
        self.assertEqual(live_probe.read_status(self.record), result)
        raw = self.record.read_bytes()
        for private in (self.key, b'private-response-id', b'provider-metadata', b'SUPERAGENT_OK', b'credential.key'):
            self.assertNotIn(private, raw)

    def test_intent_is_visible_before_reader_and_prevents_concurrent_attempt(self):
        reading = Event()
        release = Event()

        def read(path):
            reading.set()
            if not release.wait(timeout=5):
                raise TimeoutError
            return self.key

        self.reader.side_effect = read
        with ThreadPoolExecutor(max_workers=1) as executor:
            first = executor.submit(self.probe)
            try:
                self.assertTrue(reading.wait(timeout=5))
                status = live_probe.read_status(self.record)
                self.assertEqual(status['status'], 'unknown')
                self.assertEqual(status['error'], 'incomplete_record')
                with self.assertRaises(FileExistsError):
                    self.probe()
                self.provider.assert_not_called()
            finally:
                release.set()
            self.assertEqual(first.result(timeout=5)['status'], 'matched')
        with self.assertRaises(FileExistsError):
            self.probe()
        self.assertEqual(self.reader.call_count, 1)
        self.assertEqual(self.provider.call_count, 1)

    def test_partial_records_remain_reserved(self):
        self.record.write_bytes(b'{"unexpected":"private material')
        self.assertEqual(live_probe.read_status(self.record)['error'], 'incomplete_record')
        with self.assertRaises(FileExistsError):
            self.probe()
        self.reader.assert_not_called()
        self.provider.assert_not_called()

    def test_sync_failure_prevents_credential_read_and_provider_call(self):
        with patch.object(live_probe.os, 'fsync', side_effect=OSError('storage error')):
            with self.assertRaises(OSError):
                self.probe()
        with self.assertRaises(FileExistsError):
            self.probe()
        self.reader.assert_not_called()
        self.provider.assert_not_called()

    def test_provider_failures_are_redacted_and_not_retried(self):
        cases = (
            (DeepSeekError('deadline_exceeded', outcome_unknown=True), 'unknown', 'deadline_exceeded'),
            (DeepSeekError('timeout'), 'unknown', 'timeout'),
            (DeepSeekError('auth_rejected', status=401), 'provider_refused', 'auth_rejected'),
            (DeepSeekError(self.key.decode(), outcome_unknown=True), 'unknown', 'invalid_response'),
            (ValueError(self.key.decode()), 'unknown', 'provider_process_failed'),
        )
        for index, (failure, status, code) in enumerate(cases):
            with self.subTest(code=code):
                self.record = self.root / f'failure-{index}.jsonl'
                self.provider.reset_mock(side_effect=True)
                self.provider.side_effect = failure
                self.assertEqual(self.probe()['status'], status)
                self.assertEqual(live_probe.read_status(self.record)['error'], code)
                with self.assertRaises(FileExistsError):
                    self.probe()
                self.provider.assert_called_once()
                self.assertNotIn(self.key, self.record.read_bytes())

    def test_credential_failure_never_reaches_provider(self):
        self.reader.return_value = b'invalid key'
        self.assertEqual(self.probe()['status'], 'credential_refused')
        self.provider.assert_not_called()

    def test_unexpected_reply_and_truncation_keep_only_hash(self):
        for reason, expected in (('stop', 'unexpected_reply'), ('length', 'truncated')):
            self.record = self.root / f'{reason}.jsonl'
            self.provider.return_value = Completion('id', self.model, reason, 'other text', self.usage)
            self.assertEqual(self.probe()['status'], expected)
            self.assertNotIn(b'other text', self.record.read_bytes())

    def test_cli_status_is_read_only(self):
        result = self.probe()
        before = self.record.read_bytes()
        output = StringIO()
        with patch.object(live_probe, 'run_probe') as runner, redirect_stdout(output):
            self.assertEqual(live_probe.main(['status', '--record', str(self.record)]), 0)
            runner.assert_not_called()
        self.assertEqual(json.loads(output.getvalue()), result)
        self.assertEqual(self.record.read_bytes(), before)
