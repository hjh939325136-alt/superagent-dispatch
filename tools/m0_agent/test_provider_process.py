"""Real subprocess tests using a synthetic child, never the provider endpoint."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from tools.m0_agent import provider_process as runner
from tools.m0_agent.deepseek_client import DeepSeekError


class ProcessTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix='sa-provider-process-')
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.script = self.root / 'synthetic_child.py'

    def invoke(self, body, **kwargs):
        self.script.write_text(body, encoding='utf-8')
        with patch.object(runner, '_argv', return_value=[sys.executable, '-I', '-B', str(self.script)]):
            return runner.complete_bounded(model='simulated-model', approved_text='synthetic public example',
                api_key='synthetic-secret-only', max_tokens=10, **kwargs)

    def test_pipe_and_scrubbed_environment(self):
        code = '''import json,os,sys
r=json.load(sys.stdin)
assert r['api_key']=='synthetic-secret-only'
assert all('synthetic-secret-only' not in x for x in sys.argv)
assert 'HTTPS_PROXY' not in os.environ and 'SSLKEYLOGFILE' not in os.environ
print(json.dumps({'ok':True,'completion':{'response_id':'simulated','model':r['model'],
'finish_reason':'stop','text':'simulated output','usage':{'prompt_tokens':1,'completion_tokens':2,
'total_tokens':3,'prompt_cache_hit_tokens':None,'prompt_cache_miss_tokens':None}}}))
'''
        with patch.dict(os.environ, {'HTTPS_PROXY':'http://not-used', 'SSLKEYLOGFILE':'not-used'}):
            result = self.invoke(code)
        self.assertEqual(result.text, 'simulated output')
        self.assertFalse(result.truncated)

    def test_deadline_kills_and_reaps_child_without_retry(self):
        marker = self.root / 'started'
        code = 'import pathlib,sys,time\nsys.stdin.read()\npathlib.Path(' + repr(str(marker)) + ').write_text("one")\ntime.sleep(60)'
        with self.assertRaises(DeepSeekError) as caught:
            self.invoke(code, deadline_seconds=1)
        self.assertEqual(caught.exception.kind, 'deadline_exceeded')
        self.assertTrue(caught.exception.outcome_unknown)
        self.assertEqual(marker.read_text(), 'one')
        self.assertIsNone(caught.exception.__context__)

    def test_safe_auth_failure(self):
        with self.assertRaises(DeepSeekError) as caught:
            self.invoke('import sys\nsys.stdin.read()\nprint(\'{"ok":false,"kind":"auth_rejected","status":401,"outcome_unknown":false}\')')
        self.assertEqual(caught.exception.kind, 'auth_rejected')
        self.assertFalse(caught.exception.outcome_unknown)

    def test_child_error_and_bad_output_never_exposed(self):
        for code in ('import sys\nsys.stdin.read()\nraise Exception("synthetic-secret-only")',
                     'import sys\nsys.stdin.read()\nprint("synthetic-secret-only")'):
            with self.subTest(code=code), self.assertRaises(DeepSeekError) as caught:
                self.invoke(code)
            self.assertTrue(caught.exception.outcome_unknown)
            self.assertNotIn('synthetic-secret-only', repr(caught.exception))

    def test_actual_child_rejects_invalid_request_before_network(self):
        with self.assertRaises(DeepSeekError) as caught:
            runner.complete_bounded(model='', approved_text='synthetic', api_key='synthetic', max_tokens=1)
        self.assertEqual(caught.exception.kind, 'invalid_request')
        self.assertFalse(caught.exception.outcome_unknown)


if __name__ == '__main__':
    unittest.main()
