"""Real subprocess tests using a synthetic child, never the provider endpoint."""
from io import BytesIO
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from tools.m0_agent import provider_process as runner
from tools.m0_agent.deepseek_client import Completion, DeepSeekError, TokenUsage


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
assert set(r)=={'model','approved_text','api_key','max_tokens'}
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

    def test_explicit_thinking_reaches_subprocess(self):
        result = self.invoke('''import json,sys
r=json.load(sys.stdin)
assert r['thinking']=='disabled'
print(json.dumps({'ok':True,'completion':{'response_id':'simulated','model':r['model'],
'finish_reason':'stop','text':'simulated output','usage':{'prompt_tokens':1,'completion_tokens':2,
'total_tokens':3,'prompt_cache_hit_tokens':None,'prompt_cache_miss_tokens':None}}}))
''', thinking='disabled')
        self.assertEqual(result.text, 'simulated output')

    def test_connect_proxy_locator_is_passed_only_in_the_request_pipe(self):
        result = self.invoke('''import json,os,sys
r=json.load(sys.stdin)
assert r['https_connect_proxy']=={'host':'10.0.2.2','port':7897}
assert '10.0.2.2' not in ' '.join(sys.argv)
assert 'HTTPS_PROXY' not in os.environ
print(json.dumps({'ok':True,'completion':{'response_id':'simulated','model':r['model'],
'finish_reason':'stop','text':'simulated output','usage':{'prompt_tokens':1,'completion_tokens':2,
'total_tokens':3,'prompt_cache_hit_tokens':None,'prompt_cache_miss_tokens':None}}}))
''', thinking='disabled', https_connect_proxy={'host': '10.0.2.2', 'port': 7897})
        self.assertEqual(result.text, 'simulated output')

    def test_child_accepts_only_known_fields_and_thinking_value(self):
        request = dict(model='simulated-model', approved_text='synthetic', api_key='synthetic', max_tokens=1)
        provider = Mock(return_value=Completion('id', 'simulated-model', 'stop', 'synthetic',
                                                TokenUsage(1, 1, 2, None, None)))
        for optional, ok in (({}, True), ({'thinking': 'disabled'}, True), ({'thinking': None}, False),
                             ({'thinking': {}}, False), ({'thinking': 'auto'}, False),
                             ({'https_connect_proxy': {'host': '10.0.2.2', 'port': 7897}}, True),
                             ({'https_connect_proxy': {'host': 'bad', 'port': 7897}}, False),
                             ({'thinking': 'enabled', 'extra': True}, False)):
            with self.subTest(optional=optional):
                provider.reset_mock()
                payload = request | optional
                input_buffer = BytesIO(json.dumps(payload).encode())
                output_buffer = BytesIO()
                with (patch.object(runner.sys, 'stdin', Mock(buffer=input_buffer)),
                      patch.object(runner.sys, 'stdout', Mock(buffer=output_buffer)),
                      patch.object(runner, 'complete_text', provider)):
                    runner._child()
                envelope = json.loads(output_buffer.getvalue())
                self.assertIs(envelope['ok'], ok)
                if ok:
                    provider.assert_called_once_with(**payload)
                else:
                    self.assertEqual(envelope['kind'], 'invalid_request')
                    provider.assert_not_called()

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
