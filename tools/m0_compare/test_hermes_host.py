"""Hermes adapter contract and optional real-client smoke over a fake Unix socket."""

import importlib.metadata
import importlib.util
from http.server import BaseHTTPRequestHandler
import json
from pathlib import Path
import socketserver
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Thread
import unittest
from unittest.mock import Mock


def load_adapter():
    spec = importlib.util.spec_from_file_location('sa_hermes_host', Path(__file__).with_name('hermes_host.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


host = load_adapter()


class HostContractTests(unittest.TestCase):
    def test_two_fixed_tool_schemas(self):
        first, second = host.tool_schemas()
        self.assertEqual((first['name'], second['name']), ('knowledge_search', 'knowledge_get'))
        for schema, keys in ((first, {'q', 'domains', 'limit'}), (second, {'ref', 'domains'})):
            params = schema['parameters']
            self.assertEqual(set(params['required']), keys)
            self.assertFalse(params['additionalProperties'])
            self.assertTrue(params['properties']['domains']['uniqueItems'])

    def test_handler_passes_bounded_requests_to_broker_for_denial(self):
        bad_scope = {'q': 'private fixture', 'domains': ['unapproved'], 'limit': 2}
        client = Mock(return_value={'ok': False, 'error': {'code': 'REFUSED', 'retryable': False}})
        invoke = host._tool_handler('knowledge_search', client)
        self.assertEqual(json.loads(invoke(bad_scope))['error']['code'], 'REFUSED')
        client.assert_called_once_with('knowledge_search', bad_scope)
        self.assertEqual(json.loads(invoke({'q': 'oversized' * 1000}))['error']['code'], 'INVALID_REQUEST')
        client.assert_called_once()
        client.side_effect = RuntimeError('must not serialize traceback')
        self.assertEqual(json.loads(invoke(bad_scope))['error']['code'], 'TOOL_CHANNEL_UNAVAILABLE')

    def test_incomplete_and_failed_text_is_not_promoted(self):
        for result in (dict(completed=False, final_response='partial'), dict(completed=True, failed=True),
                       dict(completed=True, final_response='partial', interrupted=True),
                       dict(completed=True, final_response='partial', partial=True)):
            self.assertEqual(host._outcome(result)['status'], 'failed')
            self.assertIsNone(host._outcome(result)['text'])
        success = dict(completed=True, final_response='synthetic result', api_calls=2,
                       messages=['must not surface'])
        self.assertEqual(host._outcome(success), dict(status='succeeded', text='synthetic result', reported_api_calls=2))


def _installed_pin():
    try:
        return sys.platform == 'linux' and importlib.metadata.version('hermes-agent') == '0.16.0'
    except importlib.metadata.PackageNotFoundError:
        return False


if sys.platform == 'linux':
    class _FakeApi(socketserver.UnixStreamServer):
        def __init__(self, path, responses):
            self.responses = iter(responses)
            self.requests = []
            super().__init__(path, _FakeHandler)


class _FakeHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.server.requests.append((self.path, payload))
        try:
            status, message = next(self.server.responses)
        except StopIteration:
            status, message = 400, {'error': {'message': 'unexpected request', 'type': 'fixture'}}
        if status == 200:
            finish = 'tool_calls' if 'tool_calls' in message else 'stop'
            answer = {'id': 'fixture-cmpl', 'object': 'chat.completion', 'created': 1, 'model': payload['model'],
                      'choices': [{'index': 0, 'message': message, 'finish_reason': finish}],
                      'usage': {'prompt_tokens': 10, 'completion_tokens': 3, 'total_tokens': 13}}
        else:
            answer = message
        if status == 200 and payload.get('stream'):
            delta = dict(message)
            if 'tool_calls' in delta:
                delta['tool_calls'] = [dict(call, index=index) for index, call in enumerate(delta['tool_calls'])]
            chunk = {key: answer[key] for key in ('id', 'created', 'model')}
            chunk['object'] = 'chat.completion.chunk'
            chunks = [
                chunk | {'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]},
                chunk | {'choices': [{'index': 0, 'delta': {}, 'finish_reason': finish}], 'usage': answer['usage']},
            ]
            raw = ('\n\n'.join('data: ' + json.dumps(item) for item in chunks) + '\n\ndata: [DONE]\n\n').encode()
            content_type = 'text/event-stream'
        else:
            raw = json.dumps(answer).encode()
            content_type = 'application/json'
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(raw)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(raw)


@unittest.skipUnless(_installed_pin(), 'requires pinned Linux Hermes environment')
class PinnedHermesSmokeTests(unittest.TestCase):
    def run_case(self, responses, *, tools_enabled=True):
        with TemporaryDirectory(prefix='sa-hm-') as directory:
            socket_path = str(Path(directory) / 'broker.sock')
            with _FakeApi(socket_path, responses) as fake:
                server = Thread(target=fake.serve_forever, daemon=True)
                server.start()
                try:
                    child = subprocess.run([sys.executable, '-I', '-B', str(Path(__file__).resolve()),
                                            '--smoke-child', socket_path, directory,
                                            'tools' if tools_enabled else 'no-tools'], capture_output=True, timeout=35,
                                           env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'HOME': directory})
                    self.assertEqual(child.returncode, 0, child.stdout.decode('utf-8', errors='replace'))
                    return json.loads(child.stdout), fake.requests
                finally:
                    fake.shutdown()
                    server.join(timeout=2)

    def test_real_pinned_client_uses_fresh_uds_connections_for_two_turns(self):
        lookup = {'role': 'assistant', 'content': None, 'tool_calls': [{
            'id': 'fixture-call-1', 'type': 'function', 'function': {
                'name': 'knowledge_search', 'arguments': json.dumps({
                    'q': 'fixture', 'domains': ['research-content'], 'limit': 1})}}]}
        report, requests = self.run_case([
            (200, lookup), (200, {'role': 'assistant', 'content': 'synthetic summary from fixture evidence'})])
        self.assertEqual(report['outcome']['status'], 'succeeded', report)
        self.assertEqual(len(requests), 2)
        self.assertEqual([route for route, _ in requests], [host.BROKER_PATH, host.BROKER_PATH])
        self.assertTrue(all(not request.get('stream') and request.get('max_tokens') == 512
                            for _, request in requests))
        self.assertEqual({entry['function']['name'] for entry in requests[0][1]['tools']},
                         {'knowledge_search', 'knowledge_get'})
        self.assertEqual(report['tool_calls'], [['knowledge_search',
                                                {'q': 'fixture', 'domains': ['research-content'], 'limit': 1}]])
        self.assertTrue(any(message['role'] == 'tool' and 'fixture-evidence' in message['content']
                            for message in requests[1][1]['messages']))

    def test_definite_rate_limit_does_not_resend_and_no_tools_are_offered(self):
        report, requests = self.run_case([(429, {'error': {'message': 'fixture limit', 'type': 'fixture'}})],
                                         tools_enabled=False)
        self.assertEqual(report['outcome']['status'], 'failed', report)
        self.assertEqual(len(requests), 1)
        self.assertFalse(requests[0][1].get('tools'))
        self.assertEqual(report['tool_calls'], [])


def _child(path, directory, mode):
    calls = []

    def tool(name, args):
        calls.append([name, args])
        return {'ok': True, 'results': [{'snippet': 'fixture-evidence'}]}

    try:
        result = host.run('Compare only fixture evidence.', task_id='fixture-smoke', model='deepseek-v4-flash',
                          provider_socket=path, tool_client=tool, tools_enabled=mode == 'tools', work_dir=directory)
    except host.HermesHostError as error:
        print(json.dumps({'code': str(error)}))
        return 1
    print(json.dumps(dict(outcome=result, tool_calls=calls)))
    return 0


if __name__ == '__main__':
    if len(sys.argv) == 5 and sys.argv[1] == '--smoke-child':
        raise SystemExit(_child(*sys.argv[2:]))
    unittest.main()
