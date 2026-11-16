"""Exercise the comparison's Unix HTTP sockets without provider access."""

import http.client
import importlib.util
import json
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import Mock


def _load_service():
    spec = importlib.util.spec_from_file_location('sa_compare_socket_service', Path(__file__).with_name('socket_service.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


service = _load_service()


@unittest.skipUnless(os.name == 'posix' and hasattr(socket, 'AF_UNIX'), 'requires Unix domain sockets')
class SocketServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory(prefix='sa-sockets-'))).resolve()
        self.run = Mock()
        self.run.complete.return_value = {'state': 'settled', 'response': {'id': 'fake-response'}}
        self.run.tool.return_value = {'ok': True, 'results': []}
        self.enterContext(service.RunSockets(self.directory, self.run))

    def post(self, channel, body, *, path=None, headers=None):
        if isinstance(body, dict):
            body = service.encoded(body)
        path = path or ('/v1/chat/completions' if channel == 'provider' else '/tools')
        headers = [('Content-Length', str(len(body)))] if headers is None else headers
        request = f'POST {path} HTTP/1.1\r\nHost: localhost\r\n'.encode('ascii')
        request += ''.join(f'{name}: {value}\r\n' for name, value in headers).encode('ascii')
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(3)
            client.connect(str(self.directory / f'{channel}.sock'))
            client.sendall(request + b'\r\n' + body)
            client.shutdown(socket.SHUT_WR)
            with http.client.HTTPResponse(client) as response:
                response.begin()
                raw = response.read(service.MAX_BODY + 1)
                self.assertEqual(response.getheader('Content-Type'), 'application/json')
                self.assertEqual(response.getheader('Connection'), 'close')
                self.assertEqual(int(response.getheader('Content-Length')), len(raw))
                return response.status, json.loads(raw)

    def test_duplicate_payload_reuses_sequence_on_provider_socket(self):
        self.run.complete.side_effect = [
            {'state': 'settled', 'response': {'id': 'first'}},
            {'state': 'cached', 'response': {'id': 'first'}},
            {'state': 'settled', 'response': {'id': 'second'}},
        ]
        data = {'model': 'fixture', 'messages': [{'role': 'user', 'content': 'example'}]}
        self.assertEqual(self.post('provider', data), (200, {'id': 'first'}))
        equivalent = b'{ "messages" : [ { "content": "example", "role": "user" } ], "model": "fixture" }'
        self.assertEqual(self.post('provider', equivalent), (200, {'id': 'first'}))
        changed = data | {'max_tokens': 10}
        self.assertEqual(self.post('provider', changed), (200, {'id': 'second'}))
        self.assertEqual([item.args for item in self.run.complete.call_args_list], [(0, data), (0, data), (1, changed)])

    def test_malformed_duplicate_oversized_and_ambiguous_framing_never_reaches_broker(self):
        for channel in ('provider', 'tools'):
            for raw, headers in (
                (b'{', None),
                (b'[]', None),
                (b'{"outer":{"a":1,"a":2}}', None),
                (b'{"value":NaN}', None),
                (b'{}', [('Content-Length', str(service.MAX_BODY + 1))]),
                (b'{}', [('Content-Length', '3')]),
                (b'{}', [('Content-Length', '2'), ('Content-Length', '2')]),
                (b'{}', [('Content-Length', '2'), ('Transfer-Encoding', 'chunked')]),
                (b'{}', []),
            ):
                with self.subTest(channel=channel, raw=raw, headers=headers):
                    status, result = self.post(channel, raw, headers=headers)
                    self.assertEqual(status, 400)
                    self.assertEqual(result['error']['message'], 'comparison_request_refused')
        self.run.complete.assert_not_called()
        self.run.tool.assert_not_called()
        self.assertEqual(self.post('provider', {'model': 'fixture'})[0], 200)
        self.run.complete.assert_called_once_with(0, {'model': 'fixture'})

    def test_truncated_and_unknown_provider_envelopes_return_409_without_body_leak(self):
        for index, result in enumerate((
            {'state': 'settled', 'response': None, 'code': 'OUTPUT_TRUNCATED'},
            {'state': 'cached', 'response': None, 'code': 'OUTPUT_TRUNCATED'},
            {'state': 'unknown', 'response': None, 'code': 'private transport detail'},
        )):
            self.run.complete.return_value = result
            self.assertEqual(self.post('provider', {'fixture': index}),
                             (409, {'error': {'message': 'comparison_gate_closed', 'type': 'comparison'}}))
        self.assertEqual([item.args[0] for item in self.run.complete.call_args_list], [0, 1, 2])

    def test_tool_socket_passes_explicit_sequence_and_routes_are_disjoint(self):
        args = {'domains': ['research-content'], 'q': 'fixture', 'limit': 1}
        payload = {'sequence': 7, 'name': 'knowledge_search', 'arguments': args}
        self.assertEqual(self.post('tools', payload), (200, {'ok': True, 'results': []}))
        self.assertEqual(self.post('tools', payload), (200, {'ok': True, 'results': []}))
        self.assertEqual([item.args for item in self.run.tool.call_args_list], [(7, 'knowledge_search', args)] * 2)
        self.assertEqual(self.post('tools', payload | {'extra': True})[0], 400)
        self.assertEqual(self.post('provider', payload, path='/tools')[0], 400)
        self.assertEqual(self.post('tools', payload, path='/v1/chat/completions')[0], 400)
        self.run.complete.assert_not_called()


if __name__ == '__main__':
    unittest.main()
