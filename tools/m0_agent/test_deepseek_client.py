"""Simulated DeepSeek tests. All responses and credentials are synthetic."""

from io import BytesIO
import json
import os
import ssl
import unittest
from unittest.mock import MagicMock, Mock, patch

from tools.m0_agent import deepseek_client as client


SIMULATED_KEY = "simulated-key"


def simulated_completion():
    return {
        "id": "simulated-response", "model": "simulated-returned-model",
        "choices": [{"finish_reason": "stop",
                     "message": {"role": "assistant", "content": "simulated answer"}}],
        "usage": {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17,
                  "prompt_cache_hit_tokens": 0, "prompt_cache_miss_tokens": 12},
    }


class DeepSeekClientTest(unittest.TestCase):
    def complete(self, transport, **overrides):
        request = {"model": "simulated-requested-model", "approved_text": "批准的示例",
                   "api_key": SIMULATED_KEY, "max_tokens": 50}
        return client.complete_text(**(request | overrides), _transport=transport)

    def assert_failure(self, kind, unknown, transport, **overrides):
        with self.assertRaises(client.DeepSeekError) as raised:
            self.complete(transport, **overrides)
        error = raised.exception
        self.assertEqual(error.kind, kind)
        self.assertIs(error.outcome_unknown, unknown)
        self.assertNotIn(SIMULATED_KEY, repr(error))
        self.assertIsNone(error.__context__)
        return error

    def test_simulated_success_and_request_shape(self):
        transport = Mock(return_value=(200, json.dumps(simulated_completion()).encode()))
        result = self.complete(transport)
        transport.assert_called_once()
        key, body = transport.call_args.args
        self.assertTrue(key == SIMULATED_KEY)
        self.assertEqual(body, ('{"model":"simulated-requested-model",'
                                '"messages":[{"role":"user","content":"批准的示例"}],'
                                '"max_tokens":50,"stream":false}').encode())
        self.assertEqual(json.loads(body), {
            "model": "simulated-requested-model",
            "messages": [{"role": "user", "content": "批准的示例"}],
            "max_tokens": 50, "stream": False,
        })
        self.assertEqual(result.response_id, "simulated-response")
        self.assertEqual(result.model, "simulated-returned-model")
        self.assertEqual(result.text, "simulated answer")
        self.assertEqual(result.finish_reason, "stop")
        self.assertFalse(result.truncated)
        self.assertEqual(result.usage, client.TokenUsage(12, 5, 17, 0, 12))
        self.assertNotIn(result.text, repr(result))

    def test_explicit_thinking_modes_serialize_as_objects(self):
        transport = Mock(return_value=(200, json.dumps(simulated_completion()).encode()))
        for mode in ('enabled', 'disabled'):
            with self.subTest(mode=mode):
                self.complete(transport, thinking=mode)
                self.assertEqual(json.loads(transport.call_args.args[1])['thinking'], {'type': mode})

    def test_direct_transport_only_uses_fixed_host_and_does_not_follow_redirect(self):
        connection = MagicMock()
        response = connection.getresponse.return_value.__enter__.return_value
        response.status = 307
        response.getheader.return_value = "https://wrong.example/"
        with patch.object(client, "HTTPSConnection", return_value=connection) as https:
            with patch.dict(os.environ, {"HTTPS_PROXY": "http://proxy.example/"}):
                error = self.assert_failure("redirect_rejected", True, client._direct_https)
        self.assertEqual(error.status, 307)
        https.assert_called_once()
        self.assertEqual(https.call_args.args, ("api.deepseek.com",))
        self.assertEqual(https.call_args.kwargs["timeout"], client.SOCKET_TIMEOUT_SECONDS)
        context = https.call_args.kwargs["context"]
        self.assertTrue(context.check_hostname)
        self.assertIsNone(context.keylog_filename)
        connection.request.assert_called_once()
        self.assertEqual(connection.request.call_args.args, ("POST", "/chat/completions"))
        self.assertTrue(connection.request.call_args.kwargs["headers"]["Authorization"]
                        == f"Bearer {SIMULATED_KEY}")
        connection.set_tunnel.assert_not_called()
        response.read.assert_not_called()
        response.getheader.assert_not_called()
        connection.close.assert_called_once()

    def test_connect_proxy_keeps_provider_sni_and_bearer_inside_tls(self):
        plain, tls = MagicMock(), MagicMock()
        plain.makefile.return_value = BytesIO(b'HTTP/1.1 200 Connection established\r\n\r\n')
        tls.makefile.return_value = BytesIO(b'HTTP/1.1 307 Temporary Redirect\r\nContent-Length: 0\r\n\r\n')
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        verified_context = Mock(wraps=context)
        verified_context.wrap_socket.return_value = tls
        proxy = {'host': '10.0.2.2', 'port': 7897}
        with (patch('http.client.socket.create_connection', return_value=plain) as connect,
              patch.object(client.ssl, 'SSLContext', return_value=verified_context),
              patch.dict(os.environ, {'HTTPS_PROXY': 'http://ignored.example/'})):
            error = self.assert_failure('redirect_rejected', True, None, https_connect_proxy=proxy)
        self.assertEqual(error.status, 307)
        connect.assert_called_once()
        self.assertEqual(connect.call_args.args[0], ('10.0.2.2', 7897))
        verified_context.wrap_socket.assert_called_once_with(plain, server_hostname='api.deepseek.com')
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertIsNone(context.keylog_filename)
        connect_bytes = b''.join(call.args[0] for call in plain.sendall.call_args_list)
        https_bytes = b''.join(call.args[0] for call in tls.sendall.call_args_list)
        self.assertTrue(connect_bytes.startswith(b'CONNECT api.deepseek.com:443 HTTP/'))
        self.assertNotIn(b'Authorization', connect_bytes)
        self.assertNotIn(SIMULATED_KEY.encode(), connect_bytes)
        self.assertTrue(https_bytes.startswith(b'POST /chat/completions HTTP/'))
        self.assertIn(b'Authorization: Bearer ' + SIMULATED_KEY.encode(), https_bytes)
        self.assertIn(b'Host: api.deepseek.com', https_bytes)

    def test_definite_invalid_input_never_calls_transport(self):
        transport = Mock()
        for overrides in ({"model": ""}, {"api_key": "simulated-key\r\nX: y"},
                          {"approved_text": "你" * client.MAX_REQUEST_BYTES},
                          {"approved_text": "\ud800"}, {"max_tokens": True},
                          {"max_tokens": client.MAX_OUTPUT_TOKENS + 1}, {"thinking": "auto"},
                          {"thinking": {"type": "disabled"}},
                          {'https_connect_proxy': {'host': 'wrong.example', 'port': 7897}},
                          {'https_connect_proxy': {'host': '10.0.2.2', 'port': 7898}},
                          {'https_connect_proxy': {'host': '10.0.2.2', 'port': 7897, 'authorization': 'x'}}):
            with self.subTest(field=next(iter(overrides))):
                self.assert_failure("invalid_request", False, transport, **overrides)
        transport.assert_not_called()

    def test_http_statuses_do_not_expose_response_details(self):
        for status, kind, unknown in ((401, "auth_rejected", False), (403, "auth_rejected", False),
                                      (429, "rate_limited", False), (400, "provider_error", False),
                                      (500, "provider_error", True)):
            with self.subTest(status=status):
                transport = Mock(return_value=(status, SIMULATED_KEY.encode()))
                error = self.assert_failure(kind, unknown, transport)
                self.assertEqual(error.status, status)
                transport.assert_called_once()

    def test_connection_failures_are_ambiguous_and_never_retried(self):
        for failure, kind in ((TimeoutError(SIMULATED_KEY), "timeout"),
                              (ConnectionResetError(SIMULATED_KEY), "connection_error")):
            with self.subTest(kind=kind):
                transport = Mock(side_effect=failure)
                self.assert_failure(kind, True, transport)
                transport.assert_called_once()

    def test_response_size_and_partial_body(self):
        connection = MagicMock()
        response = connection.getresponse.return_value.__enter__.return_value
        response.status = 200
        with patch.object(client, "HTTPSConnection", return_value=connection):
            response.length = client.MAX_RESPONSE_BYTES + 1
            self.assert_failure("response_too_large", True, client._direct_https)
            response.read.assert_not_called()
            response.length = None
            response.read.return_value = b"x" * (client.MAX_RESPONSE_BYTES + 1)
            self.assert_failure("response_too_large", True, client._direct_https)
            response.read.assert_called_once_with(client.MAX_RESPONSE_BYTES + 1)
            response.read.return_value = json.dumps(simulated_completion()).encode()
            response.length = len(response.read.return_value) + 1
            self.assert_failure("incomplete_response", True, client._direct_https)

    def test_malformed_or_incomplete_json_is_ambiguous(self):
        for payload in (b"{", b"\xff", b"{}", b'{"choices": []}',
                        json.dumps(simulated_completion()).encode()[:-1]):
            self.assert_failure("invalid_response", True, Mock(return_value=(200, payload)))
        result = simulated_completion()
        result["choices"][0]["finish_reason"] = None
        self.assert_failure("invalid_response", True, Mock(return_value=(200, json.dumps(result).encode())))

    def test_length_keeps_usage_and_marks_result_truncated(self):
        result = simulated_completion()
        result["choices"][0]["finish_reason"] = "length"
        completion = self.complete(Mock(return_value=(200, json.dumps(result).encode())))
        self.assertTrue(completion.truncated)
        self.assertEqual(completion.text, "simulated answer")
        self.assertEqual(completion.usage.total_tokens, 17)

    def test_unexpected_finish_reasons_are_invalid(self):
        for finish in ("aborted", "unknown", "tool_calls", "content_filter"):
            with self.subTest(finish=finish):
                result = simulated_completion()
                result["choices"][0]["finish_reason"] = finish
                self.assert_failure("invalid_response", True,
                                    Mock(return_value=(200, json.dumps(result).encode())))

    def test_requires_assistant_text_without_tool_calls(self):
        for fields in ({"role": "user"}, {"tool_calls": [{"id": "simulated-tool-call"}]}):
            with self.subTest(fields=fields):
                result = simulated_completion()
                result["choices"][0]["message"].update(fields)
                self.assert_failure("invalid_response", True,
                                    Mock(return_value=(200, json.dumps(result).encode())))

    def test_usage_rejects_invalid_counts_and_preserves_absent_cache_counts(self):
        result = simulated_completion()
        for count in (True, None, -1, "10", 1.0):
            with self.subTest(count=count):
                result["usage"]["prompt_tokens"] = count
                self.assert_failure("invalid_response", True,
                                    Mock(return_value=(200, json.dumps(result).encode())))
        result["usage"] = {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17}
        completion = self.complete(Mock(return_value=(200, json.dumps(result).encode())))
        self.assertEqual(completion.usage, client.TokenUsage(12, 5, 17, None, None))

    def test_rejects_inconsistent_usage_totals(self):
        for field, value in (("total_tokens", 18), ("prompt_cache_miss_tokens", 11)):
            with self.subTest(field=field):
                result = simulated_completion()
                result["usage"][field] = value
                self.assert_failure("invalid_response", True,
                                    Mock(return_value=(200, json.dumps(result).encode())))
