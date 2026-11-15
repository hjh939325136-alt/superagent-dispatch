"""Black-box CLI checks using an isolated synthetic handoff index."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
STATUS = {'op': 'status', 'role': 'manager', 'args': {}}


class QueryProtocolTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix='manager-protocol-')
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.binding = self.root / 'binding.json'
        self.missing_binding = self.root / 'missing-binding.json'

    def invoke(self, request, binding, exit_code):
        raw = request if isinstance(request, bytes) else json.dumps(request).encode('utf-8')
        child = subprocess.run(
            [sys.executable, '-B', '-m', 'tools.m0_manager', '--binding', str(binding)],
            cwd=ROOT, input=raw, capture_output=True, timeout=15,
        )
        self.assertEqual(child.stderr, b'')
        self.assertEqual(child.returncode, exit_code, child.stdout)
        return json.loads(child.stdout)

    def assert_rejected(self, request, code, *, binding=None):
        result = self.invoke(request, binding or self.missing_binding, 2)
        self.assertEqual(result, {'ok': False, 'error': code, 'schema_version': '0.1'})

    def test_legacy_and_explicit_version_have_same_success_response(self):
        source_path = self.root / 'handoffs.json'
        source_path.write_text(json.dumps({
            'schemaVersion': 1, 'updatedAt': '2000-01-01T00:00:00Z', 'items': [],
        }), encoding='utf-8')
        source = {
            'source_id': 'fixture-handoffs', 'source_instance': 'fixture-index', 'kind': 'handoff',
            'class': 'local_only', 'locator': str(source_path), 'accepted_schema_versions': ['1'],
            'freshness_s': 3600, 'options': {'format': 'superagent.handoffs.v1'},
        }
        self.binding.write_text(json.dumps({'version': 1, 'sources': [source]}), encoding='utf-8')
        responses = [self.invoke(request, self.binding, 0) for request in (STATUS, STATUS | {'schema_version': '0.1'})]
        for response in responses:
            self.assertLessEqual({
                'ok', 'schema_version', 'op', 'role', 'role_template', 'binding', 'sources',
                'items', 'withheld', 'authorization_basis', 'truncated', 'coverage',
            }, response.keys())
            self.assertIs(response['ok'], True)
            self.assertEqual(response['schema_version'], '0.1')
            self.assertEqual(response['op'], 'status')
            self.assertEqual(response['role'], 'manager')
            self.assertRegex(response['binding'], r'^manager/binding@1#sha256:[a-f0-9]{64}$')
            self.assertRegex(response['role_template'], r'^manager/role/manager@1#sha256:[a-f0-9]{64}$')
            self.assertEqual(response['authorization_basis'], 'trusted_local_caller_role_declaration')
            self.assertEqual(response['withheld'], {'by_role': 0, 'by_class': 0, 'by_policy': 0})
            self.assertEqual(response['items'], [])
            self.assertIs(response['truncated'], False)
            self.assertEqual(response['coverage'], 'partial')
            descriptor, = response['sources']
            self.assertEqual(descriptor['source_id'], 'fixture-handoffs')
            self.assertEqual(descriptor['schema_version'], '1')
            self.assertEqual(descriptor['coverage'], 'partial')
            self.assertIsInstance(descriptor.pop('observed_at'), str)
        self.assertEqual(*responses)

    def test_invalid_versions_fail_before_binding(self):
        for version in ('9.9', '', None, [], 0.1, True, {}):
            with self.subTest(version=version):
                self.assert_rejected(STATUS | {'schema_version': version}, 'E_PROTOCOL_UNSUPPORTED')

    def test_duplicate_json_keys_are_rejected(self):
        for raw in (
            b'{"op":"status","role":"manager","args":{},"schema_version":"0.1","schema_version":"0.1"}',
            b'{"op":"status","role":"manager","args":{"limit":1,"limit":2}}',
        ):
            with self.subTest(raw=raw):
                self.assert_rejected(raw, 'E_REQUEST_INVALID')

    def test_malformed_json_is_rejected(self):
        self.assert_rejected(b'{', 'E_REQUEST_INVALID')

    def test_deep_array_is_rejected(self):
        # JSON decoder depth limits vary across Python versions. Decoders that
        # accept the array reach the query envelope's object check.
        result = self.invoke(b'[' * 5000 + b']' * 5000, self.missing_binding, 2)
        self.assertIs(result['ok'], False)
        self.assertEqual(result['schema_version'], '0.1')
        self.assertIn(result['error'], {'E_REQUEST_INVALID', 'E_FIELD_NOT_ALLOWED'})

    def test_oversized_input_is_rejected(self):
        raw = json.dumps(STATUS).encode('utf-8').ljust(65537)
        self.assert_rejected(raw, 'E_REQUEST_TOO_LARGE')

    def test_outside_fields_are_rejected(self):
        versioned = STATUS | {'schema_version': '0.1'}
        self.assert_rejected(versioned | {'binding': 'injected'}, 'E_FIELD_NOT_ALLOWED')
        self.assert_rejected(versioned | {'args': {'locator': 'injected'}}, 'E_FIELD_NOT_ALLOWED')

    def test_top_level_must_be_an_object(self):
        for request in (None, []):
            with self.subTest(request=request):
                self.assert_rejected(request, 'E_FIELD_NOT_ALLOWED')

    def test_binding_error_uses_versioned_envelope(self):
        self.assert_rejected(STATUS | {'schema_version': '0.1'}, 'E_REQUEST_INVALID')

    def test_restricted_roles_still_cannot_query_knowledge(self):
        for role in ('technical', 'progress'):
            with self.subTest(role=role):
                self.assert_rejected(
                    {'schema_version': '0.1', 'op': 'knowledge.search', 'role': role, 'args': {'q': 'fixture'}},
                    'E_ROLE_NOT_ALLOWED',
                )
