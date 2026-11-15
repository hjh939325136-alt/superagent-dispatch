"""Trusted local configuration for persistent synthetic dispatch reads.

This fixture binding is operator configuration, never an authority supplied by a
task. It is not an OS sandbox against an operator who can edit the fixture files.
"""
import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools.m0_backup.domain_binding import (ENVELOPE_SCHEMA, HASH, IDENTITY, RecoveryAdmission,
    check_recovery_binding_current, historical_path, load_recovery_binding, require_recovery_operation)

from catalog import APPLICATION_ID, Catalog, CatalogError, MARKER, SOURCES, identity, parse_ref
from fixture_store import canonical_json
from search import SearchError, authorize, get, search

SCHEMA = 'm0.synthetic_dispatch_binding.v1'
AUTHORITY = 'fixture://m0-research/dispatch-three-source-v1'
GRANT = 'fixture-three-source'
BINDING_FIELD = 'source_binding'
FIELDS = {'schema', 'synthetic_only', 'authorization_source', 'catalog_root',
          'catalog_instance', 'catalog_marker_sha256', 'grant_id'}
OPERATIONS = {'research.search': 'q', 'research.get': 'ref',
              'research.context_get': 'context_id', 'research.context_reassemble': 'context_id'}


def fail(code, message):
    raise SearchError(code, message)


def safe_path(value, *, directory=False):
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts:
        fail('INVALID_BINDING', 'Binding paths must be explicit absolute paths')
    for part in reversed((path, *path.parents)):
        info = os.lstat(part)
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            fail('INVALID_BINDING', 'Binding paths cannot contain links or reparse points')
    if directory:
        if not stat.S_ISDIR(info.st_mode):
            fail('INVALID_BINDING', 'Catalog must be a directory')
    elif not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        fail('INVALID_BINDING', 'Binding input must be an unshared regular file')
    return path, info


def read_file(path, *, limit=16384, prefix=False):
    path, before = safe_path(path)
    if not prefix and before.st_size > limit:
        fail('INVALID_BINDING', 'Binding metadata is too large')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            fail('INVALID_BINDING', 'Binding file identity changed')
        data = stream.read(limit if prefix else limit + 1)
    if not prefix and len(data) > limit:
        fail('INVALID_BINDING', 'Binding metadata changed or is too large')
    return data


def parse_json(data):
    def unique(pairs):
        result = dict(pairs)
        if len(result) != len(pairs):
            fail('INVALID_BINDING', 'Duplicate binding fields are not allowed')
        return result
    return json.loads(data, object_pairs_hook=unique)


@dataclass(frozen=True)
class Binding:
    path: Path
    catalog_root: Path
    instance: str
    marker_hash: str
    digest: str
    grant: str = GRANT
    recovery_digest: str | None = None
    recovery_admission: RecoveryAdmission | None = None

    def catalog(self):
        # Authorization must be checked by the caller before this source preflight.
        check_recovery_binding_current(self, 'research')
        safe_path(self.catalog_root, directory=True)
        raw = read_file(self.catalog_root / 'catalog.json')
        marker = parse_json(raw)
        if (not isinstance(marker, dict) or marker.get('schema') != MARKER or
                marker.get('synthetic_only') is not True or marker.get('sources') != list(SOURCES) or
                marker.get('instance') != self.instance or
                hashlib.sha256(raw).hexdigest() != self.marker_hash):
            fail('NOT_SYNTHETIC_CATALOG', 'Catalog fixture marker no longer matches the local binding')
        # Inspect only the fixed header before opening any SQLite connection. A
        # substituted unmarked database cannot be queried as a fixture source.
        for name in ('catalog', *SOURCES):
            header = read_file(self.catalog_root / (name + '.sqlite3'), limit=100, prefix=True)
            if (len(header) != 100 or header[:16] != b'SQLite format 3\x00' or
                    int.from_bytes(header[68:72], 'big') != APPLICATION_ID):
                fail('NOT_SYNTHETIC_CATALOG', 'Database is not marked as this synthetic fixture type')
        selected = Catalog(self.catalog_root)
        if selected.instance != self.instance:
            fail('NOT_SYNTHETIC_CATALOG', 'Catalog instance changed')
        return selected


def load_binding(path):
    path = Path(path)
    data = parse_json(read_file(path))
    recovered = None
    if isinstance(data, dict) and data.get('schema') == ENVELOPE_SCHEMA:
        recovered = load_recovery_binding(path, 'research')
        data = recovered.original
    if (not isinstance(data, dict) or set(data) != FIELDS or data['schema'] != SCHEMA or
            data['synthetic_only'] is not True or data['authorization_source'] != AUTHORITY or
            data['grant_id'] != GRANT or not isinstance(data['catalog_instance'], str) or
            not isinstance(data['catalog_marker_sha256'], str) or len(data['catalog_marker_sha256']) != 64):
        fail('INVALID_BINDING', 'Only the explicit three-source synthetic fixture binding is supported')
    digest = 'sha256:' + hashlib.sha256(canonical_json(data).encode()).hexdigest()
    if recovered is not None:
        if (not historical_path(data['catalog_root']) or not IDENTITY.fullmatch(data['catalog_instance']) or
                not HASH.fullmatch(data['catalog_marker_sha256'])):
            fail('INVALID_BINDING', 'Invalid original research binding')
        return Binding(path, recovered.role_paths['catalog_root'], data['catalog_instance'],
                       data['catalog_marker_sha256'], digest, recovery_digest=recovered.recovery_digest,
                       recovery_admission=recovered.admission)
    root = Path(data['catalog_root'])
    if not root.is_absolute() or '..' in root.parts:
        fail('INVALID_BINDING', 'Catalog binding requires an absolute local path')
    # Deliberately do not stat/open the catalog before task authorization.
    return Binding(path, root, data['catalog_instance'], data['catalog_marker_sha256'], digest)


def initialize_binding(catalog_root, output):
    root, _ = safe_path(catalog_root, directory=True)
    marker_data = read_file(root / 'catalog.json')
    marker = parse_json(marker_data)
    if not isinstance(marker, dict) or marker.get('synthetic_only') is not True or marker.get('schema') != MARKER:
        fail('NOT_SYNTHETIC_CATALOG', 'An initialized synthetic catalog is required')
    data = {'schema': SCHEMA, 'synthetic_only': True, 'authorization_source': AUTHORITY,
            'catalog_root': str(root), 'catalog_instance': marker.get('instance'),
            'catalog_marker_sha256': hashlib.sha256(marker_data).hexdigest(), 'grant_id': GRANT}
    prospective = Binding(Path(output), root, data['catalog_instance'], data['catalog_marker_sha256'], '')
    prospective.catalog().load(SOURCES)
    output = Path(output)
    if not output.is_absolute() or '..' in output.parts:
        fail('INVALID_BINDING', 'Binding output must be absolute')
    safe_path(output.parent, directory=True)
    with output.open('xb') as stream:
        stream.write((canonical_json(data) + '\n').encode())
    binding = load_binding(output)
    return {'binding': str(output), 'source_binding': binding.digest, 'catalog_instance': binding.instance,
            'grant_id': binding.grant, 'synthetic_only': True}


def authorize_bound(params, binding, operation, *, submitted=False):
    if operation not in OPERATIONS:
        fail('INVALID_REQUEST', 'Unsupported read operation')
    require_recovery_operation(binding, operation)
    if not isinstance(params, dict):
        fail('INVALID_REQUEST', 'Request must be an object')
    clean = dict(params)
    existing = clean.pop(BINDING_FIELD, None)
    if existing != binding.digest and (not submitted or existing is not None):
        fail('BINDING_CHANGED', 'Task must keep the source binding recorded at submission')
    authorize(clean, binding.grant, OPERATIONS[operation])
    if operation == 'research.search':
        q, limit = clean['q'], clean.get('limit', 10)
        if not isinstance(q, str) or not q.strip() or len(q) > 200 or type(limit) is not int or not 1 <= limit <= 100:
            fail('INVALID_REQUEST', 'Query or result limit outside allowed bounds')
    elif operation == 'research.get':
        domain, _, _, _ = parse_ref(clean['ref'])
        if domain not in clean['domains']:
            fail('NOT_FOUND', 'Reference domain is not selected')
    else:
        identity(clean['context_id'])
    return clean


def execute_bound(operation, params, binding):
    clean = authorize_bound(params, binding, operation)
    selected = binding.catalog()
    if operation == 'research.search':
        result = search(clean, binding.grant, catalog=selected)
    elif operation == 'research.get':
        result = get(clean, binding.grant, catalog=selected)
    else:
        result = selected.context_get(clean, binding.grant, reassemble=operation == 'research.context_reassemble')
    if binding.recovery_admission is not None:
        result.update(binding.recovery_admission.provenance())
    return result


def receipt_projection(data, binding):
    """Durable task receipts contain references and integrity facts, not content."""
    def item(value):
        result = {key: value[key] for key in ('ref', 'status', 'class', 'observed_at', 'expires_at',
                  'human_reviewed', 'human_locked') if key in value}
        result['readback_sha256'] = hashlib.sha256(canonical_json(value).encode()).hexdigest()
        return result
    result = {key: value for key, value in data.items() if key not in ('results', 'result', 'items')}
    for name in ('results', 'items'):
        if name in data:
            result[name] = [item(value) for value in data[name]]
    if 'result' in data:
        result['result'] = item(data['result'])
    result.update(source_binding=binding.digest, catalog_instance=binding.instance,
                  receipt_content='references_only', synthetic_only=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    init = sub.add_parser('fixture.bind')
    init.add_argument('--catalog', type=Path, required=True)
    init.add_argument('--output', type=Path, required=True)
    live = sub.add_parser('read')
    live.add_argument('--binding', type=Path, required=True)
    live.add_argument('--operation', choices=(*OPERATIONS, 'context.assemble'), required=True)
    live.add_argument('--request', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.action == 'fixture.bind':
            result = initialize_binding(args.catalog, args.output)
        else:
            binding = load_binding(args.binding)
            params = parse_json(read_file(args.request, limit=128 * 1024))
            if args.operation == 'context.assemble':
                require_recovery_operation(binding, args.operation)
                authorize(params, binding.grant, 'refs')
                result = binding.catalog().assemble(params, binding.grant)
            else:
                clean = authorize_bound(params, binding, args.operation, submitted=True)
                result = execute_bound(args.operation, clean | {BINDING_FIELD: binding.digest}, binding)
        print(json.dumps({'ok': True, 'data': result}, ensure_ascii=True, sort_keys=True))
        return 0
    except (SearchError, CatalogError, ValueError, OSError, sqlite3.Error) as error:
        print(json.dumps({'ok': False, 'error': {'code': getattr(error, 'code', 'INVALID_BINDING')}}, ensure_ascii=True))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
