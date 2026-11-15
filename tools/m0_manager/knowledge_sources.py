"""Live authorization over the three synthetic research sources; no writable Catalog."""
from contextlib import contextmanager
import hashlib
import importlib
import json
from pathlib import Path
import sqlite3
import sys

from .inventory_sources import canonical, page_args, read_json_file, stamp
from .readonly import SourceError, authorize_read, local_file, local_sqlite_file

SCHEMA = 'm0-synthetic-research-catalog-v1'
ROOT = Path(__file__).resolve().parents[2] / 'knowledge' / 'm0_search'
CALLER = {'agent': 'fixture-agent', 'task_id': 'fixture-three-source-task', 'purpose': 'fixture-review'}


def _modules():
    # The old fixture modules use absolute sibling imports. Only this fixed code root is allowed.
    sys.path.insert(0, str(ROOT))
    try:
        result = tuple(importlib.import_module(name) for name in ('dispatch_binding', 'catalog', 'search'))
        for module in result:
            if Path(module.__file__).resolve().parent != ROOT:
                raise SourceError('source_adapter_conflict')
        return result
    finally:
        sys.path.remove(str(ROOT))


class _Reader:
    """Only read methods; reuse verified hashing/policy reads, not Catalog construction."""
    def __init__(self, binding, catalog_module):
        self.binding, self.module, self.instance = binding, catalog_module, binding.instance
        self._policy_matches = catalog_module.Catalog._policy_matches

    def _revision(self, db, alias):
        return self.module.Catalog._revision(self, db, alias)

    def load(self, db, domains):
        return self.module.Catalog._load(self, db, domains)

    def lookup(self, ref, domains, db):
        return self.module.Catalog.lookup(self, ref, domains, db)

    @contextmanager
    def transaction(self):
        binding, module = self.binding, self.module
        paths = {name: local_sqlite_file(binding.catalog_root / (name + '.sqlite3')) for name in ('catalog', *module.SOURCES)}
        identities = {name: (path.stat().st_dev, path.stat().st_ino) for name, path in paths.items()}
        db = sqlite3.connect(paths['catalog'].as_uri() + '?mode=ro', uri=True, timeout=2, isolation_level=None)
        try:
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA query_only=ON')
            for domain in module.SOURCES:
                db.execute(f'ATTACH DATABASE ? AS {module.ALIASES[domain]}', (paths[domain].as_uri() + '?mode=ro',))
            db.execute('BEGIN')
            db.set_authorizer(authorize_read)
            # Same lock order as the fixture writer: main catalog snapshot before sources.
            meta = dict(db.execute('SELECT key,value FROM catalog_meta'))
            if meta != {'schema': module.MARKER, 'instance': binding.instance}:
                raise SourceError('source_instance_changed')
            for domain in module.SOURCES:
                meta = dict(db.execute(f'SELECT key,value FROM {module.ALIASES[domain]}.source_meta'))
                expected = 'm0-signalhub-summary-fixture-v1' if domain == 'signalhub' else module.MARKER
                if meta.get('schema') != expected or meta.get('instance') != binding.instance or meta.get('domain') != domain:
                    raise SourceError('source_instance_changed')
            for name, path in paths.items():
                current = local_sqlite_file(path).stat()
                if identities[name] != (current.st_dev, current.st_ino):
                    raise SourceError('source_instance_changed')
            yield db
        finally:
            db.close()


def _preflight(binding, binding_module, catalog_module):
    marker, digest = read_json_file(binding.catalog_root / 'catalog.json', 16384)
    if (not isinstance(marker, dict) or marker.get('schema') != catalog_module.MARKER or
            marker.get('synthetic_only') is not True or marker.get('sources') != list(catalog_module.SOURCES) or
            marker.get('instance') != binding.instance or digest != binding.marker_hash):
        raise SourceError('source_not_synthetic')
    for name in ('catalog', *catalog_module.SOURCES):
        header = binding_module.read_file(binding.catalog_root / (name + '.sqlite3'), limit=100, prefix=True)
        if (len(header) != 100 or header[:16] != b'SQLite format 3\x00' or
                int.from_bytes(header[68:72], 'big') != catalog_module.APPLICATION_ID):
            raise SourceError('source_not_synthetic')


def _get(reader, db, ref, params, grant, search_module):
    domains, maximum, scopes, at = search_module.authorize(params | {'ref': ref}, grant, 'ref')
    match = reader.lookup(ref, domains, db)
    if not match:
        return None, None
    row, current, revision = match
    if search_module.visibility(row, maximum, scopes, at) != 'visible' or search_module.visibility(current, maximum, scopes, at) != 'visible':
        return None, None
    item = search_module.result_item(ref.split('/')[0], row, at)
    if search_module.result_item(ref.split('/')[0], current, at)['status'] == 'expired':
        item['status'] = 'expired'
    return item, revision


def _item(value):
    # Never return a body or snippet, even though this source is synthetic.
    result = {key: value[key] for key in ('ref', 'title', 'status', 'class', 'observed_at',
              'expires_at', 'human_reviewed', 'human_locked') if key in value}
    result['readback_sha256'] = hashlib.sha256(canonical(value)).hexdigest()
    return result


def knowledge_query(source: dict, op: str, args: dict, role: str) -> dict:
    if role not in {'technical', 'manager', 'progress'} or (op != 'context.status' and role != 'manager'):
        raise SourceError('source_access_denied')
    if op not in {'knowledge.search', 'knowledge.get', 'context.status'}:
        raise SourceError('source_operation_unsupported')
    fields = {'knowledge.search': {'source_id', 'q', 'limit'}, 'knowledge.get': {'source_id', 'ref'},
              'context.status': {'source_id', 'context_id', 'limit'}}
    if not isinstance(args, dict) or set(args) - fields[op]:
        raise SourceError('E_FIELD_NOT_ALLOWED')
    selected = args if op == 'knowledge.search' else ({'ids': [args.get('ref')]} if op == 'knowledge.get' else
                {'ids': [args['context_id']] if 'context_id' in args else [], 'limit': args.get('limit', 50)})
    limit, offset, ids, words = page_args(selected, get=op == 'knowledge.get')
    if op == 'knowledge.search' and not words:
        raise SourceError('source_invalid_request')
    if SCHEMA not in source['accepted_schema_versions']:
        raise SourceError('source_schema_unsupported')
    try:
        bound, catalog, search = _modules()
        binding = bound.load_binding(local_file(source['locator']))
        options = source.get('options', {})
        if binding.digest != options.get('binding_sha256') or binding.instance != options.get('catalog_instance'):
            raise SourceError('source_instance_changed')
        observed = stamp()
        params = {'caller': dict(CALLER), 'domains': list(catalog.SOURCES),
                  'scopes': ['research:read', 'signalhub:refs'], 'max_class': 'internal', 'as_of': observed}
        # Reload the fixture grant on every request before source stat/SQL preflight.
        field = 'q' if op == 'knowledge.search' else 'ref' if op == 'knowledge.get' else 'context_id'
        search.authorize(params | {field: args.get('q', '') if field == 'q' else (sorted(ids)[0] if ids else 'manager-context-list')}, binding.grant, field)
        _preflight(binding, bound, catalog)
        reader, items, versions, withheld = _Reader(binding, catalog), [], {}, 0
        with reader.transaction() as db:
            if op == 'knowledge.search':
                domains, maximum, scopes, at = search.authorize(params | {'q': args['q'], 'limit': limit}, binding.grant, 'q')
                for domain, (records, revision) in reader.load(db, domains).items():
                    versions[domain] = revision
                    for row in records:
                        if all(word in f"{row['title']} {row['body']}".casefold() for word in words):
                            if search.visibility(row, maximum, scopes, at) == 'visible':
                                items.append(_item(search.result_item(domain, row, at)))
                            else:
                                withheld += 1
                items.sort(key=lambda item: item['ref'])
                items.sort(key=lambda item: item.get('observed_at', ''), reverse=True)
                items.sort(key=lambda item: item.get('status') != 'current')
            elif op == 'knowledge.get':
                for ref in sorted(ids):
                    catalog.parse_ref(ref)
                    value, revision = _get(reader, db, ref, params, binding.grant, search)
                    if value is None:
                        withheld += 1
                    else:
                        items.append(_item(value))
                        versions[ref.split('/')[0]] = revision
            else:
                selected_ids = sorted(ids) if ids else [row[0] for row in db.execute('SELECT context_id FROM contexts ORDER BY context_id LIMIT 1000')]
                for ident in selected_ids:
                    catalog.identity(ident)
                    row = db.execute('SELECT * FROM contexts WHERE context_id=?', (ident,)).fetchone()
                    if not row:
                        withheld += 1
                        continue
                    payload = json.loads(row['payload'])
                    if catalog.canonical_hash(payload) != row['sha256']:
                        raise SourceError('source_hash_mismatch')
                    if payload['caller'] != params['caller'] or not set(payload['domains']) <= set(params['domains']):
                        withheld += 1
                        continue
                    state, usable = row['status'], False
                    if state == 'current':
                        values = [_get(reader, db, ref, params, binding.grant, search)[0] for ref in payload['refs']]
                        if any(value is None for value in values):
                            state = 'withheld'
                        elif any(value['status'] == 'expired' for value in values):
                            state = 'expired'
                        else:
                            usable = True
                    items.append({'id': ident, 'ref': f"knowledge/context/{ident}#sha256:{row['sha256']}",
                                  'state': state, 'usable': usable, 'state_basis': 'current', 'as_of': observed})
        for item in items:
            item.setdefault('state_basis', 'current')
            item.setdefault('as_of', observed)
        attention = [{'rule': 'A12@1', 'state': 'stale_by_revocation', 'subject_id': item['id'], 'ref': item['ref'],
                      'state_basis': 'current', 'as_of': observed} for item in items if item.get('state') == 'stale_by_revocation']
        return {'items': items[offset:offset + limit], 'attention': attention, 'coverage': 'partial', 'state_basis': 'current',
                'unknown': ['synthetic_sources_only', 'signalhub_incremental_changes_unsupported', 'body_withheld_by_manager_projection'],
                'source_instance': source['source_instance'], 'schema_version': SCHEMA, 'observed_at': observed,
                'incremental': {'research-scatter': 'supported', 'research-content': 'supported', 'signalhub': 'unsupported'},
                'collection_modes': {'research-scatter': 'incremental', 'research-content': 'incremental', 'signalhub': 'snapshot_only'},
                'index_versions': versions, 'source_binding': binding.digest, 'catalog_instance': binding.instance,
                'synthetic_only': True, 'withheld': {'by_policy': withheld}, 'actions_taken': 0}
    except SourceError:
        raise
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error) as error:
        if getattr(error, 'code', '') == 'ACCESS_DENIED':
            raise SourceError('source_access_denied') from None
        raise SourceError('source_unavailable') from None
