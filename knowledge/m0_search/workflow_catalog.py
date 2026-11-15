"""Explicit synthetic context writes, durable receipts, and held read guards.

Only initialize_receipts changes the schema. Read/lookup never create a table.
The read guard holds DELETE-journal main/source SHARED locks until context exit.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys

from catalog import (ALIASES, APPLICATION_ID, CatalogError, MARKER, OPERATOR, SOURCES,
                     canonical_hash, canonical_json, identity, operator, parse_ref)
from dispatch_binding import Binding, GRANT, load_binding, read_file, safe_path
from fixture_store import record_hash
from search import CLASSES, SearchError, authorize, result_item, visibility

OPERATION = 'context.assemble'
TABLE = 'workflow_operation_receipts'
BASE_FIELDS = {'caller', 'domains', 'scopes', 'max_class', 'as_of'}
RECEIPT_SCHEMA = 'm0.workflow_context_receipt.v1'
SNAPSHOT_SCHEMA = 'm0.workflow_context_snapshot.v1'
DDL = '''CREATE TABLE workflow_operation_receipts(
 write_key TEXT PRIMARY KEY, operation TEXT NOT NULL, request_hash TEXT NOT NULL,
 semantic_hash TEXT NOT NULL, binding_hash TEXT NOT NULL, request_json TEXT NOT NULL,
 result_json TEXT NOT NULL, result_sha256 TEXT NOT NULL, committed_at TEXT NOT NULL)'''
COLUMNS = ('write_key', 'operation', 'request_hash', 'semantic_hash', 'binding_hash',
           'request_json', 'result_json', 'result_sha256', 'committed_at')


def fail(code):
    raise CatalogError(code, code)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _hash(value):
    return 'sha256:' + hashlib.sha256(canonical_json(value).encode('utf-8')).hexdigest()


def _hash_ref(value):
    if not isinstance(value, str) or re.fullmatch(r'sha256:[0-9a-f]{64}', value) is None:
        fail('INVALID_REQUEST_HASH')
    return value


def _normalize(request, field=None):
    fields = BASE_FIELDS | ({field} if field else set())
    if not isinstance(request, dict) or set(request) != fields:
        fail('INVALID_REQUEST')
    value = json.loads(canonical_json(request))
    for key in ('domains', 'scopes'):
        values = value[key]
        if not isinstance(values, list) or not values or any(not isinstance(x, str) for x in values) or len(values) != len(set(values)):
            fail('INVALID_REQUEST')
        value[key] = sorted(values)
    if field == 'refs':
        refs = value['refs']
        if not isinstance(refs, list) or not 1 <= len(refs) <= 100 or any(not isinstance(x, str) for x in refs) or len(refs) != len(set(refs)):
            fail('INVALID_REFS')
        for ref in refs:
            domain, _, _, _ = parse_ref(ref)
            if domain not in value['domains']:
                fail('ACCESS_DENIED')
        value['refs'] = sorted(refs)
    elif field == 'context_id':
        identity(value['context_id'])
    return value


def request_digest(binding, request):
    """Inner semantic digest, not the outer invocation-envelope request_hash."""
    return _hash({'operation': OPERATION, 'source_binding': binding.digest,
                  'request': _normalize(request, 'refs')})


def _authorize(binding, request, field):
    if not isinstance(binding, Binding) or binding.grant != GRANT:
        fail('INVALID_BINDING')
    # Validate submitted timestamp syntax too; only actual now governs usability.
    authorize(request, binding.grant, field)
    current = {**request, 'as_of': _now()}
    return current, authorize(current, binding.grant, field)


def _lookup_authority(binding):
    request = {'caller': {'agent': 'fixture-agent', 'task_id': 'fixture-three-source-task', 'purpose': 'fixture-review'},
               'domains': list(SOURCES), 'scopes': ['research:read', 'signalhub:refs'],
               'max_class': 'internal', 'as_of': _now(), 'context_id': 'workflow-lookup'}
    _authorize(binding, request, 'context_id')


def _sql_path(path):
    path, _ = safe_path(path)
    if str(path).startswith(('\\\\', '//')):
        fail('SOURCE_FILESYSTEM_NOT_LOCAL')
    if sys.platform == 'win32':
        import ctypes
        if ctypes.windll.kernel32.GetDriveTypeW(str(path.anchor)) in (0, 1, 4):
            fail('SOURCE_FILESYSTEM_NOT_LOCAL')
    elif sys.platform.startswith('linux'):
        selected = None
        try:
            for line in Path('/proc/self/mountinfo').read_text().splitlines():
                left, right = line.split(' - ', 1)
                mount = Path(re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), left.split()[4]))
                if path.is_relative_to(mount) and (selected is None or len(mount.parts) > selected[0]):
                    selected = len(mount.parts), right.split()[0]
        except (OSError, ValueError, IndexError):
            fail('SOURCE_FILESYSTEM_UNVERIFIED')
        if selected is None:
            fail('SOURCE_FILESYSTEM_UNVERIFIED')
        if selected[1] in {'9p', 'drvfs', 'cifs', 'smbfs', 'nfs', 'nfs4', 'ntfs', 'ntfs3', 'fuseblk', 'fuse.sshfs'}:
            fail('SOURCE_FILESYSTEM_NOT_LOCAL')
    header = read_file(path, limit=100, prefix=True)
    if len(header) != 100 or header[:16] != b'SQLite format 3\x00' or int.from_bytes(header[68:72], 'big') != APPLICATION_ID:
        fail('NOT_SYNTHETIC_CATALOG')
    if header[18:20] != b'\x01\x01':
        fail('UNSAFE_JOURNAL_MODE')
    return path


def _readonly(action, arg1, arg2, database, trigger):
    if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ):
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_FUNCTION and (arg2 or '').lower() in {'max', 'coalesce', 'length'}:
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_PRAGMA and arg1 == 'table_info':
        return sqlite3.SQLITE_OK
    return sqlite3.SQLITE_DENY


@contextmanager
def _transaction(binding, domains=(), *, write=False):
    fresh = load_binding(binding.path)
    if fresh != binding:
        fail('BINDING_CHANGED')
    paths = {name: _sql_path(binding.catalog_root / (name + '.sqlite3')) for name in ('catalog', *domains)}
    selected = fresh.catalog()  # Marker/instance validation only; no migration.
    identities = {name: (path.stat().st_dev, path.stat().st_ino) for name, path in paths.items()}
    db = sqlite3.connect(paths['catalog'].as_uri() + ('?mode=rw' if write else '?mode=ro'),
                         uri=True, isolation_level=None, timeout=5)
    db.row_factory = sqlite3.Row
    try:
        if not write:
            db.execute('PRAGMA query_only=ON')
        for domain in domains:
            db.execute(f'ATTACH DATABASE ? AS {ALIASES[domain]}', (paths[domain].as_uri() + '?mode=ro',))
        for alias in ('main', *(ALIASES[d] for d in domains)):
            if db.execute(f'PRAGMA {alias}.journal_mode').fetchone()[0].lower() != 'delete':
                fail('UNSAFE_JOURNAL_MODE')
        db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
        # This first main read takes the shared lock that blocks lifecycle COMMIT.
        if dict(db.execute('SELECT key,value FROM catalog_meta')) != {'schema': MARKER, 'instance': binding.instance}:
            fail('CATALOG_INSTANCE_CHANGED')
        for domain in domains:
            values = dict(db.execute(f'SELECT key,value FROM {ALIASES[domain]}.source_meta'))
            expected = 'm0-signalhub-summary-fixture-v1' if domain == 'signalhub' else MARKER
            if any(values.get(key) != value for key, value in {'schema': expected, 'instance': binding.instance, 'domain': domain}.items()):
                fail('SOURCE_INSTANCE_CHANGED')
        for name, path in paths.items():
            # Do not open/close a plain file descriptor after SQLite owns a
            # POSIX lock: close() cancels this process's locks on that inode.
            # Header/filesystem checks ran before connect; verify identity and
            # links here using metadata-only lstat, without reading file bytes.
            _, current = safe_path(path)
            if identities[name] != (current.st_dev, current.st_ino):
                fail('SOURCE_INSTANCE_CHANGED')
        if not write:
            db.set_authorizer(_readonly)
        yield selected, db
        if write:
            db.execute('COMMIT')
    except BaseException as error:
        if write and db.in_transaction:
            db.execute('ROLLBACK')
            # Only a completed rollback proves that this attempted transaction
            # left no committed effects. An unobservable commit stays unknown.
            if isinstance(error, (CatalogError, SearchError)):
                error.commit_state = 'rolled_back'
        raise
    finally:
        db.close()


def _schema(db):
    columns = list(db.execute(f'PRAGMA table_info({TABLE})'))
    if not columns:
        fail('WORKFLOW_SCHEMA_REQUIRED')
    if (tuple(row['name'] for row in columns) != COLUMNS or any(row['type'].upper() != 'TEXT' for row in columns)
            or columns[0]['pk'] != 1 or any(row['pk'] or not row['notnull'] for row in columns[1:])):
        fail('WORKFLOW_SCHEMA_UNSUPPORTED')


def initialize_receipts(binding, *, operator_grant):
    """Explicit fixture upgrade; neither assemble nor any read initializes."""
    operator(operator_grant)
    _lookup_authority(binding)
    with _transaction(binding, write=True) as (_, db):
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone()
        if not exists:
            db.execute(DDL)
        _schema(db)
    return {'schema': RECEIPT_SCHEMA, 'table': TABLE, 'created': not bool(exists), 'catalog_instance': binding.instance}


def _key(operation, write_key, request_hash):
    if operation != OPERATION:
        fail('UNSUPPORTED_OPERATION')
    if not isinstance(write_key, str) or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:/@-]{0,200}', write_key) is None:
        fail('INVALID_WRITE_KEY')
    _hash_ref(request_hash)


def _stored(binding, row):
    request = json.loads(row['request_json'])
    if row['binding_hash'] != binding.digest or row['semantic_hash'] != request_digest(binding, request):
        fail('RECEIPT_INTEGRITY_ERROR')
    _authorize(binding, request, 'refs')
    result = json.loads(row['result_json'])
    if row['result_sha256'] != _hash(result) or result.get('request_hash') != row['request_hash'] or result.get('write_key') != row['write_key']:
        fail('RECEIPT_INTEGRITY_ERROR')
    return result


def _insert_receipt(db, values):
    db.execute(f'INSERT INTO {TABLE} VALUES(?,?,?,?,?,?,?,?,?)', values)


def assemble(binding, request, write_key, request_hash, *, max_age_s=86400, committed_fence=None):
    _key(OPERATION, write_key, request_hash)
    clean = _normalize(request, 'refs')
    _, (domains, _, _, _) = _authorize(binding, clean, 'refs')
    if type(max_age_s) is not int or not 1 <= max_age_s <= 86400:
        fail('INVALID_FRESHNESS_WINDOW')
    if committed_fence is not None and (type(committed_fence) is not int or committed_fence < 1):
        fail('INVALID_FENCE')
    semantic = request_digest(binding, clean)
    with _transaction(binding, domains, write=True) as (selected, db):
        _schema(db)
        prior = db.execute(f'SELECT * FROM {TABLE} WHERE write_key=?', (write_key,)).fetchone()
        if prior:
            if prior['operation'] != OPERATION or prior['request_hash'] != request_hash or prior['semantic_hash'] != semantic:
                fail('IDEMPOTENCY_CONFLICT')
            return _stored(binding, prior)
        effective, _ = _authorize(binding, clean, 'refs')
        elapsed = (datetime.fromisoformat(effective['as_of']) - datetime.fromisoformat(clean['as_of'])).total_seconds()
        if not 0 <= elapsed <= max_age_s:
            fail('AS_OF_OUTSIDE_FRESHNESS_WINDOW')
        reused = db.execute(f'SELECT write_key FROM {TABLE} WHERE operation=? AND semantic_hash=? ORDER BY committed_at,write_key LIMIT 1', (OPERATION, semantic)).fetchone()
        result = selected._assemble(db, clean, binding.grant, effective_as_of=effective['as_of'])
        committed = _now()
        receipt = {'schema': RECEIPT_SCHEMA, 'operation': OPERATION, 'write_key': write_key,
                   'request_hash': request_hash, 'semantic_hash': semantic, 'context_id': result['context_id'],
                   'manifest_sha256': result['sha256'], 'refs': clean['refs'], 'catalog_instance': binding.instance,
                   'source_binding': binding.digest, 'committed_at': committed,
                   'decision_at': effective['as_of'], 'created_as_of': clean['as_of'],
                   'committed_fence': committed_fence, 'reused_from': reused['write_key'] if reused else None,
                   'state_basis': 'historical', 'status': 'committed', 'synthetic_only': True}
        _insert_receipt(db, (write_key, OPERATION, request_hash, semantic, binding.digest,
                            canonical_json(clean), canonical_json(receipt), _hash(receipt), committed))
        return receipt


def lookup(binding, operation, write_key, request_hash):
    _key(operation, write_key, request_hash)
    _lookup_authority(binding)
    with _transaction(binding, SOURCES) as (selected, db):
        _schema(db)
        row = db.execute(f'SELECT * FROM {TABLE} WHERE write_key=?', (write_key,)).fetchone()
        if not row:
            return {'status': 'absent', 'operation': operation, 'write_key': write_key, 'request_hash': request_hash}
        if row['operation'] != operation or row['request_hash'] != request_hash:
            return {'status': 'conflict', 'operation': operation, 'write_key': write_key, 'request_hash': request_hash}
        receipt = _stored(binding, row)
        request = json.loads(row['request_json'])
        snapshot = CatalogSnapshot(binding, {key: value for key, value in request.items() if key != 'refs'}, selected, db)
        try:
            current = snapshot.inspect(receipt['context_id'], receipt['manifest_sha256'])
        finally:
            snapshot._active = False
        return {'status': 'committed', 'receipt': receipt, 'state_basis': 'historical',
                'current_usable': current['usable'], 'usable_reason': current['status'],
                'checked_at': current['checked_at']}


class CatalogSnapshot:
    """One active read transaction. inspect never returns a source body."""
    def __init__(self, binding, request_base, selected, db):
        self.binding, self.base, self.selected, self._db = binding, request_base, selected, db
        self._active = True

    @property
    def active(self):
        return self._active

    def inspect(self, context_id, expected_manifest=None):
        if not self.active:
            fail('SNAPSHOT_CLOSED')
        identity(context_id)
        current, (domains, maximum, scopes, at) = _authorize(self.binding, {**self.base, 'context_id': context_id}, 'context_id')
        row = self._db.execute('SELECT * FROM contexts WHERE context_id=?', (context_id,)).fetchone()
        if not row:
            fail('CONTEXT_NOT_FOUND')
        payload = json.loads(row['payload'])
        expected_keys = {'schema', 'caller', 'domains', 'refs', 'created_as_of'}
        if not isinstance(payload, dict) or set(payload) != expected_keys or payload['schema'] != 'm0-synthetic-context-v1' or canonical_hash(payload) != row['sha256'] or context_id != 'ctx_' + row['sha256'][:26]:
            fail('CONTEXT_HASH_MISMATCH')
        if expected_manifest is not None and (not isinstance(expected_manifest, str) or expected_manifest.removeprefix('sha256:') != row['sha256']):
            fail('CONTEXT_MANIFEST_MISMATCH')
        if payload['caller'] != self.base['caller'] or not set(payload['domains']) <= set(domains):
            fail('ACCESS_DENIED')
        refs = payload['refs']
        if not isinstance(refs, list) or not 1 <= len(refs) <= 100 or any(not isinstance(ref, str) for ref in refs) or refs != sorted(set(refs)):
            fail('CONTEXT_HASH_MISMATCH')
        required = {'signalhub:refs' if parse_ref(ref)[0] == 'signalhub' else 'research:read' for ref in refs}
        if not required <= scopes:
            fail('ACCESS_DENIED')
        members = [(parse_ref(ref)[0], parse_ref(ref)[1], parse_ref(ref)[2], ref) for ref in refs]
        actual = [tuple(value) for value in self._db.execute('SELECT domain,ref_id,version,ref FROM context_members WHERE context_id=? ORDER BY ref', (context_id,))]
        if members != actual:
            fail('CONTEXT_MEMBERS_MISMATCH')
        state, deadlines = row['status'], []
        if state not in ('current', 'stale_by_revocation'):
            fail('CONTEXT_STATE_INVALID')
        # Revocation permits metadata-only history, never a bypass of the live
        # class/scope grant. Read policy from the fixed version and current head.
        if state == 'stale_by_revocation':
            for ref in refs:
                domain, ident, version, expected = parse_ref(ref)
                alias = ALIASES[domain]
                if domain == 'signalhub':
                    matched = self.selected.lookup(ref, domains, self._db)
                    records = matched[:2] if matched else ()
                else:
                    original = self._db.execute(f'SELECT payload,sha256 FROM {alias}.record_versions WHERE ref_id=? AND version=?', (ident, version)).fetchone()
                    head = self.selected._head(self._db, alias, ident)
                    if not original or not head:
                        fail('CONTEXT_SOURCE_MISSING')
                    payload_record = json.loads(original['payload'])
                    if original['sha256'] != expected or record_hash(payload_record) != expected:
                        fail('CONTEXT_HASH_MISMATCH')
                    records = (payload_record, head)
                if not records or any(CLASSES[value['class']] > min(maximum, CLASSES['internal']) or value['required_scope'] not in scopes for value in records):
                    fail('ACCESS_DENIED')
        if state == 'current':
            for ref in refs:
                matched = self.selected.lookup(ref, domains, self._db)
                if matched is None:
                    state = 'withheld'
                    break
                original, head, _ = matched
                if visibility(original, maximum, scopes, at) != 'visible' or visibility(head, maximum, scopes, at) != 'visible':
                    state = 'withheld'
                    break
                for record in (original, head):
                    if record.get('expires_at'):
                        deadlines.append(record['expires_at'] + 'T00:00:00+00:00')
                    if result_item(parse_ref(ref)[0], record, at)['status'] == 'expired':
                        state = 'expired'
        return {'schema': SNAPSHOT_SCHEMA, 'source_binding': self.binding.digest,
                'catalog_instance': self.binding.instance, 'context_id': context_id,
                'manifest_sha256': row['sha256'], 'refs': [] if state == 'withheld' else refs,
                'status': state, 'usable': state == 'current', 'current_usable': state == 'current',
                'checked_at': current['as_of'], 'decision_at': current['as_of'], 'valid_until': min(deadlines) if deadlines else None,
                'synthetic_only': True}


@contextmanager
def catalog_snapshot(binding, request_base):
    base = _normalize(request_base)
    _, (domains, _, _, _) = _authorize(binding, {**base, 'context_id': 'workflow-snapshot'}, 'context_id')
    with _transaction(binding, domains) as (selected, db):
        snapshot = CatalogSnapshot(binding, base, selected, db)
        try:
            yield snapshot
        finally:
            snapshot._active = False


@contextmanager
def context_snapshot(binding, request, expected_manifest=None):
    clean = _normalize(request, 'context_id')
    with catalog_snapshot(binding, {key: value for key, value in clean.items() if key != 'context_id'}) as snapshot:
        yield snapshot.inspect(clean['context_id'], expected_manifest)
