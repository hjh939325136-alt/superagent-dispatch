"""Candidate release probe; run as the service user, never load live identities."""
import argparse
from contextlib import closing
import hashlib
import http.client
import json
from pathlib import Path
import re
import secrets
import sqlite3
import sys
import threading

REVIEW_SCHEMA = 'm0_deploy.late_receipt_review.v1'
RECEIPT_SCHEMA = 'm0_deploy.synthetic_receipt.v1'
REVIEW_CLASSIFICATION = 'unknown_submitter_preserved_untrusted_audit_not_accepted'
REVIEW_LIMIT = 64 * 1024


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def fingerprint(ledger, columns=None, *, migration_versions=None):
    with closing(sqlite3.connect(Path(ledger).resolve().as_uri() + '?mode=ro', uri=True)) as conn:
        conn.execute('BEGIN')
        if conn.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
            raise ValueError('SQLite integrity check failed')
        if conn.execute('PRAGMA foreign_key_check').fetchall():
            raise ValueError('SQLite foreign key check failed')
        present = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                   if not r[0].startswith('sqlite_')}
        result = {}
        for table in sorted(columns or present):
            quoted = '"' + table.replace('"', '""') + '"'
            names = columns[table]['columns'] if columns else [r[1] for r in conn.execute('PRAGMA table_info(' + quoted + ')')]
            fields = ','.join('"' + name.replace('"', '""') + '"' for name in names)
            query, params = 'SELECT ' + fields + ' FROM ' + quoted, ()
            if table == 'schema_migrations' and migration_versions is not None:
                query += ' WHERE version IN (' + ','.join('?' for _ in migration_versions) + ')'
                params = tuple(migration_versions)
            rows = sorted([canonical(list(row)).decode() for row in conn.execute(query, params)])
            result[table] = {'columns': names, 'rows': len(rows), 'sha256': hashlib.sha256(canonical(rows)).hexdigest()}
        return result


def migration_identity(ledger):
    """Stable migration identities; new applied_at timestamps may differ in rehearsal."""
    with closing(sqlite3.connect(Path(ledger).resolve().as_uri() + '?mode=ro', uri=True)) as conn:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'").fetchone():
            return []
        return [list(row) for row in conn.execute('SELECT version,name FROM schema_migrations ORDER BY version')]


def historical_fingerprint(ledger, baseline, columns):
    """Preserve every old row; only permit additions to the migration registry.

    Baseline is the verified immutable SQLite backup. Old migration rows include
    their original applied_at values in the comparison. Ordinary tables retain
    an exact row-set comparison, including detection of unexpected insertions.
    """
    versions = [row[0] for row in migration_identity(baseline)]
    return fingerprint(ledger, columns, migration_versions=versions)


def reviewed_late_refs(records, store, review):
    """Inventory explicitly reviewed, untrusted audit rows for local maintenance."""
    if (not isinstance(review, dict)
            or set(review) != {'schema', 'binding', 'classification', 'basis', 'rows'}
            or review['schema'] != REVIEW_SCHEMA
            or review['classification'] != REVIEW_CLASSIFICATION
            or not isinstance(review['basis'], str) or not review['basis'].strip()
            or not isinstance(review['rows'], list) or not review['rows']):
        raise ValueError('invalid late receipt review document')

    if any(lease['state'] == 'unknown' for lease in records['leases']):
        raise ValueError('uncertain lease history requires separate reconciliation')
    rows = {row['id']: row for row in records['late_results']}
    tasks = {task['task_id']: task for task in records['tasks']}
    leases = {(lease['task_id'], lease['fence']): lease for lease in records['leases']}
    grants = {(grant['task_id'], grant['lease_id'], grant['content_hash'])
              for grant in records['transport_artifact_grants'] if grant['access'] == 'synthetic'}
    refs = set()
    seen = set()
    for reviewed in review['rows']:
        if (not isinstance(reviewed, dict)
                or set(reviewed) != {'id', 'task_id', 'fence', 'disposition', 'status', 'row_sha256', 'cas'}
                or not isinstance(reviewed['id'], str) or reviewed['id'] in seen
                or type(reviewed['fence']) is not int):
            raise ValueError('invalid late receipt review row')
        row = rows.get(reviewed['id'])
        if (row is None or any(reviewed[key] != row[key] for key in ('task_id', 'fence', 'disposition'))
                or row['disposition'] != 'ignored'
                or reviewed['row_sha256'] != hashlib.sha256(canonical(row)).hexdigest()):
            raise ValueError('late receipt does not match the reviewed audit row')
        task = tasks.get(row['task_id'])
        old_lease = leases.get((row['task_id'], row['fence']))
        if (task is None or task['access'] != 'synthetic' or task['egress'] != 'none'
                or task['side_effect'] != 'none' or task['status'] != 'succeeded'
                or not 0 < row['fence'] < task['fence'] or old_lease is None
                or old_lease['state'] != 'expired'):
            raise ValueError('late receipt is not bound to a settled synthetic task and expired historical fence')
        accepted_lease = leases.get((row['task_id'], task['fence']))
        if accepted_lease is None or accepted_lease['state'] != 'completed':
            raise ValueError('late receipt lacks a separate successful task completion')

        outcome = json.loads(row['outcome_json'])
        if (not isinstance(outcome, dict) or set(outcome) != {'status', 'result', 'evidence'}
                or canonical(outcome).decode() != row['outcome_json']
                or outcome['status'] != 'succeeded' or outcome['status'] != reviewed['status']
                or not isinstance(outcome['result'], dict) or set(outcome['result']) != {'receipt'}
                or not isinstance(outcome['evidence'], list) or len(outcome['evidence']) != 1):
            raise ValueError('unsupported late receipt outcome')
        ref = outcome['result']['receipt']
        evidence = outcome['evidence'][0]
        if (not isinstance(ref, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', ref)
                or evidence != {'kind': 'runtime_fixture', 'content_hash': ref, 'access': 'synthetic',
                                'ref': None, 'artifact_hash': ref}
                or reviewed['cas'] != {'ref': ref, 'schema': RECEIPT_SCHEMA}
                or (task['task_id'], old_lease['lease_id'], ref) not in grants
                or json.loads(task['result_json'])['receipt'] == ref):
            raise ValueError('late receipt evidence does not match the reviewed historical grant')
        data = store.get(ref)
        receipt = json.loads(data)
        if (not isinstance(receipt, dict) or set(receipt) != {'schema', 'synthetic', 'run_id', 'task_id', 'fence'}
                or canonical(receipt) != data
                or receipt['schema'] != RECEIPT_SCHEMA or receipt['synthetic'] is not True
                or not isinstance(receipt['run_id'], str) or not receipt['run_id']
                or receipt['task_id'] != task['task_id'] or type(receipt['fence']) is not int
                or receipt['fence'] != row['fence']):
            raise ValueError('late receipt CAS schema or task/fence association differs')
        seen.add(row['id'])
        refs.add(ref)
    if seen != rows.keys():
        raise ValueError('unreviewed late receipt present')
    return refs


def inspect(release, ledger, artifacts, *, review=None):
    sys.path.insert(0, str(Path(release).resolve()))
    from sa_dispatch.bundle import ledger as read_ledger, _artifact_inventory, _settled
    from sa_dispatch.artifacts import ArtifactStore
    records = read_ledger(ledger)
    if any(row['access'] != 'synthetic' for row in records['tasks'] + records['evidence']):
        raise ValueError('only synthetic tasks and evidence are allowed')
    if any(row['enabled'] for row in records['schedules']):
        raise ValueError('disable schedules before this maintenance operation')
    store = ArtifactStore(artifacts)
    if review is None:
        refs = _artifact_inventory(records, store)
    else:
        _settled(records)
        reviewed_refs = reviewed_late_refs(records, store, review)
        # The isolated backup retains every SQLite row; this projection is only
        # the input to the portable artifact inventory, which rejects any late
        # rows. Add the separately validated audit closure explicitly.
        ordinary_refs = _artifact_inventory({**records, 'late_results': []}, store)
        refs = sorted(set(ordinary_refs) | reviewed_refs)
    blobs = [{'ref': ref, 'size': len(store.get(ref))} for ref in refs]
    return {'tables': fingerprint(ledger), 'blobs': blobs,
            'blob_bytes': sum(row['size'] for row in blobs), 'synthetic': True,
            'settled': True, 'enabled_schedules': 0}


def migrate(release, ledger, artifacts, *, review=None):
    sys.path.insert(0, str(Path(release).resolve()))
    from sa_dispatch.store_sqlite import SqliteStore
    if review is not None:
        inspect(release, ledger, artifacts, review=review)
    SqliteStore(ledger, lease_ttl_seconds=60)
    return inspect(release, ledger, artifacts, review=review)


def smoke(release, ledger, artifacts):
    sys.path.insert(0, str(Path(release).resolve()))
    from sa_dispatch.store_sqlite import SqliteStore
    from sa_dispatch.transport import CoordinatorAPI, create_server
    store = SqliteStore(ledger, lease_ttl_seconds=60)
    token = secrets.token_urlsafe(32)
    api = CoordinatorAPI(store, {'maintenance-smoke': hashlib.sha256(token.encode()).hexdigest()})
    server = create_server(api, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    statuses = []
    try:
        for authorization in (None, 'Bearer ' + token):
            conn = http.client.HTTPConnection('127.0.0.1', server.server_address[1], timeout=3)
            headers = {'Content-Type': 'application/json'}
            if authorization:
                headers['Authorization'] = authorization
            conn.request('POST', '/health', body=b'{}', headers=headers)
            response = conn.getresponse()
            statuses.append(response.status)
            body = json.loads(response.read())
            conn.close()
            if authorization and body.get('data', {}).get('status') != 'ok':
                raise ValueError('authenticated smoke health failed')
        if statuses != [401, 200]:
            raise ValueError('unexpected health status')
        return {'health_statuses': statuses, 'fresh_ephemeral_identity': True,
                'task_writes': 0, 'listening_address': '127.0.0.1'}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('inspect', 'migrate', 'smoke'))
    parser.add_argument('--release', required=True)
    parser.add_argument('--ledger', required=True)
    parser.add_argument('--artifacts', required=True)
    parser.add_argument('--late-review-stdin', action='store_true')
    args = parser.parse_args()
    try:
        options = {}
        if args.late_review_stdin:
            raw = sys.stdin.buffer.read(REVIEW_LIMIT + 1)
            if len(raw) > REVIEW_LIMIT or args.action == 'smoke':
                raise ValueError('unexpected late review input')
            review = json.loads(raw)
            if not isinstance(review, dict):
                raise ValueError('late review must be an object')
            options['review'] = review
        result = globals()[args.action](args.release, args.ledger, args.artifacts, **options)
        print(json.dumps({'ok': True, 'result': result}, sort_keys=True))
    except Exception as exc:
        # Do not echo SQL payloads, process output or credential-bearing values.
        print(json.dumps({'ok': False, 'error': type(exc).__name__}))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
