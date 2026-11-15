"""Bound inventory JSON metadata only. Never follow locators inside a record."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from urllib.parse import quote

from .readonly import SourceError, local_file

CATEGORIES = {'projects': 'project', 'threads': 'thread', 'artifacts': 'artifact',
              'environments': 'environment', 'cleanup-candidates': 'cleanup'}
ROLES = {'technical': {'environments', 'artifacts'}, 'manager': set(CATEGORIES),
         'progress': {'projects', 'threads'}}
STATES = {'active', 'idle', 'running', 'completed', 'archived', 'unknown', 'removed',
          'recommendation_ready', 'planned', 'retired', 'available', 'unavailable'}
DECISIONS = {'cleanup_candidate', 'installer_cleanup_review', 'hold_for_package_review',
             'hold_unknown_origin', 'archive_candidate'}
KINDS = {'codex', 'claude_code', 'hermes', 'runtime', 'command_resolution',
         'agent_or_runtime_location', 'virtual_environment', 'download_location',
         'project_output_location', 'planned_remote_environment', 'local_ai_deployment_or_related_item',
         'local_music_and_song_generation', 'script', 'draft', 'media', 'config'}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def stamp():
    return datetime.now(timezone.utc).isoformat()


def read_json_file(locator, limit=1024 * 1024):
    """No linked/shared source file; bounded read and identity/change checks."""
    try:
        path = local_file(locator)
        before = path.stat()
        if before.st_nlink != 1 or before.st_size > limit:
            raise SourceError('source_file_not_allowed')
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
                raise SourceError('source_changed')
            raw = stream.read(limit + 1)
            after = os.fstat(stream.fileno())
        current = local_file(path).stat()
        fingerprint = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns)
        if len(raw) > limit or fingerprint(before) != fingerprint(after) or fingerprint(after) != fingerprint(current):
            raise SourceError('source_changed')
        def unique(pairs):
            result = dict(pairs)
            if len(result) != len(pairs):
                raise SourceError('source_invalid')
            return result
        def invalid(_):
            raise SourceError('source_invalid')
        return json.loads(raw, object_pairs_hook=unique, parse_constant=invalid), hashlib.sha256(raw).hexdigest()
    except SourceError:
        raise
    except (OSError, ValueError, TypeError, UnicodeError):
        raise SourceError('source_unavailable') from None


def text(value, maximum=512):
    return value if isinstance(value, str) and len(value) <= maximum and not any(ord(c) < 32 for c in value) else None


def texts(value, maximum=100):
    return sorted(set(value)) if isinstance(value, list) and len(value) <= maximum and all(text(v) is not None for v in value) else None


def safe_time(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return value if parsed.utcoffset() is not None else None
    except ValueError:
        return None


def page_args(args, *, get=False):
    if not isinstance(args, dict) or set(args) - {'source_id', 'ids', 'q', 'limit', 'offset'}:
        raise SourceError('E_FIELD_NOT_ALLOWED')
    limit, offset = args.get('limit', 50), args.get('offset', 0)
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or not 0 <= offset <= 100000:
        raise SourceError('source_invalid_request')
    ids = args.get('ids', [])
    if not isinstance(ids, list) or len(ids) > 100 or any(text(i, 512) is None or not i for i in ids):
        raise SourceError('source_invalid_request')
    q = args.get('q', '')
    if not isinstance(q, str) or len(q) > 200 or (get and (not ids or q)):
        raise SourceError('source_invalid_request')
    return limit, offset, set(ids), q.casefold().split()


def _project(record, category, updated, observed, freshness):
    ident = record.get('projectId', record.get('id')) if category == 'projects' else record.get('id')
    if text(ident, 256) is None or not ident:
        raise SourceError('source_invalid_identity')
    observed_source = safe_time(record.get('checkedAt')) or safe_time(record.get('observedAt')) or updated
    state = record.get('status')
    state = state if state in STATES else 'unknown'
    if category == 'threads' and type(record.get('archived')) is bool:
        state = 'archived' if record['archived'] else state
    basis = 'stale'
    if observed_source and 0 <= (datetime.fromisoformat(observed) - datetime.fromisoformat(observed_source)).total_seconds() <= freshness:
        basis = 'current'
    if category == 'cleanup-candidates' and state == 'removed':
        basis = 'historical'
    row = {'id': ident, 'state': state, 'state_basis': basis, 'as_of': observed_source}
    if category == 'projects':
        row.update(name=text(record.get('name')) or text(record.get('label')),
                   path=text(record.get('path'), 2048), host_id=text(record.get('hostId')))
    elif category == 'threads':
        row.update(name=text(record.get('originalTitle')) or text(record.get('title')),
                   project_id=text(record.get('projectId')), path=text(record.get('cwd'), 2048),
                   host_id=text(record.get('hostId')), kind=record.get('kind') if record.get('kind') in KINDS else None)
    elif category in {'artifacts', 'environments'}:
        row.update(name=text(record.get('name')), path=text(record.get('absolutePath'), 2048),
                   project_ids=texts(record.get('projectIds')), thread_ids=texts(record.get('threadIds')))
        kind = record.get('kind', record.get('role'))
        row['kind'] = kind if kind in KINDS else 'unknown'
        if category == 'environments':
            row['version'] = text(record.get('version'), 100)
        # Descriptive artifact version dictionaries and existenceCheck prose stay at source.
    else:
        row.update(path=text(record.get('absolutePath'), 2048), canDelete=False,
                   decision=record.get('decision') if record.get('decision') in DECISIONS else None,
                   actions_taken=0, report_kind='recorded_recommendations')
        for key in ('logicalBytes', 'exists'):
            value = record.get(key)
            row[key] = value if (type(value) is int and value >= 0 if key == 'logicalBytes' else type(value) is bool) else None
        value = record.get('sha256')
        row['historical_sha256'] = value if isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) else None
    refs = record.get('business_refs')
    valid_refs = isinstance(refs, list) and len(refs) <= 100 and all(isinstance(x, str) and re.fullmatch(r'mgmt/project/[A-Za-z0-9_.:-]{1,128}', x) for x in refs)
    row['business_refs'] = sorted(set(refs)) if valid_refs else []
    row['association'] = 'declared' if row['business_refs'] else 'none_declared'
    row['unknown_fields'] = sorted(key for key, value in row.items() if value is None)
    if refs is not None and not valid_refs:
        row['unknown_fields'].append('business_refs_invalid')
    # The digest excludes the observation-dependent current/stale classification.
    row['ref'] = f"inventory/{CATEGORIES[category]}/{quote(ident, safe='')}@1#sha256:{sha({k:v for k,v in row.items() if k != 'state_basis'})}"
    return row


def inventory_query(source: dict, op: str, args: dict, role: str) -> dict:
    category = source.get('options', {}).get('category')
    if role not in ROLES or category not in ROLES[role]:
        raise SourceError('source_access_denied')
    if op not in {'inventory.search', 'inventory.get'}:
        raise SourceError('source_operation_unsupported')
    if not isinstance(args, dict) or set(args) - ({'source_id', 'id'} if op.endswith('.get') else {'source_id', 'q', 'limit', 'offset'}):
        raise SourceError('E_FIELD_NOT_ALLOWED')
    selected = {'ids': [args.get('id')]} if op.endswith('.get') else args
    limit, offset, ids, words = page_args(selected, get=op.endswith('.get'))
    data, digest = read_json_file(source['locator'])
    if not isinstance(data, dict) or type(data.get('schemaVersion')) is not int or str(data['schemaVersion']) not in source['accepted_schema_versions'] or data['schemaVersion'] != 1:
        raise SourceError('source_schema_unsupported')
    rows = data.get('items')
    if not isinstance(rows, list) or len(rows) > 2000 or any(not isinstance(row, dict) for row in rows):
        raise SourceError('source_invalid')
    observed, updated = stamp(), safe_time(data.get('updatedAt'))
    items = [_project(row, category, updated, observed, source['freshness_s']) for row in rows]
    if len({row['id'] for row in items}) != len(items):
        raise SourceError('source_invalid_identity')
    if role == 'progress':
        items = [{key: row[key] for key in ('id', 'ref', 'name', 'state', 'state_basis', 'as_of') if key in row} for row in items]
    # Filtering occurs after role projection, so a hidden path cannot become a search oracle.
    found = [row for row in items if (not ids or row['id'] in ids or row['ref'] in ids) and
             all(word in ' '.join(value for key, value in row.items() if key in {'id', 'name', 'path', 'kind', 'state'} and isinstance(value, str)).casefold() for word in words)]
    found.sort(key=lambda row: row['id'])
    unknown = ['source_is_partial_index', 'referenced_files_not_opened', 'business_associations_not_inferred']
    if not updated:
        unknown.append('source_updated_at_unknown')
    if category == 'cleanup-candidates':
        unknown.append('historical_recommendations_not_current_cleanup_authority')
    if ids - {value for row in found for value in (row['id'], row['ref'])}:
        unknown.append('some_requested_ids_not_found')
    coverage = data.get('coverage', {})
    details = {key: value for key, value in coverage.items() if key in {
        'wholeMachine', 'fullArtifactCensus', 'contentValidation', 'recursiveScan', 'sizeAudit',
        'credentialsRead', 'softwareRegistryAudited', 'reclaimableWholeMachineSpaceAssessed'} and type(value) is bool} if isinstance(coverage, dict) else {}
    source_basis = 'current' if updated and 0 <= (datetime.fromisoformat(observed) - datetime.fromisoformat(updated)).total_seconds() <= source['freshness_s'] else 'stale'
    return {'items': found[offset:offset + limit], 'coverage': 'partial', 'coverage_details': details, 'state_basis': source_basis,
            'unknown': unknown, 'source_instance': source['source_instance'], 'schema_version': '1',
            'observed_at': observed, 'source_updated_at': updated, 'source_sha256': digest,
            'record_count': len(rows), 'matched_count': len(found), 'actions_taken': 0}
