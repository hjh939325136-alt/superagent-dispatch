"""Read a bounded handoff index snapshot without following its references."""
from datetime import date
import re

from .binding import digest, fixed_ref, now
from .hook_sources import source_check, request_check, source_time, basis
from .inventory_sources import read_json_file
from .readonly import SourceError

FORMAT = 'superagent.handoffs.v1'
MAX_BYTES = 1024 * 1024
MAX_ROWS = 2000
# These are the version-1 states actually supported by this source format.
# New source states require an explicit adapter update rather than an inference.
STATES = {'available_receipt_unconfirmed', 'received'}
PRODUCER = {'completed'}
CONSUMER = {'unknown'}
RECEIVER = {'unconfirmed'}
LOCAL = {'received_and_reviewed'}
IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}\Z')


def _object(value):
    if not isinstance(value, dict):
        raise SourceError('source_invalid')
    return value


def _enum(value, allowed, *, optional=False):
    if value is None and optional:
        return None
    if not isinstance(value, str) or value not in allowed:
        raise SourceError('source_schema_unsupported')
    return value


def _boolean(value):
    if value is not None and type(value) is not bool:
        raise SourceError('source_invalid')
    return value


def _integer(value):
    if value is not None and (type(value) is not int or value < 0 or value > 2**63 - 1):
        raise SourceError('source_invalid')
    return value


def _date(value):
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise SourceError('source_invalid_time')
    try:
        date.fromisoformat(value)
    except ValueError:
        raise SourceError('source_invalid_time') from None
    return value


def _receipt(value, *, local=False):
    if value is None:
        return None
    value = _object(value)
    return {'status': _enum(value.get('status'), LOCAL if local else RECEIVER),
            'evidence_present': value.get('evidence') is not None,
            'reviewed_at' if local else 'last_checked_at':
                source_time(value.get('reviewedAt' if local else 'lastCheckedAt'), optional=True)}


def _project(record, source, observed):
    record = _object(record)
    identity = record.get('id')
    if not isinstance(identity, str) or not IDENTIFIER.fullmatch(identity):
        raise SourceError('source_invalid_identity')
    updated = source_time(record.get('updatedAt'))
    version = _object(record.get('version', {}))
    declared_hash = version.get('sha256')
    if declared_hash is not None and (not isinstance(declared_hash, str) or not re.fullmatch('[0-9a-f]{64}', declared_hash)):
        raise SourceError('source_invalid')
    existence = _object(record.get('existenceCheck', {}))
    retention = _object(record.get('retention', {}))
    item = {'id': identity, 'kind': 'handoff.index_observed', 'status': _enum(record.get('status'), STATES),
            'producer_status': _enum(record.get('producerStatus'), PRODUCER, optional=True),
            'consumer_business_status': _enum(record.get('consumerBusinessStatus'), CONSUMER, optional=True),
            'receiver_receipt': _receipt(record.get('receiverReceipt')),
            'local_receipt': _receipt(record.get('localReceipt'), local=True),
            'created_date': _date(record.get('createdDate')), 'created_date_precision': 'date' if record.get('createdDate') else None,
            'declared_version_sha256': declared_hash, 'version_hash_verified': False,
            'declared_exists': _boolean(existence.get('exists')),
            'existence_checked_at': source_time(existence.get('checkedAt'), optional=True),
            'declared_logical_bytes': _integer(existence.get('logicalBytes')),
            'automatic_deletion': _boolean(retention.get('automaticDeletion')), 'actions_taken': 0,
            'as_of': updated, 'state_basis': basis(updated, observed, source['freshness_s']),
            'declaration_basis': 'source_index_only'}
    item['unknown_fields'] = sorted(key for key, value in item.items() if value is None)
    stable = {k: v for k, v in item.items() if k != 'state_basis'}
    item['ref'] = fixed_ref(source, 'handoff-index', identity, stable)
    return item


def handoff_query(source: dict, op: str, args: dict, role: str) -> dict:
    source_check(source, 'handoff', FORMAT, role)
    limit, _ = request_check(op, args, {'status', 'handoff.list'})
    data, raw_hash = read_json_file(source['locator'], limit=MAX_BYTES)
    data = _object(data)
    if type(data.get('schemaVersion')) is not int or data['schemaVersion'] != 1:
        raise SourceError('source_schema_unsupported')
    rows = data.get('items')
    if not isinstance(rows, list) or len(rows) > MAX_ROWS:
        raise SourceError('source_record_limit')
    observed, updated = now(), source_time(data.get('updatedAt'))
    items = sorted((_project(row, source, observed) for row in rows), key=lambda row: row['id'])
    if len({row['id'] for row in items}) != len(items):
        raise SourceError('source_invalid_identity')
    coverage = _object(data.get('coverage', {}))
    declared = {'checked_at': source_time(coverage.get('checkedAt'), optional=True),
                'verified_date': _date(coverage.get('verifiedDate')),
                'file_count': _integer(coverage.get('fileCount')),
                'whole_machine': _boolean(coverage.get('wholeMachine'))}
    declared['state_basis'] = basis(declared['checked_at'], observed, source['freshness_s'])
    policy = _object(data.get('policy', {}))
    policy_flags = {key: _boolean(policy.get(key)) for key in ('backgroundService', 'scheduledCleanup')}
    stable = [{k: v for k, v in row.items() if k != 'state_basis'} for row in items]
    unknown = ['partial_index_only', 'source_states_are_declarations', 'referenced_files_not_opened',
               'receipt_evidence_not_read_or_verified', 'producer_and_local_receipt_do_not_prove_receiver_business_completion',
               'native_source_instance_absent', 'native_sequence_and_tombstones_absent',
               'missing_items_do_not_prove_deletion', 'no_history_or_incremental_collection',
               'declared_policy_is_not_service_observation']
    if declared['checked_at'] is None:
        unknown.append('coverage_checked_at_unknown')
    elif declared['state_basis'] == 'stale':
        unknown.append('declared_coverage_stale')
    if any(row['receiver_receipt'] is None or not row['receiver_receipt']['evidence_present'] for row in items):
        unknown.append('receiver_evidence_missing')
    if any(row['consumer_business_status'] in (None, 'unknown') for row in items):
        unknown.append('consumer_business_completion_unknown')
    details = {'mode': 'snapshot_only', 'scope': 'current_index_items', 'declared_coverage': declared,
               'source_instance_basis': 'trusted_local_binding', 'declared_policy': policy_flags,
               'metadata_sha256': digest(stable), 'referenced_files_opened': 0, 'native_sequence': False}
    return {'items': items[:limit], 'coverage': 'partial', 'coverage_details': details,
            'unknown': unknown, 'state_basis': basis(updated, observed, source['freshness_s']),
            'source_instance': source['source_instance'], 'schema_version': '1', 'observed_at': observed,
            'source_updated_at': updated, 'source_sha256': raw_hash, 'record_count': len(rows),
            'matched_count': len(items), 'incremental': False, 'collection_modes': ['snapshot_only'], 'actions_taken': 0}
