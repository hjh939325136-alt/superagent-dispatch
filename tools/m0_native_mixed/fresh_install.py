"""Read-only admission and Collector verification for a newly created ledger."""

from contextlib import closing
from dataclasses import asdict
import json
import re
import sqlite3

from dispatch.sa_dispatch.bundle import snapshot_records
from dispatch.sa_dispatch.collector import (
    binding_identity_matches, digest as collector_digest, enrich, project, readonly,
    source_info, validate_records,
)
from dispatch.sa_dispatch.collector_cli import unique
from dispatch.sa_dispatch.models import DispatchError
from tools.m0_deploy.install_target import (
    InstallTargetError, TARGET_FIELDS, TARGET_SCHEMA, TARGET_SCOPE, _read_protected, validate_manifest_sha256,
)
from tools.m0_native_mixed.maintenance import MaintenanceError, _safe, digest


FRESH_MODE = 'fresh_0012'
FRESH_SCHEMA = 'native_mixed.fresh_install.v1'
FRESH_NAME = 'fresh-install.json'
INSTALL_TARGET_NAME = 'install-target.json'
WORKER_SOURCE_NAME = 'http-worker-source.json'
FRESH_FILES = (FRESH_NAME, INSTALL_TARGET_NAME, WORKER_SOURCE_NAME, 'fresh-install-state.json')
FRESH_STREAMS = ('events', 'control_events', 'schedule_events')
PROOF_LIMIT = 4096
INITIAL_TABLES = {
    'schema_migrations', 'ledger_mode', 'devices', 'schedules', 'schedule_events',
    'control_budgets', 'unified_events', 'collector_cursors',
}


def read_fresh_file(layout, name, *, fixture):
    if name not in FRESH_FILES:
        raise MaintenanceError('unknown fresh installation record')
    path = layout.config_root / name
    try:
        if not fixture:
            return _read_protected(path, limit=PROOF_LIMIT)
        with _safe(path).open('rb') as stream:
            raw = stream.read(PROOF_LIMIT + 1)
        if len(raw) <= PROOF_LIMIT:
            return raw
    except (OSError, InstallTargetError) as exc:
        raise MaintenanceError('cannot securely read fresh installation record') from exc
    raise MaintenanceError('fresh installation record too large')


def read_fresh_proof(layout, expected_sha256, deployment_manifest, binding, identity, *, fixture):
    files = {name: read_fresh_file(layout, name, fixture=fixture) for name in FRESH_FILES}
    hashes = {name: digest(raw) for name, raw in files.items()}
    if hashes[FRESH_NAME] != expected_sha256:
        raise MaintenanceError('fresh installation proof hash differs')
    try:
        proof = json.loads(files[FRESH_NAME].decode('utf-8'), object_pairs_hook=unique)
        target = json.loads(files[INSTALL_TARGET_NAME].decode('utf-8'), object_pairs_hook=unique)
        manifest = validate_manifest_sha256(proof['deployment_manifest'])
        baseline_hash = validate_manifest_sha256(proof['baseline_rows_sha256'])
    except (ValueError, KeyError, TypeError, RecursionError) as exc:
        raise MaintenanceError('invalid fresh installation proof') from exc
    source = asdict(binding)
    source['accepted_schema_versions'] = list(binding.accepted_schema_versions)
    if (proof != dict(schema=FRESH_SCHEMA, phase='completed', creation='exclusive_new_ledger',
                      deployment_manifest=manifest, ledger=str(layout.ledger), ledger_identity=identity,
                      source_binding=source, baseline_rows_sha256=baseline_hash) or
            (deployment_manifest is not None and manifest != deployment_manifest) or
            any(type(proof['ledger_identity'][key]) is not int for key in ('device', 'inode'))):
        raise MaintenanceError('fresh installation proof identity or scope differs')
    if (not isinstance(target, dict) or set(target) != TARGET_FIELDS or
            any(type(value) is not str for value in target.values()) or
            target['schema'] != TARGET_SCHEMA or target['scope'] != TARGET_SCOPE or
            target['manifest_sha256'] != manifest or not target['hostname'] or
            re.fullmatch('[0-9a-f]{32}', target['machine_id']) is None):
        raise MaintenanceError('fresh installation target evidence differs')
    return proof, hashes


def _verify_projection(binding, stream, cursor, conn, projected_rows):
    source_rows = conn.execute(f'SELECT rowid AS _collector_seq,* FROM {stream} ORDER BY rowid')
    last, redactions, anchor = 0, 0, None
    for source in source_rows:
        row = enrich(conn, binding, stream, source)
        last += 1
        if row['_collector_seq'] != last:
            raise MaintenanceError('fresh Collector source sequence is not contiguous')
        event, redacted = project(binding, stream, row, cursor['updated_at'])
        stored = projected_rows.pop(f'{stream}:{last}', None)
        if (stored is None or any(stored[name] != event[name] for name in
                ('trace_id', 'task_id', 'attempt_epoch', 'actor', 'component', 'kind', 'at')) or
                json.loads(stored['refs_json']) != event['refs'] or
                json.loads(stored['metadata_json']) != event['metadata']):
            raise MaintenanceError('fresh Collector source projection differs')
        redactions += redacted
        anchor = collector_digest(row)
    if (cursor['last_seq'], cursor['anchor_sha256'], cursor['redacted_count']) != (last, anchor, redactions):
        raise MaintenanceError('fresh Collector tail or anchor differs')


def verify_fresh_streams(path, binding):
    """Read the same SQLite snapshot for exact cursors, raw streams and projections."""
    try:
        if (binding.kind != 'dispatch' or binding.locator != str(path) or
                list(binding.accepted_schema_versions) != ['0012'] or
                binding.collection_mode != 'incremental' or not binding_identity_matches(binding)):
            raise MaintenanceError('fresh Collector binding differs')
        with closing(readonly(path)) as conn:
            rows = snapshot_records(conn)
            cursors = rows['collector_cursors']
            validate_records(cursors, rows['collector_gaps'])
            by_stream = {row['stream']: row for row in cursors}
            if (len(cursors) != len(FRESH_STREAMS) or set(by_stream) != set(FRESH_STREAMS) or
                    rows['collector_gaps']):
                raise MaintenanceError('fresh Collector requires exactly three cursors and no gaps')
            namespace = f'{binding.source_id}@{binding.source_instance}:e1'
            projections = {row['source_id']: row for row in rows['unified_events']
                           if row['source'] == namespace and row['class'] == binding.data_class}
            if len(projections) != len(rows['unified_events']):
                raise MaintenanceError('unexpected fresh Collector projection namespace')
            for stream, cursor in by_stream.items():
                binding.validate(stream)
                version, schema = source_info(conn, binding, stream)
                if (version != '0012' or cursor['schema_version'] != version or
                        cursor['schema_sha256'] != schema or cursor['source_id'] != binding.source_id or
                        cursor['source_instance'] != binding.source_instance or
                        cursor['binding_sha256'] != binding.fingerprint() or cursor['authority'] != 'local_owned' or
                        cursor['epoch'] != 1):
                    raise MaintenanceError('fresh Collector cursor identity, schema or epoch differs')
                _verify_projection(binding, stream, cursor, conn, projections)
            if projections or not binding_identity_matches(binding):
                raise MaintenanceError('fresh Collector projection or source identity drift')
            return rows
    except MaintenanceError:
        raise
    except (OSError, ValueError, TypeError, KeyError, LookupError, sqlite3.DatabaseError, DispatchError) as exc:
        raise MaintenanceError('invalid fresh Collector snapshot') from exc


def verify_fresh_baseline(path, binding):
    """Return the verified isolated 0012 snapshot used to seal the creation proof."""
    rows = verify_fresh_streams(path, binding)
    schedules = {row['id']: row for row in rows['schedules']}
    if (rows['ledger_mode'] != [{'id': 1, 'mode': 'isolated'}] or
            any(value for table, value in rows.items() if table not in INITIAL_TABLES) or
            any(row['enabled'] or row['last_fire_key'] is not None or row['last_tick_at'] is not None
                for row in schedules.values()) or
            any(row['used_units'] or row['reserved_units'] for row in rows['control_budgets']) or
            any(row['kind'] not in {'schedule.bound_removed', 'schedule.exhausted'} or
                row['schedule_id'] not in schedules or row['version'] > schedules[row['schedule_id']]['version']
                for row in rows['schedule_events'])):
        raise MaintenanceError('fresh ledger contains business history or active scheduling')
    return rows
