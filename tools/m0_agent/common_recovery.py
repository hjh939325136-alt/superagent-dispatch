"""Private cold recovery for the common dispatch ledger and its native domains."""

from contextlib import closing
from dataclasses import dataclass
import json
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import tomllib

from dispatch.sa_dispatch.external_migration import verify_schema
from dispatch.sa_dispatch.models import DispatchError
from tools.m0_native_mixed.native_adapter import directory_lock
from tools.m0_native_mixed.recovery_set import (CONFIGS, DATABASES, DOMAIN_FILES, HEX, LEGACY,
                                                 _database, _identity, _info, _lexical,
                                                 _matches_tree, _read)
from tools.m0_native_mixed.fresh_install import FRESH_FILES
from tools.m0_native_mixed.maintenance import MaintenanceError, _sync_dir, canonical, digest

from .deepseek_client import allowed_connect_proxy


SCHEMA = 'superagent.common_private_recovery.v1'
QUARANTINE_SCHEMA = 'superagent.common_private_quarantine.v1'
LIMIT = 32 * 1024 * 1024
ENTRY_LIMIT = 4096
MANIFEST = 'manifest.json'
QUARANTINE_MARKER = 'QUARANTINE.json'
GRANT_FILE = 'agent-external.json'
NETWORK_FILE = 'provider-network.json'
PUBLIC_CONFIG = frozenset((*CONFIGS, *FRESH_FILES, 'instance.json', GRANT_FILE, NETWORK_FILE))
OMITTED_CONFIG = frozenset(('identities.json', '.mixed-maintenance'))
LEDGER_ALIAS = 'ledger/dispatch.sqlite3'
NATIVE_GUARD = 'config/.mixed-maintenance/guard.json'
FIXED = (frozenset(DATABASES) | {f'domain/{name}' for name in DOMAIN_FILES}
         | {f'config/{name}' for name in PUBLIC_CONFIG} | {f'legacy/{name}' for name in LEGACY} | {NATIVE_GUARD})
REQUIRED = frozenset(DATABASES) | {f'domain/{name}' for name in DOMAIN_FILES} | {'config/coordinator.toml'}
TREES = ('cas', 'domain/journal', 'http-journal')
DENIED_FIELDS = frozenset(('apikey', 'authorization', 'bearer', 'credentials', 'deepseekapikey',
                           'accesstoken', 'oauthtoken', 'password', 'privatekey', 'secret', 'token', 'tokens'))


@dataclass(frozen=True)
class RecoveryLayout:
    config_root: Path = Path('/etc/sa-dispatch')
    ledger: Path = Path('/var/lib/sa-dispatch/dispatch.sqlite3')
    artifacts: Path = Path('/var/lib/sa-dispatch/artifacts')
    domain: Path = Path('/var/lib/sa-dispatch-native/domain')
    legacy_bindings: Path = Path('/var/lib/sa-dispatch-manager')
    http_journal: Path = Path('/var/lib/sa-dispatch/journal')


def _require(condition, message):
    if not condition:
        raise MaintenanceError(message)


def _overlap(left, right):
    return left.is_relative_to(right) or right.is_relative_to(left)


def _children(path):
    with os.scandir(path) as directory:
        names = set()
        for entry in directory:
            _require(len(names) < ENTRY_LIMIT, 'recovery entry limit exceeded')
            names.add(entry.name)
    return names


def _private(path, *, directory=False):
    info = _info(path, directory=directory)
    _require(os.name == 'nt' or not info.st_mode & 0o077, 'recovery archive permissions are not private')


def _private_mkdir(path, root):
    folder = root
    for segment in path.relative_to(root).parts:
        folder /= segment
        if not folder.exists():
            folder.mkdir(mode=0o700)
            _sync_dir(folder.parent)


def _write_private(path, raw):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_BINARY', 0)
    with os.fdopen(os.open(path, flags, 0o600), 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    _sync_dir(path.parent)


def _config_payload(alias, raw):
    if not (alias.startswith(('config/', 'legacy/')) or alias in {f'domain/{name}' for name in DOMAIN_FILES}):
        return
    value = tomllib.loads(raw.decode('utf-8')) if alias.endswith('.toml') else json.loads(raw)
    _require(isinstance(value, dict), 'invalid fixed recovery configuration')
    if alias == f'config/{NETWORK_FILE}':
        _require(set(value) == {'schema', 'https_connect_proxy'}
                 and value['schema'] == 'superagent.provider-network.v1'
                 and allowed_connect_proxy(value['https_connect_proxy']), 'invalid private recovery network configuration')
    pending = [value]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            _require(not any(''.join(c for c in name.lower() if c.isalnum()) in DENIED_FIELDS for name in value),
                     'credential-bearing field in fixed recovery configuration')
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)


def _sqlite_payload(alias, path):
    if alias not in DATABASES:
        return None
    _database(path, DATABASES[alias])
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)) as conn:
        conn.execute('PRAGMA trusted_schema=OFF')
        _require(conn.execute('PRAGMA foreign_key_check').fetchone() is None, 'SQLite recovery foreign-key check failed')
        if alias == LEDGER_ALIAS:
            version = verify_schema(conn)
            _require(conn.execute('SELECT id,mode FROM ledger_mode').fetchall() == [(1, 'mixed')],
                     'private recovery requires the common mixed ledger')
            return version
    return None


def _member(alias, directory):
    if alias in (TREES if directory else FIXED):
        return True
    return any(alias.startswith(tree + '/') and _matches_tree(tree, alias.removeprefix(tree + '/'), directory)
               for tree in TREES)


def _ancestor_folders(names):
    return {parent.as_posix() for alias in names for parent in Path(alias).parents if parent != Path('.')}


def _fixed_sources(layout):
    sources = {LEDGER_ALIAS: _lexical(layout.ledger)}
    sources.update({alias: layout.domain / alias.removeprefix('domain/') for alias in FIXED if alias.startswith('domain/')})
    sources.update({f'config/{name}': layout.config_root / name for name in PUBLIC_CONFIG})
    sources[NATIVE_GUARD] = layout.config_root / '.mixed-maintenance/guard.json'
    sources.update({f'legacy/{name}': layout.legacy_bindings / name for name in LEGACY})
    trees = {'cas': layout.artifacts, 'domain/journal': layout.domain / 'journal', 'http-journal': layout.http_journal}
    return sources, trees


def _capture_sources(layout, staging):
    sources, trees = _fixed_sources(layout)
    entries, source_ids = {}, {}
    _info(layout.config_root / 'identities.json', optional=True)
    for root, allowed, optional in (
        (layout.config_root, PUBLIC_CONFIG | OMITTED_CONFIG, False),
        (layout.domain, {'research', 'journal'} | {Path(n).name for n in DATABASES if n.count('/') == 1 and n != LEDGER_ALIAS}
         | {n for n in DOMAIN_FILES if '/' not in n}, False),
        (layout.domain / 'research', {Path(n).name for n in FIXED if n.startswith('domain/research/')}, False),
        (layout.legacy_bindings, set(LEGACY), True),
    ):
        info = _info(root, directory=True, optional=optional)
        if info is not None:
            _require(_children(root) <= allowed, 'unknown file in recovery source directory')
            source_ids[root] = (True, _identity(info))

    total, version = 0, None

    def record(alias, path, info, *, directory=False):
        nonlocal total, version
        _require(len(entries) < ENTRY_LIMIT - 1, 'recovery entry limit exceeded')
        source_ids[path] = (directory, _identity(info))
        target = staging / alias
        if directory:
            entries[alias] = {'kind': 'directory'}
            _private_mkdir(target, staging)
            return
        raw, observed = _read(path, max_bytes=LIMIT - total)
        _require(source_ids[path][1] == observed, 'recovery source changed during capture')
        _config_payload(alias, raw)
        sha = digest(raw)
        _require(not alias.startswith('cas/') or sha == path.name, 'recovery CAS bytes do not match their address')
        total += len(raw)
        entries[alias] = {'kind': 'file', 'sha256': sha, 'bytes': len(raw)}
        _private_mkdir(target.parent, staging)
        checked = _sqlite_payload(alias, path)
        if alias == LEDGER_ALIAS:
            version = checked
        _write_private(target, raw)

    for alias, path in sorted(sources.items()):
        info = _info(path, optional=alias not in REQUIRED)
        if info is not None:
            record(alias, path, info)
    pending = [(alias, path) for alias, path in trees.items()
               if _info(path, directory=True, optional=alias != 'cas') is not None]
    while pending:
        alias, path = pending.pop()
        record(alias, path, _info(path, directory=True), directory=True)
        for name in sorted(_children(path)):
            child, member = path / name, f'{alias}/{name}'
            directory = stat.S_ISDIR(child.lstat().st_mode)
            _require(_member(member, directory), 'unknown file in recovery source tree')
            if directory:
                pending.append((member, child))
            else:
                record(member, child, _info(child))
    for path, (directory, identity) in source_ids.items():
        _require(_identity(_info(path, directory=directory)) == identity, 'recovery source changed before publication')
    _require(_sqlite_payload(LEDGER_ALIAS, layout.ledger) == version, 'source ledger changed during recovery capture')
    return {'schema': SCHEMA, 'policy': 'private_cold_restore_only', 'ledger_version': version,
            'entries': entries, 'file_count': sum(v['kind'] == 'file' for v in entries.values()), 'total_bytes': total}


def _inventory(root):
    pending = [root]
    folders, files = set(), set()
    while pending:
        parent = pending.pop()
        for name in _children(parent):
            path = parent / name
            alias = path.relative_to(root).as_posix()
            directory = stat.S_ISDIR(path.lstat().st_mode)
            _private(path, directory=directory)
            _require(len(files) + len(folders) < ENTRY_LIMIT, 'recovery archive entry limit exceeded')
            if directory:
                folders.add(alias)
                pending.append(path)
            else:
                files.add(alias)
    return folders, files


def _verified(destination, expected_manifest_sha256):
    destination = _lexical(destination)
    _private(destination, directory=True)
    raw, _ = _read(destination / MANIFEST, max_bytes=LIMIT)
    manifest = json.loads(raw)
    _require(isinstance(manifest, dict) and set(manifest) == {
        'schema', 'policy', 'ledger_version', 'entries', 'file_count', 'total_bytes'}, 'invalid recovery manifest')
    _require(canonical(manifest) == raw and manifest['schema'] == SCHEMA
             and manifest['policy'] == 'private_cold_restore_only'
             and manifest['ledger_version'] in ('0012', '0013'), 'invalid recovery manifest identity')
    manifest_sha = digest(raw)
    _require(expected_manifest_sha256 in (None, manifest_sha), 'recovery manifest hash differs from its receipt')
    entries = manifest['entries']
    _require(isinstance(entries, dict) and len(entries) < ENTRY_LIMIT, 'invalid recovery manifest entries')
    folders, files = set(), set()
    for alias, item in entries.items():
        _require(isinstance(item, dict), 'invalid recovery member')
        if item.get('kind') == 'directory':
            _require(set(item) == {'kind'} and _member(alias, True), 'unknown recovery directory')
            folders.add(alias)
        else:
            _require(set(item) == {'kind', 'sha256', 'bytes'} and item['kind'] == 'file' and _member(alias, False)
                     and isinstance(item['sha256'], str) and HEX.fullmatch(item['sha256'])
                     and type(item['bytes']) is int and 0 <= item['bytes'] <= LIMIT, 'invalid recovery file')
            files.add(alias)
    _require(REQUIRED <= files and 'cas' in folders, 'required recovery members are missing')
    _require(_inventory(destination) == (folders | _ancestor_folders(files | folders), files | {MANIFEST}),
             'recovery archive contains missing or unknown members')
    total, version = 0, None
    for alias in sorted(files):
        path = destination / alias
        payload, _ = _read(path, max_bytes=LIMIT - len(raw) - total)
        total += len(payload)
        item = entries[alias]
        _require(digest(payload) == item['sha256'] and len(payload) == item['bytes'], 'recovery file hash differs')
        _require(not alias.startswith('cas/') or item['sha256'] == path.name, 'recovery CAS address differs')
        _config_payload(alias, payload)
        checked = _sqlite_payload(alias, path)
        if alias == LEDGER_ALIAS:
            version = checked
    _require(total == manifest['total_bytes'] and len(files) == manifest['file_count']
             and version == manifest['ledger_version'], 'recovery totals or ledger version differ')
    if version == '0013':
        from dispatch.sa_dispatch.artifacts import ArtifactStore
        from dispatch.sa_dispatch.campaign_validation import campaign_records, validate_campaign_records

        artifacts = ArtifactStore(destination / 'cas')
        def read_artifact(ref):
            payload, _ = _read(artifacts.path(ref), max_bytes=1024 * 1024)
            return payload
        with closing(sqlite3.connect((destination / LEDGER_ALIAS).as_uri() + '?mode=ro&immutable=1',
                                     uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA trusted_schema=OFF')
            validate_campaign_records(campaign_records(conn), read_artifact)
    return {'schema': SCHEMA, 'manifest_sha256': manifest_sha, 'ledger_version': version,
            'total_bytes': total, 'file_count': len(files), 'verified': True}, entries, raw


def verify(destination, *, expected_manifest_sha256=None):
    """Verify a private archive and optionally pin it to a previously trusted receipt."""
    try:
        receipt, _, _ = _verified(destination, expected_manifest_sha256)
        return receipt
    except MaintenanceError:
        raise
    except (DispatchError, OSError, sqlite3.Error, ValueError) as error:
        raise MaintenanceError('private recovery archive failed verification') from error


def capture(layout, destination, *, offline=False):
    """Capture while all native writers and timers are stopped by the caller."""
    _require(offline is True, 'stopped writers and offline maintenance must be explicitly confirmed')
    destination = _lexical(destination)
    roots = tuple(_lexical(getattr(layout, name)) for name in RecoveryLayout.__dataclass_fields__)
    _require(all(not _overlap(destination, root) for root in roots), 'recovery destination overlaps native data')
    try:
        _info(destination.parent, directory=True)
        with directory_lock(layout.ledger.parent), tempfile.TemporaryDirectory(
                prefix='.common-recovery-', dir=destination.parent) as temporary:
            staging = Path(temporary) / 'archive'
            staging.mkdir(mode=0o700)
            manifest = _capture_sources(layout, staging)
            raw = canonical(manifest)
            _require(len(raw) + manifest['total_bytes'] <= LIMIT, 'recovery byte limit exceeded')
            _write_private(staging / MANIFEST, raw)
            receipt = verify(staging)
            if _info(destination, directory=True, optional=True) is not None:
                return verify(destination, expected_manifest_sha256=receipt['manifest_sha256'])
            staging.rename(destination)
            _sync_dir(destination.parent)
        return receipt
    except MaintenanceError:
        raise
    except (DispatchError, OSError, sqlite3.Error, ValueError) as error:
        raise MaintenanceError('could not capture the common private recovery archive') from error


def restore_to_quarantine(archive, new_empty_dir, *, expected_manifest_sha256=None):
    """Restore frozen bytes into a private staging tree with no activation step."""
    archive, destination = _lexical(archive), _lexical(new_empty_dir)
    system = RecoveryLayout()
    protected = (*vars(system).values(), system.ledger.parent, system.domain.parent,
                 Path('/etc/systemd'), Path('/opt/sa-dispatch'), Path('/opt/sa-dispatch-native'))
    _require(not any(_overlap(destination, path) for path in (archive, *protected)),
             'quarantine path overlaps archive or production data')
    try:
        _info(destination.parent, directory=True)
        if _info(destination, directory=True, optional=True) is not None:
            _require(not _children(destination), 'quarantine destination must be new or empty')
        receipt, entries, raw = _verified(archive, expected_manifest_sha256)
        with tempfile.TemporaryDirectory(prefix='.common-restore-', dir=destination.parent) as temporary:
            staging = Path(temporary) / 'quarantine'
            staging.mkdir(mode=0o700)
            for alias, item in sorted(entries.items()):
                target = staging / alias
                if item['kind'] == 'directory':
                    _private_mkdir(target, staging)
                else:
                    _private_mkdir(target.parent, staging)
                    payload, _ = _read(archive / alias, max_bytes=item['bytes'])
                    _require(digest(payload) == item['sha256'], 'archive changed during quarantine restore')
                    _write_private(target, payload)
            _write_private(staging / MANIFEST, raw)
            verify(staging, expected_manifest_sha256=receipt['manifest_sha256'])
            marker = {'schema': QUARANTINE_SCHEMA, 'manifest_sha256': receipt['manifest_sha256'],
                      'ledger_version': receipt['ledger_version'], 'credential_injection': False,
                      'timer_enabled': False, 'external_execution_enabled': False, 'services_started': False}
            _write_private(staging / QUARANTINE_MARKER, canonical(marker))
            if destination.exists():
                destination.rmdir()
            staging.rename(destination)
            _sync_dir(destination.parent)
        return {**marker, 'restore_verified': True}
    except MaintenanceError:
        raise
    except (DispatchError, OSError, sqlite3.Error, ValueError) as error:
        raise MaintenanceError('could not verify the quarantined common restore') from error
