"""Bounded offline evidence for the fixed native mixed-route installation.

The native adapter holds its maintenance lock and proves every writer quiescent
before calling capture. Verification reads the frozen archive, including raw
SQLite images, so later activation can legitimately change the live sources.
"""
from contextlib import closing
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import tomllib

from .maintenance import MaintenanceError, _sync_dir, canonical, digest
from .fresh_install import FRESH_FILES, FRESH_MODE, FRESH_NAME


LIMIT = 16 * 1024 * 1024
ENTRY_LIMIT = 2048
MANIFEST = 'manifest.json'
PENDING = '.pending'
CONFIGS = ('coordinator.toml', 'local.toml', 'manager-binding.json',
           'collector-binding.json', 'native-guard.json', 'http-workers.json')
LEGACY = ('manager-binding.json', 'collector-binding.json')
DOMAIN_FILES = ('stage-manifest.json', 'local.toml', 'research-binding.json',
                'workflow-binding.json', 'research/catalog.json')
DATABASES = {
    'ledger/dispatch.sqlite3': ('tasks', 'leases', 'events', 'late_results'),
    'domain/content.sqlite3': ('artifacts', 'heads', 'operation_receipts'),
    'domain/research/catalog.sqlite3': ('catalog_meta', 'contexts', 'workflow_operation_receipts'),
    'domain/research/research-scatter.sqlite3': ('records', 'record_versions', 'source_meta'),
    'domain/research/research-content.sqlite3': ('records', 'record_versions', 'source_meta'),
    'domain/research/signalhub.sqlite3': ('source_meta', 'reference_works'),
}
SIDECARS = ('-wal', '-shm', '-journal')
JOURNAL_NAME = re.compile(r'l_[0-9a-f]{32}\.jsonl')
HEX = re.compile(r'[0-9a-f]{64}')
SHARD = re.compile(r'[0-9a-f]{2}')


def _require(condition, message):
    if not condition:
        raise MaintenanceError(message)


def _lexical(path):
    path = Path(path)
    _require(path.is_absolute() and path != Path(path.anchor) and '..' not in path.parts
             and not str(path).startswith(('\\\\', '//')), 'invalid recovery path')
    if path.drive:
        _require(not any(':' in part or part.endswith(('.', ' ')) or Path(part).is_reserved()
                         for part in path.parts[1:]), 'invalid recovery path alias')
    return path


def _info(path, *, directory=False, optional=False):
    path = _lexical(path)
    for part in (*reversed(path.parents), path):
        try:
            info = part.lstat()
        except FileNotFoundError:
            if optional:
                return None
            raise MaintenanceError(f'missing recovery path: {path}') from None
        _require(not getattr(info, 'st_reparse_tag', 0) and
                 not getattr(info, 'st_file_attributes', 0) & 0x400,
                 f'reparse recovery path refused: {part}')
        if part != path or directory:
            _require(stat.S_ISDIR(info.st_mode), f'expected unlinked directory: {part}')
        else:
            _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
                     f'expected unlinked regular file: {part}')
    return info


def _identity(info):
    return {'device': info.st_dev, 'inode': info.st_ino, 'mode': stat.S_IMODE(info.st_mode),
            'uid': info.st_uid, 'gid': info.st_gid, 'size': info.st_size,
            'mtime_ns': info.st_mtime_ns, 'ctime_ns': info.st_ctime_ns}


def _read(path, *, max_bytes=LIMIT):
    info = _info(path)
    _require(info.st_size <= max_bytes, 'recovery byte limit exceeded')
    flags = os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0)
    with os.fdopen(os.open(path, flags), 'rb') as stream:
        _require(_identity(os.fstat(stream.fileno())) == _identity(info), 'recovery file changed before read')
        raw = stream.read(max_bytes + 1)
        _require(len(raw) == info.st_size and _identity(os.fstat(stream.fileno())) == _identity(info),
                 'recovery file changed during read')
    _require(_identity(_info(path)) == _identity(info), 'recovery path changed during read')
    return raw, _identity(info)


def _database(path, tables):
    _info(path)
    for suffix in SIDECARS:
        _require(_info(path.with_name(path.name + suffix), optional=True) is None,
                 f'SQLite sidecar blocks offline copy: {path.name}{suffix}')
    try:
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
            db.execute('PRAGMA trusted_schema=OFF')
            objects = db.execute('SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name').fetchall()
            _require(set(tables) <= {item[1] for item in objects if item[0] == 'table'},
                     f'wrong SQLite schema: {path.name}')
            _require(db.execute('PRAGMA integrity_check').fetchone() == ('ok',),
                     f'SQLite integrity check failed: {path.name}')
            pragmas = [db.execute('PRAGMA ' + key).fetchone()[0]
                       for key in ('application_id', 'user_version', 'encoding')]
    except sqlite3.Error as exc:
        raise MaintenanceError(f'invalid SQLite image: {path.name}') from exc
    return digest(canonical([pragmas, objects]))


def _children(path, *, optional=False):
    if _info(path, directory=True, optional=optional) is None:
        return set()
    names = set()
    with os.scandir(path) as entries:
        for entry in entries:
            names.add(entry.name)
            _require(len(names) <= ENTRY_LIMIT, 'recovery entry limit exceeded')
    return names


def _fixed(layout, plan, destination):
    root, domain = _lexical(layout.config_root), _lexical(layout.domain)
    operation = plan['operation_id']
    _require(isinstance(operation, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', operation)
             and plan['native_root'] == str(root)
             and _lexical(destination) == root / '.mixed-maintenance' / operation / 'recovery',
             'recovery destination does not match the fixed operation')
    bindings = _lexical(layout.legacy_bindings)
    ledger = _lexical(layout.ledger)
    system = root.parent / 'systemd' / 'system'
    unit = system / 'sa-dispatch.service'
    dropins = system / 'sa-dispatch.service.d'
    services = tuple(map(_lexical, layout.service_files))
    _require(unit in services and len(services) == len(set(services)) and
             all(p == unit or (p.parent == dropins and
                 re.fullmatch(r'[A-Za-z0-9-]+\.conf', p.name)) for p in services),
             'invalid approved native service files')
    fixed = {'ledger/dispatch.sqlite3': (ledger, True)}
    fixed.update({alias: (domain / alias.removeprefix('domain/'), True)
                  for alias in DATABASES if alias.startswith('domain/')})
    fixed.update({f'domain/{name}': (domain / name, True) for name in DOMAIN_FILES})
    fixed.update({f'config/{name}': (root / name, name == 'coordinator.toml') for name in CONFIGS})
    fixed.update({f'config/{name}': (root / name, True) for name in _fresh_authority(plan)})
    fixed.update({f'legacy/{name}': (bindings / name, False) for name in LEGACY})
    fixed.update({f'service/{path.relative_to(system).as_posix()}': (path, path == unit)
                  for path in services})
    trees = {'cas': (_lexical(layout.artifacts), False), 'domain/journal': (domain / 'journal', False),
             'http-journal': (_lexical(layout.http_journal), True)}
    sources = {path for path, _ in fixed.values()} | {path for path, _ in trees.values()}
    identity = root / 'identities.json'
    _require(len(sources) == len(fixed) + len(trees) and identity not in sources
             and all(not destination.is_relative_to(path) and not path.is_relative_to(destination)
                     for path in sources), 'overlapping recovery paths')
    return fixed, trees, identity, dropins


def _authority(plan):
    configs = {}
    for item in plan['configs']:
        name, before = item['path'], item['before_sha256']
        _require(name in CONFIGS and name not in configs
                 and (before is None or isinstance(before, str) and HEX.fullmatch(before)),
                 'invalid frozen native config baseline')
        configs[name] = before
    return configs


def _fresh_authority(plan):
    anchor = plan['anchor']
    if not {'collector_mode', 'fresh_install_sha256', 'fresh_files'} & anchor.keys():
        return {}
    files = anchor.get('fresh_files')
    _require(anchor.get('collector_mode') == FRESH_MODE and isinstance(files, dict) and
             set(files) == set(FRESH_FILES) and
             all(isinstance(sha, str) and HEX.fullmatch(sha) for sha in files.values()) and
             files[FRESH_NAME] == anchor.get('fresh_install_sha256'), 'invalid fresh recovery authority')
    return files


def _service(raw):
    # Known systemd paths may reference the identity file by name. Inline
    # credentials, environment files and unexpected environment values refuse.
    for line in raw.decode('utf-8').splitlines():
        key, _, value = line.strip().partition('=')
        key, value = key.strip(), value.strip()
        if key == 'Environment':
            _require(re.fullmatch(r'"?PYTHONPATH=/[A-Za-z0-9_./:-]+"?', value) is not None,
                     'unapproved native service environment')
        _require(key not in {'EnvironmentFile', 'SetCredential', 'SetCredentialEncrypted',
                             'LoadCredential', 'LoadCredentialEncrypted', 'PassEnvironment'},
                 'unapproved native service credential directive')


def _matches_tree(tree, rel, directory):
    parts = rel.split('/')
    if tree == 'cas':
        if directory:
            return parts == ['sha256'] or (len(parts) == 2 and parts[0] == 'sha256' and SHARD.fullmatch(parts[1]))
        return (len(parts) == 3 and parts[0] == 'sha256' and HEX.fullmatch(parts[2])
                and parts[1] == parts[2][:2])
    return not directory and len(parts) == 1 and JOURNAL_NAME.fullmatch(rel)


def _walk_source(tree, path, *, optional):
    root_info = _info(path, directory=True, optional=optional)
    if root_info is None:
        return
    yield tree, path, root_info
    pending = [(tree, path)]
    while pending:
        prefix, parent = pending.pop()
        for name in sorted(_children(parent)):
            child, alias = parent / name, f'{prefix}/{name}'
            info = child.lstat()
            folder = stat.S_ISDIR(info.st_mode)
            _require(_matches_tree(tree, alias.removeprefix(tree + '/'), folder),
                     f'unknown native recovery tree entry: {alias}')
            yield alias, child, _info(child, directory=folder)
            if folder:
                pending.append((alias, child))


def _source_snapshot(layout, plan, destination):
    fixed, trees, identity, dropins = _fixed(layout, plan, destination)
    configs = _authority(plan)
    domain = layout.domain
    for parent, expected in (
        (domain, {'research', 'journal'} | {Path(alias).name for alias in fixed
                                            if alias.startswith('domain/') and alias.count('/') == 1}),
        (domain / 'research', {Path(alias).name for alias in fixed if alias.startswith('domain/research/')}),
        (layout.legacy_bindings, set(LEGACY)),
        (dropins, {path.name for path, _ in fixed.values() if path.parent == dropins}),
        (layout.config_root, {*CONFIGS, *_fresh_authority(plan), 'identities.json', '.mixed-maintenance'}),
    ):
        actual = _children(parent, optional=parent in (dropins, layout.legacy_bindings))
        _require(not actual - expected, f'unknown entry in fixed native recovery directory: {parent}')
    entries, payloads, total = {}, {}, 0

    def record(alias, path, info, *, metadata_only=False):
        nonlocal total
        _require(len(entries) < ENTRY_LIMIT, 'recovery entry limit exceeded')
        entry = {'source': str(path), 'kind': 'absent'}
        entries[alias] = entry
        if info is None:
            return
        entry.update(kind='metadata' if metadata_only else 'directory' if stat.S_ISDIR(info.st_mode) else 'file',
                     metadata=_identity(info))
        if entry['kind'] != 'file':
            return
        raw, observed = _read(path, max_bytes=LIMIT - total)
        _require(entry['metadata'] == observed, 'native source identity changed during capture')
        total += len(raw)
        entry['sha256'] = digest(raw)
        if alias in DATABASES:
            entry['schema_sha256'] = _database(path, DATABASES[alias])
            _require(_identity(_info(path)) == observed, 'SQLite identity changed during inspection')
        if alias.startswith('cas/'):
            _require(entry['sha256'] == path.name, 'native CAS blob does not match its address')
        if alias.startswith('service/'):
            _service(raw)
        payloads[alias] = raw

    for alias, (path, required) in sorted(fixed.items()):
        record(alias, path, _info(path, optional=not required))
    record('config/identities.json', identity, _info(identity), metadata_only=True)
    for tree, (path, optional) in trees.items():
        items = iter(_walk_source(tree, path, optional=optional))
        first = next(items, None)
        if first is None:
            record(tree, path, None)
            continue
        record(*first)
        for item in items:
            record(*item)

    http = tomllib.loads(payloads['config/coordinator.toml'].decode('utf-8'))
    _require(Path(http['db_path']) == layout.ledger and Path(http['artifact_root']) == layout.artifacts
             and Path(http['journal_dir']) == layout.http_journal, 'native HTTP data aliases differ')
    for name, before in configs.items():
        _require(entries[f'config/{name}'].get('sha256') == before, 'native config differs from frozen baseline')
    for name, sha in _fresh_authority(plan).items():
        _require(entries[f'config/{name}'].get('sha256') == sha, 'fresh recovery evidence differs from frozen baseline')
    manifest = {'version': 1, 'plan_sha256': digest(canonical(plan)), 'entries': entries,
                'file_count': len(payloads), 'database_count': len(DATABASES), 'total_bytes': total}
    return manifest, payloads


def _archive_inventory(destination):
    folders, files = set(), set()
    pending = [('', destination)]
    while pending:
        prefix, parent = pending.pop()
        for name in _children(parent):
            child = parent / name
            alias = f'{prefix}/{name}' if prefix else name
            _require(len(files) + len(folders) < ENTRY_LIMIT * 2, 'recovery archive entry limit exceeded')
            if stat.S_ISDIR(child.lstat().st_mode):
                _info(child, directory=True)
                folders.add(alias)
                pending.append((alias, child))
            else:
                _info(child)
                files.add(alias)
    return files, folders


def _publish(path, raw):
    """Publish under the coordinator's exclusive operation lock."""
    pending = path.with_name(path.name + PENDING)
    if _info(pending, optional=True) is None:
        fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_BINARY', 0), 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    _require(_read(pending)[0] == raw and _info(path, optional=True) is None,
             'pending recovery bytes differ or target appeared')
    os.replace(pending, path)
    _sync_dir(path.parent)


def _archive_layout(entries):
    files = {alias for alias, entry in entries.items() if entry['kind'] == 'file'}
    folders = {alias for alias, entry in entries.items() if entry['kind'] == 'directory'}
    for alias in files | folders:
        folders.update(parent.as_posix() for parent in Path(alias).parents if parent != Path('.'))
    return files, folders


def _receipt(manifest):
    return {'version': 1, 'manifest_sha256': digest(canonical(manifest)),
            **{key: manifest[key] for key in ('file_count', 'database_count', 'total_bytes')}}


def capture(layout, plan, destination):
    """Capture or finish the identical frozen source set while native writers are stopped."""
    destination = _lexical(destination)
    manifest, payloads = _source_snapshot(layout, plan, destination)
    raw = canonical(manifest)
    _require(len(raw) + manifest['total_bytes'] <= LIMIT, 'recovery byte limit exceeded')
    _info(destination.parent, directory=True)
    _info(destination, directory=True, optional=True)
    destination.mkdir(mode=0o700, exist_ok=True)
    _sync_dir(destination.parent)
    existing, old_folders = _archive_inventory(destination)
    file_names, folders = _archive_layout(manifest['entries'])
    if MANIFEST not in existing:
        _require(not old_folders and existing <= {MANIFEST + PENDING}, 'existing recovery has no frozen manifest')
        _publish(destination / MANIFEST, raw)
        existing = {MANIFEST}
    _require(_read(destination / MANIFEST)[0] == raw, 'native source set changed since capture began')
    allowed_pending = {alias + PENDING for alias in file_names - existing}
    _require(existing <= file_names | allowed_pending | {MANIFEST} and old_folders <= folders,
             'unknown file in partial native recovery')
    for alias in existing - {MANIFEST}:
        expected = payloads[alias.removesuffix(PENDING)]
        _require(_read(destination / alias, max_bytes=len(expected))[0] == expected,
                 'existing recovery file differs from frozen source')
    for alias in sorted(folders, key=lambda name: (name.count('/'), name)):
        if alias not in old_folders:
            target = destination / alias
            target.mkdir(mode=0o700)
            _sync_dir(target.parent)
    for alias in sorted(file_names - existing):
        _publish(destination / alias, payloads[alias])
    receipt = _receipt(manifest)
    verify(layout, plan, destination, receipt)
    return receipt


def verify(layout, plan, destination, receipt):
    """Verify only the frozen recovery set and its metadata against its receipt."""
    destination = _lexical(destination)
    fixed, trees, identity, _ = _fixed(layout, plan, destination)
    raw, _ = _read(destination / MANIFEST)
    try:
        manifest = json.loads(raw)
        entries = manifest['entries']
        _require(canonical(manifest) == raw and manifest['version'] == 1
                 and manifest['plan_sha256'] == digest(canonical(plan))
                 and _receipt(manifest) == receipt, 'native recovery manifest or receipt differs')
        _require(len(entries) <= ENTRY_LIMIT, 'recovery entry limit exceeded')
        for alias, (source, required) in fixed.items():
            entry = entries[alias]
            _require(entry['source'] == str(source) and entry['kind'] in
                     ({'file'} if required else {'file', 'absent'}), 'fixed recovery source differs')
        credential = entries['config/identities.json']
        _require(set(credential) == {'source', 'kind', 'metadata'} and credential['source'] == str(identity)
                 and credential['kind'] == 'metadata', 'credential metadata differs')
        for name, before in _authority(plan).items():
            _require(entries[f'config/{name}'].get('sha256') == before, 'frozen config baseline differs')
        for name, sha in _fresh_authority(plan).items():
            _require(entries[f'config/{name}'].get('sha256') == sha, 'frozen fresh recovery evidence differs')
        moving = set(entries) - set(fixed) - {'config/identities.json'}
        for tree, (source, optional) in trees.items():
            entry = entries[tree]
            _require(entry['source'] == str(source) and entry['kind'] in
                     ({'directory', 'absent'} if optional else {'directory'}), 'recovery tree root differs')
            moving.remove(tree)
            for alias in [alias for alias in moving if alias.startswith(tree + '/')]:
                item = entries[alias]
                rel = alias.removeprefix(tree + '/')
                _require(entry['kind'] == 'directory' and item['kind'] in {'directory', 'file'}
                         and _matches_tree(tree, rel, item['kind'] == 'directory')
                         and item['source'] == str(source / rel)
                         and entries[Path(alias).parent.as_posix()]['kind'] == 'directory',
                         'invalid native recovery tree member')
                moving.remove(alias)
        _require(not moving, 'unknown source in recovery manifest')
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        if isinstance(exc, MaintenanceError):
            raise
        raise MaintenanceError('invalid native recovery manifest') from exc

    files, folders = _archive_layout(entries)
    _require(_archive_inventory(destination) == (files | {MANIFEST}, folders),
             'native recovery archive contents differ')
    total = 0
    for alias in sorted(files):
        path = destination / alias
        payload, _ = _read(path, max_bytes=LIMIT - len(raw) - total)
        total += len(payload)
        _require(digest(payload) == entries[alias].get('sha256'), 'native recovery file hash differs')
        if alias in DATABASES:
            _require(_database(path, DATABASES[alias]) == entries[alias].get('schema_sha256'),
                     'native recovery schema hash differs')
        if alias.startswith('cas/'):
            _require(digest(payload) == path.name, 'native recovery CAS address differs')
    _require(total == manifest['total_bytes'] and len(files) == manifest['file_count']
             and manifest['database_count'] == len(DATABASES), 'native recovery totals differ')
