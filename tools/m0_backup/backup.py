"""Synthetic-only SQLite backup API snapshots; preflight precedes all target writes."""
from __future__ import annotations
import argparse
import base64
from contextlib import closing, contextmanager
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import sqlite3
import stat
import subprocess
import sys
import uuid
import zipfile

MAX_BYTES = 200 * 1024 * 1024
MAX_EVIDENCE = 4 * 1024 * 1024
SCHEMA = 'm0.synthetic_backup.v1'
APPLICATION_ID = 0x4D305359
DENIED = re.compile(r'cookie|credential|password|secret|token|session|(^|[._-])auth([._-]|$)|id_rsa|id_ed25519|^\.env|^\.ssh|^\.aws|^\.codex', re.I)
KINDS = {'dispatch', 'research-scatter', 'research-content', 'evidence'}


class BackupError(Exception):
    def __init__(self, code, message, *, state='rejected', partial=None):
        super().__init__(message)
        self.code, self.state, self.partial = code, state, partial


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def sha(data): return hashlib.sha256(data).hexdigest()
def now(): return dt.datetime.now(dt.timezone.utc).isoformat()
def fail(code, message): raise BackupError(code, message)


def relative_name(value):
    if not isinstance(value, str) or not value or '\\' in value or ':' in value or '\x00' in value:
        fail('E_PATH', 'relative POSIX file path required')
    path = PurePosixPath(value)
    if path.is_absolute() or any(p in ('', '.', '..') for p in value.split('/')):
        fail('E_PATH', 'path traversal or noncanonical path denied')
    if any(DENIED.search(part) for part in path.parts):
        fail('E_DENYLIST', 'credential/session path denied')
    return path


def safe_stat(path, *, missing=False):
    path = Path(os.path.abspath(path))
    parts = list(reversed([path, *path.parents]))
    last = None
    for part in parts:
        try:
            last = os.lstat(part)
        except FileNotFoundError:
            if missing:
                return None
            raise BackupError('E_MISSING', 'explicit path is missing') from None
        if stat.S_ISLNK(last.st_mode) or getattr(last, 'st_file_attributes', 0) & 0x400:
            fail('E_LINK', 'symlink/junction/reparse paths are not followed')
    return last


def regular_bytes(path, limit=MAX_BYTES):
    before = safe_stat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
        fail('E_SIZE', 'regular file exceeds the bounded input limit')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            fail('E_CHANGED', 'source identity changed before read')
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    if len(data) > limit or (opened.st_size, opened.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        fail('E_CHANGED', 'source changed while reading')
    return data


@contextmanager
def directory_handle(path, *, create=False):
    """Walk directories without following links; keep ancestry pinned for writes.

    Windows handles deny delete sharing, preventing checked ancestors from being
    renamed/replaced while held. POSIX operations use the returned directory fd.
    """
    path = Path(os.path.abspath(path))
    handles = []
    if os.name == 'nt':
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        class Info(ctypes.Structure):
            _fields_ = [('attributes', wintypes.DWORD), ('created', wintypes.FILETIME),
                        ('accessed', wintypes.FILETIME), ('written', wintypes.FILETIME),
                        ('volume', wintypes.DWORD), ('size_high', wintypes.DWORD),
                        ('size_low', wintypes.DWORD), ('links', wintypes.DWORD),
                        ('index_high', wintypes.DWORD), ('index_low', wintypes.DWORD)]
        kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                      wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
        kernel.CreateFileW.restype = wintypes.HANDLE
        kernel.GetFileInformationByHandle.argtypes = [wintypes.HANDLE, ctypes.POINTER(Info)]
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        invalid = ctypes.c_void_p(-1).value
        try:
            current = Path(path.anchor)
            for part in (None, *path.parts[1:]):
                if part is not None:
                    current /= part
                handle = kernel.CreateFileW(str(current), 0x80, 0x1 | 0x2, None, 3,
                                            0x02000000 | 0x00200000, None)
                if handle == invalid and create and ctypes.get_last_error() in (2, 3):
                    # Parent handles already prevent ancestor substitution.
                    try:
                        os.mkdir(current)
                    except FileExistsError:
                        pass
                    handle = kernel.CreateFileW(str(current), 0x80, 0x1 | 0x2, None, 3,
                                                0x02000000 | 0x00200000, None)
                if handle == invalid:
                    fail('E_PATH_CHANGED', 'cannot pin the target directory ancestry')
                handles.append(handle)
                info = Info()
                if not kernel.GetFileInformationByHandle(handle, ctypes.byref(info)):
                    fail('E_PATH_CHANGED', 'cannot verify directory handle')
                if info.attributes & 0x400 or not info.attributes & 0x10:
                    fail('E_LINK', 'target ancestry contains a reparse point or nondirectory')
            yield None
        finally:
            for handle in reversed(handles):
                kernel.CloseHandle(handle)
    else:
        flags = os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0) | getattr(os, 'O_NOFOLLOW', 0)
        try:
            try:
                handles.append(os.open(path.anchor, flags))
                for part in path.parts[1:]:
                    try:
                        handle = os.open(part, flags, dir_fd=handles[-1])
                    except FileNotFoundError:
                        if not create:
                            raise
                        try:
                            os.mkdir(part, dir_fd=handles[-1])
                        except FileExistsError:
                            pass
                        handle = os.open(part, flags, dir_fd=handles[-1])
                    handles.append(handle)
                    if not stat.S_ISDIR(os.fstat(handle).st_mode):
                        fail('E_LINK', 'target ancestry is not a directory')
            except OSError as exc:
                raise BackupError('E_PATH_CHANGED', 'cannot pin target directory ancestry') from exc
            yield handles[-1]
        finally:
            for handle in reversed(handles):
                os.close(handle)


def secure_mkdir(path):
    # Every missing layer is created only beneath an already verified handle.
    with directory_handle(path, create=True):
        pass


def publish_directory(source, destination):
    """Rename within one verified parent; never traverse either leaf as a link."""
    source, destination = Path(source), Path(destination)
    if source.parent != destination.parent:
        fail('E_WRITE_SCOPE', 'directory publication must stay within one parent')
    with directory_handle(source.parent) as parent_fd:
        options = {} if parent_fd is None else {'dir_fd': parent_fd}
        source_arg = source if parent_fd is None else source.name
        target_arg = destination if parent_fd is None else destination.name
        before = os.stat(source_arg, follow_symlinks=False, **options)
        if not stat.S_ISDIR(before.st_mode) or getattr(before, 'st_file_attributes', 0) & 0x400:
            fail('E_LINK', 'publication source is not a plain directory')
        try:
            os.stat(target_arg, follow_symlinks=False, **options)
        except FileNotFoundError:
            pass
        else:
            fail('E_EXISTS', 'publication destination already exists')
        rename_options = {} if parent_fd is None else {'src_dir_fd': parent_fd, 'dst_dir_fd': parent_fd}
        os.rename(source_arg, target_arg, **rename_options)
        after = os.stat(target_arg, follow_symlinks=False, **options)
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            fail('E_PATH_CHANGED', 'published directory identity changed')


def tree_bytes(root):
    """Quota includes existing history/partials. Links fail closed, never followed."""
    root = Path(root)
    if not stat.S_ISDIR(safe_stat(root).st_mode):
        fail('E_TARGET', 'target root must already be a directory')
    total, stack = 0, [root]
    while stack:
        parent = stack.pop()
        for child in parent.iterdir():
            info = safe_stat(child)
            if stat.S_ISDIR(info.st_mode):
                stack.append(child)
            elif stat.S_ISREG(info.st_mode):
                total += info.st_size
            else:
                fail('E_TARGET', 'unsupported object in quota root')
    return total


@contextmanager
def target_lock(root):
    """Serialize this tool's publishers without writing a lock file outside scope."""
    if os.name == 'nt':
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.CreateMutexW.argtypes = [wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR]
        kernel.CreateMutexW.restype = wintypes.HANDLE
        kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel.ReleaseMutex.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        name = 'Local\\M0SyntheticBackup-' + sha(os.path.normcase(os.path.abspath(root)).encode())
        handle = kernel.CreateMutexW(None, False, name)
        if not handle: fail('E_LOCK', 'cannot create target mutex')
        acquired = False
        try:
            wait = kernel.WaitForSingleObject(handle, 10000)
            acquired = wait in (0, 0x80)
            if not acquired: fail('E_LOCK', 'another backup holds this target')
            yield
        finally:
            if acquired: kernel.ReleaseMutex(handle)
            kernel.CloseHandle(handle)
    else:
        import fcntl
        fd = os.open(root, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0) | getattr(os, 'O_NOFOLLOW', 0))
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fail('E_LOCK', 'another operation holds this target')
            yield
        finally:
            os.close(fd)


def _nearest(path):
    path = Path(path)
    while not path.exists():
        path = path.parent
    safe_stat(path)
    return path


def device_identities(source_root, target_root, linux_devices=None):
    paths = {'source': Path(source_root), 'target': Path(target_root)}
    if os.name == 'nt':
        letters = {}
        for name, path in paths.items():
            drive = os.path.splitdrive(str(path))[0]
            if not re.fullmatch('[A-Za-z]:', drive):
                fail('E_DEVICE', 'local drive-letter volumes required')
            letters[name] = drive[0].upper()
        env = dict(os.environ, M0_DRIVES=''.join(sorted(set(letters.values()))))
        script = "$rows=@($env:M0_DRIVES.ToCharArray() | ForEach-Object { Get-Partition -DriveLetter $_ -ErrorAction Stop | Select-Object DriveLetter,DiskNumber,PartitionNumber }); ConvertTo-Json -InputObject $rows -Compress"
        pwsh = Path(os.environ.get('M0_BACKUP_PWSH', ''))
        if not pwsh.is_absolute() or pwsh.name.casefold() != 'pwsh.exe' or not pwsh.is_file():
            fail('E_DEVICE', 'set M0_BACKUP_PWSH to a trusted absolute pwsh.exe path')
        result = subprocess.run([str(pwsh), '-NoProfile', '-NonInteractive', '-Command', script], env=env,
            capture_output=True, text=True, check=True, creationflags=subprocess.CREATE_NO_WINDOW)
        partitions = json.loads(result.stdout.lstrip('\ufeff'))
        indexed = {r['DriveLetter']: r for r in partitions}
        devices = {name: {'volume': letters[name], 'physical_disk': indexed[letters[name]]['DiskNumber'],
                         'partition': indexed[letters[name]]['PartitionNumber'], 'st_dev': os.stat(_nearest(path)).st_dev}
                   for name, path in paths.items()}
        if devices['source']['physical_disk'] == devices['target']['physical_disk']:
            fail('E_SAME_DISK', 'source and backup target are on the same physical disk')
        return {'method': 'Get-Partition', 'measured_at': now(), 'devices': devices, 'physical_separation_verified': True}
    if not isinstance(linux_devices, dict) or set(linux_devices) != {'source', 'target'}:
        fail('E_DEVICE', 'Linux requires explicit source and target device IDs and expected st_dev')
    devices = {}
    for name, path in paths.items():
        measured = os.stat(_nearest(path)).st_dev
        supplied = linux_devices[name]
        if type(supplied.get('st_dev')) is not int or measured != supplied['st_dev'] or not supplied.get('device_id'):
            fail('E_DEVICE', 'declared Linux device identity does not match measured st_dev')
        devices[name] = {'st_dev': measured, 'major': os.major(measured), 'minor': os.minor(measured),
                         'device_id': supplied['device_id']}
    if devices['source']['st_dev'] == devices['target']['st_dev'] or devices['source']['device_id'] == devices['target']['device_id']:
        fail('E_SAME_DISK', 'Linux source and target device numbers must differ')
    return {'method': 'explicit_device_ids_plus_stat', 'measured_at': now(), 'devices': devices,
            'physical_separation_verified': False,
            'limitation': 'different st_dev verifies filesystem separation, not independent underlying physical disks'}


def sql_name(name): return '"' + name.replace('"', '""') + '"'


def _cell(value):
    if isinstance(value, bytes): return {'sqlite_blob_base64': base64.b64encode(value).decode('ascii')}
    return value


def db_checks(conn, kind):
    if kind not in KINDS - {'evidence'}:
        fail('E_SCOPE', 'unsupported database role')
    conn.execute('PRAGMA query_only=ON')
    conn.execute('PRAGMA trusted_schema=OFF')
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    if conn.execute('PRAGMA application_id').fetchone()[0] != APPLICATION_ID or any(DENIED.search(t) for t in tables):
        fail('E_SCOPE', 'database lacks synthetic scope marker or has denied tables')
    if kind == 'dispatch':
        if not {'tasks', 'evidence'} <= tables:
            fail('E_SCOPE', 'dispatch schema is incomplete')
        if conn.execute("SELECT 1 FROM tasks WHERE access IS NOT 'synthetic' LIMIT 1").fetchone() or conn.execute("SELECT 1 FROM evidence WHERE access IS NOT 'synthetic' LIMIT 1").fetchone():
            fail('E_SCOPE', 'non-synthetic dispatch rows denied')
    else:
        if 'records' not in tables:
            fail('E_SCOPE', 'research records missing')
        if conn.execute("SELECT 1 FROM records WHERE domain IS NOT ? OR license IS NOT 'synthetic_fixture' OR class IS NULL OR class NOT IN('public','internal') LIMIT 1", (kind,)).fetchone():
            fail('E_SCOPE', 'non-synthetic or restricted research rows denied')
    if conn.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
        fail('E_SQLITE', 'SQLite integrity check failed')
    result = {}
    for table in sorted(tables):
        cursor = conn.execute('SELECT * FROM ' + sql_name(table))
        rows = [[_cell(cell) for cell in row] for row in cursor]
        rows.sort(key=canonical)
        result[table] = {'rows': len(rows), 'sha256': sha(canonical({'columns': [c[0] for c in cursor.description], 'rows': rows}))}
    return {'integrity': 'ok', 'tables': result, 'query_sha256': sha(canonical(result))}


def sqlite_snapshot(path, kind):
    before = safe_stat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_BYTES:
        fail('E_SIZE', 'SQLite source exceeds limit')
    for suffix in ('-wal', '-shm', '-journal'):
        safe_stat(str(path) + suffix, missing=True)
    with closing(sqlite3.connect(Path(path).as_uri() + '?mode=ro', uri=True)) as source, closing(sqlite3.connect(':memory:')) as target:
        source.execute('BEGIN')
        checks = db_checks(source, kind)
        size = source.execute('PRAGMA page_count').fetchone()[0] * source.execute('PRAGMA page_size').fetchone()[0]
        if size > MAX_BYTES:
            fail('E_SIZE', 'SQLite snapshot exceeds limit')
        source.backup(target)
        if db_checks(target, kind) != checks:
            fail('E_SQLITE', 'backup API snapshot differs from source read transaction')
        data = target.serialize()
        # SQLite documents this normalization for deserialize(): a backup copied
        # from WAL can retain header 2/2 even though all committed pages are now
        # in this standalone image. Never alter the source database or its WAL.
        # https://www.sqlite.org/c3ref/deserialize.html
        data = data[:18] + b'\x01\x01' + data[20:]
        with closing(sqlite3.connect(':memory:')) as verifier:
            verifier.deserialize(data)
            if db_checks(verifier, kind) != checks:
                fail('E_SQLITE', 'standalone serialized snapshot differs from source')
    after = safe_stat(path)
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        fail('E_CHANGED', 'database identity changed during snapshot')
    return data, checks


def evidence_checks(data):
    try: value = json.loads(data)
    except (UnicodeDecodeError, ValueError): fail('E_SCOPE', 'only explicitly marked synthetic JSON evidence is allowed')
    if not isinstance(value, dict) or value.get('schema') != 'm0.synthetic_evidence.v1' or value.get('synthetic_only') is not True:
        fail('E_SCOPE', 'evidence must carry synthetic-only schema marker')
    stack = [value]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if any(DENIED.search(key) for key in node): fail('E_DENYLIST', 'credential/session field denied')
            stack.extend(node.values())
        elif isinstance(node, list): stack.extend(node)
    return {'synthetic_marker': True}


def validate_plan(plan, target_root):
    if plan.get('schema') != 'm0.synthetic_backup_plan.v1' or plan.get('synthetic_only') is not True:
        fail('E_SCOPE', 'explicit synthetic-only plan required')
    source_root = Path(plan['source_root'])
    if not source_root.is_absolute(): fail('E_PATH', 'source root must be absolute')
    if not stat.S_ISDIR(safe_stat(source_root).st_mode): fail('E_PATH', 'source root must be a directory')
    op_id = plan['op_id']
    if not isinstance(op_id, str) or not re.fullmatch('[A-Za-z0-9_-]{1,80}', op_id): fail('E_PATH', 'invalid operation ID')
    if (target_root/op_id).exists() or (target_root/(op_id+'.partial')).exists(): fail('E_EXISTS', 'operation already exists; choose a new operation ID')
    files = plan.get('files')
    if not isinstance(files, list) or not 1 <= len(files) <= 50: fail('E_SCOPE', 'explicit 1..50 file allowlist required')
    seen, validated = set(), []
    for item in files:
        relative = str(relative_name(item['path']))
        if relative.casefold() in seen: fail('E_PATH', 'duplicate or case-colliding file path')
        seen.add(relative.casefold())
        kind = item['kind']
        if kind not in KINDS: fail('E_SCOPE', 'unsupported file kind')
        suffix = PurePosixPath(relative).suffix.lower()
        if suffix not in ({'.json'} if kind == 'evidence' else {'.db', '.sqlite', '.sqlite3'}): fail('E_DENYLIST', 'media and unsupported file extensions denied')
        path = source_root.joinpath(*PurePosixPath(relative).parts)
        if safe_stat(path).st_dev != os.stat(source_root).st_dev:
            fail('E_DEVICE', 'selected file crosses an unverified source device')
        validated.append((relative, path, kind))
    return source_root, op_id, validated


def archive_bytes(manifest, payloads):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for name, data in [('manifest.json', canonical(manifest)), *[('payload/'+p, b) for p, b in sorted(payloads.items())]]:
            info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100600 << 16
            archive.writestr(info, data)
    return buffer.getvalue()


def inspect_archive(data):
    try:
        if len(data) > MAX_BYTES: fail('E_SIZE', 'archive exceeds limit')
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            infos = archive.infolist()
            names = [x.filename for x in infos]
            if len(names) != len(set(names)) or 'manifest.json' not in names: fail('E_ARCHIVE', 'duplicate/missing archive entries')
            if sum(x.file_size for x in infos) > MAX_BYTES or any(x.flag_bits & 1 or stat.S_ISLNK(x.external_attr >> 16) for x in infos): fail('E_ARCHIVE', 'oversize/encrypted/link archive entry')
            manifest = json.loads(archive.read('manifest.json'))
            if manifest.get('schema') != SCHEMA or manifest.get('synthetic_only') is not True: fail('E_SCOPE', 'unsupported archive scope')
            expected, payloads = {'manifest.json'}, {}
            if not isinstance(manifest.get('files'), list) or not 1 <= len(manifest['files']) <= 50: fail('E_ARCHIVE', 'invalid manifest file count')
            folded = set()
            for item in manifest['files']:
                relative = str(relative_name(item['rel_path']))
                if item['kind'] not in KINDS or item.get('class') != 'internal': fail('E_SCOPE', 'unsupported manifest kind/class')
                suffix = PurePosixPath(relative).suffix.lower()
                if suffix not in ({'.json'} if item['kind'] == 'evidence' else {'.db', '.sqlite', '.sqlite3'}): fail('E_DENYLIST', 'unsupported manifest extension')
                if relative.casefold() in folded: fail('E_ARCHIVE', 'duplicate/colliding manifest path')
                folded.add(relative.casefold())
                name = 'payload/' + relative
                expected.add(name)
                content = archive.read(name)
                if len(content) != item['bytes'] or sha(content) != item['sha256']: fail('E_HASH', 'payload hash or size mismatch')
                if item['kind'] == 'evidence':
                    checks = evidence_checks(content)
                else:
                    with closing(sqlite3.connect(':memory:')) as conn:
                        conn.deserialize(content)
                        checks = db_checks(conn, item['kind'])
                if checks != item['checks']: fail('E_QUERY', 'restored query checks differ')
                payloads[relative] = content
            if set(names) != expected: fail('E_ARCHIVE', 'unlisted archive files are denied')
            return manifest, payloads
    except BackupError: raise
    except (KeyError, ValueError, TypeError, OSError, sqlite3.Error, zipfile.BadZipFile) as exc:
        raise BackupError('E_ARCHIVE', 'archive validation failed') from exc


def preflight(plan, target_root, quota_bytes=MAX_BYTES):
    target_root = Path(os.path.abspath(target_root))
    if type(quota_bytes) is not int or not 0 < quota_bytes <= MAX_BYTES: fail('E_QUOTA', 'quota must be within the authorized 200 MiB ceiling')
    existing = tree_bytes(target_root)
    source_root, op_id, files = validate_plan(plan, target_root)
    devices = device_identities(source_root, target_root, plan.get('linux_devices'))
    payloads, entries = {}, []
    selected = 0
    for relative, path, kind in files:
        if kind == 'evidence':
            data = regular_bytes(path, MAX_EVIDENCE)
            checks = evidence_checks(data)
        else:
            data, checks = sqlite_snapshot(path, kind)
        selected += len(data)
        if existing + selected + 16384 > quota_bytes: fail('E_QUOTA', 'existing usage plus selected snapshots exceeds quota')
        payloads[relative] = data
        entries.append({'rel_path': relative, 'kind': kind, 'bytes': len(data), 'sha256': sha(data),
                        'class': 'internal', 'checks': checks})
    manifest = {'schema': SCHEMA, 'op_id': op_id, 'created_at': now(), 'synthetic_only': True,
                'files': entries, 'devices': devices, 'snapshot_method': 'sqlite_backup_api_to_memory',
                'sqlite_image_format': 'standalone_rollback_header_after_consistent_backup',
                'selected_bytes': selected, 'source_plan_sha256': sha(canonical(plan))}
    data = archive_bytes(manifest, payloads)
    inspect_archive(data)
    required = len(data) + 16384
    if existing + required > quota_bytes: fail('E_QUOTA', 'archive and operation metadata exceed quota')
    return {'manifest': manifest, 'archive': data, 'existing_bytes': existing, 'required_bytes': required,
            'quota_bytes': quota_bytes, 'target_root': target_root, 'op_id': op_id}


def write_file(root, path, data, log, *, replace=False, quota_bytes=None):
    root, path = Path(os.path.abspath(root)), Path(os.path.abspath(path))
    if not path.is_relative_to(root): fail('E_WRITE_SCOPE', 'write escaped target root')
    with directory_handle(path.parent, create=False) as parent_fd:
        before = safe_stat(path, missing=True)
        if before and (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1):
            fail('E_LINK', 'existing target must be an unshared regular file')
        if before and not replace:
            fail('E_EXISTS', 'target file already exists')
        if quota_bytes is not None and tree_bytes(root) + len(data) > quota_bytes:
            fail('E_QUOTA', 'next bounded write would exceed target quota')
        temporary = '.' + path.name + '.' + uuid.uuid4().hex + '.tmp'
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0)
        file_arg = path.parent / temporary if parent_fd is None else temporary
        options = {} if parent_fd is None else {'dir_fd': parent_fd}
        fd = os.open(file_arg, flags, 0o600, **options)
        try:
            with os.fdopen(fd, 'wb') as stream:
                opened = os.fstat(stream.fileno())
                linked = os.stat(file_arg, follow_symlinks=False, **options)
                if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1 or
                        (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)):
                    fail('E_LINK', 'new output is not an exclusive regular file')
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
                linked = os.stat(file_arg, follow_symlinks=False, **options)
                if (os.fstat(stream.fileno()).st_nlink != 1 or
                        (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)):
                    fail('E_PATH_CHANGED', 'exclusive output identity changed while writing')
            if parent_fd is None:
                # Atomic replacement removes a late hardlink; it never opens its target.
                if replace:
                    os.replace(file_arg, path)
                else:
                    os.rename(file_arg, path)  # Windows refuses an existing destination.
            elif replace:
                os.replace(temporary, path.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            else:
                # link+unlink publishes exclusively without POSIX rename overwriting.
                os.link(temporary, path.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd, follow_symlinks=False)
                os.unlink(temporary, dir_fd=parent_fd)
        finally:
            try:
                os.unlink(file_arg, **options)
            except FileNotFoundError:
                pass
    log.append({'path': path.relative_to(root).as_posix(), 'operation': 'replace' if replace else 'create', 'bytes': len(data)})


def backup(plan, target_root, quota_bytes=MAX_BYTES):
    safe_stat(target_root)
    with directory_handle(target_root), target_lock(target_root):
        return _backup_locked(plan, target_root, quota_bytes)


def _backup_locked(plan, target_root, quota_bytes):
    prepared = preflight(plan, target_root, quota_bytes)
    root, op_id = prepared['target_root'], prepared['op_id']
    # Recheck total immediately before publishing any new operation directory.
    if tree_bytes(root) + prepared['required_bytes'] > quota_bytes: fail('E_QUOTA', 'target usage changed after preflight')
    partial, final = root/(op_id+'.partial'), root/op_id
    if final.exists() or partial.exists(): fail('E_EXISTS', 'operation ID was claimed concurrently')
    safe_stat(root)
    secure_mkdir(partial)
    log = [{'path': partial.relative_to(root).as_posix(), 'operation': 'mkdir'}]
    state = {'schema': 'm0.backup_status.v1', 'op_id': op_id, 'state': 'writing', 'phase': 'archive', 'at': now()}
    try:
        write_file(root, partial/'status.json', canonical(state), log, quota_bytes=quota_bytes)
        write_file(root, partial/'backup.zip', prepared['archive'], log, quota_bytes=quota_bytes)
        inspect_archive(regular_bytes(partial/'backup.zip'))
        state.update(state='completed', phase='verified', archive_sha256=sha(prepared['archive']),
                     archive_bytes=len(prepared['archive']), selected_bytes=prepared['manifest']['selected_bytes'], at=now())
        write_file(root, partial/'status.json', canonical(state), log, replace=True, quota_bytes=quota_bytes)
        log.append({'path': (partial/'writes.json').relative_to(root).as_posix(), 'operation': 'create'})
        log.append({'path': final.relative_to(root).as_posix(), 'from': partial.relative_to(root).as_posix(), 'operation': 'rename'})
        write_file(root, partial/'writes.json', canonical(log), [], replace=False, quota_bytes=quota_bytes)
        total_bytes = tree_bytes(root)
        if total_bytes > quota_bytes: fail('E_QUOTA', 'quota changed during operation; partial retained')
        safe_stat(partial)
        if final.exists(): fail('E_EXISTS', 'final operation directory appeared concurrently')
        publish_directory(partial, final)
        return {**state, 'directory': str(final), 'archive': str(final/'backup.zip'),
                'existing_bytes_before': prepared['existing_bytes'], 'total_root_bytes': total_bytes,
                'quota_bytes': quota_bytes, 'devices': prepared['manifest']['devices'], 'write_log': log}
    except Exception as exc:
        state.update(state='failed', phase='partial_retained', error_code=getattr(exc, 'code', type(exc).__name__), at=now())
        try:
            write_file(root, partial/'status.json', canonical(state), log, replace=True, quota_bytes=quota_bytes)
            write_file(root, partial/'writes.json', canonical(log), [], replace=(partial/'writes.json').exists(), quota_bytes=quota_bytes)
        except Exception: pass
        raise BackupError('E_PARTIAL', 'backup failed after writes; inspect retained partial directory', state='failed', partial=str(partial)) from exc


def restore(archive_path, target_root, *, expected_target_st_dev=None):
    data = regular_bytes(archive_path)
    manifest, payloads = inspect_archive(data)
    root = Path(os.path.abspath(target_root))
    safe_stat(root, missing=True)
    if root.exists() and (not root.is_dir() or any(root.iterdir())): fail('E_TARGET', 'restore target must be new or empty')
    actual_device = os.stat(_nearest(root)).st_dev
    if os.name != 'nt' and (type(expected_target_st_dev) is not int or actual_device != expected_target_st_dev):
        fail('E_DEVICE', 'Linux restore requires the matching measured target st_dev')
    safe_stat(root.parent)
    secure_mkdir(root)
    with directory_handle(root), target_lock(root):
        # A competing process may have claimed an initially empty root.
        if any(root.iterdir()): fail('E_TARGET', 'restore target must remain empty until locked')
        return _restore_locked(data, manifest, payloads, root, actual_device)


def _restore_locked(data, manifest, payloads, root, actual_device):
    partial, final = root/'files.partial', root/'files'
    secure_mkdir(partial)
    log = [{'path': '.', 'operation': 'mkdir'}, {'path': 'files.partial', 'operation': 'mkdir'}]
    state = {'schema': 'm0.restore_status.v1', 'state': 'writing', 'op_id': manifest['op_id'], 'at': now(), 'target_st_dev': actual_device}
    try:
        write_file(root, root/'status.json', canonical(state), log)
        for relative, content in sorted(payloads.items()):
            path = partial.joinpath(*PurePosixPath(relative).parts)
            secure_mkdir(path.parent)
            write_file(root, path, content, log)
            if sha(regular_bytes(path)) != sha(content): fail('E_HASH', 'restored file hash mismatch')
        checks = {}
        for item in manifest['files']:
            path = partial.joinpath(*PurePosixPath(item['rel_path']).parts)
            if item['kind'] != 'evidence':
                with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1', uri=True)) as conn:
                    checks[item['rel_path']] = db_checks(conn, item['kind'])
                if checks[item['rel_path']] != item['checks']: fail('E_QUERY', 'restored on-disk database checks differ')
        publish_directory(partial, final)
        log.append({'path': 'files', 'from': 'files.partial', 'operation': 'rename'})
        state.update(state='completed', phase='verified', files_verified=len(payloads), queries=checks,
                     archive_sha256=sha(data), at=now())
        write_file(root, root/'status.json', canonical(state), log, replace=True)
        log.append({'path': 'writes.json', 'operation': 'create'})
        write_file(root, root/'writes.json', canonical(log), [], replace=False)
        return {**state, 'directory': str(root), 'write_log': log}
    except Exception as exc:
        state.update(state='failed', phase='partial_retained', error_code=getattr(exc, 'code', type(exc).__name__), at=now())
        try:
            write_file(root, root/'status.json', canonical(state), log, replace=True)
            write_file(root, root/'writes.json', canonical(log), [], replace=(root/'writes.json').exists())
        except Exception: pass
        raise BackupError('E_PARTIAL', 'restore failed after writes; choose a new empty target', state='failed', partial=str(root)) from exc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for command in ('preflight', 'backup'):
        child = sub.add_parser(command)
        child.add_argument('--plan', required=True)
        child.add_argument('--target-root', required=True)
        child.add_argument('--quota-bytes', type=int, default=MAX_BYTES)
    child = sub.add_parser('restore')
    child.add_argument('--archive', required=True)
    child.add_argument('--target-root', required=True)
    child.add_argument('--expected-target-st-dev', type=int)
    args = parser.parse_args()
    try:
        if args.command == 'restore':
            result = restore(args.archive, args.target_root, expected_target_st_dev=args.expected_target_st_dev)
        else:
            if os.name == 'nt':
                fail('E_TARGET', 'Windows backup and preflight CLI require a deployment-specific target policy')
            plan = json.loads(regular_bytes(args.plan))
            if args.command == 'preflight':
                value = preflight(plan, args.target_root, args.quota_bytes)
                result = {key: item for key, item in value.items() if key not in ('archive', 'target_root')}
                result.update(state='ready', writes=0, archive_bytes=len(value['archive']))
            else:
                result = backup(plan, args.target_root, args.quota_bytes)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except BackupError as exc:
        print(json.dumps({'state': exc.state, 'code': exc.code, 'message': str(exc), 'partial': exc.partial}), file=sys.stderr)
        return 2
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, subprocess.SubprocessError) as exc:
        # Any error after publication starts has already become E_PARTIAL above.
        print(json.dumps({'state': 'rejected', 'code': 'E_PREFLIGHT_IO',
                          'message': type(exc).__name__, 'partial': None}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
