"""Read-only SQLite snapshots: no application constructors or migrations."""
from contextlib import contextmanager
from pathlib import Path
import os
import sqlite3
import stat
import re
import sys


class SourceError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def local_file(path):
    path = Path(path)
    if not path.is_absolute() or str(path).startswith(('\\\\', '//')):
        raise SourceError('source_locator_not_local')
    # Do not follow symlink/junction aliases into another source or OS.
    for part in (path, *path.parents):
        info = part.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise SourceError('source_link_not_allowed')
    if not path.is_file():
        raise SourceError('source_unavailable')
    return path


def authorize_read(action, arg1, arg2, database, trigger):
    if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_RECURSIVE):
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_FUNCTION and (arg2 or '').lower() in {
        'count', 'max', 'min', 'sum', 'coalesce', 'json_extract', 'json_valid', 'json_type',
        'length', 'lower', 'upper', 'abs'}:
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_PRAGMA and (arg1 in {'table_info', 'index_list', 'index_info'} or (arg1 == 'user_version' and arg2 is None)):
        return sqlite3.SQLITE_OK
    return sqlite3.SQLITE_DENY


def local_sqlite_file(path):
    path=local_file(path)
    if sys.platform=='win32':
        import ctypes
        drive_type=ctypes.windll.kernel32.GetDriveTypeW(str(path.anchor))
        if drive_type in (0,1,4):
            raise SourceError('source_filesystem_not_local_sqlite')
    if sys.platform.startswith('linux'):
        try:
            mounts=Path('/proc/self/mountinfo').read_text().splitlines()
            selected=None
            for line in mounts:
                left,right=line.split(' - ',1)
                fields=left.split();kind=right.split()[0]
                mount=Path(re.sub(r'\\([0-7]{3})',lambda m:chr(int(m[1],8)),fields[4]))
                if path.is_relative_to(mount) and (selected is None or len(mount.parts)>selected[0]):
                    selected=(len(mount.parts),kind)
            if selected is None:
                raise SourceError('source_filesystem_unverified')
            if selected[1] in {'9p','drvfs','cifs','smbfs','nfs','nfs4','ntfs','ntfs3','fuseblk','fuse.sshfs'}:
                raise SourceError('source_filesystem_not_local_sqlite')
        except (OSError,ValueError,IndexError):
            raise SourceError('source_filesystem_unverified') from None
    return path


@contextmanager
def readonly_db(path, *, identity=None):
    path = local_sqlite_file(path)
    before = path.stat()
    if identity is not None and identity != {'device': before.st_dev, 'inode': before.st_ino}:
        raise SourceError('source_instance_changed')
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)
    try:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        db.set_authorizer(authorize_read)
        # Establish the snapshot before checking file identity again.
        db.execute('SELECT name FROM sqlite_master LIMIT 1').fetchall()
        after = path.stat()
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise SourceError('source_instance_changed')
        yield db
    finally:
        db.close()
