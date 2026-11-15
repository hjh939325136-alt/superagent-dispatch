"""Reuse the audited local-path primitives; do not grant arbitrary writer scope."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT/'dispatch') not in sys.path:
    sys.path.insert(0, str(ROOT/'dispatch'))
from tools.m0_backup.backup import (directory_handle, regular_bytes, safe_stat, secure_mkdir,
                                  target_lock, tree_bytes, write_file)
from tools.m0_manager.readonly import local_sqlite_file, readonly_db

MAX_LAB = 512*1024*1024
MAX_CAS = 32*1024*1024
MAX_BLOB = 1024*1024
MAX_JSON = 8*1024*1024
MAX_FILES = 20000
MAX_ROWS = 250000
MAX_CANDIDATES = 10000
APP_ID = 0x4D305254
GRANT = 'm0-retention-fixture-v1'
HASH = re.compile(r'^sha256:[0-9a-f]{64}$')


class RetentionError(ValueError):
    def __init__(self, code):
        self.code=code
        super().__init__(code)


def fail(code):raise RetentionError(code)
def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')
def sha(data):return hashlib.sha256(data).hexdigest()
def digest(value):return sha(canonical(value))
def now():return datetime.now(timezone.utc).isoformat()


def instant(value):
    try:
        parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
        if parsed.tzinfo is None:fail('E_TIME')
        return parsed.astimezone(timezone.utc)
    except (ValueError,TypeError,AttributeError):fail('E_TIME')


def parse(data):
    def pairs(items):
        result=dict(items)
        if len(result)!=len(items):fail('E_DUPLICATE_FIELD')
        return result
    def invalid(_):fail('E_JSON')
    return json.loads(data,object_pairs_hook=pairs,parse_constant=invalid)


def identity(path):
    info=safe_stat(path)
    if stat.S_ISREG(info.st_mode) and info.st_nlink!=1:fail('E_SHARED_FILE')
    return {'device':info.st_dev,'inode':info.st_ino}


def read(path, limit=MAX_JSON):
    identity(path)
    return regular_bytes(path,limit)


def json_file(path):return parse(read(path))


def quota(root, additional=0):
    size=tree_bytes(root)
    if size+additional>MAX_LAB:fail('E_LAB_QUOTA')
    return size


def write(root,path,data,*,replace=False):
    path=Path(path);root=Path(root)
    if not path.is_relative_to(root) or path==root:fail('E_WRITE_SCOPE')
    quota(root,len(data))
    secure_mkdir(path.parent)
    write_file(root,path,data,[],replace=replace,quota_bytes=MAX_LAB)


def document(root,path,value,*,replace=False):
    write(root,path,canonical(value)+b'\n',replace=replace)


def sealed(value):return {**value,'sha256':digest(value)}


def verify_seal(value):
    if not isinstance(value,dict) or value.get('sha256')!=digest({k:v for k,v in value.items() if k!='sha256'}):fail('E_DOCUMENT_HASH')
    return value


def files(root):
    root=Path(root);result=[];stack=[root]
    if not stat.S_ISDIR(safe_stat(root).st_mode):fail('E_DIRECTORY')
    while stack:
        parent=stack.pop()
        for path in sorted(parent.iterdir()):
            info=safe_stat(path)
            if stat.S_ISDIR(info.st_mode):stack.append(path)
            elif stat.S_ISREG(info.st_mode):
                identity(path);result.append(path)
                if len(result)>MAX_FILES:fail('E_FILE_LIMIT')
            else:fail('E_FILE_TYPE')
    return sorted(result)


def cas_path(root,ref):
    if not isinstance(ref,str) or not HASH.fullmatch(ref):fail('E_CAS_REF')
    return Path(root)/'sha256'/ref[7:9]/ref[7:]


def cas_inventory(root):
    result={};total=0
    for path in files(root):
        rel=path.relative_to(root).as_posix()
        match=re.fullmatch(r'sha256/([0-9a-f]{2})/([0-9a-f]{64})',rel)
        if not match or match[1]!=match[2][:2]:fail('E_CAS_LAYOUT')
        data=read(path,MAX_BLOB)
        if sha(data)!=match[2]:fail('E_CAS_HASH')
        info=path.stat();total+=len(data)
        if total>MAX_CAS:fail('E_CAS_QUOTA')
        result['sha256:'+match[2]]={'bytes':len(data),'identity':identity(path),'mtime_ns':info.st_mtime_ns}
    return result


def unlink_verified(path, expected_hash):
    """Only closed, exclusively owned fixture leaves. Parent handles reject links.

    No online/hostile-local-writer guarantee is claimed on POSIX; apply admission
    requires the closed fixture producer protocol in addition to SQLite locks.
    """
    path=Path(path)
    before=identity(path)
    if sha(read(path,MAX_BLOB))!=expected_hash:fail('E_FILE_CHANGED')
    with directory_handle(path.parent) as fd:
        if identity(path)!=before:fail('E_FILE_CHANGED')
        if fd is None:
            # Windows denies delete-sharing on checked ancestors. The fixture
            # lease additionally excludes all supported writers of this leaf.
            os.unlink(path)
        else:
            current=os.stat(path.name,dir_fd=fd,follow_symlinks=False)
            if {'device':current.st_dev,'inode':current.st_ino}!=before:fail('E_FILE_CHANGED')
            os.unlink(path.name,dir_fd=fd)
