"""Local implementation of Fable's storage.target contract; no physical delete.

This trusted-operator adapter grants no source-data permission. The default M0
payload policy admits only verified synthetic backup packages/marked evidence.
NAS transports can implement the same API without changing backup callers.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import stat
import unicodedata
import uuid

from tools.m0_backup.backup import (MAX_BYTES, MAX_EVIDENCE, BackupError, canonical,
    directory_handle, evidence_checks, fail, inspect_archive, now, regular_bytes,
    relative_name, safe_stat, secure_mkdir, sha, target_lock, tree_bytes, write_file)

SCHEMA='m0.storage_target.v1'
MAX_INDEX=4*1024*1024

def synthetic_payload(data):
    if data.startswith(b'PK'):
        inspect_archive(data)
    else:
        if len(data)>MAX_EVIDENCE:fail('E_SIZE','synthetic evidence is too large')
        evidence_checks(data)

def relname(value):
    path=relative_name(value)
    if unicodedata.normalize('NFC',value)!=value or len(value)>512:
        fail('E_PATH','canonical bounded Unicode relative name required')
    if any(part.endswith((' ','.')) for part in path.parts):
        fail('E_PATH','trailing dots and spaces are not portable')
    return path.as_posix()

def sync_directory(path):
    # Linux ordering: object bytes + directory entry precede the index commit.
    # The Windows path retains atomic publication/file flushing, but this adapter
    # does not claim power-loss durability without a filesystem-level drill.
    if os.name=='posix':
        with directory_handle(path) as handle:os.fsync(handle)

class LocalDirTarget:
    """put/get/stat/list/tombstone, with immutable names and irreversible markers.

    root must already exist and be inside the explicit shared quota root. All
    writers, including the existing backup tool, share the quota-root lock.
    Same-name same-content put is idempotent; new content needs a new versioned
    name. A tombstoned name cannot be resurrected by another put.
    """
    def __init__(self,root,*,quota_root,quota_bytes=MAX_BYTES,payload_policy=synthetic_payload):
        self.root=Path(os.path.abspath(root));self.quota_root=Path(os.path.abspath(quota_root))
        if not self.root.is_relative_to(self.quota_root) or self.root==self.quota_root:
            fail('E_WRITE_SCOPE','target requires its own directory inside the quota root')
        if type(quota_bytes) is not int or not 0<quota_bytes<=MAX_BYTES:
            fail('E_QUOTA','quota must be positive and at most 200 MiB')
        for path in (self.root,self.quota_root):
            if not stat.S_ISDIR(safe_stat(path).st_mode):fail('E_TARGET','explicit target directories must exist')
        self.quota_bytes=quota_bytes;self.payload_policy=payload_policy
        self.index=self.root/'index.json';self.objects=self.root/'objects'

    @contextmanager
    def _lock(self):
        with directory_handle(self.quota_root),directory_handle(self.root),target_lock(self.quota_root):
            # Also reject links in unrelated history instead of following them
            # while calculating the shared root's existing occupancy.
            tree_bytes(self.quota_root)
            yield

    def _read(self):
        try:value=json.loads(regular_bytes(self.index,MAX_INDEX))
        except (ValueError,UnicodeError):fail('E_MANIFEST','target index is not valid JSON')
        if (not isinstance(value,dict) or set(value)!={'schema','instance_id','revision','entries'} or
            value['schema']!=SCHEMA or not re.fullmatch('[0-9a-f]{32}',str(value['instance_id'])) or
            type(value['revision']) is not int or value['revision']<0 or not isinstance(value['entries'],dict)):
            fail('E_MANIFEST','invalid target index')
        folded=set()
        for rel,row in value['entries'].items():
            relname(rel)
            if rel.casefold() in folded:fail('E_MANIFEST','case-colliding target names')
            folded.add(rel.casefold())
            if (not isinstance(row,dict) or set(row)!={'sha256','bytes','state','revision','changed_at'} or
                not re.fullmatch('[0-9a-f]{64}',str(row['sha256'])) or type(row['bytes']) is not int or
                not 0<=row['bytes']<=MAX_BYTES or row['state'] not in ('available','tombstoned') or
                type(row['revision']) is not int or not 1<=row['revision']<=value['revision'] or
                not isinstance(row['changed_at'],str)):
                fail('E_MANIFEST','invalid target entry')
        return value

    def _quota(self,additional):
        used=tree_bytes(self.quota_root)
        if used+additional>self.quota_bytes:fail('E_QUOTA','shared backup-root quota exceeded')
        return used

    def initialize(self):
        with self._lock():
            if self.index.exists():return self._summary(self._read())
            if any(self.root.iterdir()):fail('E_TARGET','new target directory must be empty')
            value={'schema':SCHEMA,'instance_id':uuid.uuid4().hex,'revision':0,'entries':{}}
            data=canonical(value);self._quota(len(data))
            write_file(self.quota_root,self.index,data,[],quota_bytes=self.quota_bytes)
            sync_directory(self.root)
            return self._summary(value)

    @staticmethod
    def _summary(value):
        return {key:value[key] for key in ('schema','instance_id','revision')}

    @staticmethod
    def _row(value,rel):
        if rel not in value['entries']:fail('E_NOT_FOUND','target name is not present')
        return value['entries'][rel]

    def _blob(self,row,limit=MAX_BYTES):
        data=regular_bytes(self.objects/(row['sha256']+'.blob'),limit)
        if len(data)!=row['bytes'] or sha(data)!=row['sha256']:fail('E_HASH_MISMATCH','stored artifact failed verification')
        return data

    def put(self,rel,data,sha256):
        rel=relname(rel)
        if type(data) is not bytes or len(data)>MAX_BYTES:fail('E_SIZE','bounded bytes required')
        if not isinstance(sha256,str) or not re.fullmatch('[0-9a-f]{64}',sha256) or sha(data)!=sha256:
            fail('E_HASH_MISMATCH','input checksum mismatch')
        self.payload_policy(data)
        with self._lock():
            value=self._read()
            if any(key.casefold()==rel.casefold() and key!=rel for key in value['entries']):
                fail('E_CONFLICT','case-colliding target name')
            old=value['entries'].get(rel)
            if old:
                if old['state']=='tombstoned':fail('E_TOMBSTONED','name has been withdrawn; use a new versioned name')
                if old['sha256']!=sha256 or old['bytes']!=len(data):fail('E_CONFLICT','immutable name already contains other data')
                self._blob(old)
                return {'rel':rel,**old,'deduplicated':True}
            blob=self.objects/(sha256+'.blob')
            exists=safe_stat(blob,missing=True) is not None
            row={'sha256':sha256,'bytes':len(data),'state':'available','revision':value['revision']+1,'changed_at':now()}
            if exists:self._blob(row)
            value['revision']+=1;value['entries'][rel]=row
            manifest=canonical(value)
            if len(manifest)>MAX_INDEX:fail('E_SIZE','target index capacity exceeded')
            # Reserve object plus full new index, including atomic temporary
            # coexistence with the old index, before creating either output.
            self._quota((0 if exists else len(data))+len(manifest))
            if not exists:
                secure_mkdir(self.objects)
                sync_directory(self.root)
                write_file(self.quota_root,blob,data,[],quota_bytes=self.quota_bytes)
                sync_directory(self.objects)
            self._blob(row)
            write_file(self.quota_root,self.index,manifest,[],replace=True,quota_bytes=self.quota_bytes)
            sync_directory(self.root)
            return {'rel':rel,**row,'deduplicated':False}

    def get(self,rel):
        rel=relname(rel)
        with self._lock():
            row=self._row(self._read(),rel)
            if row['state']=='tombstoned':fail('E_TOMBSTONED','artifact is logically withdrawn')
            return self._blob(row)

    def stat(self,rel):
        rel=relname(rel)
        with self._lock():
            value=self._read();row=self._row(value,rel)
            if row['state']=='available':self._blob(row)
            return {'rel':rel,**row,'instance_id':value['instance_id'],'payload_verified':row['state']=='available'}

    def list(self,prefix=''):
        if not isinstance(prefix,str):fail('E_PATH','string prefix required')
        prefix=relname(prefix.rstrip('/')) if prefix else ''
        with self._lock():
            value=self._read()
            return [{'rel':rel,**row,'instance_id':value['instance_id']} for rel,row in sorted(value['entries'].items())
                    if not prefix or rel==prefix or rel.startswith(prefix+'/')]

    def readonly_snapshot(self, *, verify_names=(), max_blob_bytes=MAX_BYTES):
        """Observe an atomic index without creating/touching the writer lock.

        Optional names are verified against immutable CAS bytes. A concurrent
        index change invalidates this observation rather than returning a mixed
        view. This is a snapshot, never a claim that a backup job is running.
        """
        if type(max_blob_bytes) is not int or not 0<max_blob_bytes<=MAX_BYTES:
            fail('E_SIZE','invalid readonly blob limit')
        before=regular_bytes(self.index,MAX_INDEX)
        value=self._read()
        verified={}
        for rel in verify_names:
            rel=relname(rel);row=self._row(value,rel)
            if row['state']!='available':fail('E_TOMBSTONED','artifact is logically withdrawn')
            if row['bytes']>max_blob_bytes:fail('E_SIZE','readonly blob exceeds limit')
            verified[rel]=self._blob(row,max_blob_bytes)
        if json.loads(before)!=value or before!=regular_bytes(self.index,MAX_INDEX):
            fail('E_SOURCE_CHANGED','storage index changed during observation')
        return value,verified

    def tombstone(self,rel):
        rel=relname(rel)
        with self._lock():
            value=self._read();row=self._row(value,rel)
            if row['state']=='tombstoned':return {'rel':rel,**row,'deduplicated':True}
            value['revision']+=1
            row.update(state='tombstoned',revision=value['revision'],changed_at=now())
            data=canonical(value)
            if len(data)>MAX_INDEX:fail('E_SIZE','tombstone exceeds index capacity; prior index remains unchanged')
            self._quota(len(data))
            write_file(self.quota_root,self.index,data,[],replace=True,quota_bytes=self.quota_bytes)
            sync_directory(self.root)
            return {'rel':rel,**row,'deduplicated':False}
