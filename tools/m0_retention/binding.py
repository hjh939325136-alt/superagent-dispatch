"""Trusted path enrollment; no operation can turn an existing database into a fixture."""
from dataclasses import dataclass
import os
from pathlib import Path
import re
import stat

from .common import (APP_ID, GRANT, MAX_CAS, MAX_LAB, digest, fail, identity,
                     json_file, local_sqlite_file, read, safe_stat, verify_seal)

SCHEMA='m0.retention_binding.v1'
FIELDS={'schema','mode','root','instance','ledger_identity','marker_sha256','sources'}


def _quiet_rollback_database(path):
    """Do not let SQLite create WAL/SHM files during an ordinary dry run."""
    if any(os.path.lexists(str(path)+suffix) for suffix in ('-wal','-shm','-journal')):
        fail('E_DRY_RUN_NOT_QUIESCENT')
    before=identity(path)
    fd=os.open(path,os.O_RDONLY|getattr(os,'O_BINARY',0)|getattr(os,'O_NOFOLLOW',0))
    with os.fdopen(fd,'rb') as stream:
        opened=os.fstat(stream.fileno())
        if before!={'device':opened.st_dev,'inode':opened.st_ino}:fail('E_INSTANCE_CHANGED')
        header=stream.read(20)
    if header[:16]!=b'SQLite format 3\x00' or header[18:20]!=b'\x01\x01':
        fail('E_DRY_RUN_ROLLBACK_DATABASE_REQUIRED')


@dataclass(frozen=True)
class RetentionBinding:
    path:Path
    root:Path
    instance:str
    mode:str
    ledger_identity:dict
    sources:tuple
    digest:str

    @property
    def ledger(self):return self.root/'ledger.sqlite3'
    @property
    def cas(self):return self.root/'cas'
    @property
    def journals(self):return self.root/'journals'


def load_binding(path):
    path=Path(path)
    if not path.is_absolute():fail('E_BINDING_PATH')
    value=json_file(path)
    if (not isinstance(value,dict) or set(value)!=FIELDS or value['schema']!=SCHEMA or
            value['mode'] not in ('fixture','dry_run') or
            not isinstance(value['instance'],str) or not re.fullmatch('[0-9a-f]{32}',value['instance'])):
        fail('E_BINDING')
    root=Path(value['root'])
    if not root.is_absolute() or '..' in root.parts or str(root).startswith(('\\\\','//')):fail('E_SCOPE')
    if not stat.S_ISDIR(safe_stat(root).st_mode):fail('E_SCOPE')
    if path!=root/'binding.json':fail('E_BINDING_PATH')
    marker=verify_seal(json_file(root/'fixture.json')) if value['mode']=='fixture' else None
    if marker is not None and (digest(marker)!=value['marker_sha256'] or marker.get('schema')!='m0.retention_fixture.v1' or
            marker.get('instance')!=value['instance'] or marker.get('synthetic_only') is not True or
            marker.get('created_new') is not True or marker.get('producer_protocol')!='closed-local-fixture-v1' or
            marker.get('quota_bytes')!=MAX_LAB or marker.get('cas_quota_bytes')!=MAX_CAS or
            marker.get('grant')!=GRANT or not isinstance(marker.get('generated_objects'),dict)):fail('E_FIXTURE_MARKER')
    expected={'fixture.json','binding.json','ledger.sqlite3','ledger.sqlite3-journal','cas','journals','sources',
              'generations.json','consumers.json','operations','plans'}
    if marker is not None and any(p.name not in expected for p in root.iterdir()):fail('E_FIXTURE_LAYOUT')
    if marker is None and value['marker_sha256'] is not None:fail('E_DRY_RUN_MARKER')
    ledger=local_sqlite_file(root/'ledger.sqlite3')
    if identity(ledger)!=value['ledger_identity']:fail('E_INSTANCE_CHANGED')
    if marker is None:_quiet_rollback_database(ledger)
    sources=value['sources']
    if not isinstance(sources,list) or len(sources)>10:fail('E_SOURCES')
    from sa_dispatch.collector import SourceBinding
    allowed=set(SourceBinding.__dataclass_fields__)
    seen=set()
    for entry in sources:
        if not isinstance(entry,dict) or set(entry)!=allowed|{'stream'}:fail('E_SOURCE_BINDING')
        if entry['source_id'] in seen:fail('E_SOURCE_BINDING')
        seen.add(entry['source_id'])
        source_path=Path(entry['locator'])
        if '..' in source_path.parts:fail('E_SOURCE_SCOPE')
        if marker is not None and not source_path.is_relative_to(root/'sources'):fail('E_SOURCE_SCOPE')
        local_sqlite_file(source_path);identity(source_path)
        _quiet_rollback_database(source_path)
        bound=SourceBinding(**{k:v for k,v in entry.items() if k!='stream'})
        bound.validate(entry['stream'])
    return RetentionBinding(path,root,value['instance'],value['mode'],value['ledger_identity'],
                            tuple(sources),digest(value))


def fixture_blob_enrolled(binding,item,enrolled):
    return enrolled.get(item['ref'])=={k:item[k] for k in ('bytes','identity','created_at')}


def require_fixture(binding,grant):
    if (not isinstance(binding,RetentionBinding) or binding.mode!='fixture' or grant!=GRANT or
            load_binding(binding.path)!=binding):fail('E_FIXTURE_APPLY_ONLY')
    # No public initializer/adopter exists for existing roots. Only lab creation
    # sets this header on a newly created ledger; unknown applications fail shut.
    import sqlite3
    from contextlib import closing
    with closing(sqlite3.connect(binding.ledger.as_uri()+'?mode=ro',uri=True)) as db:
        if db.execute('PRAGMA application_id').fetchone()[0]!=APP_ID:fail('E_FIXTURE_DATABASE')
        if db.execute('PRAGMA journal_mode').fetchone()[0].lower()!='delete':fail('E_JOURNAL_MODE')
        db.row_factory=sqlite3.Row
        schema=[dict(r) for r in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name') if not r['name'].startswith('sqlite_')]
        if digest(schema)!=json_file(binding.root/'fixture.json').get('ledger_schema_sha256'):fail('E_FIXTURE_SCHEMA_CHANGED')
