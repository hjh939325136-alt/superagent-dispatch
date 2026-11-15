"""Explicit synthetic-source collector. Readers never instantiate this writer.

Bindings are trusted local configuration, not request authorization. Enrollment
pins a caller-assigned instance plus OS file identity; catalog markers are also
checked. No path alone can authorize resuming an imported cursor.
"""
from contextlib import closing, nullcontext
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys

from .clock import Clock, timestamp
from .unified_events import _token, _at, _ulid, _REF, _KINDS, _META_VALUES, _STATUSES

SCHEMA='''
CREATE TABLE IF NOT EXISTS collector_cursors(
 source_id TEXT NOT NULL, stream TEXT NOT NULL, source_instance TEXT NOT NULL,
 epoch INTEGER NOT NULL DEFAULT 1, last_seq INTEGER NOT NULL DEFAULT 0,
 anchor_sha256 TEXT, updated_at TEXT NOT NULL, binding_sha256 TEXT NOT NULL,
 schema_sha256 TEXT NOT NULL, schema_version TEXT NOT NULL,
 authority TEXT NOT NULL DEFAULT 'local_owned', redacted_count INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(source_id,stream));
CREATE TABLE IF NOT EXISTS collector_gaps(
 gap_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, stream TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN('seq_gap','source_truncated','instance_changed','source_rewritten','schema_changed','redaction_rejected')),
 from_seq INTEGER, to_seq INTEGER, detected_at TEXT NOT NULL,
 resolved_at TEXT, resolution TEXT, resolved_by TEXT);
'''
STREAMS={'dispatch':('events','control_events','schedule_events','reconciliations','host_events'),
         'catalog':('changes',),'fixture_hook':('hook_events',),'fixture_handoff':('handoffs',)}
REASONS=frozenset(('accept_new_instance','accept_gap','accept_rewrite','accept_schema','accept_redaction','bind_imported_history'))


def validate_records(cursors,gaps):
    """Portable audit rows carry no arbitrary explanation or execution rights."""
    for row in cursors:
        for key in ('source_id','stream','source_instance','schema_version'):_token(row[key])
        if type(row['epoch']) is not int or row['epoch']<1:raise ValueError('invalid collector epoch')
        for key in ('last_seq','redacted_count'):
            if type(row[key]) is not int or row[key]<0:raise ValueError('invalid collector count')
        for key in ('binding_sha256','schema_sha256'):
            if not re.fullmatch('[0-9a-f]{64}',row[key]):raise ValueError('invalid collector hash')
        anchor=row['anchor_sha256']
        if (row['last_seq'] and not anchor) or (anchor is not None and not re.fullmatch('[0-9a-f]{64}',anchor)):
            raise ValueError('invalid collector anchor')
        if row['authority'] not in ('local_owned','historical_only'):raise ValueError('invalid collector authority')
        _at(row['updated_at'])
    for row in gaps:
        for key in ('gap_id','source_id','stream'):_token(row[key])
        if row['kind'] not in ('seq_gap','source_truncated','instance_changed','source_rewritten','schema_changed','redaction_rejected'):
            raise ValueError('invalid collector gap kind')
        _at(row['detected_at'])
        for key in ('from_seq','to_seq'):
            if row[key] is not None and (type(row[key]) is not int or row[key]<0):raise ValueError('invalid gap sequence')
        if row['resolved_at'] is not None:
            _at(row['resolved_at']);_token(row['resolved_by'])
            value=json.loads(row['resolution'])
            if set(value)!= {'reason','resume_after','epoch'} or value['reason'] not in REASONS:
                raise ValueError('invalid gap resolution')
            if type(value['epoch']) is not int or value['epoch']<1 or type(value['resume_after']) is not int or value['resume_after']<0:
                raise ValueError('invalid gap resolution sequence')
        elif row['resolution'] is not None or row['resolved_by'] is not None:raise ValueError('partial gap resolution')


def initialize_schema(conn):
    for sql in SCHEMA.split(';'):
        if sql.strip():conn.execute(sql)


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def plain(path):
    if not Path(path).is_absolute() or str(path).startswith(('\\\\','//')):
        raise ValueError('absolute local path required; network/WSL shares forbidden')
    p=Path(path)
    if '..' in p.parts:raise ValueError('parent traversal forbidden')
    for item in (p,*p.parents):
        info=item.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info,'st_file_attributes',0)&0x400:raise ValueError('links forbidden')
    if not p.is_file():raise ValueError('regular source required')
    return p


def file_identity(path):
    s=plain(path).stat()
    return f'{s.st_dev}:{s.st_ino}'


def _mount_info():return Path('/proc/self/mountinfo').read_text()


def _windows_drive_type(anchor):
    import ctypes
    return ctypes.windll.kernel32.GetDriveTypeW(str(anchor))


def local_sqlite_file(path):
    """SQLite may not cross an OS/filesystem boundary, even via a local path."""
    path=plain(path)
    if sys.platform=='win32':
        if _windows_drive_type(path.anchor) in (0,1,4):raise ValueError('SQLite filesystem is not local')
    elif sys.platform.startswith('linux'):
        try:
            selected=None
            for line in _mount_info().splitlines():
                left,right=line.split(' - ',1)
                mount=Path(re.sub(r'\\([0-7]{3})',lambda m:chr(int(m[1],8)),left.split()[4]))
                if path.is_relative_to(mount) and (selected is None or len(mount.parts)>selected[0]):
                    selected=(len(mount.parts),right.split()[0])
            if selected is None:raise ValueError('SQLite filesystem unverified')
            if selected[1] in {'9p','drvfs','cifs','smbfs','nfs','nfs4','ntfs','ntfs3','fuseblk','fuse.sshfs'}:
                raise ValueError('SQLite filesystem is not local')
        except (OSError,IndexError):raise ValueError('SQLite filesystem unverified') from None
    else:raise ValueError('unsupported SQLite host OS')
    return path


@dataclass(frozen=True)
class SourceBinding:
    source_id: str
    kind: str
    locator: str
    source_instance: str
    file_identity: str
    accepted_schema_versions: tuple
    data_class: str='internal'
    synthetic_only: bool=True
    collection_mode: str='incremental'

    @classmethod
    def enroll(cls,source_id,kind,locator,source_instance,accepted_schema_versions,*,data_class='internal',collection_mode='incremental'):
        """Trusted local enrollment; does not write source or destination."""
        return cls(source_id,kind,str(plain(locator)),source_instance,file_identity(locator),tuple(accepted_schema_versions),data_class,True,collection_mode)

    def validate(self,stream):
        for value in (self.source_id,self.source_instance,stream):_token(value)
        if len(self.source_id)>60 or len(self.source_instance)>80:raise ValueError('source namespace too long')
        if self.kind not in STREAMS or stream not in STREAMS[self.kind]:raise ValueError('unsupported source stream')
        if self.synthetic_only is not True or self.data_class not in ('public','internal','local_only'):raise ValueError('synthetic source required')
        if not self.file_identity or not self.accepted_schema_versions:raise ValueError('pinned identity/schema required')
        if self.collection_mode not in ('incremental','snapshot_only'):raise ValueError('invalid collection mode')

    def fingerprint(self):
        return digest({'kind':self.kind,'file_identity':self.file_identity,'instance':self.source_instance,
                       'class':self.data_class,'synthetic_only':self.synthetic_only,'collection_mode':self.collection_mode})


def readonly(path):
    conn=sqlite3.connect(local_sqlite_file(path).as_uri()+'?mode=ro',uri=True,timeout=5)
    conn.row_factory=sqlite3.Row
    conn.execute('PRAGMA query_only=ON')
    def authorize(action,arg1,arg2,*unused):
        allowed=action in (sqlite3.SQLITE_SELECT,sqlite3.SQLITE_READ)
        allowed |= action==sqlite3.SQLITE_FUNCTION and (arg2 or '').lower() in ('count','max','min','coalesce')
        allowed |= action==sqlite3.SQLITE_TRANSACTION and arg1 in ('BEGIN','ROLLBACK')
        allowed |= action==sqlite3.SQLITE_PRAGMA and arg1=='table_info' and arg2 is not None
        return sqlite3.SQLITE_OK if allowed else sqlite3.SQLITE_DENY
    conn.set_authorizer(authorize)
    conn.execute('BEGIN')
    return conn


def source_info(conn,binding,stream,*,require_stream=True):
    columns=[list(r) for r in conn.execute('PRAGMA table_info('+stream+')')]
    if not columns and require_stream:raise ValueError('missing source table')
    if binding.kind=='dispatch':
        version=conn.execute('SELECT MAX(version) FROM schema_migrations').fetchone()[0]
        task_columns={row[1]:row for row in conn.execute('PRAGMA table_info(tasks)')}
        if 'access' not in task_columns or task_columns['access'][2:4]!=('TEXT',1):
            raise ValueError('not the dispatch task schema')
        expected={'events':'seq task_id from_status to_status fence reason actor at',
                  'control_events':'seq event_id event_key kind task_id fence actor at refs_json detail_code',
                  'schedule_events':'seq schedule_id version kind at detail_code',
                  'reconciliations':'id task_id epoch method result evidence_ref decided_by at',
                  'host_events':'task_id fence seq event_ref'}
        if [row[1] for row in columns]!=expected[stream].split():raise ValueError('unsupported dispatch stream schema')
        if conn.execute("SELECT 1 FROM tasks WHERE access IS NOT 'synthetic' LIMIT 1").fetchone():raise ValueError('non-synthetic task')
    else:
        table='source_meta' if binding.kind=='catalog' else 'collector_fixture_meta'
        meta=dict(conn.execute('SELECT key,value FROM '+table))
        version=meta['schema']
        if meta['instance']!=binding.source_instance:raise LookupError('instance_changed')
        if binding.kind!='catalog' and meta.get('synthetic_only')!='true':raise ValueError('not a fixture')
        if binding.kind=='catalog' and version not in ('m0-synthetic-research-catalog-v1','m0-signalhub-summary-fixture-v1'):raise ValueError('not a synthetic catalog')
    return str(version),digest(columns)


def raw_ref(value,owner,ident):
    if not isinstance(value,str) or not re.fullmatch(r'sha256:[0-9a-f]{64}',value):raise ValueError('invalid CAS ref')
    _token(ident)
    return f'{owner}/artifact/{ident}@1#{value[7:]}'


def enrich(source,binding,stream,row):
    row=dict(row)
    if stream=='changes':
        meta=dict(source.execute('SELECT key,value FROM source_meta'))
        version=source.execute('SELECT sha256 FROM record_versions WHERE ref_id=? AND version=?',
                               (row['ref_id'],row['version'])).fetchone()
        if not version:raise ValueError('change has no immutable version')
        row['_fixed_ref']=f"{meta['domain']}/note/{row['ref_id']}@{row['version']}#{version[0]}"
    return row


def project(binding,stream,row,at):
    """Whitelist projection. Raw body/argv/path/reason never persists."""
    seq=row['_collector_seq']; task=row.get('task_id'); fence=row.get('fence',row.get('epoch'))
    event={'actor':'collector','component':binding.kind,'at':row.get('at',at),
           'task_id':task,'attempt_epoch':fence,'refs':[],'metadata':{},'kind':'hook.observed'}
    if stream=='events':
        old,new=row.get('from_status'),row['to_status']
        if new not in _STATUSES or (old is not None and old not in _STATUSES):raise ValueError('invalid task state')
        kind='task.submitted' if old is None and new=='queued' else 'task.succeeded' if new=='no_new' else 'task.'+new
        if old=='unknown' and new=='succeeded':kind='task.reconciled'
        event.update(kind=kind,actor=row['actor'],metadata={'to_status':new,**({'from_status':old} if old else {})})
    elif stream in ('control_events','schedule_events'):
        event['kind']=row['kind']
        if stream=='control_events':
            event['actor']=row['actor'];event['refs']=[raw_ref(ref,'dispatch',row['event_id']) for ref in json.loads(row['refs_json'])]
    elif stream=='reconciliations':
        event.update(kind='reconciliation.observed',actor=row['decided_by'])
        if row.get('evidence_ref'):event['refs']=[raw_ref(row['evidence_ref'],'dispatch',row['id'])]
    elif stream=='host_events':
        event.update(kind='host.observed',refs=[raw_ref(row['event_ref'],'dispatch',task)])
        # This source has no wall timestamp. Use collection observation time,
        # never the mutable host_attempts.updated_at as an anchor.
    elif stream=='changes':
        if row['op'] not in ('upsert','revoke','delete','restore'):raise ValueError('invalid change operation')
        _token(row['ref_id'])
        event['kind']='revocation.propagated' if row['op'] in ('revoke','delete') else 'knowledge.changed'
        event['refs']=[row['_fixed_ref']]
    elif stream=='hook_events':
        body=json.loads(row['body']);event.update(at=body['at'],actor='audit-hook',component='audit-hook',
            metadata={'hook_event':body['event'],'mode':body['mode'],'decision':body['decision']})
        if body.get('schemaVersion')!=1:raise ValueError('unsupported hook schema')
    elif stream=='handoffs':
        state=row['state']
        event.update(kind='handoff.'+state,at=row['at'],actor='handoff-adapter',component='handoff')
        event['refs']=json.loads(row['refs_json'])
        if state=='completed' and not event['refs']:raise ValueError('receipt required')
    for key in ('actor','component'):_token(event[key])
    if task is not None:_token(task)
    if fence is not None and (type(fence) is not int or fence<0):raise ValueError('invalid fence')
    if event['kind'] not in _KINDS:raise ValueError('unsupported event kind')
    if any(not isinstance(ref,str) or not _REF.fullmatch(ref) for ref in event['refs']):raise ValueError('invalid fixed ref')
    if any(k not in _META_VALUES or v not in _META_VALUES[k] for k,v in event['metadata'].items()):raise ValueError('invalid metadata')
    event['at']=_at(event['at'])
    event['trace_id']=task or binding.source_id+':'+stream
    # Count omitted free prose without retaining it. Nested Hook fields are
    # omitted in exactly the same way; the raw hash is only an anchor.
    redacted=sum(bool(row.get(key)) for key in ('reason','detail','detail_code','body'))
    return event,redacted


class Collector:
    def __init__(self,target,*,clock=None):
        self.target=plain(target);self.clock=clock or Clock()

    def _connect(self):
        c=sqlite3.connect(local_sqlite_file(self.target),timeout=10);c.row_factory=sqlite3.Row;return c

    def _source_snapshot(self, target, binding, target_identity):
        source_path=local_sqlite_file(binding.locator)
        if file_identity(source_path)==target_identity==file_identity(self.target):
            # BEGIN IMMEDIATE already provides the source snapshot. A second
            # connection's read lock blocks cache spill in rollback-journal mode.
            return nullcontext(target)
        return closing(readonly(source_path))

    def _gap(self,c,b,stream,kind,last,maximum,at):
        existing=c.execute('SELECT gap_id FROM collector_gaps WHERE source_id=? AND stream=? AND resolved_at IS NULL',(b.source_id,stream)).fetchone()
        if not existing:
            ident='gap_'+digest([b.source_id,stream,kind,last,maximum,at])[:32]
            c.execute('INSERT INTO collector_gaps VALUES(?,?,?,?,?,?,?,NULL,NULL,NULL)',(ident,b.source_id,stream,kind,last,maximum,at))
        return {'state':'frozen','kind':kind,'last_seq':last,'projection_lag':max(0,maximum-last)}

    def sync(self,binding,stream,*,limit=100,fault=None):
        """Atomic events+cursor. fault is a trusted test callback, never a CLI field."""
        binding.validate(stream)
        if type(limit) is not int or not 1<=limit<=1000:raise ValueError('invalid batch bound')
        if binding.collection_mode=='snapshot_only':
            if file_identity(binding.locator)!=binding.file_identity:raise ValueError('source identity mismatch')
            with closing(readonly(binding.locator)) as source:
                version,_=source_info(source,binding,stream,require_stream=False)
                if version not in binding.accepted_schema_versions:raise ValueError('unaccepted schema')
            return {'state':'snapshot_only','incremental':'unsupported','added':0,
                    'source_instance':binding.source_instance,'projection_lag':None}
        at=timestamp(self.clock.now())
        target_identity=file_identity(self.target)
        with closing(self._connect()) as c,c:
            c.execute('BEGIN IMMEDIATE')
            prior=c.execute('SELECT * FROM collector_cursors WHERE source_id=? AND stream=?',(binding.source_id,stream)).fetchone()
            last=prior['last_seq'] if prior else 0
            gap=c.execute('SELECT kind FROM collector_gaps WHERE source_id=? AND stream=? AND resolved_at IS NULL',(binding.source_id,stream)).fetchone()
            if gap:return {'state':'frozen','kind':gap['kind'],'last_seq':last}
            if (prior and (prior['authority']!='local_owned' or prior['source_instance']!=binding.source_instance or prior['binding_sha256']!=binding.fingerprint())) or file_identity(binding.locator)!=binding.file_identity:
                return self._gap(c,binding,stream,'instance_changed',last,last,at)
            with self._source_snapshot(c,binding,target_identity) as source:
                try:version,schema=source_info(source,binding,stream)
                except LookupError:return self._gap(c,binding,stream,'instance_changed',last,last,at)
                except (ValueError,KeyError,sqlite3.DatabaseError):return self._gap(c,binding,stream,'schema_changed',last,last,at)
                if version not in binding.accepted_schema_versions or (prior and (prior['schema_sha256']!=schema or prior['schema_version']!=version)):
                    return self._gap(c,binding,stream,'schema_changed',last,last,at)
                # rowid is the global sequence for composite host_events and UUID
                # reconciliations; integer-primary-key streams alias it naturally.
                maximum=source.execute('SELECT COALESCE(MAX(rowid),0) FROM '+stream).fetchone()[0]
                if last:
                    anchor=source.execute('SELECT rowid AS _collector_seq,* FROM '+stream+' WHERE rowid=?',(last,)).fetchone()
                    if anchor is None:return self._gap(c,binding,stream,'source_truncated',last,maximum,at)
                    try:anchor=enrich(source,binding,stream,anchor)
                    except (ValueError,KeyError,sqlite3.DatabaseError):return self._gap(c,binding,stream,'source_rewritten',last,maximum,at)
                    if digest(anchor)!=prior['anchor_sha256']:return self._gap(c,binding,stream,'source_rewritten',last,maximum,at)
                try:rows=[enrich(source,binding,stream,r) for r in source.execute('SELECT rowid AS _collector_seq,* FROM '+stream+' WHERE rowid>? ORDER BY rowid LIMIT ?',(last,limit))]
                except (ValueError,KeyError,sqlite3.DatabaseError):return self._gap(c,binding,stream,'redaction_rejected',last,maximum,at)
                if any(r['_collector_seq']!=last+i+1 for i,r in enumerate(rows)):
                    return self._gap(c,binding,stream,'seq_gap',last,maximum,at)
                try:projected=[project(binding,stream,r,at) for r in rows]
                except (ValueError,KeyError,TypeError):return self._gap(c,binding,stream,'redaction_rejected',last,maximum,at)
                epoch=prior['epoch'] if prior else 1
                namespace=f'{binding.source_id}@{binding.source_instance}:e{epoch}'
                for row,(event,redacted) in zip(rows,projected):
                    key=stream+':'+str(row['_collector_seq'])
                    c.execute('''INSERT INTO unified_events(event_id,source,source_id,trace_id,task_id,attempt_epoch,actor,component,kind,at,refs_json,class,metadata_json)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',(_ulid(event['at']),namespace,key,event['trace_id'],event['task_id'],event['attempt_epoch'],event['actor'],event['component'],event['kind'],event['at'],json.dumps(event['refs']),binding.data_class,canonical(event['metadata'])))
                if fault:fault('events_inserted')
                new_last=rows[-1]['_collector_seq'] if rows else last
                anchor=digest(rows[-1]) if rows else (prior['anchor_sha256'] if prior else None)
                count=(prior['redacted_count'] if prior else 0)+sum(item[1] for item in projected)
                c.execute('''INSERT INTO collector_cursors VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                  ON CONFLICT(source_id,stream) DO UPDATE SET last_seq=excluded.last_seq,anchor_sha256=excluded.anchor_sha256,updated_at=excluded.updated_at,redacted_count=excluded.redacted_count''',
                  (binding.source_id,stream,binding.source_instance,epoch,new_last,anchor,at,binding.fingerprint(),schema,version,'local_owned',count))
                if fault:fault('cursor_updated')
                return {'state':'synced','added':len(rows),'last_seq':new_last,'projection_lag':max(0,maximum-new_last),'epoch':epoch,'redacted':sum(item[1] for item in projected)}

    def adopt(self,binding,stream,*,reason,actor,resume_after=0):
        """Explicit trusted action; skip only an operator-selected existing anchor.

        reason is an enum, never free prose. Old events/gaps remain historical.
        New binding must already be locally enrolled against the actual source.
        """
        binding.validate(stream);_token(actor)
        if binding.collection_mode!='incremental':raise ValueError('snapshot source has no cursor to adopt')
        if reason not in REASONS:raise ValueError('enumerated adoption reason required')
        if type(resume_after) is not int or resume_after<0:raise ValueError('invalid adoption anchor')
        if file_identity(binding.locator)!=binding.file_identity:raise ValueError('source identity mismatch')
        at=timestamp(self.clock.now())
        target_identity=file_identity(self.target)
        with closing(self._connect()) as c,c:
            c.execute('BEGIN IMMEDIATE')
            prior=c.execute('SELECT * FROM collector_cursors WHERE source_id=? AND stream=?',(binding.source_id,stream)).fetchone()
            gap=c.execute('SELECT 1 FROM collector_gaps WHERE source_id=? AND stream=? AND resolved_at IS NULL',(binding.source_id,stream)).fetchone()
            if not gap and (not prior or prior['authority']=='local_owned'):raise ValueError('adoption requires frozen or imported stream')
            with self._source_snapshot(c,binding,target_identity) as source:
                version,schema=source_info(source,binding,stream)
                if version not in binding.accepted_schema_versions:raise ValueError('unaccepted schema')
                row=source.execute('SELECT rowid AS _collector_seq,* FROM '+stream+' WHERE rowid=?',(resume_after,)).fetchone() if resume_after else None
                if resume_after and row is None:raise ValueError('adoption anchor absent')
                epoch=(prior['epoch'] if prior else 1)+1
                if not gap:
                    self._gap(c,binding,stream,'instance_changed',prior['last_seq'],resume_after,at)
                c.execute('INSERT OR REPLACE INTO collector_cursors VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(binding.source_id,stream,binding.source_instance,epoch,resume_after,digest(enrich(source,binding,stream,row)) if row else None,at,binding.fingerprint(),schema,version,'local_owned',prior['redacted_count'] if prior else 0))
                c.execute('UPDATE collector_gaps SET resolved_at=?,resolution=?,resolved_by=? WHERE source_id=? AND stream=? AND resolved_at IS NULL',
                    (at,canonical({'reason':reason,'resume_after':resume_after,'epoch':epoch}),actor,binding.source_id,stream))
                return {'state':'adopted','epoch':epoch,'last_seq':resume_after}
