"""Explicit synthetic-source collector. Readers never instantiate this writer.

Bindings are trusted local configuration, not request authorization. Enrollment
pins a caller-assigned instance plus OS file identity; catalog markers are also
checked. No path alone can authorize resuming an imported cursor.
"""
from contextlib import closing, contextmanager
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
from .campaign_validation import DEVICES, PREFIX
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
         'catalog':('changes',),'fixture_hook':('hook_events',),'fixture_handoff':('handoffs',),
         'content':('content_events',)}
REASONS=frozenset(('accept_new_instance','accept_gap','accept_rewrite','accept_schema','accept_redaction','bind_imported_history'))
CONTENT_SCHEMA='m0-content-synthetic-v1'
CONTENT_WORKFLOW_SCHEMA='m0.research_content_workflow.v1'
# Only these three producer tables are read. Pin their DDL including constraints
# so a view, generated column, or non-rowid replacement cannot pass enrollment.
CONTENT_TABLES={
    'meta':'CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)',
    'artifacts':('CREATE TABLE artifacts(ref TEXT PRIMARY KEY, identity TEXT NOT NULL, version INTEGER NOT NULL, '
                 'trace_id TEXT NOT NULL, payload TEXT NOT NULL, UNIQUE(identity,version))'),
    'content_events':('CREATE TABLE content_events(seq INTEGER PRIMARY KEY, event_id TEXT UNIQUE NOT NULL, '
                      'trace_id TEXT NOT NULL, operation TEXT NOT NULL, kind TEXT NOT NULL, at TEXT NOT NULL, '
                      'refs_json TEXT NOT NULL, metadata_json TEXT NOT NULL)'),
}
CONTENT_TOKEN=re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,100}')
CONTENT_REF=re.compile(r'([a-z][a-z0-9_-]*/[a-z][a-z0-9_-]*/[A-Za-z0-9:_-]+)@([1-9][0-9]*)#([0-9a-f]{64})')
CONTENT_OPERATIONS={
    'artifact.put': ('content-fixture-artifact', (None,)),
    'content.script_from_context': ('content-script', ('creative/context', 'mgmt/script')),
    'approval.record': ('content-approval', ('mgmt/approval',)),
    'publish.prepare': ('content-fixture-publish', ('webee/publish_prep',)),
    'fixture.submit_unknown': ('content-fixture-publish', ('webee/publish_prep',)),
    'publish.observe': ('content-fixture-publish', ('webee/receipt',)),
    'publish.metrics_import': ('content-fixture-publish', ('webee/metrics_obs',)),
    'publish.metrics_match': ('content-fixture-publish', ('webee/metrics_match',)),
    'strategy.activate': ('content-fixture-strategy', ('mgmt/strategy_change',)),
    'strategy.rollback': ('content-fixture-strategy', ('mgmt/strategy_change',)),
}
CONTENT_OUTCOMES={
    'approval.record': {'approval.decided': {'decision':'observed'}},
    'fixture.submit_unknown': {'task.unknown': {'to_status':'unknown'}},
    'publish.observe': {'task.reconciled': {'to_status':'succeeded'}, 'task.failed': {'to_status':'failed'},
                        'task.cancelled': {'to_status':'cancelled'}},
}


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


def binding_identity_matches(binding):
    try:return file_identity(binding.locator)==binding.file_identity
    except (FileNotFoundError,ValueError):return False


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
    profile: str | None=None

    @classmethod
    def enroll(cls,source_id,kind,locator,source_instance,accepted_schema_versions,*,data_class='internal',collection_mode='incremental'):
        """Trusted local enrollment; does not write source or destination."""
        return cls(source_id,kind,str(plain(locator)),source_instance,file_identity(locator),tuple(accepted_schema_versions),data_class,True,collection_mode)

    def validate(self,stream):
        for value in (self.source_id,self.source_instance,stream):_token(value)
        if len(self.source_id)>60 or len(self.source_instance)>80:raise ValueError('source namespace too long')
        if self.kind not in STREAMS or stream not in STREAMS[self.kind]:raise ValueError('unsupported source stream')
        if self.profile is None:
            if self.synthetic_only is not True or self.data_class not in ('public','internal','local_only'):raise ValueError('synthetic source required')
        elif (self.profile!='common_metadata_0013' or self.kind!='dispatch' or self.synthetic_only is not False
              or self.data_class!='local_only' or tuple(self.accepted_schema_versions)!=('0013',)
              or self.collection_mode!='incremental' or stream not in ('events','control_events','schedule_events')):
            raise ValueError('invalid common metadata profile')
        if not self.file_identity or not self.accepted_schema_versions:raise ValueError('pinned identity/schema required')
        if self.collection_mode not in ('incremental','snapshot_only'):raise ValueError('invalid collection mode')

    def fingerprint(self):
        value={'kind':self.kind,'file_identity':self.file_identity,'instance':self.source_instance,
               'class':self.data_class,'synthetic_only':self.synthetic_only,'collection_mode':self.collection_mode}
        if self.profile is not None:value['profile']=self.profile
        return digest(value)


def readonly(path):
    conn=sqlite3.connect(local_sqlite_file(path).as_uri()+'?mode=ro',uri=True,timeout=5)
    conn.row_factory=sqlite3.Row
    conn.execute('PRAGMA query_only=ON')
    def authorize(action,arg1,arg2,*unused):
        allowed=action in (sqlite3.SQLITE_SELECT,sqlite3.SQLITE_READ)
        allowed |= action==sqlite3.SQLITE_FUNCTION and (arg2 or '').lower() in ('count','max','min','coalesce')
        allowed |= action==sqlite3.SQLITE_TRANSACTION and arg1 in ('BEGIN','ROLLBACK')
        allowed |= action==sqlite3.SQLITE_PRAGMA and ((arg1=='table_info' and arg2 is not None) or (arg1=='user_version' and arg2 is None))
        return sqlite3.SQLITE_OK if allowed else sqlite3.SQLITE_DENY
    conn.set_authorizer(authorize)
    conn.execute('BEGIN')
    return conn


def source_info(conn,binding,stream,*,require_stream=True):
    if binding.kind=='content':
        definitions={row[0]:' '.join(row[1].split()) for row in conn.execute(
            "SELECT name,sql FROM sqlite_schema WHERE type='table' AND name IN ('meta','artifacts','content_events')")}
        if definitions!=CONTENT_TABLES:raise ValueError('unsupported content table schema')
        meta=dict(conn.execute('SELECT key,value FROM meta'))
        if 'workflow_schema' in meta and meta.pop('workflow_schema')!=CONTENT_WORKFLOW_SCHEMA:
            raise ValueError('unsupported content workflow schema')
        if set(meta)!={'schema','instance','platform_calls'} or meta['schema']!=CONTENT_SCHEMA or meta['platform_calls']!='0':
            raise ValueError('not a synthetic content source')
        if meta['instance']!=binding.source_instance:raise LookupError('instance_changed')
        return CONTENT_SCHEMA,digest(definitions)
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
        if binding.profile=='common_metadata_0013':
            from .external_migration import verify_schema
            if verify_schema(conn)!='0013':raise ValueError('common metadata requires verified 0013')
            if conn.execute("SELECT 1 FROM tasks WHERE (execution_route IS NULL OR execution_route IN ('http_synthetic','local_synthetic')) AND (access!='synthetic' OR egress!='none') LIMIT 1").fetchone():
                raise ValueError('synthetic route changed')
            if conn.execute("SELECT 1 FROM tasks WHERE execution_route='agent_external' AND (kind!='agent.run' OR kind_version!=1 OR side_effect!='external' OR access!='exportable' OR egress!='derived' OR max_attempts!=1) LIMIT 1").fetchone():
                raise ValueError('invalid external metadata source')
        elif conn.execute("SELECT 1 FROM tasks WHERE access IS NOT 'synthetic' LIMIT 1").fetchone():raise ValueError('non-synthetic task')
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


def content_refs(source,row):
    if any(not isinstance(value,str) for key,value in row.items() if key not in ('seq','_collector_seq')):
        raise ValueError('content event text fields must be text')
    operation=row['operation']
    component,expected=CONTENT_OPERATIONS[operation]
    refs=json.loads(row['refs_json'])
    if not isinstance(refs,list) or len(refs)!=len(expected):raise ValueError('invalid content refs')
    for ref,prefix in zip(refs,expected):
        match=CONTENT_REF.fullmatch(ref) if isinstance(ref,str) and len(ref)<=256 else None
        if not match:raise ValueError('invalid content ref')
        identity,version,sha=match.groups()
        owner_kind=identity.rsplit('/',1)[0]
        if prefix is not None and owner_kind!=prefix:raise ValueError('unexpected content ref kind')
        artifact=source.execute('SELECT identity,version,trace_id,payload FROM artifacts WHERE ref=?',(ref,)).fetchone()
        if artifact is None or tuple(artifact)[:3]!=(identity,int(version),row['trace_id']):
            raise ValueError('content ref is outside the event trace')
        raw=artifact['payload']
        if not isinstance(raw,str) or len(raw)>1024*1024:raise ValueError('invalid content artifact size')
        payload=json.loads(raw)
        owner,kind,ident=identity.split('/')
        if (not isinstance(payload,dict) or payload.get('synthetic') is not True or
                payload.get('class')!='internal' or not isinstance(payload.get('body'),dict) or
                [payload.get(key) for key in ('owner','kind','id','version','trace_id')]!=
                [owner,kind,ident,int(version),row['trace_id']] or digest(payload)!=sha):
            raise ValueError('content artifact does not match its fixed ref')
    body=payload['body']
    if operation=='content.script_from_context' and (payload.get('derived_from')!=refs[:1] or body.get('context_ref')!=refs[0] or
            body.get('generation')!='template_synthetic' or body.get('ai_generated') is not False):
        raise ValueError('content script does not bind its synthetic context')
    if operation in ('content.script_from_context','approval.record') and (
            body.get('human_reviewed') is not False or body.get('publish_authorized') is not False):
        raise ValueError('content fixture cannot authorize human review or publishing')
    if operation=='publish.observe':
        states={'task.reconciled':'published','task.failed':'rejected','task.cancelled':'withdrawn'}
        if body.get('verification_status')!='unverified_import' or body.get('observed_status')!=states.get(row['kind']):
            raise ValueError('content receipt is not a matching fixture observation')
    return component,refs


def enrich(source,binding,stream,row):
    row=dict(row)
    if stream=='changes':
        meta=dict(source.execute('SELECT key,value FROM source_meta'))
        version=source.execute('SELECT sha256 FROM record_versions WHERE ref_id=? AND version=?',
                               (row['ref_id'],row['version'])).fetchone()
        if not version:raise ValueError('change has no immutable version')
        row['_fixed_ref']=f"{meta['domain']}/note/{row['ref_id']}@{row['version']}#{version[0]}"
    elif stream=='content_events':
        row['_component'],row['_refs']=content_refs(source,row)
    return row


def _validate_campaign_event(binding, stream, row):
    """Recognize only the metadata shapes produced by a common campaign."""
    if binding.profile != 'common_metadata_0013':
        raise ValueError('campaign event requires the common metadata profile')
    campaign_id = r'[A-Za-z0-9][A-Za-z0-9_.-]{0,47}'
    prefix = re.escape(PREFIX) + campaign_id
    if row['kind'] == 'campaign.context_read':
        refs = json.loads(row['refs_json']) if stream == 'control_events' else None
        if (stream != 'control_events' or row['actor'] != DEVICES['reviewer'] or row['fence'] != 1
                or row['detail_code'] != 'observed'
                or not re.fullmatch(r't_[0-9a-f]{32}', row['task_id'] or '')
                or not re.fullmatch(r'[0-7][0-9A-HJKMNP-TV-Z]{25}', row['event_id'])
                or not re.fullmatch('campaign-context:' + prefix + r':slot:(?:[1-9]|[1-3][0-9]|4[0-8])',
                                    row['event_key'])
                or not isinstance(refs, list) or len(refs) != 3
                or any(not isinstance(ref, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', ref) for ref in refs)):
            raise ValueError('invalid campaign context event')
    elif (stream != 'schedule_events' or row['version'] != 1 or row['detail_code'] != 'operator_stop'
          or not re.fullmatch(prefix + r':(?:producer|reviewer)', row['schedule_id'])):
        raise ValueError('invalid campaign pause event')


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
        if row['kind'] in ('campaign.context_read', 'campaign.paused'):
            _validate_campaign_event(binding, stream, row)
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
    elif stream=='content_events':
        trace=row['trace_id'];kind=row['kind'];metadata=json.loads(row['metadata_json'])
        if not isinstance(trace,str) or not CONTENT_TOKEN.fullmatch(trace):raise ValueError('invalid content trace')
        if not isinstance(row['event_id'],str) or not re.fullmatch('[0-7][0-9A-HJKMNP-TV-Z]{25}',row['event_id']):
            raise ValueError('invalid content event id')
        outcome=CONTENT_OUTCOMES.get(row['operation'],{'task.succeeded':{}})
        if not isinstance(metadata,dict) or metadata!=outcome.get(kind) or canonical(metadata)!=row['metadata_json']:
            raise ValueError('invalid content event outcome')
        event.update(kind=kind,component=row['_component'],refs=row['_refs'],metadata=metadata,trace_id=trace)
    for key in ('actor','component'):_token(event[key])
    if task is not None:_token(task)
    if fence is not None and (type(fence) is not int or fence<0):raise ValueError('invalid fence')
    if event['kind'] not in _KINDS:raise ValueError('unsupported event kind')
    if any(not isinstance(ref,str) or not _REF.fullmatch(ref) for ref in event['refs']):raise ValueError('invalid fixed ref')
    if any(k not in _META_VALUES or v not in _META_VALUES[k] for k,v in event['metadata'].items()):raise ValueError('invalid metadata')
    event['at']=_at(event['at'])
    if stream=='content_events':
        if event['at']<'1970-01-01':raise ValueError('content timestamp outside range')
    else:
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

    @contextmanager
    def _source_snapshot(self, target, binding, target_identity):
        if not binding_identity_matches(binding):
            yield None
            return
        source_path=local_sqlite_file(binding.locator)
        if binding.file_identity==target_identity==file_identity(self.target):
            if binding.kind=='content':raise ValueError('content source must be separate from target')
            # BEGIN IMMEDIATE already provides the source snapshot. A second
            # connection's read lock blocks cache spill in rollback-journal mode.
            yield target
            return
        if binding.kind=='content':
            # A read-only SQLite WAL connection may still create a SHM file.
            # Read the header before holding any SQLite lock on the content file.
            with source_path.open('rb') as file:
                header=file.read(20)
            if not header.startswith(b'SQLite format 3\0') or header[18:20]!=b'\x01\x01':
                yield False
                return
        with closing(readonly(source_path)) as source:
            yield source if binding_identity_matches(binding) else None

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
            with self._source_snapshot(None,binding,None) as source:
                if not source:raise ValueError('source identity or journal mode mismatch')
                version,_=source_info(source,binding,stream,require_stream=False)
                if version not in binding.accepted_schema_versions:raise ValueError('unaccepted schema')
            return {'state':'snapshot_only','incremental':'unsupported','added':0,
                    'source_instance':binding.source_instance,'projection_lag':None}
        at=timestamp(self.clock.now())
        target_identity=file_identity(self.target)
        if binding.kind=='content' and binding.file_identity==target_identity:raise ValueError('content source must be separate from target')
        with closing(self._connect()) as c,c:
            c.execute('BEGIN IMMEDIATE')
            prior=c.execute('SELECT * FROM collector_cursors WHERE source_id=? AND stream=?',(binding.source_id,stream)).fetchone()
            last=prior['last_seq'] if prior else 0
            gap=c.execute('SELECT kind FROM collector_gaps WHERE source_id=? AND stream=? AND resolved_at IS NULL',(binding.source_id,stream)).fetchone()
            if gap:return {'state':'frozen','kind':gap['kind'],'last_seq':last}
            if prior and (prior['authority']!='local_owned' or prior['source_instance']!=binding.source_instance or prior['binding_sha256']!=binding.fingerprint()):
                return self._gap(c,binding,stream,'instance_changed',last,last,at)
            with self._source_snapshot(c,binding,target_identity) as source:
                if source is None:return self._gap(c,binding,stream,'instance_changed',last,last,at)
                if source is False:return self._gap(c,binding,stream,'schema_changed',last,last,at)
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
                    except (ValueError,KeyError,TypeError,RecursionError,sqlite3.DatabaseError):return self._gap(c,binding,stream,'source_rewritten',last,maximum,at)
                    if digest(anchor)!=prior['anchor_sha256']:return self._gap(c,binding,stream,'source_rewritten',last,maximum,at)
                try:rows=[enrich(source,binding,stream,r) for r in source.execute('SELECT rowid AS _collector_seq,* FROM '+stream+' WHERE rowid>? ORDER BY rowid LIMIT ?',(last,limit))]
                except (ValueError,KeyError,TypeError,RecursionError,sqlite3.DatabaseError):return self._gap(c,binding,stream,'redaction_rejected',last,maximum,at)
                if any(r['_collector_seq']!=last+i+1 for i,r in enumerate(rows)):
                    return self._gap(c,binding,stream,'seq_gap',last,maximum,at)
                try:projected=[project(binding,stream,r,at) for r in rows]
                except (ValueError,KeyError,TypeError,AttributeError,OverflowError,RecursionError):return self._gap(c,binding,stream,'redaction_rejected',last,maximum,at)
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
        if binding.kind=='content' and binding.file_identity==target_identity:raise ValueError('content source must be separate from target')
        with closing(self._connect()) as c,c:
            c.execute('BEGIN IMMEDIATE')
            prior=c.execute('SELECT * FROM collector_cursors WHERE source_id=? AND stream=?',(binding.source_id,stream)).fetchone()
            gap=c.execute('SELECT 1 FROM collector_gaps WHERE source_id=? AND stream=? AND resolved_at IS NULL',(binding.source_id,stream)).fetchone()
            if not gap and (not prior or prior['authority']=='local_owned'):raise ValueError('adoption requires frozen or imported stream')
            with self._source_snapshot(c,binding,target_identity) as source:
                if not source:raise ValueError('source identity or journal mode mismatch')
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
