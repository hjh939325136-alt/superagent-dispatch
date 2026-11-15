"""Bounded read-only plans. No store constructor, DML, migration, or VACUUM."""
from contextlib import ExitStack
from datetime import timedelta
import hashlib
import sqlite3
from .binding import fixture_blob_enrolled,load_binding
from .common import (MAX_CANDIDATES,MAX_ROWS,canonical,cas_inventory,digest,fail,files,
    identity,instant,json_file,now,parse,read,readonly_db,sealed,sha)
from .refs import References
from sa_dispatch.bundle import SNAPSHOT_TABLES
from sa_dispatch.collector import SourceBinding,source_info,enrich,project

def ledger_state(db):
    schema=[dict(r) for r in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name') if not r['name'].startswith('sqlite_')]
    tables=[r['name'] for r in schema if r['type']=='table']
    known=set(SNAPSHOT_TABLES);unknown=sorted(set(tables)-known)
    unknown.extend(r['type']+':'+r['name'] for r in schema if r['type'] in ('trigger','view'))
    if set(known)-set(tables):fail('E_LEDGER_SCHEMA')
    versions=[r[0] for r in db.execute('SELECT version FROM schema_migrations ORDER BY version')]
    if not versions or versions[-1]!='0011':fail('E_LEDGER_VERSION')
    if db.execute("SELECT 1 FROM tasks WHERE access IS NOT 'synthetic' LIMIT 1").fetchone():fail('E_NON_SYNTHETIC')
    h=hashlib.sha256(canonical(schema));counts={};rows={};total=0
    for table in sorted(tables):
        # SQL identifiers come from a schema whitelist, never request text.
        if table not in known:continue
        counts[table]=0
        if table not in ('unified_events','events','schedule_events'):rows[table]=[]
        for row in db.execute('SELECT * FROM "'+table+'" ORDER BY '+SNAPSHOT_TABLES[table]):
            value=dict(row);h.update(canonical([table,value]));counts[table]+=1;total+=1
            if total>MAX_ROWS:fail('E_ROW_LIMIT')
            if table in rows:rows[table].append(value)
    # Unknown schema is never eligible for apply; no unknown table is read.
    return {'sha256':h.hexdigest(),'counts':counts,'rows':rows,'unknown_tables':unknown,'schema':schema}

def _source(binding,entry,cursors,gaps,stack):
    bound=SourceBinding(**{k:v for k,v in entry.items() if k!='stream'});stream=entry['stream']
    actual=identity(bound.locator)
    if bound.file_identity!=f"{actual['device']}:{actual['inode']}":fail('E_SOURCE_IDENTITY')
    db=stack.enter_context(readonly_db(bound.locator));version,schema=source_info(db,bound,stream)
    if version not in bound.accepted_schema_versions:fail('E_SOURCE_SCHEMA')
    h=hashlib.sha256();count=0;contiguous=True
    for row in db.execute('SELECT rowid AS _collector_seq,* FROM '+stream+' ORDER BY rowid'):
        h.update(canonical(enrich(db,bound,stream,row)));count+=1
        contiguous &= row['_collector_seq']==count
        if count>MAX_ROWS:fail('E_SOURCE_ROWS')
    cursor=next((r for r in cursors if r['source_id']==bound.source_id and r['stream']==stream),None)
    eligible=False
    if cursor:
        anchor=db.execute('SELECT rowid AS _collector_seq,* FROM '+stream+' WHERE rowid=?',(cursor['last_seq'],)).fetchone()
        eligible=(cursor['authority']=='local_owned' and cursor['source_instance']==bound.source_instance and
            cursor['binding_sha256']==bound.fingerprint() and cursor['schema_sha256']==schema and
            cursor['schema_version']==version and (cursor['last_seq']==0 or
                (anchor is not None and digest(enrich(db,bound,stream,anchor))==cursor['anchor_sha256'])) and
            not any(r['source_id']==bound.source_id and r['stream']==stream and r['resolved_at'] is None for r in gaps))
    namespace=f"{bound.source_id}@{bound.source_instance}:e{cursor['epoch']}" if cursor else None
    return ({'source_id':bound.source_id,'stream':stream,'sha256':h.hexdigest(),'count':count,'identity':actual,
            'eligible':bool(eligible and contiguous and bound.kind=='fixture_hook' and stream=='hook_events'),
            'namespace':namespace,'last_seq':cursor['last_seq'] if cursor else 0},bound,db)

def _matches_hook(row,source,bound,db):
    stream,seq_text=row['source_id'].split(':')
    seq=int(seq_text)
    if stream!=source['stream'] or seq_text!=str(seq) or not 0<seq<=source['last_seq']:
        return False
    raw=db.execute('SELECT rowid AS _collector_seq,* FROM hook_events WHERE rowid=?',(seq,)).fetchone()
    if raw is None:return False
    expected,_=project(bound,stream,enrich(db,bound,stream,raw),row['at'])
    return (all(row[key]==val for key,val in expected.items() if key not in ('refs','metadata')) and
            parse(row['refs_json'])==expected['refs'] and
            parse(row['metadata_json'])==expected['metadata'] and row['class']==bound.data_class)

def inspect_state(binding,db,decision_at):
    ledger=ledger_state(db);inventory=cas_inventory(binding.cas);refs=References(binding.cas,inventory)
    enrollment=(json_file(binding.root/'fixture.json')['generated_objects']
                if binding.mode=='fixture' else None)
    for name in ledger['unknown_tables']:refs.doubt('unknown_table:'+name)
    for table,rows in ledger['rows'].items():
        for row in rows:refs.row(table,row)
    metadata={};generation={}
    for name,schema in [('generations','m0.retention_generations.v1'),('consumers','m0.retention_consumers.v1')]:
        path=binding.root/(name+'.json')
        if not path.exists():refs.doubt('missing_'+name);metadata[name]=None;continue
        value=json_file(path);metadata[name]=digest(value)
        if value.get('schema')!=schema or value.get('instance')!=binding.instance:fail('E_METADATA')
        if name=='generations':generation=value['objects']
        else:
            if value.get('complete') is not True:refs.doubt('incomplete_consumers')
            for ref in value.get('refs',[]):refs.add(ref,'consumer')
    journals={}
    if not binding.journals.exists():refs.doubt('missing_journals')
    else:
        for path in files(binding.journals):
            data=read(path);journals[path.relative_to(binding.journals).as_posix()]=sha(data)
            try:
                if data and not data.endswith(b'\n'):raise ValueError('truncated')
                for line in data.splitlines():
                    event=parse(line);phase=event['phase']
                    if phase=='intent':refs.add(event['request_ref'],'journal.intent')
                    elif phase=='result':refs.add(event['artifact'],'journal.result')
                    elif phase not in ('invoked','completed_ack'):refs.doubt('unknown_journal_phase')
            except (ValueError,KeyError,TypeError):refs.doubt('invalid_journal')
    with ExitStack() as stack:
        snapshots=[_source(binding,entry,ledger['rows']['collector_cursors'],ledger['rows']['collector_gaps'],stack)
                   for entry in binding.sources]
        sources=[snapshot[0] for snapshot in snapshots]
        by_namespace={item[0]['namespace']:item for item in snapshots if item[0]['namespace'] is not None}
        high=db.execute('SELECT COALESCE(MAX(seq),0) FROM unified_events').fetchone()[0]
        candidates=[];preserved={};cutoff=instant(decision_at)-timedelta(days=180)
        for record in db.execute('SELECT * FROM unified_events ORDER BY seq'):
            row=dict(record);reason='unproven_source';snapshot=by_namespace.get(row['source'])
            try:
                if row['seq']==high:reason='sequence_high_watermark'
                elif instant(row['at'])>=cutoff:reason='within_180_days'
                elif row['task_id'] is not None:reason='task_subject'
                elif parse(row['refs_json'])!=[]:reason='pinned_reference'
                elif snapshot and snapshot[0]['eligible'] and _matches_hook(row,*snapshot):reason=None
            except (ValueError,TypeError,KeyError):reason='invalid_event'
            if reason is None:candidates.append({'seq':row['seq'],'event_id':row['event_id'],'sha256':digest(row)})
            else:
                preserved[reason]=preserved.get(reason,0)+1;refs.row('unified_events',row)
            if len(candidates)>MAX_CANDIDATES:fail('E_CANDIDATE_LIMIT')
    refs.close();blobs=[];retained={};output_cutoff=instant(decision_at)-timedelta(days=30)
    for ref,item in inventory.items():
        gen=generation.get(ref);reason=None
        if ref in refs.pins:reason='referenced'
        elif refs.unknown:reason='reference_coverage_unknown'
        elif not isinstance(gen,dict) or gen.get('bytes')!=item['bytes']:reason='age_unproven'
        else:
            try:
                if instant(gen['created_at'])>=output_cutoff:reason='within_30_days'
                elif enrollment is not None and not fixture_blob_enrolled(
                        binding,{'ref':ref,**item,'created_at':gen['created_at']},enrollment):
                    reason='fixture_object_unenrolled'
            except (KeyError,ValueError):reason='age_unproven'
        if reason:retained[reason]=retained.get(reason,0)+1
        else:blobs.append({'ref':ref,**item,'created_at':gen['created_at']})
    state={'ledger_sha256':ledger['sha256'],'ledger_identity':identity(binding.ledger),'cas':inventory,
           'metadata':metadata,'journals':journals,'sources':sources}
    return {'state':state,'state_sha256':digest(state),'events':candidates,'blobs':blobs,'high_seq':high,
            'counts':ledger['counts'],'preserved_events':preserved,'preserved_blobs':retained,
            'pins':refs.pins,'unknown':refs.unknown,'coverage':'complete' if not refs.unknown else 'unknown'}

def plan_retention(binding,decision_at=None):
    if load_binding(binding.path)!=binding:fail('E_BINDING_CHANGED')
    at=decision_at or now();instant(at)
    with readonly_db(binding.ledger,identity=binding.ledger_identity) as db:value=inspect_state(binding,db,at)
    return sealed({'schema':'m0.retention_plan.v1','binding_sha256':binding.digest,'instance':binding.instance,
        'decision_at':at,'event_days':180,'output_days':30,'production_apply':False,**value})
