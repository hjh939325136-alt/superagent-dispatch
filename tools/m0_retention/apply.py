"""Offline synthetic fixture only. Real DELETE/unlink with fixed-set recovery.

The local lease is NOT a lock shared by production CAS producers. No production
apply entry exists. A process-crash drill is not a power-loss durability claim.
"""
from contextlib import closing
from pathlib import Path
import sqlite3
from .binding import fixture_blob_enrolled,require_fixture
from .common import (MAX_BLOB,canonical,cas_path,digest,document,fail,identity,instant,
    json_file,now,quota,read,readonly_db,sealed,secure_mkdir,sha,target_lock,
    unlink_verified,verify_seal,write,directory_handle)
from .plan import inspect_state,ledger_state

def _connection(binding):
    db=sqlite3.connect(binding.ledger.as_uri()+'?mode=rw',uri=True,timeout=2)
    db.row_factory=sqlite3.Row
    return db

def _operation(binding,path):
    path=Path(path)
    if path.parent!=binding.root/'operations' or len(path.name)!=64 or any(c not in '0123456789abcdef' for c in path.name):fail('E_OPERATION_PATH')
    return path

def _checkpoint(stage):
    """Trusted test patch seam; no CLI or request can specify code/commands."""

def apply_fixture(binding,plan_path,fixture_grant):
    require_fixture(binding,fixture_grant)
    plan_path=Path(plan_path)
    if plan_path.parent!=binding.root/'plans':fail('E_PLAN_PATH')
    plan=verify_seal(json_file(plan_path))
    if (plan.get('schema')!='m0.retention_plan.v1' or plan.get('instance')!=binding.instance or
        plan.get('binding_sha256')!=binding.digest or plan.get('production_apply') is not False):fail('E_PLAN_BINDING')
    if instant(plan['decision_at'])>instant(now()):fail('E_PLAN_FUTURE')
    op=binding.root/'operations'/plan['sha256']
    with target_lock(binding.root),directory_handle(binding.root):
        if op.exists():
            if (op/'restored.json').exists():fail('E_PLAN_ALREADY_RESTORED')
            if (op/'receipt.json').exists():return verify_seal(json_file(op/'receipt.json'))
            fail('E_RECOVERY_REQUIRED')
        # This full comparison occurs BEFORE any operation directory or backup.
        with readonly_db(binding.ledger,identity=binding.ledger_identity) as db:
            actual=inspect_state(binding,db,plan['decision_at'])
        if any(actual[k]!=plan.get(k) for k in actual):fail('E_PLAN_CHANGED')
        if any(x.startswith('unknown_table:') for x in plan['unknown']):fail('E_UNKNOWN_SCHEMA')
        if not plan['events'] and not plan['blobs']:fail('E_EMPTY_PLAN')
        backup_bytes=binding.ledger.stat().st_size+sum(b['bytes'] for b in plan['blobs'])+len(canonical(plan))*3+1024*1024
        quota(binding.root,backup_bytes)
        secure_mkdir(op);secure_mkdir(op/'blobs')
        document(binding.root,op/'plan.json',plan)
        # SQLite backup API creates a consistent copy; never copy a live DB file.
        backup=op/'ledger-before.sqlite3'
        write(binding.root,backup,b'')
        with closing(_connection(binding)) as source,closing(sqlite3.connect(backup)) as destination:
            source.backup(destination)
        with readonly_db(backup) as db:
            if ledger_state(db)['sha256']!=plan['state']['ledger_sha256']:fail('E_BACKUP_CHANGED')
        rows=[]
        with readonly_db(binding.ledger,identity=binding.ledger_identity) as db:
            for event in plan['events']:
                row=db.execute('SELECT * FROM unified_events WHERE seq=?',(event['seq'],)).fetchone()
                if row is None or digest(dict(row))!=event['sha256']:fail('E_PLAN_CHANGED')
                rows.append(dict(row))
        document(binding.root,op/'deleted_rows.json',rows)
        for item in plan['blobs']:
            data=read(cas_path(binding.cas,item['ref']),MAX_BLOB)
            if sha(data)!=item['ref'][7:]:fail('E_PLAN_CHANGED')
            write(binding.root,op/'blobs'/item['ref'][7:],data)
        # Durable preparation is recorded before a destructive statement.
        manifest=sealed({'schema':'m0.retention_recovery.v1','instance':binding.instance,
            'binding_sha256':binding.digest,'plan_sha256':plan['sha256'],
            'ledger_before':plan['state']['ledger_sha256'],'rows_sha256':digest(rows),
            'backup_sha256':sha(read(backup,512*1024*1024)),
            'blobs':{x['ref']:x['bytes'] for x in plan['blobs']}})
        document(binding.root,op/'prepared.json',manifest)
        _checkpoint('after_backup')
        with closing(_connection(binding)) as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                actual=inspect_state(binding,db,plan['decision_at'])
                if any(actual[k]!=plan.get(k) for k in actual):fail('E_PLAN_CHANGED')
                for event in plan['events']:
                    db.execute('DELETE FROM unified_events WHERE seq=? AND event_id=?',(event['seq'],event['event_id']))
                    if db.execute('SELECT changes()').fetchone()[0]!=1:fail('E_DELETE_COUNT')
                _checkpoint('after_delete')
                after=ledger_state(db)['sha256']
                document(binding.root,op/'commit-intent.json',sealed({'ledger_after':after,'plan_sha256':plan['sha256']}))
                _checkpoint('before_commit')
                db.commit()
            except BaseException:
                if db.in_transaction:db.rollback()
                raise
        _checkpoint('after_commit')
        for item in plan['blobs']:
            path=cas_path(binding.cas,item['ref'])
            if identity(path)!=item['identity']:fail('E_BLOB_CHANGED')
            unlink_verified(path,item['ref'][7:])
            _checkpoint('after_unlink')
        receipt=sealed({'schema':'m0.retention_receipt.v1','state':'applied','state_basis':'historical',
            'instance':binding.instance,'plan_sha256':plan['sha256'],'ledger_before':manifest['ledger_before'],
            'ledger_after':after,'deleted_events':len(rows),'unlinked_blobs':len(plan['blobs']),
            'unlinked_bytes':sum(x['bytes'] for x in plan['blobs']),'high_seq':plan['high_seq'],
            'operation_dir':str(op),'completed_at':now(),'power_loss_tested':False})
        document(binding.root,op/'receipt.json',receipt)
        quota(binding.root)
        return receipt

def recover_fixture(binding,operation_dir,fixture_grant):
    require_fixture(binding,fixture_grant);op=_operation(binding,operation_dir)
    with target_lock(binding.root),directory_handle(binding.root):
        if (op/'restored.json').exists():return verify_seal(json_file(op/'restored.json'))
        manifest=verify_seal(json_file(op/'prepared.json'));plan=verify_seal(json_file(op/'plan.json'))
        if (manifest.get('schema')!='m0.retention_recovery.v1' or manifest['instance']!=binding.instance or
            manifest['binding_sha256']!=binding.digest or manifest['plan_sha256']!=plan['sha256'] or op.name!=plan['sha256']):fail('E_RECOVERY_BINDING')
        rows=json_file(op/'deleted_rows.json')
        if digest(rows)!=manifest['rows_sha256'] or len(rows)!=len(plan['events']):fail('E_RECOVERY_ROWS')
        if [{k:r[k] for k in ('seq','event_id')}|{'sha256':digest(r)} for r in rows]!=plan['events']:fail('E_RECOVERY_ROWS')
        backup=op/'ledger-before.sqlite3'
        if sha(read(backup,512*1024*1024))!=manifest['backup_sha256']:fail('E_RECOVERY_BACKUP')
        with readonly_db(backup) as db:
            if ledger_state(db)['sha256']!=manifest['ledger_before']:fail('E_RECOVERY_BACKUP')
            for row in rows:
                original=db.execute('SELECT * FROM unified_events WHERE seq=?',(row['seq'],)).fetchone()
                if original is None or dict(original)!=row:fail('E_RECOVERY_ROWS')
        expected_blobs={x['ref']:x['bytes'] for x in plan['blobs']}
        enrolled=json_file(binding.root/'fixture.json')['generated_objects']
        if any(not fixture_blob_enrolled(binding,item,enrolled) for item in plan['blobs']):
            fail('E_RECOVERY_BLOBS')
        if manifest['blobs']!=expected_blobs:fail('E_RECOVERY_BLOBS')
        missing=[]
        for ref,length in expected_blobs.items():
            data=read(op/'blobs'/ref[7:],MAX_BLOB)
            if len(data)!=length or sha(data)!=ref[7:]:fail('E_RECOVERY_BLOBS')
            path=cas_path(binding.cas,ref)
            if path.exists():
                if sha(read(path,MAX_BLOB))!=ref[7:]:fail('E_RECOVERY_CONFLICT')
            else:missing.append((path,data))
        after=None
        if (op/'commit-intent.json').exists():
            intent=verify_seal(json_file(op/'commit-intent.json'))
            if intent['plan_sha256']!=plan['sha256']:fail('E_RECOVERY_BINDING')
            after=intent['ledger_after']
        # A writer connection rolls back an interrupted hot journal. No source
        # table is replaced and no foreign concurrent changes are overwritten.
        with closing(_connection(binding)) as db:
            try:
                db.execute('BEGIN IMMEDIATE');current=ledger_state(db)['sha256']
                if current not in (manifest['ledger_before'],after):fail('E_RECOVERY_CONFLICT')
                quota(binding.root,sum(len(data) for _,data in missing)+8192)
                for path,data in missing:write(binding.root,path,data)
                if current!=manifest['ledger_before']:
                    columns=tuple(r[1] for r in db.execute('PRAGMA table_info(unified_events)'))
                    for row in rows:
                        if set(row)!=set(columns):fail('E_RECOVERY_ROWS')
                        db.execute('INSERT INTO unified_events('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')',[row[k] for k in columns])
                if ledger_state(db)['sha256']!=manifest['ledger_before']:fail('E_RECOVERY_VERIFY')
                db.commit()
            except BaseException:
                if db.in_transaction:db.rollback()
                raise
        result=sealed({'schema':'m0.retention_recovery_receipt.v1','state':'restored','instance':binding.instance,
            'plan_sha256':plan['sha256'],'restored_events':len(rows),'restored_blobs':len(expected_blobs),
            'ledger_sha256':manifest['ledger_before'],'completed_at':now(),'scope':'fixed_candidate_set_only'})
        document(binding.root,op/'restored.json',result)
        return result

def read_retention_status(binding):
    """Metadata only; future manager integration must explicitly bind this source."""
    result=[]
    for op in sorted((binding.root/'operations').iterdir()):
        _operation(binding,op)
        if (op/'restored.json').exists():value=verify_seal(json_file(op/'restored.json'))
        elif (op/'receipt.json').exists():value=verify_seal(json_file(op/'receipt.json'))
        else:value={'state':'recovery_required','plan_sha256':op.name}
        result.append(value)
    return {'schema':'m0.retention_status.v1','instance':binding.instance,'items':result,
            'state_basis':'historical','production_apply':False}
