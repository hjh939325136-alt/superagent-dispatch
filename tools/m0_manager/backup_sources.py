"""Read LocalDirTarget metadata and explicitly bound small run receipts."""
import json
from .binding import now, fixed_ref, instant
from .readonly import SourceError
from tools.m0_backup.backup import BackupError
from tools.m0_storage.target import LocalDirTarget


def backup_query(source,op,args,role):
    options=source.get('options',{})
    names=options.get('receipt_names',[])
    if not isinstance(names,list) or len(names)>100:
        raise SourceError('source_binding_invalid')
    try:
        target=LocalDirTarget(source['locator'],quota_root=options['quota_root'])
        preliminary,_=target.readonly_snapshot()
        for name in names:
            if name not in preliminary['entries'] or preliminary['entries'][name]['bytes']>65536:
                raise SourceError('backup_receipt_invalid')
        index,receipts=target.readonly_snapshot(verify_names=names,max_blob_bytes=65536)
        if preliminary!=index:
            raise SourceError('source_changed')
    except BackupError as exc:
        raise SourceError('backup_target_unavailable') from exc
    observed=now()
    if index['instance_id']!=source['source_instance']:
        raise SourceError('source_instance_changed')
    items=[]
    stale=False
    if names:
        parsed=[]
        for name,raw in receipts.items():
            if len(raw)>65536:
                raise SourceError('backup_receipt_invalid')
            value=json.loads(raw)
            if (value.get('schema')!='m0.synthetic_evidence.v1' or value.get('synthetic_only') is not True or
                value.get('evidence_type')!='backup_run' or value.get('version')!=1 or
                value.get('status') not in ('succeeded','failed','quota_exceeded','restore_failed')):
                raise SourceError('backup_receipt_invalid')
            instant(value['at'])
            parsed.append({'status':value['status'],'at':value['at'],
                           'receipt_sha256':index['entries'][name]['sha256']})
        latest=max(parsed,key=lambda r:instant(r['at']))
        age=(instant(observed)-instant(latest['at'])).total_seconds()
        if age < -300:
            raise SourceError('backup_receipt_future_timestamp')
        stale=age>source['freshness_s']
        state=latest['status']
        if op=='status' or state!='succeeded':
            data={'state':state,**latest}
            if op=='attention.list':data['rule']='A7@1'
            items.append({**data,'state_basis':'historical','as_of':observed,
                          'ref':fixed_ref(source,'backup','latest',data)})
    else:
        data={'state':'unknown:no_backup_evidence','rule':'A7@1'}
        items=[{**data,'state_basis':'current','as_of':observed,
                'ref':fixed_ref(source,'backup','evidence_missing',data)}]
    return {'items':items,'coverage':'complete' if names and not stale else 'partial',
            'unknown':['backup_evidence_stale'] if stale else [] if names else ['no_backup_run_receipts'],
            'source_instance':index['instance_id'],'schema_version':index['schema'],
            'observed_at':observed,'state_basis':'stale' if stale else 'historical' if names else 'current'}
