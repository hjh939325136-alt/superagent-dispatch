"""Explicit writer CLI. No reads in sa-manager implicitly invoke this module."""
import argparse
from dataclasses import fields
import json
from pathlib import Path
import sys
from .collector import Collector,SourceBinding,canonical,digest,plain


def unique(pairs):
    result=dict(pairs)
    if len(result)!=len(pairs):raise ValueError('duplicate JSON field')
    return result


def load_binding(path):
    path=plain(path)
    if path.stat().st_size>65536:raise ValueError('binding too large')
    value=json.loads(path.read_bytes(),object_pairs_hook=unique)
    if set(value)!={'schema','target','sources'} or value['schema']!='sa_dispatch.collector_binding.v1':raise ValueError('invalid trusted binding')
    target=plain(value['target'])
    if not isinstance(value['sources'],list) or len(value['sources'])>100:raise ValueError('invalid source list')
    sources={};allowed={f.name for f in fields(SourceBinding)}
    for row in value['sources']:
        if set(row) not in (allowed,allowed-{'collection_mode'}):raise ValueError('unexpected binding source field')
        source=SourceBinding(**row)
        if source.source_id in sources:raise ValueError('duplicate source id')
        sources[source.source_id]=source
    return target,sources,digest(value)


def main(argv=None):
    parser=argparse.ArgumentParser(prog='sa-collector')
    parser.add_argument('--binding',type=Path,required=True)
    parser.add_argument('command',choices=('sync','adopt'))
    args=parser.parse_args(argv)
    try:
        target,sources,binding_hash=load_binding(args.binding)
        raw=sys.stdin.buffer.read(16385)
        if len(raw)>16384:raise ValueError('request too large')
        request=json.loads(raw,object_pairs_hook=unique)
        allowed={'source_id','stream'} if args.command=='sync' else {'source_id','stream','reason','actor','resume_after'}
        if not isinstance(request,dict) or set(request)!=allowed:
            print(json.dumps({'ok':False,'error':'E_FIELD_NOT_ALLOWED'}));return 1
        binding=sources[request['source_id']];writer=Collector(target)
        if args.command=='sync':result=writer.sync(binding,request['stream'])
        else:result=writer.adopt(binding,request['stream'],reason=request['reason'],actor=request['actor'],resume_after=request['resume_after'])
        print(json.dumps({'ok':True,'binding_sha256':binding_hash,'result':result},sort_keys=True))
        return 0
    except Exception as error:
        print(json.dumps({'ok':False,'error':'E_COLLECTOR_REFUSED','cause':type(error).__name__}))
        return 1

if __name__=='__main__':raise SystemExit(main())
