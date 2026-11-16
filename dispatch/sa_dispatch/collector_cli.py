"""Explicit writer CLI. No reads in sa-manager implicitly invoke this module."""
import argparse
from dataclasses import fields
import json
from pathlib import Path
import sys
from .collector import Collector,SourceBinding,canonical,digest,plain


SCHEMA_VERSION = '0.1'


def unique(pairs):
    result=dict(pairs)
    if len(result)!=len(pairs):raise ValueError('duplicate JSON field')
    return result


def load_binding(path):
    path=plain(path)
    if path.stat().st_size>65536:raise ValueError('binding too large')
    value=json.loads(path.read_bytes(),object_pairs_hook=unique)
    if set(value)!={'schema','target','sources'} or value['schema'] not in ('sa_dispatch.collector_binding.v1','sa_dispatch.collector_binding.v2'):raise ValueError('invalid trusted binding')
    target=plain(value['target'])
    if not isinstance(value['sources'],list) or len(value['sources'])>100:raise ValueError('invalid source list')
    sources={};allowed={f.name for f in fields(SourceBinding)}
    for row in value['sources']:
        if set(row) not in (allowed,allowed-{'profile'},allowed-{'collection_mode'},allowed-{'collection_mode','profile'}):raise ValueError('unexpected binding source field')
        if row.get('profile') is not None and value['schema']!='sa_dispatch.collector_binding.v2':raise ValueError('metadata profile requires binding v2')
        source=SourceBinding(**row)
        if source.source_id in sources:raise ValueError('duplicate source id')
        sources[source.source_id]=source
    return target,sources,digest(value)


def respond(ok, **payload):
    print(json.dumps({'schema_version': SCHEMA_VERSION, 'ok': ok, **payload}, sort_keys=ok))
    return 0 if ok else 1


def main(argv=None):
    parser = argparse.ArgumentParser(prog='sa-collector')
    parser.add_argument('--binding', type=Path, required=True)
    parser.add_argument('command', choices=('sync', 'adopt'))
    args = parser.parse_args(argv)
    try:
        raw = sys.stdin.buffer.read(16385)
        if len(raw) > 16384:
            raise ValueError('request too large')
        request = json.loads(raw, object_pairs_hook=unique)
        if not isinstance(request, dict):
            return respond(False, error='E_FIELD_NOT_ALLOWED')
        if request.get('schema_version', SCHEMA_VERSION) != SCHEMA_VERSION:
            return respond(False, error='E_PROTOCOL_UNSUPPORTED')

        required = {'source_id', 'stream'}
        if args.command == 'adopt':
            required |= {'reason', 'actor', 'resume_after'}
        if request.keys() - {'schema_version'} != required:
            return respond(False, error='E_FIELD_NOT_ALLOWED')

        source_id = request['source_id']
        stream = request['stream']
        if not isinstance(source_id, str) or not isinstance(stream, str):
            raise ValueError('source_id and stream must be strings')
        if args.command == 'adopt' and (type(request['resume_after']) is not int or request['resume_after'] < 0):
            raise ValueError('invalid adoption anchor')

        target, sources, binding_hash = load_binding(args.binding)
        binding = sources[source_id]
        writer = Collector(target)
        if args.command == 'sync':
            result = writer.sync(binding, stream)
        else:
            result = writer.adopt(binding, stream, reason=request['reason'], actor=request['actor'],
                                  resume_after=request['resume_after'])
        return respond(True, binding_sha256=binding_hash, result=result)
    except Exception as error:
        return respond(False, error='E_COLLECTOR_REFUSED', cause=type(error).__name__)

if __name__=='__main__':raise SystemExit(main())
