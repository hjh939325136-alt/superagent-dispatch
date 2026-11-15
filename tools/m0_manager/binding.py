"""Only trusted local configuration selects filesystem sources."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from .readonly import local_file, SourceError

TOKEN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.:@/#+-]{0,199}$')
KINDS = {'dispatch', 'inventory', 'knowledge', 'content', 'backup', 'hook', 'handoff'}
ROLES = {'technical', 'manager', 'progress'}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def parse_json(data):
    def pairs(items):
        value=dict(items)
        if len(value)!=len(items):
            raise ValueError('duplicate JSON key')
        return value
    def invalid(_):
        raise ValueError('non-finite JSON value')
    return json.loads(data,object_pairs_hook=pairs,parse_constant=invalid)


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat()


def instant(value):
    at = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if at.tzinfo is None:
        raise ValueError('timezone required')
    return at


def token(value):
    return isinstance(value, str) and TOKEN.fullmatch(value) is not None


def load_binding(path):
    selected = local_file(path)
    if selected.stat().st_size > 1024 * 1024:
        raise SourceError('binding_invalid')
    with selected.open('rb') as stream:
        data = stream.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise SourceError('binding_invalid')
    value = parse_json(data)
    if not isinstance(value, dict) or set(value) != {'version', 'sources'} or type(value['version']) is not int or value['version'] != 1:
        raise SourceError('binding_invalid')
    if not isinstance(value['sources'], list) or len(value['sources']) > 100:
        raise SourceError('binding_invalid')
    seen = set()
    fields = {'source_id', 'source_instance', 'kind', 'locator', 'accepted_schema_versions',
              'class', 'freshness_s', 'options'}
    required = fields - {'options'}
    for source in value['sources']:
        if not isinstance(source, dict) or not required <= set(source) or set(source) - fields:
            raise SourceError('binding_invalid')
        if (not token(source['source_id']) or not token(source['source_instance']) or
                source['source_id'] in seen or source['kind'] not in KINDS or
                source['class'] not in {'public', 'internal', 'local_only'} or
                type(source['freshness_s']) is not int or not 1 <= source['freshness_s'] <= 31536000 or
                not isinstance(source.get('options', {}), dict)):
            raise SourceError('binding_invalid')
        versions = source['accepted_schema_versions']
        if not isinstance(versions, list) or not versions or any(not token(v) for v in versions):
            raise SourceError('binding_invalid')
        if not isinstance(source['locator'], str) or not Path(source['locator']).is_absolute():
            raise SourceError('binding_invalid')
        if source['kind'] in ('hook','handoff'):
            options=source.get('options',{})
            required_options={'format','identity'} if source['kind']=='hook' else {'format'}
            expected_format='superagent_hook.events.v1' if source['kind']=='hook' else 'superagent.handoffs.v1'
            if (source['class']!='local_only' or set(options)!=required_options or
                options.get('format')!=expected_format or '1' not in versions):
                raise SourceError('binding_metadata_scope_invalid')
            if source['kind']=='hook':
                identity=options.get('identity')
                if (not isinstance(identity,dict) or set(identity)!={'device','inode'} or
                    any(type(v) is not int or v<0 for v in identity.values())):
                    raise SourceError('binding_metadata_scope_invalid')
        seen.add(source['source_id'])
    return value, hashlib.sha256(data).hexdigest()


def fixed_ref(source, kind, identity, value, version='1'):
    # Hash identifiers that are not bounded metadata tokens; never echo paths.
    identity = identity if token(identity) and '/' not in identity and '\\' not in identity else digest(identity)
    return f"manager/{kind}/{source['source_id']}:{identity}@{version}#sha256:{digest(value)}"
