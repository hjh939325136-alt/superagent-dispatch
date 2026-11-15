"""Project entry point that registers research retrieval with the shared ledger."""
import argparse
import json
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'dispatch'))

from sa_dispatch.cli import main as dispatch_main
from sa_dispatch.config import load_config
from sa_dispatch.models import DispatchError
from search import authorize
from dispatch_binding import (BINDING_FIELD, OPERATIONS, authorize_bound, load_binding, require_recovery_operation)
from tools.m0_manager.readonly import SourceError


class ResearchKind:
    name = 'research.search'
    version = 1
    component = 'research'
    side_effect = 'none'
    enabled = True
    allow_no_new = False

    def __init__(self, operation='research.search', binding=None):
        self.name, self.binding = operation, binding

    def validate(self, params):
        if self.binding:
            clean = authorize_bound(params, self.binding, self.name, submitted=True)
            return clean | {BINDING_FIELD: self.binding.digest}
        # This entry has a fixed, preconfigured synthetic authority.
        authorize(params, 'fixture-both', 'q')
        return params

    def scope(self, params):
        result = {'domains': sorted(params['domains'])}
        if self.binding:
            result[BINDING_FIELD] = params[BINDING_FIELD]
        return result

    def request(self, task):
        if task.spec.access != 'synthetic' or task.spec.egress != 'none':
            raise ValueError('research lab adapter requires synthetic no-egress tasks')
        require_recovery_operation(self.binding, self.name)
        return {'schema_version': '0.1', 'request_id': task.task_id,
                'operation': self.name, 'idempotency_key': None, 'payload': task.spec.params}

    def result(self, response, task):
        if response.get('schema_version') != '0.1' or response.get('ok') is not True or \
                response.get('request_id') != task.task_id:
            raise ValueError('research response does not bind to the task')
        data = response['data']
        if self.binding:
            if (data.get('source_binding') != task.spec.params.get(BINDING_FIELD) or
                    data.get('receipt_content') != 'references_only' or data.get('synthetic_only') is not True):
                raise ValueError('persistent response does not bind to the configured fixture')
            rows = data.get('results', data.get('items', [data['result']] if 'result' in data else []))
            if not isinstance(rows, list) or any('ref' not in row or 'readback_sha256' not in row for row in rows):
                raise ValueError('persistent response is missing verified references')
            return {'count': len(rows), 'refs': [row['ref'] for row in rows],
                    'index_versions': data.get('index_versions', {}),
                    'source_binding': data['source_binding'], 'catalog_instance': data['catalog_instance'],
                    **{key: data[key] for key in ('status', 'usable', 'context_id', 'revocation_basis',
                                                 'post_snapshot_revocations') if key in data}}
        if not isinstance(data.get('results'), list) or not isinstance(data.get('index_versions'), dict):
            raise ValueError('research response missing references or versions')
        return {'count': len(data['results']), 'refs': [item['ref'] for item in data['results']],
                'index_versions': data['index_versions'], 'withheld': data['withheld']}


KINDS = {ResearchKind.name: ResearchKind()}


def configured_kinds(config):
    if config.execution_route not in (None, 'local_synthetic'):
        raise ValueError('research requires the local execution route')
    component = config.components.get('research')
    if not component or '--source-binding' not in component.argv:
        return KINDS
    argv = list(component.argv)
    executable = Path(argv.pop(0))
    if argv and argv[0] == '-B':
        argv.pop(0)
    if (not config.synthetic or len(argv) != 3 or argv[1] != '--source-binding' or
            executable.resolve() != Path(sys.executable).resolve() or
            Path(argv[0]).resolve() != Path(__file__).with_name('dispatch_component.py').resolve()):
        raise ValueError('persistent research requires the bundled synthetic component and explicit local binding')
    binding = load_binding(Path(argv[2]))
    return {name: ResearchKind(name, binding) for name in OPERATIONS}


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--config', type=Path)
    selected, args = parser.parse_known_args(argv)
    try:
        config = load_config(selected.config, allowed_plugins={'research'}) if selected.config else None
        kinds = configured_kinds(config) if config is not None else KINDS
        binding = next(iter(kinds.values())).binding
        if binding is not None and binding.recovery_digest is not None:
            from knowledge.m0_content.recovery_dispatch import run_recovery_dispatch
            data = run_recovery_dispatch(config, {'research': kinds}, binding, 'research', args)
            print(json.dumps({'schema': 'sa_dispatch.v0', 'ok': True, 'data': data}))
            return 0
    except (ValueError, OSError, TypeError, DispatchError, SourceError, sqlite3.Error) as error:
        print(json.dumps({'schema': 'sa_dispatch.v0', 'ok': False,
                          'error': {'code': getattr(error, 'code', 'E_CONFIG'), 'message': str(error)}}))
        return 2
    return dispatch_main(argv, plugins={'research': kinds})


if __name__ == '__main__':
    sys.exit(main())
