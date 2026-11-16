"""Single-run adapter for the pinned Hermes AIAgent and a Unix-socket broker."""

from contextlib import redirect_stderr, redirect_stdout
import json
import os
from pathlib import Path
import re
import stat
import sys
from tempfile import TemporaryDirectory


HERMES_COMMIT = '38c8a9c10fb3680beb6170a604cbec209fcb89a5'
BROKER_BASE_URL = 'http://127.0.0.1/v1'
BROKER_PATH = '/v1/chat/completions'
PLACEHOLDER_KEY = 'sa-hermes-local-transport'
TOOLSET = 'superagent_fixture_ro'
DOMAINS = ('research-scatter', 'research-content')
MAX_TURNS = 8
MAX_OUTPUT_TOKENS = 512
MAX_PROMPT_BYTES = 32 * 1024
MAX_TOOL_INPUT_BYTES = 2048
MAX_TOOL_OUTPUT_BYTES = 32 * 1024
ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}')
REF = r'(research-scatter|research-content)/note/[0-9A-HJKMNP-TV-Z]{26}@[1-9][0-9]{0,8}#[a-f0-9]{64}'


class HermesHostError(ValueError):
    """A fixed adapter error safe to include in an execution receipt."""


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(',', ':'))


def _schema(name, description, fields):
    domains = dict(type='array', items={'type': 'string', 'enum': list(DOMAINS)}, minItems=1, maxItems=2,
                   uniqueItems=True)
    return dict(name=name, description=description, parameters=dict(type='object',
                properties={'domains': domains, **fields}, required=['domains', *fields],
                additionalProperties=False))


def tool_schemas():
    return (
        _schema('knowledge_search', 'Search the authorized research fixtures and return fixed references.',
                {'q': {'type': 'string', 'minLength': 1, 'maxLength': 200},
                 'limit': {'type': 'integer', 'minimum': 1, 'maximum': 10}}),
        _schema('knowledge_get', 'Read an exact versioned reference from the authorized research fixtures.',
                {'ref': {'type': 'string', 'pattern': '^' + REF + '$'}}),
    )


def _tool_handler(name, tool_client):
    def handle(arguments, **_):
        try:
            if type(arguments) is not dict or len(_json(arguments).encode('utf-8')) > MAX_TOOL_INPUT_BYTES:
                raise ValueError()
        except (ValueError, TypeError, UnicodeError, RecursionError):
            return _json({'ok': False, 'error': {'code': 'INVALID_REQUEST', 'retryable': False}})
        # The broker verifies domains and authorization and records refusals.
        try:
            result = tool_client(name, arguments)
            if type(result) is not dict or type(result.get('ok')) is not bool:
                raise ValueError()
            encoded = _json(result)
            if len(encoded.encode('utf-8')) > MAX_TOOL_OUTPUT_BYTES:
                raise ValueError()
            return encoded
        except Exception:
            return _json({'ok': False, 'error': {'code': 'TOOL_CHANNEL_UNAVAILABLE', 'retryable': False}})

    return handle


def _profile(home):
    # Run in a dedicated process. Do not carry model keys, proxies, profile
    # selectors, plugin credentials or home paths into Hermes' import phase.
    os.environ.clear()
    os.environ.update(HERMES_HOME=str(home), HOME=str(home), XDG_CONFIG_HOME=str(home / 'xdg-config'),
                      XDG_CACHE_HOME=str(home / 'xdg-cache'), XDG_DATA_HOME=str(home / 'xdg-data'),
                      PATH='/usr/bin:/bin', LANG='C.UTF-8')
    settings = {
        'agent': {'api_max_retries': 1, 'environment_probe': False, 'tool_use_enforcement': 'off',
                  'task_completion_guidance': False},
        'compression': {'enabled': False},
        'model': {'context_length': 65536},
        'memory': {'memory_enabled': False, 'user_profile_enabled': False},
        'plugins': {'enabled': []},
        'mcp_servers': {},
        'fallback_providers': [],
    }
    # JSON is valid YAML. Keep this fresh file free of credentials and endpoints.
    (home / 'config.yaml').write_text(_json(settings), encoding='utf-8')


def _agent_class(base, httpx, provider_socket):
    class BrokerTransport(httpx.HTTPTransport):
        def handle_request(self, request):
            if (request.method != 'POST' or request.url.scheme != 'http' or request.url.host != '127.0.0.1'
                    or request.url.port not in (None, 80) or request.url.raw_path != BROKER_PATH.encode('ascii')):
                raise HermesHostError('broker_route_refused')
            return super().handle_request(request)

    class BrokerAgent(base):
        # This pinned version defaults to streaming even without callbacks.
        # The broker validates a bounded, non-streaming completion envelope.
        _disable_streaming = True

        @staticmethod
        def _build_keepalive_http_client(base_url=''):
            if base_url.rstrip('/') != BROKER_BASE_URL:
                raise HermesHostError('broker_endpoint_refused')
            # The pinned Hermes version closes each request client. A fresh
            # transport here avoids reusing an already closed connection pool.
            return httpx.Client(transport=BrokerTransport(uds=str(provider_socket), retries=0),
                                trust_env=False, follow_redirects=False, timeout=45)

    return BrokerAgent


def _build_agent(base, registry, httpx, model, provider_socket, tool_client, tools_enabled):
    schemas = tool_schemas() if tools_enabled else ()
    for schema in schemas:
        name = schema['name']
        registry.register(name=name, toolset=TOOLSET, schema=schema,
                          handler=_tool_handler(name, tool_client), max_result_size_chars=MAX_TOOL_OUTPUT_BYTES)
    agent_type = _agent_class(base, httpx, provider_socket)
    agent = agent_type(base_url=BROKER_BASE_URL, api_key=PLACEHOLDER_KEY, provider='custom',
                       api_mode='chat_completions', model=model, enabled_toolsets=[TOOLSET] if schemas else [],
                       max_iterations=MAX_TURNS, max_tokens=MAX_OUTPUT_TOKENS, tool_delay=0,
                       quiet_mode=True, verbose_logging=False, save_trajectories=False,
                       skip_context_files=True, load_soul_identity=False, skip_memory=True,
                       session_db=None, checkpoints_enabled=False, fallback_model=None, credential_pool=None,
                       reasoning_config={'enabled': False})
    expected = {schema['name']: schema for schema in schemas}
    offered = [entry['function'] for entry in (agent.tools or ())]
    if (set(agent.valid_tool_names) != set(expected) or len(offered) != len(expected)
            or any(schema != expected.get(schema['name']) for schema in offered)
            or agent._api_max_retries != 1 or agent.compression_enabled or agent._fallback_chain):
        agent.release_clients()
        raise HermesHostError('pinned_agent_contract_refused')
    return agent


def _outcome(result):
    count = result.get('api_calls')
    count = count if type(count) is int and 0 <= count <= 64 else None
    text = result.get('final_response')
    if (result.get('completed') is not True or result.get('failed') or result.get('partial')
            or result.get('interrupted') or not isinstance(text, str) or not text.strip()):
        return dict(status='failed', text=None, reported_api_calls=count)
    if len(text.encode('utf-8')) > MAX_PROMPT_BYTES:
        raise HermesHostError('host_output_exceeds_bound')
    return dict(status='succeeded', text=text, reported_api_calls=count)


def run(prompt, *, task_id, model, provider_socket, tool_client, tools_enabled=True, work_dir=None):
    """Run one pinned Hermes turn in a fresh process; broker owns usage and egress."""
    try:
        socket_path = Path(provider_socket)
        if (not isinstance(prompt, str) or not prompt.strip() or len(prompt.encode('utf-8')) > MAX_PROMPT_BYTES
                or not isinstance(task_id, str) or not ID.fullmatch(task_id)
                or not isinstance(model, str) or not ID.fullmatch(model)
                or type(tools_enabled) is not bool or (tools_enabled and not callable(tool_client))
                or not socket_path.is_absolute() or not stat.S_ISSOCK(socket_path.lstat().st_mode)):
            raise HermesHostError('invalid_host_input')
        if any(module in sys.modules for module in ('run_agent', 'hermes_constants', 'hermes_cli.config', 'tools.registry')):
            raise HermesHostError('fresh_hermes_process_required')
        with TemporaryDirectory(prefix='hermes-profile-', dir=work_dir) as scratch:
            _profile(Path(scratch))
            with open(os.devnull, 'w', encoding='utf-8') as log, redirect_stdout(log), redirect_stderr(log):
                import httpx
                from run_agent import AIAgent
                from tools.registry import registry

                agent = _build_agent(AIAgent, registry, httpx, model, socket_path, tool_client, tools_enabled)
                try:
                    return _outcome(agent.run_conversation(prompt, task_id=task_id))
                finally:
                    agent.release_clients()
    except HermesHostError:
        raise
    except Exception:
        raise HermesHostError('hermes_run_failed') from None
