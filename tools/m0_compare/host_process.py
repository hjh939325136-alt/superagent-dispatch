"""Fresh sandbox process: native Hermes or the existing one-shot text Worker."""

from http.client import HTTPConnection
import importlib.util
import json
from pathlib import Path
import socket
import sys


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _UnixHTTP(HTTPConnection):
    def __init__(self, path):
        super().__init__('127.0.0.1', timeout=65)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def exchange(path, route, payload):
    client = _UnixHTTP(path)
    try:
        client.request('POST', route, body=payload, headers={'Content-Type': 'application/json'})
        response = client.getresponse()
        body = response.read(256 * 1024 + 1)
        if len(body) > 256 * 1024:
            raise ValueError('response_too_large')
        return response.status, body
    finally:
        client.close()


def main():
    # Caller passes only the selected public prompt; never benchmark goldens or credentials.
    raw = sys.stdin.buffer.read(40 * 1024 + 1)
    if len(raw) > 40 * 1024:
        raise ValueError('job_too_large')
    job = json.loads(raw)
    if (type(job) is not dict or set(job) != {'arm', 'prompt', 'task_id', 'tools_enabled'}
            or job['arm'] not in ('worker', 'hermes') or type(job['tools_enabled']) is not bool):
        raise ValueError('invalid_job')
    root = Path(__file__).resolve().parents[2]
    provider, tools = '/run/sa-compare-ipc/provider.sock', '/run/sa-compare-ipc/tools.sock'
    if job['arm'] == 'worker':
        if job['tools_enabled']:
            result = {'status': 'capability_missing', 'text': None, 'reported_api_calls': 0}
        else:
            worker = _load('sa_fixed_text_worker', root / 'tools/m0_agent/deepseek_client.py')
            completion = worker.complete_text(model='deepseek-flash', approved_text=job['prompt'],
                api_key='comparison-placeholder', max_tokens=512, thinking='disabled',
                _transport=lambda _, body: exchange(provider, '/v1/chat/completions', body))
            result = {'status': 'truncated' if completion.truncated else 'succeeded',
                      'text': completion.text, 'reported_api_calls': 1}
    else:
        adapter = _load('sa_native_hermes_adapter', Path(__file__).with_name('hermes_host.py'))
        sequence = 0

        def tool_client(name, arguments):
            nonlocal sequence
            current, sequence = sequence, sequence + 1
            payload = json.dumps({'sequence': current, 'name': name, 'arguments': arguments},
                                 ensure_ascii=False, allow_nan=False).encode()
            status, body = exchange(tools, '/tools', payload)
            if status != 200:
                raise ValueError('tool_channel_refused')
            return json.loads(body)

        result = adapter.run(job['prompt'], task_id=job['task_id'], model='deepseek-flash',
                             provider_socket=provider, tool_client=tool_client,
                             tools_enabled=job['tools_enabled'], work_dir='/tmp')
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('{"status":"failed","code":"comparison_host_failed"}')
        raise SystemExit(1)
