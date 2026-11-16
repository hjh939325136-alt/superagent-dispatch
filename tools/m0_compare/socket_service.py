"""One pre-authorized comparison run, exposed through two local Unix sockets."""

from contextlib import ExitStack
from hashlib import sha256
from http.server import BaseHTTPRequestHandler
import json
import os
from pathlib import Path
import socketserver
from threading import Thread


MAX_BODY = 256 * 1024


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate key')
        value[key] = item
    return value


class _Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def setup(self):
        self.request.settimeout(65)
        super().setup()

    def log_message(self, *args):
        pass

    def do_POST(self):
        status, answer = 400, {'error': {'message': 'comparison_request_refused', 'type': 'comparison'}}
        try:
            lengths = self.headers.get_all('Content-Length', [])
            if len(lengths) != 1 or self.headers.get('Transfer-Encoding'):
                raise ValueError()
            length = int(lengths[0])
            if not 1 <= length <= MAX_BODY:
                raise ValueError()
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError()
            value = json.loads(raw, object_pairs_hook=_unique, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
            if type(value) is not dict:
                raise ValueError()
            if self.server.channel == 'provider' and self.path == '/v1/chat/completions':
                digest = sha256(encoded(value)).hexdigest()
                sequences = self.server.sequences
                if digest not in sequences:
                    if len(sequences) >= 20:
                        raise ValueError()
                    sequences[digest] = len(sequences)
                result = self.server.run.complete(sequences[digest], value)
                self.server.events.append({'sequence': sequences[digest], 'payload_sha256': digest,
                                           'state': result['state'], 'code': result.get('code')})
                if result['state'] in ('settled', 'cached') and type(result.get('response')) is dict:
                    status, answer = 200, result['response']
                else:
                    status = 409
                    answer = {'error': {'message': 'comparison_gate_closed', 'type': 'comparison'}}
            elif self.server.channel == 'tools' and self.path == '/tools':
                if set(value) != {'sequence', 'name', 'arguments'}:
                    raise ValueError()
                status, answer = 200, self.server.run.tool(value['sequence'], value['name'], value['arguments'])
        except Exception:
            # Provider and credential errors never become HTTP error bodies.
            pass
        body = encoded(answer)
        if len(body) > MAX_BODY:
            status, body = 500, b'{"error":{"message":"comparison_response_refused"}}'
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        self.end_headers()
        try:
            self.wfile.write(body)
        except (OSError, TimeoutError):
            pass
        self.close_connection = True


class RunSockets:
    """Directory must be root-private; mount only the named sockets into the child."""

    def __init__(self, directory, run):
        self.directory, self.run = Path(directory), run
        self._stack = ExitStack()
        self._servers, self._threads = [], []
        self.events = []

    def __enter__(self):
        if os.name != 'posix' or not self.directory.is_absolute() or self.directory.resolve() != self.directory:
            raise ValueError('unsafe_socket_directory')
        info = self.directory.stat()
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError('socket_directory_not_private')
        try:
            for channel in ('provider', 'tools'):
                path = self.directory / (channel + '.sock')
                server = self._stack.enter_context(socketserver.UnixStreamServer(str(path), _Handler))
                self._stack.callback(path.unlink, missing_ok=True)
                # Parent 0700 prevents host-side access. Namespace binds expose
                # these two leaf sockets to the unprivileged dynamic child only.
                path.chmod(0o666)
                server.channel, server.run, server.sequences = channel, self.run, {}
                server.events = self.events
                thread = Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05}, daemon=True)
                self._servers.append(server)
                self._threads.append(thread)
                thread.start()
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *exc):
        for server in self._servers:
            server.shutdown()
        for thread in self._threads:
            thread.join(timeout=70)
        self._stack.close()
