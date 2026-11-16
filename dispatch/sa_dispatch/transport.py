"""Opt-in loopback transport; never starts a service on import."""
from dataclasses import asdict
from contextlib import closing
import base64
import binascii
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from socketserver import TCPServer
import ipaddress
import json
import re
import threading

from .models import DispatchError, Outcome, TaskSpec, FenceRejected
from .routing import ledger_mode

class TransportError(Exception):
    def __init__(self, code, status):
        self.code, self.status = code, status
        super().__init__(code)

class CoordinatorAPI:
    """Token hashes are separate trusted configuration, never task payloads."""
    def __init__(self, store, token_hashes, *, synthetic_handoff=False, roles=None, artifact_store=None):
        self.store = store
        self.synthetic_handoff = synthetic_handoff
        self.roles = {key:set(value) for key,value in (roles or {}).items()}
        self.artifact_store = artifact_store
        self.grant_lock = threading.RLock()
        if synthetic_handoff:
            if artifact_store is None or store.kinds is None or set(self.roles)!=set(token_hashes):
                raise ValueError('synthetic handoff requires artifacts, kind registry and explicit roles')
            if any(not value or not value <= {'dispatcher','worker'} for value in self.roles.values()):
                raise ValueError('invalid principal roles')
        self.token_hashes = dict(token_hashes)
        if not self.token_hashes or len(set(self.token_hashes.values())) != len(self.token_hashes):
            raise ValueError('unique token hash per device required')
        for device, digest in self.token_hashes.items():
            if not isinstance(device, str) or not device or not isinstance(digest,str) or not re.fullmatch('[0-9a-f]{64}',digest):
                raise ValueError('device -> SHA256 hex configuration required')
        self._transport_route()

    def _transport_route(self):
        if self.store.routing_mode() == 'mixed':
            if self.store.route != 'http_synthetic' or not self.synthetic_handoff:
                raise TransportError('E_ROUTE', 403)
        elif self.store.route is not None:
            raise TransportError('E_ROUTE', 403)

    def identity(self, authorization):
        if not isinstance(authorization,str) or not authorization.startswith('Bearer '):
            raise TransportError('E_AUTH',401)
        token=authorization[7:]
        if len(token)<32 or len(token)>1024:
            raise TransportError('E_AUTH',401)
        digest=hashlib.sha256(token.encode('utf-8')).hexdigest()
        identity=None
        for device, expected in self.token_hashes.items():
            if hmac.compare_digest(digest,expected): identity=device
        if identity is None: raise TransportError('E_AUTH',401)
        return identity

    def invoke(self, path, payload, authorization):
        device=self.identity(authorization)
        self._transport_route()
        if not isinstance(payload,dict): raise TransportError('E_VALIDATION',400)
        fields={'/health':set(),'/claim':set(),'/renew':{'lease_id','fence'},
                '/complete':{'lease_id','fence','outcome'},'/status':{'task_id'}}
        if self.synthetic_handoff:
            fields.update({'/submit':{'spec','inputs'},'/task':{'lease_id','fence'},
                           '/artifact/get':{'lease_id','fence','hash'},
                           '/artifact/put':{'lease_id','fence','hash','content_base64'}})
        if path not in fields: raise TransportError('E_NOT_FOUND',404)
        if set(payload)!=fields[path]: raise TransportError('E_VALIDATION',400)
        if path=='/health': return {'status':'ok','transport':'loopback-only'}
        if self.synthetic_handoff:
            role='dispatcher' if path=='/submit' else 'worker'
            if role not in self.roles[device]: raise TransportError('E_ROLE',403)
            self._synthetic_scope()
            if path=='/submit': return self._submit(payload,device)
        if path=='/claim':
            if self.synthetic_handoff:
                # A crash after submit but before grant commit leaves a safe
                # blocked task. Replaying submit restores the handoff marker.
                with self.store._tx(execution=True) as conn:
                    route_sql = '' if self.store.route is None else ' AND t.execution_route=?'
                    route_args = () if self.store.route is None else (self.store.route,)
                    missing=conn.execute("SELECT 1 FROM tasks t LEFT JOIN transport_handoffs h ON h.task_id=t.task_id WHERE t.status='queued' AND h.task_id IS NULL" + route_sql + ' LIMIT 1', route_args).fetchone()
                    if missing: raise TransportError('E_HANDOFF_NOT_READY',409)
            lease=self.store.claim(device, synthetic_handoff=self.synthetic_handoff)
            return {'lease':asdict(lease) if lease else None}
        if path=='/status':
            task_id=payload['task_id']
            if not isinstance(task_id,str): raise TransportError('E_VALIDATION',400)
            if not any(l.device_id==device for l in self.store.list_leases(task_id)):
                raise TransportError('E_NOT_FOUND',404)
            task=self.store.get_task(task_id)
            self.store.require_task_route(task)
            # No raw params, refs, results or private scope leave this metadata API.
            return {k:task[k] for k in ('task_id','status','fence','attempts','updated_at')}
        if not isinstance(payload['lease_id'],str) or type(payload['fence']) is not int or payload['fence']<1:
            raise TransportError('E_VALIDATION',400)
        # Reject foreign identities before touching store; do not add forged late results.
        lease=self.store.get_lease(payload['lease_id'])
        if lease is None or lease.device_id!=device:
            raise TransportError('E_NOT_FOUND',404)
        self.store.require_task_route(self.store.get_task(lease.task_id))
        if path in ('/task','/artifact/get','/artifact/put'):
            return self._handoff(path,payload,device)
        if path=='/renew':
            return {'lease':asdict(self.store.renew(payload['lease_id'],payload['fence'],device))}
        try:
            outcome=Outcome.from_dict(payload['outcome'])
        except (TypeError, ValueError, KeyError):
            raise TransportError('E_VALIDATION',400) from None
        if self.synthetic_handoff:
            if any(ev.access!='synthetic' for ev in outcome.evidence): raise TransportError('E_SCOPE',403)
            with self.grant_lock:
                with self.store._tx(execution=True) as conn:
                    grants=self._grants(conn,lease.task_id,lease.lease_id)
                for ev in outcome.evidence:
                    if ev.artifact_hash is None or ev.artifact_hash not in grants or ev.content_hash!=ev.artifact_hash:
                        raise TransportError('E_ARTIFACT_SCOPE',403)
                if outcome.result.get('receipt') is not None and outcome.result['receipt'] not in grants:
                    raise TransportError('E_ARTIFACT_SCOPE',403)
        self.store.complete(payload['lease_id'],payload['fence'],outcome,device)
        return {'completed':True}

    def _synthetic_scope(self):
        # Shared-ledger metadata does not grant HTTP access to another route.
        # Isolated transports retain the original whole-ledger restriction.
        with closing(self.store._connect()) as conn:
            conn.execute('BEGIN')
            mode = ledger_mode(conn)
            if mode == 'mixed':
                if self.store.route != 'http_synthetic':
                    raise TransportError('E_SCOPE',403)
                version = conn.execute('SELECT MAX(version) FROM schema_migrations').fetchone()[0]
                if version == '0013':
                    from .external_migration import verify_schema
                    verify_schema(conn)
                elif version != '0012':
                    raise TransportError('E_SCOPE',403)
                where, params = ' WHERE execution_route=?', ('http_synthetic',)
            else:
                where, params = '', ()
            if conn.execute('SELECT 1 FROM tasks' + where +
                    (' AND ' if where else ' WHERE ') + "access!='synthetic' LIMIT 1", params).fetchone():
                raise TransportError('E_SCOPE',403)
            evidence_sql = ("SELECT 1 FROM evidence e JOIN tasks t ON t.task_id=e.task_id WHERE t.execution_route=? AND e.access!='synthetic' LIMIT 1"
                            if where else "SELECT 1 FROM evidence WHERE access!='synthetic' LIMIT 1")
            if conn.execute(evidence_sql, params).fetchone():
                raise TransportError('E_SCOPE',403)

    def _decode(self, digest, encoded):
        if not isinstance(digest,str) or not re.fullmatch('sha256:[0-9a-f]{64}',digest):
            raise TransportError('E_VALIDATION',400)
        if not isinstance(encoded,str) or len(encoded)>200000: raise TransportError('E_TOO_LARGE',413)
        try: content=base64.b64decode(encoded,validate=True)
        except (ValueError,binascii.Error): raise TransportError('E_VALIDATION',400) from None
        if 'sha256:'+hashlib.sha256(content).hexdigest()!=digest:
            raise TransportError('E_HASH_MISMATCH',400)
        return content

    def _submit(self,payload,actor):
        try: spec=TaskSpec.from_dict(payload['spec'])
        except (TypeError,ValueError,KeyError): raise TransportError('E_VALIDATION',400) from None
        if spec.access!='synthetic': raise TransportError('E_SCOPE',403)
        inputs=payload['inputs']
        if not isinstance(inputs,dict) or len(inputs)>32: raise TransportError('E_VALIDATION',400)
        allowed={ref.hash for ref in spec.input_refs}
        if set(inputs)!=allowed: raise TransportError('E_ARTIFACT_SCOPE',403)
        decoded={digest:self._decode(digest,value) for digest,value in inputs.items()}
        with self.grant_lock:
            # Stage and verify all bytes before publishing any task. Orphaned
            # staged blobs carry no grants and cannot be read through the API.
            for digest,content in decoded.items():
                self.artifact_store.put(content)
            task,dedup=self.store.submit(spec)
            with self.store._tx(execution=True) as conn:
                current=self.store._task(conn.execute('SELECT * FROM tasks WHERE task_id=?',(task.task_id,)).fetchone())
                self.store.require_task_route(current, conn=conn)
                if current.access!='synthetic': raise TransportError('E_SCOPE',403)
                for digest in decoded:self._grant(conn,task.task_id,None,digest)
                conn.execute('INSERT OR IGNORE INTO transport_handoffs VALUES(?,?,?)',(task.task_id,actor,self.store._now()))
                self.store._event(conn,current,current.status,current.status,'synthetic inputs authorized',actor)
        return {'task_id':task.task_id,'status':task.status,'deduplicated':dedup}

    def _grants(self,conn,task_id,lease_id):
        return {row[0] for row in conn.execute('SELECT content_hash FROM transport_artifact_grants WHERE task_id=? AND (lease_id IS NULL OR lease_id=?)',(task_id,lease_id))}

    def _grant(self,conn,task_id,lease_id,digest):
        key=hashlib.sha256(json.dumps([task_id,lease_id,digest],separators=(',',':')).encode()).hexdigest()
        conn.execute('INSERT OR IGNORE INTO transport_artifact_grants VALUES(?,?,?,?,?,?)',(key,task_id,lease_id,digest,'synthetic',self.store._now()))

    def _handoff(self,path,payload,device):
        # Keep lease fence stable through artifact access. The dedicated local
        # artifact store uses bounded content; no untrusted filesystem paths.
        with self.grant_lock, self.store._tx(execution=True) as conn:
            lease,task,valid=self.store._active(conn,payload['lease_id'],payload['fence'],device)
            if not valid: raise FenceRejected('handoff requires active owned unexpired lease')
            if task['access']!='synthetic': raise TransportError('E_SCOPE',403)
            if not conn.execute('SELECT 1 FROM transport_handoffs WHERE task_id=?',(task['task_id'],)).fetchone():
                raise TransportError('E_HANDOFF_NOT_READY',409)
            if path=='/task': return {'spec':asdict(self.store._task(task).spec)}
            digest=payload['hash']
            if path=='/artifact/put':
                content=self._decode(digest,payload['content_base64'])
                self.artifact_store.put(content)
                self._grant(conn,task['task_id'],lease['lease_id'],digest)
                self.store._event(conn,task,task['status'],task['status'],'synthetic output authorized',device)
                return {'hash':digest}
            allowed=self._grants(conn,task['task_id'],lease['lease_id'])
            if not isinstance(digest,str) or digest not in allowed: raise TransportError('E_ARTIFACT_SCOPE',403)
            content=self.artifact_store.get(digest)
            return {'hash':digest,'content_base64':base64.b64encode(content).decode('ascii')}

def create_server(api, host='127.0.0.1', port=0, *, max_body_bytes=262144):
    """Returns an unstarted server. Caller owns serve_forever/shutdown/server_close."""
    try: address=ipaddress.ip_address(host)
    except ValueError: raise ValueError('literal IPv4 loopback required') from None
    if address.version!=4 or not address.is_loopback:
        raise ValueError('only IPv4 loopback listeners are permitted')
    if type(max_body_bytes) is not int or max_body_bytes<1: raise ValueError('positive body bound required')

    class Handler(BaseHTTPRequestHandler):
        server_version='LocalCoordinator'

        def log_message(self,*args):
            pass  # Never put bearer tokens or request payloads into HTTP logs.

        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def send_json(self,status,body):
            data=json.dumps(body,ensure_ascii=False,allow_nan=False,separators=(',',':')).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type','application/json; charset=utf-8')
            self.send_header('Content-Length',str(len(data)))
            self.send_header('Cache-Control','no-store')
            self.send_header('Connection','close')
            self.end_headers()
            self.wfile.write(data)

        def dispatch(self):
            try:
                expected=f'{self.server.server_address[0]}:{self.server.server_address[1]}'
                if self.headers.get_all('Host')!=[expected]: raise TransportError('E_HOST',403)
                if self.headers.get('Origin') or self.headers.get('Sec-Fetch-Site'):
                    raise TransportError('E_BROWSER_FORBIDDEN',403)
                if len(self.headers.get_all('Authorization',[]))!=1: raise TransportError('E_AUTH',401)
                if self.command=='GET':
                    if self.path!='/health': raise TransportError('E_NOT_FOUND',404)
                    payload={}
                else:
                    if self.headers.get('Transfer-Encoding') or len(self.headers.get_all('Content-Length',[]))!=1:
                        raise TransportError('E_LENGTH',400)
                    length_text=self.headers.get('Content-Length','')
                    if not length_text.isdecimal(): raise TransportError('E_LENGTH',400)
                    length=int(length_text)
                    if length>max_body_bytes: raise TransportError('E_TOO_LARGE',413)
                    if self.headers.get_content_type()!='application/json': raise TransportError('E_CONTENT_TYPE',415)
                    raw=self.rfile.read(length)
                    if len(raw)!=length: raise TransportError('E_LENGTH',400)
                    def pairs(items):
                        value={}
                        for key,item in items:
                            if key in value: raise ValueError('duplicate key')
                            value[key]=item
                        return value
                    payload=json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,
                                       parse_constant=lambda _:(_ for _ in ()).throw(ValueError('nonfinite')))
                data=api.invoke(self.path,payload,self.headers.get('Authorization'))
                self.send_json(200,{'schema':'sa_dispatch.v0','ok':True,'data':data})
            except TransportError as exc:
                self.send_json(exc.status,{'schema':'sa_dispatch.v0','ok':False,'error':{'code':exc.code,'retryable':False}})
            except DispatchError as exc:
                status=409 if exc.exit_code in (3,4) else 400
                self.send_json(status,{'schema':'sa_dispatch.v0','ok':False,'error':{'code':exc.code,'retryable':exc.retryable}})
            except (ValueError,TypeError,KeyError,UnicodeError):
                self.send_json(400,{'schema':'sa_dispatch.v0','ok':False,'error':{'code':'E_VALIDATION','retryable':False}})

        do_GET=dispatch
        do_POST=dispatch

    class LoopbackServer(ThreadingHTTPServer):
        def server_bind(self):
            # HTTPServer.server_bind resolves the hostname even for a literal
            # address. This listener has no DNS dependency.
            TCPServer.server_bind(self)
            self.server_name, self.server_port = self.server_address[:2]

    return LoopbackServer((host,port),Handler)
