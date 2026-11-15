"""Durable synthetic-only client for the existing Codex Rust workbench.

This is an opt-in local adapter, not a model runner or desktop-chat controller.
The supplied workbench must be an isolated instance with a synthetic worker.
The OS lock covers the task across fences; a crash releases it automatically.
"""
from contextlib import contextmanager, closing
import json
import os
from pathlib import Path
import socket
import time
import uuid
from urllib import request, error, parse

from .canonical import canonical_json
from .models import DispatchError, Evidence, Outcome

SCHEMA = """
CREATE TABLE IF NOT EXISTS host_attempts(
 task_id TEXT NOT NULL REFERENCES tasks(task_id), fence INTEGER NOT NULL,
 endpoint TEXT NOT NULL, agent_id TEXT NOT NULL, prompt_ref TEXT NOT NULL,
 title TEXT NOT NULL, request_id TEXT NOT NULL, session_id TEXT, run_id TEXT,
 codex_thread_id TEXT, last_event_seq INTEGER NOT NULL DEFAULT 0,
 phase TEXT NOT NULL, adopted_fence INTEGER, report_ref TEXT, updated_at TEXT NOT NULL,
 PRIMARY KEY(task_id,fence));
CREATE TABLE IF NOT EXISTS host_events(
 task_id TEXT NOT NULL, fence INTEGER NOT NULL, seq INTEGER NOT NULL,
 event_ref TEXT NOT NULL, PRIMARY KEY(task_id,fence,seq),
 FOREIGN KEY(task_id,fence) REFERENCES host_attempts(task_id,fence));
"""


def initialize_schema(conn):
    for statement in SCHEMA.split(';'):
        if statement.strip(): conn.execute(statement)
    columns={r[1] for r in conn.execute('PRAGMA table_info(host_attempts)')}
    for field in ('cancel_requested_at','cancel_sent_at'):
        if field not in columns:conn.execute('ALTER TABLE host_attempts ADD COLUMN '+field+' TEXT')


class HostError(DispatchError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise HostError('HOST_REDIRECT_DENIED')


class WorkbenchClient:
    def __init__(self, base_url, *, timeout=1):
        url = parse.urlsplit(base_url)
        if (url.scheme != 'http' or url.hostname != '127.0.0.1' or not url.port or
                url.username or url.password or url.path not in ('', '/') or url.query or url.fragment):
            raise HostError('HOST_LOOPBACK_REQUIRED')
        self.base_url = 'http://127.0.0.1:' + str(url.port)
        self.timeout = timeout
        self.opener = request.build_opener(request.ProxyHandler({}), NoRedirect())

    def _open(self, path, body=None):
        req = request.Request(self.base_url + path, data=None if body is None else canonical_json(body),
                              headers={'Content-Type': 'application/json'})
        try:
            return self.opener.open(req, timeout=self.timeout)
        except error.HTTPError as exc:
            try: code = json.loads(exc.read(4096)).get('error', {}).get('code')
            except (ValueError, AttributeError): code = None
            if exc.code == 409 and code == 'IDEMPOTENCY_CONFLICT':
                raise HostError('HOST_409_TEXT_MISMATCH') from exc
            raise HostError('HOST_HTTP_' + str(exc.code)) from exc
        except (error.URLError, TimeoutError, OSError) as exc:
            raise HostError('HOST_UNREACHABLE') from exc

    def json(self, path, body=None):
        try:
            with self._open(path, body) as response:
                blob = response.read(4 * 1024 * 1024 + 1)
        except (TimeoutError,OSError) as exc:
            raise HostError('HOST_UNREACHABLE') from exc
        if len(blob) > 4 * 1024 * 1024: raise HostError('HOST_RESPONSE_TOO_LARGE')
        try:
            value = json.loads(blob)
            if value['ok'] is not True: raise ValueError()
            return value['data']
        except (ValueError, KeyError, TypeError) as exc:
            raise HostError('HOST_PROTOCOL') from exc

    def event(self, session_id, after):
        """One SSE event per connection; timeout allows timely lease renewals."""
        try:
            with self._open(f'/api/sessions/{session_id}/events?after={after}') as response:
                for _ in range(10):
                    line = response.readline(1024 * 1024 + 1)
                    if len(line) > 1024 * 1024: raise HostError('HOST_RESPONSE_TOO_LARGE')
                    if not line: return None
                    if line.startswith(b'data:'):
                        return json.loads(line[5:])
        except (socket.timeout, TimeoutError):
            return None
        except ValueError as exc:
            raise HostError('HOST_PROTOCOL') from exc
        return None


@contextmanager
def task_lock(ledger, task_id):
    # Only ledger-generated task IDs become names. No arbitrary paths accepted.
    if not task_id.startswith('t_') or any(c not in 't_0123456789abcdef' for c in task_id):
        raise HostError('HOST_TASK_ID')
    root = Path(ledger).parent / '.host-locks'
    root.mkdir(exist_ok=True)
    with (root / (task_id + '.lock')).open('a+b') as handle:
        if handle.tell() == 0:
            handle.write(b'0'); handle.flush()
        handle.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise HostError('HOST_ADAPTER_BUSY') from exc
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == 'nt': msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else: fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class HostAdapter:
    def __init__(self, store, artifacts, client, *, isolated_synthetic=False):
        if isolated_synthetic is not True:
            raise HostError('HOST_ISOLATED_FIXTURE_REQUIRED')
        self.store, self.artifacts, self.client = store, artifacts, client
        self.client.timeout = min(self.client.timeout, max(.1, store.lease_ttl_seconds / 4))
        with store._tx() as conn: initialize_schema(conn)

    def attempt(self, task_id, fence):
        with closing(self.store._connect()) as conn:
            row = conn.execute('SELECT * FROM host_attempts WHERE task_id=? AND fence=?', (task_id, fence)).fetchone()
            return dict(row) if row else None

    def _save(self, lease, **values):
        values['updated_at'] = self.store._now()
        with self.store._tx() as conn:
            _, _, valid = self.store._active(conn, lease.lease_id, lease.fence, lease.device_id)
            if not valid: raise HostError('HOST_LEASE_LOST')
            conn.execute('UPDATE host_attempts SET ' + ','.join(k+'=?' for k in values) +
                         ' WHERE task_id=? AND fence=?', (*values.values(), lease.task_id, lease.fence))

    def _session(self, row):
        if not row['session_id']: raise HostError('HOST_NOT_OWNER')
        session = self.client.json('/api/sessions/' + row['session_id'])
        if (session.get('id') != row['session_id'] or session.get('title') != row['title'] or
                session.get('agent_id') != row['agent_id']):
            raise HostError('HOST_NOT_OWNER')
        return session

    def _run_state(self,row,session,text):
        # The real Rust public API deliberately removes its private requests
        # array. Use the saved run_id and public run/message events instead.
        starts=[e for e in session['events'] if e.get('run_id')==row['run_id'] and e.get('type')=='run.started']
        if len(starts)!=1: raise HostError('HOST_RUN_MISSING')
        messages=[e['message'] for e in session['events'] if e.get('run_id')==row['run_id'] and
                  e.get('type')=='message' and e.get('message',{}).get('role')=='user']
        if len(messages)!=1 or messages[0].get('text')!=text: raise HostError('HOST_409_TEXT_MISMATCH')
        terminals=[e for e in session['events'] if e.get('run_id')==row['run_id'] and
                   e.get('type') in ('run.completed','run.failed','run.cancelled','run.interrupted')]
        return terminals[-1]['status'] if terminals else 'running'

    def _prepare(self, lease, resume_interrupted):
        task = self.store.get_task(lease.task_id)
        if task.access != 'synthetic' or task.egress != 'none' or task.side_effect != 'none':
            raise HostError('HOST_SYNTHETIC_READ_ONLY_REQUIRED')
        if task.kind != 'host.synthetic' or set(task.params) != {'prompt_ref', 'agent_id'}:
            raise HostError('HOST_TASK_CONTRACT')
        prompt_ref, agent = task.params['prompt_ref'], task.params['agent_id']
        if agent not in ('lead','research','writer','director','publisher'):
            raise HostError('HOST_AGENT_UNKNOWN')
        if not any(ref['hash'] == prompt_ref and ref['version'] != 'latest' for ref in task.input_refs):
            raise HostError('HOST_FIXED_PROMPT_REQUIRED')
        prompt = json.loads(self.artifacts.get(prompt_ref))
        if (not isinstance(prompt, dict) or prompt.get('synthetic') is not True or
                set(prompt) != {'synthetic', 'text'} or not isinstance(prompt['text'], str) or
                not 0 < len(prompt['text'].strip()) <= 30000):
            raise HostError('HOST_SYNTHETIC_PROMPT_REQUIRED')
        row = self.attempt(lease.task_id, lease.fence)
        if row:
            if (row['endpoint'],row['agent_id'],row['prompt_ref']) != (self.client.base_url,agent,prompt_ref):
                raise HostError('HOST_JOB_CHANGED')
            return row, prompt['text']
        with closing(self.store._connect()) as conn:
            old = conn.execute('SELECT * FROM host_attempts WHERE task_id=? ORDER BY fence DESC LIMIT 1',
                               (lease.task_id,)).fetchone()
        row = dict(task_id=lease.task_id, fence=lease.fence, endpoint=self.client.base_url, agent_id=agent,
                   prompt_ref=prompt_ref, title='sad:'+lease.task_id+':'+uuid.uuid4().hex,
                   request_id=f'{lease.task_id}:{lease.fence}', session_id=None, run_id=None,
                   codex_thread_id=None,last_event_seq=0,phase='prepared',adopted_fence=None,
                   report_ref=None,updated_at=self.store._now(),cancel_requested_at=None,cancel_sent_at=None)
        if old:
            old = dict(old)
            if (old['endpoint'],old['agent_id'],old['prompt_ref']) != (self.client.base_url,agent,prompt_ref):
                raise HostError('HOST_JOB_CHANGED')
            row.update(cancel_requested_at=old['cancel_requested_at'],cancel_sent_at=old['cancel_sent_at'])
            if not old['session_id']:
                matches=[s for s in self.client.json('/api/sessions?agent_id='+agent) if s['title']==old['title']]
                if len(matches)>1: raise HostError('HOST_CREATE_AMBIGUOUS')
                if matches:
                    old['session_id']=matches[0]['id']
                elif old['phase']=='prepared':
                    # No HTTP creation was attempted. Reuse the durable ownership
                    # title, so a new fence cannot abandon an earlier intent.
                    row.update(title=old['title'],adopted_fence=old['fence'] if old['cancel_requested_at'] else None)
                    old=None
                else:raise HostError('HOST_PREVIOUS_CREATE_UNCERTAIN')
        if old:
            session = self._session(old)
            if not old['run_id'] and old['cancel_requested_at'] and old['phase'] in ('prepared','session_ready'):
                row.update(title=old['title'],session_id=old['session_id'],phase='session_ready',
                           request_id=old['request_id'],adopted_fence=old['fence'])
                with self.store._tx() as conn:
                    if not self.store._active(conn,lease.lease_id,lease.fence,lease.device_id)[2]:
                        raise HostError('HOST_LEASE_LOST')
                    conn.execute('INSERT INTO host_attempts('+','.join(row)+') VALUES('+','.join('?' for _ in row)+')',tuple(row.values()))
                return row,prompt['text']
            if not old['run_id']:
                # No public requests array exists. Replaying the exact original
                # key+CAS text obtains its prior run or submits it once.
                prior_run=self.client.json('/api/sessions/'+old['session_id']+'/messages',
                                           {'text':prompt['text'],'request_id':old['request_id']})
                old['run_id']=prior_run['run_id']
                with self.store._tx() as conn:
                    _,_,valid=self.store._active(conn,lease.lease_id,lease.fence,lease.device_id)
                    if not valid:raise HostError('HOST_LEASE_LOST')
                    conn.execute('UPDATE host_attempts SET session_id=?,run_id=?,phase=?,updated_at=? '
                                 'WHERE task_id=? AND fence=?',
                                 (old['session_id'],old['run_id'],'observing',self.store._now(),lease.task_id,old['fence']))
                session=self._session(old)
            prior_status = self._run_state(old,session,prompt['text'])
            row.update(title=old['title'],session_id=old['session_id'],codex_thread_id=session['codex_thread_id'])
            if prior_status == 'interrupted':
                if not resume_interrupted: raise HostError('HOST_EXPLICIT_RESUME_REQUIRED')
                row.update(phase='session_ready',cancel_requested_at=None,cancel_sent_at=None)
            else:
                # A stale lease never justifies a second live model run. Adopt
                # observation of the original run, including its terminal receipt.
                row.update(request_id=old['request_id'],run_id=old['run_id'],phase='observing',
                           adopted_fence=old['fence'])
        with self.store._tx() as conn:
            _,_,valid = self.store._active(conn,lease.lease_id,lease.fence,lease.device_id)
            if not valid: raise HostError('HOST_LEASE_LOST')
            conn.execute('INSERT INTO host_attempts('+','.join(row)+') VALUES('+','.join('?' for _ in row)+')',tuple(row.values()))
        return row,prompt['text']

    def _ensure_run(self, lease, row, text):
        if row.get('cancel_requested_at') and row['phase'] in ('prepared','session_ready'):
            return False
        if not row['session_id']:
            matches = [s for s in self.client.json('/api/sessions?agent_id='+row['agent_id']) if s['title']==row['title']]
            if len(matches)>1: raise HostError('HOST_CREATE_AMBIGUOUS')
            if matches:
                session=matches[0]
            elif row['phase']=='prepared':
                self._save(lease,phase='create_requested')
                session=self.client.json('/api/sessions',{'agent_id':row['agent_id'],'title':row['title']})
            else:
                raise HostError('HOST_CREATE_UNCERTAIN')
            self._save(lease,session_id=session['id'],phase='session_ready')
            row=self.attempt(lease.task_id,lease.fence)
        session=self._session(row)
        if row['run_id']:
            self._run_state(row,session,text)
            run={'run_id':row['run_id']}
        else:
            row=self.attempt(lease.task_id,lease.fence)
            if row.get('cancel_requested_at') and row['phase']=='session_ready':return False
            self._save(lease,phase='submit_requested')
            run=self.client.json('/api/sessions/'+row['session_id']+'/messages',
                                 {'text':text,'request_id':row['request_id']})
        if row['run_id'] and row['run_id']!=run['run_id']: raise HostError('HOST_RUN_CHANGED')
        self._save(lease,run_id=run['run_id'],phase='observing')
        return True

    def _dispatch_cancel(self,lease):
        row=self.attempt(lease.task_id,lease.fence)
        if not row['cancel_requested_at'] or row['cancel_sent_at'] or not row['run_id']:return
        session=self._session(row)
        self._run_state(row,session,json.loads(self.artifacts.get(row['prompt_ref']))['text'])
        starts=[e for e in session['events'] if e.get('type')=='run.started']
        if session['status']!='running':return
        if not starts or starts[-1]['run_id']!=row['run_id']:raise HostError('HOST_NOT_OWNER')
        # At-most-one attempted cancel. Lost HTTP results remain observable via
        # the durable run, and are never reported as a confirmed cancellation.
        self._save(lease,cancel_sent_at=self.store._now())
        self.client.json('/api/sessions/'+row['session_id']+'/cancel',{})

    def _record(self,lease,event):
        row=self.attempt(lease.task_id,lease.fence)
        seq=event.get('seq')
        if type(seq) is not int or seq!=row['last_event_seq']+1: raise HostError('EVENT_GAP')
        ref=self.artifacts.put(canonical_json(event))
        with self.store._tx() as conn:
            _,_,valid=self.store._active(conn,lease.lease_id,lease.fence,lease.device_id)
            if not valid: raise HostError('HOST_LEASE_LOST')
            conn.execute('INSERT INTO host_events VALUES(?,?,?,?)',(lease.task_id,lease.fence,seq,ref))
            conn.execute('UPDATE host_attempts SET last_event_seq=?,updated_at=? WHERE task_id=? AND fence=?',
                         (seq,self.store._now(),lease.task_id,lease.fence))

    def _finish(self,lease,row,session,terminal):
        report={'schema':'sa_dispatch.host_report.v1','synthetic':True,'task_id':lease.task_id,'fence':lease.fence,
                'host_run':{k:row[k] for k in ('session_id','run_id','request_id','last_event_seq','adopted_fence')},
                'codex_thread_id':session['codex_thread_id'],'status':terminal['status'],
                'messages':session['messages'],'usage':terminal.get('usage'),'model_execution':'fixture_worker',
                'not_started':terminal.get('not_started',False)}
        ref=self.artifacts.put(canonical_json(report))
        self._save(lease,phase=terminal['status'],report_ref=ref,codex_thread_id=session['codex_thread_id'])
        if row['adopted_fence'] is not None:
            with self.store._tx() as conn:
                conn.execute('UPDATE host_attempts SET phase=?,report_ref=?,updated_at=? '
                             'WHERE task_id=? AND fence<? AND session_id IS ? AND run_id IS ?',
                             (terminal['status'],ref,self.store._now(),lease.task_id,lease.fence,row['session_id'],row['run_id']))
        if row['run_id'] and terminal.get('seq'):
            self._settle_usage(row,terminal)
        if terminal['status']=='interrupted':
            # Leave to the normal lease-expiry transition, then explicitly resume.
            return report
        outcome=Outcome(status='succeeded' if terminal['status']=='completed' else 'failed',
                        result={'receipt':ref,'host_report_ref':ref,'host_status':terminal['status']},
                        evidence=(Evidence('host_report',ref,'synthetic',artifact_hash=ref),))
        self.store.complete(lease.lease_id,lease.fence,outcome,lease.device_id)
        return report

    def _settle_usage(self,row,terminal):
        from .control_policy import ControlPolicy
        event_ref=self.artifacts.put(canonical_json(terminal))
        wrapper={'schema':'sa_dispatch.host_terminal.v1','synthetic':True,'task_id':row['task_id'],
                 'endpoint':row['endpoint'],'session_id':row['session_id'],'run_id':row['run_id'],'event_ref':event_ref}
        ref=self.artifacts.put(canonical_json(wrapper))
        return ControlPolicy(self.store,artifacts=self.artifacts).settle_host_run(
            row['task_id'],row['run_id'],terminal_event_ref=ref,actor='synthetic-host-meter')

    def run(self,lease,*,timeout_s=30,resume_interrupted=False):
        try:
            return self._run(lease,timeout_s=timeout_s,resume_interrupted=resume_interrupted)
        except HostError as exc:
            if exc.code!='HOST_409_TEXT_MISMATCH': raise
            report={'schema':'sa_dispatch.host_report.v1','synthetic':True,'task_id':lease.task_id,
                    'fence':lease.fence,'status':'failed_final','error':exc.code,
                    'host_stopped':False,'model_execution':'fixture_worker'}
            ref=self.artifacts.put(canonical_json(report))
            if self.attempt(lease.task_id,lease.fence):self._save(lease,phase='protocol_error',report_ref=ref)
            self.store.complete(lease.lease_id,lease.fence,
                Outcome(status='failed',result={'receipt':ref,'host_report_ref':ref,'host_error':exc.code},
                        evidence=(Evidence('host_report',ref,'synthetic',artifact_hash=ref),)),lease.device_id)
            return report

    def _run(self,lease,*,timeout_s=30,resume_interrupted=False):
        if not 0<timeout_s<=3600: raise HostError('HOST_TIMEOUT_RANGE')
        with task_lock(self.store.path,lease.task_id):
            self.store.renew(lease.lease_id,lease.fence,lease.device_id)
            row,text=self._prepare(lease,resume_interrupted)
            if not self._ensure_run(lease,row,text):
                row=self.attempt(lease.task_id,lease.fence)
                return self._finish(lease,row,{'codex_thread_id':None,'messages':[]},
                                    {'status':'cancelled','usage':None,'not_started':True})
            deadline=time.monotonic()+timeout_s
            while time.monotonic()<deadline:
                self.store.renew(lease.lease_id,lease.fence,lease.device_id)
                self._dispatch_cancel(lease)
                row=self.attempt(lease.task_id,lease.fence)
                event=self.client.event(row['session_id'],row['last_event_seq'])
                session=None
                if event is not None and event.get('seq')==row['last_event_seq']+1:
                    events=[event]
                else:
                    session=self._session(row)
                    events=[e for e in session['events'] if e['seq']>row['last_event_seq']]
                for item in events:
                    self._record(lease,item)
                    if item.get('run_id')==row['run_id'] and item.get('type') in (
                            'run.completed','run.failed','run.cancelled','run.interrupted'):
                        session=session or self._session(row)
                        return self._finish(lease,self.attempt(lease.task_id,lease.fence),session,item)
                if session is not None:
                    # A crash can occur after durably advancing the cursor but
                    # before committing the receipt. Re-read its terminal event.
                    terminals=[e for e in session['events'] if e.get('run_id')==row['run_id'] and
                               e.get('type') in ('run.completed','run.failed','run.cancelled','run.interrupted')]
                    if terminals:
                        return self._finish(lease,self.attempt(lease.task_id,lease.fence),session,terminals[-1])
            # Observation timeout does not cancel, resubmit or refund the model.
            raise HostError('HOST_OBSERVATION_TIMEOUT')

    def cancel(self,task_id,fence):
        # This method only records intent. The owning observer holds task_lock
        # and performs the actual scoped HTTP request on its next iteration.
        with self.store._tx() as conn:
            row=conn.execute('SELECT * FROM host_attempts WHERE task_id=? AND fence=?',(task_id,fence)).fetchone()
            lease=conn.execute('SELECT * FROM leases WHERE task_id=? AND fence=? AND state=\'active\'',
                               (task_id,fence)).fetchone()
            valid=lease and self.store._active(conn,lease['lease_id'],fence,lease['device_id'])[2]
            if not valid or not row or row['endpoint']!=self.client.base_url:raise HostError('HOST_NOT_OWNER')
            conn.execute('UPDATE host_attempts SET cancel_requested_at=COALESCE(cancel_requested_at,?) '
                         'WHERE task_id=? AND fence=?',(self.store._now(),task_id,fence))
        return {'cancel_requested':True,'host_cancel_confirmed':False}

    def settle(self,task_id,*,timeout_s=5):
        """Reconcile owned orphan runs of a terminal task; never submit work."""
        if not 0<timeout_s<=60:raise HostError('HOST_TIMEOUT_RANGE')
        with task_lock(self.store.path,task_id):
            task=self.store.get_task(task_id)
            if task.status not in ('succeeded','failed','cancelled','no_new') or task.access!='synthetic':
                raise HostError('HOST_SETTLE_TERMINAL_REQUIRED')
            with closing(self.store._connect()) as conn:
                attempts=[dict(r) for r in conn.execute('SELECT * FROM host_attempts WHERE task_id=? ORDER BY fence', (task_id,))]
            reports=[]
            for row in attempts:
                if row['endpoint']!=self.client.base_url:raise HostError('HOST_NOT_OWNER')
                if row['phase'] in ('orphan_terminal','orphan_cancelled','not_started'):
                    cached=json.loads(self.artifacts.get(row['report_ref']))
                    if cached.get('terminal'):self._settle_usage(row,cached['terminal'])
                    reports.append(cached);continue
                session=None;terminal=None;status='orphan_unknown'
                try:
                    if not row['session_id']:
                        matches=[s for s in self.client.json('/api/sessions?agent_id='+row['agent_id']) if s['title']==row['title']]
                        if len(matches)==1:row['session_id']=matches[0]['id']
                        elif not matches and row['phase']=='prepared':status='not_started'
                    if row['session_id']:
                        session=self._session(row)
                        starts=[e for e in session['events'] if e.get('type')=='run.started']
                        if not row['run_id'] and len(starts)==1:row['run_id']=starts[0]['run_id']
                        if not starts and row['phase'] in ('prepared','session_ready'):status='not_started'
                        if row['run_id']:
                            self._run_state(row,session,json.loads(self.artifacts.get(row['prompt_ref']))['text'])
                            terminals=lambda s:[e for e in s['events'] if e.get('run_id')==row['run_id'] and
                                e.get('type') in ('run.completed','run.failed','run.cancelled','run.interrupted')]
                            found=terminals(session)
                            if not found and session['status']=='running' and starts[-1]['run_id']==row['run_id']:
                                if not row['cancel_sent_at']:
                                    with self.store._tx() as conn:
                                        conn.execute('UPDATE host_attempts SET cancel_sent_at=? WHERE task_id=? AND fence=?',
                                                     (self.store._now(),task_id,row['fence']))
                                    self.client.json('/api/sessions/'+row['session_id']+'/cancel',{})
                                deadline=time.monotonic()+timeout_s
                                while time.monotonic()<deadline and not found:
                                    time.sleep(.05);session=self._session(row);found=terminals(session)
                            if found:
                                terminal=found[-1]
                                status='orphan_cancelled' if terminal['status']=='cancelled' else 'orphan_terminal'
                except HostError:
                    # Unreachable/ambiguous is evidence of uncertainty, not absence.
                    status='orphan_unknown'
                report={'schema':'sa_dispatch.host_settlement.v1','synthetic':True,'task_id':task_id,
                        'fence':row['fence'],'status':status,'session_id':row['session_id'],'run_id':row['run_id'],
                        'terminal':terminal,'host_running':session and session['status']=='running',
                        'no_message_submitted':True}
                ref=self.artifacts.put(canonical_json(report))
                with self.store._tx() as conn:
                    # Original terminal task/evidence are retained; only the host
                    # recovery record receives the new observation and receipt.
                    if conn.execute('SELECT status FROM tasks WHERE task_id=?',(task_id,)).fetchone()[0] not in ('succeeded','failed','cancelled','no_new'):
                        raise HostError('HOST_SETTLE_TERMINAL_REQUIRED')
                    conn.execute('UPDATE host_attempts SET phase=?,report_ref=?,session_id=?,run_id=?,updated_at=? '
                                 'WHERE task_id=? AND fence=?',
                                 (status,ref,row['session_id'],row['run_id'],self.store._now(),task_id,row['fence']))
                if terminal:self._settle_usage(row,terminal)
                reports.append(report)
            return {'task_id':task_id,'settled':all(r['status']!='orphan_unknown' for r in reports),'reports':reports}


def main():
    import argparse
    from .artifacts import ArtifactStore
    from .store_sqlite import SqliteStore
    from .models import Lease
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('ledger','artifacts','lease-id','base-url'): parser.add_argument('--'+name,required=True)
    parser.add_argument('--lease-ttl',type=int,default=10)
    parser.add_argument('--timeout',type=float,default=30)
    parser.add_argument('--isolated-synthetic',action='store_true',required=True)
    parser.add_argument('--resume-interrupted',action='store_true')
    args=parser.parse_args()
    store=SqliteStore(args.ledger,lease_ttl_seconds=args.lease_ttl)
    row=store.get_lease(args.lease_id)
    if not row: parser.error('lease not found')
    lease=Lease(**{k:row[k] for k in Lease.__dataclass_fields__})
    try:
        report=HostAdapter(store,ArtifactStore(args.artifacts),WorkbenchClient(args.base_url),
                           isolated_synthetic=args.isolated_synthetic).run(
                               lease,timeout_s=args.timeout,resume_interrupted=args.resume_interrupted)
        print(json.dumps(report,ensure_ascii=False))
    except DispatchError as exc:
        print(json.dumps({'ok':False,'code':exc.code}))
        return 2
    return 0


if __name__=='__main__':
    raise SystemExit(main())
