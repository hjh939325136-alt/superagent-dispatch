"""Opt-in synthetic Claude CLI lifecycle; no ambient tools or business context.

A detached, bounded supervisor owns the actual child. The adapter records launch
intent before spawning; a missing result never authorizes a duplicate launch.
"""
from contextlib import closing
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

from .canonical import canonical_json
from .host_adapter import task_lock
from .models import DispatchError, Evidence, Outcome

SCHEMA = """
CREATE TABLE IF NOT EXISTS claude_attempts(
 task_id TEXT NOT NULL REFERENCES tasks(task_id), fence INTEGER NOT NULL,
 origin_fence INTEGER NOT NULL, session_id TEXT NOT NULL, model TEXT NOT NULL,
 prompt_ref TEXT NOT NULL, request_id TEXT NOT NULL, run_dir TEXT NOT NULL,
 phase TEXT NOT NULL, report_ref TEXT, updated_at TEXT NOT NULL,
 execution_authority TEXT NOT NULL DEFAULT 'local_owned',
 PRIMARY KEY(task_id,fence));
"""


class ClaudeError(DispatchError):
    def __init__(self, code):
        self.code=code
        super().__init__(code)


def initialize_schema(conn):
    conn.execute(SCHEMA)
    if 'execution_authority' not in {r[1] for r in conn.execute('PRAGMA table_info(claude_attempts)')}:
        conn.execute("ALTER TABLE claude_attempts ADD COLUMN execution_authority TEXT NOT NULL DEFAULT 'local_owned'")


def _atomic(path, value):
    path=Path(path); temporary=path.with_name(path.name+'.tmp')
    with temporary.open('wb') as stream:
        stream.write(_json_bytes(value)); stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary,path)


def _json_bytes(value):
    # Host receipts carry decimal USD; task fingerprint canonicalization remains
    # integer-only and is deliberately not used for external receipts.
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')


def command(cli, *, model, session_id, resume, budget_usd):
    return [*cli,'-p','--safe-mode','--tools','','--disable-slash-commands',
        '--strict-mcp-config','--mcp-config','{"mcpServers":{}}','--no-chrome',
        '--permission-mode','dontAsk','--system-prompt',
        'Synthetic lifecycle task. Use only the supplied text; no tools or external context.',
        '--output-format','stream-json','--verbose','--model',model,'--max-budget-usd',str(budget_usd),
        '--resume' if resume else '--session-id',session_id]


def supervise(job_path):
    """Private helper: child identity is the live Popen handle, never a reused PID."""
    job_path=Path(job_path).resolve(); directory=job_path.parent
    job=json.loads(job_path.read_bytes())
    with task_lock(directory/'supervisor', 't_0'):
        if (directory/'exit.json').exists(): return
        # The marker also prevents manually re-running this helper after a crash.
        marker=directory/'spawned.json'
        try:
            with marker.open('xb') as stream:
                stream.write(canonical_json({'supervisor_pid':os.getpid()}))
        except FileExistsError: return
        child=None; reason=None; code=None
        try:
            (directory/'prompt.txt').write_text(job['prompt'],encoding='utf-8')
            with (directory/'stdout.json').open('wb') as out, (directory/'stderr.txt').open('wb') as err, \
                    (directory/'prompt.txt').open('rb') as prompt:
                child_env=dict(os.environ)
                # CLI 2.1.185's print loop otherwise starts a small-model session
                # title request independently of --model. This documented switch
                # applies only to this owned CLI child; never alter global config.
                child_env['CLAUDE_CODE_DISABLE_TERMINAL_TITLE']='1'
                child=subprocess.Popen(job['command'],stdin=prompt,stdout=out,stderr=err,
                    cwd=job['cwd'],env=child_env,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
                _atomic(directory/'pid.json',{'supervisor_pid':os.getpid(),'child_pid':child.pid})
                deadline=time.monotonic()+job['timeout_s']
                while child.poll() is None:
                    if time.monotonic()>=deadline: reason='CLAUDE_TIMEOUT';break
                    if os.fstat(out.fileno()).st_size>2_000_000 or os.fstat(err.fileno()).st_size>2_000_000: reason='CLAUDE_OUTPUT_LIMIT';break
                    time.sleep(.05)
                if reason and child.poll() is None: child.kill()
                code=child.wait(timeout=10)
        except Exception as exc:
            reason='CLAUDE_SUPERVISOR_'+type(exc).__name__.upper()
            if child is not None and child.poll() is None:
                child.kill();child.wait(timeout=10)
        _atomic(directory/'exit.json',{'exit_code':code,'error':reason,'supervisor_pid':os.getpid(),
                                      'child_pid':child.pid if child else None})


class ClaudeAdapter:
    def __init__(self,store,artifacts,*,cli_command,run_root,isolated_synthetic=False,
                 allowed_models=('claude-fable-5-2',)):
        if isolated_synthetic is not True: raise ClaudeError('CLAUDE_ISOLATED_SYNTHETIC_REQUIRED')
        if not cli_command or not Path(cli_command[0]).is_absolute(): raise ClaudeError('CLAUDE_ABSOLUTE_EXECUTABLE_REQUIRED')
        self.store,self.artifacts=store,artifacts
        self.cli=list(cli_command);self.run_root=Path(run_root).absolute()
        if self.run_root.resolve()!=self.run_root:raise ClaudeError('CLAUDE_RUN_ROOT_LINK')
        self.allowed_models=frozenset(allowed_models)
        self.run_root.mkdir(parents=True,exist_ok=True)
        with store._tx() as conn: initialize_schema(conn)

    def attempt(self,task_id,fence):
        with closing(self.store._connect()) as conn:
            row=conn.execute('SELECT * FROM claude_attempts WHERE task_id=? AND fence=?',(task_id,fence)).fetchone()
            return dict(row) if row else None

    def _save(self,lease,**values):
        values['updated_at']=self.store._now()
        with self.store._tx() as conn:
            if not self.store._active(conn,lease.lease_id,lease.fence,lease.device_id)[2]:
                raise ClaudeError('CLAUDE_LEASE_LOST')
            conn.execute('UPDATE claude_attempts SET '+','.join(key+'=?' for key in values)+
                ' WHERE task_id=? AND fence=?',(*values.values(),lease.task_id,lease.fence))

    def _save_terminal_reports(self,lease,row,report,ref):
        """Close abandoned observers only after this lease read the same proven exit."""
        with self.store._tx() as conn:
            if not self.store._active(conn,lease.lease_id,lease.fence,lease.device_id)[2]:
                raise ClaudeError('CLAUDE_LEASE_LOST')
            conn.execute('UPDATE claude_attempts SET phase=?,report_ref=?,updated_at=? WHERE task_id=? AND fence=?',
                (report['status'],ref,self.store._now(),lease.task_id,lease.fence))
            if not report.get('launch_ref') or type(report.get('exit_code')) is not int:
                return
            prior=conn.execute('SELECT * FROM claude_attempts WHERE task_id=? AND fence<? AND origin_fence=?',
                (lease.task_id,lease.fence,row['origin_fence'])).fetchall()
            for old in prior:
                if (old['execution_authority']!='local_owned' or old['report_ref'] is not None or
                        old['phase'] not in ('launch_intent','running','uncertain') or
                        any(old[k]!=row[k] for k in ('run_dir','session_id','request_id','model','prompt_ref'))):
                    continue
                recovered={**report,'fence':old['fence'],'observed_by_fence':lease.fence,
                    'observation_basis':'recovered_same_origin_exit'}
                recovered_ref=self.artifacts.put(_json_bytes(recovered))
                conn.execute('UPDATE claude_attempts SET phase=?,report_ref=?,updated_at=? WHERE task_id=? AND fence=?',
                    (report['status'],recovered_ref,self.store._now(),lease.task_id,old['fence']))

    def _task(self,lease):
        task=self.store.get_task(lease.task_id)
        if task.access!='synthetic' or task.side_effect!='none' or task.egress!='none':
            raise ClaudeError('CLAUDE_SYNTHETIC_READ_ONLY_REQUIRED')
        if task.kind!='claude.synthetic' or set(task.params)!={'prompt_ref','model'}:
            raise ClaudeError('CLAUDE_TASK_CONTRACT')
        ref,model=task.params['prompt_ref'],task.params['model']
        if not isinstance(model,str) or model not in self.allowed_models:
            raise ClaudeError('CLAUDE_FULL_MODEL_REQUIRED')
        if not any(r['hash']==ref and r['version']!='latest' for r in task.input_refs):
            raise ClaudeError('CLAUDE_FIXED_PROMPT_REQUIRED')
        prompt=json.loads(self.artifacts.get(ref))
        if not isinstance(prompt,dict) or set(prompt)!={'synthetic','text'} or prompt['synthetic'] is not True or \
                not isinstance(prompt['text'],str) or not 0<len(prompt['text'].strip())<=30000:
            raise ClaudeError('CLAUDE_SYNTHETIC_PROMPT_REQUIRED')
        return task,ref,model,prompt['text']

    def _owned_directory(self,row):
        if row.get('execution_authority')!='local_owned':raise ClaudeError('CLAUDE_HISTORICAL_ONLY')
        expected=self.run_root/row['task_id']/str(row['origin_fence'])
        directory=Path(row['run_dir'])
        if directory!=expected or directory.resolve()!=directory.absolute():
            raise ClaudeError('CLAUDE_RUN_DIRECTORY_NOT_OWNED')
        return directory

    def _validate_resume_evidence(self,old,report,exit_status):
        """Re-read the complete owned exit chain; a report flag is not authority."""
        try:
            identity={k:old[k] for k in ('task_id','origin_fence','session_id','request_id','model','prompt_ref')}
            if old['request_id']!=old['task_id']+':'+str(old['origin_fence']):raise ValueError('request origin')
            raw=self.artifacts.get(report['stdout_ref'])
            directory=Path(old['run_dir'])
            if raw!=(directory/'stdout.json').read_bytes():raise ValueError('stdout changed')
            events=[json.loads(line) for line in raw.splitlines() if line.strip()]
            if any(not isinstance(event,dict) for event in events):raise ValueError('invalid stream')
            inits=[event for event in events if event.get('type')=='system' and event.get('subtype')=='init']
            init=json.loads(self.artifacts.get(report['init_ref']))
            if len(inits)!=1 or inits[0]!=init or init.get('tools')!=[] or init.get('mcp_servers')!=[]:
                raise ValueError('unverified isolation')
            if init.get('model')!=old['model'] or init.get('session_id')!=old['session_id']:
                raise ValueError('foreign init')
            # A resumable interruption has no completed result receipt. A result
            # mixed with a killed process requires explicit review, not replay.
            if any(event.get('type')=='result' for event in events):raise ValueError('ambiguous terminal result')
            exit_proof=json.loads(self.artifacts.get(report['exit_ref']))
            launch=json.loads(self.artifacts.get(report['launch_ref']))
            for body,schema in ((exit_proof,'sa_dispatch.claude_exit.v1'),(launch,'sa_dispatch.claude_launch.v1')):
                expected={'schema':schema,'synthetic':True,**identity}
                if not isinstance(body,dict) or any(type(body.get(k)) is not type(v) or body.get(k)!=v for k,v in expected.items()):
                    raise ValueError('foreign lifecycle proof')
            if any(type(exit_proof.get(k)) is not type(v) or exit_proof.get(k)!=v for k,v in exit_status.items()):
                raise ValueError('exit changed')
            pid=json.loads((directory/'pid.json').read_bytes())
            for key in ('child_pid','supervisor_pid'):
                if (type(launch.get(key)) is not int or launch[key]<=0 or
                        type(exit_proof.get(key)) is not int or type(pid.get(key)) is not int or
                        launch[key]!=exit_proof[key] or launch[key]!=pid[key]):
                    raise ValueError('unproven process exit')
        except (ValueError,KeyError,TypeError,OSError,RuntimeError,DispatchError):
            raise ClaudeError('CLAUDE_RESUME_EVIDENCE_UNVERIFIED') from None

    def _prepare(self,lease,timeout_s,budget_usd,resume):
        task,ref,model,text=self._task(lease)
        current=self.attempt(lease.task_id,lease.fence)
        if current:
            self._owned_directory(current)
            if (current['prompt_ref'],current['model'])!=(ref,model):raise ClaudeError('CLAUDE_JOB_CHANGED')
            return current,False
        with closing(self.store._connect()) as conn:
            old=conn.execute('SELECT * FROM claude_attempts WHERE task_id=? ORDER BY fence DESC LIMIT 1',(lease.task_id,)).fetchone()
            old=dict(old) if old else None
        if old:self._owned_directory(old)
        if old and (old['prompt_ref'],old['model'])!=(ref,model): raise ClaudeError('CLAUDE_JOB_CHANGED')
        if resume:
            if not old or not (Path(old['run_dir'])/'exit.json').is_file():
                raise ClaudeError('CLAUDE_RESUME_REQUIRES_CONFIRMED_EXIT')
            if old['phase'] != 'interrupted':
                raise ClaudeError('CLAUDE_RESUME_REQUIRES_INTERRUPTED_ATTEMPT')
            exit_status=json.loads((Path(old['run_dir'])/'exit.json').read_bytes())
            if (type(exit_status.get('exit_code')) is not int or
                    exit_status.get('error') not in ('CLAUDE_TIMEOUT','CLAUDE_OUTPUT_LIMIT')):
                raise ClaudeError('CLAUDE_RESUME_REQUIRES_CONFIRMED_EXIT')
            report=json.loads(self.artifacts.get(old['report_ref'])) if old.get('report_ref') else None
            expected={'schema':'sa_dispatch.claude_report.v1','synthetic':True,'task_id':lease.task_id,
                'fence':old['fence'],'origin_fence':old['origin_fence'],'session_id':old['session_id'],
                'request_id':old['request_id'],'model':model,'prompt_ref':ref,'status':'interrupted',
                'exit_code':exit_status['exit_code'],'error':exit_status['error']}
            if not isinstance(report,dict) or any(type(report.get(k)) is not type(v) or report.get(k)!=v for k,v in expected.items()):
                raise ClaudeError('CLAUDE_RESUME_REQUIRES_BOUND_REPORT')
            if report.get('resume_permitted') is not True:
                raise ClaudeError('CLAUDE_RESUME_ISOLATION_UNVERIFIED')
            self._validate_resume_evidence(old,report,exit_status)
        launch=old is None or resume
        directory=self.run_root/lease.task_id/str(lease.fence) if launch else Path(old['run_dir'])
        row=dict(task_id=lease.task_id,fence=lease.fence,origin_fence=lease.fence if launch else old['origin_fence'],
            session_id=old['session_id'] if old else str(uuid.uuid4()),model=model,prompt_ref=ref,
            request_id=f'{lease.task_id}:{lease.fence}' if launch else old['request_id'],
            run_dir=str(directory),phase='launch_intent' if launch else old['phase'],report_ref=None,
            updated_at=self.store._now(),execution_authority='local_owned')
        if launch:
            directory.mkdir(parents=True,exist_ok=False);(directory/'cwd').mkdir()
            cwd=directory/'cwd'
            if resume:
                old_job=json.loads((Path(old['run_dir'])/'job.json').read_bytes())
                cwd=Path(old_job['cwd'])
                if cwd.name!='cwd' or not cwd.parent.name.isdecimal() or cwd.parent.parent!=self.run_root/lease.task_id or cwd.resolve()!=cwd.absolute():
                    raise ClaudeError('CLAUDE_RESUME_CWD_NOT_OWNED')
            _atomic(directory/'job.json',{'command':command(self.cli,model=model,session_id=row['session_id'],
                resume=resume,budget_usd=budget_usd),'prompt':text,'timeout_s':timeout_s,'cwd':str(cwd)})
        with self.store._tx() as conn:
            if not self.store._active(conn,lease.lease_id,lease.fence,lease.device_id)[2]:raise ClaudeError('CLAUDE_LEASE_LOST')
            conn.execute('INSERT INTO claude_attempts('+','.join(row)+') VALUES('+','.join('?' for _ in row)+')',tuple(row.values()))
        return row,launch

    def _report(self,lease,row):
        directory=Path(row['run_dir']);exit_status=json.loads((directory/'exit.json').read_bytes())
        if type(exit_status.get('exit_code')) is not int:
            self._save(lease,phase='uncertain')
            raise ClaudeError('CLAUDE_UNVERIFIED_EXIT')
        raw=(directory/'stdout.json').read_bytes() if (directory/'stdout.json').exists() else b''
        raw_ref=self.artifacts.put(raw)
        pid=json.loads((directory/'pid.json').read_bytes()) if (directory/'pid.json').exists() else {}
        launch_ref=None
        if all(type(pid.get(k)) is int and pid[k]>0 and pid[k]==exit_status.get(k) for k in ('child_pid','supervisor_pid')):
            launch_ref=self.artifacts.put(_json_bytes({'schema':'sa_dispatch.claude_launch.v1','synthetic':True,
                **{k:row[k] for k in ('task_id','origin_fence','session_id','request_id','model','prompt_ref')},
                'child_pid':pid['child_pid'],'supervisor_pid':pid['supervisor_pid']}))
        error=exit_status['error'];data=None;init=None;validation_error=None
        interrupted=exit_status['error'] in ('CLAUDE_TIMEOUT','CLAUDE_OUTPUT_LIMIT')
        events=[];parse_error=False
        for line in raw.splitlines():
            if not line.strip():continue
            try:
                event=json.loads(line)
                if not isinstance(event,dict):raise ValueError('invalid event')
                events.append(event)
            except (ValueError,UnicodeError):parse_error=True
        # Init is independent of result: an interrupted stream can still prove
        # isolation while carrying no final usage/cost receipt.
        inits=[event for event in events if event.get('type')=='system' and event.get('subtype')=='init']
        if len(inits)==1:init=inits[0]
        isolation_error=None
        if init is None or init.get('tools')!=[] or init.get('mcp_servers')!=[]:
            isolation_error='CLAUDE_UNVERIFIED_TOOL_ISOLATION'
        elif init.get('session_id')!=row['session_id'] or init.get('model')!=row['model']:
            isolation_error='CLAUDE_INIT_MISMATCH'
        results=[event for event in events if event.get('type')=='result']
        if len(results)==1:data=results[0]
        elif results or not interrupted:validation_error='CLAUDE_INVALID_RESULT'
        if parse_error and not interrupted:validation_error='CLAUDE_INVALID_RESULT'
        usage_known=False
        if data is not None:
            if data.get('session_id')!=row['session_id']:validation_error='CLAUDE_SESSION_MISMATCH'
            elif exit_status['exit_code']!=0 or data.get('is_error') is not False:
                validation_error='CLAUDE_PROCESS_ERROR'
            elif data.get('type')!='result' or data.get('subtype')!='success':validation_error='CLAUDE_INVALID_RESULT'
            elif not isinstance(data.get('modelUsage'),dict) or set(data['modelUsage'])!={row['model']}:
                validation_error='MODEL_MISMATCH'
            else:
                usage=data['modelUsage'][row['model']]
                if not isinstance(usage,dict) or any(type(usage.get(k)) is not int or usage[k]<0
                        for k in ('inputTokens','outputTokens')) or \
                        type(usage.get('costUSD')) not in (int,float) or not math.isfinite(usage['costUSD']) or usage['costUSD']<0:
                    validation_error='CLAUDE_INVALID_USAGE'
                else:usage_known=True
        error=error or validation_error or isolation_error
        # Isolation failures are never hidden by a timeout in the resume gate.
        resume_permitted=interrupted and isolation_error is None and validation_error is None and not parse_error
        report={'schema':'sa_dispatch.claude_report.v1','synthetic':True,'task_id':lease.task_id,
            'fence':lease.fence,'origin_fence':row['origin_fence'],'session_id':row['session_id'],
            'request_id':row['request_id'],'model':row['model'],'prompt_ref':row['prompt_ref'],
            'requested_model':row['model'],'observed_model':init.get('model') if init else None,
            'usage_known':usage_known,'cost_unknown':not usage_known,
            'isolation_verified':isolation_error is None,'isolation_error':isolation_error,
            'validation_error':validation_error,'resume_permitted':resume_permitted,
            'init_ref':self.artifacts.put(_json_bytes(init)) if init is not None else None,
            'stdout_ref':raw_ref,'exit_ref':self.artifacts.put(_json_bytes({**exit_status,
                'schema':'sa_dispatch.claude_exit.v1','synthetic':True,
                **{k:row[k] for k in ('task_id','origin_fence','session_id','request_id','model','prompt_ref')}})),
            'launch_ref':launch_ref,'exit_code':exit_status['exit_code'],'error':error,
            'status':'interrupted' if interrupted else 'failed' if error else 'succeeded',
            'isolation':{'init_observed':init is not None,'tools':init.get('tools') if init else None,
                         'mcp_servers':init.get('mcp_servers') if init else None},
            'modelUsage':data.get('modelUsage',{}) if data else {},'result':data.get('result') if data else None}
        ref=self.artifacts.put(_json_bytes(report));self._save_terminal_reports(lease,row,report,ref)
        if not interrupted:
            self.store.complete(lease.lease_id,lease.fence,
                Outcome(status='failed' if error else 'succeeded',result={'claude_report_ref':ref},
                    evidence=(Evidence('claude_report',ref,'synthetic',artifact_hash=ref),)),lease.device_id)
        self.settle(lease.task_id)
        return report

    def settle(self,task_id):
        from .claude_settlement import settle_claude_runs
        return settle_claude_runs(self.store,self.artifacts,task_id)

    def run(self,lease,*,timeout_s=60,budget_usd=.05,resume=False,observe_timeout_s=90):
        if any(type(v) not in (int,float) or not math.isfinite(v) for v in (timeout_s,budget_usd,observe_timeout_s)) or \
                not 0<timeout_s<=300 or not 0<budget_usd<=.1 or not 0<observe_timeout_s<=600 or type(resume) is not bool:
            raise ClaudeError('CLAUDE_LIMIT_RANGE')
        with task_lock(self.store.path,lease.task_id):
            self.store.renew(lease.lease_id,lease.fence,lease.device_id)
            row,launch=self._prepare(lease,timeout_s,budget_usd,resume)
            directory=Path(row['run_dir'])
            if launch:
                env=dict(os.environ)
                env['PYTHONPATH']=str(Path(__file__).resolve().parent.parent)
                # The helper inherits authentication normally; nothing reads or
                # serializes credential values into jobs or reports.
                supervisor=subprocess.Popen([sys.executable,'-B','-m','sa_dispatch.claude_adapter','--supervise',str(directory/'job.json')],
                    stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,env=env,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0,
                    start_new_session=os.name!='nt')
                threading.Thread(target=supervisor.wait,daemon=True).start()
                self._save(lease,phase='running')
            deadline=time.monotonic()+observe_timeout_s
            while not (directory/'exit.json').is_file():
                self.store.renew(lease.lease_id,lease.fence,lease.device_id)
                if time.monotonic()>=deadline:
                    self._save(lease,phase='uncertain')
                    raise ClaudeError('CLAUDE_OBSERVATION_UNCERTAIN')
                time.sleep(.1)
            return self._report(lease,row)


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--supervise',required=True)
    supervise(parser.parse_args().supervise)
