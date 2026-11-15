"""Durable receipts for one leased component invocation."""

from dataclasses import asdict
import json
import os
from pathlib import Path
import re
import sqlite3
from contextlib import closing

from .artifacts import ArtifactStore
from .canonical import sha256_ref
from .models import DispatchError, Evidence, FenceRejected, Outcome


def receipt_bytes(response):
    return json.dumps(response, ensure_ascii=False, allow_nan=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


class Executor:
    def __init__(self, config, store, runner, kinds):
        self.config, self.store, self.runner, self.kinds = config, store, runner, kinds
        self.artifacts = ArtifactStore(config.artifact_root)

    def journal_path(self, lease):
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", lease.lease_id):
            raise ValueError("invalid lease identity")
        return Path(self.config.journal_dir) / (lease.lease_id + ".jsonl")

    def log(self, lease, phase, **data):
        path = self.journal_path(lease)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as handle:
            handle.write(receipt_bytes({"phase": phase, "lease": asdict(lease), **data}) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())

    def outcome(self, task, response, artifact):
        kind = self.kinds[task.spec.kind]
        result = kind.result(response, task)
        result["receipt"] = artifact
        return Outcome("succeeded", result, (Evidence("component_response", artifact, task.spec.access,
                                                       artifact_hash=artifact),))

    def _request(self, task):
        kind = self.kinds[task.spec.kind]
        hook = getattr(kind, 'request_with_context', None)
        if not callable(hook):
            return kind.request(task)
        approval = None
        uri = Path(self.config.db_path).resolve().as_uri() + '?mode=ro'
        with closing(sqlite3.connect(uri, uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA query_only=ON')
            conn.execute('BEGIN')
            row = conn.execute("""SELECT a.* FROM control_task_rules r JOIN control_approvals a
                ON a.approval_id=r.approval_id WHERE r.task_id=? AND r.fingerprint=?""",
                (task.task_id, task.fingerprint)).fetchone()
            if row:
                approval = dict(row)
                if approval['task_id'] != task.task_id or approval['fingerprint'] != task.fingerprint:
                    raise ValueError('control approval identity mismatch')
                approval['proof_ref'] = approval['evidence_ref']
                approval['proof'] = json.loads(self.artifacts.get(approval['proof_ref']))
                if not isinstance(approval['proof'], dict):
                    raise ValueError('invalid control approval proof')
        return hook(task, {'task_fingerprint': task.fingerprint, 'control_approval': approval})

    def _original_request(self, task):
        if not callable(getattr(self.kinds[task.spec.kind], 'request_with_context', None)):
            return self.kinds[task.spec.kind].request(task)
        references = set()
        for path in Path(self.config.journal_dir).glob('*.jsonl'):
            for line in path.read_bytes().splitlines(keepends=True):
                if not line.endswith(b'\n'):continue
                row = json.loads(line)
                lease = row.get('lease', {})
                if (row.get('phase') == 'intent' and lease.get('task_id') == task.task_id and
                        lease.get('fence') == task.fence and row.get('request_ref')):
                    references.add(row['request_ref'])
        if len(references) != 1:
            raise ValueError('immutable original request unavailable')
        request = json.loads(self.artifacts.get(references.pop()))
        if request.get('request_id') != task.task_id:
            raise ValueError('original request belongs to another task')
        return request

    def run_once(self):
        self.store.sweep()
        runnable={name for name,kind in self.kinds.items() if not getattr(kind,'dedicated_runner',False)}
        lease = self.store.claim(self.config.device_id,kinds=runnable)
        if lease is None:
            return None
        task = self.store.get_task(lease.task_id)
        expected_access = "synthetic" if self.config.synthetic else "local_only"
        if task.spec.access != expected_access:
            self.store.complete(lease.lease_id, lease.fence, Outcome("failed", {"code": "E_ACCESS_MISMATCH"}),
                                lease.device_id)
            return self.store.get_task(lease.task_id)
        kind = self.kinds[task.spec.kind]
        try:
            request = self._request(task)
            request_ref = self.artifacts.put(receipt_bytes(request)) if callable(getattr(kind, 'request_with_context', None)) else None
            self.log(lease, "intent", write_key=request["idempotency_key"], request_ref=request_ref)
        except (ValueError, KeyError, TypeError, OSError, RuntimeError, DispatchError) as exc:
            self.store.complete(lease.lease_id, lease.fence,
                Outcome('failed', {'code':getattr(exc,'code','E_REQUEST_REJECTED'),'phase':'pre_invocation'}), lease.device_id)
            return self.store.get_task(task.task_id)
        try:
            self.log(lease, "invoked")
            result = self.runner.run_component(kind.component, request)
            response = result.json
            artifact = self.artifacts.put(receipt_bytes(response))
            self.log(lease, "result", artifact=artifact, exit=result.exit)
            rejection = getattr(kind, 'rejection_outcome', None)
            if callable(rejection) and response.get('request_id') == request['request_id']:
                rejected = rejection(response, task)
                if rejected is not None:
                    if not isinstance(rejected, Outcome) or rejected.status != 'failed':
                        raise ValueError('invalid verified precommit rejection')
                    rejected = Outcome('failed', {**rejected.result,'receipt':artifact},
                        (*rejected.evidence, Evidence('component_rejection', artifact, task.spec.access, artifact_hash=artifact)))
                    self.store.complete(lease.lease_id, lease.fence, rejected, lease.device_id)
                    self.log(lease, 'completed_ack')
                    return self.store.get_task(task.task_id)
            if result.exit != 0 or response.get("request_id") != request["request_id"]:
                raise ValueError("component result could not be verified")
            outcome = self.outcome(task, response, artifact)
        except (ValueError, OSError, RuntimeError) as exc:
            code = getattr(exc, "code", "E_COMPONENT_UNVERIFIED")
            try:
                if kind.side_effect == "none":
                    self.store.complete(lease.lease_id, lease.fence, Outcome("failed", {"code": code}),
                                        lease.device_id)
                else:
                    self.store.mark_unknown(lease.lease_id, lease.fence, code, lease.device_id)
            except FenceRejected:
                self.store.sweep()
            return self.store.get_task(lease.task_id)
        try:
            self.store.complete(lease.lease_id, lease.fence, outcome, lease.device_id)
        except FenceRejected:
            self.store.sweep()
            return self.store.get_task(lease.task_id)
        self.log(lease, "completed_ack")
        return self.store.get_task(lease.task_id)

    def recover(self):
        from .models import Lease
        recovered = []
        for path in sorted(Path(self.config.journal_dir).glob("*.jsonl")):
            rows = []
            for line in path.read_bytes().splitlines(keepends=True):
                if line.endswith(b"\n"):
                    rows.append(json.loads(line))
            if not rows:
                continue
            row = rows[-1]
            if row["phase"] == "completed_ack":
                continue
            lease = Lease(**row["lease"])
            if lease.device_id != self.config.device_id:
                continue
            task = self.store.get_task(lease.task_id)
            if task.status == "leased" and task.fence == lease.fence:
                try:
                    if row["phase"] == "result" and row["exit"] == 0:
                        response = json.loads(self.artifacts.get(row["artifact"]))
                        if response.get("request_id") != task.task_id:
                            raise ValueError("receipt request does not bind to task")
                        outcome = self.outcome(task, response, row["artifact"])
                        self.store.complete(lease.lease_id, lease.fence, outcome, lease.device_id)
                        self.log(lease, "completed_ack")
                    elif self.kinds[task.spec.kind].side_effect != "none":
                        self.store.mark_unknown(lease.lease_id, lease.fence, "worker_interrupted", lease.device_id)
                except FenceRejected:
                    pass
                except (ValueError, OSError, RuntimeError, DispatchError):
                    if self.kinds[task.spec.kind].side_effect != "none":
                        try:
                            self.store.mark_unknown(lease.lease_id, lease.fence, "receipt_unverified", lease.device_id)
                        except FenceRejected:
                            pass
                recovered.append(task.task_id)
        self.store.sweep()
        return recovered

    def _lookup_reconcile(self, task, kind, original):
        """Optional trusted kind extension; lookup can prove history, never retry authority."""
        request_hash = sha256_ref(receipt_bytes(original))
        expected = {'task_id': task.task_id, 'fence': task.fence,
                    'operation': original['operation'], 'write_key': original['idempotency_key'],
                    'request_hash': request_hash}
        try:
            if not isinstance(expected['write_key'], str) or not expected['write_key']:
                raise ValueError('write key required')
            request = kind.lookup_request(task, original, request_hash)
            if (not isinstance(request, dict) or request.get('operation') != 'operation.lookup' or
                    request.get('idempotency_key') is not None or not isinstance(request.get('payload'), dict) or
                    any(request['payload'].get(key) != value for key, value in expected.items()) or
                    request.get('request_id') != 'reconcile-' + task.task_id):
                raise ValueError('invalid read-only lookup request')
            payload = request['payload']
            validate_lookup = getattr(kind, 'validate_lookup', None)
            if set(payload) != set(expected) and not callable(validate_lookup):
                raise ValueError('extended lookup requires plugin validation')
            run = self.runner.run_component(kind.component, request)
            envelope = run.json
            if (run.exit != 0 or not isinstance(envelope, dict) or envelope.get('ok') is not True or
                    envelope.get('request_id') != request['request_id']):
                raise ValueError('unverified lookup envelope')
            data = envelope['data']
            if (not isinstance(data, dict) or data.get('state') != 'committed' or
                    any(data.get(key) != value for key, value in payload.items()) or
                    type(data.get('fence')) is not int or type(data.get('current_usable')) is not bool):
                raise ValueError('unbound or incomplete lookup')
            if callable(validate_lookup) and validate_lookup(task, original, data) is not True:
                raise ValueError('plugin lookup binding rejected')
            response = data['response']
            if (not isinstance(response, dict) or response.get('request_id') != task.task_id or
                    response.get('ok') is not True or
                    data.get('response_hash') != sha256_ref(receipt_bytes(response))):
                raise ValueError('unverified historical response')
            artifact = self.artifacts.put(receipt_bytes(envelope))
            outcome = self.outcome(task, response, artifact)
            proof = self.artifacts.put(receipt_bytes({
                'schema': 'sa_dispatch.reconciliation.v1', **expected,
                'idem_key': task.idem_key, 'component': kind.component,
                'lookup_ref': artifact, 'current_usable': data['current_usable'],
                'outcome_hash': sha256_ref(receipt_bytes(asdict(outcome)))}))
            self.store.resolve_unknown(task.task_id, 'confirmed_done', 'fixture-reconciliation',
                outcome=outcome, evidence_ref=proof, expected_fence=task.fence, artifacts=self.artifacts)
            return {'verdict': 'confirmed_done', 'task_id': task.task_id,
                    'current_usable': data['current_usable'], 'historical_commit': True}
        except (ValueError, KeyError, TypeError, OSError, RuntimeError, DispatchError):
            return {'verdict': 'still_unknown'}

    def reconcile(self, task):
        if task.status != "unknown" or not self.config.synthetic:
            return {"verdict": "still_unknown"}
        kind = self.kinds[task.spec.kind]
        try:
            original = self._original_request(task)
        except (ValueError, KeyError, TypeError, OSError, RuntimeError):
            return {'verdict':'still_unknown'}
        if callable(getattr(kind, "lookup_request", None)):
            return self._lookup_reconcile(task, kind, original)
        request = {
            **original, "request_id": "reconcile-" + task.task_id, "operation": "fake.write.get",
            "idempotency_key": None,
            "payload": {"operation": original["operation"], "idempotency_key": original["idempotency_key"]},
        }
        try:
            run = self.runner.run_component(kind.component, request)
            data = run.json["data"]
            if (run.exit != 0 or run.json.get("ok") is not True or
                    run.json.get("request_id") != request["request_id"] or data.get("found") is not True):
                return {"verdict": "still_unknown"}
            response = {"schema_version": "0.1", "ok": True, "data": data["result"]}
            artifact = self.artifacts.put(receipt_bytes(run.json))
            outcome = self.outcome(task, response, artifact)
        except (ValueError, KeyError, OSError, RuntimeError):
            return {"verdict": "still_unknown"}
        proof = self.artifacts.put(receipt_bytes({
            'schema': 'sa_dispatch.reconciliation.v1', 'task_id': task.task_id,
            'fence': task.fence, 'idem_key': task.idem_key, 'component': kind.component,
            'outcome_hash': sha256_ref(receipt_bytes(asdict(outcome)))}))
        self.store.resolve_unknown(task.task_id, "confirmed_done", "fixture-reconciliation", outcome=outcome,
                                   evidence_ref=proof, expected_fence=task.fence, artifacts=self.artifacts)
        return {"verdict": "confirmed_done", "task_id": task.task_id}
