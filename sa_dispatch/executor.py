"""Durable receipts for one leased component invocation."""

from dataclasses import asdict
import json
import os
from pathlib import Path
import re

from .artifacts import ArtifactStore
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

    def run_once(self):
        self.store.sweep()
        lease = self.store.claim(self.config.device_id)
        if lease is None:
            return None
        task = self.store.get_task(lease.task_id)
        expected_access = "synthetic" if self.config.synthetic else "local_only"
        if task.spec.access != expected_access:
            self.store.complete(lease.lease_id, lease.fence, Outcome("failed", {"code": "E_ACCESS_MISMATCH"}),
                                lease.device_id)
            return self.store.get_task(lease.task_id)
        kind = self.kinds[task.spec.kind]
        request = kind.request(task)
        self.log(lease, "intent", write_key=request["idempotency_key"])
        try:
            self.log(lease, "invoked")
            result = self.runner.run_component(kind.component, request)
            response = result.json
            artifact = self.artifacts.put(receipt_bytes(response))
            self.log(lease, "result", artifact=artifact, exit=result.exit)
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

    def reconcile(self, task):
        if task.status != "unknown" or not self.config.synthetic:
            return {"verdict": "still_unknown"}
        kind = self.kinds[task.spec.kind]
        original = kind.request(task)
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
        self.store.resolve_unknown(task.task_id, "confirmed_done", "fixture-reconciliation", outcome=outcome)
        return {"verdict": "confirmed_done", "task_id": task.task_id}
