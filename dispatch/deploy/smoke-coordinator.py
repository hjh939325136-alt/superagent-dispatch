"""Submit and complete one synthetic task through a local or forwarded port."""

import argparse
from base64 import b64decode, b64encode
from hashlib import sha256
import json
from pathlib import Path
import re
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


CAPABILITY = "component:mgmtcore"
SOURCE = b"synthetic coordinator smoke input\n"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def digest(data):
    return "sha256:" + sha256(data).hexdigest()


def client(port, token_path):
    with token_path.open(encoding="ascii") as stream:
        token = stream.read(1025).strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{32,128}", token):
        raise ValueError("expected a URL-safe token in the selected file")
    opener = build_opener(ProxyHandler({}), NoRedirect())

    def call(route, body=None):
        request = Request(f"http://127.0.0.1:{port}{route}",
                          None if body is None else json.dumps(body).encode(),
                          {"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        try:
            with opener.open(request, timeout=10) as response:
                payload = json.load(response)
        except HTTPError as exc:
            raise RuntimeError(f"{route} returned HTTP {exc.code}") from None
        if payload.get("ok") is not True:
            raise RuntimeError(f"{route} did not confirm success")
        return payload["data"]

    return call


def submit_task(call, key):
    source_hash = digest(SOURCE)
    spec = {"idem_key": key, "kind": "hotspot.topic_fetch", "kind_version": 1,
            "params": {"op": "topic.list", "filters": {"limit": 1}},
            "input_refs": [{"owner": "fixture", "kind": "source", "id": "smoke", "version": "1",
                            "hash": source_hash}],
            "required_caps": [CAPABILITY], "access": "synthetic", "egress": "none", "max_attempts": 2}
    return call("/submit", {"spec": spec, "inputs": {source_hash: b64encode(SOURCE).decode("ascii")}})


def complete_task(call, key, task_id):
    lease = call("/claim", {})["lease"]
    if not lease or lease["task_id"] != task_id:
        raise RuntimeError("expected the selected smoke task in an isolated queue")
    binding = {key: lease[key] for key in ("lease_id", "fence")}
    assigned = call("/task", binding)
    if assigned["spec"]["idem_key"] != key or assigned["spec"]["access"] != "synthetic":
        raise RuntimeError("unexpected task scope or submission key")
    call("/renew", binding)
    source_hash = digest(SOURCE)
    blob = call("/artifact/get", {**binding, "hash": source_hash})
    if b64decode(blob["content_base64"], validate=True) != SOURCE:
        raise RuntimeError("synthetic input bytes did not match")

    receipt = json.dumps({"scope": "synthetic", "input_hash": source_hash, "task_id": task_id},
                         sort_keys=True).encode()
    receipt_hash = digest(receipt)
    put = call("/artifact/put", {**binding, "hash": receipt_hash, "content_base64": b64encode(receipt).decode()})
    if put["hash"] != receipt_hash:
        raise RuntimeError("receipt hash did not match")
    finished = call("/complete", {**binding, "outcome": {"status": "succeeded",
                    "result": {"receipt": receipt_hash}, "evidence": [{"kind": "smoke_receipt",
                    "content_hash": receipt_hash, "artifact_hash": receipt_hash, "access": "synthetic"}]}})
    status = call("/status", {"task_id": task_id})
    if not finished.get("completed") or status.get("status") != "succeeded":
        raise RuntimeError("completion could not be verified")
    return {"task_id": task_id, "status": status["status"], "attempts": status["attempts"],
            "receipt": receipt_hash}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    actions = parser.add_subparsers(dest="action", required=True)
    for action, role in (("submit", "dispatcher"), ("complete", "worker")):
        command = actions.add_parser(action)
        command.add_argument(f"--{role}-token-file", type=Path, dest="token", required=True)
        command.add_argument("--key", required=True)
        if action == "complete":
            command.add_argument("--task-id", required=True)
    args = parser.parse_args()
    try:
        if not 1 <= args.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        call = client(args.port, args.token)
        call("/health")
        if args.action == "submit":
            result = submit_task(call, args.key)
        else:
            result = complete_task(call, args.key, args.task_id)
        print(json.dumps(result, sort_keys=True))
    except (ValueError, OSError, RuntimeError, KeyError) as exc:
        parser.exit(1, f"Smoke check failed: {exc}\n")


if __name__ == "__main__":
    main()
