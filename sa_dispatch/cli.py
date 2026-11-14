"""JSON command line for the local dispatch ledger."""

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
import sys

from sa_dispatch_hotspot import KINDS

from .adapters import task_registry
from .artifacts import ArtifactStore
from .bundle import export_bundle, import_bundle, verify_bundle
from .config import load_config
from .executor import Executor, receipt_bytes
from .models import DispatchError, Ref, TaskSpec
from .runner import ComponentRunner
from .store_sqlite import SqliteStore


def request(path):
    limit = 512 * 1024
    if path == "-":
        content = sys.stdin.buffer.read(limit + 1)
    else:
        with Path(path).open("rb") as source:
            content = source.read(limit + 1)
    if len(content) > limit:
        raise ValueError("task request exceeds 512 KiB")

    def unique(pairs):
        result = dict(pairs)
        if len(result) != len(pairs):
            raise ValueError("task request has duplicate fields")
        return result

    value = json.loads(content, object_pairs_hook=unique)
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


def submit(config, store, path):
    data = request(path)
    references = data.pop("input_refs", [])
    if not isinstance(references, list):
        raise ValueError("input_refs must be an array")
    refs = tuple(Ref(**ref) for ref in references)
    capabilities = data.pop("required_caps", [])
    if not isinstance(capabilities, list):
        raise ValueError("required_caps must be an array")
    spec = TaskSpec(**data, input_refs=refs, required_caps=tuple(capabilities))
    if spec.access != ("synthetic" if config.synthetic else "local_only") or spec.egress != "none":
        raise ValueError("task access does not match this execution environment")
    task, reused = store.submit(spec)
    return {"task": task.to_dict(), "deduplicated": reused}


def main(argv=None, *, plugins=None):
    if plugins is None:
        plugins = {"hotspot": KINDS}
    parser = argparse.ArgumentParser(prog="sa_dispatch")
    parser.add_argument("--config", type=Path)
    subs = parser.add_subparsers(dest="action", required=True)
    for action in ("init-db", "device-register", "run-once", "recover", "sweep"):
        subs.add_parser(action)
    subs.add_parser("selftest").add_argument("--bundle-output", type=Path)
    server = subs.add_parser("serve", help="serve a dedicated synthetic ledger on IPv4 loopback")
    server.add_argument("--identities", type=Path, required=True)
    server.add_argument("--port", type=int, default=8765)
    subs.add_parser("submit").add_argument("--input", default="-")
    subs.add_parser("status").add_argument("task_id", nargs="?")
    subs.add_parser("receipt").add_argument("task_id")
    subs.add_parser("reconcile").add_argument("task_id")
    subs.add_parser("cancel").add_argument("task_id")
    subs.add_parser("export-bundle").add_argument("directory", type=Path)
    subs.add_parser("import-bundle").add_argument("directory", type=Path)
    subs.add_parser("verify-bundle").add_argument("directory", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.action == "verify-bundle":
            data = verify_bundle(args.directory)
        elif args.action == "selftest":
            from .selftest import run_selftest
            data = run_selftest(args.bundle_output)
        else:
            if args.config is None:
                raise ValueError("--config is required")
            config = load_config(args.config, allowed_plugins=plugins)
            kinds = task_registry(config.plugins, plugins)
            if args.action == "serve":
                from .service import serve
                serve(config, kinds, args.identities, args.port)
                return 0
            store = SqliteStore(config.db_path, lease_ttl_seconds=config.lease_ttl_seconds, kinds=kinds)
            executor = Executor(config, store, ComponentRunner(config), kinds)
            action = args.action
            if action == "init-db":
                data = {"initialized": True}
            elif action == "device-register":
                store.register_device(config.device_id, config.capabilities)
                data = {"device_id": config.device_id}
            elif action == "submit":
                data = submit(config, store, args.input)
            elif action == "run-once":
                task = executor.run_once()
                data = {"task": task.to_dict() if task is not None else None}
            elif action == "status":
                data = {"task": store.get_task(args.task_id).to_dict(),
                        "events": store.events(args.task_id)} if args.task_id else {
                    "tasks": [task.to_dict() for task in store.list_tasks()]}
            elif action == "receipt":
                task = store.get_task(args.task_id)
                ref = (task.result or {}).get("receipt")
                if ref is None:
                    raise ValueError("task has no accepted receipt")
                data = {"task_id": task.task_id, "sha256": ref,
                        "response": json.loads(ArtifactStore(config.artifact_root).get(ref))}
            elif action == "reconcile":
                data = executor.reconcile(store.get_task(args.task_id))
            elif action == "recover":
                data = {"checked_tasks": executor.recover()}
            elif action == "sweep":
                data = asdict(store.sweep())
            elif action == "cancel":
                store.cancel(args.task_id, config.device_id)
                data = {"task": store.get_task(args.task_id).to_dict()}
            elif action == "export-bundle":
                data = export_bundle(config, args.directory)
            elif action == "import-bundle":
                data = import_bundle(config, args.directory, store)
        response = {"schema": "sa_dispatch.v0", "ok": True, "data": data}
        code = 0
    except (DispatchError, ValueError, TypeError, OSError, RuntimeError, sqlite3.Error) as exc:
        error = getattr(exc, "code", "E_VALIDATION")
        code = getattr(exc, "exit_code", {"E_EXPORT_BLOCKED": 6, "E_CONFIG": 7, "E_PLATFORM": 7}.get(error, 2))
        response = {"schema": "sa_dispatch.v0", "ok": False,
                    "error": {"code": error, "message": str(exc), "retryable": getattr(exc, "retryable", False)}}
    sys.stdout.buffer.write(receipt_bytes(response) + b"\n")
    return code
