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
        from .host_kind import KINDS as HOST_KINDS
        from .claude_kind import KINDS as CLAUDE_KINDS
        plugins = {"hotspot": KINDS, 'host_fixture':HOST_KINDS, 'claude_runtime':CLAUDE_KINDS}
    parser = argparse.ArgumentParser(prog="sa_dispatch")
    parser.add_argument("--config", type=Path)
    subs = parser.add_subparsers(dest="action", required=True)
    for action in ("init-db", "device-register", "run-once", "recover", "sweep"):
        subs.add_parser(action)
    subs.add_parser("selftest").add_argument("--bundle-output", type=Path)
    server = subs.add_parser("serve", help="serve a dedicated synthetic ledger on IPv4 loopback")
    server.add_argument("--identities", type=Path, required=True)
    server.add_argument("--port", type=int, default=8765)
    server.add_argument("--schedules", action="store_true", help="enable synthetic read-only interval schedules")
    subs.add_parser("schedule-upsert",help="upsert a synthetic rule; max_slots is lifetime across versions; omit to preserve, null to remove").add_argument("--input", default="-")
    subs.add_parser("schedule-tick")
    subs.add_parser("schedule-status")
    prove = subs.add_parser("prove-idempotency", help="prove a bundled synthetic component with two isolated same-key writes")
    prove.add_argument("--component", required=True)
    prove.add_argument("--kind", required=True)
    subs.add_parser("submit").add_argument("--input", default="-")
    subs.add_parser("status").add_argument("task_id", nargs="?")
    subs.add_parser("receipt").add_argument("task_id")
    subs.add_parser("trace").add_argument("task_id")
    host=subs.add_parser('host-run',help='run an opt-in synthetic task against an isolated fixture workbench')
    host.add_argument('--base-url',required=True)
    host.add_argument('--isolated-synthetic',action='store_true',required=True)
    host.add_argument('--lease-id')
    host.add_argument('--timeout',type=float,default=30)
    host.add_argument('--resume-interrupted',action='store_true')
    claude=subs.add_parser('claude-run',help='run an isolated synthetic text task through the verified Claude CLI')
    claude.add_argument('--executable',type=Path,required=True)
    claude.add_argument('--run-root',type=Path,required=True)
    claude.add_argument('--isolated-synthetic',action='store_true',required=True)
    claude.add_argument('--lease-id')
    claude.add_argument('--timeout',type=float,default=60)
    claude.add_argument('--budget-usd',type=float,default=.05)
    claude.add_argument('--observe-timeout',type=float,default=90)
    claude.add_argument('--resume-interrupted',action='store_true')
    settlement=subs.add_parser('claude-settle',help='settle proven synthetic Claude launch counts without running a process')
    settlement.add_argument('task_id')
    settlement.add_argument('--isolated-synthetic',action='store_true',required=True)
    for command in ('host-cancel','host-settle'):
        item=subs.add_parser(command)
        item.add_argument('task_id')
        item.add_argument('--base-url',required=True)
        item.add_argument('--isolated-synthetic',action='store_true',required=True)
        if command=='host-cancel':item.add_argument('--fence',type=int,required=True)
    subs.add_parser('control').add_argument('--input',default='-')
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
                serve(config, kinds, args.identities, args.port, schedules=args.schedules)
                return 0
            if args.action.startswith('schedule-') and (not config.synthetic or config.components):
                raise ValueError('schedule commands require a dedicated synthetic configuration without components')
            if args.action == 'prove-idempotency' and not config.synthetic:
                raise ValueError('automatic capability proof requires a synthetic configuration')
            # Reject scope before SqliteStore can initialize/migrate a ledger.
            if args.action in ('claude-run','claude-settle'):
                if not config.synthetic or config.components or 'claude_runtime' not in config.plugins:
                    raise ValueError('Claude lifecycle requires an isolated synthetic claude_runtime configuration without components')
                if args.action=='claude-run' and (not args.executable.is_absolute() or not args.executable.is_file()):
                    raise ValueError('trusted local absolute executable required')
            if args.action in ('host-run','host-cancel','host-settle'):
                if not config.synthetic or config.components or 'host_fixture' not in config.plugins:
                    raise ValueError('host lifecycle requires an isolated synthetic host_fixture configuration without components')
            store = SqliteStore(config.db_path, lease_ttl_seconds=config.lease_ttl_seconds, kinds=kinds,
                                component_config=config)
            executor = Executor(config, store, ComponentRunner(config), kinds)
            action = args.action
            if action == 'claude-settle':
                from .claude_settlement import settle_task
                data=settle_task(store,ArtifactStore(config.artifact_root),args.task_id,actor='claude-adapter')
            elif action == 'claude-run':
                from .claude_adapter import ClaudeAdapter
                from .models import Lease
                adapter=ClaudeAdapter(store,ArtifactStore(config.artifact_root),
                    cli_command=[str(args.executable)],run_root=args.run_root,
                    isolated_synthetic=args.isolated_synthetic)
                if args.lease_id:
                    row=store.get_lease(args.lease_id)
                    if not row or row.device_id!=config.device_id:raise ValueError('owned lease required')
                    lease=Lease(**{k:row[k] for k in Lease.__dataclass_fields__})
                else:
                    lease=store.claim(config.device_id,kinds={'claude.synthetic'})
                data={'task':None} if lease is None else adapter.run(lease,timeout_s=args.timeout,
                    budget_usd=args.budget_usd,resume=args.resume_interrupted,observe_timeout_s=args.observe_timeout)
            elif action in ('host-run','host-cancel','host-settle'):
                from .host_adapter import HostAdapter,WorkbenchClient
                from .models import Lease
                host_adapter=HostAdapter(store,ArtifactStore(config.artifact_root),WorkbenchClient(args.base_url),
                                         isolated_synthetic=args.isolated_synthetic)
                if action=='host-cancel':
                    data=host_adapter.cancel(args.task_id,args.fence)
                elif action=='host-settle':
                    data=host_adapter.settle(args.task_id)
                elif args.lease_id:
                    row=store.get_lease(args.lease_id)
                    if not row or row.device_id!=config.device_id:raise ValueError('owned lease required')
                    lease=Lease(**{k:row[k] for k in Lease.__dataclass_fields__})
                else:
                    lease=store.claim(config.device_id,kinds={'host.synthetic'})
                if action=='host-run':
                    data={'task':None} if lease is None else host_adapter.run(
                            lease,timeout_s=args.timeout,resume_interrupted=args.resume_interrupted)
            elif action == 'control':
                if not config.synthetic:raise ValueError('control requires a synthetic configuration')
                from .control_cli import execute_request
                data=execute_request(store,ArtifactStore(config.artifact_root),request(args.input))
            elif action == "prove-idempotency":
                from .component_proof import prove_idempotency
                if args.kind not in kinds:
                    raise ValueError("task kind is not registered")
                data = prove_idempotency(config, args.component, kinds[args.kind],
                                         artifacts=ArtifactStore(config.artifact_root))
                store.register_capability(args.component, data["proof"]["capability"],
                                          lookup_by_write_key=True, idempotency_proof="verified",
                                          proof_ref=data["proof_ref"], artifacts=ArtifactStore(config.artifact_root))
                data["registration_status"] = "operator_registered"
            elif action.startswith('schedule-'):
                if not config.synthetic or config.components:
                    raise ValueError('schedule commands require a dedicated synthetic configuration without components')
                from .scheduler import Scheduler
                scheduler = Scheduler(store)
                if action == 'schedule-upsert':
                    data = scheduler.upsert(**request(args.input))
                elif action == 'schedule-tick':
                    data = {'fires': scheduler.tick(synthetic_handoff=True)}
                else:
                    data = scheduler.status()
            elif action == "init-db":
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
                result = task.result or {}
                ref = result.get("receipt") or result.get('claude_report_ref') or result.get('host_report_ref')
                if ref is None:
                    raise ValueError("task has no accepted receipt")
                data = {"task_id": task.task_id, "sha256": ref,
                        "response": json.loads(ArtifactStore(config.artifact_root).get(ref))}
            elif action == "trace":
                from .unified_events import UnifiedEventStore
                task = store.get_task(args.task_id)
                projection = UnifiedEventStore(store.path, clock=store.clock)
                data = {"task_id": task.task_id, "trace_id": task.task_id,
                        "events": projection.project_task(store, task.task_id),
                        "reconciliations": store.reconciliation_history(task.task_id)}
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
