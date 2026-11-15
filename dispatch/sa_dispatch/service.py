"""Foreground service for a dedicated synthetic coordinator ledger."""

import json
import re
import signal
import sys
from time import monotonic

from .artifacts import ArtifactStore
from .store_sqlite import SqliteStore
from .transport import CoordinatorAPI, create_server


def load_identities(path):
    def unique_fields(pairs):
        result = dict(pairs)
        if len(pairs) != len(result):
            raise ValueError("duplicate identity configuration field")
        return result

    with path.open("rb") as stream:
        content = stream.read(64 * 1024 + 1)
    if len(content) > 64 * 1024:
        raise ValueError("identity configuration exceeds 64 KiB")
    try:
        data = json.loads(content, object_pairs_hook=unique_fields)
    except (UnicodeError, ValueError):
        raise ValueError("invalid identity configuration JSON") from None
    if not isinstance(data, dict) or set(data) != {"devices"}:
        raise ValueError("identity configuration requires a devices object")
    devices = data["devices"]
    if not isinstance(devices, dict) or not 1 <= len(devices) <= 128:
        raise ValueError("expected one to 128 devices")

    hashes, roles, workers = {}, {}, {}
    for device, entry in devices.items():
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", device):
            raise ValueError("invalid device identifier")
        if not isinstance(entry, dict) or not {"token_sha256", "roles"} <= entry.keys() <= {
            "token_sha256", "roles", "capabilities"
        }:
            raise ValueError("invalid identity fields")
        digest, grant = entry["token_sha256"], entry["roles"]
        capabilities = entry.get("capabilities", [])
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("expected a lowercase SHA-256 token digest")
        if not isinstance(grant, list) or not grant or any(role not in ("dispatcher", "worker") for role in grant):
            raise ValueError("expected dispatcher and/or worker role")
        if (not isinstance(capabilities, list) or len(capabilities) > 64
                or any(not isinstance(cap, str) or not cap.strip() or len(cap) > 128 for cap in capabilities)):
            raise ValueError("invalid worker capabilities")
        if capabilities and "worker" not in grant:
            raise ValueError("capabilities require a worker role")
        hashes[device], roles[device] = digest, grant
        if "worker" in grant:
            workers[device] = capabilities
    return hashes, roles, workers


def serve(config, kinds, identities, port, *, schedules=False):
    if not config.synthetic or config.components or not kinds:
        raise ValueError("serve requires a dedicated synthetic configuration, a task plugin, and no local components")
    if config.execution_route not in (None, 'http_synthetic'):
        raise ValueError('HTTP service requires the HTTP execution route')
    if not 0 <= port <= 65535:
        raise ValueError("port must be between 0 and 65535")
    hashes, roles, workers = load_identities(identities)
    store = SqliteStore(config.db_path, lease_ttl_seconds=config.lease_ttl_seconds, kinds=kinds,
                        route=config.execution_route)
    api = CoordinatorAPI(store, hashes, roles=roles, synthetic_handoff=True,
                         artifact_store=ArtifactStore(config.artifact_root))
    with create_server(api, port=port) as server:
        api._synthetic_scope()
        scheduler = None
        if schedules:
            from .scheduler import Scheduler
            scheduler = Scheduler(store)
        for worker, caps in workers.items():
            store.register_device(worker, caps)
        store.sweep(worker_ids=workers)
        stopping = False

        def stop(_signum, _frame):
            nonlocal stopping
            stopping = True

        signals = (signal.SIGINT, signal.SIGTERM)
        previous_handlers = {sig: signal.signal(sig, stop) for sig in signals}
        try:
            server.timeout = 0.5
            next_sweep = monotonic() + 5
            print(f"Coordinator listening on 127.0.0.1:{server.server_port}", file=sys.stderr, flush=True)
            while not stopping:
                if scheduler is not None:
                    scheduler.tick(synthetic_handoff=True)
                server.handle_request()
                if monotonic() >= next_sweep:
                    store.sweep(worker_ids=workers)
                    next_sweep = monotonic() + 5
        finally:
            for sig, handler in previous_handlers.items():
                signal.signal(sig, handler)
