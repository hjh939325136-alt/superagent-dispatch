"""Run the end-to-end portability acceptance against synthetic component data."""

from dataclasses import replace
import json
from pathlib import Path
import sys
import tempfile

from sa_dispatch_hotspot import KINDS

from .bundle import export_bundle, import_bundle, verify_bundle
from .config import ComponentConfig, Config
from .executor import Executor
from .models import Ref, TaskSpec
from .runner import ComponentRunner
from .store_sqlite import SqliteStore


def run_selftest(bundle_output=None):
    fixture = Path(__file__).resolve().parents[1] / "tests/fakes/fake_component.py"
    with tempfile.TemporaryDirectory(prefix="sa-dispatch-test-") as folder:
        root = Path(folder)
        local = root / "source"
        other = root / "destination"
        local.mkdir()
        other.mkdir()
        config = Config(
            local / "dispatch.sqlite3", local / "artifacts", local / "journal", "fixture-one",
            ("component:mgmtcore",), 60, 10, ("hotspot",),
            {"mgmtcore": ComponentConfig((sys.executable, str(fixture), "--data-dir", str(local / "component")), "any")},
            True,
        )
        store = SqliteStore(config.db_path, lease_ttl_seconds=config.lease_ttl_seconds, kinds=KINDS)
        store.register_device(config.device_id, config.capabilities)
        executor = Executor(config, store, ComponentRunner(config), KINDS)
        specs = []

        def step(name, params, refs=(), parent=None):
            spec = TaskSpec(name, "hotspot." + name, 1, params, refs, config.capabilities, "synthetic", "none", 2,
                            parent_task_id=parent)
            task, reused = store.submit(spec)
            done = executor.run_once()
            if reused or done is None or done.status != "succeeded" or done.task_id != task.task_id:
                raise RuntimeError(f"synthetic step failed: {name}")
            specs.append((spec, done.task_id))
            return done

        found = step("topic_fetch", {"op": "topic.list", "filters": {"limit": 10}})
        receipt = json.loads(executor.artifacts.get(found.result["receipt"]))
        lead = receipt["data"]["upstream"]["data"]["items"][0]
        ref = lead["ref"]
        pin = Ref(ref["owner"], ref["kind"], ref["id"], ref["version"], found.result["receipt"])
        adopted = step("topic_adopt", {"project_id": "synthetic-project", "title": lead["title"],
                       "body": lead["summary"], "author": "synthetic", "evidence_refs": [ref]},
                       (pin,), found.task_id)
        prepared = step("context_prepare", {"project_id": "synthetic-project", "title": lead["title"],
                        "brief": lead["summary"], "lane": "ai_knowledge", "source_refs": [ref]},
                        (pin,), adopted.task_id)
        if prepared.result.get("component_state") != "needs_agent":
            raise RuntimeError("fixture did not reach the creative Agent handoff")
        destination = root / "bundle"
        manifest = export_bundle(config, destination)
        verified = verify_bundle(destination)
        restored = replace(config, db_path=other / "dispatch.sqlite3", artifact_root=other / "artifacts",
                           journal_dir=other / "journal", device_id="fixture-two", components={})
        recovery_store = SqliteStore(restored.db_path, lease_ttl_seconds=60, kinds=KINDS)
        import_bundle(restored, destination, recovery_store)
        recovery_store.register_device(restored.device_id, restored.capabilities)
        for spec, identity in specs:
            row, duplicate = recovery_store.submit(spec)
            if not duplicate or row.task_id != identity or row.status != "succeeded":
                raise RuntimeError("restored task did not replay by its original identity")
        if recovered_hashes(restored, manifest) != manifest["artifacts"] or recovery_store.claim("fixture-two") is not None:
            raise RuntimeError("portable replay did not retain the original receipts")
        store.submit(replace(specs[0][0], idem_key="local-only", access="local_only"))
        try:
            export_bundle(config, root / "blocked")
        except ValueError as exc:
            if getattr(exc, "code", "") != "E_EXPORT_BLOCKED":
                raise
        else:
            raise RuntimeError("restricted task escaped the bundle filter")
        if bundle_output is not None:
            # Publish the verified synthetic bundle only after the full self-test passed.
            from shutil import copytree

            copytree(destination, bundle_output)
        return {**verified, "restored_replay": True, "restricted_export_blocked": True,
                "component_calls_on_replay": 0, "creative_handoff": "needs_agent", "scope": "synthetic"}


def recovered_hashes(config, manifest):
    from .artifacts import ArtifactStore
    from .canonical import sha256_ref

    store = ArtifactStore(config.artifact_root)
    return [sha256_ref(store.get(ref)) for ref in manifest["artifacts"]]
