from contextlib import contextmanager
from dataclasses import asdict
from types import SimpleNamespace
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from sa_dispatch.artifacts import ArtifactError
from sa_dispatch.bundle import export_bundle, verify_bundle
from sa_dispatch.config import ComponentConfig, Config
from sa_dispatch.executor import Executor, receipt_bytes
from sa_dispatch.models import TaskSpec
from sa_dispatch.runner import ComponentRunner
from sa_dispatch.selftest import run_selftest
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch_hotspot import KINDS


class DispatchFlowTests(unittest.TestCase):
    @contextmanager
    def work(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = Path(__file__).resolve().parent / "fakes/fake_component.py"
            config = Config(root / "queue.db", root / "artifacts", root / "journal", "local-test",
                            ("component:mgmtcore",), 60, 2, ("hotspot",),
                            {"mgmtcore": ComponentConfig((sys.executable, str(fake), "--data-dir",
                                                         str(root / "fake")), "any")}, True)
            store = SqliteStore(config.db_path, lease_ttl_seconds=60, kinds=KINDS)
            store.register_device(config.device_id, config.capabilities)
            yield root, config, store, Executor(config, store, ComponentRunner(config), KINDS)

    def submit(self, store, kind, params, key="key"):
        return store.submit(TaskSpec(key, "hotspot." + kind, 1, params, (),
                                     ("component:mgmtcore",), "synthetic", "none", 2))[0]

    def test_bundle_restores_without_provider_or_component(self):
        with patch.dict(os.environ, {"COZE_API_TOKEN": "fixture-never-used"}):
            result = run_selftest()
        self.assertEqual(result["tasks"], 3)
        self.assertEqual(result["component_calls_on_replay"], 0)
        self.assertTrue(result["restricted_export_blocked"])

    def test_corrupted_bundle_rejected(self):
        with self.work() as (root, config, store, executor):
            self.submit(store, "topic_fetch", {"op": "topic.list", "filters": {}})
            self.assertEqual(executor.run_once().status, "succeeded")
            export_bundle(config, root / "bundle")
            (root / "bundle" / "tasks.jsonl").write_bytes(b"corrupted")
            with self.assertRaisesRegex(ArtifactError, "hash mismatch"):
                verify_bundle(root / "bundle")

    def test_crash_after_write_goes_unknown_and_queries_original_receipt(self):
        with self.work() as (_, _, store, executor):
            ref = {"owner": "fixture", "kind": "topic", "id": "source", "version": "1"}
            self.submit(store, "topic_adopt", {"project_id": "synthetic-project",
                        "title": "Synthetic title", "body": "Synthetic body", "author": "Synthetic",
                        "evidence_refs": [ref]})
            with patch.dict(os.environ, {"FAKE_FAIL": "crash_after_write", "FAKE_FAIL_OPERATION": "content.upsert"}):
                wrote = executor.run_once()
            self.assertEqual(wrote.status, "unknown")
            self.assertIsNone(executor.run_once())
            self.assertEqual(executor.reconcile(wrote)["verdict"], "confirmed_done")
            self.assertEqual(store.get_task(wrote.task_id).status, "succeeded")
            self.assertEqual(store.get_task(wrote.task_id).attempts, 1)

    def test_recovery_replays_saved_result_without_component_invocation(self):
        with self.work() as (_, config, store, executor):
            task = self.submit(store, "topic_fetch", {"op": "topic.list", "filters": {}})
            lease = store.claim(config.device_id)
            original = KINDS[task.kind].request(task)
            result = executor.runner.run_component("mgmtcore", original)
            artifact = executor.artifacts.put(receipt_bytes(result.json))
            executor.log(lease, "intent", write_key=None)
            executor.log(lease, "invoked")
            executor.log(lease, "result", artifact=artifact, exit=0)
            with patch.object(executor.runner, "run_component", side_effect=AssertionError("unexpected invocation")):
                executor.recover()
            self.assertEqual(store.get_task(task.task_id).status, "succeeded")
            self.assertEqual(store.get_task(task.task_id).result["receipt"], artifact)

    def test_recovery_skips_other_adapters_before_loading_receipt(self):
        for variant in ('unknown_kind', 'wrong_version', 'disabled', 'dedicated',
                        'foreign_device', 'forged_lease_owner'):
            with self.subTest(variant=variant), self.work() as (_, config, store, executor):
                # A separate trusted submitter can have kinds this executor does not own.
                raw = SqliteStore(config.db_path, lease_ttl_seconds=60)
                name = 'other.adapter' if variant == 'unknown_kind' else 'hotspot.topic_fetch'
                version = 2 if variant == 'wrong_version' else 1
                task, _ = raw.submit(TaskSpec('foreign', name, version, {}, (),
                    config.capabilities, 'synthetic', 'none', 1), side_effect='external')
                owner = 'other-device' if variant in ('foreign_device', 'forged_lease_owner') else config.device_id
                raw.register_device(owner, config.capabilities)
                lease = raw.claim(owner)
                if variant in ('disabled', 'dedicated'):
                    executor.kinds = {name: SimpleNamespace(version=1,
                        enabled=variant != 'disabled', dedicated_runner=variant == 'dedicated')}
                if variant == 'forged_lease_owner':
                    from dataclasses import replace
                    lease = replace(lease, device_id=config.device_id)
                executor.log(lease, 'result', artifact='sha256:' + '0' * 64, exit=0)
                before = raw.snapshot()
                with patch.object(executor.artifacts, 'get', side_effect=AssertionError('foreign receipt read')), \
                     patch.object(executor.runner, 'run_component', side_effect=AssertionError('foreign invocation')):
                    self.assertEqual(executor.recover(), [])
                self.assertEqual(raw.snapshot(), before)

    def test_reconcile_rejects_foreign_owner_and_unregistered_kind(self):
        for foreign_owner in (False, True):
            with self.subTest(foreign_owner=foreign_owner), self.work() as (_, config, store, executor):
                raw = SqliteStore(config.db_path, lease_ttl_seconds=60)
                name = 'hotspot.topic_fetch' if foreign_owner else 'other.adapter'
                task, _ = raw.submit(TaskSpec('unknown', name, 1, {}, (),
                    config.capabilities, 'synthetic', 'none', 1), side_effect='external')
                owner = 'other-device' if foreign_owner else config.device_id
                raw.register_device(owner, config.capabilities)
                lease = raw.claim(owner)
                raw.mark_unknown(lease.lease_id, lease.fence, 'interrupted', owner)
                before = raw.snapshot()
                with patch.object(executor, '_original_request', side_effect=AssertionError('foreign intent read')), \
                     patch.object(executor.runner, 'run_component', side_effect=AssertionError('foreign invocation')):
                    self.assertEqual(executor.reconcile(raw.get_task(task.task_id)), {'verdict': 'still_unknown'})
                self.assertEqual(raw.snapshot(), before)
