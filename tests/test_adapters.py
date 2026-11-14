"""Verify a project can attach its own task kind and configured component."""

from pathlib import Path
import sys
import tempfile
import unittest

from sa_dispatch.adapters import task_registry
from sa_dispatch.config import ComponentConfig, Config
from sa_dispatch.executor import Executor
from sa_dispatch.models import TaskSpec
from sa_dispatch.runner import ComponentRunner
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch_hotspot import HotspotKind


class ProjectKind(HotspotKind):
    component = "my-project"


class AdapterTest(unittest.TestCase):
    def test_project_adapter_executes_its_component(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = Path(__file__).resolve().parent / "fakes" / "fake_component.py"
            kind = ProjectKind("topic_fetch", "none")
            plugins = {"my-project": {kind.name: kind}}
            kinds = task_registry(["my-project"], plugins)
            config = Config(root / "db", root / "artifacts", root / "journal", "worker", ("project:read",),
                            60, 10, ("my-project",), {"my-project": ComponentConfig(
                                (sys.executable, str(fixture), "--data-dir", str(root / "fixture")), "any")}, True)
            store = SqliteStore(config.db_path, lease_ttl_seconds=config.lease_ttl_seconds, kinds=kinds)
            store.register_device("worker", config.capabilities)
            task, _ = store.submit(TaskSpec("custom-read", kind.name, 1, {"op": "topic.list", "filters": {"limit": 1}},
                                            (), config.capabilities, "synthetic", "none", 2))
            finished = Executor(config, store, ComponentRunner(config), kinds).run_once()
            self.assertEqual((finished.task_id, finished.status), (task.task_id, "succeeded"))
            self.assertEqual(finished.result["count"], 1)
