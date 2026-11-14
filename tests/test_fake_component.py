import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4


SCRIPT = Path(__file__).parent / "fakes" / "fake_component.py"
PROJECT = "synthetic-project"


class FakeComponentTest(unittest.TestCase):
    def setUp(self):
        working = tempfile.TemporaryDirectory()
        self.addCleanup(working.cleanup)
        self.root = Path(working.name)
        self.state = self.root / "state"

    def invoke(self, operation, payload, *, key=None, fault=None, timeout=5, project=None):
        request_id = uuid4().hex
        request = self.root / f"{request_id}.json"
        request.write_text(json.dumps({
            "schema_version": "0.1", "request_id": request_id, "operation": operation,
            "project_id": project, "task_id": None, "idempotency_key": key,
            "input_refs": [], "payload": payload, "authorization": {"mode": "local_only"},
        }), encoding="utf-8")
        env = {name: value for name, value in os.environ.items() if not name.startswith("FAKE_")}
        if fault:
            env["FAKE_FAIL"] = fault
            env["FAKE_FAIL_OPERATION"] = operation
        response = subprocess.run(
            [sys.executable, str(SCRIPT), "--data-dir", str(self.state),
             "invoke", "--request", str(request), "--json"],
            env=env, capture_output=True, encoding="utf-8", timeout=timeout,
        )
        return response, request_id

    def data(self, operation, payload, **kwargs):
        response, request_id = self.invoke(operation, payload, **kwargs)
        self.assertEqual(response.returncode, 0, response.stderr or response.stdout)
        result = json.loads(response.stdout)
        self.assertEqual(result["schema_version"], "0.1")
        self.assertEqual(result["request_id"], request_id)
        self.assertIs(result["ok"], True)
        self.assertIsNone(result["error"])
        return result["data"]

    def topic(self):
        result = self.data("component.query", {"component": "signal_hub", "component_operation": "topic.list",
                                              "input": {"track": "ai-practice"}})
        self.assertIs(result["persisted"], False)
        self.assertIs(result["upstream"]["ok"], True)
        return result["upstream"]["data"]["items"][0]

    def adoption(self):
        item = self.topic()
        return {"kind": "topic", "title": item["title"], "body": item["summary"],
                "author": "fixture", "source_type": "imported", "evidence_refs": [item["ref"]]}

    def test_topic_list_get_empty_and_stale(self):
        ref = self.topic()["ref"]
        payload = {"component": "signal_hub", "component_operation": "topic.get",
                   "input": {"id": ref["id"], "version": ref["version"]}}
        self.assertEqual(self.data("component.query", payload)["upstream"]["data"]["ref"], ref)
        payload["input"]["version"] = "outdated"
        upstream = self.data("component.query", payload)["upstream"]
        self.assertIs(upstream["ok"], False)
        self.assertEqual(upstream["error"]["code"], "STALE_REFERENCE")
        self.assertEqual(self.data("topic.list", {"track": "game"})["items"], [])

    def test_concurrent_write_replays_once_and_conflicts(self):
        payload = self.adoption()
        with ThreadPoolExecutor(max_workers=2) as pool:
            first, duplicate = pool.map(
                lambda _: self.data("content.upsert", payload, key="same", project=PROJECT), range(2)
            )
        self.assertEqual(first, duplicate)
        self.assertEqual(self.data("content.list", {}, project=PROJECT)["contents"], [first["content"]])
        ref = first["content"]["ref"]
        self.assertEqual(self.data("content.get", {"item_id": ref["id"], "version": 1}), first)
        changed = {**payload, "title": "Another synthetic title"}
        conflict, _ = self.invoke("content.upsert", changed, key="same", project=PROJECT)
        self.assertEqual(conflict.returncode, 2)
        self.assertEqual(json.loads(conflict.stdout)["error"]["code"], "IDEMPOTENCY_CONFLICT")

    def test_recipe_and_retained_context(self):
        source = self.topic()
        payload = {"title": source["title"], "brief": source["summary"], "source_refs": [source["ref"]]}
        recipe = self.data("recipe.start", payload, key="recipe", project=PROJECT)
        self.assertEqual(recipe["status"], "needs_agent")
        self.assertEqual(recipe["steps"]["creative_context"]["output"], recipe["agent_handoff"]["creative_context"])
        self.assertEqual(recipe, self.data("recipe.get", {"recipe_id": recipe["recipe_id"]}, project=PROJECT))
        self.assertEqual(recipe, self.data("recipe.start", payload, key="recipe", project=PROJECT))
        self.assertEqual(self.data("recipe.list", {}, project=PROJECT)["recipes"], [recipe])

    def test_faults_before_commit(self):
        payload = self.adoption()
        with self.assertRaises(subprocess.TimeoutExpired):
            self.invoke("content.upsert", payload, key="timeout", fault="timeout", timeout=0.5, project=PROJECT)
        bad, _ = self.invoke("content.upsert", payload, key="malformed", fault="bad_json", project=PROJECT)
        self.assertEqual(bad.returncode, 0)
        with self.assertRaises(json.JSONDecodeError):
            json.loads(bad.stdout)
        failed, _ = self.invoke("content.upsert", payload, key="failure", fault="exit5", project=PROJECT)
        self.assertEqual(failed.returncode, 5)
        self.assertEqual(json.loads(failed.stdout)["error"]["code"], "EXECUTION_FAILED")
        self.assertEqual(self.data("content.list", {}, project=PROJECT)["contents"], [])

    def test_crash_after_commit_can_be_queried_and_deduplicated(self):
        payload = self.adoption()
        lookup = {"operation": "content.upsert", "idempotency_key": "crash"}
        self.assertEqual(self.data("fake.write.get", lookup), {"found": False, "result": None})
        crashed, _ = self.invoke("content.upsert", payload, key="crash", fault="crash_after_write", project=PROJECT)
        self.assertEqual(crashed.returncode, 5)
        self.assertEqual(crashed.stdout, "")
        written = self.data("fake.write.get", lookup)
        self.assertIs(written["found"], True)
        self.assertEqual(written["result"], self.data("content.upsert", payload, key="crash", project=PROJECT))
        self.assertEqual(self.data("content.list", {}, project=PROJECT)["contents"], [written["result"]["content"]])
