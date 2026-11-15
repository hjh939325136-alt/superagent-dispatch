"""The worker process creates its own component result and returns it to the coordinator."""

from hashlib import sha256
import json
from pathlib import Path
from secrets import token_urlsafe
import subprocess
import sys
import tempfile
from threading import Thread
import unittest

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.transport import CoordinatorAPI, create_server
from sa_dispatch_hotspot import KINDS


CLIENT = r"""
from base64 import b64decode, b64encode
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.request import Request, urlopen
from sa_dispatch.config import ComponentConfig, Config
from sa_dispatch.runner import ComponentRunner
from sa_dispatch_hotspot import KINDS
import sys

config_input = json.load(sys.stdin)
base, submit_token, worker_token, root, fixture = (
    config_input[k] for k in ("base", "submit", "worker", "root", "fixture")
)
root = Path(root)
def post(route, data, token=worker_token):
    request = Request(base+route, json.dumps(data).encode(), {
        "Authorization": "Bearer " + token, "Content-Type": "application/json"})
    with urlopen(request, timeout=5) as response:
        payload = json.load(response)
    return payload.get("data", payload)

source = b'{"fixture":"consumed by the worker"}'
digest = "sha256:" + hashlib.sha256(source).hexdigest()
spec = {
    "idem_key": "test-proc-a", "kind": "hotspot.topic_fetch", "kind_version": 1,
    "params": {"op": "topic.list", "filters": {"track": "ai-practice"}},
    "input_refs": [{"owner":"fixture","kind":"source","id":"x","version":"1","hash":digest}],
    "required_caps": ["component:mgmtcore"], "access": "synthetic", "egress": "none", "max_attempts": 2,
}
submitted = post("/submit", {"spec": spec, "inputs": {digest: b64encode(source).decode()}}, submit_token)
lease = post("/claim", {})["lease"]
binding = {k: lease[k] for k in ("lease_id", "fence")}
assigned = post("/task",binding)
assert assigned["spec"]["params"] == spec["params"]
assert b64decode(post("/artifact/get", {**binding, "hash": digest})["content_base64"]) == source
config = Config(root/"unused.sqlite3",root/"artifacts",root/"journal","worker",
                ("component:mgmtcore",),60,3,("hotspot",),
                {"mgmtcore":ComponentConfig((sys.executable, fixture, "--data-dir", str(root/"fake")), "any")},
                True)
task = SimpleNamespace(task_id=lease["task_id"], spec=SimpleNamespace(**assigned["spec"]))
kind = KINDS[spec["kind"]]
reply = ComponentRunner(config).run_component("mgmtcore", kind.request(task))
assert reply.exit == 0
result = kind.result(reply.json, task)
data = json.dumps(reply.json, ensure_ascii=False, sort_keys=True).encode()
result_digest = "sha256:" + hashlib.sha256(data).hexdigest()
post("/artifact/put", {**binding, "hash":result_digest, "content_base64":b64encode(data).decode()})
post("/complete", {**binding,"outcome":{"status":"succeeded","result":{**result,"receipt":result_digest},
    "evidence":[{"kind":"component_response","content_hash":result_digest,"artifact_hash":result_digest,
                 "access":"synthetic"}]}})
assert post("/status", {"task_id":lease["task_id"]})["status"] == "succeeded"
print(json.dumps({"task_id":lease["task_id"],"artifact":result_digest}))
"""


class LoopbackProcessTest(unittest.TestCase):
    def test_synthetic_handoff_from_a_separate_process(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = SqliteStore(root / "dispatch.sqlite3", lease_ttl_seconds=60, kinds=KINDS)
            store.register_device("worker", ["component:mgmtcore"])
            tokens = {key: token_urlsafe(32) for key in ("dispatcher", "worker")}
            artifacts = ArtifactStore(root / "coordinator-artifacts")
            api = CoordinatorAPI(store, {key: sha256(token.encode()).hexdigest() for key, token in tokens.items()},
                                 synthetic_handoff=True, roles={"dispatcher": {"dispatcher"}, "worker": {"worker"}},
                                 artifact_store=artifacts)
            server = create_server(api)
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(thread.join, 3)
            try:
                payload = {"base": "http://127.0.0.1:" + str(server.server_address[1]), "submit": tokens["dispatcher"],
                           "worker": tokens["worker"], "root": str(root / "different-worker-dir"),
                           "fixture": str(Path(__file__).resolve().parent / "fakes/fake_component.py")}
                child = subprocess.run([sys.executable, "-B", "-c", CLIENT], input=json.dumps(payload),
                                       encoding="utf-8", capture_output=True, timeout=20)
                self.assertEqual(child.returncode, 0, child.stderr)
                report = json.loads(child.stdout)
                final = store.get_task(report["task_id"])
                self.assertEqual(final.status, "succeeded")
                self.assertEqual(final.attempts, 1)
                self.assertEqual(final.result["count"], 1)
                self.assertTrue(json.loads(artifacts.get(report["artifact"]))["ok"])
            finally:
                server.shutdown()
                server.server_close()
