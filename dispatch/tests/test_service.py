"""Check the public foreground CLI across a restart with isolated fixture data."""

from hashlib import sha256
import json
from pathlib import Path
from secrets import token_urlsafe
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]


class CoordinatorServiceTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.config = self.directory / "dispatch.toml"
        self.identities = self.directory / "identities.json"
        self.tokens = {}
        devices = {}
        for role in ("dispatcher", "worker"):
            token = token_urlsafe(32)
            path = self.directory / role
            path.write_text(token, encoding="ascii")
            self.tokens[role] = path
            devices[role] = {"token_sha256": sha256(token.encode()).hexdigest(), "roles": [role]}
        devices["worker"]["capabilities"] = ["component:mgmtcore"]
        self.identities.write_text(json.dumps({"devices": devices}), encoding="utf-8")

    def command(self, synthetic=True):
        paths = {"db_path": "ledger.sqlite3", "artifact_root": "artifacts", "journal_dir": "journal"}
        config = [f'{key} = {json.dumps((self.directory / name).as_posix())}' for key, name in paths.items()]
        config.extend(['device_id = "coordinator"', 'capabilities = []', 'plugins = ["hotspot"]',
                       'lease_ttl_seconds = 60', 'component_timeout_seconds = 10',
                       f'synthetic = {str(synthetic).lower()}', '[components]'])
        self.config.write_text("\n".join(config), encoding="utf-8")
        return [sys.executable, "-B", "-m", "sa_dispatch", "--config", str(self.config), "serve",
                "--identities", str(self.identities), "--port", "0"]

    def start(self):
        startup_log = self.enterContext(tempfile.NamedTemporaryFile(dir=self.directory, delete=False))
        process = subprocess.Popen(self.command(), cwd=ROOT, stdout=startup_log, stderr=subprocess.STDOUT)
        self.addCleanup(self.stop, process)
        startup_log.close()
        prefix = "Coordinator listening on 127.0.0.1:"
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and process.poll() is None:
            output = Path(startup_log.name).read_text(encoding="utf-8", errors="replace")
            for line in output.splitlines():
                if line.startswith(prefix) and line[len(prefix):].isdigit():
                    return process, line[len(prefix):]
            time.sleep(0.05)
        output = Path(startup_log.name).read_text(encoding="utf-8", errors="replace")
        self.fail(f"Coordinator did not become ready (exit {process.poll()}): {output[-2000:]}")

    @staticmethod
    def stop(process):
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=10)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)

    def smoke(self, port, action, role, *args):
        return subprocess.run([sys.executable, "-B", "deploy/smoke-coordinator.py", "--port", port, action,
                               f"--{'dispatcher' if action == 'submit' else 'worker'}-token-file",
                               str(self.tokens[role]), "--key", "test-restart", *args],
                              cwd=ROOT, capture_output=True, text=True, timeout=15)

    def test_roles_restart_and_idempotency(self):
        first, port = self.start()
        denied = self.smoke(port, "submit", "worker")
        self.assertNotEqual(denied.returncode, 0)
        self.assertIn("HTTP 403", denied.stderr)
        submitted = self.smoke(port, "submit", "dispatcher")
        self.assertEqual(submitted.returncode, 0, submitted.stderr)
        task = json.loads(submitted.stdout)
        self.assertEqual(task["status"], "queued")
        self.stop(first)

        _, port = self.start()
        finished = self.smoke(port, "complete", "worker", "--task-id", task["task_id"])
        self.assertEqual(finished.returncode, 0, finished.stderr)
        receipt = json.loads(finished.stdout)
        self.assertEqual(receipt["attempts"], 1)
        self.assertRegex(receipt["receipt"], r"^sha256:[0-9a-f]{64}$")
        replay = self.smoke(port, "submit", "dispatcher")
        self.assertEqual(replay.returncode, 0, replay.stderr)
        self.assertEqual(json.loads(replay.stdout), {"task_id": task["task_id"], "status": "succeeded",
                                                     "deduplicated": True})

    def test_refuses_real_config_before_database_creation(self):
        denied = subprocess.run(self.command(synthetic=False), cwd=ROOT, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(denied.returncode, 0)
        self.assertIn("dedicated synthetic configuration", denied.stdout)
        self.assertFalse((self.directory / "ledger.sqlite3").exists())
