"""Actual isolated writes and implementation-bound capability proof regressions."""
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.component_proof import (ComponentProofError, FAKE, FIXTURE,
    component_fingerprint, fingerprint_details, prove_idempotency)
from sa_dispatch.config import ComponentConfig, Config
from sa_dispatch.models import DispatchError
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch_hotspot import KINDS


class ComponentProofTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="component proof ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = Config(self.root / "ledger.sqlite3", self.root / "artifacts", self.root / "journal",
                             "fixture", ("component:mgmtcore",), 30, 10, ("hotspot",),
                             {"mgmtcore": ComponentConfig((sys.executable, str(FAKE.resolve()),
                               "--data-dir", str(self.root / "configured-data")), "any")}, True)

    def with_command(self, argv, cwd=None):
        return replace(self.config, components={"mgmtcore": ComponentConfig(tuple(argv), "any", cwd)})

    def test_fingerprint_changes_with_argv_script_bytes_and_cwd(self):
        script = self.root / "entry.py"
        script.write_text("print('first')\n", encoding="utf-8")
        original = self.with_command((sys.executable, str(script)))
        one = component_fingerprint(original, "mgmtcore")
        self.assertEqual(one, component_fingerprint(original, "mgmtcore"))
        flags = self.with_command((sys.executable, "-B", str(script)))
        self.assertNotEqual(one, component_fingerprint(flags, "mgmtcore"))
        script.write_text("print('second')\n", encoding="utf-8")
        self.assertNotEqual(one, component_fingerprint(original, "mgmtcore"))
        self.assertNotEqual(component_fingerprint(original, "mgmtcore"),
                            component_fingerprint(self.with_command(original.components["mgmtcore"].argv, self.root), "mgmtcore"))

    def test_module_entry_hash_is_read_without_importing_code(self):
        package = self.root / "proof_package"
        package.mkdir()
        (package / "__init__.py").write_text("raise RuntimeError('must not import')\n", encoding="utf-8")
        main = package / "__main__.py"
        main.write_text("raise RuntimeError('must not execute')\n", encoding="utf-8")
        config = self.with_command((sys.executable, "-m", "proof_package"), self.root)
        before = component_fingerprint(config, "mgmtcore")
        main.write_text("raise RuntimeError('changed')\n", encoding="utf-8")
        self.assertNotEqual(before, component_fingerprint(config, "mgmtcore"))
        self.assertEqual(len(fingerprint_details(config, "mgmtcore")["files"]), 3)
        with self.assertRaises(ComponentProofError):
            component_fingerprint(self.with_command((sys.executable, "-m", "not_resolvable"), self.root), "mgmtcore")

    def test_manifest_does_not_export_argv_secrets_or_hash_unrelated_data(self):
        secret = "synthetic-secret-must-not-be-in-manifest"
        config = self.with_command((sys.executable, str(FAKE.resolve()), "--opaque", secret))
        before = fingerprint_details(config, "mgmtcore")
        self.assertNotIn(secret, json.dumps(before))
        model = self.root / "unrelated-model.bin"
        model.write_bytes(b"not-code")
        self.assertEqual(before, fingerprint_details(config, "mgmtcore"))
        model.write_bytes(b"changed-not-code")
        self.assertEqual(before, fingerprint_details(config, "mgmtcore"))

    def test_two_real_calls_generate_readable_proof_for_both_write_kinds(self):
        data = self.root / "configured-data"
        data.mkdir()
        sentinel = data / "untouched.txt"
        sentinel.write_text("not part of this lab", encoding="utf-8")
        for name in ("hotspot.topic_adopt", "hotspot.context_prepare"):
            result = prove_idempotency(self.config, "mgmtcore", KINDS[name])
            artifacts = ArtifactStore(self.config.artifact_root)
            proof = json.loads(artifacts.get(result["proof_ref"]))
            self.assertEqual(proof["component_fingerprint"], component_fingerprint(self.config, "mgmtcore"))
            self.assertEqual((proof["calls"], proof["records_after_first"], proof["records_after_second"]), (2, 1, 1))
            self.assertEqual(proof["first_result_hash"], proof["second_result_hash"])
            self.assertEqual(artifacts.get(proof["state_after_first_ref"]), artifacts.get(proof["state_after_second_ref"]))
            self.assertEqual(proof["generated_by"], "sa_dispatch.prove-idempotency.v1")
            self.assertEqual(result["registration_status"], "not_registered")
            self.assertTrue(proof["isolation"]["temporary_directory_removed"])
        self.assertEqual(list(data.iterdir()), [sentinel])
        self.assertEqual(sentinel.read_text(), "not part of this lab")

    def test_real_component_external_fixture_and_wrong_kind_are_not_invoked(self):
        other = self.root / "fake_component.py"
        other.write_text("raise RuntimeError('must not run')\n", encoding="utf-8")
        configs = [replace(self.config, synthetic=False), self.with_command((sys.executable, str(other), "--data-dir", str(self.root / "danger"))),
                   self.with_command((*self.config.components["mgmtcore"].argv, "--fixture", str(other)))]
        with patch("sa_dispatch.component_proof.ComponentRunner.run_component", side_effect=AssertionError("must not run")):
            for config in configs:
                with self.subTest(config=config), self.assertRaises(ComponentProofError):
                    prove_idempotency(config, "mgmtcore", KINDS["hotspot.topic_adopt"])
            with self.assertRaises(ComponentProofError):
                prove_idempotency(self.config, "mgmtcore", KINDS["hotspot.topic_fetch"])
        self.assertFalse((self.root / "danger").exists())

    def test_subprocess_failure_does_not_generate_verified_artifact(self):
        with patch.dict(os.environ, {"FAKE_FAIL": "exit5"}):
            with self.assertRaises(ComponentProofError):
                prove_idempotency(self.config, "mgmtcore", KINDS["hotspot.topic_adopt"])
        self.assertFalse(self.config.artifact_root.exists())
        self.assertFalse((self.root / "configured-data").exists())

    def test_current_config_required_and_drifted_proof_cannot_register(self):
        result = prove_idempotency(self.config, "mgmtcore", KINDS["hotspot.topic_adopt"])
        store = SqliteStore(self.config.db_path, lease_ttl_seconds=30, kinds=KINDS, component_config=self.config)
        artifacts = ArtifactStore(self.config.artifact_root)
        store.register_capability("mgmtcore", "hotspot.topic_adopt@1", idempotency_proof="verified",
                                  proof_ref=result["proof_ref"], artifacts=artifacts)
        altered = self.with_command((sys.executable, "-B", *self.config.components["mgmtcore"].argv[1:]))
        reopened = SqliteStore(self.config.db_path, lease_ttl_seconds=30, kinds=KINDS, component_config=altered)
        with self.assertRaises(DispatchError):
            reopened.register_capability("mgmtcore", "hotspot.topic_adopt@1", idempotency_proof="verified",
                                          proof_ref=result["proof_ref"], artifacts=artifacts)

    def test_cli_generates_and_registers_operator_proof(self):
        def quoted(value):
            return json.dumps(str(value))
        path = self.root / "config.toml"
        path.write_text("\n".join((
            f"db_path = {quoted(self.config.db_path)}", f"artifact_root = {quoted(self.config.artifact_root)}",
            f"journal_dir = {quoted(self.config.journal_dir)}", 'device_id = "fixture"',
            'capabilities = ["component:mgmtcore"]', 'lease_ttl_seconds = 30', 'component_timeout_seconds = 10',
            'plugins = ["hotspot"]', 'synthetic = true', '[components.mgmtcore]', 'platform = "any"',
            'argv = [' + ', '.join(quoted(value) for value in self.config.components["mgmtcore"].argv) + ']')) + "\n", encoding="utf-8")
        run = subprocess.run([sys.executable, "-B", "-m", "sa_dispatch", "--config", str(path), "prove-idempotency",
                              "--component", "mgmtcore", "--kind", "hotspot.topic_adopt"], cwd=FAKE.parents[2], capture_output=True)
        self.assertEqual(run.returncode, 0, run.stdout.decode() + run.stderr.decode())
        output = json.loads(run.stdout)
        self.assertEqual(output["data"]["registration_status"], "operator_registered")
        self.assertEqual(output["data"]["proof"]["component_fingerprint"], component_fingerprint(self.config, "mgmtcore"))
        self.assertFalse((self.root / "configured-data").exists())


if __name__ == "__main__":
    unittest.main()
