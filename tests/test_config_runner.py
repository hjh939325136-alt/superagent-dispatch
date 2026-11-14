import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sa_dispatch.config import ConfigError, load_config
from sa_dispatch.runner import ComponentError, ComponentRunner


class ConfigRunnerTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="dispatch config ")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.config_file = self.root / "dispatch.toml"
        self.script = self.root / "fake script.py"
        self.script.write_text(
            """import json, os, sys, time
from pathlib import Path
invoke, flag, filename, output = sys.argv[1:]
assert (invoke, flag, output) == ("invoke", "--request", "--json")
request = json.loads(Path(filename).read_text(encoding="utf-8"))
assert "SECRET_TEST_TOKEN" not in os.environ
if request.get("mode") == "timeout":
    (Path.cwd() / "request-path.txt").write_text(filename, encoding="utf-8")
    time.sleep(10)
elif request.get("mode") == "invalid":
    print("{} {}")
elif request.get("mode") == "error":
    print('{"ok": false}')
    print("private credential", file=sys.stderr)
    sys.exit(5)
else:
    print(json.dumps({"request": request, "path": filename, "cwd": str(Path.cwd()),
                      "failure_mode": os.environ.get("FAKE_FAIL")}))
""",
            encoding="utf-8",
        )

    def write_config(self, *, platform="any", ttl=10, timeout=3, executable=sys.executable,
                     root_extra=""):
        def literal(value):
            return json.dumps(str(value), ensure_ascii=False)

        self.config_file.write_text(
            f"""
db_path = {literal(self.root / 'dispatch.sqlite3')}
artifact_root = {literal(self.root / 'artifacts')}
journal_dir = {literal(self.root / 'journal')}
device_id = "test-device"
capabilities = ["component:test"]
lease_ttl_seconds = {ttl}
component_timeout_seconds = {timeout}
plugins = ["hotspot"]
{root_extra}
[components.test]
argv = [{literal(executable)}, {literal(self.script)}]
platform = {literal(platform)}
cwd = {literal(self.root)}
""",
            encoding="utf-8",
        )
        return load_config(self.config_file)

    def test_config_validates_fields_and_positive_intervals(self):
        for settings in ({"ttl": 0}, {"timeout": 0}, {"root_extra": "extra = true"},
                         {"root_extra": 'synthetic = "true"'}, {"executable": "python"}):
            with self.subTest(settings=settings), self.assertRaises(ConfigError) as error:
                self.write_config(**settings)
            self.assertEqual(error.exception.code, "E_CONFIG")

        config = self.write_config(root_extra="synthetic = true")
        self.assertTrue(config.synthetic)
        self.assertEqual(config.capabilities, ("component:test",))
        self.assertFalse(self.write_config().synthetic)

    def test_mismatching_platform(self):
        with patch("sa_dispatch.config.sys.platform", "linux"):
            with self.assertRaises(ConfigError) as error:
                self.write_config(platform="win32")
        self.assertEqual(error.exception.code, "E_PLATFORM")

    def test_shell_and_batch_entrypoints_are_rejected(self):
        for filename in ("operator.cmd", "operator.BAT", "cmd.exe", "pwsh.exe"):
            with self.subTest(filename=filename), self.assertRaises(ConfigError):
                self.write_config(executable=self.root / filename)

    def test_invocation_uses_generated_file_and_filtered_environment(self):
        runner = ComponentRunner(self.write_config())
        request = {"payload": {"subject": "合成主题"}}
        with patch.dict(os.environ, {"SECRET_TEST_TOKEN": "private test value", "FAKE_FAIL": "exit5"}):
            result = runner.run_component("test", request)
            synthetic = ComponentRunner(self.write_config(root_extra="synthetic = true"))
            self.assertEqual(synthetic.run_component("test", request).json["failure_mode"], "exit5")
        self.assertEqual(result.exit, 0)
        self.assertEqual(result.json["request"], request)
        self.assertEqual(Path(result.json["cwd"]), self.root)
        self.assertFalse(Path(result.json["path"]).exists())
        self.assertIsNone(result.json["failure_mode"])

    def test_execution_fields_rejected_before_launch(self):
        runner = ComponentRunner(self.write_config())
        for request in (
            {"argv": ["other.exe"]},
            {"payload": {"items": [{"EnV": {"SECRET_TEST_TOKEN": "x"}}]}},
            {"payload": {"data-dir": str(self.root)}},
        ):
            with self.subTest(request=request), self.assertRaises(ComponentError) as error:
                runner.run_component("test", request)
            self.assertEqual(error.exception.code, "E_COMPONENT_REQUEST")

    def test_bad_output_and_nonzero_exit(self):
        runner = ComponentRunner(self.write_config())
        with self.assertRaises(ComponentError) as error:
            runner.run_component("test", {"mode": "invalid"})
        self.assertEqual(error.exception.code, "E_COMPONENT_BAD_JSON")
        result = runner.run_component("test", {"mode": "error"})
        self.assertEqual(result.exit, 5)
        self.assertEqual(result.json, {"ok": False})
        self.assertIn("suppressed", result.stderr_tail)
        self.assertNotIn("credential", result.stderr_tail)

    def test_timeout_cleans_generated_file(self):
        runner = ComponentRunner(self.write_config(timeout=2))
        with self.assertRaises(ComponentError) as error:
            runner.run_component("test", {"mode": "timeout"})
        self.assertEqual(error.exception.code, "E_COMPONENT_TIMEOUT")
        filename = (self.root / "request-path.txt").read_text(encoding="utf-8")
        self.assertFalse(Path(filename).exists())
