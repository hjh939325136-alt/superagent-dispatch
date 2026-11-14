"""Invoke a configured component using one generated JSON request file."""

import json
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .config import Config


_STDOUT_LIMIT = 8 * 1024 * 1024
_SAFE_ENV = {"SYSTEMROOT", "WINDIR", "TEMP", "TMP", "TMPDIR"}
_SYNTHETIC_ENV = {"FAKE_FAIL", "FAKE_FAIL_OPERATION", "FAKE_TIMEOUT_SECONDS"}
_EXECUTION_FIELDS = {
    "argv", "args", "cmd", "command", "command_line", "shell", "executable", "env",
    "environment", "cwd", "workdir", "path", "paths", "data_dir", "request_file",
}


class ComponentError(RuntimeError):
    def __init__(
        self, code: str, message: str, *, stderr_tail: str = "", exit: int | None = None
    ):
        super().__init__(message)
        self.code = code
        self.stderr_tail = stderr_tail
        self.exit = exit


@dataclass(frozen=True)
class RunResult:
    exit: int
    json: dict
    stderr_tail: str


def _check_request(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ComponentError("E_COMPONENT_REQUEST", "request keys must be strings")
            normalized = key.casefold().replace("-", "_")
            if normalized in _EXECUTION_FIELDS:
                raise ComponentError("E_COMPONENT_REQUEST", "request contains a prohibited execution field")
            _check_request(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _check_request(item)


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-JSON constant: {value}")


def _stderr_summary(stream) -> str:
    byte_count = stream.tell()
    # Free-form component stderr can contain request data, paths, or secrets.
    return f"component stderr suppressed ({byte_count} bytes)" if byte_count else ""


class ComponentRunner:
    def __init__(self, config: Config):
        self.config = config

    def run_component(self, name: str, request: dict) -> RunResult:
        component = self.config.components.get(name)
        if component is None:
            raise ComponentError("E_COMPONENT_UNKNOWN", "component is not configured")
        if not isinstance(request, dict):
            raise ComponentError("E_COMPONENT_REQUEST", "request must be a JSON object")
        try:
            _check_request(request)
            payload = json.dumps(request, ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (ValueError, TypeError, RecursionError):
            raise ComponentError("E_COMPONENT_REQUEST", "request must contain valid JSON") from None

        allowed = _SAFE_ENV | _SYNTHETIC_ENV if self.config.synthetic else _SAFE_ENV
        environment = {key: value for key, value in os.environ.items() if key in allowed}
        environment["PYTHONUTF8"] = "1"
        try:
            with (
                tempfile.TemporaryDirectory(prefix="sa-dispatch-") as directory,
                tempfile.TemporaryFile() as stdout,
                tempfile.TemporaryFile() as stderr,
            ):
                request_path = Path(directory).resolve() / "request.json"
                request_path.write_bytes(payload)
                argv = [*component.argv, "invoke", "--request", str(request_path), "--json"]
                try:
                    completed = subprocess.run(
                        argv,
                        shell=False,
                        timeout=self.config.component_timeout_seconds,
                        cwd=component.cwd,
                        env=environment,
                        stdin=subprocess.DEVNULL,
                        stdout=stdout,
                        stderr=stderr,
                    )
                except subprocess.TimeoutExpired:
                    raise ComponentError(
                        "E_COMPONENT_TIMEOUT", "component exceeded its execution timeout",
                        stderr_tail=_stderr_summary(stderr),
                    ) from None

                diagnostic = _stderr_summary(stderr) if completed.returncode else ""
                if stdout.tell() > _STDOUT_LIMIT:
                    raise ComponentError(
                        "E_COMPONENT_BAD_JSON", "component JSON exceeds the output limit",
                        stderr_tail=diagnostic or _stderr_summary(stderr), exit=completed.returncode,
                    )
                stdout.seek(0)
                try:
                    result = json.loads(stdout.read().decode("utf-8"), parse_constant=_reject_constant)
                except (ValueError, RecursionError):
                    result = None
                if not isinstance(result, dict):
                    raise ComponentError(
                        "E_COMPONENT_BAD_JSON", "component stdout must contain one UTF-8 JSON object",
                        stderr_tail=diagnostic or _stderr_summary(stderr), exit=completed.returncode,
                    )
                return RunResult(completed.returncode, result, diagnostic)
        except OSError:
            raise ComponentError("E_COMPONENT_EXEC", "unable to run configured component") from None
