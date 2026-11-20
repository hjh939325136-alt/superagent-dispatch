"""Prove a disposable developer queue and a worker credential rotation."""

from contextlib import contextmanager
from hashlib import sha256
import json
import os
from pathlib import Path
import platform
import re
import secrets
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import monotonic, sleep


DEPLOY = Path(__file__).resolve().parent
ROOT = DEPLOY.parent
TOKEN_ENV = "SA_DISPATCH_DEV_TOKEN"
READY = re.compile(r"^Coordinator listening on 127\.0\.0\.1:(\d+)$", re.MULTILINE)
SCOPE = {"dispatcher": [], "worker": ["component:mgmtcore"]}


def fresh_tokens():
    return {role: secrets.token_urlsafe(32) for role in SCOPE}


def child_environment():
    # Do not forward the invoking user's provider credentials or project settings.
    allowed = {"SYSTEMROOT", "WINDIR", "TEMP", "TMP", "TMPDIR", "LANG", "LC_ALL"}
    return {key: value for key, value in os.environ.items() if key.upper() in allowed}


def prepare(directory, tokens):
    config = directory / "dev.toml"
    text = (DEPLOY / "coordinator.example.toml").read_text(encoding="utf-8")
    fields = {
        "db_path": directory / "dev.sqlite3",
        "artifact_root": directory / "artifacts",
        "journal_dir": directory / "journal",
    }
    for field, path in fields.items():
        entry = field + " = " + json.dumps(path.as_posix())
        text, count = re.subn(r"^" + field + r" = [^\n]+", lambda _: entry, text, flags=re.MULTILINE)
        require(count == 1, f"unexpected configuration field {field}")
    config.write_text(text, encoding="utf-8")
    identities = directory / "identities.json"
    devices = {
        f"dev-{role}": {
            "token_sha256": sha256(tokens[role].encode("ascii")).hexdigest(),
            "roles": [role],
            "capabilities": capabilities,
        }
        for role, capabilities in SCOPE.items()
    }
    identities.write_text(json.dumps({"devices": devices}), encoding="utf-8")
    return config, identities


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


@contextmanager
def coordinator(directory, tokens):
    config, identities = prepare(directory, tokens)
    log = directory / "coordinator.log"
    with log.open("wb") as diagnostic:
        process = subprocess.Popen(
            [sys.executable, "-B", "-m", "sa_dispatch", "--config", str(config),
             "serve", "--identities", str(identities), "--port", "0"],
            cwd=ROOT, env=child_environment(), stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=diagnostic,
        )
        try:
            deadline = monotonic() + 12
            while monotonic() < deadline:
                require(process.poll() is None, "the development coordinator exited during startup")
                match = READY.search(log.read_text(encoding="utf-8", errors="replace"))
                if match:
                    yield int(match[1])
                    return
                sleep(0.05)
            raise RuntimeError("the development coordinator did not report its loopback port")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def smoke(port, token, action, key, *, task_id=None, denied=None):
    role = "dispatcher" if action == "submit" else "worker"
    args = [sys.executable, "-B", str(DEPLOY / "smoke-coordinator.py"), "--port", str(port),
            action, f"--{role}-token-env", "--key", key]
    if task_id:
        args.extend(["--task-id", task_id])
    result = subprocess.run(args, cwd=ROOT, env=dict(child_environment(), **{TOKEN_ENV: token}),
                            capture_output=True, encoding="utf-8", timeout=15)
    if denied:
        require(result.returncode == 1 and denied in result.stderr,
                f"{action} did not return the expected denial ({denied})")
        return
    require(result.returncode == 0, f"{action} failed in the synthetic environment")
    return json.loads(result.stdout)


def complete(port, token, key, task_id):
    receipt = smoke(port, token, "complete", key, task_id=task_id)
    require(receipt["task_id"] == task_id and receipt["status"] == "succeeded"
            and receipt["attempts"] == 1, "expected exactly one verified completion")
    return receipt


def trial():
    key = "development-trial-v1"
    recovery_key = "development-trial-after-rotation"
    tokens = fresh_tokens()
    rotated = dict(tokens, worker=secrets.token_urlsafe(32))
    with TemporaryDirectory(prefix="sa-dispatch-dev-") as directory:
        root = Path(directory).resolve()
        with coordinator(root, tokens) as port:
            smoke(port, tokens["worker"], "submit", key, denied="/submit returned HTTP 403")
            first = smoke(port, tokens["dispatcher"], "submit", key)
            smoke(port, tokens["dispatcher"], "complete", key, task_id=first["task_id"],
                  denied="/claim returned HTTP 403")
            initial = complete(port, tokens["worker"], key, first["task_id"])
            queued = smoke(port, tokens["dispatcher"], "submit", recovery_key)

        with coordinator(root, rotated) as port:
            smoke(port, tokens["worker"], "complete", recovery_key, task_id=queued["task_id"],
                  denied="/health returned HTTP 401")
            repeated = smoke(port, tokens["dispatcher"], "submit", recovery_key)
            require(repeated["task_id"] == queued["task_id"], "restart changed the queued task id")
            resumed = complete(port, rotated["worker"], recovery_key, queued["task_id"])

    with TemporaryDirectory(prefix="sa-dispatch-dev-rebuild-") as directory:
        rebuilt_tokens = fresh_tokens()
        with coordinator(Path(directory).resolve(), rebuilt_tokens) as port:
            clean = smoke(port, rebuilt_tokens["dispatcher"], "submit", key)
            require(clean["task_id"] != initial["task_id"], "clean rebuild reused a previous task id")
            smoke(port, rotated["worker"], "complete", key, task_id=clean["task_id"],
                  denied="/health returned HTTP 401")
            rebuilt = complete(port, rebuilt_tokens["worker"], key, clean["task_id"])

    return {
        "schema": 1,
        "trial": "development-credential-rotation",
        "platform": platform.system(),
        "python": platform.python_version(),
        "scope": "temporary synthetic ledgers on IPv4 loopback",
        "admin_permission_required": False,
        "worker_submission_denied": True,
        "dispatcher_claim_denied": True,
        "old_worker_token_denied_after_restart": True,
        "rebuild_rejects_previous_environment_token": True,
        "before_rotation": initial,
        "resumed_after_rotation": resumed,
        "clean_rebuild": rebuilt,
        "temporary_state_removed": True,
    }


if __name__ == "__main__":
    try:
        print(json.dumps(trial(), indent=2))
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        raise SystemExit(f"Development trial failed: {error}") from None
