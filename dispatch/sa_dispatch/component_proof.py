"""Bind capability evidence to a configured executable and its bounded entrypoint.

Proof generation only executes this repository's synthetic FakeComponent. It is
an operator-registered laboratory check, not a production signature authority.
"""
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import re
import sys
import tempfile
from types import SimpleNamespace
import uuid

from .artifacts import ArtifactStore
from .canonical import canonical_json, sha256_ref
from .config import ComponentConfig
from .models import DispatchError
from .runner import ComponentRunner


ROOT = Path(__file__).resolve().parents[1]
FAKE = ROOT / "tests/fakes/fake_component.py"
FIXTURE = ROOT / "fixtures/synthetic/topic_supply.json"
GENERATOR = "sa_dispatch.prove-idempotency.v1"


class ComponentProofError(DispatchError):
    code = "E_COMPONENT_PROOF"


def _path(path, cwd=None):
    value = Path(path)
    if not value.is_absolute():
        value = (cwd or Path.cwd()) / value
    return value.resolve()


def _normalized(path):
    return os.path.normcase(str(path.resolve()))


def _file(path, role):
    if not path.is_file():
        raise ComponentProofError("Component executable or entrypoint is not a readable regular file")
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return {"role": role, "path": _normalized(path), "sha256": "sha256:" + value.hexdigest()}


def _entrypoints(argv, executable, cwd):
    """Read only direct code files. Never import a module merely to discover it."""
    entries = []
    is_python = executable.name.lower().startswith(("python", "pypy"))
    if is_python:
        index = 1
        while index < len(argv):
            arg = argv[index]
            if arg == "-c":
                if index + 1 >= len(argv):
                    raise ComponentProofError("Python -c has no code argument")
                # Code bytes are already bound by the argv hash, without exporting them.
                return entries
            if arg == "-m":
                if index + 1 >= len(argv) or not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*", argv[index + 1]):
                    raise ComponentProofError("Invalid Python module entrypoint")
                parts = argv[index + 1].split(".")
                module = cwd.joinpath(*parts)
                if module.is_dir():
                    entry = module / "__main__.py"
                else:
                    entry = module.with_suffix(".py")
                if not entry.is_file():
                    raise ComponentProofError("Module entrypoint cannot be resolved under the configured working directory")
                for size in range(1, len(parts) + 1):
                    init = cwd.joinpath(*parts[:size]) / "__init__.py"
                    if init.is_file():
                        entries.append((init.resolve(), "module_init"))
                entries.append((entry.resolve(), "entrypoint"))
                return entries
            if arg in ("-W", "-X"):
                index += 2
                continue
            if arg.startswith("-") and arg != "-":
                index += 1
                continue
            if arg == "-":
                raise ComponentProofError("Stdin code has no stable entrypoint")
            entries.append((_path(arg, cwd), "entrypoint"))
            return entries
        raise ComponentProofError("Interactive Python is not a component entrypoint")
    # Executables are bound even if their arguments contain no script path.
    # Recognize direct script launchers without scanning data/model directories.
    for arg in argv[1:]:
        if not arg.startswith("-") and Path(arg).suffix.lower() in (".py", ".pyw", ".js", ".mjs", ".cjs"):
            entries.append((_path(arg, cwd), "entrypoint"))
            break
    return entries


def fingerprint_details(config, component):
    """Return hashes and paths, never raw argv (which might contain secrets)."""
    configured = config.components.get(component)
    if configured is None or not configured.argv:
        raise ComponentProofError("Component is not configured")
    cwd = _path(configured.cwd) if configured.cwd is not None else Path.cwd().resolve()
    executable = _path(configured.argv[0], cwd)
    argv = list(configured.argv)
    argv[0] = _normalized(executable)
    entries = _entrypoints(argv, executable, cwd)
    # Normalize explicit entrypoint paths, while preserving order and all other argv bytes.
    for path, role in entries:
        if role == "entrypoint":
            for index, value in enumerate(argv[1:], 1):
                if not value.startswith("-") and _path(value, cwd) == path:
                    argv[index] = _normalized(path)
                    break
    files = [_file(executable, "executable")] + [_file(path, role) for path, role in entries]
    manifest = {"schema": "sa_dispatch.component_fingerprint.v1", "component": component,
                "argv_sha256": sha256_ref(canonical_json(argv)), "cwd": _normalized(cwd),
                "platform": configured.platform, "host_platform": sys.platform, "files": files}
    return {**manifest, "component_fingerprint": sha256_ref(canonical_json(manifest))}


def component_fingerprint(config, component):
    return fingerprint_details(config, component)["component_fingerprint"]


def _fixture_config(config, component, isolated_root):
    if not config.synthetic:
        raise ComponentProofError("Automatic capability proof is restricted to synthetic configurations")
    configured = config.components.get(component)
    if configured is None:
        raise ComponentProofError("Component is not configured")
    argv = list(configured.argv)
    cwd = _path(configured.cwd) if configured.cwd else Path.cwd().resolve()
    if not argv or _path(argv[0], cwd) != Path(sys.executable).resolve():
        raise ComponentProofError("Proof only supports the current interpreter and bundled FakeComponent")
    index = 1
    while index < len(argv) and argv[index] in ("-B", "-u", "-I"):
        index += 1
    if index >= len(argv) or _path(argv[index], cwd) != FAKE.resolve():
        raise ComponentProofError("Proof refuses real components and unrecognized fixture entrypoints")
    python_flags = argv[1:index]
    index += 1
    options = {}
    while index < len(argv):
        flag = argv[index]
        if flag not in ("--data-dir", "--fixture") or flag in options or index + 1 >= len(argv):
            raise ComponentProofError("Fixture command contains unsupported or duplicate options")
        options[flag] = argv[index + 1]
        index += 2
    if "--data-dir" not in options:
        raise ComponentProofError("Fixture command must have an explicit isolated-data option")
    if "--fixture" in options and _path(options["--fixture"], cwd) != FIXTURE.resolve():
        raise ComponentProofError("Proof refuses external fixture data")
    # The only substitutions are bounded lab paths and the bundled synthetic fixture.
    isolated = ComponentConfig((str(Path(sys.executable).resolve()), *python_flags, str(FAKE.resolve()),
                                "--data-dir", str(isolated_root / "data"), "--fixture", str(FIXTURE.resolve())),
                               configured.platform, isolated_root)
    return replace(config, components={component: isolated})


def _request(kind):
    from sa_dispatch_hotspot import KINDS
    if kind is not KINDS.get(getattr(kind, "name", None)) or kind.name not in ("hotspot.topic_adopt", "hotspot.context_prepare"):
        raise ComponentProofError("No built-in synthetic proof recipe for this task kind")
    if kind.side_effect != "idempotent_write" or not kind.enabled:
        raise ComponentProofError("Proof requires an enabled idempotent-write task kind")
    reference = {"owner": "fixture", "kind": "topic", "id": "synthetic-proof", "version": "1"}
    if kind.name == "hotspot.topic_adopt":
        params = {"project_id": "synthetic-project", "title": "Synthetic proof", "body": "Synthetic proof only",
                  "author": "synthetic-proof-tool", "evidence_refs": [reference]}
    else:
        params = {"project_id": "synthetic-project", "title": "Synthetic proof", "brief": "Synthetic proof only",
                  "lane": "ai_knowledge", "source_refs": [reference]}
    params = kind.validate(params)
    task = SimpleNamespace(task_id="synthetic-proof-" + uuid.uuid4().hex, spec=SimpleNamespace(params=params))
    return kind.request(task), task


def _state(path, operation, write_key):
    import json
    state = json.loads(path.read_bytes())
    writes = state["writes"].get(operation, {})
    if set(writes) != {write_key}:
        raise ComponentProofError("Fixture did not persist exactly the requested write key")
    if operation == "content.upsert":
        count = sum(len(versions) for versions in state["contents"].values())
    elif operation == "recipe.start":
        count = len(state["recipes"])
    else:
        raise ComponentProofError("No count verifier for this operation")
    if count != 1:
        raise ComponentProofError("Fixture contains duplicate or missing logical records")
    return state, count


def prove_idempotency(config, component, kind, *, artifacts=None):
    """Actually call bundled FakeComponent twice; return a proof, not registration.

    `kind` is the registered kind object. No operator-supplied request or proof
    body is accepted. Caller registers the result against its current Config.
    """
    if getattr(kind, "component", None) != component:
        raise ComponentProofError("Task kind does not target this component")
    artifacts = artifacts or ArtifactStore(config.artifact_root)
    initial = fingerprint_details(config, component)
    request, task = _request(kind)
    with tempfile.TemporaryDirectory(prefix="sa-component-proof-") as directory:
        root = Path(directory).resolve()
        isolated = _fixture_config(config, component, root)
        executed = fingerprint_details(isolated, component)
        runner = ComponentRunner(isolated)
        outputs, states, counts = [], [], []
        for _ in range(2):
            result = runner.run_component(component, request)
            if result.exit != 0 or result.json.get("request_id") != request["request_id"] or result.json.get("ok") is not True:
                raise ComponentProofError("Actual fixture invocation did not confirm success")
            kind.result(result.json, task)
            state, count = _state(root / "data/fake-state.json", request["operation"], request["idempotency_key"])
            outputs.append(result.json["data"])
            states.append(state)
            counts.append(count)
        if outputs[0] != outputs[1] or states[0] != states[1]:
            raise ComponentProofError("Same-key fixture invocation changed result or persisted state")
        if fingerprint_details(config, component) != initial or fingerprint_details(isolated, component) != executed:
            raise ComponentProofError("Component changed while the proof was running")
        first_hash, second_hash = [artifacts.put(canonical_json(value)) for value in outputs]
        state_refs = [artifacts.put(canonical_json(value)) for value in states]
    report = {"schema": "sa_dispatch.idempotency_proof.v1", "component": component,
              "capability": kind.name + "@" + str(kind.version), "component_fingerprint": initial["component_fingerprint"],
              "generated_by": GENERATOR, "scope": "synthetic_only", "registration_authority": "operator_registered",
              "same_write_key": True, "calls": 2, "records_after_first": counts[0], "records_after_second": counts[1],
              "first_result_hash": first_hash, "second_result_hash": second_hash,
              "state_after_first_ref": state_refs[0], "state_after_second_ref": state_refs[1],
              "request_sha256": sha256_ref(canonical_json(request)), "write_key": request["idempotency_key"],
              "configured_implementation": initial, "executed_implementation": executed,
              "isolation": {"data_directory": "fresh_temporary", "working_directory": "fresh_temporary",
                            "fixture": "bundled_synthetic", "temporary_directory_removed": True,
                            "configured_data_directory_touched": False},
              "fixture_sha256": _file(FIXTURE, "fixture")["sha256"], "platform_calls": 0}
    proof_ref = artifacts.put(canonical_json(report))
    return {"proof_ref": proof_ref, "proof": report, "registration_status": "not_registered"}
