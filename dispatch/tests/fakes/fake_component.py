"""A subprocess fixture for an offline WorkStudio v0.1 slice."""

import argparse
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import sys
from tempfile import NamedTemporaryFile
import time


FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "synthetic" / "topic_supply.json"
WRITES = {"project.create", "content.upsert", "recipe.start"}
FIELDS = {
    "topic.list": ("", "track query kind since limit"),
    "topic.get": ("id version", ""),
    "component.query": ("component component_operation input", ""),
    "project.create": ("title", "account content_type brief"),
    "project.get": ("", "project_id"),
    "content.upsert": ("kind title body author source_type evidence_refs", "project_id item_id expected_version card_refs"),
    "content.get": ("item_id", "version"),
    "content.list": ("", "project_id"),
    "recipe.start": ("title brief", "account source_refs card_refs lane platforms creation_mode shot_options"),
    "recipe.get": ("recipe_id", ""),
    "recipe.list": ("", "limit"),
    "context.assemble": ("stage account_id lane purpose evidence_refs", "retrieval_scope remix exploration_ratio"),
    "fake.write.get": ("operation idempotency_key", ""),
}


class FixtureError(Exception):
    def __init__(self, code, message, exit_code=2):
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code


def invalid(message):
    raise FixtureError("VALIDATION_ERROR", message)


def required(value, name):
    if not isinstance(value, str) or not value.strip():
        invalid(f"{name} must be a nonempty string")
    return value


def digest(value):
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def envelope(request_id, data=None, error=None):
    return {
        "schema_version": "0.1", "request_id": request_id, "ok": error is None,
        "data": data, "error": None if error is None else {
            "code": error.code, "message": str(error), "retryable": False, "details": {},
        }, "warnings": [],
    }


@contextmanager
def state_lock(directory):
    """Serialize fake transactions across subprocesses on Windows and Unix."""
    with (directory / "fake-state.lock").open("a+b") as handle:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield


def save_state(path, state):
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as file:
        temporary = Path(file.name)
        try:
            json.dump(state, file, ensure_ascii=False)
            file.flush()
            os.fsync(file.fileno())
            file.close()
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def fixed_refs(value):
    if not isinstance(value, list):
        invalid("references must be an array")
    for ref in value:
        if not isinstance(ref, dict):
            invalid("ref must be an object")
        for field in ("owner", "kind", "id", "version"):
            required(ref.get(field), f"ref.{field}")
        if ref["version"].lower() == "latest":
            invalid("ref.version must be fixed")
    return value


def timestamp(value):
    try:
        result = datetime.fromisoformat(required(value, "since").replace("Z", "+00:00"))
        if result.tzinfo is not None:
            return result
    except ValueError:
        pass
    invalid("since requires an ISO timestamp with a timezone")


class FakeComponent:
    def __init__(self, fixture, state):
        self.fixture = fixture
        self.state = state

    def invoke(self, request):
        if request.get("schema_version") != "0.1":
            raise FixtureError("UNSUPPORTED_SCHEMA", "schema_version must be 0.1")
        required(request.get("request_id"), "request_id")
        operation = required(request.get("operation"), "operation")
        if operation not in FIELDS:
            raise FixtureError("UNSUPPORTED_OPERATION", "operation is not supported")
        authorization = request.get("authorization")
        if authorization != {"mode": "local_only"}:
            code = "ACCESS_DENIED" if operation.startswith("topic.") else "AUTHORIZATION_DENIED"
            raise FixtureError(code, "only local_only authorization is accepted", 4)
        payload = request.get("payload", {})
        mandatory, optional = (set(fields.split()) for fields in FIELDS[operation])
        if not isinstance(payload, dict) or mandatory - payload.keys() or payload.keys() - (mandatory | optional):
            invalid(f"invalid fields for {operation}")
        project_id = request.get("project_id")
        task_id = request.get("task_id")
        for name, value in (("project_id", project_id), ("task_id", task_id)):
            if value is not None:
                required(value, name)
        refs = fixed_refs(request.get("input_refs", []))
        if operation not in WRITES:
            return self.dispatch(operation, payload, project_id)
        key = required(request.get("idempotency_key"), "idempotency_key")
        fingerprint = digest({"project_id": project_id, "task_id": task_id, "input_refs": refs,
                              "payload": payload, "authorization": authorization})
        writes = self.state["writes"].setdefault(operation, {})
        if key in writes:
            entry = writes[key]
            if entry["fingerprint"] != fingerprint:
                raise FixtureError("IDEMPOTENCY_CONFLICT", "key already used with different business input")
            return entry["result"]
        result = self.dispatch(operation, payload, project_id)
        writes[key] = {"fingerprint": fingerprint, "result": result}
        return result

    def dispatch(self, operation, payload, project_id):
        if operation == "component.query":
            return self.query(payload)
        if operation.startswith("topic."):
            return self.topic(operation, payload)
        if operation == "fake.write.get":
            target = required(payload["operation"], "operation")
            key = required(payload["idempotency_key"], "idempotency_key")
            if target not in WRITES:
                invalid("unsupported write lookup operation")
            entry = self.state["writes"].get(target, {}).get(key)
            return {"found": entry is not None, "result": entry["result"] if entry else None}
        if operation == "context.assemble":
            return self.context(payload)
        if operation == "project.create":
            return {"project": self.new_project(payload)}
        if operation == "project.get":
            return {"project": self.project(project_id or payload.get("project_id"))}
        if operation.startswith("content."):
            return self.content(operation, payload, project_id)
        return self.recipe(operation, payload, project_id)

    def query(self, payload):
        component = payload["component"]
        operation = payload["component_operation"]
        allowed = {"signal_hub": ("topic.list", "topic.get"), "creative_library": ("context.assemble",)}
        if not isinstance(component, str) or operation not in allowed.get(component, ()):
            raise FixtureError("UNSUPPORTED_OPERATION", "query is not allowlisted")
        upstream_request = {
            "schema_version": "0.1", "request_id": "query_synthetic", "operation": operation,
            "authorization": {"mode": "local_only"}, "payload": payload["input"],
        }
        try:
            upstream = envelope("query_synthetic", self.invoke(upstream_request))
            exit_code = 0
        except FixtureError as error:
            upstream = envelope("query_synthetic", error=error)
            exit_code = error.exit_code
        stdout = (json.dumps(upstream, ensure_ascii=False) + "\n").encode("utf-8")
        metadata = {"exit_code": exit_code, "elapsed_ms": 0, "stdout_bytes": len(stdout),
                    "stderr_bytes": 0, "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
                    "stderr_sha256": hashlib.sha256(b"").hexdigest()}
        return {"component": component, "operation": operation, "upstream": upstream,
                "metadata": metadata, "persisted": False}

    def topic(self, operation, payload):
        items = self.fixture["items"]
        if operation == "topic.get":
            item_id = required(payload["id"], "id")
            version = required(payload["version"], "version")
            for item in items:
                if item["ref"]["id"] == item_id:
                    if item["ref"]["version"] != version:
                        raise FixtureError("STALE_REFERENCE", "requested topic version is not available")
                    return item
            raise FixtureError("NOT_FOUND", "topic not available")
        track = payload.get("track")
        kind = payload.get("kind")
        query = payload.get("query", "")
        limit = payload.get("limit", 10)
        since = timestamp(payload["since"]) if "since" in payload else None
        if track is not None and track not in self.fixture["tracks"]:
            invalid("unknown track")
        if kind is not None:
            required(kind, "kind")
        if not isinstance(query, str) or len(query) > 200 or type(limit) is not int or not 1 <= limit <= 50:
            invalid("query must have at most 200 characters and limit must be 1..50")
        query = query.strip().casefold()
        matched = [item for item in items if
                   (track is None or item["track"] == track)
                   and (kind is None or item["kind"] == kind)
                   and (since is None or timestamp(item["observed_at"]) >= since)
                   and query in " ".join(item.get(field) or "" for field in
                                         ("title", "summary", "audience_hypothesis", "fit")).casefold()]
        matched.sort(key=lambda item: timestamp(item["observed_at"]), reverse=True)
        return {"items": matched[:limit], "tracks": self.fixture["tracks"], "matched": len(matched),
                "visibility": "local_only", "analysis_scan_limit_hit": False}

    def project(self, project_id):
        project_id = required(project_id, "project_id")
        if project_id not in self.state["projects"]:
            raise FixtureError("NOT_FOUND", "project not found")
        return self.state["projects"][project_id]

    def new_project(self, payload):
        projects = self.state["projects"]
        project_id = f"prj_synthetic_{len(projects)}"
        project = {"id": project_id, **payload, "title": required(payload.get("title"), "title")}
        projects[project_id] = project
        return project

    def content(self, operation, payload, project_id):
        contents = self.state["contents"]
        if operation == "content.get":
            versions = contents.get(required(payload["item_id"], "item_id"), [])
            version = payload.get("version", len(versions))
            if type(version) is not int or ("version" in payload and version < 1):
                invalid("version must be a positive integer")
            if not 1 <= version <= len(versions) or (project_id and versions[0]["project_id"] != project_id):
                raise FixtureError("NOT_FOUND", "content version not found")
            return {"content": {**versions[version - 1], "current_version": len(versions)}}
        project_id = self.project(project_id or payload.get("project_id"))["id"]
        if operation == "content.list":
            return {"contents": [versions[-1] for versions in reversed(contents.values())
                                 if versions[-1]["project_id"] == project_id]}
        kind = required(payload["kind"], "kind")
        snapshot = {field: required(payload[field], field)
                    for field in ("title", "body", "author", "source_type")}
        if kind not in ("topic", "script") or snapshot["source_type"] not in ("human_authored", "agent_draft", "imported"):
            invalid("unsupported kind or source type")
        snapshot["evidence_refs"] = fixed_refs(payload["evidence_refs"])
        snapshot["card_refs"] = fixed_refs(payload.get("card_refs", []))
        if not snapshot["evidence_refs"]:
            invalid("at least one fixed evidence ref is required")
        item_id = payload.get("item_id")
        expected = payload.get("expected_version")
        if item_id is None:
            if expected is not None:
                invalid("expected_version is only for updates")
            item_id = f"cnt_synthetic_{len(contents) + 1}"
            versions = contents.setdefault(item_id, [])
        else:
            versions = contents.get(required(item_id, "item_id"))
            if not versions or versions[-1]["project_id"] != project_id or versions[-1]["kind"] != kind:
                raise FixtureError("NOT_FOUND", "content item not found in project")
            if type(expected) is not int or expected != len(versions):
                raise FixtureError("VERSION_CONFLICT", "expected_version must equal current_version")
        version = len(versions) + 1
        sha256 = digest(snapshot)
        ref = {"owner": "contentops_core", "kind": kind, "id": item_id, "version": str(version),
               "uri": f"contentops://content/{item_id}/versions/{version}", "sha256": sha256}
        content = {**snapshot, "item_id": item_id, "version": version, "current_version": version,
                   "project_id": project_id, "kind": kind, "sha256": sha256, "ref": ref}
        versions.append(content)
        return {"content": content}

    def recipe(self, operation, payload, project_id):
        recipes = self.state["recipes"]
        if operation == "recipe.get":
            recipe = recipes.get(required(payload["recipe_id"], "recipe_id"))
            if recipe is None or (project_id and recipe["project_id"] != project_id):
                raise FixtureError("NOT_FOUND", "recipe not found in project")
            return recipe
        if operation == "recipe.list":
            limit = payload.get("limit", 20)
            if type(limit) is not int or not 1 <= limit <= 100:
                invalid("limit must be 1..100")
            return {"recipes": [r for r in reversed(recipes.values())
                                if project_id is None or r["project_id"] == project_id][:limit]}
        title = required(payload["title"], "title")
        brief = required(payload["brief"], "brief")
        recipe_id = f"recipe_synthetic_{len(recipes) + 1}"
        refs = fixed_refs(payload.get("source_refs", [])) or [
            {"owner": "contentops_core", "kind": "brief", "id": recipe_id, "version": "1",
             "sha256": hashlib.sha256(brief.encode("utf-8")).hexdigest()},
        ]
        source_evidence = []
        for ref in refs:
            evidence = {"ref": ref}
            if ref["owner"] == "signal_hub" and ref["kind"] in {"analysis_topic", "radar_card"}:
                evidence["evidence"] = self.topic("topic.get", ref)
            source_evidence.append(evidence)
        context = self.context({
            "stage": "script", "account_id": payload.get("account") or "local-unspecified",
            "lane": payload.get("lane", "ai_knowledge"), "purpose": brief, "evidence_refs": refs,
        })
        project = self.project(project_id) if project_id else self.new_project({"title": title, "brief": brief})
        result = {
            "recipe_id": recipe_id, "project_id": project["id"], "status": "needs_agent",
            "steps": {"creative_context": {"status": "succeeded", "output": context}},
            "context_gaps": context["gaps"], "pending_items": [],
            "artifacts": {}, "human_review_pending": True, "publishable": False, "published": False,
            "creation_mode": payload.get("creation_mode", "quick_preview"),
            "agent_handoff": {"brief": brief, "source_refs": refs, "source_evidence": source_evidence,
                              "creative_context": context, "instruction": "Draft a synthetic script.",
                              "resume_operation": "recipe.resume", "resume_payload": {"recipe_id": recipe_id,
                                  "script": {"body": "<synthetic script>", "author": "Codex",
                                             "source_type": "agent_draft"}}},
        }
        recipes[recipe_id] = result
        return result

    @staticmethod
    def context(payload):
        if payload["stage"] != "script" or payload["lane"] not in ("ai_knowledge", "ai_game"):
            invalid("fake supports the script stage and supported WorkStudio lanes")
        for field in ("account_id", "purpose"):
            required(payload[field], field)
        if not fixed_refs(payload["evidence_refs"]):
            invalid("at least one fixed evidence ref is required")
        return {
            **payload, "selected_cards": [], "excluded": [], "selection_strategy": {},
            "retrieval_scope": payload.get("retrieval_scope", {}),
            "remix": payload.get("remix", {"change_level": "constrained"}),
            "exploration_ratio": payload.get("exploration_ratio", 0),
            "gaps": [{"code": "STAGE_CARD_MISSING", "message": "Fixture includes no creative cards."}],
            "constraints": ["Preserve evidence usage limits."],
        }


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def initial_state():
    return {"projects": {"synthetic-project": {"id": "synthetic-project", "title": "Synthetic project"}},
            "contents": {}, "recipes": {}, "writes": {}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=os.environ.get("FAKE_DATA_DIR"), type=Path)
    parser.add_argument("--fixture", default=FIXTURE, type=Path)
    invoke = parser.add_subparsers(dest="command", required=True).add_parser("invoke")
    invoke.add_argument("--request", required=True, type=Path)
    invoke.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.data_dir is None:
        parser.error("--data-dir or FAKE_DATA_DIR is required")
    sys.stdout.reconfigure(encoding="utf-8")
    request_id = None
    try:
        try:
            request = read_json(args.request)
        except (ValueError, OSError) as exc:
            raise FixtureError("VALIDATION_ERROR", "request must be a readable JSON object") from exc
        if not isinstance(request, dict):
            invalid("request must be an object")
        request_id = request.get("request_id") if isinstance(request.get("request_id"), str) else None
        operation = request.get("operation")
        fail_operation = os.environ.get("FAKE_FAIL_OPERATION")
        fault = os.environ.get("FAKE_FAIL") if not fail_operation or fail_operation == operation else None
        if fault == "timeout":
            time.sleep(float(os.environ.get("FAKE_TIMEOUT_SECONDS", "30")))
        if fault == "bad_json":
            print("fixture: malformed response")
            return 0
        if fault == "exit5":
            raise FixtureError("EXECUTION_FAILED", "fixture: execution failure before write", 5)
        args.data_dir.mkdir(parents=True, exist_ok=True)
        path = args.data_dir / "fake-state.json"
        fixture = read_json(args.fixture)
        with state_lock(args.data_dir):
            state = read_json(path) if path.exists() else initial_state()
            result = FakeComponent(fixture, state).invoke(request)
            if isinstance(operation, str) and operation in WRITES:
                save_state(path, state)
                if fault == "crash_after_write":
                    print("fixture: process aborted after commit", file=sys.stderr, flush=True)
                    os._exit(5)
        print(json.dumps(envelope(request_id, result), ensure_ascii=False))
        return 0
    except FixtureError as error:
        print(json.dumps(envelope(request_id, error=error), ensure_ascii=False))
        return error.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
