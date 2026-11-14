"""Adapters for the existing content operations CLI."""

import re


def text(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 12000:
        raise ValueError("expected non-empty text")
    return value


def keys(value, required, optional=()):
    if not isinstance(value, dict) or not set(required) <= value.keys() or set(value) - set(required) - set(optional):
        raise ValueError("invalid fields for hotspot operation")


def reference(value):
    keys(value, ("owner", "kind", "id", "version"), ("sha256", "hash", "uri", "access_level", "usage_restrictions"))
    for name in ("owner", "kind", "id", "version"):
        text(value[name])
    if value["version"].lower() == "latest":
        raise ValueError("evidence reference requires a fixed version")
    if "sha256" in value and not re.fullmatch(r"[0-9a-f]{64}", text(value["sha256"])):
        raise ValueError("invalid reference SHA-256")
    return {k: value[k] for k in ("owner", "kind", "id", "version", "access_level", "usage_restrictions", "sha256") if k in value}


class HotspotKind:
    version = 1
    allow_no_new = False
    component = "mgmtcore"

    def __init__(self, name, side_effect, enabled=True):
        self.name = "hotspot." + name
        self.side_effect = side_effect
        self.enabled = enabled

    def scope(self, params):
        return {"project_id": params.get("project_id")}

    def validate(self, params):
        if self.name == "hotspot.topic_fetch":
            keys(params, ("op", "filters"))
            filters = params["filters"]
            if params["op"] == "topic.get":
                keys(filters, ("id", "version"))
                text(filters["id"])
                if not re.fullmatch(r"[0-9a-f]{64}", text(filters["version"])):
                    raise ValueError("expected fixed topic version")
            elif params["op"] == "topic.list":
                keys(filters, (), ("track", "query", "kind", "since", "limit"))
                for k in filters.keys() - {"limit", "query"}:
                    text(filters[k])
                if "query" in filters and (not isinstance(filters["query"], str) or len(filters["query"]) > 200):
                    raise ValueError("invalid query")
                if "limit" in filters and (type(filters["limit"]) is not int or not 1 <= filters["limit"] <= 50):
                    raise ValueError("invalid topic limit")
            else:
                raise ValueError("unsupported topic operation")
        elif self.name == "hotspot.topic_adopt":
            keys(params, ("project_id", "title", "body", "author", "evidence_refs"))
            for k in ("project_id", "title", "body", "author"):
                text(params[k])
            self.refs(params["evidence_refs"])
        else:
            keys(params, ("project_id", "title", "brief", "lane", "source_refs"), ("account",))
            for k in params.keys() - {"source_refs"}:
                text(params[k])
            if params["lane"] not in {"ai_knowledge", "ai_game"}:
                raise ValueError("unsupported creative lane")
            self.refs(params["source_refs"])
        return params

    @staticmethod
    def refs(refs):
        if not isinstance(refs, list) or not 1 <= len(refs) <= 20:
            raise ValueError("expected one to twenty evidence references")
        return [reference(ref) for ref in refs]

    def request(self, task):
        params = task.spec.params
        if self.name == "hotspot.topic_fetch":
            operation = "component.query"
            payload = {"component": "signal_hub", "component_operation": params["op"], "input": params["filters"]}
        elif self.name == "hotspot.topic_adopt":
            operation = "content.upsert"
            payload = {k: params[k] for k in ("title", "body", "author")}
            payload.update(kind="topic", source_type="imported", evidence_refs=self.refs(params["evidence_refs"]))
        else:
            operation = "recipe.start"
            payload = {k: params[k] for k in params.keys() - {"project_id", "source_refs"}}
            payload["source_refs"] = self.refs(params["source_refs"])
        return {
            "schema_version": "0.1", "request_id": task.task_id, "operation": operation,
            "project_id": params.get("project_id"), "task_id": None, "input_refs": [],
            "idempotency_key": None if self.side_effect == "none" else "sad:" + task.task_id + ":invoke",
            "payload": payload, "authorization": {"mode": "local_only"},
        }

    def result(self, response, task):
        data = checked(response)
        if self.name == "hotspot.topic_fetch":
            if data.get("component") != "signal_hub" or data.get("operation") != task.spec.params["op"]:
                raise ValueError("response does not identify the requested component")
            topics = checked(data.get("upstream"))
            if task.spec.params["op"] == "topic.list":
                items = topics.get("items")
                if not isinstance(items, list):
                    raise ValueError("topic list response is missing its items")
                return {"count": len(items)}
            ref = reference(topics.get("ref"))
            if any(ref[key] != value for key, value in task.spec.params["filters"].items()):
                raise ValueError("detail response does not match the fixed topic reference")
            return {"count": 1, "ref": ref}
        if self.name == "hotspot.topic_adopt":
            ref = data.get("content", {}).get("ref")
            if not isinstance(ref, dict):
                raise ValueError("component did not return a fixed content reference")
            return {"ref": reference(ref)}
        status = data.get("status")
        if status not in {"needs_agent", "needs_human_action", "succeeded"}:
            raise ValueError("recipe outcome requires review before completion")
        return {"recipe_id": text(data.get("recipe_id")), "component_state": status}


def checked(response):
    if not isinstance(response, dict) or response.get("schema_version") != "0.1" or response.get("ok") is not True:
        raise ValueError("component did not confirm success")
    data = response.get("data")
    if not isinstance(data, dict):
        raise ValueError("component response is missing its data object")
    return data


KINDS = {kind.name: kind for kind in (
    HotspotKind("topic_fetch", "none"),
    HotspotKind("topic_adopt", "idempotent_write"),
    HotspotKind("context_prepare", "idempotent_write"),
    HotspotKind("collect", "external", False),
)}
