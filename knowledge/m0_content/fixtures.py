"""Two invented chains. No real account, script, research body, media, or approval."""
from contract import ulid

GRANT = "m0-content-contract-fixture-v1"


def put(service, trace, owner, kind, slug, body, parents=()):
    return service.invoke("artifact.put", {"owner": owner, "kind": kind, "id": ulid([trace, slug]),
                          "body": body, "derived_from": list(parents), "trace_id": trace})["ref"]


def approve(service, subject_ref, scope):
    return service.invoke("approval.record", {"subject_ref": subject_ref, "scope": scope, "fixture_grant": GRANT})["ref"]


def chain(service, name="game"):
    trace = "synthetic-content-" + name
    strategy = put(service, trace, "creative", "strategy", "strategy", {"hypothesis": "Synthetic hook comparison", "variable": "hook"})
    research = put(service, trace, "research-content", "note", "research", {"text": "Invented research note; no real business content"})
    topic = put(service, trace, "signalhub", "topic", "topic", {"title": "Invented " + name + " topic"})
    adopted = put(service, trace, "mgmt", "adopted_topic", "adopted", {"title": "Synthetic adopted topic"}, [topic])
    context = put(service, trace, "creative", "context", "context", {"purpose": "synthetic script"}, [adopted, research, strategy])
    script = put(service, trace, "mgmt", "script", "script", {"text": "Invented script for " + name + "; no actual video"}, [context])
    script_approval = approve(service, script, "script_ready")
    cut = put(service, trace, "production", "take", "cut", {"candidate": "Synthetic reference only; no media rendered"}, [script, script_approval])
    cut_approval = approve(service, cut, "cut_review")
    evidence = put(service, trace, "fixture", "evidence", "evidence", {"source": "synthetic_test", "statement": "Simulated export only"})
    request = {"account_ref": "webee/account/synthetic-" + name,
               "inputs": {"cut_ref": cut, "approval_ref": cut_approval, "approval_version_bound": 1},
               "experiment": {"experiment_id": "synthetic-hook-" + name, "arm": "B", "variable": "hook", "strategy_ref": strategy}}
    return locals() | {"service": None}


def observation(fixture, preparation_id, *, status="published", at="2026-11-15T01:00:00+00:00"):
    return {"preparation_id": preparation_id, "target_id": "dy:synthetic-" + fixture["name"],
            "observed_status": status, "observed_at": at, "source_type": "synthetic_test", "source_ref": fixture["evidence"]}


def metrics(fixture, preparation_id, *, window="T+72h", at="2026-11-18T01:00:00+00:00"):
    return {"preparation_id": preparation_id, "target_id": "dy:synthetic-" + fixture["name"],
            "observed_at": at, "window": window, "source_type": "synthetic_test", "source_ref": fixture["evidence"],
            "metrics": {"views": 1200, "likes": 80, "comments": None, "shares": 3, "favorites": 11}, "missing_fields": ["comments"]}
