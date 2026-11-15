"""Isolated, synthetic-only content/WeBee contract. No platform or network adapter.

This is not an authority for human review. The only grant is an explicitly named
test fixture; every approval records human_reviewed=False and publish_authorized=False.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import secrets
import sqlite3
import sys

ROOT = Path(__file__).resolve().parent
METRICS = frozenset(("views", "likes", "comments", "shares", "favorites"))
WINDOWS = frozenset(("T+1h", "T+24h", "T+72h", "T+7d"))
REF = re.compile(r"^([a-z][a-z0-9_-]*)/([a-z][a-z0-9_-]*)/([A-Za-z0-9:_-]+)@([1-9][0-9]*)#([0-9a-f]{64})$")
TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,100}$")
SCHEMA = """
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE artifacts(ref TEXT PRIMARY KEY, identity TEXT NOT NULL, version INTEGER NOT NULL,
 trace_id TEXT NOT NULL, payload TEXT NOT NULL, UNIQUE(identity,version));
CREATE TABLE heads(identity TEXT PRIMARY KEY, ref TEXT NOT NULL REFERENCES artifacts(ref));
CREATE TABLE approvals(ref TEXT PRIMARY KEY REFERENCES artifacts(ref), subject_ref TEXT NOT NULL,
 scope TEXT NOT NULL, version_bound INTEGER NOT NULL, upstream_refs TEXT NOT NULL);
CREATE TABLE preparations(preparation_id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE NOT NULL,
 fingerprint TEXT NOT NULL, ref TEXT NOT NULL REFERENCES artifacts(ref), trace_id TEXT NOT NULL,
 account_ref TEXT NOT NULL, state TEXT NOT NULL, target_id TEXT, evidence_ref TEXT, unknown_at TEXT,
 simulated_external_calls INTEGER NOT NULL DEFAULT 0);
CREATE TABLE observations(preparation_id TEXT NOT NULL, observed_at TEXT NOT NULL,
 ref TEXT UNIQUE NOT NULL, fingerprint TEXT NOT NULL, PRIMARY KEY(preparation_id,observed_at));
CREATE TABLE metric_snapshots(snapshot_id TEXT PRIMARY KEY, preparation_id TEXT NOT NULL,
 target_id TEXT NOT NULL, account_ref TEXT NOT NULL, window TEXT NOT NULL, observed_at TEXT NOT NULL,
 ref TEXT NOT NULL REFERENCES artifacts(ref), fingerprint TEXT NOT NULL,
 UNIQUE(preparation_id,target_id,window,observed_at));
CREATE TABLE metric_matches(snapshot_id TEXT PRIMARY KEY REFERENCES metric_snapshots(snapshot_id),
 preparation_id TEXT NOT NULL, ref TEXT NOT NULL REFERENCES artifacts(ref));
CREATE TABLE strategy_pointers(identity TEXT PRIMARY KEY, ref TEXT NOT NULL REFERENCES artifacts(ref));
CREATE TABLE changes(seq INTEGER PRIMARY KEY, trace_id TEXT NOT NULL, op TEXT NOT NULL,
 from_ref TEXT, to_ref TEXT NOT NULL, change_ref TEXT NOT NULL REFERENCES artifacts(ref));
CREATE TABLE content_events(seq INTEGER PRIMARY KEY, event_id TEXT UNIQUE NOT NULL,
 trace_id TEXT NOT NULL, operation TEXT NOT NULL, kind TEXT NOT NULL, at TEXT NOT NULL,
 refs_json TEXT NOT NULL, metadata_json TEXT NOT NULL);
"""


class ContractError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def reject(code, message):
    raise ContractError(code, message)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def ulid(seed=None):
    # Fixture IDs are deterministic ULID-form values, never real business IDs.
    value = int(digest(seed)[:32], 16) if seed is not None else (
        (int(datetime.now(timezone.utc).timestamp() * 1000) << 80) | secrets.randbits(80))
    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    result = ""
    for _ in range(26):
        result = alphabet[value & 31] + result
        value >>= 5
    return result


def timestamp(value=None):
    try:
        dt = datetime.now(timezone.utc) if value is None else datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            raise ValueError("timezone missing")
        return dt.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError, AttributeError):
        reject("INVALID_TIMESTAMP", "An ISO timestamp with timezone is required")


def parse_ref(ref):
    match = REF.fullmatch(ref) if isinstance(ref, str) else None
    if not match:
        reject("INVALID_REF", "A complete fixed owner/kind/id@version#sha256 reference is required")
    owner, kind, ident, version, sha = match.groups()
    return f"{owner}/{kind}/{ident}", int(version), sha


def token(value):
    if not isinstance(value, str) or not TOKEN.fullmatch(value):
        reject("INVALID_ID", "An explicit metadata identifier is required")
    return value


class Contract:
    def __init__(self, path, *, create=False, dependency_guard=None):
        self.path = Path(path).resolve()
        # This object is supplied by trusted workflow code, never an invocation field.
        self.dependency_guard = dependency_guard
        if create:
            # Refuse to open or migrate an existing business database.
            with self.path.open("xb"):
                pass
        elif not self.path.is_file():
            reject("NOT_FOUND", "Synthetic database does not exist")
        self.db = sqlite3.connect(self.path, isolation_level=None, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        if create:
            self.db.executescript(SCHEMA)
            self.db.executemany("INSERT INTO meta VALUES (?,?)", (
                ("schema", "m0-content-synthetic-v1"), ("instance", ulid()), ("platform_calls", "0")))
        try:
            if self.db.execute("SELECT value FROM meta WHERE key='schema'").fetchone()[0] != "m0-content-synthetic-v1":
                raise ValueError("wrong schema")
        except (sqlite3.Error, TypeError, ValueError):
            self.db.close()
            reject("NOT_SYNTHETIC_DATABASE", "This module only opens its own fixture database")

    def close(self):
        self.db.close()

    def _event(self, trace_id, operation, refs, kind="task.succeeded", metadata=None):
        self.db.execute("INSERT INTO content_events(event_id,trace_id,operation,kind,at,refs_json,metadata_json) VALUES(?,?,?,?,?,?,?)",
                        (ulid(), token(trace_id), operation, kind, timestamp(), canonical(refs), canonical(metadata or {})))

    def get(self, ref):
        identity, version, expected = parse_ref(ref)
        row = self.db.execute("SELECT * FROM artifacts WHERE ref=?", (ref,)).fetchone()
        if row is None:
            reject("REF_NOT_FOUND", "Reference cannot be resolved in this synthetic store")
        payload = json.loads(row["payload"])
        if digest(payload) != expected or row["identity"] != identity or row["version"] != version:
            reject("HASH_MISMATCH", "Stored artifact does not match its fixed reference")
        return {"ref": ref, **payload}

    def _put(self, owner, kind, ident, body, derived_from, trace_id):
        token(trace_id)
        identity = f"{owner}/{kind}/{ident}"
        parse_ref(f"{identity}@1#{'0' * 64}")
        if len(set(derived_from)) != len(derived_from):
            reject("INVALID_REFS", "Duplicate dependencies are not permitted")
        for ref in derived_from:
            self.get(ref)
        head = self.db.execute("SELECT ref FROM heads WHERE identity=?", (identity,)).fetchone()
        version = parse_ref(head[0])[1] + 1 if head else 1
        payload = {"owner": owner, "kind": kind, "id": ident, "version": version,
                   "class": "internal", "synthetic": True, "trace_id": trace_id,
                   "body": body, "derived_from": sorted(derived_from)}
        ref = f"{identity}@{version}#{digest(payload)}"
        self.db.execute("INSERT INTO artifacts VALUES(?,?,?,?,?)", (ref, identity, version, trace_id, canonical(payload)))
        self.db.execute("INSERT INTO heads VALUES(?,?) ON CONFLICT(identity) DO UPDATE SET ref=excluded.ref", (identity, ref))
        return ref

    def closure(self, subject_ref, *, current=False):
        refs, pending = set(), [subject_ref]
        while pending:
            ref = pending.pop()
            if ref in refs:
                continue
            item = self.get(ref)
            if current:
                identity = parse_ref(ref)[0]
                head = self.db.execute("SELECT ref FROM heads WHERE identity=?", (identity,)).fetchone()[0]
                if head != ref:
                    reject("APPROVAL_STALE", "An upstream version or hash has changed; explicit new review is required")
                mapping = self.external_context(ref)
                if mapping is not None:
                    if self.dependency_guard is None:
                        reject("EXTERNAL_DEPENDENCY_UNVERIFIED", "A live trusted research guard is required")
                    checked = self.dependency_guard.check_external(mapping)
                    if not isinstance(checked, dict) or checked.get("usable") is not True:
                        state = checked.get("state") if isinstance(checked, dict) else None
                        code = {"stale_by_revocation": "EXTERNAL_DEPENDENCY_REVOKED",
                                "expired": "EXTERNAL_DEPENDENCY_EXPIRED"}.get(state, "EXTERNAL_DEPENDENCY_UNVERIFIED")
                        reject(code, "Current research authorization or dependency validity is unavailable")
            refs.add(ref)
            pending.extend(item["derived_from"])
        return sorted(refs)

    def external_context(self, ref):
        """Return hash-bound metadata only; legacy databases need no migration."""
        body = self.get(ref)["body"]
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='external_contexts'").fetchone():
            if "external_context" in body:
                reject("EXTERNAL_DEPENDENCY_UNVERIFIED", "External context registration is missing")
            return None
        row = self.db.execute("SELECT * FROM external_contexts WHERE content_context_ref=?", (ref,)).fetchone()
        if row is None:
            if "external_context" in body:
                reject("EXTERNAL_DEPENDENCY_UNVERIFIED", "External context registration is missing")
            return None
        mapping = {key: row[key] for key in ("workflow_binding", "source_binding", "catalog_instance", "context_id", "manifest_sha256", "max_class")}
        for name in ("refs", "caller", "domains", "scopes"):
            mapping[name] = json.loads(row[name + "_json"])
        if (digest(mapping) != row["mapping_sha256"] or body.get("external_context") != mapping or
                body.get("workflow_binding") != row["workflow_binding"]):
            reject("HASH_MISMATCH", "External context registration does not match its fixed artifact")
        return mapping

    def _approve(self, p, *, control_approval=None):
        grant = json.loads((ROOT / "fixture_authorization.json").read_text(encoding="utf-8"))
        if p.get("fixture_grant") != grant["grant_id"]:
            reject("FIXTURE_AUTHORIZATION_REQUIRED", "Only the named synthetic grant is accepted; this is not human review")
        subject = self.get(p["subject_ref"])
        expected_scope = {"script": "script_ready", "take": "cut_review"}.get(subject["kind"])
        if not expected_scope or p.get("scope") != expected_scope:
            reject("APPROVAL_SCOPE_MISMATCH", "Approval scope must match the script or candidate cut")
        refs = self.closure(subject["ref"], current=True)
        if p.get("refs") is not None and sorted(p["refs"]) != refs:
            reject("INVALID_APPROVAL_REFS", "Approval must bind the complete transitive upstream set")
        body = {"decision": "approved", "scope": expected_scope, "version_bound": subject["version"],
                "subject_ref": subject["ref"], "upstream_refs": refs, "upstream_sha256": digest(refs),
                "authorization": grant, "decided_by": grant["actor"], "human_reviewed": False,
                "publish_authorized": False}
        if control_approval is not None:
            body["control_approval"] = control_approval
        ref = self._put("mgmt", "approval", ulid(), body, refs, subject["trace_id"])
        self.db.execute("INSERT INTO approvals VALUES(?,?,?,?,?)", (ref, subject["ref"], expected_scope, subject["version"], canonical(refs)))
        self._event(subject["trace_id"], "approval.record", [ref], "approval.decided", {"decision": "observed"})
        return self.get(ref)

    def verify_approval(self, approval_ref, subject_ref, scope, version_bound):
        if type(version_bound) is not int or version_bound < 1:
            reject("APPROVAL_STALE", "An explicit integer approval_version_bound is required")
        row = self.db.execute("SELECT * FROM approvals WHERE ref=?", (approval_ref,)).fetchone()
        if not row or row["subject_ref"] != subject_ref or row["scope"] != scope or row["version_bound"] != version_bound:
            reject("APPROVAL_STALE", "Approval is absent or bound to a different version/scope")
        approved = self.get(approval_ref)["body"]
        control = approved.get("control_approval")
        if control is not None:
            if (not isinstance(control, dict) or "expires_at" not in control or
                    datetime.fromisoformat(timestamp(control["expires_at"])) <= datetime.now(timezone.utc)):
                reject("CONTROL_APPROVAL_EXPIRED", "The synthetic control approval is no longer current")
        refs = self.closure(subject_ref, current=True)
        if refs != json.loads(row["upstream_refs"]) or digest(refs) != approved["upstream_sha256"]:
            reject("APPROVAL_STALE", "Approval must match every explicit upstream version and hash")
        return refs

    def _prepare(self, p):
        inputs = p["inputs"]
        if not isinstance(inputs, dict) or set(inputs) != {"cut_ref", "approval_ref", "approval_version_bound"}:
            reject("INVALID_INPUTS", "Prepare requires cut_ref, approval_ref, and approval_version_bound")
        cut = self.get(inputs["cut_ref"])
        if cut["owner"] != "production" or cut["kind"] != "take":
            reject("INVALID_CUT", "A versioned candidate take is required")
        refs = self.verify_approval(inputs["approval_ref"], cut["ref"], "cut_review", inputs["approval_version_bound"])
        account = p["account_ref"]
        if not re.fullmatch(r"webee/account/synthetic-[A-Za-z0-9_-]+", account):
            reject("SYNTHETIC_ACCOUNT_REQUIRED", "Only synthetic account identifiers are accepted")
        experiment = p.get("experiment")
        if experiment:
            if set(experiment) != {"experiment_id", "arm", "variable", "strategy_ref"}:
                reject("INVALID_EXPERIMENT", "Experiment requires id, arm, variable, and fixed strategy_ref")
            strategy = self.get(experiment["strategy_ref"])
            if strategy["owner"] != "creative" or strategy["kind"] != "strategy":
                reject("INVALID_STRATEGY", "Experiment must use a fixed strategy version")
            if strategy["ref"] not in refs:
                reject("APPROVAL_STALE", "Experiment strategy must belong to the approved upstream reference set")
        body = {"account_ref": account, "session_holder": "synthetic-local", "inputs": inputs,
                "upstream_refs": refs, "experiment": experiment, "publish_authorized": False}
        fingerprint = digest(body)
        preparation_id = p.get("preparation_id", ulid(["preparation", fingerprint]))
        token(preparation_id)
        key = f"pub:{preparation_id}:{account.rsplit('/', 1)[-1]}:{cut['ref']}"
        if p.get("idempotency_key", key) != key:
            reject("IDEMPOTENCY_KEY_MISMATCH", "Idempotency key must include this preparation, account, and full fixed cut")
        prior = self.db.execute("SELECT * FROM preparations WHERE preparation_id=? OR idempotency_key=?", (preparation_id, key)).fetchone()
        if prior:
            if prior["fingerprint"] != fingerprint:
                reject("IDEMPOTENCY_CONFLICT", "Existing preparation identity has different immutable inputs")
            return self.status(prior["preparation_id"])
        same = self.db.execute("SELECT preparation_id FROM preparations WHERE fingerprint=?", (fingerprint,)).fetchone()
        if same:
            reject("DUPLICATE_PREPARATION", "Identical inputs already have a preparation; changing its id cannot replay the work")
        unresolved = self.db.execute("SELECT 1 FROM preparations WHERE state='submitted_unknown' AND (trace_id=? OR account_ref=?)",
                                     (cut["trace_id"], account)).fetchone()
        if unresolved:
            reject("EXTERNAL_UNKNOWN_UNRESOLVED", "Reconcile this trace/account's unknown effect before creating another preparation")
        body.update(preparation_id=preparation_id, idempotency_key=key)
        ref = self._put("webee", "publish_prep", preparation_id, body,
                        sorted(set(refs + [inputs["approval_ref"]] + ([experiment["strategy_ref"]] if experiment else []))), cut["trace_id"])
        self.db.execute("INSERT INTO preparations(preparation_id,idempotency_key,fingerprint,ref,trace_id,account_ref,state) VALUES(?,?,?,?,?,?,'prepared')",
                        (preparation_id, key, fingerprint, ref, cut["trace_id"], account))
        self._event(cut["trace_id"], "publish.prepare", [ref])
        return self.status(preparation_id)

    def _prep(self, ident):
        row = self.db.execute("SELECT * FROM preparations WHERE preparation_id=?", (ident,)).fetchone()
        if not row:
            reject("PREPARATION_NOT_FOUND", "Unknown preparation")
        return row

    def status(self, ident, as_of=None):
        row = self._prep(ident)
        attention = bool(row["state"] == "submitted_unknown" and datetime.fromisoformat(timestamp(as_of)) -
                         datetime.fromisoformat(row["unknown_at"]) >= timedelta(hours=24))
        return {**self.get(row["ref"])["body"], "ref": row["ref"], "state": row["state"],
                "platform_work_id": row["target_id"], "evidence_ref": row["evidence_ref"],
                "effect": "external", "dispatch_state": "unknown" if row["state"] == "submitted_unknown" else None,
                "needs_human": attention, "automatic_retry_allowed": False,
                "verification_status": "unverified_import", "simulated_external_calls": row["simulated_external_calls"],
                "platform_calls": 0, "synthetic": True}

    def _unknown(self, p):
        row = self._prep(p["preparation_id"])
        if row["state"] != "prepared" or row["simulated_external_calls"]:
            reject("EXTERNAL_REPLAY_FORBIDDEN", "Unknown or completed external effects cannot be automatically replayed")
        if self.db.execute("SELECT 1 FROM preparations WHERE state='submitted_unknown' AND (trace_id=? OR account_ref=?)",
                           (row["trace_id"], row["account_ref"])).fetchone():
            reject("EXTERNAL_UNKNOWN_UNRESOLVED", "A new preparation cannot bypass an unresolved effect")
        body = self.get(row["ref"])["body"]
        i = body["inputs"]
        self.verify_approval(i["approval_ref"], i["cut_ref"], "cut_review", i["approval_version_bound"])
        at = timestamp(p.get("at"))
        self.db.execute("UPDATE preparations SET state='submitted_unknown',unknown_at=?,simulated_external_calls=1 WHERE preparation_id=?", (at, row["preparation_id"]))
        self._event(row["trace_id"], "fixture.submit_unknown", [row["ref"]], "task.unknown", {"to_status": "unknown"})
        return self.status(row["preparation_id"], at)

    def _source(self, p):
        if p["source_type"] not in ("synthetic_test", "manual_export", "third_party_export"):
            reject("INVALID_SOURCE", "Only explicit imported observations are supported")
        evidence = self.get(p["source_ref"])
        if evidence["owner"] != "fixture" or evidence["kind"] != "evidence":
            reject("FIXTURE_EVIDENCE_REQUIRED", "This laboratory only accepts synthetic evidence")
        return timestamp(p["observed_at"])

    def _target(self, target):
        if not isinstance(target, str) or not re.fullmatch(r"(?:dy|bilibili|xhs):synthetic-[A-Za-z0-9_-]+", target):
            reject("OWNERSHIP_MISMATCH", "Expected a synthetic owned publication; reference works belong to SignalHub")

    def _observe(self, p):
        row = self._prep(p["preparation_id"])
        observed = self._source(p)
        self._target(p["target_id"])
        if p["observed_status"] not in ("published", "rejected", "withdrawn"):
            reject("INVALID_STATUS", "Unsupported external observation")
        if row["target_id"] and row["target_id"] != p["target_id"]:
            reject("OWNERSHIP_MISMATCH", "Observation cannot rebind an existing publication")
        other = self.db.execute("SELECT preparation_id FROM preparations WHERE target_id=? AND preparation_id<>?", (p["target_id"], row["preparation_id"])).fetchone()
        if other:
            reject("OWNERSHIP_MISMATCH", "Publication is already bound to another preparation")
        body = {**p, "observed_at": observed, "account_ref": row["account_ref"], "verification_status": "unverified_import"}
        prior = self.db.execute("SELECT * FROM observations WHERE preparation_id=? AND observed_at=?", (row["preparation_id"], observed)).fetchone()
        if prior:
            if prior["fingerprint"] != digest(body):
                reject("OBSERVATION_CONFLICT", "Same observation timestamp has different evidence")
            return self.status(row["preparation_id"])
        if row["state"] == "withdrawn" and p["observed_status"] != "withdrawn":
            reject("INVALID_STATE_TRANSITION", "A terminal withdrawal cannot be overwritten")
        newest = self.db.execute("SELECT MAX(observed_at) FROM observations WHERE preparation_id=?", (row["preparation_id"],)).fetchone()[0]
        if newest and observed < newest:
            reject("OBSERVATION_OUT_OF_ORDER", "Older observation cannot replace the current platform observation")
        ref = self._put("webee", "receipt", ulid(), body, [row["ref"], p["source_ref"]], row["trace_id"])
        self.db.execute("INSERT INTO observations VALUES(?,?,?,?)", (row["preparation_id"], observed, ref, digest(body)))
        self.db.execute("UPDATE preparations SET state=?,target_id=?,evidence_ref=? WHERE preparation_id=?", (p["observed_status"], p["target_id"], ref, row["preparation_id"]))
        kind, state = {"published": ("task.reconciled", "succeeded"),
                       "rejected": ("task.failed", "failed"),
                       "withdrawn": ("task.cancelled", "cancelled")}[p["observed_status"]]
        self._event(row["trace_id"], "publish.observe", [ref], kind, {"to_status": state})
        return self.status(row["preparation_id"])

    def _metrics_import(self, p):
        row = self._prep(p["preparation_id"])
        observed = self._source(p)
        self._target(p["target_id"])
        if p["target_id"] != row["target_id"] or row["state"] != "published":
            reject("OWNERSHIP_MISMATCH", "Metrics must belong to this preparation's observed owned publication")
        if p["window"] not in WINDOWS:
            reject("INVALID_WINDOW", "Allowed windows: T+1h, T+24h, T+72h, T+7d")
        metrics, missing = p["metrics"], p["missing_fields"]
        if not isinstance(metrics, dict) or not isinstance(missing, list) or any(not isinstance(x, str) for x in missing):
            reject("INVALID_METRICS", "Metrics must be an object and missing_fields a list")
        if set(metrics) - METRICS or set(missing) - METRICS or len(set(missing)) != len(missing):
            reject("INVALID_METRICS", "Unsupported or duplicate metric names")
        for name in METRICS:
            value = metrics.get(name)
            if value is None:
                if name not in missing:
                    reject("METRIC_MISSING", "Every absent/null metric must be listed as missing")
            elif name in missing:
                reject("METRIC_CONFLICT", "A metric cannot have a number and be missing")
            elif type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                reject("INVALID_METRICS", "Metric values must be finite nonnegative numbers")
        body = {**p, "observed_at": observed, "account_ref": row["account_ref"],
                "missing_fields": sorted(missing), "verification_status": "unverified_import"}
        fingerprint = digest(body)
        prior = self.db.execute("SELECT * FROM metric_snapshots WHERE preparation_id=? AND target_id=? AND window=? AND observed_at=?",
                                (row["preparation_id"], p["target_id"], p["window"], observed)).fetchone()
        if prior:
            if prior["fingerprint"] != fingerprint:
                reject("METRIC_OBSERVATION_CONFLICT", "Same window/time cannot overwrite an observation")
            return self._metric(prior["snapshot_id"])
        snapshot_id = ulid(["metric", fingerprint])
        ref = self._put("webee", "metrics_obs", snapshot_id, body, [row["evidence_ref"], p["source_ref"]], row["trace_id"])
        self.db.execute("INSERT INTO metric_snapshots VALUES(?,?,?,?,?,?,?,?)", (snapshot_id, row["preparation_id"], p["target_id"], row["account_ref"], p["window"], observed, ref, fingerprint))
        self._event(row["trace_id"], "publish.metrics_import", [ref])
        return self._metric(snapshot_id)

    def _metric(self, ident):
        row = self.db.execute("SELECT * FROM metric_snapshots WHERE snapshot_id=?", (ident,)).fetchone()
        if row is None:
            reject("SNAPSHOT_NOT_FOUND", "Unknown metric snapshot")
        body = self.get(row["ref"])["body"]
        matched = self.db.execute("SELECT ref FROM metric_matches WHERE snapshot_id=?", (ident,)).fetchone()
        return {**body, "ref": row["ref"], "snapshot_id": ident, "metrics_sha256": digest(body["metrics"]),
                "matching_status": "matched" if matched else "pending_matching", "match_ref": matched[0] if matched else None}

    def _metrics_match(self, p):
        row = self._prep(p["preparation_id"])
        snap = self._metric(p["snapshot_id"])
        if snap["preparation_id"] != row["preparation_id"] or snap["target_id"] != row["target_id"] or snap["account_ref"] != row["account_ref"]:
            reject("OWNERSHIP_MISMATCH", "Matching cannot change account, target, preparation, or metric numbers")
        if not snap["match_ref"]:
            ref = self._put("webee", "metrics_match", ulid(), {"snapshot_id": p["snapshot_id"], "preparation_id": row["preparation_id"],
                            "metrics_sha256": snap["metrics_sha256"], "target_id": row["target_id"], "account_ref": row["account_ref"]},
                            [snap["ref"], row["evidence_ref"]], row["trace_id"])
            self.db.execute("INSERT INTO metric_matches VALUES(?,?,?)", (p["snapshot_id"], row["preparation_id"], ref))
            self._event(row["trace_id"], "publish.metrics_match", [ref])
        return self._metric(p["snapshot_id"])

    def _strategy(self, p, rollback):
        item = self.get(p["strategy_ref"])
        if item["owner"] != "creative" or item["kind"] != "strategy":
            reject("INVALID_STRATEGY", "Strategy pointers require a fixed strategy ref")
        identity = parse_ref(item["ref"])[0]
        prior = self.db.execute("SELECT ref FROM strategy_pointers WHERE identity=?", (identity,)).fetchone()
        old = prior[0] if prior else None
        if rollback and (not old or parse_ref(old)[1] <= item["version"]):
            reject("INVALID_ROLLBACK", "Rollback must restore a prior version of the same strategy")
        if old == item["ref"]:
            return {"strategy_ref": old, "unchanged": True}
        if not rollback and old and parse_ref(old)[1] > item["version"]:
            reject("INVALID_ROLLBACK", "Use explicit strategy.rollback for an older version")
        operation = "strategy.rollback" if rollback else "strategy.activate"
        body = {"from_ref": old, "to_ref": item["ref"], "operation": operation, "published_work_actions": 0}
        ref = self._put("mgmt", "strategy_change", ulid(), body, [item["ref"]] + ([old] if old else []), item["trace_id"])
        self.db.execute("INSERT INTO strategy_pointers VALUES(?,?) ON CONFLICT(identity) DO UPDATE SET ref=excluded.ref", (identity, item["ref"]))
        self.db.execute("INSERT INTO changes(trace_id,op,from_ref,to_ref,change_ref) VALUES(?,?,?,?,?)", (item["trace_id"], operation, old, item["ref"], ref))
        self._event(item["trace_id"], operation, [ref])
        return {"strategy_ref": item["ref"], "change_ref": ref, "published_work_actions": 0}

    def trace(self, trace_id):
        token(trace_id)
        events = []
        for row in self.db.execute("SELECT * FROM content_events WHERE trace_id=? ORDER BY seq", (trace_id,)):
            event = dict(row)
            event["refs"] = json.loads(event.pop("refs_json"))
            event["metadata"] = json.loads(event.pop("metadata_json"))
            event.update(actor="synthetic-fixture-not-human", component="m0-content", **{"class": "internal"})
            events.append(event)
        artifacts = [self.get(row[0]) for row in self.db.execute("SELECT ref FROM artifacts WHERE trace_id=? ORDER BY rowid", (trace_id,))]
        approvals = []
        for row in self.db.execute("SELECT a.* FROM approvals a JOIN artifacts b ON a.ref=b.ref WHERE b.trace_id=?", (trace_id,)):
            try:
                self.verify_approval(row["ref"], row["subject_ref"], row["scope"], row["version_bound"])
                state = "fixture_approved"
            except ContractError as error:
                external = {"EXTERNAL_DEPENDENCY_REVOKED": "stale_by_revocation",
                            "EXTERNAL_DEPENDENCY_EXPIRED": "expired",
                            "CONTROL_APPROVAL_EXPIRED": "expired",
                            "EXTERNAL_DEPENDENCY_UNVERIFIED": "external_dependency_unverified"}
                if error.code != "APPROVAL_STALE" and error.code not in external:
                    raise
                state = external.get(error.code, "stale")
            approvals.append({"ref": row["ref"], "subject_ref": row["subject_ref"], "version_bound": row["version_bound"], "state": state,
                              "usable": state == "fixture_approved", "human_reviewed": False})
        return {"trace_id": trace_id, "events": events, "artifacts": artifacts, "approvals": approvals,
                "platform_calls": 0, "studio_integration": "not_connected", "synthetic": True}

    def project_events(self, trace_id):
        instance = self.db.execute("SELECT value FROM meta WHERE key='instance'").fetchone()[0]
        return [{"source": "m0-content:" + instance, "source_id": event["event_id"], "trace_id": trace_id,
                 "actor": event["actor"], "component": event["component"], "kind": event["kind"], "at": event["at"],
                 "refs": event["refs"], "data_class": "internal", "metadata": event["metadata"]}
                for event in self.trace(trace_id)["events"]]

    def capabilities(self):
        return {"account_ref": "webee/account/synthetic-only", "session_holder": "synthetic-local",
                "platform": {"submit": "unverified", "status_lookup": "unverified"},
                "import": {"metrics_import": True, "metrics_match": True}, "platform_calls": 0,
                "synthetic": True, "human_approval_authority": False}

    def invoke(self, operation, p=None):
        p = p or {}
        fields = {
            "publish.capabilities": set(), "artifact.get": {"ref"}, "trace.get": {"trace_id"}, "events.export": {"trace_id"},
            "publish.status": {"preparation_id", "as_of"}, "publish.metrics_get": {"snapshot_id"},
            "artifact.put": {"owner", "kind", "id", "body", "derived_from", "trace_id"},
            "approval.record": {"subject_ref", "scope", "fixture_grant", "refs"},
            "publish.prepare": {"inputs", "account_ref", "experiment", "preparation_id", "idempotency_key"},
            "fixture.submit_unknown": {"preparation_id", "at"},
            "publish.observe": {"preparation_id", "target_id", "observed_status", "observed_at", "source_type", "source_ref"},
            "publish.metrics_import": {"preparation_id", "target_id", "observed_at", "window", "source_type", "source_ref", "metrics", "missing_fields"},
            "publish.metrics_match": {"preparation_id", "snapshot_id"},
            "strategy.activate": {"strategy_ref"}, "strategy.rollback": {"strategy_ref"}}
        if not isinstance(p, dict) or (operation in fields and set(p) - fields[operation]):
            reject("INVALID_REQUEST", "Operation contains unsupported fields")
        read = {"publish.capabilities": lambda: self.capabilities(), "artifact.get": lambda: self.get(p["ref"]),
                "trace.get": lambda: self.trace(p["trace_id"]), "events.export": lambda: self.project_events(p["trace_id"]),
                "publish.status": lambda: self.status(p["preparation_id"], p.get("as_of")),
                "publish.metrics_get": lambda: self._metric(p["snapshot_id"])}
        if operation in read:
            return read[operation]()
        if operation == "platform.submit":
            reject("PLATFORM_UNVERIFIED", "No platform submission implementation exists; real calls remain zero")
        operations = {"approval.record": self._approve, "publish.prepare": self._prepare,
                      "fixture.submit_unknown": self._unknown, "publish.observe": self._observe,
                      "publish.metrics_import": self._metrics_import, "publish.metrics_match": self._metrics_match,
                      "strategy.activate": lambda v: self._strategy(v, False), "strategy.rollback": lambda v: self._strategy(v, True)}
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if operation == "artifact.put":
                parents = p.get("derived_from", [])
                for ref in parents:
                    self.closure(ref, current=True)
                ref = self._put(p["owner"], p["kind"], p["id"], p["body"], parents, p["trace_id"])
                self._event(p["trace_id"], operation, [ref])
                result = self.get(ref)
            elif operation in operations:
                result = operations[operation](p)
            else:
                reject("UNSUPPORTED_OPERATION", "Unsupported contract operation")
            self.db.execute("COMMIT")
            return result
        except BaseException:
            self.db.execute("ROLLBACK")
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("operation")
    parser.add_argument("--request", type=Path)
    args = parser.parse_args()
    service = None
    try:
        service = Contract(args.db, create=args.operation == "init")
        payload = json.loads(args.request.read_text(encoding="utf-8")) if args.request else {}
        data = {"database": str(service.path), "synthetic": True} if args.operation == "init" else service.invoke(args.operation, payload)
        print(json.dumps({"ok": True, "data": data}, ensure_ascii=False, indent=2, allow_nan=False))
        return 0
    except (ContractError, KeyError, ValueError, TypeError, OSError) as error:
        print(json.dumps({"ok": False, "error": {"code": getattr(error, "code", "INVALID_REQUEST"), "message": str(error)}}, ensure_ascii=False))
        return 2
    finally:
        if service:
            service.close()


if __name__ == "__main__":
    sys.exit(main())
