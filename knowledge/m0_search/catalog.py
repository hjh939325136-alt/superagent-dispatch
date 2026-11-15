"""Persistent lifecycle for explicitly created synthetic fixture catalogs only.

Each research source remains an independent SQLite database. An ATTACH transaction
updates one source and its context invalidations together, using DELETE journals.
"""
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import argparse
import json
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import uuid

from fixture_store import (DOMAINS, SCHEMA, build_domain, canonical_hash, canonical_json,
                           fixture_id, fixture_records, read_json, record_hash)
from signalhub_adapter import build_source, load_connection as load_signal

SOURCES = (*DOMAINS, "signalhub")
ALIASES = {"research-scatter": "scatter", "research-content": "content", "signalhub": "signal"}
MARKER = "m0-synthetic-research-catalog-v1"
OPERATOR = "m0-fixture-curator-v1"
APPLICATION_ID = 1396788034
SCENARIOS = ("revise_open", "draft_locked", "revoke_open", "delete_open", "restrict_open",
             "unreview_open", "expire_open", "mixed_update", "revoke_locked", "new_record")
SOURCE_EXTRA = """
CREATE TABLE source_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE batch_members(batch_id TEXT NOT NULL,ref_id TEXT NOT NULL,before_payload TEXT,
 after_version INTEGER NOT NULL,applied_head INTEGER NOT NULL,PRIMARY KEY(batch_id,ref_id));
CREATE TABLE revocations(seq INTEGER PRIMARY KEY,ref_id TEXT NOT NULL,through_version INTEGER NOT NULL,
 op TEXT NOT NULL,batch_id TEXT NOT NULL,at TEXT NOT NULL);
"""
CATALOG_SCHEMA = """
CREATE TABLE catalog_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE contexts(context_id TEXT PRIMARY KEY,payload TEXT NOT NULL,sha256 TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('current','stale_by_revocation')),created_at TEXT NOT NULL);
CREATE TABLE context_members(context_id TEXT NOT NULL,domain TEXT NOT NULL,ref_id TEXT NOT NULL,
 version INTEGER NOT NULL,ref TEXT NOT NULL,PRIMARY KEY(context_id,ref));
CREATE TABLE context_events(seq INTEGER PRIMARY KEY,context_id TEXT NOT NULL,op TEXT NOT NULL,at TEXT NOT NULL);
"""


class CatalogError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def fail(code, message):
    raise CatalogError(code, message)


def stamp():
    return datetime.now(timezone.utc).isoformat()


def operator(grant):
    if grant != OPERATOR:
        fail("FIXTURE_OPERATOR_REQUIRED", "Only the named local synthetic lifecycle grant is accepted")


def identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,120}", value):
        fail("INVALID_ID", "Explicit bounded identifier required")
    return value


def fixed_ref(domain, row):
    return f'{domain}/{row.get("ref_kind", "note")}/{row["id"]}@{row["version"]}#{record_hash(row)}'


def parse_ref(ref):
    found = re.fullmatch(r"(research-scatter|research-content|signalhub)/(note|reference_work)/([A-Za-z0-9:_-]+)@([1-9][0-9]*)#([a-f0-9]{64})", ref or "")
    if not found:
        fail("INVALID_REF", "Complete fixed source/version/hash reference required")
    domain, kind, ident, version, sha = found.groups()
    if kind != ("reference_work" if domain == "signalhub" else "note"):
        fail("INVALID_REF", "Reference kind does not match source")
    return domain, ident, int(version), sha


def fixture_changes(domain, scenario):
    """There is deliberately no arbitrary body, source path, or import JSON input."""
    if domain not in DOMAINS or scenario not in SCENARIOS:
        fail("INVALID_FIXTURE_BATCH", "Select one built-in research domain and synthetic scenario")
    slug = "sample-notes" if domain == "research-scatter" else "sample-clip-method"
    target = fixture_id(domain, slug if scenario in ("draft_locked", "revoke_locked") else "control-07")
    if scenario == "new_record":
        row = {"id": fixture_id(domain, "lifecycle-new"), "version": 1, "title": "合成新增剪辑返工记录",
               "body": "固定生命周期夹具，无业务输入。", "class": "internal", "source_ref": f"fixture:{domain}/lifecycle-new",
               "observed_at": "2026-11-15", "expires_at": None, "state": "current", "human_reviewed": True,
               "human_locked": False, "required_scope": "research:read"}
        return [{"id": row["id"], "op": "upsert", "new": row}]
    patches = {
        "revise_open": {"title": "合成剪辑返工更新", "body": "合成批次更新了镜头顺序。"},
        "draft_locked": {"body": "自动派生的合成建议，只能作为draft。"},
        "restrict_open": {"class": "restricted"}, "unreview_open": {"human_reviewed": False},
        "expire_open": {"expires_at": "2026-11-14", "state": "expired"},
        "mixed_update": {"body": "合成原子批次第一条更新"}}
    op = "revoke" if scenario.startswith("revoke") else "delete" if scenario == "delete_open" else "upsert"
    items = [{"id": target, "op": op, "patch": patches.get(scenario, {})}]
    if scenario == "mixed_update":
        items.append({"id": fixture_id(domain, "control-08"), "op": "upsert", "patch": {"body": "合成原子批次第二条更新"}})
    return sorted(items, key=lambda value: value["id"])


def initialize(root, *, operator_grant):
    operator(operator_grant)
    root = Path(root).absolute()
    if root.exists() or root.is_symlink() or not root.parent.is_dir():
        fail("CATALOG_EXISTS", "Choose a new catalog directory under an existing parent")
    instance = uuid.uuid4().hex
    # Build in a unique same-volume sibling. Failure cannot partially publish a catalog.
    with tempfile.TemporaryDirectory(prefix=".m0-catalog-build-", dir=root.parent) as staging:
        candidate = Path(staging) / "catalog"
        candidate.mkdir()
        for domain in DOMAINS:
            path = candidate / f"{domain}.sqlite3"
            build_domain(path, domain)
            with closing(sqlite3.connect(path)) as db, db:
                db.executescript(SOURCE_EXTRA)
                db.execute(f"PRAGMA application_id={APPLICATION_ID}")
                db.executemany("INSERT INTO source_meta VALUES(?,?)", (("schema", MARKER), ("instance", instance), ("domain", domain)))
                for ref_id, version in db.execute("SELECT ref_id,version FROM records"):
                    db.execute("INSERT INTO batch_members VALUES('synthetic-fixture-v1',?,?,?,1)", (ref_id, None, version))
                for ref_id, version, state in db.execute("SELECT ref_id,version,status FROM records WHERE status IN ('revoked','deleted')"):
                    db.execute("INSERT INTO revocations(ref_id,through_version,op,batch_id,at) VALUES(?,?,?,'synthetic-fixture-v1',?)",
                               (ref_id, version, "delete" if state == "deleted" else "revoke", stamp()))
        build_source(candidate / "signalhub.sqlite3")
        with closing(sqlite3.connect(candidate / "signalhub.sqlite3")) as db, db:
            db.execute(f"PRAGMA application_id={APPLICATION_ID}")
            db.executemany("INSERT INTO source_meta VALUES(?,?)", (("instance", instance), ("domain", "signalhub")))
        with closing(sqlite3.connect(candidate / "catalog.sqlite3")) as db, db:
            db.executescript(CATALOG_SCHEMA)
            db.execute(f"PRAGMA application_id={APPLICATION_ID}")
            db.executemany("INSERT INTO catalog_meta VALUES(?,?)", (("schema", MARKER), ("instance", instance)))
        marker = {"schema": MARKER, "instance": instance, "synthetic_only": True, "sources": list(SOURCES),
                  "authorization_source": "fixture://m0-research/lifecycle-specification-v1"}
        (candidate / "catalog.json").write_text(canonical_json(marker) + "\n", encoding="utf-8")
        Catalog(candidate).load(SOURCES)
        candidate.rename(root)
    return {"catalog": str(root), "synthetic_only": True, "sources": list(SOURCES), "instance": instance}


class Catalog:
    def __init__(self, root):
        self.root = Path(root).resolve()
        try:
            marker = read_json(self.root / "catalog.json")
            if marker.get("schema") != MARKER or marker.get("synthetic_only") is not True or marker.get("sources") != list(SOURCES):
                raise ValueError("not a fixture catalog")
            self.instance = identity(marker["instance"])
            for name in ("catalog", *SOURCES):
                path = self.root / (name + ".sqlite3")
                if not path.is_file() or path.is_symlink():
                    raise ValueError("source missing or redirected")
        except (OSError, ValueError, KeyError, TypeError):
            fail("NOT_SYNTHETIC_CATALOG", "This interface only opens its own explicitly initialized fixture catalog")

    @contextmanager
    def transaction(self, domains=(), *, write=False):
        if any(domain not in SOURCES for domain in domains):
            fail("INVALID_DOMAIN", "Unknown fixture source")
        uri = (self.root / "catalog.sqlite3").as_uri() + ("?mode=rw" if write else "?mode=ro")
        db = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=15)
        db.row_factory = sqlite3.Row
        try:
            if db.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID:
                fail("NOT_SYNTHETIC_CATALOG", "Catalog application marker mismatch")
            values = dict(db.execute("SELECT key,value FROM catalog_meta"))
            if values != {"schema": MARKER, "instance": self.instance}:
                fail("NOT_SYNTHETIC_CATALOG", "Catalog instance marker mismatch")
            for domain in dict.fromkeys(domains):
                alias = ALIASES[domain]
                source_uri = (self.root / f"{domain}.sqlite3").as_uri() + ("?mode=rw" if write and domain in DOMAINS else "?mode=ro")
                db.execute(f"ATTACH DATABASE ? AS {alias}", (source_uri,))
                meta = dict(db.execute(f"SELECT key,value FROM {alias}.source_meta"))
                expected_schema = "m0-signalhub-summary-fixture-v1" if domain == "signalhub" else MARKER
                if meta.get("schema") != expected_schema or meta.get("instance") != self.instance or meta.get("domain") != domain or db.execute(f"PRAGMA {alias}.application_id").fetchone()[0] != APPLICATION_ID:
                    fail("NOT_SYNTHETIC_CATALOG", "Source instance marker mismatch")
                if write and domain in DOMAINS and db.execute(f"PRAGMA {alias}.journal_mode").fetchone()[0].lower() != "delete":
                    fail("UNSAFE_JOURNAL_MODE", "Atomic attached writes require DELETE journals")
            if write and db.execute("PRAGMA main.journal_mode").fetchone()[0].lower() != "delete":
                fail("UNSAFE_JOURNAL_MODE", "Atomic attached writes require DELETE journals")
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            # Shared main snapshot prevents source writers committing between domain reads.
            db.execute("SELECT value FROM catalog_meta WHERE key='instance'").fetchone()
            yield db
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
        finally:
            db.close()

    def _revision(self, db, alias):
        seq = db.execute(f"SELECT COALESCE(MAX(seq),0) FROM {alias}.changes").fetchone()[0]
        batches = [tuple(row) for row in db.execute(f"SELECT batch_id,state,manifest_sha256 FROM {alias}.import_batches ORDER BY batch_id")]
        return "catalog-" + canonical_hash([self.instance, alias, seq, batches])

    def _load(self, db, domains):
        result = {}
        for domain in domains:
            alias = ALIASES[domain]
            if domain == "signalhub":
                result[domain] = load_signal(db, alias)
                continue
            records = []
            for row in db.execute(f"SELECT r.*,v.payload,v.sha256 AS version_hash FROM {alias}.records r JOIN {alias}.record_versions v ON r.ref_id=v.ref_id AND r.version=v.version ORDER BY r.ref_id"):
                payload = json.loads(row["payload"])
                if record_hash(payload) != row["sha256"] or row["sha256"] != row["version_hash"] or payload["state"] != row["status"]:
                    fail("HASH_MISMATCH", "Current source version hash or state mismatch")
                self._policy_matches(row, payload)
                records.append(payload)
            result[domain] = (records, self._revision(db, alias))
        return result

    def load(self, domains, connection=None):
        if connection is not None:
            return self._load(connection, domains)
        with self.transaction(domains) as db:
            return self._load(db, domains)

    def lookup(self, ref, domains, connection=None):
        try:
            domain, ident, version, expected = parse_ref(ref)
        except CatalogError:
            return None
        if domain not in domains:
            return None
        if connection is None:
            with self.transaction([domain]) as db:
                return self.lookup(ref, domains, db)
        db, alias = connection, ALIASES[domain]
        if domain == "signalhub":
            rows, revision = load_signal(db, alias)
            for row in rows:
                if fixed_ref(domain, row) == ref:
                    return row, row, revision
            return None
        head = db.execute(f"SELECT * FROM {alias}.records WHERE ref_id=?", (ident,)).fetchone()
        if not head or head["status"] in ("revoked", "deleted", "archived"):
            return None
        revoked = db.execute(f"SELECT COALESCE(MAX(through_version),0) FROM {alias}.revocations WHERE ref_id=?", (ident,)).fetchone()[0]
        if version <= revoked:
            return None
        raw = db.execute(f"SELECT payload,sha256 FROM {alias}.record_versions WHERE ref_id=? AND version=?", (ident, version)).fetchone()
        if not raw:
            return None
        row = json.loads(raw["payload"])
        if record_hash(row) != expected or raw["sha256"] != expected:
            return None
        current_raw = db.execute(f"SELECT payload FROM {alias}.record_versions WHERE ref_id=? AND version=?", (ident, head["version"])).fetchone()[0]
        current = json.loads(current_raw)
        if record_hash(current) != head["sha256"] or current["state"] != head["status"]:
            fail("HASH_MISMATCH", "Current source version hash or state mismatch")
        self._policy_matches(head, current)
        return row, current, self._revision(db, alias)

    def _head(self, db, alias, ident):
        raw = db.execute(f"SELECT r.*,v.payload FROM {alias}.records r JOIN {alias}.record_versions v ON r.ref_id=v.ref_id AND r.version=v.version WHERE r.ref_id=?", (ident,)).fetchone()
        if not raw:
            return None
        row = json.loads(raw["payload"])
        if record_hash(row) != raw["sha256"] or row["state"] != raw["status"]:
            fail("HASH_MISMATCH", "Current source version hash or state mismatch")
        self._policy_matches(raw, row)
        return row

    @staticmethod
    def _policy_matches(head, row):
        for key in ("class", "required_scope", "human_locked", "observed_at", "expires_at"):
            if head[key] != row[key]:
                fail("HASH_MISMATCH", "Versioned policy metadata differs from the current head")

    def _invalidate(self, db, domain, ident, at):
        contexts = db.execute("SELECT DISTINCT c.context_id FROM contexts c JOIN context_members m ON c.context_id=m.context_id WHERE m.domain=? AND m.ref_id=? AND c.status!='stale_by_revocation'", (domain, ident)).fetchall()
        for row in contexts:
            db.execute("UPDATE contexts SET status='stale_by_revocation' WHERE context_id=?", (row[0],))
            db.execute("INSERT INTO context_events(context_id,op,at) VALUES(?,'stale_by_revocation',?)", (row[0], at))

    def _append(self, db, domain, batch_id, before, desired, op, *, draft=False):
        alias, at = ALIASES[domain], stamp()
        ident = desired["id"]
        maximum = db.execute(f"SELECT COALESCE(MAX(version),0) FROM {alias}.record_versions WHERE ref_id=?", (ident,)).fetchone()[0]
        row = {**desired, "version": maximum + 1}
        if draft:
            row["human_reviewed"] = False
        sha = record_hash(row)
        review = "reviewed" if row["human_reviewed"] and not draft else "draft"
        db.execute(f"INSERT INTO {alias}.record_versions VALUES(?,?,?,?,?,?,?)", (ident, row["version"], sha, row["source_ref"], "synthetic-fixture-curator" if not draft else "synthetic-derived-agent", review, canonical_json(row)))
        if not draft:
            db.execute(f"INSERT INTO {alias}.records VALUES(?,?,?,?,?,?,?,?,?,?,?,0,?) ON CONFLICT(ref_id) DO UPDATE SET version=excluded.version,sha256=excluded.sha256,class=excluded.class,status=excluded.status,required_scope=excluded.required_scope,human_locked=excluded.human_locked,observed_at=excluded.observed_at,expires_at=excluded.expires_at,batch_id=excluded.batch_id",
                       (ident, domain, row["version"], sha, row["class"], row["state"], row["required_scope"], int(row["human_locked"]), row["observed_at"], row["expires_at"], "synthetic_fixture", batch_id))
        db.execute(f"INSERT INTO {alias}.batch_members VALUES(?,?,?,?,?)", (batch_id, ident, canonical_json(before) if before else None, row["version"], int(not draft)))
        db.execute(f"INSERT INTO {alias}.changes(ref_id,op,version,batch_id,at) VALUES(?,?,?,?,?)", (ident, op, row["version"], batch_id, at))
        if not draft and row["state"] in ("revoked", "deleted"):
            db.execute(f"INSERT INTO {alias}.revocations(ref_id,through_version,op,batch_id,at) VALUES(?,?,?,?,?)", (ident, row["version"], op, batch_id, at))
            self._invalidate(db, domain, ident, at)
        return {"ref": fixed_ref(domain, row), "version": row["version"], "review_state": review, "active": not draft}

    def apply_batch(self, domain, batch_id, scenario, *, operator_grant, expected_cursor=None, _after_write=None):
        operator(operator_grant)
        identity(batch_id)
        if batch_id.startswith("rollback:"):
            fail("INVALID_ID", "rollback: is reserved for generated rollback batches")
        operations = sorted(fixture_changes(domain, scenario), key=lambda entry: entry["id"])
        manifest = canonical_hash({"domain": domain, "scenario": scenario, "operations": operations})
        alias = ALIASES[domain]
        with self.transaction([domain], write=True) as db:
            prior = db.execute(f"SELECT * FROM {alias}.import_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if prior:
                if prior["manifest_sha256"] != manifest:
                    fail("BATCH_CONFLICT", "Batch id is already bound to another manifest")
                return {"batch_id": batch_id, "state": prior["state"], "deduplicated": True, "changes": []}
            cursor = db.execute(f"SELECT COALESCE(MAX(seq),0) FROM {alias}.changes").fetchone()[0]
            if expected_cursor is not None and (type(expected_cursor) is not int or expected_cursor != cursor):
                fail("CURSOR_CONFLICT", "Source changed after the supplied cursor")
            db.execute(f"INSERT INTO {alias}.import_batches VALUES(?,?,'staged',NULL,?,?)", (batch_id, "fixture:" + scenario, manifest, stamp()))
            changed = []
            for entry in operations:
                before = self._head(db, alias, entry["id"])
                if before is None and "new" not in entry:
                    fail("MISSING_FIXTURE_RECORD", "Expected synthetic source record is absent")
                desired = dict(entry.get("new", before or {}))
                desired.update(entry.get("patch", {}))
                op = entry["op"]
                if op in ("revoke", "delete"):
                    desired["state"] = "revoked" if op == "revoke" else "deleted"
                elif before and before["state"] in ("revoked", "deleted"):
                    fail("REVOCATION_REQUIRES_RESTORE", "A new import cannot revive a revoked/deleted record")
                compare = lambda value: {k: v for k, v in value.items() if k != "version"}
                if before and compare(before) == compare(desired):
                    continue
                draft = bool(before and before["human_locked"] and op == "upsert")
                if draft:
                    newest = db.execute(f"SELECT payload FROM {alias}.record_versions WHERE ref_id=? ORDER BY version DESC LIMIT 1", (entry["id"],)).fetchone()
                    if newest and compare(json.loads(newest[0])) == compare({**desired, "human_reviewed": False}):
                        continue
                changed.append(self._append(db, domain, batch_id, before, desired, op, draft=draft))
                if _after_write:
                    _after_write(len(changed))
            db.execute(f"UPDATE {alias}.import_batches SET state='applied' WHERE batch_id=?", (batch_id,))
            return {"batch_id": batch_id, "state": "applied", "deduplicated": False, "changes": changed}

    def rollback(self, domain, batch_id, *, operator_grant):
        operator(operator_grant)
        if domain not in DOMAINS:
            fail("INVALID_DOMAIN", "SignalHub is a read-only source")
        identity(batch_id)
        alias, rollback_id = ALIASES[domain], "rollback:" + batch_id
        with self.transaction([domain], write=True) as db:
            batch = db.execute(f"SELECT * FROM {alias}.import_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not batch:
                fail("BATCH_NOT_FOUND", "Unknown import batch")
            if batch["state"] == "rolled_back":
                return {"batch_id": batch_id, "state": "rolled_back", "deduplicated": True, "changes": []}
            members = db.execute(f"SELECT * FROM {alias}.batch_members WHERE batch_id=? ORDER BY ref_id", (batch_id,)).fetchall()
            for member in members:
                latest = db.execute(f"SELECT MAX(version) FROM {alias}.record_versions WHERE ref_id=?", (member["ref_id"],)).fetchone()[0]
                if latest != member["after_version"]:
                    fail("ROLLBACK_CONFLICT", "Later versions exist; rollback cannot overwrite subsequent work")
            db.execute(f"INSERT INTO {alias}.import_batches VALUES(?,'fixture:rollback','staged',?,?,?)", (rollback_id, batch_id, canonical_hash([batch_id, batch["manifest_sha256"]]), stamp()))
            changed = []
            for member in members:
                current = self._head(db, alias, member["ref_id"])
                desired = json.loads(member["before_payload"]) if member["before_payload"] else {**current, "state": "deleted"}
                changed.append(self._append(db, domain, rollback_id, current, desired, "restore", draft=not member["applied_head"]))
            db.execute(f"UPDATE {alias}.import_batches SET state='rolled_back' WHERE batch_id=?", (batch_id,))
            db.execute(f"UPDATE {alias}.import_batches SET state='applied' WHERE batch_id=?", (rollback_id,))
            return {"batch_id": batch_id, "state": "rolled_back", "rollback_batch": rollback_id, "deduplicated": False, "changes": changed}

    def changes(self, domain, since_cursor=0, *, operator_grant):
        operator(operator_grant)
        if domain not in DOMAINS or type(since_cursor) is not int or since_cursor < 0:
            fail("INVALID_CURSOR", "Research domain and nonnegative integer cursor required")
        alias = ALIASES[domain]
        with self.transaction([domain]) as db:
            cursor = db.execute(f"SELECT COALESCE(MAX(seq),0) FROM {alias}.changes").fetchone()[0]
            if since_cursor > cursor:
                fail("INVALID_CURSOR", "Cursor is ahead of this source instance")
            items = []
            for row in db.execute(f"SELECT c.*,v.payload,v.sha256,v.review_state,r.version AS head_version FROM {alias}.changes c JOIN {alias}.record_versions v ON c.ref_id=v.ref_id AND c.version=v.version JOIN {alias}.records r ON r.ref_id=c.ref_id WHERE c.seq>? ORDER BY c.seq", (since_cursor,)):
                payload = json.loads(row["payload"])
                if record_hash(payload) != row["sha256"]:
                    fail("HASH_MISMATCH", "Change references a modified version")
                items.append({key: row[key] for key in ("seq", "ref_id", "op", "version", "batch_id", "at", "review_state")} |
                             {"ref": fixed_ref(domain, payload), "active": row["version"] == row["head_version"], "tombstone": payload["state"] == "deleted"})
            return {"domain": domain, "source_instance": self.instance, "changes": items, "cursor": cursor, "synthetic_only": True}

    def assemble(self, request, grant_id):
        from search import authorize
        domains, _, _, _ = authorize(request, grant_id, "refs")
        refs = request["refs"]
        if not isinstance(refs, list) or not 1 <= len(refs) <= 100 or any(not isinstance(ref, str) for ref in refs) or len(refs) != len(set(refs)):
            fail("INVALID_REFS", "Context requires distinct explicit fixed references")
        with self.transaction(domains, write=True) as db:
            return self._assemble(db, request, grant_id)

    def _assemble(self, db, request, grant_id, *, effective_as_of=None):
        """Caller owns the transaction; the workflow adds its receipt atomically.

        Existing callers keep the original as_of semantics. Workflow callers
        supply actual current time for usability while keeping the requested
        created_as_of in the immutable context manifest.
        """
        from search import authorize, get
        domains, _, _, _ = authorize(request, grant_id, "refs")
        refs = request["refs"]
        if not isinstance(refs, list) or not 1 <= len(refs) <= 100 or any(not isinstance(ref, str) for ref in refs) or len(refs) != len(set(refs)):
            fail("INVALID_REFS", "Context requires distinct explicit fixed references")
        if not db.in_transaction:
            fail("TRANSACTION_REQUIRED", "Context assembly requires the caller transaction")
        items = []
        for ref in refs:
            single = {k: v for k, v in request.items() if k != "refs"} | {"ref": ref}
            if effective_as_of is not None:
                # Fixed created_as_of still bounds selected source visibility;
                # current authorization/expiry is checked separately below.
                get(single, grant_id, catalog=self, _connection=db)
                single["as_of"] = effective_as_of
            item = get(single, grant_id, catalog=self, _connection=db)["result"]
            if item["status"] == "expired":
                fail("CONTEXT_EXPIRED", "Expired source cannot assemble an active context")
            items.append(item)
        payload = {"schema": "m0-synthetic-context-v1", "caller": request["caller"], "domains": domains,
                   "refs": sorted(refs), "created_as_of": request["as_of"]}
        sha = canonical_hash(payload)
        context_id = "ctx_" + sha[:26]
        prior = db.execute("SELECT status FROM contexts WHERE context_id=?", (context_id,)).fetchone()
        if prior and prior["status"] == "stale_by_revocation":
            fail("CONTEXT_REVOKED", "Previously revoked contexts cannot be revived")
        if not prior:
            db.execute("INSERT INTO contexts VALUES(?,?,?,'current',?)", (context_id, canonical_json(payload), sha, stamp()))
            for ref in refs:
                domain, ident, version, _ = parse_ref(ref)
                db.execute("INSERT INTO context_members VALUES(?,?,?,?,?)", (context_id, domain, ident, version, ref))
            db.execute("INSERT INTO context_events(context_id,op,at) VALUES(?,'assembled',?)", (context_id, stamp()))
        return {"context_id": context_id, "sha256": sha, "status": "current", "usable": True, "items": items, "synthetic_only": True}

    def context_get(self, request, grant_id, *, reassemble=False):
        from search import authorize, get, SearchError
        domains, _, _, _ = authorize(request, grant_id, "context_id")
        identity(request["context_id"])
        with self.transaction(domains) as db:
            row = db.execute("SELECT * FROM contexts WHERE context_id=?", (request["context_id"],)).fetchone()
            if not row:
                fail("CONTEXT_NOT_FOUND", "Context is not visible")
            payload = json.loads(row["payload"])
            if canonical_hash(payload) != row["sha256"]:
                fail("HASH_MISMATCH", "Context reference manifest was modified")
            if payload["caller"] != request["caller"] or not set(payload["domains"]) <= set(domains):
                fail("ACCESS_DENIED", "Context caller or domain scope does not match")
            if row["status"] == "stale_by_revocation":
                if reassemble:
                    fail("CONTEXT_REVOKED", "Context references a record revoked after assembly")
                return {"context_id": row["context_id"], "status": "stale_by_revocation", "usable": False, "items": []}
            items = []
            for ref in payload["refs"]:
                single = {k: v for k, v in request.items() if k != "context_id"} | {"ref": ref}
                try:
                    items.append(get(single, grant_id, catalog=self, _connection=db)["result"])
                except SearchError:
                    fail("ACCESS_DENIED", "Context sources are no longer visible under current authorization")
            expired = any(item["status"] == "expired" for item in items)
            if expired and reassemble:
                fail("CONTEXT_EXPIRED", "Context sources are expired")
            return {"context_id": row["context_id"], "sha256": row["sha256"], "status": "expired" if expired else "current",
                    "usable": not expired, "items": [] if expired else items, "synthetic_only": True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("fixture.init", "fixture.import", "fixture.rollback", "fixture.changes"))
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--operator", required=True)
    parser.add_argument("--domain", choices=DOMAINS)
    parser.add_argument("--batch")
    parser.add_argument("--scenario", choices=SCENARIOS)
    parser.add_argument("--since", type=int, default=0)
    parser.add_argument("--expected-cursor", type=int)
    args = parser.parse_args(argv)
    try:
        if args.operation == "fixture.init":
            result = initialize(args.catalog, operator_grant=args.operator)
        else:
            catalog = Catalog(args.catalog)
            if args.operation == "fixture.import":
                result = catalog.apply_batch(args.domain, args.batch, args.scenario, operator_grant=args.operator, expected_cursor=args.expected_cursor)
            elif args.operation == "fixture.rollback":
                result = catalog.rollback(args.domain, args.batch, operator_grant=args.operator)
            else:
                result = catalog.changes(args.domain, args.since, operator_grant=args.operator)
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
        return 0
    except (CatalogError, OSError, sqlite3.DatabaseError, ValueError, TypeError) as error:
        print(json.dumps({"error": {"code": getattr(error, "code", "CATALOG_UNAVAILABLE"), "message": str(error)}}, ensure_ascii=True))
        return 2


if __name__ == "__main__":
    sys.exit(main())
