"""Build two independent SQLite copies of the approved synthetic fixtures."""

from contextlib import closing
from hashlib import sha256
import json
from pathlib import Path
import sqlite3


ROOT = Path(__file__).resolve().parent
DOMAINS = ("research-scatter", "research-content")
ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
SCHEMA = """
CREATE TABLE records (
    ref_id TEXT PRIMARY KEY, domain TEXT NOT NULL CHECK(domain IN ('research-scatter', 'research-content')),
    version INTEGER NOT NULL, sha256 TEXT NOT NULL,
    class TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('current', 'expired', 'revoked', 'archived', 'deleted')),
    required_scope TEXT NOT NULL,
    human_locked INTEGER NOT NULL, observed_at TEXT NOT NULL, expires_at TEXT,
    license TEXT NOT NULL, train_allowed INTEGER NOT NULL DEFAULT 0, batch_id TEXT NOT NULL
);
CREATE TABLE record_versions (
    ref_id TEXT NOT NULL, version INTEGER NOT NULL, sha256 TEXT NOT NULL,
    body_ref TEXT NOT NULL, author TEXT NOT NULL,
    review_state TEXT NOT NULL CHECK(review_state IN ('draft', 'reviewed', 'rejected')),
    payload TEXT NOT NULL, PRIMARY KEY (ref_id, version)
);
CREATE TABLE changes (
    seq INTEGER PRIMARY KEY, ref_id TEXT NOT NULL,
    op TEXT NOT NULL CHECK(op IN ('upsert', 'revoke', 'delete', 'restore')),
    version INTEGER NOT NULL, batch_id TEXT NOT NULL, at TEXT NOT NULL
);
CREATE TABLE import_batches (
    batch_id TEXT PRIMARY KEY, source TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('staged', 'applied', 'rolled_back')),
    parent_batch TEXT, manifest_sha256 TEXT NOT NULL, at TEXT NOT NULL
);
"""


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonical_hash(value):
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def fixture_id(domain, slug):
    """Create a stable ULID-form identifier for a synthetic fixture."""
    number = int(canonical_hash([domain, slug])[:32], 16)
    digits = []
    for _ in range(26):
        number, remainder = divmod(number, 32)
        digits.append(ENCODING[remainder])
    return "".join(reversed(digits))


def record_hash(record):
    return canonical_hash({key: value for key, value in record.items() if key != "state"})


def fixture_records(domain):
    if domain not in DOMAINS:
        raise ValueError("Unknown fixture domain")
    seeds = read_json(ROOT / "fixtures" / f"{domain}.json")
    while len(seeds) < 30:
        slug = f"control-{len(seeds):02d}"
        seeds.append({"id": slug, "version": 1, "title": "合成对照 " + slug,
                      "body": "用于两库容量和稳定标识验证的合成记录。", "class": "internal",
                      "source_ref": f"fixture:{domain}/{slug}", "observed_at": "2026-10-01",
                      "expires_at": None, "state": "current", "human_reviewed": True})
    records = [{"required_scope": "research:read", "human_locked": False,
                **row, "id": fixture_id(domain, row["id"])} for row in seeds]
    return sorted(records, key=lambda row: row["id"])


def build_domain(path, domain):
    records = fixture_records(domain)
    batch = "synthetic-fixture-v1"
    stamp = "2026-11-15T00:00:00+08:00"
    with path.open("xb"):
        pass
    with closing(sqlite3.connect(path)) as db:
        db.executescript(SCHEMA)
        db.execute("INSERT INTO import_batches VALUES (?, ?, 'applied', NULL, ?, ?)",
                   (batch, "synthetic-fixtures", canonical_hash(records), stamp))
        for row in records:
            ident, version = row["id"], row["version"]
            digest = record_hash(row)
            db.execute("INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
                       (ident, domain, version, digest, row["class"], row["state"], row["required_scope"],
                        row["human_locked"], row["observed_at"], row["expires_at"], "synthetic_fixture", batch))
            db.execute("INSERT INTO record_versions VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (ident, version, digest, row["source_ref"], "synthetic-fixture",
                        "reviewed" if row["human_reviewed"] else "draft", canonical_json(row)))
            operation = {"revoked": "revoke", "deleted": "delete"}.get(row["state"], "upsert")
            db.execute("INSERT INTO changes(ref_id, op, version, batch_id, at) VALUES (?, ?, ?, ?, ?)",
                       (ident, operation, version, batch, stamp))
        db.commit()


def load_domain(root, domain):
    uri = (root / f"{domain}.sqlite3").as_uri() + "?mode=ro&immutable=1"
    with closing(sqlite3.connect(uri, uri=True)) as db:
        records = []
        for payload, digest, version_digest in db.execute(
            "SELECT v.payload, r.sha256, v.sha256 FROM records AS r JOIN record_versions AS v "
            "ON r.ref_id = v.ref_id AND r.version = v.version ORDER BY r.ref_id"
        ):
            row = json.loads(payload)
            if not record_hash(row) == digest == version_digest:
                raise sqlite3.DatabaseError("Fixture version hash mismatch")
            records.append(row)
        revision = db.execute("SELECT manifest_sha256 FROM import_batches").fetchone()[0]
    return records, "fixture-" + revision
