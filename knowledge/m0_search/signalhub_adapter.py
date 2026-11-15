"""Fixed-schema, synthetic-only SignalHub reference/summary query adapter."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3

from fixture_store import ROOT, canonical_hash, canonical_json, read_json, record_hash


def fixture_records():
    rows = read_json(ROOT / "fixtures/signalhub.json")
    return sorted([{"ref_kind": "reference_work", "required_scope": "signalhub:refs", "human_locked": False,
                    **row} for row in rows], key=lambda row: row["id"])


def build_source(path):
    with Path(path).open("xb"):
        pass
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript("""CREATE TABLE source_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE reference_works(work_id TEXT PRIMARY KEY,version INTEGER NOT NULL,
          sha256 TEXT NOT NULL,payload TEXT NOT NULL);""")
        db.execute("INSERT INTO source_meta VALUES('schema','m0-signalhub-summary-fixture-v1')")
        rows = fixture_records()
        for row in rows:
            db.execute("INSERT INTO reference_works VALUES(?,?,?,?)", (row["id"], row["version"], record_hash(row), canonical_json(row)))
        db.execute("INSERT INTO source_meta VALUES('revision',?)", (canonical_hash(rows),))


def load_connection(db, alias="main"):
    if alias not in ("main", "signal"):
        raise ValueError("invalid fixed source alias")
    marker = db.execute(f"SELECT value FROM {alias}.source_meta WHERE key='schema'").fetchone()
    if not marker or marker[0] != "m0-signalhub-summary-fixture-v1":
        raise sqlite3.DatabaseError("Not a synthetic SignalHub summary source")
    rows = []
    for ident, version, expected, payload in db.execute(f"SELECT * FROM {alias}.reference_works ORDER BY work_id"):
        row = json.loads(payload)
        if row["id"] != ident or row["version"] != version or record_hash(row) != expected:
            raise sqlite3.DatabaseError("SignalHub fixture hash mismatch")
        rows.append(row)
    return rows, "signalhub-fixture-" + db.execute(f"SELECT value FROM {alias}.source_meta WHERE key='revision'").fetchone()[0]


def load_source(root):
    with closing(sqlite3.connect((Path(root) / "signalhub.sqlite3").resolve().as_uri() + "?mode=ro", uri=True)) as db:
        return load_connection(db)
