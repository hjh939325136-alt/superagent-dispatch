"""Export and restore a settled dispatch ledger and its referenced artifacts."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile

from .artifacts import ArtifactError, ArtifactStore
from .canonical import sha256_ref
from .executor import receipt_bytes


TABLES = {
    "tasks": "rowid",
    "leases": "task_id,fence",
    "events": "seq",
    "evidence": "evidence_id",
    "late_results": "id",
}
FORMAT = "sa_dispatch.bundle.v1"


def ledger(path):
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        return {table: [dict(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY {order}")]
                for table, order in TABLES.items()}


def portable(records):
    blocked = [task["task_id"] for task in records["tasks"]
               if task["access"] == "local_only" or
               (task["access"] == "exportable" and task["egress"] == "none")]
    blocked.extend(item["task_id"] for item in records["evidence"] if item["access"] == "local_only")
    if blocked:
        raise ArtifactError("E_EXPORT_BLOCKED", "restricted tasks: " + ", ".join(sorted(set(blocked))))
    if any(row["status"] not in {"succeeded", "no_new", "failed", "cancelled"} for row in records["tasks"]):
        raise ArtifactError("E_EXPORT_BLOCKED", "settle queued, leased or unknown tasks before exporting")
    if records["late_results"]:
        raise ArtifactError("E_EXPORT_BLOCKED", "bundles with late receipts require local review")
    return sorted({item["artifact_hash"] for item in records["evidence"] if item["artifact_hash"]})


def export_bundle(config, destination):
    path = Path(destination).resolve()
    if path.exists():
        raise ArtifactError("E_BUNDLE_EXISTS", "choose a new bundle directory")
    records = ledger(config.db_path)
    hashes = portable(records)
    source = ArtifactStore(config.artifact_root)
    with tempfile.TemporaryDirectory(prefix=".dispatch-", dir=path.parent) as work:
        staging = Path(work) / "bundle"
        staging.mkdir()
        target = ArtifactStore(staging / "artifacts")
        files = {}
        for table, items in records.items():
            name = table + ".jsonl"
            content = b"".join(receipt_bytes(item) + b"\n" for item in items)
            (staging / name).write_bytes(content)
            files[name] = sha256_ref(content)
        for ref in hashes:
            target.put(source.get(ref))
        manifest = {"schema": FORMAT, "files": files, "artifacts": hashes}
        (staging / "manifest.json").write_bytes(receipt_bytes(manifest))
        staging.rename(path)
    return manifest


def read_bundle(path):
    path = Path(path).resolve()
    try:
        manifest_path = path / "manifest.json"
        if not manifest_path.resolve().is_relative_to(path):
            raise ValueError("manifest location escapes the bundle")
        manifest = json.loads(manifest_path.read_bytes())
        expected = {name + ".jsonl" for name in TABLES}
        if manifest["schema"] != FORMAT or set(manifest["files"]) != expected:
            raise ValueError("unsupported bundle layout")
        records = {}
        for table in TABLES:
            name = table + ".jsonl"
            file = path / name
            if not file.resolve().is_relative_to(path):
                raise ValueError("record location escapes the bundle")
            content = file.read_bytes()
            if sha256_ref(content) != manifest["files"][name]:
                raise ArtifactError("E_HASH_MISMATCH", "bundle record hash mismatch")
            records[table] = [json.loads(line) for line in content.splitlines()]
        if portable(records) != manifest["artifacts"]:
            raise ValueError("unexpected artifact inventory")
        store = ArtifactStore(path / "artifacts")
        if not store.root.is_relative_to(path):
            raise ValueError("artifact root escapes the bundle")
        blobs = {ref: store.get(ref) for ref in manifest["artifacts"]}
        return manifest, records, blobs
    except ArtifactError:
        raise
    except (KeyError, TypeError, OSError, ValueError) as exc:
        raise ArtifactError("E_BUNDLE_INVALID", "bundle cannot be read") from exc


def verify_bundle(path):
    manifest, records, _ = read_bundle(path)
    return {"tasks": len(records["tasks"]), "artifacts": len(manifest["artifacts"])}


def import_bundle(config, path, store):
    manifest, records, blobs = read_bundle(path)
    target = ArtifactStore(config.artifact_root)
    # Artifacts are immutable and may be staged before the ledger transaction.
    for blob in blobs.values():
        target.put(blob)
    store.import_snapshot({**records, "devices": []})
    return {"tasks": len(records["tasks"]), "artifacts": len(manifest["artifacts"])}
