"""M0 card 5. Standard-library-only; never scans recursively or modifies input files."""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import errno
import hashlib
import json
import ntpath
import os
from pathlib import Path
import re
import stat
import sys

VERSION = "0.1.0"
REPARSE_POINT = 0x400


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def rows(value):
    result = value.get("items", []) if isinstance(value, dict) else value
    if result is None:
        return []
    if not isinstance(result, list) or any(not isinstance(x, dict) for x in result):
        raise ValueError("table must be an array of objects or an items envelope")
    return result


def project(document):
    """Preserve source fields under source_record; do not invent missing authority."""
    result = {"schemaVersion": "m0-management-projection/1", "scanner_version": VERSION,
              "class": "internal", "authority": "derived_read_only", "actions_taken": 0,
              "source_sha256": digest(document), "coverage": copy.deepcopy(document.get("coverage", {}))}
    tables = {name: [] for name in ("projects", "conversations", "artifacts", "environments", "hosts", "copies")}
    for record in rows(document.get("projects", [])):
        tables["projects"].append({"id": record.get("projectId", record.get("id")),
            "root_ref": record.get("root_ref", record.get("path")),
            "owner_role": record.get("owner_role"), "business_authority": record.get("business_authority", []),
            "status": record.get("status", "unknown"), "source_record": copy.deepcopy(record)})
    for record in rows(document.get("conversations", document.get("threads", []))):
        tables["conversations"].append({"id": record.get("id"), "host": record.get("host", record.get("kind", "unknown")),
            "host_id": record.get("hostId", record.get("host_id")), "project_id": record.get("project_id", record.get("projectId")),
            "purpose": record.get("purpose", record.get("summary")),
            "last_seen": record.get("last_seen", record.get("observedAt", record.get("updatedAt"))),
            "handoff_ref": record.get("handoff_ref"), "source_record": copy.deepcopy(record)})
    for record in rows(document.get("artifacts", [])):
        project_ids = record.get("projectIds", [])
        tables["artifacts"].append({"ref": record.get("ref"), "id": record.get("id"),
            "project_id": record.get("project_id", project_ids[0] if len(project_ids) == 1 else None), "project_ids": project_ids,
            "kind": record.get("kind", record.get("role", "unknown")), "version": record.get("version"),
            "author": record.get("author"), "supersedes": record.get("supersedes"),
            "paths": artifact_paths(record), "source_record": copy.deepcopy(record)})
    for record in rows(document.get("environments", [])):
        tables["environments"].append({"id": record.get("id"), "host": record.get("host", record.get("hostId")),
            "runtime": record.get("runtime", record.get("name")), "deps_lock_sha256": record.get("deps_lock_sha256"),
            "verified_at": record.get("verified_at", record.get("checkedAt")), "source_record": copy.deepcopy(record)})
    for record in rows(document.get("hosts", [])):
        tables["hosts"].append({"id": record.get("id", record.get("hostId")), "source_record": copy.deepcopy(record)})
    # Supplied copies are carried as unverified claims. A preview replaces them with observations.
    for record in rows(document.get("copies", [])):
        tables["copies"].append({"sha256": record.get("sha256"), "paths": record.get("paths", []),
            "canonical_path": record.get("canonical_path"), "verification": "supplied_unverified"})
    for name, table in tables.items():
        tables[name] = sorted(table, key=canonical)
    result["tables"] = tables
    result["unknown_fields"] = [{"table": name, "row": i, "field": key}
        for name, table in tables.items() for i, row in enumerate(table)
        for key, value in row.items() if value is None]
    result["deterministic_sha256"] = digest(result)
    return result


def artifact_paths(record):
    found = []
    for field in ("path", "absolutePath"):
        if record.get(field):
            found.append(record[field])
    found.extend(record.get("paths", []))
    return sorted(set(found))


def is_nonlocal(path):
    if path.startswith(("\\\\", "//")):
        return True
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", path):
        return True
    return bool(re.match(r"^[^/\\:]+:/", path) and not re.match(r"^[A-Za-z]:[/\\]", path))


def lexical(path, aliases):
    if not isinstance(path, str) or not path:
        raise ValueError("paths must be nonempty strings")
    if is_nonlocal(path):
        return None, "not_local"
    # Alias mappings are explicit input claims, never guesses about mount configuration.
    for mapping in sorted(aliases, key=lambda a: -len(a["from"])):
        prefix = mapping["from"].rstrip("/\\")
        if path == prefix or path.startswith(prefix + "/") or path.startswith(prefix + "\\"):
            suffix = path[len(prefix):].lstrip("/\\")
            path = os.path.join(mapping["to"], *re.split(r"[/\\]", suffix))
            break
    if is_nonlocal(path):
        return None, "not_local"
    if os.name != "nt" and ntpath.isabs(path) and not path.startswith("/"):
        return None, "unmapped_host_path"
    if not os.path.isabs(path):
        raise ValueError("all local roots and file paths must be absolute")
    return os.path.normcase(os.path.abspath(path)), None


def within(path, roots):
    for root in roots:
        try:
            if os.path.commonpath([path, root]) == root:
                return True
        except ValueError:
            continue
    return False


def safe_lstat(path):
    """Inspect each ancestor without traversing a known symlink/junction."""
    parts = [Path(path)]
    parts.extend(Path(path).parents)
    final = None
    for part in reversed(parts):
        observed = os.lstat(part)
        if stat.S_ISLNK(observed.st_mode) or getattr(observed, "st_file_attributes", 0) & REPARSE_POINT:
            return observed, "symlink_or_junction"
        final = observed
    return final, None


def identity(observed):
    # CPython stat uses Windows volume serial + file index, POSIX dev + inode.
    return f"vol:{observed.st_dev}:file:{observed.st_ino}"


def stamp(seconds):
    return dt.datetime.fromtimestamp(seconds, dt.timezone.utc).isoformat()


def open_nofollow(path):
    if os.name != "nt":
        return os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    import ctypes
    from ctypes import wintypes
    import msvcrt
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                                  wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    # OPEN_REPARSE_POINT prevents following a replaced final symlink.
    handle = kernel.CreateFileW(path, 0x80000000, 7, None, 3, 0x00200000, None)
    if handle == wintypes.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        return msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    except Exception:
        kernel.CloseHandle(handle)
        raise


def file_hash(path, observed):
    fd = open_nofollow(path)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        if (identity(before), before.st_size, before.st_mtime_ns) != (identity(observed), observed.st_size, observed.st_mtime_ns) or not stat.S_ISREG(before.st_mode):
            raise RuntimeError("file_changed")
        value = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
        after = os.fstat(stream.fileno())
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
            raise RuntimeError("file_changed")
    return value.hexdigest()


def error_reason(error):
    if getattr(error, "winerror", None) in (32, 33):
        return "in_use_unknown"
    if isinstance(error, PermissionError) or getattr(error, "winerror", None) == 5:
        return "ACCESS_DENIED"
    if getattr(error, "errno", None) == errno.ENAMETOOLONG or getattr(error, "winerror", None) == 206:
        return "PATH_TOO_LONG"
    if isinstance(error, FileNotFoundError):
        return "missing_path"
    return "io_error"


def preview(document, generated_at=None):
    projected = project(document)
    config = document.get("cleanup", {})
    aliases = config.get("path_aliases", [])
    roots = []
    for raw in config.get("roots", []):
        root, issue = lexical(raw, aliases)
        if issue:
            raise ValueError("cleanup roots must be local paths")
        roots.append(root)
    roots = sorted(set(roots))
    if not roots:
        raise ValueError("cleanup.roots requires at least one explicit local absolute root")
    requested = rows(config.get("files", []))
    normalized = []
    for record in requested:
        path, issue = lexical(record.get("path"), aliases)
        normalized.append((record, path, issue))
    if len({r[0]["path"] for r in normalized}) != len(normalized):
        raise ValueError("duplicate input path; use path_aliases for alternate spellings")
    references = list(config.get("references", []))
    for artifact in projected["tables"]["artifacts"]:
        for path in artifact["paths"]:
            references.append({"path": path, "ref": artifact["ref"] or f"artifact:{artifact['id']}"})
    reference_paths, reference_ids, reference_directories = {}, {}, {}
    for reference in references:
        path, issue = lexical(reference["path"], aliases)
        if issue:
            continue
        reference_paths.setdefault(path, set()).add(reference["ref"])
        if not within(path, roots):
            continue
        try:
            info, link = safe_lstat(path)
            if not link and stat.S_ISREG(info.st_mode):
                reference_ids.setdefault(identity(info), set()).add(reference["ref"])
            elif not link and stat.S_ISDIR(info.st_mode):
                reference_directories.setdefault(path, set()).add(reference["ref"])
        except OSError:
            pass
    items = []
    for record, path, issue in normalized:
        item = {"path": record["path"], "normalized_path": path, "file_id": None, "bytes": 0,
                "verdict": "hold", "reason": None, "referenced_by": [], "duplicate_of": None,
                "last_write": None, "last_access": None, "sha256": None, "nlink": None,
                "usage_evidence": record.get("usage_evidence"), "unique_bytes": 0}
        items.append(item)
        if issue:
            item.update(verdict="not_local", reason=issue)
            continue
        item["referenced_by"] = sorted(reference_paths.get(path, set()))
        for directory, refs in reference_directories.items():
            if within(path, [directory]):
                item["referenced_by"] = sorted(set(item["referenced_by"]) | refs)
        if not within(path, roots):
            item.update(verdict="not_local", reason="outside_allowlist")
            continue
        if record.get("volume_state") == "offline":
            item["reason"] = "offline_volume"
            continue
        try:
            info, link = safe_lstat(path)
            if link:
                item["reason"] = link
                continue
            if not stat.S_ISREG(info.st_mode):
                item["reason"] = "directory_not_traversed" if stat.S_ISDIR(info.st_mode) else "unsupported_file_type"
                continue
            item.update(file_id=identity(info), bytes=info.st_size, nlink=info.st_nlink,
                        last_write=stamp(info.st_mtime), last_access=stamp(info.st_atime))
            item["referenced_by"] = sorted(set(item["referenced_by"]) | reference_ids.get(item["file_id"], set()))
            item["sha256"] = file_hash(path, info)
        except OSError as error:
            item["reason"] = error_reason(error)
            continue
        except RuntimeError:
            item["reason"] = "file_changed"
            continue
        if item["referenced_by"]:
            item.update(verdict="keep", reason="referenced")
        elif record.get("original") or record.get("kind") in {"original", "source", "model_weights", "authoritative", "final"}:
            item.update(verdict="keep", reason="original_or_authoritative")
        elif info.st_nlink > 1:
            item["reason"] = "hardlink"
        elif record.get("dependency_state") == "shared":
            item["reason"] = "shared_dependency"
        elif record.get("dependency_state") not in {"none", "verified_unused"}:
            item["reason"] = "dependency_unknown"
        elif record.get("usage_state") != "idle" or not record.get("usage_evidence"):
            item["reason"] = "in_use_unknown"
        elif record.get("purpose") not in {"temporary", "cache"}:
            item["reason"] = "purpose_unknown"
        elif record.get("rebuildable") is not True:
            item["reason"] = "rebuildability_unknown"
        elif config.get("reference_scan_complete") is not True:
            item["reason"] = "reference_scope_unknown"
        else:
            item.update(verdict="candidate", reason="orphan")
    items.sort(key=lambda i: i["path"])
    seen = set()
    for item in items:
        file_id = item["file_id"]
        if file_id is not None and file_id not in seen:
            item["unique_bytes"] = item["bytes"]
            seen.add(file_id)
    hashed = {}
    for item in items:
        if item["sha256"]:
            hashed.setdefault(item["sha256"], []).append(item)
    copies = []
    for sha256, group in sorted(hashed.items()):
        canonical_item = sorted(group, key=lambda i: (i["verdict"] != "keep", i["path"]))[0]
        # Aliases and hardlinks are retained in paths but are not duplicate physical copies.
        count = len({x["file_id"] for x in group})
        copies.append({"sha256": sha256, "paths": [x["path"] for x in group],
                       "canonical_path": canonical_item["path"], "physical_copies": count,
                       "duplicate_bytes": sum({x["file_id"]: x["bytes"] for x in group}.values()) - canonical_item["bytes"]})
        for item in group:
            if item is not canonical_item:
                item["duplicate_of"] = canonical_item["path"]
    candidate_ids = {i["file_id"]: i["bytes"] for i in items if i["verdict"] == "candidate"}
    # A contradictory alias claim cannot downgrade another observation of the same object.
    protected_ids = {i["file_id"] for i in items if i["verdict"] in {"keep", "hold"} and i["file_id"]}
    for item in items:
        if item["verdict"] == "candidate" and item["file_id"] in protected_ids:
            item.update(verdict="hold", reason="conflicting_alias_evidence")
            candidate_ids.pop(item["file_id"], None)
    totals = {name: sum(i["verdict"] == name for i in items) for name in ("candidate", "hold", "keep", "not_local")}
    totals.update(candidate_bytes=sum(candidate_ids.values()), unique_bytes=sum(i["unique_bytes"] for i in items),
                  candidate_objects=len(candidate_ids), logical_bytes=sum(i["bytes"] for i in items),
                  duplicate_bytes=sum(c["duplicate_bytes"] for c in copies))
    result = {"preview_id": None, "scanner_version": VERSION, "roots": roots,
              "generated_at": generated_at or now(), "deterministic_sha256": None, "class": "internal",
              "retention_days": 180, "input_sha256": digest(document), "items": items, "copies": copies,
              "totals": totals, "actions_taken": 0, "action_taken": 0,
              "limitations": ["explicit paths only; no recursive or business-store scan",
                  "usage and reference completeness are supplied attestations, not OS-wide discovery",
                  "concurrent adversarial ancestor replacement is outside this preview's assurance",
                  "unique_bytes measures logical file sizes by identity, not allocated disk blocks",
                  "last_access is observational and excluded from stable hash because reading may change atime"]}
    stable = copy.deepcopy(result)
    for key in ("preview_id", "generated_at", "deterministic_sha256"):
        stable.pop(key)
    for item in stable["items"]:
        item.pop("last_access")
    result["deterministic_sha256"] = digest(stable)
    result["preview_id"] = "cp_" + result["deterministic_sha256"][:24]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("project", "preview", "bundle"))
    parser.add_argument("--input", required=True, help="explicit UTF-8 JSON; outputs JSON to stdout")
    args = parser.parse_args()
    try:
        with open(args.input, encoding="utf-8-sig") as stream:
            document = json.load(stream)
        if args.command == "project":
            result = project(document)
        elif args.command == "preview":
            result = preview(document)
        else:
            report = preview(document)
            projection = project(document)
            projection["tables"]["copies"] = report["copies"]
            projection.pop("deterministic_sha256")
            projection["deterministic_sha256"] = digest(projection)
            result = {"projection": projection, "cleanup_preview": report, "actions_taken": 0}
        sys.stdout.reconfigure(encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(json.dumps({"error": str(error), "actions_taken": 0}), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
