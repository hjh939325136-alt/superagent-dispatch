"""Read-only federation over synthetic research and a fixed SignalHub summary source."""

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory

from fixture_store import DOMAINS, ROOT, build_domain, load_domain, read_json, record_hash
from signalhub_adapter import build_source, load_source
from catalog import Catalog, CatalogError, SOURCES

CLASSES = {name: i for i, name in enumerate(("public", "internal", "local_only", "restricted"))}
CALLER_FIELDS = {"agent", "task_id", "purpose"}


class SearchError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def authorize(request, grant_id, field):
    required = {field, "domains", "caller", "scopes", "max_class", "as_of"}
    optional = {"limit"} if field == "q" else set()
    if not isinstance(request, dict) or not required <= request.keys() <= required | optional:
        raise SearchError("INVALID_REQUEST", "Invalid request fields")
    caller = request["caller"]
    domains = request["domains"]
    scopes = request["scopes"]
    classification = request["max_class"]
    if (not isinstance(caller, dict) or caller.keys() != CALLER_FIELDS or
            any(not isinstance(value, str) or not value for value in caller.values())):
        raise SearchError("INVALID_REQUEST", "Caller requires agent, task_id and purpose")
    if (not isinstance(domains, list) or not domains or
            any(not isinstance(name, str) or name not in SOURCES for name in domains) or
            len(set(domains)) != len(domains)):
        raise SearchError("INVALID_REQUEST", "Domains must be a nonempty list of distinct fixture domains")
    if not isinstance(classification, str) or classification not in CLASSES:
        raise SearchError("INVALID_REQUEST", "Invalid max_class")
    if (not isinstance(scopes, list) or not scopes or
            any(not isinstance(scope, str) or not scope for scope in scopes) or
            len(set(scopes)) != len(scopes)):
        raise SearchError("INVALID_REQUEST", "Scopes must be a nonempty list of distinct names")
    try:
        instant = datetime.fromisoformat(request["as_of"])
        if instant.utcoffset() is None:
            raise ValueError("missing offset")
    except (TypeError, ValueError):
        raise SearchError("INVALID_REQUEST", "as_of must include an ISO-8601 time and offset") from None
    grant = read_json(ROOT / "fixtures" / "grants.json").get(grant_id)
    if (not grant or grant.get("role", "agent_reader") not in ("agent_reader", "curator") or caller != grant["caller"] or not set(domains) <= set(grant["domains"]) or
            not set(scopes) <= set(grant["scopes"])):
        raise SearchError("ACCESS_DENIED", "Caller, domains or scopes outside the selected fixture grant")
    maximum = min(CLASSES[classification], CLASSES[grant["max_class"]])
    return domains, maximum, frozenset(scopes), instant.date()


def read_catalog(domains, catalog=None):
    if catalog is not None:
        selected = catalog if isinstance(catalog, Catalog) else Catalog(catalog)
        return selected.load(domains)
    with TemporaryDirectory(prefix="m0-research-") as directory:
        root = Path(directory)
        for domain in domains:
            if domain == "signalhub":
                build_source(root / "signalhub.sqlite3")
            else:
                build_domain(root / f"{domain}.sqlite3", domain)
        return {domain: load_source(root) if domain == "signalhub" else load_domain(root, domain) for domain in domains}


def record_ref(domain, record):
    return f'{domain}/{record.get("ref_kind", "note")}/{record["id"]}@{record["version"]}#{record_hash(record)}'


def visibility(record, maximum, scopes, at):
    if date.fromisoformat(record["observed_at"]) > at or record["state"] in {"archived", "deleted"}:
        return "excluded"
    # All current fixture sources are visible at most through the internal tier.
    if CLASSES[record["class"]] > min(maximum, CLASSES["internal"]):
        return "restricted"
    if record["state"] == "revoked":
        return "revoked"
    if record["required_scope"] not in scopes:
        return "out_of_scope"
    if not record["human_reviewed"]:
        return "unreviewed"
    return "visible"


def result_item(domain, record, at):
    expiry = record["expires_at"]
    result = {key: record[key] for key in ("title", "class", "source_ref", "observed_at", "expires_at",
                                         "human_reviewed", "human_locked")}
    result["ref"] = record_ref(domain, record)
    result["status"] = "expired" if record["state"] == "expired" or (expiry and date.fromisoformat(expiry) <= at) else "current"
    result["snippet"] = record["body"][:240]
    return result


def search(request, grant_id, catalog=None):
    domains, maximum, scopes, at = authorize(request, grant_id, "q")
    q = request["q"]
    limit = request.get("limit", 10)
    if not isinstance(q, str) or not q.strip() or len(q) > 200 or type(limit) is not int or not 1 <= limit <= 100:
        raise SearchError("INVALID_REQUEST", "Query or result limit outside allowed bounds")
    words = q.casefold().split()
    results, versions = [], {}
    withheld = dict.fromkeys(("restricted", "revoked", "out_of_scope", "unreviewed"), 0)
    for domain, (records, revision) in read_catalog(domains, catalog).items():
        versions[domain] = revision
        for row in records:
            text = f'{row["title"]} {row["body"]}'.casefold()
            if not all(word in text for word in words):
                continue
            outcome = visibility(row, maximum, scopes, at)
            if outcome in withheld:
                withheld[outcome] += 1
            elif outcome == "visible":
                results.append(result_item(domain, row, at))
    results.sort(key=lambda item: item["ref"])
    results.sort(key=lambda item: item["observed_at"], reverse=True)
    results.sort(key=lambda item: item["status"] != "current")
    return {"results": results[:limit], "withheld": withheld, "index_versions": versions}


def get(request, grant_id, catalog=None, _connection=None):
    domains, maximum, scopes, at = authorize(request, grant_id, "ref")
    ref = request["ref"]
    if not isinstance(ref, str) or not ref or len(ref) > 220:
        raise SearchError("INVALID_REQUEST", "Invalid fixed reference")
    matching = [domain for domain in domains if ref.startswith(domain + "/")]
    if catalog is not None:
        selected = catalog if isinstance(catalog, Catalog) else Catalog(catalog)
        match = selected.lookup(ref, domains, _connection)
        if match:
            row, current, revision = match
            if visibility(row, maximum, scopes, at) == visibility(current, maximum, scopes, at) == "visible":
                item = result_item(matching[0], row, at)
                if result_item(matching[0], current, at)["status"] == "expired":
                    item["status"] = "expired"
                return {"result": item, "index_versions": {matching[0]: revision}}
        raise SearchError("NOT_FOUND", "No visible record at this exact version and hash")
    for domain, (records, index_version) in read_catalog(matching).items():
        for row in records:
            if visibility(row, maximum, scopes, at) == "visible" and record_ref(domain, row) == ref:
                return {"result": result_item(domain, row, at), "index_versions": {domain: index_version}}
    raise SearchError("NOT_FOUND", "No visible record at this exact version and hash")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("knowledge.search", "knowledge.get", "context.assemble", "context.get", "context.reassemble"))
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--grant", required=True)
    parser.add_argument("--catalog", type=Path)
    args = parser.parse_args(argv)
    try:
        request = read_json(args.request)
        if args.operation.startswith("context."):
            if args.catalog is None:
                raise SearchError("CATALOG_REQUIRED", "Context lifecycle requires an explicitly initialized synthetic catalog")
            selected = Catalog(args.catalog)
            response = selected.assemble(request, args.grant) if args.operation == "context.assemble" else selected.context_get(request, args.grant, reassemble=args.operation == "context.reassemble")
        else:
            handler = search if args.operation == "knowledge.search" else get
            response = handler(request, args.grant, catalog=args.catalog)
    except (SearchError, CatalogError) as error:
        print(json.dumps({"error": {"code": error.code, "message": str(error)}}, ensure_ascii=True))
        return 2
    except (OSError, UnicodeError, json.JSONDecodeError, sqlite3.DatabaseError):
        print(json.dumps({"error": {"code": "INPUT_UNAVAILABLE", "message": "Could not read the request or fixture"}}))
        return 2
    print(json.dumps(response, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
