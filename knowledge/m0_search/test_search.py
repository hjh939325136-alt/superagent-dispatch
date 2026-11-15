"""Synthetic integration tests for the two fixture domains."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from fixture_store import DOMAINS, ROOT, build_domain, fixture_records, load_domain, read_json
from search import SearchError, get, record_ref, search


class SearchTest(unittest.TestCase):
    expected_withheld = {"restricted": 1, "revoked": 1, "out_of_scope": 1, "unreviewed": 1}

    def setUp(self):
        self.request = read_json(ROOT / "fixtures" / "search-request.json")
        self.grant = "fixture-both"

    def lookup(self, ref):
        request = {key: value for key, value in self.request.items() if key not in {"q", "limit"}}
        return get({**request, "ref": ref}, self.grant)

    def test_two_domains_keep_exact_references(self):
        result = search(self.request, self.grant)
        self.assertEqual(list(result["index_versions"]), ["research-scatter", "research-content"])
        rows = result["results"]
        self.assertEqual([row["status"] for row in rows], ["current", "current", "expired"])
        self.assertEqual(len({row["ref"].split("/")[0] for row in rows}), 2)
        for row in rows:
            self.assertRegex(row["ref"], r"^research-(scatter|content)/note/[0-7][0-9A-HJKMNP-TV-Z]{25}@\d+#[a-f0-9]{64}$")
            self.assertEqual(self.lookup(row["ref"])["result"], row)
        with self.assertRaises(SearchError) as caught:
            self.lookup(rows[0]["ref"][:-64] + "0" * 64)
        self.assertEqual(caught.exception.code, "NOT_FOUND")

    def test_withholding_returns_counts_and_no_hidden_details(self):
        self.request["max_class"] = "restricted"
        result = search(self.request, self.grant)
        self.assertEqual(result["withheld"], self.expected_withheld)
        raw = json.dumps(result)
        for tag in ("RESTRICTED_FIXTURE_SENTINEL", "REVOKED_FIXTURE_SENTINEL", "ARCHIVED_FIXTURE_SENTINEL",
                    "SCOPE_FIXTURE_SENTINEL", "UNREVIEWED_FIXTURE_SENTINEL", "DELETED_FIXTURE_SENTINEL",
                    "sample-restricted", "sample-revoked", "sample-archived", "sample-future",
                    "sample-out-of-scope", "sample-unreviewed", "sample-deleted"):
            self.assertNotIn(tag, raw)
        for domain, ident in (
            ("research-scatter", "sample-restricted"), ("research-content", "sample-revoked"),
            ("research-scatter", "sample-out-of-scope"), ("research-content", "sample-unreviewed"),
            ("research-scatter", "sample-deleted"),
        ):
            row = next(value for value in fixture_records(domain) if value["source_ref"].endswith("/" + ident))
            with self.assertRaises(SearchError) as caught:
                self.lookup(record_ref(domain, row))
            self.assertEqual(caught.exception.code, "NOT_FOUND")

    def test_caller_cannot_widen_grant(self):
        for change in ({"purpose": "unrelated"}, {"task_id": "unrelated"}, {"agent": "unrelated"}):
            req = {**self.request, "caller": {**self.request["caller"], **change}}
            with self.assertRaises(SearchError) as caught:
                search(req, self.grant)
            self.assertEqual(caught.exception.code, "ACCESS_DENIED")
        req = {**self.request, "caller": {**self.request["caller"], "task_id": "fixture-content-task"}}
        with self.assertRaises(SearchError) as caught:
            search(req, "fixture-content-only")
        self.assertEqual(caught.exception.code, "ACCESS_DENIED")
        req["domains"] = ["research-content"]
        result = search(req, "fixture-content-only")
        self.assertEqual({row["class"] for row in result["results"]}, {"public"})
        self.assertEqual(set(result["index_versions"]), {"research-content"})
        with self.assertRaises(SearchError) as caught:
            search({**self.request, "scopes": ["research:read", "research:limited"]}, self.grant)
        self.assertEqual(caught.exception.code, "ACCESS_DENIED")

    def test_scope_input_and_cap_are_enforced(self):
        for fields in ({"limit": True}, {"as_of": "2026-11-15"}, {"q": " "},
                       {"domains": ["wechat-kb"]}, {"scopes": []}):
            with self.assertRaises(SearchError) as caught:
                search({**self.request, **fields}, self.grant)
            self.assertEqual(caught.exception.code, "INVALID_REQUEST")
        limited = search({**self.request, "limit": 1}, self.grant)
        self.assertEqual(len(limited["results"]), 1)
        self.assertEqual(limited["withheld"], self.expected_withheld)

    def test_independent_domain_stores_and_reordered_import(self):
        with TemporaryDirectory() as directory, TemporaryDirectory() as rebuild:
            root, second_root = Path(directory), Path(rebuild)
            for domain in DOMAINS:
                filename = f"{domain}.sqlite3"
                build_domain(root / filename, domain)
                with closing(sqlite3.connect(root / filename)) as db:
                    self.assertEqual(db.execute("SELECT domain, count(*) FROM records GROUP BY domain").fetchall(),
                                     [(domain, 30)])
                    self.assertEqual(db.execute("SELECT count(*) FROM record_versions").fetchone()[0], 30)
                    self.assertGreater(db.execute("SELECT sum(human_locked) FROM records").fetchone()[0], 0)
                    self.assertEqual(db.execute("SELECT sum(train_allowed) FROM records").fetchone()[0], 0)
                    samples = db.execute("SELECT DISTINCT status, class FROM records").fetchall()
                    self.assertTrue({"expired", "revoked"} <= {status for status, _ in samples})
                    self.assertIn("restricted", {classification for _, classification in samples})
                with patch("fixture_store.read_json", side_effect=lambda path: list(reversed(read_json(path)))):
                    build_domain(second_root / filename, domain)
                original, revision = load_domain(root, domain)
                reordered, rebuilt_revision = load_domain(second_root, domain)
                self.assertEqual(original, reordered)
                self.assertEqual(revision, rebuilt_revision)

    def test_cli_does_not_mutate_fixtures(self):
        original = {path: path.read_bytes() for path in (ROOT / "fixtures").glob("*.json")}
        command = [sys.executable, "-B", str(ROOT / "search.py"), "knowledge.search", "--grant", self.grant,
                   "--request", str(ROOT / "fixtures" / "search-request.json")]
        process = subprocess.run(command, check=True, capture_output=True, encoding="utf-8")
        self.assertEqual(json.loads(process.stdout), search(self.request, self.grant))
        self.assertEqual({path: path.read_bytes() for path in original}, original)


if __name__ == "__main__":
    unittest.main()
