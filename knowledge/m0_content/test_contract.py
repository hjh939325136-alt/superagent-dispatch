import copy
from contextlib import closing
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from contract import Contract, ContractError, digest, parse_ref
from fixtures import GRANT, approve, chain, metrics, observation, put


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="m0-content-test-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "synthetic.sqlite3"
        self.service = Contract(self.path, create=True)
        self.addCleanup(self.service.close)
        for method in ("connect", "connect_ex", "sendto"):
            blocker = patch.object(socket.socket, method, side_effect=AssertionError("No network allowed"))
            blocker.start()
            self.addCleanup(blocker.stop)
        self.network = patch("socket.create_connection", side_effect=AssertionError("No network allowed"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def reject(self, code, operation, payload):
        before = list(self.service.db.iterdump())
        with self.assertRaises(ContractError) as got:
            self.service.invoke(operation, payload)
        self.assertEqual(got.exception.code, code)
        self.assertEqual(before, list(self.service.db.iterdump()), "Rejected operation left partial writes")

    def prepared(self, name="game"):
        f = chain(self.service, name)
        p = self.service.invoke("publish.prepare", f["request"])
        return f, p

    def published(self, name="game"):
        f, p = self.prepared(name)
        self.service.invoke("publish.observe", observation(f, p["preparation_id"]))
        return f, p

    def change(self, ref):
        old = self.service.get(ref)
        return self.service.invoke("artifact.put", {key: old[key] for key in ("owner", "kind", "id", "trace_id", "derived_from")} | {"body": {"text": "changed synthetic v2"}})["ref"]

    def test_all_upstream_bound_and_ref_hashes_readable_for_two_chains(self):
        for name in ("game", "ai-editing"):
            f = chain(self.service, name)
            a = self.service.get(f["cut_approval"])["body"]
            self.assertEqual(a["upstream_refs"], self.service.closure(f["cut"]))
            self.assertTrue({f[k] for k in ("topic", "adopted", "context", "research", "strategy", "script", "script_approval", "cut")} <= set(a["upstream_refs"]))
            self.assertEqual(a["version_bound"], 1)
            self.assertFalse(a["human_reviewed"])
            self.assertFalse(a["publish_authorized"])
            trace = self.service.trace(f["trace"])
            for item in trace["artifacts"]:
                self.assertEqual(digest({k: v for k, v in item.items() if k != "ref"}), parse_ref(item["ref"])[2])
            self.assertEqual(trace["studio_integration"], "not_connected")
            self.assertTrue(all(e["refs"] for e in trace["events"]))

    def test_script_v2_rejects_v1_and_marks_downstream_stale(self):
        f, p = self.prepared()
        self.change(f["script"])
        self.reject("APPROVAL_STALE", "publish.prepare", f["request"])
        self.reject("APPROVAL_STALE", "fixture.submit_unknown", {"preparation_id": p["preparation_id"]})
        with self.assertRaisesRegex(ContractError, "upstream"):
            self.service.verify_approval(f["script_approval"], f["script"], "script_ready", 1)
        self.assertEqual({a["state"] for a in self.service.trace(f["trace"])["approvals"]}, {"stale"})

    def test_cut_v2_rejects_old_approval_new_approval_gets_new_key(self):
        f, first = self.prepared()
        new_cut = self.change(f["cut"])
        request = copy.deepcopy(f["request"])
        request["inputs"].update(cut_ref=new_cut, approval_version_bound=2)
        self.reject("APPROVAL_STALE", "publish.prepare", request)
        request["inputs"]["approval_ref"] = approve(self.service, new_cut, "cut_review")
        second = self.service.invoke("publish.prepare", request)
        self.assertNotEqual(first["idempotency_key"], second["idempotency_key"])
        self.assertEqual(second["inputs"]["approval_version_bound"], 2)

    def test_changes_in_early_ancestors_invalidate_approval(self):
        for name in ("topic", "research", "context", "strategy"):
            f = chain(self.service, name)
            self.change(f[name])
            self.reject("APPROVAL_STALE", "publish.prepare", f["request"])

    def test_fixture_authorization_cannot_be_human_or_publish_permission(self):
        f = chain(self.service)
        self.reject("FIXTURE_AUTHORIZATION_REQUIRED", "approval.record", {"subject_ref": f["script"], "scope": "script_ready"})
        self.reject("FIXTURE_AUTHORIZATION_REQUIRED", "approval.record", {"subject_ref": f["script"], "scope": "script_ready", "fixture_grant": "user-approved"})
        self.reject("INVALID_REQUEST", "approval.record", {"subject_ref": f["script"], "scope": "script_ready", "fixture_grant": GRANT, "human_reviewed": True})
        self.reject("INVALID_APPROVAL_REFS", "approval.record", {"subject_ref": f["script"], "scope": "script_ready", "fixture_grant": GRANT, "refs": [f["script"]]})
        self.reject("APPROVAL_SCOPE_MISMATCH", "approval.record", {"subject_ref": f["script"], "scope": "publish", "fixture_grant": GRANT})
        self.reject("PLATFORM_UNVERIFIED", "platform.submit", {})
        self.assertEqual(self.service.capabilities()["platform_calls"], 0)

    def test_prepare_idempotency_and_conflict(self):
        f, first = self.prepared()
        self.assertEqual(first, self.service.invoke("publish.prepare", f["request"]))
        self.assertEqual(self.service.db.execute("SELECT count(*) FROM preparations").fetchone()[0], 1)
        request = copy.deepcopy(f["request"])
        request["preparation_id"] = first["preparation_id"]
        request["experiment"]["arm"] = "A"
        self.reject("IDEMPOTENCY_CONFLICT", "publish.prepare", request)
        request = copy.deepcopy(f["request"])
        request["inputs"]["approval_version_bound"] = True
        self.reject("APPROVAL_STALE", "publish.prepare", request)
        other_strategy = put(self.service, f["trace"], "creative", "strategy", "unapproved-strategy", {"text": "Unapproved strategy"})
        request = copy.deepcopy(f["request"])
        request["experiment"]["strategy_ref"] = other_strategy
        self.reject("APPROVAL_STALE", "publish.prepare", request)

    def test_unknown_survives_restart_no_replay_overdue_and_observation(self):
        f, p = self.prepared()
        key = p["preparation_id"]
        self.service.invoke("fixture.submit_unknown", {"preparation_id": key, "at": "2026-11-15T00:00:00Z"})
        with closing(Contract(self.path)) as reopened:
            status = reopened.status(key, "2026-11-16T01:00:00Z")
            self.assertEqual(status["dispatch_state"], "unknown")
            self.assertTrue(status["needs_human"])
            self.assertFalse(status["automatic_retry_allowed"])
            with self.assertRaises(ContractError) as caught:
                reopened.invoke("fixture.submit_unknown", {"preparation_id": key})
            self.assertEqual(caught.exception.code, "EXTERNAL_REPLAY_FORBIDDEN")
            self.assertEqual(reopened.status(key)["simulated_external_calls"], 1)
            settled = reopened.invoke("publish.observe", observation(f, key))
            self.assertEqual(settled["state"], "published")
            self.assertEqual(settled["platform_calls"], 0)
        self.reject("EXTERNAL_REPLAY_FORBIDDEN", "fixture.submit_unknown", {"preparation_id": key})

    def test_metrics_windows_repeated_observation_and_missing_rules(self):
        f, p = self.published()
        for window in ("T+1h", "T+24h", "T+72h", "T+7d"):
            payload = metrics(f, p["preparation_id"], window=window)
            first = self.service.invoke("publish.metrics_import", payload)
            self.assertEqual(first, self.service.invoke("publish.metrics_import", payload))
        second = self.service.invoke("publish.metrics_import", metrics(f, p["preparation_id"], at="2026-11-18T01:05:00Z"))
        self.assertEqual(self.service.db.execute("SELECT count(*) FROM metric_snapshots").fetchone()[0], 5)
        self.assertIsNone(second["metrics"]["comments"])
        bad = metrics(f, p["preparation_id"])
        bad["metrics"]["comments"] = 5
        self.reject("METRIC_CONFLICT", "publish.metrics_import", bad)
        bad["metrics"]["comments"] = None
        bad["missing_fields"] = []
        self.reject("METRIC_MISSING", "publish.metrics_import", bad)
        bad = metrics(f, p["preparation_id"])
        bad["window"] = "T+2h"
        self.reject("INVALID_WINDOW", "publish.metrics_import", bad)
        for value in (True, -1, float("nan"), "12"):
            bad = metrics(f, p["preparation_id"])
            bad["metrics"]["views"] = value
            self.reject("INVALID_METRICS", "publish.metrics_import", bad)

    def test_unknown_cannot_be_bypassed_with_new_preparation_id(self):
        f, p = self.prepared()
        self.service.invoke("fixture.submit_unknown", {"preparation_id": p["preparation_id"]})
        request = copy.deepcopy(f["request"])
        request["preparation_id"] = "synthetic-new-id"
        self.reject("DUPLICATE_PREPARATION", "publish.prepare", request)
        request["experiment"]["arm"] = "C"
        self.reject("EXTERNAL_UNKNOWN_UNRESOLVED", "publish.prepare", request)
        self.assertEqual(self.service.db.execute("SELECT sum(simulated_external_calls) FROM preparations").fetchone()[0], 1)

    def test_failed_or_withdrawn_observation_is_never_projected_as_success(self):
        for status, kind, projected_state in (("rejected", "task.failed", "failed"), ("withdrawn", "task.cancelled", "cancelled")):
            f, p = self.prepared(status)
            self.service.invoke("fixture.submit_unknown", {"preparation_id": p["preparation_id"]})
            result = self.service.invoke("publish.observe", observation(f, p["preparation_id"], status=status))
            self.assertEqual(result["state"], status)
            event = self.service.project_events(f["trace"])[-1]
            self.assertEqual(event["kind"], kind)
            self.assertEqual(event["metadata"]["to_status"], projected_state)

    def test_metric_conflicting_timestamp_and_ownership_rejected(self):
        f, p = self.published()
        payload = metrics(f, p["preparation_id"])
        self.service.invoke("publish.metrics_import", payload)
        payload["metrics"]["views"] = 1250
        self.reject("METRIC_OBSERVATION_CONFLICT", "publish.metrics_import", payload)
        for target in ("signalhub:synthetic-reference", "dy:synthetic-other", "signalhub/work/native@1#" + "a" * 64):
            payload["target_id"] = target
            self.reject("OWNERSHIP_MISMATCH", "publish.metrics_import", payload)
        payload["target_id"] = "dy:synthetic-game"
        payload["account_ref"] = "webee/account/synthetic-other"
        self.reject("INVALID_REQUEST", "publish.metrics_import", payload)

    def test_matching_never_changes_numbers_target_or_account(self):
        f, p = self.published()
        snap = self.service.invoke("publish.metrics_import", metrics(f, p["preparation_id"]))
        before = self.service.db.execute("SELECT * FROM metric_snapshots").fetchall()
        raw_before = self.service.get(snap["ref"])
        request = {"preparation_id": p["preparation_id"], "snapshot_id": snap["snapshot_id"]}
        matched = self.service.invoke("publish.metrics_match", request)
        self.assertEqual(matched["matching_status"], "matched")
        self.assertEqual(snap["metrics_sha256"], matched["metrics_sha256"])
        self.assertEqual(before, self.service.db.execute("SELECT * FROM metric_snapshots").fetchall())
        self.assertEqual(raw_before, self.service.get(snap["ref"]))
        self.assertEqual(matched, self.service.invoke("publish.metrics_match", request))
        _, other = self.published("other")
        self.reject("OWNERSHIP_MISMATCH", "publish.metrics_match", request | {"preparation_id": other["preparation_id"]})
        self.reject("INVALID_REQUEST", "publish.metrics_match", request | {"metrics": {"views": 9999}})

    def test_strategy_pointer_rollback_appends_change_without_work_mutation(self):
        f, p = self.published()
        self.service.invoke("strategy.activate", {"strategy_ref": f["strategy"]})
        second = self.change(f["strategy"])
        self.service.invoke("strategy.activate", {"strategy_ref": second})
        before = self.service.status(p["preparation_id"])
        rolled = self.service.invoke("strategy.rollback", {"strategy_ref": f["strategy"]})
        self.assertEqual(rolled["strategy_ref"], f["strategy"])
        self.assertEqual(rolled["published_work_actions"], 0)
        self.assertEqual(before, self.service.status(p["preparation_id"]))
        self.assertEqual(self.service.get(second)["version"], 2)
        self.assertEqual(self.service.get(rolled["change_ref"])["body"]["operation"], "strategy.rollback")
        self.assertEqual(self.service.db.execute("SELECT count(*) FROM changes").fetchone()[0], 3)

    def test_hash_tampering_detected(self):
        f = chain(self.service)
        self.service.db.execute("UPDATE artifacts SET payload=? WHERE ref=?", ('{"modified":true}', f["topic"]))
        with self.assertRaises(ContractError) as caught:
            self.service.get(f["topic"])
        self.assertEqual(caught.exception.code, "HASH_MISMATCH")

    def test_cli_and_business_database_refusal(self):
        process = subprocess.run([sys.executable, "-B", str(Path(__file__).with_name("contract.py")), "--db", str(self.path), "publish.capabilities"], capture_output=True, text=True)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(json.loads(process.stdout)["data"]["platform"]["submit"], "unverified")
        other = self.path.parent / "not-fixture.sqlite3"
        with closing(sqlite3.connect(other)) as db:
            db.execute("CREATE TABLE business(id INTEGER)")
        before = other.read_bytes()
        with self.assertRaises(ContractError) as caught:
            Contract(other)
        self.assertEqual(caught.exception.code, "NOT_SYNTHETIC_DATABASE")
        self.assertEqual(before, other.read_bytes())


if __name__ == "__main__":
    unittest.main()
