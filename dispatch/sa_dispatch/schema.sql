CREATE TABLE IF NOT EXISTS tasks(
 task_id TEXT PRIMARY KEY, idem_key TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL,
 kind TEXT NOT NULL, kind_version INTEGER NOT NULL, track TEXT,
 params_json TEXT NOT NULL, input_refs_json TEXT NOT NULL, required_caps_json TEXT NOT NULL,
 scope_json TEXT NOT NULL, side_effect TEXT NOT NULL CHECK(side_effect IN('none','idempotent_write','external')),
 access TEXT NOT NULL CHECK(access IN('synthetic','exportable','local_only')),
 egress TEXT NOT NULL CHECK(egress IN('none','summary','derived')),
 status TEXT NOT NULL CHECK(status IN('queued','waiting_device','leased','succeeded','no_new','failed','unknown','cancelled')),
 fence INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
 max_attempts INTEGER NOT NULL CHECK(max_attempts>=1), parent_task_id TEXT REFERENCES tasks(task_id),
 result_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_effectful_inflight ON tasks(fingerprint)
 WHERE side_effect!='none' AND status NOT IN('succeeded','no_new','failed','cancelled');
CREATE TABLE IF NOT EXISTS leases(
 lease_id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(task_id),
 fence INTEGER NOT NULL, device_id TEXT NOT NULL, granted_at TEXT NOT NULL,
 expires_at TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN('active','completed','expired','unknown')),
 completion_json TEXT, UNIQUE(task_id,fence));
CREATE UNIQUE INDEX IF NOT EXISTS one_active_lease ON leases(task_id) WHERE state='active';
CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY,task_id TEXT NOT NULL,
 from_status TEXT,to_status TEXT NOT NULL,fence INTEGER,reason TEXT NOT NULL,actor TEXT NOT NULL,at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS evidence(evidence_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,fence INTEGER NOT NULL,
 kind TEXT NOT NULL,ref_json TEXT,content_hash TEXT NOT NULL,artifact_hash TEXT,access TEXT NOT NULL,created_at TEXT NOT NULL,
 UNIQUE(task_id,fence,kind,content_hash));
CREATE TABLE IF NOT EXISTS late_results(id TEXT PRIMARY KEY,task_id TEXT NOT NULL,fence INTEGER NOT NULL,
 outcome_json TEXT NOT NULL,received_at TEXT NOT NULL,disposition TEXT NOT NULL CHECK(disposition IN('ignored','used_for_reconcile')));
CREATE TABLE IF NOT EXISTS devices(device_id TEXT PRIMARY KEY,caps_json TEXT NOT NULL,last_seen_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS transport_handoffs(
 task_id TEXT PRIMARY KEY REFERENCES tasks(task_id), actor TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS transport_artifact_grants(
 grant_id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(task_id),
 lease_id TEXT REFERENCES leases(lease_id), content_hash TEXT NOT NULL,
 access TEXT NOT NULL CHECK(access='synthetic'), created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS component_capabilities(
 component TEXT NOT NULL, capability TEXT NOT NULL,
 lookup_by_write_key INTEGER NOT NULL DEFAULT 0,
 idempotency_proof TEXT NOT NULL CHECK(idempotency_proof IN('none','claimed','verified')),
 proof_ref TEXT, updated_at TEXT NOT NULL,
 PRIMARY KEY(component,capability));
CREATE TABLE IF NOT EXISTS reconciliations(
 id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(task_id), epoch INTEGER NOT NULL,
 method TEXT NOT NULL CHECK(method IN('lookup','proven_rerun','human')),
 result TEXT NOT NULL CHECK(result IN('found','absent','mismatch','error','decided')),
 evidence_ref TEXT, decided_by TEXT NOT NULL, at TEXT NOT NULL);
