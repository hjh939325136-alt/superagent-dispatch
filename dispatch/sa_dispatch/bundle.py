"""Portable settled-ledger v7 bundles, with lossless audit rows and inert schedules.

Frozen v1-v6 layouts remain readable. Host device registrations are explicitly excluded from
portable bundles. Full in-process snapshots include them. Pending reservations
and nonterminal tasks are rejected rather than discarded or replayed on import.
"""
from contextlib import closing
import json
import re
from pathlib import Path
import sqlite3
import tempfile

from .artifacts import ArtifactError, ArtifactStore
from .canonical import sha256_ref
from .executor import receipt_bytes
from .models import DispatchError
from .routing import (ROUTED_TABLES, ROUTES, _digest, ledger_mode, read_route_evidence,
                      validate_binding_evidence, validate_mixed_records)


V1_TABLES = {"tasks": "rowid", "leases": "task_id,fence", "events": "seq",
             "evidence": "evidence_id", "late_results": "id"}
V2_TABLES = {**V1_TABLES, "component_capabilities": "component,capability",
          "reconciliations": "rowid", "schedules": "id",
          "schedule_firings": "scheduled_at,fire_key", "schedule_events": "seq",
          "unified_events": "seq", "transport_handoffs": "task_id",
          "transport_artifact_grants": "grant_id", "schema_migrations": "version"}
V3_TABLES = {**V2_TABLES, "control_budgets": "budget_id", "control_approvals": "approval_id",
          "control_task_rules": "task_id", "control_reservations": "task_id,fence",
          "control_handoffs": "handoff_id", "control_events": "seq",
          "host_attempts": "task_id,fence", "host_events": "task_id,fence,seq"}
V4_TABLES = {**V3_TABLES, "claude_attempts":"task_id,fence"}
V5_TABLES = dict(V4_TABLES)
V6_TABLES = {**V5_TABLES, 'collector_cursors':'source_id,stream', 'collector_gaps':'gap_id'}
TABLES = {**V6_TABLES, 'ledger_mode': 'id', 'route_audit': 'seq'}
SNAPSHOT_TABLES = {**TABLES, "devices": "device_id"}
V1_FORMAT = "sa_dispatch.bundle.v1"
V2_FORMAT = "sa_dispatch.bundle.v2"
V3_FORMAT = "sa_dispatch.bundle.v3"
V4_FORMAT = "sa_dispatch.bundle.v4"
V5_FORMAT = "sa_dispatch.bundle.v5"
V6_FORMAT = "sa_dispatch.bundle.v6"
FORMAT = "sa_dispatch.bundle.v7"
TERMINAL = {"succeeded", "no_new", "failed", "cancelled"}


def _present(conn):
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            if not r[0].startswith("sqlite_")}


def _unknown_populated(conn):
    for name in sorted(_present(conn) - set(SNAPSHOT_TABLES)):
        quoted = '"' + name.replace('"', '""') + '"'
        if conn.execute("SELECT 1 FROM " + quoted + " LIMIT 1").fetchone():
            return name
    return None


def snapshot_records(conn, *, include_devices=True):
    """Read all known tables in the caller's transaction; absent opt-ins are [].

    Unknown populated tables fail closed instead of silently losing new data.
    Reading a live snapshot is allowed; migration eligibility is checked later.
    """
    unknown = _unknown_populated(conn)
    if unknown:
        raise DispatchError("snapshot does not support populated table: " + unknown)
    present = _present(conn)
    tables = SNAPSHOT_TABLES if include_devices else TABLES
    records = {table: [dict(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY {order}")]
               if table in present else [] for table, order in tables.items()}
    records['ledger_mode'] = [{'id': 1, 'mode': ledger_mode(conn)}]
    for table in ROUTED_TABLES.keys() & tables.keys():
        for row in records[table]:
            row.setdefault('execution_route', None)
    return records


def ledger(path, *, include_devices=False):
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        try:
            return snapshot_records(conn, include_devices=include_devices)
        except DispatchError as exc:
            raise ArtifactError("E_EXPORT_BLOCKED", str(exc)) from exc


def _settled(records):
    from .collector import validate_records
    try:
        validate_records(records.get('collector_cursors',[]),records.get('collector_gaps',[]))
    except (ValueError,KeyError,TypeError) as exc:
        raise DispatchError('invalid collector audit records') from exc
    tasks = {row["task_id"]: row for row in records["tasks"]}
    if any(row["status"] not in TERMINAL for row in tasks.values()):
        raise DispatchError("settle queued, leased or unknown tasks before exporting")
    if any(row["state"] == "active" for row in records["leases"]):
        raise DispatchError("active leases cannot migrate")
    for table in ("leases", "events", "evidence", "late_results", "reconciliations",
                  "transport_handoffs", "transport_artifact_grants"):
        if any(row["task_id"] not in tasks for row in records.get(table, [])):
            raise DispatchError("orphan task reference in " + table)
    schedules = {row["id"] for row in records.get("schedules", [])}
    for row in records.get("schedules", []):
        if row["enabled"] not in (0, 1):
            raise DispatchError("invalid schedule enabled state")
        bound=row.get('max_slots')
        if bound is not None:
            if type(bound) is not int or not 1<=bound<=100000:
                raise DispatchError('invalid schedule lifetime slot bound')
            used=sum(f['schedule_id']==row['id'] for f in records.get('schedule_firings',[]))
            if used>bound or (used==bound and row['enabled']):
                raise DispatchError('schedule slot bound or exhaustion state disagrees with history')
    for row in records.get("schedule_firings", []):
        outcome = row.get("outcome", "created" if row.get("task_id") else "pending")
        if outcome == "pending":
            raise DispatchError("pending schedule reservations must be settled before migration")
        if outcome not in {"created", "skipped_catchup", "rejected"}:
            raise DispatchError("unknown schedule firing outcome cannot migrate")
        if row["schedule_id"] not in schedules:
            raise DispatchError("orphan schedule firing")
        if outcome == "created" and row.get("task_id") not in tasks:
            raise DispatchError("created firing requires a terminal task")
        if row.get("task_id") is not None and row["task_id"] not in tasks:
            raise DispatchError("orphan firing task")
    for row in records.get("unified_events", []):
        if row.get("source") == "dispatch" and row.get("task_id") is not None and row["task_id"] not in tasks:
            raise DispatchError("orphan dispatch projection task")
    if any(r['state'] != 'settled' for r in records.get('control_reservations', [])):
        raise DispatchError('unsettled budget reservations cannot migrate')
    if any(r['reserved_units'] != 0 for r in records.get('control_budgets', [])):
        raise DispatchError('held budget units cannot migrate')
    if any(r['state'] in ('offered','accepted') for r in records.get('control_handoffs', [])):
        raise DispatchError('active handoffs cannot migrate')
    if any(r['phase'] not in ('completed','failed','cancelled','interrupted','orphan_terminal','orphan_cancelled','not_started') for r in records.get('host_attempts', [])):
        raise DispatchError('unsettled host attempts cannot migrate')
    for row in records.get('claude_attempts',[]):
        if row['phase'] not in ('succeeded','failed','interrupted') or not row.get('report_ref'):
            raise DispatchError('unsettled Claude attempts cannot migrate')
        if row['task_id'] not in tasks:
            raise DispatchError('orphan Claude task reference')
    for table in ('control_task_rules','control_reservations','control_approvals','host_attempts','host_events'):
        if any(r['task_id'] not in tasks for r in records.get(table, [])):
            raise DispatchError('orphan task reference in '+table)


def portable(records, *, include_fixed_refs=True):
    blocked = [task["task_id"] for task in records["tasks"]
               if task["access"] == "local_only" or
               (task["access"] == "exportable" and task["egress"] == "none")]
    blocked.extend(item["task_id"] for item in records["evidence"] if item["access"] == "local_only")
    if blocked:
        raise ArtifactError("E_EXPORT_BLOCKED", "restricted tasks: " + ", ".join(sorted(set(blocked))))
    if any(row["class"] == "local_only" for row in records.get("unified_events", [])):
        raise ArtifactError("E_EXPORT_BLOCKED", "local_only projection events cannot leave this host")
    try:
        _settled(records)
    except DispatchError as exc:
        raise ArtifactError("E_EXPORT_BLOCKED", str(exc)) from exc
    if records["late_results"]:
        raise ArtifactError("E_EXPORT_BLOCKED", "bundles with late receipts require local review")
    hashes = {item["artifact_hash"] for item in records["evidence"] if item["artifact_hash"]}
    for table, field in (("reconciliations", "evidence_ref"), ("component_capabilities", "proof_ref"),
                         ("transport_artifact_grants", "content_hash"),
                         ('control_approvals','evidence_ref'), ('control_reservations','evidence_ref'),
                         ('control_handoffs','authorization_ref'), ('control_handoffs','receipt_ref'),
                         ('host_attempts','prompt_ref'), ('host_attempts','report_ref'), ('host_events','event_ref'),
                         ('claude_attempts','prompt_ref'), ('claude_attempts','report_ref')):
        hashes.update(row[field] for row in records.get(table, []) if row.get(field))
    for table,field in (('control_handoffs','scope_refs_json'),('control_handoffs','evidence_refs_json'),
                       ('control_events','refs_json')):
        for row in records.get(table, []):
            refs=json.loads(row[field])
            if not isinstance(refs,list) or any(not isinstance(r,str) for r in refs):
                raise ArtifactError('E_EXPORT_BLOCKED','invalid control evidence references')
            hashes.update(refs)
    if include_fixed_refs:
        for row in records['tasks']:
            for ref in json.loads(row['input_refs_json']):
                hashes.add(ref['hash'])
        for row in records.get('unified_events',[]):
            for ref in json.loads(row['refs_json']):
                match=re.search(r'#(?:sha256:)?([0-9a-f]{64})$',ref)
                if not match:raise ArtifactError('E_EXPORT_BLOCKED','unparseable fixed projection reference')
                hashes.add('sha256:'+match.group(1))
    return sorted(hashes)


def _artifact_inventory(records, store, *, legacy=False):
    hashes = set(portable(records,include_fixed_refs=not legacy))
    for row in records.get('route_audit', []):
        ref = row['evidence_ref']
        if ref is None:
            continue
        proof = read_route_evidence(store, ref)
        if row['action'] == 'activate':
            if (set(proof) != {'schema','actor','ledger_hash','bindings_hash'}
                    or proof['schema'] != 'sa_dispatch.mixed_activation.v1' or proof['actor'] != row['actor']
                    or not _digest(proof['ledger_hash']) or not _digest(proof['bindings_hash'])):
                raise DispatchError('invalid portable activation evidence')
        else:
            validate_binding_evidence(proof, row['object_table'], row['object_id'], row['execution_route'],
                                      row['pre_state_hash'], row['fence'])
        hashes.add(ref)
    # Only this known schema owns these four child artifacts. Do not recursively
    # scrape arbitrary hashes from component payloads or user-controlled text.
    proof_fields = ("first_result_hash", "second_result_hash",
                    "state_after_first_ref", "state_after_second_ref")
    for capability in records.get("component_capabilities", []):
        ref = capability.get("proof_ref")
        if not ref:
            continue
        try:
            proof = json.loads(store.get(ref))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            if capability["idempotency_proof"] == "verified":
                raise ArtifactError("E_BUNDLE_INVALID", "verified proof is not readable JSON") from exc
            continue
        if isinstance(proof, dict) and proof.get("schema") == "sa_dispatch.idempotency_proof.v1":
            for field in proof_fields:
                if proof.get(field):
                    store.get(proof[field])
                    hashes.add(proof[field])
    if not legacy:
        for attempt in records.get('claude_attempts',[]):
            proof=json.loads(store.get(attempt['report_ref']))
            if (not isinstance(proof,dict) or proof.get('schema')!='sa_dispatch.claude_report.v1' or
                any(proof.get(key)!=attempt[key] for key in ('task_id','fence','session_id','model','prompt_ref')) or
                proof.get('status')!=attempt['phase']):
                raise ArtifactError('E_BUNDLE_INVALID','Claude result ownership mismatch')
            if attempt['phase']=='interrupted' and (type(proof.get('exit_code')) is not int or
                    proof.get('error') not in ('CLAUDE_TIMEOUT','CLAUDE_OUTPUT_LIMIT')):
                raise ArtifactError('E_BUNDLE_INVALID','interrupted Claude attempt needs confirmed process exit')
            if not isinstance(proof.get('stdout_ref'),str) or not re.fullmatch('sha256:[0-9a-f]{64}',proof['stdout_ref']):
                raise ArtifactError('E_BUNDLE_INVALID','Claude raw output reference is required')
            if attempt['phase']=='succeeded':
                init_ref=proof.get('init_ref')
                if not isinstance(init_ref,str) or not re.fullmatch('sha256:[0-9a-f]{64}',init_ref):
                    raise ArtifactError('E_BUNDLE_INVALID','successful Claude attempt needs isolation evidence')
                init=json.loads(store.get(init_ref))
                if (not isinstance(init,dict) or init.get('type')!='system' or init.get('subtype')!='init' or
                    init.get('tools')!=[] or init.get('mcp_servers')!=[] or
                    init.get('model')!=attempt['model'] or init.get('session_id')!=attempt['session_id']):
                    raise ArtifactError('E_BUNDLE_INVALID','Claude isolation evidence does not match')
            for field in ('prompt_ref','stdout_ref','init_ref','exit_ref','launch_ref'):
                if proof.get(field):
                    store.get(proof[field]);hashes.add(proof[field])
        for reservation in records.get('control_reservations',[]):
            ref=reservation.get('evidence_ref')
            if not ref:continue
            proof=json.loads(store.get(ref))
            if isinstance(proof,dict) and proof.get('schema')=='sa_dispatch.host_run_usage.v1':
                child=proof['terminal_event_ref'];store.get(child);hashes.add(child)
            elif isinstance(proof,dict) and proof.get('schema')=='sa_dispatch.claude_run_usage.v1':
                expected={'synthetic':True,'task_id':reservation['task_id'],'fence':reservation['fence'],
                    'budget_id':reservation['budget_id'],'fingerprint':reservation['fingerprint'],
                    'actual_units':reservation['actual_units'],'unit':'claude_run','final_usage':True}
                if any(type(proof.get(k)) is not type(v) or proof.get(k)!=v for k,v in expected.items()):
                    raise ArtifactError('E_BUNDLE_INVALID','Claude usage ownership mismatch')
                child=proof.get('terminal_ref')
                if not isinstance(child,str) or not re.fullmatch('sha256:[0-9a-f]{64}',child):
                    raise ArtifactError('E_BUNDLE_INVALID','Claude usage terminal reference required')
                terminal=json.loads(store.get(child));hashes.add(child)
                if (not isinstance(terminal,dict) or terminal.get('schema')!='sa_dispatch.claude_terminal.v1' or
                    terminal.get('synthetic') is not True or terminal.get('task_id')!=proof['task_id'] or
                    terminal.get('origin_fence')!=proof.get('origin_fence')):
                    raise ArtifactError('E_BUNDLE_INVALID','Claude terminal ownership mismatch')
                for field in ('prompt_ref','exit_ref','launch_ref','stdout_ref','init_ref'):
                    linked=terminal.get(field)
                    if field=='init_ref' and linked is None:continue
                    if not isinstance(linked,str) or not re.fullmatch('sha256:[0-9a-f]{64}',linked):
                        raise ArtifactError('E_BUNDLE_INVALID','Claude terminal child reference required')
                    store.get(linked);hashes.add(linked)
    return sorted(hashes)


def _normalize_snapshot(records):
    if not isinstance(records, dict):
        raise DispatchError("invalid snapshot tables")
    supplied = set(records)
    layouts = (TABLES, V6_TABLES, V5_TABLES, V3_TABLES, V2_TABLES, V1_TABLES)
    if not any(supplied in (set(layout), set(layout) | {'devices'}) for layout in layouts):
        raise DispatchError("invalid snapshot tables")
    result = {name: records.get(name, []) for name in SNAPSHOT_TABLES}
    if any(not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows)
           for rows in result.values()):
        raise DispatchError("invalid snapshot records")
    result = {name: [dict(row) for row in rows] for name, rows in result.items()}
    _normalize_routes(result, current='ledger_mode' in supplied)
    return result


def _normalize_routes(records, *, current):
    mode = records['ledger_mode']
    if not current:
        mode = records['ledger_mode'] = [{'id': 1, 'mode': 'isolated'}]
    if (len(mode) != 1 or set(mode[0]) != {'id', 'mode'} or type(mode[0]['id']) is not int
            or mode[0]['id'] != 1 or mode[0]['mode'] not in ('isolated','mixed')):
        raise DispatchError('invalid snapshot ledger mode')
    mixed = mode[0]['mode'] == 'mixed'
    for table in ROUTED_TABLES:
        for row in records.get(table, []):
            if ('execution_route' in row) != current:
                raise DispatchError('execution route columns disagree with the snapshot format')
            if not current:
                row['execution_route'] = None
            route = row['execution_route']
            if route is not None and (not mixed or route not in ROUTES):
                raise DispatchError('invalid snapshot execution route')
    _validate_route_audit(records, mixed)
    if mixed:
        validate_mixed_records(records)


def _validate_route_audit(records, mixed):
    actions = records['route_audit']
    if not mixed and actions:
        raise DispatchError('isolated snapshots cannot carry mixed route audit')
    activations = 0
    identities = {}
    for row in actions:
        if set(row) != {'seq','action','object_table','object_id','execution_route','actor','at',
                        'evidence_ref','pre_state_hash','fence'}:
            raise DispatchError('invalid route audit columns')
        action, table, route, fence = row['action'], row['object_table'], row['execution_route'], row['fence']
        identity, ref, previous = row['object_id'], row['evidence_ref'], row['pre_state_hash']
        if any(not isinstance(row[key], str) or not row[key] for key in ('object_id','actor','at')):
            raise DispatchError('invalid route audit identity')
        if action == 'activate' and table == 'ledger_mode' and identity == '1' and route is None and \
                fence is None and previous is None and _digest(ref):
            activations += 1
        elif action == 'bind' and table in ROUTED_TABLES and route in ROUTES and _digest(ref) and \
                _digest(previous) and ((type(fence) is int and fence >= 0) if table == 'tasks' else fence is None):
            pass
        elif action == 'register' and table == 'devices' and route in ROUTES and \
                ref is None and previous is None and fence is None:
            pass
        elif action == 'disable' and table == 'devices' and route is None and \
                ref is None and _digest(previous) and fence is None:
            pass
        else:
            raise DispatchError('invalid route audit action')
        if (table, identity) in identities:
            raise DispatchError('duplicate route audit identity')
        identities[table, identity] = route
    if mixed and activations != 1:
        raise DispatchError('mixed snapshot requires exactly one activation record')
    for device in records.get('devices', []):
        key = ('devices', device['device_id'])
        if mixed and (key not in identities or identities[key] != device['execution_route']):
            raise DispatchError('mixed snapshot device route disagrees with audit')
    for table in ('tasks', 'schedules', 'schedule_firings'):
        for entity in records[table]:
            key = (table, entity[ROUTED_TABLES[table]])
            if key in identities and identities[key] != entity['execution_route']:
                raise DispatchError('mixed snapshot route disagrees with audit')


def import_snapshot_records(conn, records):
    """Use inside store._tx(); restore all rows, force schedules.enabled=0.

    No commit or external call occurs here. Source enabled states stay in the
    hashed bundle and intentional runtime changes are reported to the caller.
    """
    records = _normalize_snapshot(records)
    _settled(records)
    present = _present(conn)
    content_tables = set(SNAPSHOT_TABLES) - {"schema_migrations", 'ledger_mode'}
    if ledger_mode(conn) != 'isolated':
        raise DispatchError('import requires an isolated target ledger')
    if _unknown_populated(conn) or any(conn.execute("SELECT 1 FROM " + name + " LIMIT 1").fetchone()
                                     for name in present & content_tables):
        raise DispatchError("import requires empty store, including extension tables")
    if records["schedules"] or records["schedule_firings"] or records["schedule_events"]:
        from .scheduler import initialize_schema
        initialize_schema(conn)
    if records["unified_events"]:
        from .unified_events import initialize_schema
        initialize_schema(conn)
    migration_preserved = not bool(records["schema_migrations"])
    if records["schema_migrations"]:
        if "schema_migrations" not in present:
            raise DispatchError("target must initialize source schema migrations before import")
        target = {r["version"]: r["name"] for r in conn.execute("SELECT * FROM schema_migrations")}
        source = {r["version"]: r["name"] for r in records["schema_migrations"]}
        if any(target.get(version) != name for version,name in source.items()):
            raise DispatchError("source and target schema migrations disagree")
        migration_preserved = bool(set(target)-set(source))
        for version in source:
            conn.execute('DELETE FROM schema_migrations WHERE version=?',(version,))
    conn.execute("PRAGMA defer_foreign_keys=ON")
    disabled = []
    for table in SNAPSHOT_TABLES:
        if table == 'ledger_mode':
            continue
        info = list(conn.execute("PRAGMA table_info(" + table + ")"))
        columns = {row["name"] for row in info}
        required = {row["name"] for row in info if (row["notnull"] and row["dflt_value"] is None) or row["pk"]}
        for original in records[table]:
            row = dict(original)
            if not set(row) <= columns or not required <= set(row):
                raise DispatchError("invalid snapshot columns for " + table)
            if table == "schedules":
                if row["enabled"]:
                    disabled.append(row["id"])
                row["enabled"] = 0
            if table == 'collector_cursors':
                row['authority']='historical_only'
            if table == 'claude_attempts':
                # Imported session identifiers are audit metadata, never execution rights.
                row['run_dir']=''
                row['execution_authority']='historical_only'
            try:
                conn.execute("INSERT INTO " + table + "(" + ",".join(row) + ") VALUES(" +
                             ",".join("?" for _ in row) + ")", tuple(row.values()))
            except sqlite3.IntegrityError as exc:
                raise DispatchError("invalid snapshot rows for " + table) from exc
    if conn.execute("PRAGMA foreign_key_check").fetchone():
        raise DispatchError("invalid snapshot references")
    if records['ledger_mode'][0]['mode'] == 'mixed':
        conn.execute("UPDATE ledger_mode SET mode='mixed' WHERE id=1")
    return {"disabled_schedule_ids": sorted(disabled), "schedules_enabled": 0,
            "target_migration_metadata_retained": migration_preserved,
            "rows": {name: len(rows) for name, rows in records.items()}}


def export_bundle(config, destination):
    path = Path(destination).resolve()
    if path.exists():
        raise ArtifactError("E_BUNDLE_EXISTS", "choose a new bundle directory")
    records = ledger(config.db_path, include_devices=True)
    # Direct API callers may export an unopened pre-0010 ledger. Normalize only
    # the portable records; never mutate the source database during export.
    for schedule in records.get('schedules', []):
        schedule.setdefault('max_slots', None)
    source = ArtifactStore(config.artifact_root)
    try:
        _normalize_routes(records, current=True)
        del records['devices']
        hashes = _artifact_inventory(records, source)
    except DispatchError as exc:
        raise ArtifactError('E_EXPORT_BLOCKED', str(exc)) from exc
    for attempt in records.get('claude_attempts',[]):
        attempt['run_dir']=''
        attempt['execution_authority']='historical_only'
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
        manifest = {"schema": FORMAT, "files": files, "artifacts": hashes,
                    "import_policy": {"schedules": "disabled", "devices": "excluded"},
                    "source_counts": {name: len(rows) for name, rows in records.items()}}
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
        if manifest["schema"] not in (V1_FORMAT, V2_FORMAT,V3_FORMAT,V4_FORMAT,V5_FORMAT,V6_FORMAT, FORMAT):
            raise ValueError("unsupported bundle format")
        layout = {V1_FORMAT:V1_TABLES,V2_FORMAT:V2_TABLES,V3_FORMAT:V3_TABLES,V4_FORMAT:V4_TABLES,
                  V5_FORMAT:V5_TABLES,V6_FORMAT:V6_TABLES,FORMAT:TABLES}[manifest['schema']]
        expected = {name + ".jsonl" for name in layout}
        if set(manifest["files"]) != expected or {p.name for p in path.glob("*.jsonl")} != expected:
            raise ValueError("unsupported bundle layout")
        records = {table: [] for table in TABLES}
        for table in layout:
            name = table + ".jsonl"
            file = path / name
            if not file.resolve().is_relative_to(path):
                raise ValueError("record location escapes the bundle")
            content = file.read_bytes()
            if sha256_ref(content) != manifest["files"][name]:
                raise ArtifactError("E_HASH_MISMATCH", "bundle record hash mismatch")
            records[table] = [json.loads(line) for line in content.splitlines()]
            if any(not isinstance(row, dict) for row in records[table]):
                raise ValueError("record must be an object")
        for schedule in records.get('schedules',[]):
            if ('max_slots' in schedule)!=(manifest['schema'] in (V5_FORMAT,V6_FORMAT,FORMAT)):
                raise ValueError('schedule columns do not match the declared bundle version')
        if manifest["schema"] != V1_FORMAT:
            if manifest.get("import_policy") != {"schedules": "disabled", "devices": "excluded"}:
                raise ValueError("unsupported import policy")
            if manifest.get("source_counts") != {name: len(records[name]) for name in layout}:
                raise ValueError("record counts disagree")
        _normalize_routes(records, current=manifest['schema'] == FORMAT)
        store = ArtifactStore(path / "artifacts")
        if not store.root.is_relative_to(path):
            raise ValueError("artifact root escapes the bundle")
        if _artifact_inventory(records, store,legacy=manifest['schema'] in (V1_FORMAT,V2_FORMAT)) != manifest["artifacts"]:
            raise ValueError("unexpected artifact inventory")
        blobs = {ref: store.get(ref) for ref in manifest["artifacts"]}
        return manifest, records, blobs
    except ArtifactError:
        raise
    except (KeyError, TypeError, OSError, ValueError, DispatchError) as exc:
        raise ArtifactError("E_BUNDLE_INVALID", "bundle cannot be read") from exc


def verify_bundle(path):
    manifest, records, _ = read_bundle(path)
    return {"tasks": len(records["tasks"]), "artifacts": len(manifest["artifacts"]),
            "schema": manifest["schema"], "rows": {name: len(rows) for name, rows in records.items()}}


def import_bundle(config, path, store):
    manifest, records, blobs = read_bundle(path)
    target = ArtifactStore(config.artifact_root)
    # Immutable staging may leave unreferenced blobs on rejection, never partial ledger rows.
    for blob in blobs.values():
        target.put(blob)
    with store._tx() as conn:
        report = import_snapshot_records(conn, {**records, "devices": []})
    return {"tasks": len(records["tasks"]), "artifacts": len(manifest["artifacts"]),
            "schema": manifest["schema"], **report}
