"""Owned, temporary synthetic subprocess rehearsal; no installed service is used."""
from contextlib import closing
from datetime import datetime, timedelta, timezone
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
for directory in (ROOT, ROOT / 'dispatch'):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))


def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def query_digest(path):
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
        names = sorted(row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"))
        result = {}
        for name in names:
            quoted = '"' + name.replace('"', '""') + '"'
            rows = [list(row) for row in db.execute('SELECT * FROM ' + quoted)]
            encode = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'),
                                               default=lambda blob: {'bytes_hex': blob.hex()})
            result[name] = sorted(rows, key=encode)
        return hashlib.sha256(encode(result).encode()).hexdigest()


def create_source(work):
    from knowledge.m0_content.run_research_pipeline_lab import Commands, now, prepare
    from knowledge.m0_content.workflow_binding import request_base, search_modules
    from sa_dispatch.artifacts import ArtifactStore
    from sa_dispatch.control_policy import ControlPolicy
    from sa_dispatch.executor import receipt_bytes
    from sa_dispatch.routing import activate_mixed, activation_manifest
    from sa_dispatch.store_sqlite import SqliteStore

    binding, config = prepare(work)
    artifacts = ArtifactStore(work / 'artifacts')
    store = SqliteStore(work / 'ledger.sqlite3', lease_ttl_seconds=30)
    expiry = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    ControlPolicy(store, artifacts=artifacts).budget_upsert(
        'recovery-fixture-budget', period='fixture', unit='synthetic_units', limit_units=10,
        starts_at=(datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(), ends_at=expiry)
    with closing(store._connect()) as db:
        manifest = activation_manifest(db, actor='synthetic-recovery-lab', bindings={})
    activate_mixed(store.path, actor='synthetic-recovery-lab',
                   evidence_ref=artifacts.put(receipt_bytes(manifest)), bindings={}, artifacts=artifacts)
    config.write_text('execution_route = "local_synthetic"\n' + config.read_text(encoding='utf-8'),
                      encoding='utf-8')
    commands = Commands(work, config)
    commands.cli('device-register')
    base = request_base(now())
    search = commands.submit('research.search', base | {'q': '剪辑', 'limit': 20}, 'recovery-search')
    found = commands.execute(search)
    selected = [next(item['ref'] for item in found['results'] if
                     item['ref'].startswith(domain + '/') and item['status'] == 'current')
                for domain in base['domains']]
    assembly = commands.submit('context.assemble', base | {'workflow_binding': binding.digest,
        'refs': selected}, 'recovery-context', parent=search['task_id'], refs=selected)
    context = commands.execute(assembly)
    params = {'workflow_binding': binding.digest, 'trace_id': 'synthetic-content-game',
              'context_id': context['context_id'], 'manifest_sha256': context['manifest_sha256'],
              'template_id': binding.template_id}
    task = commands.submit('content.script_from_context', params, 'recovery-lost-ack',
                           parent=assembly['task_id'], refs=selected)
    commands.control('bind_task', {'task_id': task['task_id'], 'budget_id': 'recovery-fixture-budget',
                                   'reserve_units': 2})
    commands.execute(task, expected='unknown', crash=True)
    _, catalog, _, _ = search_modules()
    catalog.Catalog(binding.research.catalog_root).apply_batch(
        'research-scatter', 'recovery-revoke', 'revoke_locked', operator_grant=catalog.OPERATOR)
    with closing(store._connect()) as db:
        db.row_factory = sqlite3.Row
        intent = dict(db.execute("SELECT * FROM evidence WHERE task_id=? AND kind='invocation_intent'",
                                 (task['task_id'],)).fetchone())
        holds = [dict(row) for row in db.execute('SELECT * FROM control_reservations')]
    write(work / 'scenario.json', {'task': task, 'intent': intent, 'holds': holds,
        'selected': selected, 'research_sha256': binding.research.digest,
        'workflow_sha256': binding.digest, 'catalog_instance': binding.research.instance,
        'content_instance': binding.content_instance,
        'original_locators': [str(work), str(binding.research.path), str(binding.path),
                              str(binding.research.catalog_root), str(binding.content_path)]})


def recovered_config(root, restored):
    values = {'db_path': restored['dispatch_db'], 'artifact_root': restored['artifact_root'],
              'journal_dir': str(Path(restored['dispatch_db']).parent / 'journal'), 'device_id': 'research-content-fixture',
              'capabilities': ['research:read', 'signalhub:refs'], 'lease_ttl_seconds': 30,
              'component_timeout_seconds': 20, 'plugins': ['research', 'research_content'],
              'synthetic': True, 'execution_route': 'local_synthetic'}
    lines = [key + ' = ' + json.dumps(str(value) if isinstance(value, Path) else value)
             for key, value in values.items()]
    for name, source, flag, path in (
        ('research', 'knowledge/m0_search/dispatch_component.py', '--source-binding', restored['research_binding']),
        ('research_content', 'knowledge/m0_content/dispatch_component.py', '--workflow-binding', restored['workflow_binding'])):
        lines += ['[components.' + name + ']', 'platform = "any"',
                  'argv = ' + json.dumps([sys.executable, '-B', str(ROOT / source), flag, str(path)])]
    path = root / 'recovered-dispatch.toml'
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return path


def verify_restored(work, scenario, restored):
    from knowledge.m0_content.run_research_pipeline_lab import Commands, now
    from knowledge.m0_content.workflow_binding import load_workflow_binding, request_base, search_modules
    from sa_dispatch.bundle import portable, snapshot_records
    from sa_dispatch.artifacts import ArtifactError

    missing = {path: not Path(path).exists() for path in scenario['original_locators']}
    assert all(missing.values()), missing
    binding = load_workflow_binding(Path(restored['workflow_binding']))
    assert binding.digest == scenario['workflow_sha256']
    assert binding.research.digest == scenario['research_sha256']
    assert binding.research.instance == scenario['catalog_instance']
    assert binding.content_instance == scenario['content_instance']
    domain_paths = [binding.content_path, *sorted(binding.research.catalog_root.glob('*.sqlite3'))]
    before = {str(path): query_digest(path) for path in domain_paths}
    bound, _, _, _ = search_modules()
    found = bound.execute_bound('research.search', request_base(now()) | {
        'source_binding': binding.research.digest, 'q': '剪辑', 'limit': 20}, binding.research)
    assert found['results']
    assert found['revocation_basis'] == 'captured_snapshot'
    assert found['post_snapshot_revocations'] == 'unverified'
    dbpath = Path(restored['dispatch_db'])
    with closing(sqlite3.connect(dbpath.as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        records = snapshot_records(db)
    try:
        portable(records)
    except ArtifactError:
        v7_rejected = True
    else:
        raise AssertionError('v7 accepted unknown recovery ledger')
    commands = Commands(work, recovered_config(work, restored))
    task_id = scenario['task']['task_id']
    result = commands.cli('reconcile', task_id)
    assert result['verdict'] == 'confirmed_done', result
    assert result['current_usable'] is False, result
    repeat = commands.cli('reconcile', task_id, ok=False)
    denied = commands.cli('run-once', ok=False)
    with closing(sqlite3.connect(dbpath.as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        task = dict(db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone())
        intent = dict(db.execute("SELECT * FROM evidence WHERE task_id=? AND kind='invocation_intent'",
                                 (task_id,)).fetchone())
        holds = [dict(row) for row in db.execute('SELECT * FROM control_reservations')]
    assert (task['status'], task['attempts'], task['fingerprint']) == (
        'succeeded', 1, scenario['task']['fingerprint'])
    assert intent == scenario['intent'], 'original intent mutated'
    assert holds == scenario['holds'], 'lookup changed held budget'
    assert before == {str(path): query_digest(path) for path in domain_paths}, 'domain mutated'
    return {'old_locators_absent': missing, 'historical_identity_preserved': True,
            'domain_query_hashes_unchanged': before, 'v7_unknown_rejected': v7_rejected,
            'reconciliation': result, 'repeat_rejected': repeat, 'new_execution_rejected': denied,
            'held_budget_preserved': True, 'captured_revocation_preserved': True,
            'revocation_basis': 'captured_snapshot', 'post_snapshot_revocations': 'unverified'}


def run_child(*args, return_handle=False):
    process = subprocess.Popen([sys.executable, '-B', str(Path(__file__).resolve()), *map(str, args)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, encoding='utf-8')
    try:
        stdout, stderr = process.communicate(timeout=180)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        raise
    if process.returncode:
        raise RuntimeError(stderr[-7000:] + stdout[-2000:])
    return process if return_handle else subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)


def run_lab():
    from tools.m0_backup.domain_checkpoint import capture_checkpoint, restore_checkpoint
    with tempfile.TemporaryDirectory(prefix='sa-domain-recovery-') as folder:
        root = Path(folder).resolve()
        source = root / 'source'
        source.mkdir()
        stopped = run_child('--create', source, return_handle=True)
        scenario = json.loads((source / 'scenario.json').read_bytes())
        package = root / 'checkpoint'
        anchor = capture_checkpoint(source, package, stopped_processes=[stopped],
                                    creator='synthetic-recovery-lab', code_revision='working-tree')
        hidden = root / 'unavailable-source'
        assert source.resolve().is_relative_to(root) and hidden.resolve().is_relative_to(root)
        assert not hidden.exists() and source != root and hidden != root
        source.rename(hidden)
        restored = restore_checkpoint(package, root / 'restored', expected_manifest_sha256=anchor,
                                      mode='synthetic_rehearsal_lookup_only')
        info = root / 'verify-input.json'
        write(info, {'scenario': scenario, 'restored': {key: str(value) if isinstance(value, Path) else value
                                                      for key, value in restored.items()}})
        result = run_child('--verify', info)
        return {'schema': 'm0.synthetic_domain_recovery_lab.v1', 'synthetic_only': True,
                'source_process_exit': stopped.returncode, 'manifest_sha256': anchor,
                'verification': json.loads(result.stdout),
                'scope': 'owned temporary fixtures only; no original journal/config copied; no services changed'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--create', type=Path)
    mode.add_argument('--verify', type=Path)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.create:
        create_source(args.create.resolve())
        return
    if args.verify:
        data = json.loads(args.verify.read_bytes())
        print(json.dumps(verify_restored(args.verify.parent, **data), ensure_ascii=True))
        return
    report = run_lab()
    if args.report:
        write(args.report, report)
    print(json.dumps(report, ensure_ascii=True))


if __name__ == '__main__':
    main()
