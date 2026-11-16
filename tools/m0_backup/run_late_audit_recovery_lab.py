"""Owned synthetic ignored receipts survive recovery without becoming results."""
import argparse
import base64
from contextlib import closing
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / 'dispatch')]

from tools.m0_backup import run_domain_recovery_lab as domain_lab
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.canonical import canonical_json
from sa_dispatch.clock import FakeClock
from sa_dispatch.models import DispatchError, Evidence, FenceRejected, Outcome, TaskSpec
from sa_dispatch.routing import HTTP_ROUTE, LOCAL_ROUTE
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.transport import CoordinatorAPI
from sa_dispatch_hotspot import KINDS


def rows(path, table):
    with closing(sqlite3.connect(Path(path).as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY rowid')]


def add_runtime_ignored(work):
    clock = FakeClock()
    cas = ArtifactStore(work / 'artifacts')
    store = SqliteStore(work / 'ledger.sqlite3', clock=clock, lease_ttl_seconds=1, kinds=KINDS, route=HTTP_ROUTE)
    worker = 'late-audit-worker'
    store.register_device(worker, ['fixture:audit'])
    tokens = {name: 'synthetic-fixture-' + name + '-x' * 24 for name in ('dispatcher', worker)}
    api = CoordinatorAPI(store, {k: hashlib.sha256(v.encode()).hexdigest() for k, v in tokens.items()},
                         roles={'dispatcher': ['dispatcher'], worker: ['worker']},
                         synthetic_handoff=True, artifact_store=cas)
    def call(path, payload, actor=worker):
        return api.invoke(path, payload, 'Bearer ' + tokens[actor])
    spec = TaskSpec('audit-read', 'hotspot.topic_fetch', 1, {'op': 'topic.list', 'filters': {}},
                    (), ('fixture:audit',), 'synthetic', 'none', 2)
    task_id = call('/submit', {'spec': asdict(spec), 'inputs': {}}, 'dispatcher')['task_id']
    first = call('/claim', {})['lease']
    assert first['task_id'] == task_id
    def stage(lease):
        raw = canonical_json(dict(schema='m0_deploy.synthetic_receipt.v1', synthetic=True,
                                  run_id='late-audit-fixture', task_id=task_id, fence=lease['fence']))
        ref = cas.put(raw)
        call('/artifact/put', dict(lease_id=lease['lease_id'], fence=lease['fence'], hash=ref,
                                  content_base64=base64.b64encode(raw).decode()))
        return ref, dict(status='succeeded', result={'receipt': ref}, evidence=[dict(
            kind='runtime_fixture', content_hash=ref, artifact_hash=ref, access='synthetic')])
    old_ref, old_outcome = stage(first)
    clock.advance(seconds=2)
    store.sweep()
    second = call('/claim', {})['lease']
    assert second['task_id'] == task_id and second['fence'] == 2
    accepted_ref, outcome = stage(second)
    call('/complete', dict(lease_id=second['lease_id'], fence=second['fence'], outcome=outcome))
    try:
        call('/complete', dict(lease_id=first['lease_id'], fence=first['fence'], outcome=old_outcome))
    except FenceRejected:
        pass
    else:
        raise AssertionError('late fence was accepted')
    return dict(terminal_task=next(row for row in rows(store.path, 'tasks') if row['task_id'] == task_id), old_receipt=old_ref,
                accepted_receipt=accepted_ref)


def add_ignored_claims(work, scenario):
    from knowledge.m0_content.dispatch_entry import configured
    from sa_dispatch.config import load_config
    terminal = add_runtime_ignored(work)
    cas = ArtifactStore(work / 'artifacts')
    unknown_id = scenario['task']['task_id']
    config = load_config(work / 'dispatch.toml', allowed_plugins={'research', 'research_content'})
    kinds = {key: value for group in configured(config).values() for key, value in group.items()}
    local = SqliteStore(work / 'ledger.sqlite3', lease_ttl_seconds=30, route=LOCAL_ROUTE,
                        kinds=kinds, component_config=config)
    lease, = [row for row in rows(local.path, 'leases') if row['task_id'] == unknown_id]
    ignored_ref = cas.put(canonical_json(dict(schema='m0.synthetic_ignored_result.v1', synthetic=True,
        run_id='fake-success-not-authority', task_id=unknown_id, fence=lease['fence'])))
    try:
        local.complete(lease['lease_id'], lease['fence'], Outcome(result={'receipt': ignored_ref},
            evidence=(Evidence('recovery_ignored_claim', ignored_ref, 'synthetic', artifact_hash=ignored_ref),)),
            lease['device_id'])
    except FenceRejected:
        pass
    else:
        raise AssertionError('unknown task accepted a late success')
    assert local.get_task(unknown_id).status == 'unknown'
    return dict(rows=rows(local.path, 'late_results'), **terminal, unknown_claim_receipt=ignored_ref,
                classification='unknown_submitter_preserved_untrusted_audit_not_accepted')


def create_source(work):
    domain_lab.create_source(work, with_schedules=True)
    scenario = json.loads((work / 'scenario.json').read_bytes())
    scenario['late_audit'] = add_ignored_claims(work, scenario)
    domain_lab.write(work / 'scenario.json', scenario)


def child(*args):
    done = subprocess.run([sys.executable, '-B', str(Path(__file__).resolve()), *map(str, args)],
                          capture_output=True, text=True, encoding='utf-8', timeout=180)
    if done.returncode:
        raise RuntimeError(done.stderr + done.stdout)
    return done


def verify(work, scenario, restored):
    from knowledge.m0_content.dispatch_entry import configured
    from sa_dispatch.config import load_config
    dbpath = restored['dispatch_db']
    expected = scenario['late_audit']
    assert rows(dbpath, 'late_results') == expected['rows']
    task_before = next(t for t in rows(dbpath, 'tasks') if t['task_id'] == scenario['task']['task_id'])
    assert task_before['status'] == 'unknown'
    cas = ArtifactStore(restored['artifact_root'])
    for key in ('old_receipt', 'accepted_receipt', 'unknown_claim_receipt'):
        assert json.loads(cas.get(expected[key]))['synthetic'] is True
    config = load_config(domain_lab.recovered_config(work, restored), allowed_plugins={'research', 'research_content'})
    kinds = {key: value for group in configured(config).values() for key, value in group.items()}
    local = SqliteStore(dbpath, lease_ttl_seconds=30, route=LOCAL_ROUTE, kinds=kinds, component_config=config)
    # Establish valid historical invocation first, so rejection below cannot be
    # attributed to a missing route, component, config or original intent.
    local.original_invocation(task_before['task_id'], task_before['fence'], cas)
    before = Path(dbpath).read_bytes()
    claim = next(row for row in expected['rows'] if row['task_id'] == task_before['task_id'])
    try:
        local.resolve_unknown(task_before['task_id'], 'confirmed_done', 'fixture',
            outcome=Outcome.from_dict(json.loads(claim['outcome_json'])),
            evidence_ref=expected['unknown_claim_receipt'], expected_fence=task_before['fence'], artifacts=cas)
    except DispatchError:
        pass
    else:
        raise AssertionError('ignored claim accepted as reconciliation evidence')
    assert Path(dbpath).read_bytes() == before
    result = domain_lab.verify_restored(work, scenario, restored)
    assert rows(dbpath, 'late_results') == expected['rows']
    terminal = next(t for t in rows(dbpath, 'tasks') if t['task_id'] == expected['terminal_task']['task_id'])
    assert terminal == expected['terminal_task']
    reconciled = next(t for t in rows(dbpath, 'tasks') if t['task_id'] == task_before['task_id'])
    assert expected['unknown_claim_receipt'] not in reconciled['result_json']
    result['ignored_audit'] = dict(count=len(expected['rows']), rows_unchanged=True,
        terminal_result_unchanged=True, unknown_remained_until_original_lookup=True,
        ignored_claim_not_used_for_result=True, late_as_reconciliation_rejected=True,
        classification=expected['classification'])
    return result


def run_lab():
    from tools.m0_backup.domain_checkpoint import capture_checkpoint, restore_checkpoint, review_late_audit
    with tempfile.TemporaryDirectory(prefix='sa-late-audit-recovery-') as directory:
        root = Path(directory).resolve()
        source = root / 'source'
        source.mkdir()
        stopped = child('--create', source)
        scenario = json.loads((source / 'scenario.json').read_bytes())
        review = review_late_audit(source)
        package = root / 'checkpoint'
        anchor = capture_checkpoint(source, package, stopped_processes=[stopped], creator='late-audit-fixture',
                                    code_revision='working-tree', late_audit_review=review)
        hidden = root / 'original-unavailable'
        assert source.resolve().is_relative_to(root) and not hidden.exists()
        source.rename(hidden)
        restored = restore_checkpoint(package, root / 'restored', expected_manifest_sha256=anchor,
                                      mode='synthetic_rehearsal_lookup_only')
        data = root / 'verify.json'
        domain_lab.write(data, dict(scenario=scenario, restored={k: str(v) if isinstance(v, Path) else v
                                                               for k, v in restored.items()}))
        report = json.loads(child('--verify', data).stdout)
        return dict(ok=True, manifest_sha256=anchor, late_audit_review=review, verification=report,
                    scope='owned synthetic fixture only; no services or real data changed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--create', type=Path)
    parser.add_argument('--verify', type=Path)
    args = parser.parse_args()
    if args.create:
        create_source(args.create.resolve())
    elif args.verify:
        data = json.loads(args.verify.read_bytes())
        print(json.dumps(verify(args.verify.parent, **data)))
    else:
        print(json.dumps(run_lab()))


if __name__ == '__main__':
    main()
