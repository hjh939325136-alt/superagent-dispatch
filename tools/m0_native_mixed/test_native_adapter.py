"""Real 0012 ledger, staged domains, recovery archive and collector; fake systemd."""
from contextlib import closing, nullcontext, redirect_stderr
from dataclasses import asdict, replace
import copy
from io import StringIO
import json
from pathlib import Path, PurePosixPath
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dispatch.sa_dispatch.collector import Collector, SourceBinding
from dispatch.sa_dispatch.artifacts import ArtifactStore
from dispatch.sa_dispatch.bundle import ledger, portable
from dispatch.sa_dispatch.clock import FakeClock
from dispatch.sa_dispatch.models import TaskSpec, Outcome, Evidence, FenceRejected
from dispatch.sa_dispatch.store_sqlite import SqliteStore
from dispatch.sa_dispatch.routing import row_digest
from tools.m0_deploy.install_target import InstallTargetError
from tools.m0_native_mixed import native_adapter
from tools.m0_native_mixed.bootstrap import guard_dropin
from tools.m0_native_mixed.domains import stage_domains, ROOT
from tools.m0_native_mixed.maintenance import Coordinator, MaintenanceError, canonical, digest, guard_check
from tools.m0_native_mixed.native_adapter import NativeAdapter, NativeLayout, SystemdService, STREAMS, local_run


class ProcessLoss(BaseException): pass


class FakeSystemd:
    def __init__(self):
        self.running = True
        self.starts = 0
        self.guards = True
        self.untracked = False

    def stop(self): self.running = False
    def start(self):
        self.running = True
        self.starts += 1
    def verify_stopped(self):
        if self.running or self.untracked: raise MaintenanceError('untracked writer')
    def verify_guards(self):
        if not self.guards: raise MaintenanceError('guard not installed')
    def verify_permissions(self): pass
    def health(self):
        if not self.running: raise MaintenanceError('service stopped')


class NativeAdapterTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix='native-adapter-')
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.layout = NativeLayout.fixture(self.root, ROOT)
        l = self.layout
        # Must run before mkdir, SqliteStore, stage_domains or any file write.
        l.assert_fixture_contained(self.root)
        for path in (l.config_root, l.ledger.parent, l.artifacts, l.domain.parent,
                     l.legacy_bindings, l.maintenance_lock.parent, l.http_journal,
                     *(p.parent for p in l.service_files)):
            path.mkdir(parents=True, exist_ok=True)
        l.maintenance_lock.write_bytes(b'0')
        l.credentials.write_bytes(b'never-read-this-fixture-credential')
        for path in l.service_files: path.write_bytes(b'[Service]\nUser=sa-dispatch\n')
        self.store = SqliteStore(l.ledger, lease_ttl_seconds=10)
        self.store.register_device('worker', ['component:mgmtcore'])
        self.store.register_device('old-test-worker', ['lab:old-test'])
        task, _ = self.store.submit(TaskSpec('synthetic-adapter', 'synthetic.read', 1, {}, (), (), 'synthetic', 'none', 2))
        self.store.cancel(task.task_id, 'synthetic-adapter')
        self.task = task.task_id
        self.late_review = self.add_late_fixture() if getattr(self, 'with_late_audit', False) else None
        stage_domains(l.domain, ledger=l.ledger, artifacts=l.artifacts, source_root=ROOT,
                      executable=Path(sys.executable).resolve(), device_id='new-local-singleton')
        http = dict(db_path=str(l.ledger), artifact_root=str(l.artifacts), journal_dir=str(l.http_journal),
                    device_id='coordinator', capabilities=[], plugins=['hotspot'], lease_ttl_seconds=30,
                    component_timeout_seconds=20, synthetic=True)
        raw = '\n'.join(f'{k} = {json.dumps(v)}' for k,v in http.items()) + '\n[components]\n'
        (l.config_root / 'coordinator.toml').write_text(raw, encoding='utf-8')
        self.old_binding = SourceBinding.enroll('debian-lab-dispatch', 'dispatch', l.ledger,
                                                'original-instance', ['0012'])
        for stream in STREAMS: Collector(l.ledger).sync(self.old_binding, stream)
        # A schema upgrade retains the old cursors; stream row layouts are unchanged.
        with closing(sqlite3.connect(l.ledger)) as db, db:
            db.execute("UPDATE collector_cursors SET schema_version='0011'")
        self.old_binding = replace(self.old_binding, accepted_schema_versions=['0011'])
        collector = dict(schema='sa_dispatch.collector_binding.v1', target=str(l.ledger),
                         sources=[asdict(self.old_binding)])
        manager = dict(version=1, sources=[dict(source_id=self.old_binding.source_id,
            source_instance=self.old_binding.source_instance, kind='dispatch', locator=str(l.ledger),
            accepted_schema_versions=['0011'], **{'class':'internal'}, freshness_s=86400,
            options={'identity':{'device':l.ledger.stat().st_dev,'inode':l.ledger.stat().st_ino}})])
        for name, value in (('collector-binding.json', collector), ('manager-binding.json', manager)):
            (l.legacy_bindings / name).write_bytes(canonical(value))
        self.service = FakeSystemd()
        self.adapter = NativeAdapter(l, fixture=True, service=self.service, late_review=self.late_review,
            late_review_sha256=digest(canonical(self.late_review)) if self.late_review else None)
        worker = next(r for r in self.adapter.rows()['devices'] if r['device_id'] == 'worker')
        provenance = self.root / 'reviewed-public-worker.json'
        provenance.write_bytes(canonical(worker))
        l.workers.write_bytes(canonical(dict(schema='native_mixed.http_workers.v1', devices=[dict(
            device_id='worker', row_sha256=row_digest(worker), source_path=str(provenance),
            source_sha256=digest(provenance.read_bytes()))])))
        self.configs = {'coordinator.toml':raw.replace('[components]', 'execution_route = "http_synthetic"\n[components]').encode(),
                        'local.toml':(l.domain / 'local.toml').read_bytes()}
        for name,value in (('collector-binding.json', collector), ('manager-binding.json',manager)):
            candidate = copy.deepcopy(value)
            candidate['sources'][0]['accepted_schema_versions'] = ['0012']
            self.configs[name] = canonical(candidate)
        self.coordinator = Coordinator(l.config_root, 'native-test', self.adapter)
        self.plan_hash = self.adapter.stage(self.coordinator, self.configs)
        self.plan = json.loads((self.coordinator.operation / 'plan.json').read_bytes())

    def add_late_fixture(self):
        """Actual expired lease, separate accepted completion and rejected late result."""
        from tools.m0_deploy.probe import REVIEW_SCHEMA, REVIEW_CLASSIFICATION, RECEIPT_SCHEMA
        l = self.layout
        clock = FakeClock('2026-11-15T00:00:00+00:00')
        store = SqliteStore(l.ledger, clock=clock, lease_ttl_seconds=1)
        task, _ = store.submit(TaskSpec('synthetic-late-native', 'synthetic.read', 1, {}, (), (),
                                       'synthetic', 'none', 2))
        cas = ArtifactStore(l.artifacts)
        first = store.claim('worker')

        def receipt(lease):
            ref = cas.put(canonical(dict(schema=RECEIPT_SCHEMA, synthetic=True,
                run_id='native-late-review-fixture', task_id=task.task_id, fence=lease.fence)))
            with closing(sqlite3.connect(l.ledger)) as db, db:
                db.execute('INSERT INTO transport_artifact_grants VALUES(?,?,?,?,?,?)',
                    ('late-grant-' + str(lease.fence), task.task_id, lease.lease_id, ref, 'synthetic',
                     '2026-11-15T00:00:00+00:00'))
            return ref, Outcome(result={'receipt':ref}, evidence=(
                Evidence('runtime_fixture', ref, 'synthetic', artifact_hash=ref),))

        self.ignored_ref, old_outcome = receipt(first)
        clock.advance(seconds=2)
        store.sweep()
        second = store.claim('worker')
        self.accepted_ref, accepted = receipt(second)
        store.complete(second.lease_id, second.fence, accepted, 'worker')
        with self.assertRaises(FenceRejected):
            store.complete(first.lease_id, first.fence, old_outcome, 'worker')
        rows = ledger(l.ledger, include_devices=True)
        late, = rows['late_results']
        self.late_task = task.task_id
        releases = l.maintenance_lock.parent / 'releases'
        return dict(schema=REVIEW_SCHEMA, classification=REVIEW_CLASSIFICATION,
            basis='Synthetic regression: expired fence retained as ignored audit, distinct successful completion',
            binding=dict(service='sa-dispatch.service', ledger=str(l.ledger), artifacts=str(l.artifacts),
                old_release=str(releases / 'old-runtime'), release=str(releases / 'current-runtime'),
                release_manifest_sha256='a' * 64),
            rows=[dict(id=late['id'], task_id=late['task_id'], fence=late['fence'], disposition='ignored',
                status='succeeded', row_sha256=digest(canonical(late)),
                cas={'ref':self.ignored_ref, 'schema':RECEIPT_SCHEMA})])

    def run_operation(self): return self.adapter.run(self.coordinator, self.plan_hash)

    def test_real_core_recovery_activation_adoption_and_completed_noop(self):
        before = self.adapter.rows()
        result = self.run_operation()
        self.assertEqual(result['phase'], 'completed')
        after = self.adapter.rows()
        self.assertEqual(after['tasks'], before['tasks'])
        self.assertEqual(after['unified_events'], before['unified_events'])
        self.assertEqual(after['late_results'], before['late_results'])
        devices = {r['device_id']:r['execution_route'] for r in after['devices']}
        self.assertEqual(devices, {'old-test-worker':None, 'worker':'http_synthetic'})
        self.assertEqual(sum(r['action'] == 'activate' for r in after['route_audit']), 1)
        new_binding = replace(self.old_binding, accepted_schema_versions=['0012'])
        for stream in STREAMS:
            receipt = self.adapter.adopt_stream(self.plan, stream)
            self.assertEqual(receipt, result['adopted'][stream])
            self.assertEqual(Collector(self.layout.ledger).sync(new_binding, stream)['added'], 0)
        snapshot = self.layout.ledger.read_bytes()
        self.assertEqual(self.run_operation(), result)
        self.assertEqual(self.service.starts, 1)
        self.assertEqual(self.layout.ledger.read_bytes(), snapshot)

    def test_commit_state_gap_resumes_without_second_activation(self):
        def fault(label):
            if label == 'activation_committed': raise ProcessLoss()
        self.coordinator.fault = fault
        with self.assertRaises(ProcessLoss): self.run_operation()
        self.assertEqual(self.adapter.rows()['ledger_mode'][0]['mode'], 'mixed')
        with self.assertRaises(MaintenanceError): guard_check(self.layout.config_root)
        self.coordinator.fault = lambda label: None
        self.assertEqual(self.run_operation()['phase'], 'completed')
        self.assertEqual(sum(r['action']=='activate' for r in self.adapter.rows()['route_audit']), 1)

    def test_adoption_commit_gap_resumes_at_original_anchor(self):
        def fault(label):
            if label == 'stream_adopted:events': raise ProcessLoss()
        self.coordinator.fault = fault
        with self.assertRaises(ProcessLoss): self.run_operation()
        old = self.plan['anchor']['cursors']['events']
        self.coordinator.fault = lambda label: None
        result = self.run_operation()
        self.assertEqual(result['adopted']['events']['epoch'], old['epoch'] + 1)
        self.assertEqual(result['adopted']['events']['last_seq'], old['last_seq'])

    def test_domain_instance_drift_rejected_before_activation(self):
        with closing(sqlite3.connect(self.layout.domain / 'content.sqlite3')) as db, db:
            db.execute("UPDATE meta SET value='different-instance' WHERE key='instance'")
        with self.assertRaises(ValueError): self.run_operation()
        self.assertEqual(self.adapter.rows()['ledger_mode'][0]['mode'], 'isolated')

    def test_source_drift_and_missing_whole_operation_lock_rejected(self):
        with patch('tools.m0_native_mixed.native_adapter.source_files', return_value={}):
            with self.assertRaises(MaintenanceError): self.run_operation()
        with self.assertRaises(MaintenanceError): self.adapter.stop_writers()

    def test_untracked_writer_and_missing_guard_fail_closed(self):
        self.service.untracked = True
        with self.assertRaises(MaintenanceError): self.run_operation()
        self.assertEqual(self.service.starts, 0)

    def test_missing_guard_never_exposes_committed_activation(self):
        self.service.guards = False
        with self.assertRaises(MaintenanceError): self.run_operation()
        self.assertEqual(self.service.starts, 0)
        with self.assertRaises(MaintenanceError): guard_check(self.layout.config_root)

    def test_budget_rejection(self):
        with closing(sqlite3.connect(self.layout.ledger)) as db, db:
            db.execute("INSERT INTO control_budgets VALUES('budget','day','unit',10,1,0,'2026-01-01T00:00:00+00:00','2027-01-01T00:00:00+00:00')")
        with self.assertRaises(Exception): self.run_operation()
        self.assertEqual(self.adapter.rows()['ledger_mode'][0]['mode'], 'isolated')

    def test_unknown_lease_rejection(self):
        with closing(sqlite3.connect(self.layout.ledger)) as db, db:
            db.execute("INSERT INTO leases VALUES('unknown-lease',?,1,'worker',?,?,'unknown',NULL)",
                (self.task, '2026-01-01T00:00:00+00:00', '2026-01-01T00:01:00+00:00'))
        with self.assertRaises(MaintenanceError): self.run_operation()
        self.assertEqual(self.adapter.rows()['ledger_mode'][0]['mode'], 'isolated')

    def test_unknown_cas_between_stage_and_activation_rejected(self):
        self.adapter.cas.put(b'unplanned writer evidence')
        with self.assertRaises(MaintenanceError): self.run_operation()
        self.assertEqual(self.adapter.rows()['ledger_mode'][0]['mode'], 'isolated')

    def test_unknown_history_after_activation_cannot_resume(self):
        def fault(label):
            if label == 'activation_committed': raise ProcessLoss()
        self.coordinator.fault = fault
        with self.assertRaises(ProcessLoss): self.run_operation()
        with closing(sqlite3.connect(self.layout.ledger)) as db, db:
            db.execute("UPDATE events SET actor='unknown-writer' WHERE seq=1")
        self.coordinator.fault = lambda label: None
        with self.assertRaises(MaintenanceError): self.run_operation()

    def test_production_constructor_refuses_fixture_layout(self):
        with self.assertRaises(MaintenanceError): NativeAdapter(self.layout)


class NativeLateAuditTests(unittest.TestCase):
    with_late_audit = True
    setUp = NativeAdapterTests.setUp
    add_late_fixture = NativeAdapterTests.add_late_fixture
    run_operation = NativeAdapterTests.run_operation

    def test_reviewed_ignored_audit_and_separate_accepted_result_survive_full_operation(self):
        before = self.adapter.rows()
        cas_before = {ref:self.adapter.cas.get(ref) for ref in (self.ignored_ref, self.accepted_ref)}
        result = self.run_operation()
        self.assertEqual(result['phase'], 'completed')
        after = self.adapter.rows()
        for table in ('tasks','leases','late_results','evidence','events','transport_artifact_grants','unified_events'):
            self.assertEqual(after[table], before[table], table)
        task = next(r for r in after['tasks'] if r['task_id'] == self.late_task)
        self.assertEqual(json.loads(task['result_json']), {'receipt':self.accepted_ref})
        self.assertNotEqual(self.accepted_ref, self.ignored_ref)
        self.assertEqual(after['late_results'][0]['disposition'], 'ignored')
        recovery = self.coordinator.operation / 'recovery'
        self.assertEqual(ledger(recovery / 'ledger/dispatch.sqlite3', include_devices=True), before)
        for ref, raw in cas_before.items():
            self.assertEqual(self.adapter.cas.get(ref), raw)
            relative = self.adapter.cas.path(ref).relative_to(self.layout.artifacts)
            self.assertEqual((recovery / 'cas' / relative).read_bytes(), raw)
        self.assertEqual(self.plan['anchor']['late_review'], self.late_review)
        self.assertEqual(self.plan['anchor']['late_review_sha256'], digest(canonical(self.late_review)))
        for stream in STREAMS:
            self.assertEqual(self.adapter.adopt_stream(self.plan, stream), result['adopted'][stream])
        with self.assertRaisesRegex(ValueError, 'late receipts'):
            portable(after)

    def test_ignored_audit_commit_state_gap_keeps_same_single_activation(self):
        before = self.adapter.rows()
        def fault(label):
            if label == 'activation_committed': raise ProcessLoss()
        self.coordinator.fault = fault
        with self.assertRaises(ProcessLoss): self.run_operation()
        self.coordinator.fault = lambda label: None
        self.assertEqual(self.run_operation()['phase'], 'completed')
        after = self.adapter.rows()
        self.assertEqual(after['late_results'], before['late_results'])
        self.assertEqual(after['tasks'], before['tasks'])
        self.assertEqual(sum(r['action']=='activate' for r in after['route_audit']), 1)

    def test_unreviewed_or_wrong_hash_ignored_audit_cannot_stage(self):
        for review, approval in ((None,None), (self.late_review, '0' * 64)):
            candidate = NativeAdapter(self.layout, fixture=True, service=self.service,
                                      late_review=review, late_review_sha256=approval)
            with self.assertRaises(MaintenanceError): candidate.freeze()
        self.assertEqual(self.adapter.rows()['ledger_mode'][0]['mode'], 'isolated')

    def test_rehashed_wrong_review_profile_and_source_binding_refused(self):
        for change in ('row','schema','binding'):
            review = copy.deepcopy(self.late_review)
            if change == 'row': review['rows'][0]['row_sha256'] = '0' * 64
            if change == 'schema': review['rows'][0]['cas']['schema'] = 'untrusted.receipt.v1'
            if change == 'binding': review['binding']['ledger'] = str(self.root / 'different.sqlite3')
            candidate = NativeAdapter(self.layout, fixture=True, service=self.service,
                                      late_review=review, late_review_sha256=digest(canonical(review)))
            with self.subTest(change=change), self.assertRaises(MaintenanceError): candidate.freeze()

    def test_added_unreviewed_late_row_blocks_quiescent_activation(self):
        with closing(sqlite3.connect(self.layout.ledger)) as db, db:
            db.execute("INSERT INTO late_results SELECT 'unknown-late-id',task_id,fence,outcome_json,received_at,disposition FROM late_results")
        with self.assertRaises(MaintenanceError): self.run_operation()
        self.assertEqual(self.adapter.rows()['ledger_mode'][0]['mode'], 'isolated')

    def test_late_only_cas_corruption_is_not_hidden_by_terminal_task(self):
        self.adapter.cas.path(self.ignored_ref).write_bytes(b'invalid ignored evidence')
        with self.assertRaises(MaintenanceError): self.run_operation()
        self.assertEqual(self.adapter.rows()['ledger_mode'][0]['mode'], 'isolated')


class FixtureContainmentTests(unittest.TestCase):
    def test_debian_show_preserves_all_prestart_commands_and_rejects_ambiguity(self):
        release = PurePosixPath('/opt/sa-dispatch-native/releases') / ('2' * 64)
        layout = NativeLayout(release, config_root=PurePosixPath('/etc/sa-dispatch'),
                              ledger=PurePosixPath('/var/lib/sa-dispatch/dispatch.sqlite3'))
        guard = f'{release}/tools/m0_native_mixed/maintenance.py guard-check --native-root /etc/sa-dispatch'
        formal = f'/usr/bin/python3 -B {guard}'
        target = native_adapter.target_guard_command(layout, 'a' * 64)

        def record(executable, argv, *, ignore='no'):
            return (f'{{ path={executable} ; argv[]={argv} ; ignore_errors={ignore} ; '
                    'start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }')

        base_pre = record('/usr/bin/python3', f'/usr/bin/python3 -I -B {guard}')
        target_pre = record('/usr/bin/python3', target)
        formal_pre = record('/usr/bin/python3', formal)
        start = record('/usr/bin/flock', '/usr/bin/flock --shared --nonblock --no-fork '
            '/var/lib/sa-dispatch /usr/bin/python3 -P -s -B -m sa_dispatch '
            '--config /etc/sa-dispatch/coordinator.toml serve --identities /etc/sa-dispatch/identities.json --port 8876')
        scalars = '\n'.join((f'ExecStart={start}', 'UMask=0077', 'User=sa-dispatch', 'Group=sa-dispatch',
                             'ProtectSystem=strict', 'NoNewPrivileges=yes'))
        separate = '\n'.join(f'ExecStartPre={row}' for row in (base_pre, target_pre, formal_pre)) + '\n' + scalars
        combined = f'ExecStartPre={base_pre} {target_pre} {formal_pre}\n{scalars}'
        check = native_adapter._verify_effective_systemd
        check(layout, separate, formal, target)
        check(layout, combined, formal, target)
        check(layout, f'ExecStartPre={formal_pre}\n{scalars}', formal, None)

        for output, error in (
                (separate.replace(f'ExecStartPre={target_pre}\n', ''), 'target guard differs'),
                (separate.replace(target_pre, record('/usr/bin/python3', target, ignore='yes')), 'target guard differs'),
                (separate.replace(target_pre, record('/usr/bin/python3', target + ' --extra')), 'target guard differs'),
                (separate + '\nUser=sa-dispatch', 'duplicate effective systemd property'),
                (separate + '\nExecStartPre=  ', 'malformed effective systemd property'),
                (separate.replace('stop_time=[n/a]', 'bad metadata'), 'malformed effective systemd command')):
            with self.subTest(error=error), self.assertRaisesRegex(MaintenanceError, error):
                check(layout, output, formal, target)

    def test_service_config_metadata_accepts_root_managed_and_legacy_private_only(self):
        service = object.__new__(SystemdService)
        service.lab = SimpleNamespace(account=SimpleNamespace(pw_uid=995,pw_gid=990))
        for uid,gid,mode,accepted in ((0,990,0o640,True),(995,990,0o600,True),
                (0,991,0o640,False),(0,990,0o660,False),(995,990,0o644,False),
                (1000,990,0o600,False),(0,990,0o600,False)):
            metadata = SimpleNamespace(st_uid=uid,st_gid=gid,st_mode=mode)
            fake = SimpleNamespace(stat=lambda:metadata)
            with self.subTest(uid=uid,gid=gid,mode=oct(mode)), \
                    patch('tools.m0_native_mixed.native_adapter._safe',return_value=fake):
                if accepted: service.verify_config_file(Path('metadata-only-no-file'))
                else:
                    with self.assertRaises(MaintenanceError): service.verify_config_file(Path('metadata-only-no-file'))

    def test_local_entry_uses_fixed_owned_unit_and_workflow_dispatcher(self):
        with tempfile.TemporaryDirectory() as temp:
            layout = NativeLayout.fixture(Path(temp).resolve(), ROOT)
            with patch('tools.m0_native_mixed.native_adapter.directory_lock', return_value=nullcontext()), \
                    patch('tools.m0_native_mixed.native_adapter.guard_check') as gate, \
                    patch('tools.m0_native_mixed.native_adapter.subprocess.run') as run:
                run.return_value.returncode = 0
                self.assertEqual(local_run(layout, ['status']), 0)
                gate.assert_called_once_with(layout.config_root)
                command = run.call_args.args[0]
                self.assertIn('--unit=sa-dispatch-local.service', command)
                self.assertIn(str(ROOT / 'knowledge/m0_content/dispatch_entry.py'), command)
                self.assertIn('--shared', command)
                self.assertIn('--property=User=sa-dispatch', command)
                with self.assertRaises(MaintenanceError): local_run(layout, ['status', '--config=other'])

    def test_common_guard_and_writer_use_the_same_explicit_manifest(self):
        manifest = '8' * 64
        layout = NativeLayout(Path('/opt/sa-dispatch-native/releases') / ('9' * 64))
        option = f'--deployment-manifest {manifest}'
        self.assertIn(f'{option} -- "$@"', native_adapter.local_entry_bytes(layout,
            deployment_manifest=manifest).decode())
        self.assertIn(f' verify-target --source-root {layout.source_root} {option}\n',
            guard_dropin(layout, deployment_manifest=manifest).decode())
        self.assertNotIn('--deployment-manifest', native_adapter.local_entry_bytes(layout).decode())
        self.assertNotIn('verify-target', guard_dropin(layout).decode())

        args = ['--source-root', str(layout.source_root), '--deployment-manifest', manifest]
        with patch.object(native_adapter, 'NativeAdapter') as adapter, \
                patch.object(native_adapter, 'local_run', return_value=0) as writer, redirect_stderr(StringIO()):
            self.assertEqual(native_adapter.main(['verify-target', *args]), 0)
            adapter.assert_called_once_with(layout, deployment_manifest=manifest)
            writer.assert_not_called()
            self.assertEqual(native_adapter.main(['local-run', *args, '--', 'status']), 0)
            writer.assert_called_once_with(layout, ['status'])
            adapter.side_effect = InstallTargetError('installation target manifest SHA256 mismatch')
            writer.reset_mock()
            self.assertEqual(native_adapter.main(['verify-target', *args]), 1)
            self.assertEqual(native_adapter.main(['local-run', *args, '--', 'status']), 1)
            writer.assert_not_called()

    def test_all_rebased_paths_stay_under_temporary_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            layout = NativeLayout.fixture(root, ROOT)
            layout.assert_fixture_contained(root)
            self.assertEqual(list(root.iterdir()), [])
            # Windows rooted-current-drive and absolute-drive paths must reject
            # before any caller creates or opens them for writing.
            variants = [Path('/escaped'), Path(root.anchor) / 'escaped']
            if sys.platform == 'win32': variants.extend([Path('C:/escaped'), Path('\\escaped')])
            for path in variants:
                with self.subTest(path=str(path)), self.assertRaises(MaintenanceError):
                    replace(layout, ledger=path).assert_fixture_contained(root)
            self.assertEqual(list(root.iterdir()), [])


if __name__ == '__main__': unittest.main()
