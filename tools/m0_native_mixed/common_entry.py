"""Fixed plan, run, and read-only status entry for a frozen common release."""

import argparse
from contextlib import closing
import json
from pathlib import Path
import re
import stat
import sys
import tomllib

if __package__ in (None, ''):
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dispatch.sa_dispatch.collector import readonly
from dispatch.sa_dispatch.routing import ledger_mode
from tools.m0_deploy.install_target import _read_protected, validate_install_target, validate_manifest_sha256
from tools.m0_native_mixed.bootstrap import MARKER
from tools.m0_native_mixed.fresh_install import read_fresh_proof
from tools.m0_native_mixed.maintenance import Coordinator, MaintenanceError, _load, _read, _safe, canonical, digest, guard_check
from tools.m0_native_mixed.native_adapter import CONFIGS, NativeAdapter, NativeLayout, ROOT, STREAMS, _identity, _tree, source_files


RELEASES = Path('/opt/sa-dispatch-native/releases')
ENTRY_SCHEMA = 'superagent.common.native_entry.v1'


def _root_metadata(path, *, directory):
    info = _safe(path).stat()
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if (not expected(info.st_mode) or info.st_uid != 0 or info.st_gid != 0 or info.st_mode & 0o022 or
            (not directory and info.st_nlink != 1)):
        raise MaintenanceError('common native ownership or permissions differ')


def _protected(root, *, directory):
    for ancestor in reversed(root.parents):
        _root_metadata(ancestor, directory=True)
    _root_metadata(root, directory=directory)


def _record(path, *, fixture, limit=256 * 1024):
    try:
        if fixture:
            with _safe(path).open('rb') as stream:
                raw = stream.read(limit + 1)
        else:
            raw = _read_protected(path, limit=limit)
        value = json.loads(raw)
        if len(raw) > limit or not isinstance(value, dict) or canonical(value) != raw:
            raise ValueError('noncanonical record')
        return value
    except (OSError, ValueError, RecursionError) as exc:
        raise MaintenanceError('invalid protected common native record') from exc


def _verify_release(layout, *, fixture):
    release = _safe(layout.source_root)
    source_sha = validate_manifest_sha256(release.name)
    if not fixture:
        _protected(release, directory=True)
        for path in release.rglob('*'):
            _root_metadata(path, directory=path.is_dir())
    pinned = source_files(layout)
    if digest(canonical(pinned)) != source_sha:
        raise MaintenanceError('native release directory hash differs from pinned source')
    marker = canonical(dict(schema='native_mixed.frozen_source.v1', source_sha256=source_sha, files=pinned))
    if _tree(release) != {**pinned, MARKER: digest(marker)}:
        raise MaintenanceError('frozen native release contains unexpected or changed bytes')
    folders = {parent for name in (*pinned, MARKER) for parent in Path(name).parents}
    if any(path.relative_to(release) not in folders for path in release.rglob('*') if path.is_dir()):
        raise MaintenanceError('frozen native release contains an unlisted directory')
    return source_sha


def _verify_bootstrap(layout, source_sha, deployment_manifest, before_sha, *, fixture):
    record = _record(layout.domain.parent / 'bootstrap' / (source_sha + '.json'), fixture=fixture)
    intent = record.get('intent')
    if not isinstance(intent, dict) or record.get('phase') != 'completed':
        raise MaintenanceError('completed native bootstrap required')
    expected = dict(schema='native_mixed.bootstrap.v1', source_sha256=source_sha,
        coordinator_before_sha256=before_sha, release=str(layout.source_root),
        config_root=str(layout.config_root), domain=str(layout.domain),
        service_files_before={str(path): digest(_read(path)) for path in layout.service_files[:-1]})
    origin = intent.get('source')
    if not isinstance(origin, str) or not Path(origin).is_absolute():
        raise MaintenanceError('native bootstrap source identity missing')
    if not fixture and (Path(origin).parent != RELEASES.parent / 'incoming' or '..' in Path(origin).parts):
        raise MaintenanceError('native bootstrap source identity differs')
    expected['source'] = origin
    if not fixture or 'deployment_manifest' in intent:
        expected['deployment_manifest'] = deployment_manifest
    if intent != expected:
        raise MaintenanceError('completed native bootstrap provenance differs')


def _verify_runtime(layout, account):
    paths = [(layout.ledger.parent, True), (layout.ledger, False), (layout.http_journal, True)]
    paths.extend((path, False) for path in _safe(layout.http_journal).iterdir())
    for path, directory in paths:
        info = _safe(path).stat()
        expected = stat.S_ISDIR if directory else stat.S_ISREG
        mode = 0o700 if directory else 0o600
        if (not expected(info.st_mode) or (not directory and info.st_nlink != 1) or
                (account is not None and (info.st_uid != account.pw_uid or info.st_gid != account.pw_gid or
                                          stat.S_IMODE(info.st_mode) != mode))):
            raise MaintenanceError('common ledger or HTTP journal permissions differ')


class _Entry:
    def __init__(self, layout, *, deployment_manifest, fresh_install_sha256,
                 coordinator_before_sha256, operation_id, fixture=False, service=None):
        self.manifest = validate_manifest_sha256(deployment_manifest)
        self.fresh_sha = validate_manifest_sha256(fresh_install_sha256)
        self.before_sha = validate_manifest_sha256(coordinator_before_sha256)
        if operation_id != 'common-' + self.manifest[:24]:
            raise MaintenanceError('operation ID does not identify this common package')
        self.layout, self.fixture = layout, fixture
        if not fixture:
            validate_install_target(self.manifest)
        self.source_sha = _verify_release(layout, fixture=fixture)
        _verify_bootstrap(layout, self.source_sha, self.manifest, self.before_sha, fixture=fixture)
        self.adapter = NativeAdapter(layout, fixture=fixture, service=service,
            deployment_manifest=None if fixture else self.manifest, fresh_install_sha256=self.fresh_sha)
        _verify_runtime(layout, None if fixture else self.adapter.service.lab.account)
        if not fixture:
            _protected(layout.config_root, directory=True)
            _protected(layout.maintenance_lock, directory=False)
        self.coordinator = Coordinator(layout.config_root, operation_id, self.adapter)

    def _proof(self):
        return read_fresh_proof(self.layout, self.fresh_sha, self.manifest, self.adapter._old_binding(),
                                _identity(self.layout.ledger), fixture=self.fixture)

    def _configs(self):
        layout = self.layout
        before = _read(layout.config_root / 'coordinator.toml')
        isolated = tomllib.loads(before.decode('utf-8'))
        expected = dict(db_path=str(layout.ledger), artifact_root=str(layout.artifacts),
            journal_dir=str(layout.http_journal), device_id='coordinator', capabilities=[], plugins=['hotspot'],
            synthetic=True, lease_ttl_seconds=30, component_timeout_seconds=20, components={})
        if digest(before) != self.before_sha or before.startswith(b'\xef\xbb\xbf') or isolated != expected:
            raise MaintenanceError('common isolated coordinator configuration differs')
        return {'coordinator.toml': b'execution_route = "http_synthetic"\n' + before,
                'local.toml': _read(layout.domain / 'local.toml'),
                **{name: _read(layout.legacy_bindings / name)
                   for name in ('collector-binding.json', 'manager-binding.json')}}

    def _operation(self, plan_sha256):
        validate_manifest_sha256(plan_sha256)
        coordinator = self.coordinator
        if not self.fixture:
            for path in (coordinator.base, coordinator.operation):
                _root_metadata(path, directory=True)
            for name in ('plan.json', 'state.json'):
                _root_metadata(coordinator.operation / name, directory=False)
            _root_metadata(coordinator.base / 'lock', directory=False)
        operation, plan, state = _load(coordinator.root, coordinator.operation_id, plan_sha256)
        if not self.fixture:
            for item in plan['configs']:
                _root_metadata(operation / item['blob'], directory=False)
        matches = [item for item in plan['configs'] if item['path'] == 'coordinator.toml']
        if (len(plan['configs']) != len(CONFIGS) or {item['path'] for item in plan['configs']} != CONFIGS or
                plan['streams'] != STREAMS or len(matches) != 1 or matches[0]['before_sha256'] != self.before_sha or
                not self.adapter._uses_fresh(plan)):
            raise MaintenanceError('operation does not identify the reviewed common configuration')
        return plan, state

    def plan(self):
        self._proof()
        configs = self._configs()
        operation = _safe(self.coordinator.operation)
        if operation.exists():
            plan_sha = digest(_read(operation / 'plan.json'))
            plan, state = self._operation(plan_sha)
            if (state['phase'] != 'staged' or
                    {item['path']: item['after_sha256'] for item in plan['configs']} !=
                    {name: digest(raw) for name, raw in configs.items()}):
                raise MaintenanceError('existing operation is outside the retryable staged phase')
            self.adapter.validate_plan(plan)
        else:
            plan_sha = self.adapter.stage(self.coordinator, configs)
            _, state = self._operation(plan_sha)
        return {'plan_sha256': plan_sha, 'phase': state['phase']}

    def run(self, plan_sha256):
        self._operation(plan_sha256)
        state = self.adapter.run(self.coordinator, plan_sha256)
        return {'plan_sha256': plan_sha256, 'phase': state['phase']}

    def status(self, plan_sha256):
        plan, state = self._operation(plan_sha256)
        completed = state['phase'] == 'completed'
        if completed:
            guard_check(self.layout.config_root)
        proof, hashes = self._proof()
        if (hashes != plan['anchor'].get('fresh_files') or
                digest(canonical(plan['anchor']['rows'])) != proof['baseline_rows_sha256']):
            raise MaintenanceError('common native operation evidence drift')
        with closing(readonly(self.layout.ledger)) as conn:
            mode = ledger_mode(conn)
            cursors = [dict(row) for row in conn.execute('''SELECT stream, schema_version, epoch, last_seq
                FROM collector_cursors WHERE source_id=? ORDER BY stream''', (proof['source_binding']['source_id'],))]
            gaps = conn.execute('SELECT count(*) FROM collector_gaps').fetchone()[0]
        if len(cursors) != len(STREAMS) or {row['stream'] for row in cursors} != set(STREAMS):
            raise MaintenanceError('common native operation lost a Collector stream')
        result = {'plan_sha256': plan_sha256, 'phase': state['phase'], 'ledger_mode': mode,
                  'collector_cursors': cursors, 'collector_gap_count': gaps}
        if completed:
            if mode != 'mixed':
                raise MaintenanceError('completed common installation requires a mixed ledger')
            self.adapter.health(plan)
            result['runtime_healthy'] = True
        return result


def fixture_entry(layout, *, fixture_root, service, **authority):
    root = _safe(fixture_root)
    if (not root.is_dir() or root == Path(root.anchor) or service is None or
            layout.source_root.parent != root / 'opt/sa-dispatch-native/releases' or
            layout != NativeLayout.fixture(root, layout.source_root)):
        raise MaintenanceError('common native fixture must use exact bounded temporary paths')
    layout.assert_fixture_contained(root)
    return _Entry(layout, service=service, fixture=True, **authority)


def _production_entry(**authority):
    if ROOT.parent != RELEASES or Path(__file__).absolute() != ROOT / 'tools/m0_native_mixed/common_entry.py':
        raise MaintenanceError('common native entry must execute from its fixed frozen release')
    return _Entry(NativeLayout(ROOT), **authority)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Fixed common mixed activation entry', allow_abbrev=False)
    parser.add_argument('command', choices=('plan', 'run', 'status'))
    parser.add_argument('--deployment-manifest', required=True)
    parser.add_argument('--fresh-install-sha256', required=True)
    parser.add_argument('--coordinator-before-sha256', required=True)
    parser.add_argument('--operation-id', required=True)
    parser.add_argument('--plan-sha256')
    args = vars(parser.parse_args(argv))
    command, plan_sha = args.pop('command'), args.pop('plan_sha256')
    if (command == 'plan') != (plan_sha is None):
        parser.error('--plan-sha256 must be supplied exactly for run or status')
    try:
        entry = _production_entry(**args)
        result = entry.plan() if command == 'plan' else getattr(entry, command)(plan_sha)
    except Exception as exc:
        print(json.dumps({'schema': ENTRY_SCHEMA, 'ok': False, 'error': type(exc).__name__}))
        return 1
    print(json.dumps({'schema': ENTRY_SCHEMA, 'ok': True, **result}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
