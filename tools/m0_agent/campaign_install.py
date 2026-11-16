"""Install or operate the bounded two-role campaign on the accepted Debian 0013 ledger."""

import argparse
from contextlib import closing
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import time
from uuid import uuid4

if __package__ in (None, ''):
    source = Path(__file__).resolve().parents[2]
    sys.path[:0] = [str(source / 'dispatch'), str(source)]
    __package__ = 'tools.m0_agent'

from . import common_install as old


RELEASES = Path('/opt/sa-dispatch-campaign/releases')
AUTHORITY = Path('/var/lib/sa-dispatch-agent-campaign')
SYSTEMD = Path('/etc/systemd/system')
CREDENTIAL = Path('/run/credstore/provider_api_key')
ENTRY = 'tools/m0_agent/campaign_install.py'
RUNTIME = 'tools/m0_agent/campaign_runtime.py'
SCHEMA = 'superagent.agent_campaign.install.v1'
UPGRADE_SCHEMA = 'superagent.agent_campaign.upgrade.v1'
INACTIVE = ('inactive', 'failed')
_require = old._require


def _verify_release(release, manifest):
    old._source_files(release, manifest)
    for folder, _, files in os.walk(release, followlinks=False):
        _require(stat.S_IMODE(Path(folder).stat().st_mode) == 0o555, 'campaign release directory is not frozen')
        for name in files:
            _require(stat.S_IMODE((Path(folder) / name).stat().st_mode) == 0o444,
                     'campaign release file is not frozen')
    from .common_package import verify_source

    verify_source(release, expected_manifest_sha256=manifest)


def _unit_bytes(release, machine, manifest):
    from . import campaign_runtime as runtime

    guard = (f'ExecStartPre=+/usr/bin/python3 -I -B {release / ENTRY} guard '
             f'--expected-machine {machine} --manifest-sha256 {manifest}\n')
    lock = f'/usr/bin/flock --shared --nonblock --no-fork {old.LEDGER.parent}'
    units = {}
    for role, account in runtime.ACCOUNTS.items():
        service = account + '.service'
        seconds = {'producer': '00,30', 'reviewer': '15,45'}[role]
        units[SYSTEMD / service] = (
            '[Unit]\n'
            f'Description=Dispatch authorized campaign {role}\n'
            'Wants=network-online.target\nAfter=network-online.target\n'
            f'ConditionPathExists={CREDENTIAL}\n'
            '[Service]\nType=oneshot\n'
            f'User={account}\nGroup={runtime.ROLE_GROUP}\n'
            f'SupplementaryGroups={runtime.SHARED_GROUP}\nWorkingDirectory={release}\n'
            'NoNewPrivileges=yes\nProtectSystem=strict\nProtectHome=yes\nPrivateTmp=yes\n'
            'PrivateDevices=yes\nProtectKernelTunables=yes\nProtectKernelModules=yes\n'
            'ProtectControlGroups=yes\nRestrictSUIDSGID=yes\nLockPersonality=yes\n'
            'CapabilityBoundingSet=\nRestrictAddressFamilies=AF_UNIX AF_INET AF_INET6\n'
            'UMask=0007\nTimeoutStartSec=85\n'
            f'ReadWritePaths={old.LEDGER.parent}\n'
            f'LoadCredential=provider_api_key:{CREDENTIAL}\n{guard}'
            f'ExecStart={lock} /usr/bin/python3 -I -B {release / RUNTIME} tick --role {role}\n'
        ).encode('ascii')
        units[SYSTEMD / (account + '.timer')] = (
            '[Unit]\n'
            f'Description=Wake Dispatch campaign {role}\n'
            f'[Timer]\nOnCalendar=*-*-* *:*:{seconds}\nAccuracySec=1s\nPersistent=false\nUnit={service}\n'
            '[Install]\nWantedBy=timers.target\n'
        ).encode('ascii')
    return units


def _stop_old():
    old._systemctl('disable', '--now', old.TIMER)
    old._systemctl('stop', old.AGENT)
    _verify_old_inactive()


def _verify_old_inactive():
    _require(old._property(old.TIMER, 'UnitFileState') == 'disabled'
             and all(old._property(unit, 'ActiveState') in INACTIVE for unit in (old.TIMER, old.AGENT)),
             'original single-slot timer or service is still enabled or active')


def _old_budget():
    from sa_dispatch.agent_control import BUDGET_ID, read_budget
    from .common_runtime import read_config

    grant = read_config(old.CONFIG / 'agent-external.json')
    with closing(sqlite3.connect(old.LEDGER.as_uri() + '?mode=ro', uri=True)) as conn, conn:
        conn.row_factory = sqlite3.Row
        conn.execute('BEGIN')
        row = dict(read_budget(conn, grant.grant, grant.expires_at))
        reservations = [dict(item) for item in conn.execute(
            'SELECT * FROM control_reservations WHERE budget_id=? ORDER BY task_id, fence', (BUDGET_ID,),
        )]
    _require(row['limit_units'] == row['used_units'] == 1 and row['reserved_units'] == 0,
             'original single-slot grant must be fully consumed before adding a campaign')
    return sha256(old._json({'budget': row, 'reservations': reservations})).hexdigest()


def _old_baseline(machine, *, full_source_check=False):
    old._root_parents(old.AUTHORITY)
    old._root_metadata(old.AUTHORITY, directory=True, private=True)
    state = json.loads(old._root_bytes(old.AUTHORITY / 'state.json', private=True),
                       object_pairs_hook=old._unique_fields)
    manifest = state['manifest_sha256']
    _require(isinstance(manifest, str) and old.HEX.fullmatch(manifest), 'invalid old baseline manifest digest')
    release = old.RELEASES / manifest
    if full_source_check:
        old._source_files(release, manifest)
    old._Installer(release, machine, manifest).guard()
    return manifest, state['ledger_identity']


def _prepare_accounts():
    import grp
    import pwd
    from . import campaign_runtime as runtime

    try:
        grp.getgrnam(runtime.ROLE_GROUP)
    except KeyError:
        old._run('/usr/sbin/groupadd', '--system', runtime.ROLE_GROUP)
    for account in runtime.ACCOUNTS.values():
        try:
            pwd.getpwnam(account)
        except KeyError:
            old._run('/usr/sbin/useradd', '--system', '--no-create-home', '--home-dir', '/nonexistent',
                     '--shell', '/usr/sbin/nologin', '--gid', runtime.ROLE_GROUP, account)
    _, group, _, _ = runtime.verify_accounts()
    return group


def _check_credential_source():
    if not os.path.lexists(CREDENTIAL):
        return
    old._root_parents(CREDENTIAL)
    old._root_metadata(CREDENTIAL, private=True)
    fs = old._run('/usr/bin/findmnt', '-n', '-o', 'FSTYPE', '--target', str(CREDENTIAL))
    _require(fs.stdout.strip() == 'tmpfs', 'campaign credential source must be private runtime memory')


def _backup():
    from .common_recovery import RecoveryLayout, capture, restore_to_quarantine

    suffix = uuid4().hex
    archive = AUTHORITY / ('pre-campaign-' + suffix)
    quarantine = AUTHORITY / ('quarantine-' + suffix)
    receipt = capture(RecoveryLayout(), archive, offline=True)
    restored = restore_to_quarantine(archive, quarantine, expected_manifest_sha256=receipt['manifest_sha256'])
    _require(receipt['ledger_version'] == '0013' and restored['restore_verified'] is True
             and restored['external_execution_enabled'] is False, 'pre-campaign quarantine recovery failed')
    return {'archive': archive.name, 'quarantine': quarantine.name,
            'manifest_sha256': receipt['manifest_sha256'], 'ledger_version': '0013', 'restore_verified': True}


def _campaign_fingerprint():
    from sa_dispatch.campaign_validation import campaign_records

    with closing(sqlite3.connect(old.LEDGER.as_uri() + '?mode=ro', uri=True)) as conn, conn:
        conn.row_factory = sqlite3.Row
        conn.execute('BEGIN')
        records = campaign_records(conn)
        return sha256(old._json({table: sorted(sha256(old._json(row)).hexdigest() for row in rows)
                                 for table, rows in records.items()})).hexdigest()


class Installer:
    def __init__(self, release, machine, manifest):
        self.release = release
        self.machine = machine
        self.manifest = manifest
        self.units = _unit_bytes(release, machine, manifest)
        self.timers = tuple(path.name for path in self.units if path.suffix == '.timer')
        self.services = tuple(path.name for path in self.units if path.suffix == '.service')
        self.state_file = AUTHORITY / 'state.json'
        self.upgrade_file = AUTHORITY / 'upgrade.json'

    def _identity(self):
        return {'schema': SCHEMA, 'machine_id': self.machine, 'manifest_sha256': self.manifest}

    def _read_state(self):
        old._root_metadata(AUTHORITY, directory=True, private=True)
        return json.loads(old._root_bytes(self.state_file, private=True), object_pairs_hook=old._unique_fields)

    def _read(self):
        state = self._read_state()
        _require(isinstance(state, dict) and all(state.get(k) == v for k, v in self._identity().items()),
                 'campaign receipt belongs to another machine or frozen release')
        _require(state['phase'] in ('prepared', 'paused', 'enabled', 'stopped'), 'invalid campaign install phase')
        return state

    def _save(self, state, phase):
        state = {**state, **self._identity(), 'phase': phase}
        old._atomic(self.state_file, old._json(state), mode=0o600)
        return state

    def _check_units(self):
        for path, raw in self.units.items():
            _require(old._root_hash(path) == sha256(raw).hexdigest(), 'campaign systemd unit changed')
            _require(old._property(path.name, 'FragmentPath') == str(path)
                     and not old._property(path.name, 'DropInPaths'), 'effective campaign systemd unit changed')

    def _anchored(self, state):
        from . import campaign_runtime as runtime

        prior_manifest, ledger = _old_baseline(self.machine)
        config = runtime.read_config()
        _require(prior_manifest == state['prior_manifest_sha256'] and ledger == state['ledger_identity']
                 and config['ledger_identity'] == ledger and config['campaign_id'] == state['campaign_id'],
                 'installed campaign binding changed')
        _require(old._root_hash(runtime.CONFIG) == state['config_sha256'], 'campaign configuration changed')
        _require(_old_budget() == state['prior_budget_sha256'], 'original single-slot budget changed')
        _require(state['recovery']['restore_verified'] is True, 'verified campaign fallback is missing')
        _verify_old_inactive()
        group = runtime.verify_execution('status')
        runtime._managed_path(runtime.LOCK, mode=0o440, group=group)
        _require(runtime.common_runtime.read_provider_network(runtime.NETWORK_CONFIG) is not None,
                 'approved common provider network configuration is missing')

    def _operate(self, operation):
        from tools.m0_native_mixed.native_adapter import directory_lock
        from . import campaign_runtime as runtime

        with directory_lock(old.LEDGER.parent, shared=True):
            return runtime.run(operation)

    def _pause_units(self):
        old._systemctl('disable', '--now', *self.timers)
        _require(all(old._property(unit, 'UnitFileState') == 'disabled'
                     and old._property(unit, 'ActiveState') in INACTIVE for unit in self.timers),
                 'campaign timers did not become disabled')
        deadline = time.monotonic() + 90
        while not all(old._property(unit, 'ActiveState') in INACTIVE for unit in self.services):
            _require(time.monotonic() < deadline, 'campaign service is still finishing its current tick')
            time.sleep(1)

    def _prepare(self, config, prior_manifest, ledger):
        from . import campaign_runtime as runtime

        _stop_old()
        prior_budget = _old_budget()
        group = _prepare_accounts()
        os.chown(runtime.CONFIG_DIR, 0, group)
        os.chmod(runtime.CONFIG_DIR, 0o750)
        os.chown(runtime.CONFIG, 0, group)
        os.chmod(runtime.CONFIG, 0o640)
        if not os.path.lexists(runtime.LOCK):
            old._atomic(runtime.LOCK, b'', mode=0o440, gid=group)
        runtime._managed_path(runtime.LOCK, mode=0o440, group=group)
        runtime.verify_execution('status')
        if not AUTHORITY.exists():
            AUTHORITY.mkdir(mode=0o700)
            old._sync_parent(AUTHORITY.parent)
        old._root_metadata(AUTHORITY, directory=True, private=True)
        http_was_running = old._property(old.HTTP, 'ActiveState') == 'active'
        try:
            old._stop_writers()
            recovery = _backup()
        finally:
            if http_was_running:
                old._systemctl('start', old.HTTP)
        _require(_old_budget() == prior_budget, 'original budget changed during backup')
        return self._save({
            'prior_manifest_sha256': prior_manifest, 'prior_budget_sha256': prior_budget,
            'ledger_identity': ledger, 'campaign_id': config['campaign_id'],
            'config_sha256': old._root_hash(runtime.CONFIG), 'recovery': recovery,
        }, 'prepared')

    def install(self):
        from . import campaign_runtime as runtime

        prior_manifest, ledger = _old_baseline(self.machine, full_source_check=True)
        config = runtime.read_config()
        _require(config['ledger_identity'] == ledger, 'campaign names a different ledger inode')
        _require(runtime.common_runtime.read_provider_network(runtime.NETWORK_CONFIG) is not None,
                 'approved common provider network configuration is missing')
        if os.path.lexists(self.state_file):
            state = self._read()
            if state['phase'] != 'prepared':
                return self.status()
        else:
            _require(all(not os.path.lexists(path) for path in self.units), 'unowned campaign unit already exists')
            state = self._prepare(config, prior_manifest, ledger)
        self._anchored(state)
        for path, raw in self.units.items():
            if os.path.lexists(path):
                _require(old._root_hash(path) == sha256(raw).hexdigest(), 'preexisting campaign unit differs')
            else:
                old._atomic(path, raw, mode=0o644)
        old._systemctl('daemon-reload')
        self._check_units()
        self._pause_units()
        campaign = self._operate('register')
        _require(_old_budget() == state['prior_budget_sha256'], 'original budget changed during registration')
        return self._save(state, 'paused') | {'campaign': campaign}

    def guard(self):
        state = self._read()
        _require(state['phase'] == 'enabled', 'campaign startup guard is closed')
        self._anchored(state)
        self._check_units()
        _check_credential_source()
        return {'phase': state['phase'], 'campaign_id': state['campaign_id']}

    def enable(self):
        state = self._read()
        _require(state['phase'] in ('paused', 'enabled'), 'campaign must be installed and eligible to resume')
        self._anchored(state)
        self._check_units()
        _check_credential_source()
        campaign = self._operate('status')
        _require(campaign['status'] in ('scheduled', 'active'), 'campaign cannot start outside its authorization')
        state = self._save(state, 'enabled')
        try:
            old._systemctl('enable', '--now', *self.timers)
            _require(all(old._property(unit, 'ActiveState') == 'active' for unit in self.timers),
                     'campaign timers did not become active')
        except (old.InstallError, RuntimeError, OSError, old.subprocess.SubprocessError):
            self._save(state, 'paused')
            self._pause_units()
            raise
        return state | {'campaign': campaign}

    def pause(self):
        state = self._read()
        _require(state['phase'] in ('enabled', 'paused', 'stopped'), 'campaign installation is still pending')
        if state['phase'] != 'stopped':
            state = self._save(state, 'paused')
        self._pause_units()
        return state | {'campaign': self._operate('status')}

    def stop(self):
        state = self.pause()
        self._anchored(state)
        campaign = self._operate('stop')
        state.pop('campaign')
        return self._save(state, 'stopped') | {'campaign': campaign}

    def status(self):
        state = self._read()
        self._anchored(state)
        if state['phase'] != 'prepared':
            self._check_units()
        timers = {unit: old._property(unit, 'ActiveState') for unit in self.timers}
        return state | {'campaign': self._operate('status'), 'timers': timers}

    def _save_upgrade(self, receipt, phase, **fields):
        receipt = {**receipt, **fields, 'phase': phase}
        old._atomic(self.upgrade_file, old._json(receipt), mode=0o600)
        return receipt

    def _read_upgrade(self, previous):
        if not os.path.lexists(self.upgrade_file):
            return None
        receipt = json.loads(old._root_bytes(self.upgrade_file, private=True), object_pairs_hook=old._unique_fields)
        _require(receipt['schema'] == UPGRADE_SCHEMA and receipt['machine_id'] == self.machine
                 and receipt['phase'] in ('draining', 'switching', 'switched', 'complete'),
                 'invalid campaign source upgrade receipt')
        if (receipt['previous_manifest_sha256'], receipt['manifest_sha256']) == (previous, self.manifest):
            return receipt
        _require(receipt['phase'] == 'complete' and receipt['manifest_sha256'] == previous,
                 'another campaign source upgrade is pending or does not match the previous manifest')
        return None

    def _plan_upgrade(self, previous, release, state):
        phase = state['phase']
        _require(state['manifest_sha256'] == previous and phase in ('enabled', 'paused', 'stopped'),
                 'expected previous manifest must identify a complete campaign installation')
        # The previous frozen executable derives its own expected unit bytes.
        probe = old._run('/usr/bin/python3', '-I', '-B', str(release / ENTRY), 'status',
                         '--expected-machine', self.machine, '--manifest-sha256', previous)
        _require(json.loads(probe.stdout).get('ok') is True, 'previous campaign startup anchor did not verify')
        self._anchored(state)
        _require(all(old._property(timer, 'ActiveState') == 'active' if phase == 'enabled'
                     else old._property(timer, 'ActiveState') in INACTIVE for timer in self.timers),
                 'campaign timer state differs from its installed phase')
        return self._save_upgrade({
            'schema': UPGRADE_SCHEMA, 'machine_id': self.machine, 'manifest_sha256': self.manifest,
            'previous_manifest_sha256': previous, 'previous_phase': phase,
            'previous_units': {path.name: old._root_hash(path) for path in self.units},
            'http_was_active': old._property(old.HTTP, 'ActiveState') == 'active',
            'config_sha256': state['config_sha256'],
        }, 'draining')

    def _upgrade_backup(self, receipt):
        # Timers are disabled and current workers have already drained.
        try:
            old._stop_writers()
            fingerprint = _campaign_fingerprint()
            recovery = _backup()
            _require(_campaign_fingerprint() == fingerprint, 'campaign ledger changed during cold backup')
            return self._save_upgrade(receipt, 'switching', recovery=recovery, ledger_sha256=fingerprint)
        finally:
            self._restore_http(receipt)

    def _restore_http(self, receipt):
        if receipt['http_was_active'] and old._property(old.HTTP, 'ActiveState') != 'active':
            old._systemctl('start', old.HTTP)

    def _switch_units(self, receipt):
        for path, raw in self.units.items():
            installed = old._root_hash(path)
            desired = sha256(raw).hexdigest()
            _require(installed in (receipt['previous_units'][path.name], desired),
                     'campaign systemd unit changed during source upgrade')
            if installed != desired:
                old._atomic(path, raw, mode=0o644)
        old._systemctl('daemon-reload')
        self._check_units()

    def _resume_upgraded_timers(self):
        # Keep the startup guard closed until both timer units are active.
        _check_credential_source()
        try:
            old._systemctl('enable', '--now', *self.timers)
            _require(all(old._property(unit, 'ActiveState') == 'active' for unit in self.timers),
                     'campaign timers did not resume after source upgrade')
        except (old.InstallError, RuntimeError, OSError, old.subprocess.SubprocessError):
            self._pause_units()
            raise

    def upgrade(self, previous):
        _require(isinstance(previous, str) and old.HEX.fullmatch(previous) and previous != self.manifest,
                 'a different explicit previous manifest SHA256 is required')
        previous_release = RELEASES / previous
        _verify_release(previous_release, previous)
        state = self._read_state()
        _require(state['machine_id'] == self.machine and state['schema'] == SCHEMA,
                 'campaign receipt belongs to a different machine or schema')
        receipt = self._read_upgrade(previous)
        if receipt is None:
            receipt = self._plan_upgrade(previous, previous_release, state)
        _require(state['config_sha256'] == receipt['config_sha256'], 'campaign authorization changed during upgrade')
        target_phase = receipt['previous_phase']
        if receipt['phase'] == 'complete':
            return self.status() | {'upgrade': receipt}
        if receipt['phase'] == 'switched' and state['manifest_sha256'] == self.manifest and state['phase'] == target_phase:
            # Startup was restored before the final diagnostic receipt was written.
            return self.status() | {'upgrade': self._save_upgrade(receipt, 'complete')}

        _require((state['manifest_sha256'], state['phase']) in (
            (previous, target_phase), (previous, 'upgrading'), (self.manifest, 'upgrading')),
            'campaign state changed during source upgrade')
        self._anchored(state)
        if state['phase'] != 'upgrading':
            # Write using the previous identity. Older startup guards reject this phase.
            old._atomic(self.state_file, old._json({**state, 'phase': 'upgrading'}), mode=0o600)
        self._pause_units()
        if receipt['phase'] == 'draining':
            receipt = self._upgrade_backup(receipt)
        self._restore_http(receipt)
        _require(_campaign_fingerprint() == receipt['ledger_sha256'], 'campaign ledger changed during source upgrade')
        self._switch_units(receipt)
        state = self._save({**state, 'source_upgrade_recovery': receipt['recovery']}, 'upgrading')
        receipt = self._save_upgrade(receipt, 'switched')
        self._anchored(state)
        campaign = self._operate('status')
        _require(campaign['status'] != 'unregistered', 'registered campaign is missing after source upgrade')
        if target_phase == 'enabled':
            self._resume_upgraded_timers()
        timers = {unit: old._property(unit, 'ActiveState') for unit in self.timers}
        state = self._save(state, target_phase)
        return state | {'campaign': campaign, 'timers': timers, 'upgrade': self._save_upgrade(receipt, 'complete')}

    def rotate_check(self, candidate_path):
        from sa_dispatch.artifacts import ArtifactStore
        from sa_dispatch.campaign_validation import DEVICES, budget_id, validate_config
        from sa_dispatch.clock import Clock, timestamp
        from tools.m0_deploy.install_target import _read_protected
        from tools.m0_native_mixed.native_adapter import directory_lock
        from . import campaign_runtime as runtime
        from .campaign_control import _CampaignStore

        _require(candidate_path.is_absolute() and '..' not in candidate_path.parts,
                 'rotation candidate must be a protected absolute configuration path')
        current = self.status()
        campaign = current['campaign']
        _require(campaign['status'] in ('paused', 'expired') and campaign['request_units_held'] == 0,
                 'existing campaign must be terminal with no held requests')
        candidate = validate_config(json.loads(
            _read_protected(candidate_path, limit=runtime.common_runtime.CONFIG_LIMIT),
            object_pairs_hook=old._unique_fields,
        ))
        _require(candidate['machine_id'] == self.machine and candidate['ledger_identity'] == current['ledger_identity']
                 and candidate['campaign_id'] != campaign['campaign_id']
                 and candidate['authorization_id'] != campaign['authorization_id'],
                 'rotation candidate must have new identities and use the same machine and ledger')
        _require(candidate['starts_at'] > timestamp(Clock().now()), 'rotation candidate must start in the future')
        with directory_lock(old.LEDGER.parent, shared=True):
            store = _CampaignStore(old.LEDGER, ArtifactStore(old.CAS), candidate)
            _require(store.status()['status'] == 'unregistered', 'rotation candidate is already registered')
            with closing(store._connect()) as conn:
                used = conn.execute('SELECT 1 FROM control_budgets WHERE budget_id=?', (budget_id(candidate),)).fetchone()
            _require(used is None, 'rotation candidate authorization is already registered')
            for role in DEVICES:
                store._bound_text(store._prompt(role))
        return {'dry_run': True, 'campaign_id': candidate['campaign_id'], 'starts_at': candidate['starts_at'],
                'expires_at': candidate['expires_at'], 'slots': candidate['slots'],
                'max_requests': candidate['max_requests'], 'max_tokens': candidate['max_tokens']}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('command', choices=('install', 'guard', 'enable', 'pause', 'stop', 'status', 'upgrade',
                                           'rotate-check'))
    parser.add_argument('--expected-machine', required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--expected-previous-manifest')
    parser.add_argument('--candidate-config', type=Path)
    args = parser.parse_args(argv)
    if (args.command == 'upgrade') != (args.expected_previous_manifest is not None):
        parser.error('--expected-previous-manifest is required for upgrade only')
    if (args.command == 'rotate-check') != (args.candidate_config is not None):
        parser.error('--candidate-config is required for rotate-check only')
    try:
        _require(sys.platform == 'linux' and os.geteuid() == os.getuid() == 0,
                 'campaign installer requires Linux root')
        _require(re.fullmatch(r'[0-9a-f]{32}', args.expected_machine) and old.HEX.fullmatch(args.manifest_sha256),
                 'explicit machine ID and frozen campaign manifest digest are required')
        release = RELEASES / args.manifest_sha256
        _require(Path(__file__).absolute() == release / ENTRY, 'run the campaign installer from its frozen release')
        _verify_release(release, args.manifest_sha256)
        old._root_parents(AUTHORITY)
        old._root_parents(SYSTEMD / 'campaign-unit')
        from tools.m0_deploy.install_target import observe_install_target
        from tools.m0_native_mixed.maintenance import _lock

        machine, _ = observe_install_target()
        _require(machine == args.expected_machine, 'explicit machine ID differs from this host')
        os.umask(0o007)
        installer = Installer(release, machine, args.manifest_sha256)
        if args.command in ('status', 'guard'):
            result = getattr(installer, args.command)()
        else:
            with _lock(old.MAINTENANCE_LOCK):
                if args.command == 'upgrade':
                    result = installer.upgrade(args.expected_previous_manifest)
                elif args.command == 'rotate-check':
                    result = installer.rotate_check(args.candidate_config)
                else:
                    result = getattr(installer, args.command)()
    except Exception as error:
        result = {'ok': False, 'operation': args.command, 'error': type(error).__name__}
        if isinstance(error, old.InstallError):
            result['detail'] = str(error)
        print(json.dumps(result, sort_keys=True))
        return 1
    print(json.dumps({'ok': True, **result}, sort_keys=True, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
