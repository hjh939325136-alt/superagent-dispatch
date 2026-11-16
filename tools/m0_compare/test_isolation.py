"""Tests for the fixed transient Hermes isolation command contract."""

import unittest

from . import hermes_host, isolation


RUN = 'case-t3-r1'
HERMES = isolation.HERMES_ROOT / isolation.HERMES_COMMIT
CODE = isolation.CODE_ROOT / RUN
BROKER = isolation.BROKER_ROOT / RUN


class IsolationTests(unittest.TestCase):
    def command(self, **fields):
        values = dict(run_id=RUN, hermes_prefix=HERMES, code_root=CODE, broker_directory=BROKER,
                      entry='driver.py')
        return isolation.unit_command(**(values | fields))

    def test_fixed_hardened_unit_and_exact_socket_allowlist(self):
        self.assertEqual(isolation.HERMES_COMMIT, hermes_host.HERMES_COMMIT)
        argv = self.command(args=('--provider', '/run/sa-compare-ipc/provider.sock', '--run-id=case-t3-r1'))
        self.assertEqual(argv[:6], ['/usr/bin/systemd-run', '--quiet', '--wait', '--collect', '--pipe',
                                   '--unit=sa-compare-hermes-case-t3-r1.service'])
        properties = dict(entry.removeprefix('--property=').split('=', 1)
                          for entry in argv if entry.startswith('--property='))
        self.assertEqual(properties['DynamicUser'], 'yes')
        self.assertEqual(properties['User'], 'sacmp-' + RUN)
        self.assertEqual(properties['PrivateNetwork'], 'yes')
        self.assertEqual(properties['RestrictAddressFamilies'], 'AF_UNIX')
        self.assertEqual(properties['CapabilityBoundingSet'], '')
        self.assertEqual(properties['ProtectSystem'], 'strict')
        self.assertEqual(properties['ProtectProc'], 'invisible')
        for path in ('/mnt', '/root', '/home', '/etc/sa-dispatch', '/var/lib/sa-dispatch', '/opt/sa-dispatch'):
            self.assertIn('-' + path, properties['InaccessiblePaths'].split())
        self.assertEqual({partition.split(':', 1)[0] for partition in properties['TemporaryFileSystem'].split()},
                         {'/opt', '/etc', '/var', '/run', '/tmp'})
        self.assertEqual(properties['BindReadOnlyPaths'].split(), [str(HERMES), str(CODE), *[
            f'{BROKER / name}:{isolation.CHILD_BROKER / name}' for name in isolation.SOCKET_NAMES]])
        self.assertNotIn('LoadCredential', properties)
        self.assertNotIn('ReadWritePaths', properties)
        launch = argv[argv.index('--') + 1:]
        self.assertEqual(launch[:2], ['/usr/bin/env', '-i'])
        self.assertIn(str(HERMES / 'venv/bin/python'), launch)
        self.assertIn(str(CODE / 'driver.py'), launch)

    def test_refuses_alternate_source_paths_unit_names_and_socket_mounts(self):
        cases = (
            {'run_id': '../../shared'}, {'run_id': 'x' * 25},
            {'hermes_prefix': isolation.HERMES_ROOT / ('a' * 40)},
            {'code_root': isolation.CODE_ROOT / 'different-case'},
            {'broker_directory': '/run/other-broker/' + RUN},
            {'entry': '../driver.py'}, {'entry': 'arbitrary.sh'},
            {'sockets': ('provider.sock', 'admin.sock')},
            {'sockets': ('provider.sock', 'provider.sock')},
            {'sockets': ('tools.sock',)},
        )
        for change in cases:
            with self.subTest(change=change), self.assertRaises(isolation.IsolationError):
                self.command(**change)

    def test_provider_only_run_mounts_no_tool_socket(self):
        argv = self.command(sockets=('provider.sock',))
        self.assertFalse(any('tools.sock' in arg for arg in argv))
        self.assertTrue(any('provider.sock:/run/sa-compare-ipc/provider.sock' in arg for arg in argv))

    def test_argv_rejects_paths_outside_curated_sources_and_private_scratch(self):
        for value in ('--path=/etc/shadow', '/run/systemd/private', '/tmp/../etc/shadow',
                      '../input.json', 'http://127.0.0.1', '--value=%n', 'shell;command'):
            with self.subTest(value=value), self.assertRaises(isolation.IsolationError):
                self.command(args=(value,))
        argv = self.command(args=(f'--input={CODE / "request.json"}', '--work-dir=/tmp/scratch'))
        self.assertIn(f'--input={CODE / "request.json"}', argv)
        self.assertIn('--work-dir=/tmp/scratch', argv)


if __name__ == '__main__':
    unittest.main()
