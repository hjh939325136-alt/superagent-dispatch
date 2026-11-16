import errno
from io import BytesIO
import json
from pathlib import Path, PurePosixPath
import stat
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tools.m0_deploy import install_target as binding


MANIFEST = 'a' * 64
MACHINE_ID = 'b' * 32
HOSTNAME = 'fresh-debian'


class InstallTargetTests(unittest.TestCase):
    def setUp(self):
        self.target = dict(schema=binding.TARGET_SCHEMA, scope=binding.TARGET_SCOPE,
                           manifest_sha256=MANIFEST, machine_id=MACHINE_ID, hostname=HOSTNAME)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.fixture = Path(temp.name) / 'binding.json'

    def write(self, target):
        self.fixture.write_bytes(json.dumps(target).encode())

    def validate(self, manifest=MANIFEST):
        with patch.object(binding, 'observe_install_target', return_value=(MACHINE_ID, HOSTNAME)), \
                patch.object(binding, '_read_protected', side_effect=lambda path, limit: self.fixture.read_bytes()) as read:
            try:
                return binding.validate_install_target(manifest)
            finally:
                if read.called:
                    read.assert_called_once_with(binding.INSTALL_TARGET, limit=4096)

    def test_exact_target_and_each_identity_drift(self):
        self.write(self.target)
        self.assertEqual(self.validate(), self.target)
        for field, value, error in (
            ('machine_id', 'c' * 32, 'machine-id mismatch'),
            ('hostname', 'another-host', 'hostname mismatch'),
            ('manifest_sha256', 'c' * 64, 'manifest SHA256 mismatch'),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(binding.InstallTargetError, error):
                self.write({**self.target, field: value})
                self.validate()

    def test_schema_scope_extra_or_duplicate_fields_and_missing_manifest(self):
        for changes in (dict(schema='other'), dict(scope='other'), dict(credential='unused'), dict(hostname=42)):
            with self.subTest(changes=changes), self.assertRaisesRegex(binding.InstallTargetError, 'schema or scope'):
                self.write({**self.target, **changes})
                self.validate()
        self.fixture.write_bytes(b'{"hostname":"fresh-debian",' + json.dumps(self.target).encode()[1:])
        with self.assertRaisesRegex(binding.InstallTargetError, 'duplicate field'):
            self.validate()
        with self.assertRaisesRegex(binding.InstallTargetError, 'full deployment manifest'):
            self.validate('a' * 63)

    def test_observation_checks_host_and_requires_linux_systemd_root(self):
        def read(path, *, limit):
            return {binding.MACHINE_ID: f'{MACHINE_ID}\n'.encode(),
                    binding.HOSTNAME: f'{HOSTNAME}\n'.encode()}[path]

        with patch.object(binding, 'sys', SimpleNamespace(platform='linux')), \
                patch.object(binding, 'os', SimpleNamespace(geteuid=lambda: 0)), \
                patch.object(binding, 'Path') as path, \
                patch.object(binding, '_read_protected', side_effect=read) as protected, \
                patch.object(binding.socket, 'gethostname', return_value=HOSTNAME) as hostname:
            self.assertEqual(binding.observe_install_target(), (MACHINE_ID, HOSTNAME))
            path.assert_called_with('/run/systemd/system')
            hostname.return_value = 'wrong-live-host'
            with self.assertRaisesRegex(binding.InstallTargetError, 'configured hostname differs'):
                binding.observe_install_target()
            path.return_value.is_dir.return_value = False
            protected.reset_mock()
            with self.assertRaisesRegex(binding.InstallTargetError, 'Linux root under systemd'):
                binding.observe_install_target()
            protected.assert_not_called()

    def test_path_walk_rejects_unsafe_metadata_and_links(self):
        directory = SimpleNamespace(st_mode=stat.S_IFDIR | 0o755, st_uid=0, st_gid=0, st_nlink=3)
        regular = SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_uid=0, st_gid=0, st_nlink=1)
        observed = Mock(O_RDONLY=0, O_NOFOLLOW=1, O_CLOEXEC=2, O_DIRECTORY=4, O_NONBLOCK=8)
        observed.fstat.side_effect = lambda fd: regular if fd == 13 else directory
        observed.fdopen.side_effect = lambda *args, **kwargs: BytesIO(b'{}')
        path = PurePosixPath('/etc/sa-dispatch/install-target.json')
        for parent_mode, file_links, symlink_index, accepted in (
            (0o755, 1, None, True), (0o775, 1, None, False), (0o757, 1, None, False),
            (0o755, 2, None, False), (0o755, 1, 2, False), (0o755, 1, 3, False),
        ):
            with self.subTest(parent_mode=parent_mode, file_links=file_links, symlink_index=symlink_index):
                directory.st_mode = stat.S_IFDIR | parent_mode
                regular.st_nlink = file_links
                observed.reset_mock()
                descriptors = [10, 11, 12, 13]
                if symlink_index is not None:
                    descriptors[symlink_index] = OSError(errno.ELOOP, 'symlink')
                observed.open.side_effect = descriptors
                with patch.object(binding, 'os', observed):
                    if accepted:
                        self.assertEqual(binding._read_protected(path, limit=16), b'{}')
                    else:
                        with self.assertRaises(binding.InstallTargetError):
                            binding._read_protected(path, limit=16)
                for call in observed.open.call_args_list:
                    self.assertTrue(call.args[1] & observed.O_NOFOLLOW)

    def test_default_systemd_adapter_cannot_use_common_binding(self):
        from tools.m0_native_mixed.native_adapter import NativeLayout, SystemdService

        account = Mock()
        with patch.dict(sys.modules, {'pwd': SimpleNamespace(getpwnam=account)}):
            from tools.m0_deploy import lab

            with patch.object(lab, 'pwd', SimpleNamespace(getpwnam=account)), \
                    patch.object(lab, '_read_protected', side_effect=binding.InstallTargetError('missing')) as read, \
                    patch.object(lab, 'validate_install_target') as verify:
                layout = NativeLayout(Path('/opt/sa-dispatch-native/releases') / MANIFEST)
                with self.assertRaisesRegex(lab.deploy.DeployError, 'lab-only'):
                    SystemdService(layout)
                read.assert_called_once_with(lab.LEGACY_LAB_HOSTNAME, limit=256)
                verify.assert_not_called()
                account.assert_not_called()
                service = SystemdService(layout, deployment_manifest=MANIFEST)
                verify.assert_called_once_with(MANIFEST)
                account.assert_called_once_with('sa-dispatch')
                verify.side_effect = binding.InstallTargetError('installation target manifest SHA256 mismatch')
                with self.assertRaisesRegex(binding.InstallTargetError, 'manifest SHA256 mismatch'):
                    service.verify_guards()

    def test_legacy_adapter_requires_hostname_match(self):
        account = Mock()
        with patch.dict(sys.modules, {'pwd': SimpleNamespace(getpwnam=account)}):
            from tools.m0_deploy import lab
            with patch.object(lab, 'pwd', SimpleNamespace(getpwnam=account)), \
                    patch.object(lab, '_read_protected', return_value=b'review-lab.example\n'), \
                    patch.object(lab, 'observe_install_target', return_value=('a' * 32, 'other-lab.example')) as observe:
                with self.assertRaisesRegex(lab.deploy.DeployError, 'unexpected machine'):
                    lab.LabAdapter()
                account.assert_not_called()
                observe.return_value = ('a' * 32, 'review-lab.example')
                lab.LabAdapter()
                account.assert_called_once_with('sa-dispatch')


if __name__ == '__main__':
    unittest.main()
