"""Exercise protected descriptor reads on real Linux metadata in a temporary root."""
import os
from pathlib import Path
import sys
import tempfile
import unittest

from tools.m0_deploy.install_target import InstallTargetError, _read_protected


@unittest.skipUnless(sys.platform == 'linux' and getattr(os, 'geteuid', lambda: -1)() == 0,
                     'requires Linux root for real ownership checks')
class ProtectedTargetFilesystemTests(unittest.TestCase):
    def setUp(self):
        # /tmp is intentionally refused by the production parent-directory policy.
        temporary = tempfile.TemporaryDirectory(prefix='sa-target-test-', dir='/root')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.directory = self.root / 'owned'
        self.directory.mkdir(mode=0o700)
        self.path = self.directory / 'binding.json'
        self.path.write_bytes(b'{"synthetic":true}')
        self.path.chmod(0o600)

    def test_real_metadata_read_and_writable_parent_rejection(self):
        before = self.path.stat().st_mtime_ns
        self.assertEqual(_read_protected(self.path, limit=64), b'{"synthetic":true}')
        self.assertEqual(self.path.stat().st_mtime_ns, before)
        self.directory.chmod(0o770)
        with self.assertRaises(InstallTargetError):
            _read_protected(self.path, limit=64)
        self.directory.chmod(0o700)
        self.path.chmod(0o620)
        with self.assertRaises(InstallTargetError):
            _read_protected(self.path, limit=64)

    def test_real_links_ownership_and_size_rejection(self):
        alias = self.directory / 'alias.json'
        alias.symlink_to(self.path)
        with self.assertRaises(InstallTargetError):
            _read_protected(alias, limit=64)
        parent_alias = self.root / 'parent-alias'
        parent_alias.symlink_to(self.directory, target_is_directory=True)
        with self.assertRaises(InstallTargetError):
            _read_protected(parent_alias / self.path.name, limit=64)
        alias.unlink()
        os.link(self.path, alias)
        with self.assertRaises(InstallTargetError):
            _read_protected(self.path, limit=64)
        alias.unlink()
        os.chown(self.path, 65534, 65534)
        with self.assertRaises(InstallTargetError):
            _read_protected(self.path, limit=64)
        os.chown(self.path, 0, 0)
        with self.assertRaises(InstallTargetError):
            _read_protected(self.path, limit=4)
        self.assertEqual(_read_protected(self.path, limit=64), b'{"synthetic":true}')


if __name__ == '__main__':
    unittest.main()
