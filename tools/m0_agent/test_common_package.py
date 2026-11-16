"""Frozen common external source package and archive boundaries."""
from pathlib import Path
import stat
import subprocess
from tempfile import TemporaryDirectory
import unittest
import warnings
import zipfile

from tools.m0_agent import common_package as package


class CommonPackageTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory())).resolve()
        self.source = self.root / 'source'
        self.source.mkdir()
        self.git('init', '-q')
        self.forbidden = ('tools/m0_agent/test_fixture.py', 'tools/m0_agent/private.env', 'state/live.json')
        for name in package.REQUIRED | set(self.forbidden):
            path = self.source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'MIT License\n' if name == package.baseline.LICENSE else f'# fixture {name}\n'.encode())
        self.git('add', '--', '.')
        self.receipt = package.build(self.source, self.root / 'bundle.zip')
        self.archive = Path(self.receipt['path'])

    def git(self, *args):
        subprocess.run(['git', '-C', str(self.source), *args], check=True, capture_output=True, timeout=10)

    def test_deterministic_scoped_archive_and_installed_source_verification(self):
        again = package.build(self.source, self.root / 'again.zip')
        self.assertEqual(self.archive.read_bytes(), Path(again['path']).read_bytes())
        receipt, payload = package.read_archive(self.archive, expected_zip_sha256=self.receipt['zip_sha256'],
                                                expected_manifest_sha256=self.receipt['manifest_sha256'])
        self.assertEqual(receipt, self.receipt)
        self.assertFalse(set(self.forbidden) & payload.keys())
        self.assertEqual(payload.keys(), package.REQUIRED | {package.MANIFEST})
        extracted = self.root / 'verified-release'
        for alias, blob in payload.items():
            path = extracted / alias
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(blob)
        self.assertEqual(package.verify_source(extracted, expected_manifest_sha256=receipt['manifest_sha256']),
                         {'manifest_sha256': receipt['manifest_sha256'], 'files': receipt['files']})
        (extracted / 'tools/m0_agent/credential_channel.py').write_bytes(b'# altered fixture')
        with self.assertRaisesRegex(package.PackageError, 'member SHA256'):
            package.verify_source(extracted, expected_manifest_sha256=receipt['manifest_sha256'])

    def test_rejects_artifact_scope_mismatch_or_tamper_even_with_repacked_zip(self):
        _, payload = package.read_archive(self.archive)
        manifest = payload.pop(package.MANIFEST)
        target = self.root / 'tampered.zip'
        name = 'tools/m0_agent/common_install.py'
        target.write_bytes(package._zip_bytes({**payload, name: b'# changed fixture'}, manifest))
        with self.assertRaisesRegex(package.PackageError, 'member SHA256'):
            package.verify_archive(target)
        forbidden = 'tools/m0_agent/agent.key'
        added = {**payload, forbidden: b'synthetic-invalid-source'}
        target.write_bytes(package._zip_bytes(added, package.baseline._json({'schema': package.SCHEMA,
                           'files': {path: package.baseline._sha(blob) for path, blob in added.items()}})))
        with self.assertRaisesRegex(package.PackageError, 'outside common external source scope'):
            package.verify_archive(target)
        with self.assertRaisesRegex(package.PackageError, 'manifest SHA256 mismatch'):
            package.verify_archive(self.archive, expected_manifest_sha256='0' * 64)

    def test_refuses_link_traversal_and_duplicate_archive_members(self):
        cases = (
            ([('tools/m0_agent/linked.py', stat.S_IFLNK)], 'invalid common external ZIP member'),
            ([('../escape', stat.S_IFREG)], 'invalid package path'),
            ([(package.MANIFEST, stat.S_IFREG)] * 2, 'duplicate'),
        )
        target = self.root / 'invalid.zip'
        for entries, message in cases:
            with self.subTest(entries=entries), warnings.catch_warnings(), zipfile.ZipFile(target, 'w') as archive:
                warnings.simplefilter('ignore', UserWarning)
                for path, mode in entries:
                    entry = zipfile.ZipInfo(path)
                    entry.create_system = 3
                    entry.external_attr = (mode | 0o644) << 16
                    archive.writestr(entry, b'test')
            with self.assertRaisesRegex(package.PackageError, '(?i)' + message):
                package.verify_archive(target)


if __name__ == '__main__':
    unittest.main()
