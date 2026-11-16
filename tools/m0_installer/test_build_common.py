from pathlib import Path
import os
import struct
import subprocess
import tempfile
import unittest
import warnings
import zipfile

from tools.m0_installer import build_common as package


FIXTURE = package.PAYLOAD + 'knowledge/m0_search/fixtures/grants.json'


class CommonPackageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / 'checkout'
        self.root.mkdir()
        self.git('init', '--quiet')
        for name in package.REQUIRED:
            self.put(name, b'{}' if name in package.FIXTURES else b'# synthetic test source\n')
        self.put(package.LICENSE, b'MIT License\n')
        self.git('add', '--all')

    def git(self, *args):
        subprocess.run(['git', '-C', str(self.root), *args], check=True, capture_output=True)

    def put(self, name, data):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    def built(self):
        return package.build(self.root, self.base / 'output')

    def edit_archive(self, result, transform):
        archive_path = Path(result['path'])
        with zipfile.ZipFile(archive_path) as archive:
            entries = [(info, archive.read(info)) for info in archive.infolist()]
        with zipfile.ZipFile(archive_path, 'w') as archive, warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            for entry, raw in transform(entries):
                archive.writestr(entry, raw)
        return archive_path

    def test_build_is_deterministic_and_excludes_untracked_credentials(self):
        for name in ('dispatch/sa_dispatch/fake_credentials.py', 'tools/m0_deploy/identities.json',
                     'state/target-binding.json', package.INSTALLER):
            self.put(name, b'SYNTHETIC_FAKE_KEY')
        first = self.built()
        second = package.build(self.root, self.base / 'second')
        self.assertEqual(Path(first['path']).read_bytes(), Path(second['path']).read_bytes())
        self.assertEqual(self.built(), first)
        self.assertNotIn(b'SYNTHETIC_FAKE_KEY', Path(first['path']).read_bytes())
        self.assertEqual(first['manifest']['distribution'], 'source_candidate')
        self.assertIsNone(first['manifest']['installer_entry'])
        self.assertIn(package.PAYLOAD + 'tools/m0_installer/build_common.py', first['manifest']['files'])
        self.assertEqual(first, package.verify_archive(first['path'], expected_zip_sha256=first['zip_sha256'],
                                                       expected_manifest_sha256=first['manifest_sha256']))

    def test_installer_must_be_explicit_and_tracked(self):
        self.put(package.INSTALLER, b'# Never execute the installer while packaging\nraise RuntimeError\n')
        with self.assertRaisesRegex(package.PackageError, 'missing from the Git index'):
            package.build(self.root, self.base / 'installer', include_installer=True)
        self.git('add', '--', package.INSTALLER)
        self.assertIsNone(self.built()['manifest']['installer_entry'])
        with self.assertRaisesRegex(package.PackageError, 'missing from the Git index'):
            package.build(self.root, self.base / 'installer', include_installer=True)
        for name in package.INSTALLER_SUPPORT:
            self.put(name, b'# synthetic initialization source\n')
        self.git('add', '--', *sorted(package.INSTALLER_SUPPORT))
        result = package.build(self.root, self.base / 'installer', include_installer=True)
        self.assertEqual(result['manifest']['installer_entry'], package.PAYLOAD + package.INSTALLER)
        self.assertEqual(package.verify_archive(result['path']), result)

    def test_fixture_tampering_is_rejected(self):
        path = self.edit_archive(self.built(), lambda entries: [
            (entry, b'[]' if entry.filename == FIXTURE else raw) for entry, raw in entries])
        with self.assertRaisesRegex(package.PackageError, 'Payload size or SHA256 mismatch'):
            package.verify_archive(path)

    def test_extra_and_unsafe_zip_paths_are_rejected(self):
        for name, message in [('payload/dispatch/sa_dispatch/extra.py', 'file sets differ'),
                              ('../escaped.py', 'Invalid package path'),
                              ('payload/dispatch/sa_dispatch/NUL.py', 'Invalid package path')]:
            with self.subTest(name=name):
                result = self.built()
                entry = zipfile.ZipInfo(name)
                entry.external_attr = 0o100644 << 16
                path = self.edit_archive(result, lambda entries: entries + [(entry, b'extra')])
                with self.assertRaisesRegex(package.PackageError, message):
                    package.verify_archive(path)
                path.unlink()

    def test_duplicate_zip_path_is_rejected(self):
        archive = self.edit_archive(self.built(), lambda entries: entries + entries[:1])
        with self.assertRaisesRegex(package.PackageError, 'Duplicate or conflicting package path'):
            package.verify_archive(archive)

    def test_manifest_cannot_expand_approved_extensions(self):
        result = self.built()
        path = Path(result['path'])
        with zipfile.ZipFile(path) as archive:
            payload = {name: archive.read(name) for name in result['manifest']['files']}
        payload['payload/tools/m0_native_mixed/fake_credentials.json'] = b'{}'
        manifest = package._json(package._manifest(payload, False))
        path = path.with_name('superagent-common-' + package._sha(manifest)[:12] + '.zip')
        path.write_bytes(package._zip_bytes(payload, manifest))
        with self.assertRaisesRegex(package.PackageError, 'outside the common package source scope'):
            package.verify_archive(path)

    def test_tracked_hardlink_is_rejected(self):
        path = self.root / FIXTURE.removeprefix(package.PAYLOAD)
        other = self.base / 'hardlink'
        try:
            os.link(path, other)
        except OSError as error:
            self.skipTest(f'Hardlinks unavailable: {error}')
        with self.assertRaisesRegex(package.PackageError, 'singly linked regular file'):
            self.built()

    def test_archive_digest_and_filename_are_checked(self):
        result = self.built()
        with self.assertRaisesRegex(package.PackageError, 'Archive SHA256 mismatch'):
            package.verify_archive(result['path'], expected_zip_sha256='0' * 64)
        with self.assertRaisesRegex(package.PackageError, 'Manifest SHA256 mismatch'):
            package.verify_archive(result['path'], expected_manifest_sha256='0' * 64)
        renamed = Path(result['path']).rename(self.base / 'renamed.zip')
        with self.assertRaisesRegex(package.PackageError, 'filename does not match'):
            package.verify_archive(renamed)

    def test_encrypted_zip_entry_is_rejected(self):
        path = Path(self.built()['path'])
        raw = bytearray(path.read_bytes())
        central = raw.index(b'PK\x01\x02')
        struct.pack_into('<H', raw, central + 8, 1)
        struct.pack_into('<H', raw, 6, 1)
        path.write_bytes(raw)
        with self.assertRaisesRegex(package.PackageError, 'unsupported ZIP entry'):
            package.verify_archive(path)

    def test_source_file_size_limit_is_enforced(self):
        self.put(FIXTURE.removeprefix(package.PAYLOAD), b'a' * (package.MAX_FILE_BYTES + 1))
        with self.assertRaisesRegex(package.PackageError, 'exceeds size limit'):
            self.built()


if __name__ == '__main__':
    unittest.main()
