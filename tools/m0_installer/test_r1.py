"""Bounded package/config tests; real installation is separately rehearsed."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from tools.m0_installer import r1


class R1PackageTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='sa-r1-package-')
        self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name).resolve()
        self.names=['installer/r1.py','installer/credentials.py','installer/trial.py',
            'payload/tools/m0_agent/runtime.py','payload/tools/m0_agent/worker.py',
            'payload/tools/m0_agent/provider_process.py','payload/tools/m0_installer/credentials.py',
            'payload/tools/m0_agent/readonly_status.py',
            'payload/dispatch/sa_dispatch/store_sqlite.py']
        files={}
        for name in self.names:
            p=self.root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'# synthetic package\n')
            files[name]=r1.sha(p.read_bytes())
        self.manifest={'schema':r1.SCHEMA,'scope':'r1_single_scheduled_agent','files':files}
        self.raw=r1.encoded(self.manifest)
        (self.root/'INSTALL_MANIFEST.json').write_bytes(self.raw)
        self.digest=r1.sha(self.raw)

    def test_exact_package_and_digest(self):
        self.assertEqual(r1.verify(self.root,self.digest),self.manifest)
        with self.assertRaises(ValueError):r1.verify(self.root,'0'*64)

    def test_extra_and_changed_files_refused(self):
        p=self.root/'unplanned.txt';p.write_bytes(b'synthetic')
        with self.assertRaises(ValueError):r1.verify(self.root,self.digest)
        p.unlink()
        (self.root/self.names[0]).write_bytes(b'changed')
        with self.assertRaises(ValueError):r1.verify(self.root,self.digest)

    def test_missing_runtime_closure_refused(self):
        name=self.names[3];(self.root/name).unlink();self.manifest['files'].pop(name)
        raw=r1.encoded(self.manifest);(self.root/'INSTALL_MANIFEST.json').write_bytes(raw)
        with self.assertRaises(ValueError):r1.verify(self.root,r1.sha(raw))

    def test_configuration_preserves_frozen_time_and_mode(self):
        identity={'manifest_sha256':self.digest,'execution_mode':'simulation','model':'operator-selected-model',
                  'scheduled_at':'2026-11-16T00:00:10.000000+00:00'}
        config=r1.make_config(identity)
        self.assertEqual(config,r1.make_config(dict(identity)))
        self.assertEqual(config['grant']['scheduled_at'],identity['scheduled_at'])
        self.assertEqual(config['grant']['max_tokens'],64)
        self.assertEqual(config['grant']['source'],'synthetic')
        self.assertNotIn('key',r1.encoded(config).decode().lower())

    def test_service_separates_network_and_credentials(self):
        release=Path('/opt/superagent-r1/releases')/self.digest
        fake=r1.service_bytes(release,'simulation').decode()
        live=r1.service_bytes(release,'live').decode()
        self.assertIn('IPAddressDeny=any',fake)
        self.assertNotIn('IPAddressDeny=any',live)
        for unit in (fake,live):
            self.assertIn('LoadCredential=provider_api_key:',unit)
            self.assertIn('KillMode=control-group',unit)
            self.assertIn('TimeoutStartSec=70',unit)
            self.assertIn('User=sa-r1',unit)
            self.assertNotIn('Environment=API',unit)

    def test_readonly_entry_rejects_unfixed_release_and_has_no_mutating_route(self):
        for release in (r1.BASE/'releases'/'not-a-digest',r1.BASE/'other'/self.digest):
            with self.assertRaises(ValueError):r1.readonly_entry_bytes(release)
        entry=r1.readonly_entry_bytes(r1.BASE/'releases'/self.digest).decode('ascii')
        self.assertIn('"$#" -ne 0',entry)
        self.assertIn('/usr/sbin/runuser -u sa-r1',entry)
        self.assertIn('/usr/bin/env -i',entry)
        self.assertIn('-m tools.m0_agent.readonly_status',entry)
        for forbidden in ('SSH_ORIGINAL_COMMAND','provider.key','CREDENTIALS_DIRECTORY',' runtime tick',' eval '):
            self.assertNotIn(forbidden,entry)

    @unittest.skipUnless(os.name == 'posix', 'POSIX shell entry')
    def test_readonly_shell_rejects_arguments_without_echoing_or_running_them(self):
        path=self.root/'readonly-entry'
        path.write_bytes(r1.readonly_entry_bytes(r1.BASE/'releases'/self.digest))
        marker=self.root/'must-not-exist'
        command='touch '+str(marker)
        result=subprocess.run(['/bin/sh',str(path),command],capture_output=True,timeout=5,
                              env={'PATH':'/nonexistent','SSH_ORIGINAL_COMMAND':command})
        self.assertEqual(result.returncode,2)
        self.assertEqual(json.loads(result.stdout)['error'],'r1_status_invalid_arguments')
        self.assertNotIn(command.encode(),result.stdout+result.stderr)
        self.assertFalse(marker.exists())


if __name__=='__main__':unittest.main()
