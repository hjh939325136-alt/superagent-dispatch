"""True frozen 0009/v4 compatibility and bounded v5 round trip."""
import hashlib
import json
import os
import copy
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
import zipfile
import test_scheduler as fixture
from sa_dispatch.bundle import export_bundle,import_bundle,ledger,read_bundle
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.artifacts import ArtifactError
from sa_dispatch.canonical import sha256_ref
from sa_dispatch.executor import receipt_bytes

FIXTURES=Path(__file__).parent/'fixtures/legacy_bundles'

class BundleV5Tests(unittest.TestCase):
    setUp=fixture.SchedulerTests.setUp
    open_store=fixture.SchedulerTests.open_store
    schedule=fixture.SchedulerTests.schedule

    def config(self,name):
        root=Path(self.temp.name)/name
        return SimpleNamespace(db_path=root/'ledger.db',artifact_root=root/'cas')

    def verify_frozen(self):
        provenance=json.loads((FIXTURES/'provenance-v4.json').read_bytes())
        self.assertEqual(hashlib.sha256((FIXTURES/'runtime-v4.zip').read_bytes()).hexdigest(),provenance['source_archive_sha256'])
        for name,digest in provenance['fixture_files'].items():
            self.assertEqual(hashlib.sha256((FIXTURES/'v4'/name).read_bytes()).hexdigest(),digest)

    def test_real_v4_import_has_null_bound_and_disabled_schedule(self):
        self.verify_frozen()
        config=self.config('import');store=SqliteStore(config.db_path,lease_ttl_seconds=10)
        report=import_bundle(config,FIXTURES/'v4',store)
        self.assertEqual(report['schema'],'sa_dispatch.bundle.v4')
        rows=ledger(config.db_path)
        self.assertEqual(rows['schedules'][0]['max_slots'],None)
        self.assertEqual(rows['schedules'][0]['enabled'],0)
        self.assertEqual(len(rows['schedule_firings']),1)
        self.assertEqual({r['version'] for r in rows['schema_migrations']},{'0007','0008','0009','0010','0011'})

    def test_actual_0009_database_additive_migration(self):
        self.verify_frozen();root=Path(self.temp.name)/'old-runtime';root.mkdir()
        with zipfile.ZipFile(FIXTURES/'runtime-v4.zip') as archive:archive.extractall(root)
        config=self.config('old-state')
        script='''import json,sys,sqlite3
from pathlib import Path
from types import SimpleNamespace
from sa_dispatch.bundle import import_bundle,ledger,FORMAT
from sa_dispatch.store_sqlite import SqliteStore
from sa_dispatch.artifacts import ArtifactError
from sa_dispatch.canonical import sha256_ref
from sa_dispatch.executor import receipt_bytes
c=SimpleNamespace(db_path=Path(sys.argv[1]),artifact_root=Path(sys.argv[2]))
s=SqliteStore(c.db_path,lease_ttl_seconds=10)
assert FORMAT=='sa_dispatch.bundle.v4'
import_bundle(c,Path(sys.argv[3]),s)
assert 'max_slots' not in ledger(c.db_path)['schedules'][0]
assert {r['version'] for r in ledger(c.db_path)['schema_migrations']}=={'0007','0008','0009'}
print(json.dumps(ledger(c.db_path)))
'''
        env=dict(os.environ,PYTHONPATH=str(root))
        result=subprocess.run([sys.executable,'-c',script,str(config.db_path),str(config.artifact_root),str((FIXTURES/'v4').resolve())],cwd=root,env=env,capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,0,result.stderr)
        before=json.loads(result.stdout)
        direct=Path(self.temp.name)/'direct-old-export'
        export_bundle(config,direct)
        self.assertEqual(read_bundle(direct)[0]['schema'],'sa_dispatch.bundle.v6')
        self.assertNotIn('max_slots',ledger(config.db_path)['schedules'][0])
        SqliteStore(config.db_path,lease_ttl_seconds=10)
        after=ledger(config.db_path)
        for table,rows in before.items():
            if table=='schema_migrations':
                self.assertEqual(after[table][:len(rows)],rows)
                self.assertEqual([r['version'] for r in after[table][len(rows):]],['0010','0011'])
            elif table=='schedules':
                self.assertEqual(after[table],[dict(row,max_slots=None) for row in rows])
            else:self.assertEqual(after[table],rows,table)

    def test_v5_roundtrip_retains_exhaustion_and_history(self):
        self.schedule(max_slots=3,catchup='all',max_catchup=3)
        self.clock.advance(100);self.scheduler.tick()
        for task in self.store.list_tasks():self.store.cancel(task.task_id,'fixture')
        config=SimpleNamespace(db_path=self.path,artifact_root=Path(self.temp.name)/'cas')
        package=Path(self.temp.name)/'v5';export_bundle(config,package)
        self.assertEqual(read_bundle(package)[0]['schema'],'sa_dispatch.bundle.v6')
        target=self.config('target');store=SqliteStore(target.db_path,lease_ttl_seconds=10)
        import_bundle(target,package,store)
        before=ledger(config.db_path);after=ledger(target.db_path)
        self.assertEqual(before,after)
        self.assertEqual(after['schedules'][0]['max_slots'],3)
        self.assertEqual(after['schedules'][0]['enabled'],0)

    def test_v5_rejects_invalid_bounds_and_cannot_masquerade_as_v4(self):
        self.schedule(max_slots=1);self.scheduler.tick()
        for task in self.store.list_tasks():self.store.cancel(task.task_id,'fixture')
        config=SimpleNamespace(db_path=self.path,artifact_root=Path(self.temp.name)/'cas')
        package=Path(self.temp.name)/'negative';export_bundle(config,package)
        manifest=json.loads((package/'manifest.json').read_bytes())
        original=json.loads((package/'schedules.jsonl').read_bytes())
        for patch in ({'max_slots':True},{'max_slots':0},{'max_slots':100001},{'enabled':1},{}):
            row=dict(original,**patch)
            if not patch:row.pop('max_slots')
            raw=receipt_bytes(row)+b'\n';(package/'schedules.jsonl').write_bytes(raw)
            changed=copy.deepcopy(manifest);changed['files']['schedules.jsonl']=sha256_ref(raw)
            (package/'manifest.json').write_bytes(receipt_bytes(changed))
            with self.assertRaises(ArtifactError):read_bundle(package)
        raw=receipt_bytes(original)+b'\n';(package/'schedules.jsonl').write_bytes(raw)
        changed=copy.deepcopy(manifest);changed['files']['schedules.jsonl']=sha256_ref(raw)
        changed['schema']='sa_dispatch.bundle.v4'
        (package/'manifest.json').write_bytes(receipt_bytes(changed))
        with self.assertRaises(ArtifactError):read_bundle(package)

if __name__=='__main__':unittest.main()
