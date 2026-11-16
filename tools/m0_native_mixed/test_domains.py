import hashlib
import json
from pathlib import Path
import shutil
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from dispatch.sa_dispatch.config import load_config
from knowledge.m0_content.workflow_binding import load_workflow_binding, request_base, search_modules
from tools.m0_native_mixed import domains


class DomainStageTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.root = self.base / 'owned'
        self.ledger = self.base / 'ledger.sqlite3'
        self.artifacts = self.base / 'cas'
        self.arguments = {
            'ledger': self.ledger, 'artifacts': self.artifacts,
            'source_root': domains.ROOT, 'executable': Path(sys.executable).resolve(),
            'device_id': 'native-local-test',
        }

    def stage(self, **overrides):
        return domains.stage_domains(self.root, **(self.arguments | overrides))

    def test_stage_reads_back_config_and_combined_search_without_touching_ledger_or_cas(self):
        self.root.mkdir()
        self.ledger.write_bytes(b'ledger sentinel, deliberately not SQLite')
        self.artifacts.mkdir()
        sentinel = self.artifacts / 'sentinel'
        sentinel.write_bytes(b'CAS sentinel')
        before = [path.stat().st_mtime_ns for path in (self.ledger, sentinel)]

        stage = self.stage()
        self.assertEqual(json.loads(Path(stage['manifest']).read_text(encoding='utf-8')), stage)
        config = load_config(stage['local_config'], allowed_plugins={'research', 'research_content'})
        self.assertEqual((config.db_path, config.artifact_root), (self.ledger, self.artifacts))
        self.assertEqual(config.device_id, self.arguments['device_id'])
        self.assertEqual(config.execution_route, 'local_synthetic')
        self.assertTrue(config.synthetic)
        self.assertEqual(config.plugins, ('research', 'research_content'))
        self.assertEqual(config.journal_dir, self.root / 'journal')
        self.assertEqual(list(config.journal_dir.iterdir()), [])
        for name, binding_role in (('research', 'research_binding'), ('research_content', 'workflow_binding')):
            entry, flag = domains.COMPONENTS[name]
            self.assertEqual(config.components[name].argv,
                             (str(self.arguments['executable']), '-B', str(domains.ROOT / entry),
                              flag, stage[binding_role]))

        binding = load_workflow_binding(stage['workflow_binding'])
        self.assertEqual(binding.digest, stage['workflow_binding_sha256'])
        self.assertEqual(binding.research.digest, stage['research_binding_sha256'])
        self.assertEqual(binding.research.instance, stage['catalog_instance'])
        self.assertEqual(binding.content_instance, stage['content_instance'])
        bound, _, _, _ = search_modules()
        query = request_base('2026-11-16T00:00:00+00:00') | {
            bound.BINDING_FIELD: binding.research.digest, 'q': '剪辑', 'limit': 50,
        }
        result = bound.execute_bound('research.search', query, binding.research)
        matches = {item['ref'].split('/')[0] for item in result['results'] if item['status'] == 'current'}
        self.assertTrue({'research-scatter', 'research-content'} <= matches)
        self.assertEqual(set(result['index_versions']), {'research-scatter', 'research-content', 'signalhub'})
        self.assertEqual(set(stage['files']), {
            'research/catalog.json', 'research/catalog.sqlite3', 'research/research-scatter.sqlite3',
            'research/research-content.sqlite3', 'research/signalhub.sqlite3', 'content.sqlite3',
            'local.toml', 'research-binding.json', 'workflow-binding.json',
        })
        for path, expected in stage['files'].items():
            self.assertEqual(hashlib.sha256((self.root / path).read_bytes()).hexdigest(), expected)
        self.assertEqual(self.ledger.read_bytes(), b'ledger sentinel, deliberately not SQLite')
        self.assertEqual(sentinel.read_bytes(), b'CAS sentinel')
        self.assertEqual([path.stat().st_mtime_ns for path in (self.ledger, sentinel)], before)
        self.assertEqual(set(self.base.iterdir()), {self.root, self.ledger, self.artifacts})
        self.assertEqual(list(self.artifacts.iterdir()), [sentinel])

        with self.assertRaisesRegex(domains.DomainStageError, 'empty'):
            self.stage()
        self.assertEqual(load_workflow_binding(stage['workflow_binding']).digest, binding.digest)

    def test_wrong_inputs_fail_before_writes(self):
        for overrides in ({'ledger': Path('relative.db')}, {'ledger': self.root / 'content.sqlite3'},
                          {'artifacts': self.base / '..' / self.base.name / 'cas'},
                          {'device_id': 'not an ID'}, {'executable': self.ledger}):
            with self.subTest(overrides=overrides), self.assertRaises((OSError, domains.DomainStageError)):
                self.stage(**overrides)
        self.assertEqual(list(self.base.iterdir()), [])

    def test_populated_root_is_preserved(self):
        self.root.mkdir()
        existing = self.root / 'content.sqlite3'
        existing.write_bytes(b'do not replace')
        with self.assertRaisesRegex(domains.DomainStageError, 'empty'):
            self.stage()
        self.assertEqual(list(self.root.iterdir()), [existing])
        self.assertEqual(existing.read_bytes(), b'do not replace')

    def test_failed_readback_leaves_non_reusable_stage(self):
        with patch.object(domains, 'load_config', side_effect=ValueError('fixture parse failed')):
            with self.assertRaisesRegex(ValueError, 'fixture parse failed'):
                self.stage()
        state = json.loads((self.root / domains.STAGE_MANIFEST).read_text(encoding='utf-8'))
        self.assertEqual(state, {'state': 'incomplete'})
        self.assertTrue((self.root / 'workflow-binding.json').is_file())
        self.assertFalse(self.ledger.exists())
        self.assertFalse(self.artifacts.exists())
        with self.assertRaisesRegex(domains.DomainStageError, 'empty'):
            self.stage()

    def test_symlink_root_and_ancestors_are_rejected(self):
        target = self.base / 'target'
        target.mkdir()
        try:
            self.root.symlink_to(target, target_is_directory=True)
        except OSError as error:
            if sys.platform != 'win32' or getattr(error, 'winerror', None) != 1314:
                raise
            from _winapi import CreateJunction
            CreateJunction(str(target), str(self.root))
        with self.assertRaisesRegex(domains.DomainStageError, 'Links'):
            self.stage()
        self.assertEqual(list(target.iterdir()), [])
        with self.assertRaisesRegex(domains.DomainStageError, 'Links'):
            domains.stage_domains(self.root / 'child', **self.arguments)
        self.assertEqual(list(target.iterdir()), [])

    def test_frozen_tree_must_match_loaded_source(self):
        source_root = self.base / 'frozen'
        pins = domains.pin_source_files(domains.ROOT)
        for name in pins:
            target = source_root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(domains.ROOT / name, target)
        self.assertEqual(domains.pin_source_files(source_root), pins)
        copy_stage = domains.stage_domains(self.base / 'copy-stage', **(self.arguments | {'source_root': source_root}))
        original_stage = domains.stage_domains(self.base / 'original-stage', **self.arguments)
        config = load_config(copy_stage['local_config'], allowed_plugins=set(domains.COMPONENTS))
        self.assertEqual(config.components['research'].argv[2], str(source_root / domains.COMPONENTS['research'][0]))
        self.assertNotEqual(copy_stage['catalog_instance'], original_stage['catalog_instance'])
        self.assertNotEqual(copy_stage['content_instance'], original_stage['content_instance'])
        component = source_root / domains.COMPONENTS['research'][0]
        before = component.read_bytes()
        component.write_bytes(before + b'\n# changed\n')
        with self.assertRaisesRegex(domains.DomainStageError, 'differs'):
            self.stage(source_root=source_root)
        self.assertFalse(self.root.exists())
        component.unlink()
        with self.assertRaisesRegex(domains.DomainStageError, 'incomplete'):
            self.stage(source_root=source_root)
        self.assertFalse(self.root.exists())


if __name__ == '__main__':
    unittest.main()
