"""The evidence-only CLI and portable proof closure, without real models."""
import json
import unittest
from contextlib import redirect_stdout
from datetime import timedelta
from unittest.mock import patch
import test_claude_integration as fixture
from sa_dispatch.clock import timestamp
from sa_dispatch.control_policy import ControlPolicy
from sa_dispatch.bundle import export_bundle,read_bundle
from sa_dispatch.artifacts import ArtifactError
from sa_dispatch.cli import main


class SettlementEntryTests(unittest.TestCase):
    setUp=fixture.ClaudeIntegrationTests.setUp
    adapter=fixture.ClaudeIntegrationTests.adapter
    cli=fixture.ClaudeIntegrationTests.cli

    def test_cli_settles_closed_report_without_constructing_model_adapter(self):
        policy=ControlPolicy(self.store,artifacts=self.artifacts);now=self.store.clock.now()
        policy.budget_upsert('runs',period='fixture',unit='claude_run',limit_units=2,
            starts_at=timestamp(now-timedelta(minutes=1)),ends_at=timestamp(now+timedelta(hours=1)))
        policy.bind_task(self.task.task_id,budget_id='runs',reserve_units=1)
        adapter=self.adapter()
        with patch.object(adapter,'settle',return_value={}):report=adapter.run(self.store.claim('fixture'))
        with self.assertRaisesRegex(ArtifactError,'unsettled budget'):
            export_bundle(self.config,self.root/'blocked')
        with patch('sa_dispatch.claude_adapter.ClaudeAdapter',side_effect=AssertionError('no runner')):
            result=self.cli('claude-settle',self.task.task_id,'--isolated-synthetic')
            repeated=self.cli('claude-settle',self.task.task_id,'--isolated-synthetic')
        self.assertEqual(result['settled'][0]['actual_units'],1)
        self.assertEqual(len(repeated['already_settled']),1)
        self.assertFalse(result['cost_unknown'])
        bundle=self.root/'closed';manifest=export_bundle(self.config,bundle)
        proof=json.loads(self.artifacts.get(result['settled'][0]['evidence_ref']))
        self.assertIn(proof['terminal_ref'],manifest['artifacts'])
        for field in ('exit_ref','launch_ref','stdout_ref','prompt_ref','init_ref'):
            self.assertIn(report[field],manifest['artifacts'])
        read_bundle(bundle)

    def test_closed_receipt_missing_exit_child_ref_cannot_export(self):
        adapter=self.adapter();report=adapter.run(self.store.claim('fixture'))
        # CAS is append-only in normal use; remove only the isolated test object.
        ref=report['exit_ref'];path=self.artifacts.root/'sha256'/ref[7:9]/ref[7:]
        path.unlink()
        with self.assertRaises(ArtifactError):export_bundle(self.config,self.root/'incomplete')

    def test_scope_rejection_precedes_any_database_initialization(self):
        missing=self.root/'must-not-exist.db'
        config=self.config_file.read_text().replace('synthetic = true','synthetic = false')
        config=config.replace(json.dumps(str(self.config.db_path)),json.dumps(str(missing)))
        self.config_file.write_text(config)
        output=fixture.Output()
        with redirect_stdout(output):
            code=main(['--config',str(self.config_file),'claude-settle',self.task.task_id,'--isolated-synthetic'])
        self.assertEqual(code,2)
        self.assertFalse(json.loads(output.buffer.getvalue())['ok'])
        self.assertFalse(missing.exists())


if __name__=='__main__':unittest.main()
