"""Verify that only the intended campaign roles can enter provider execution."""

from contextlib import nullcontext
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from . import campaign_runtime as runtime


class CampaignRuntimeTests(unittest.TestCase):
    def test_role_mismatch_and_nonroot_operator_are_refused_at_identity_check(self):
        accounts = {'producer': SimpleNamespace(pw_uid=90), 'reviewer': SimpleNamespace(pw_uid=91)}
        with patch.object(runtime.sys, 'platform', 'linux'), \
             patch.object(runtime, 'verify_accounts', return_value=(accounts, 80, 81, 92)), \
             patch.object(runtime.os, 'getuid', return_value=91, create=True), \
             patch.object(runtime.os, 'geteuid', return_value=91, create=True), \
             patch.object(runtime, '_managed_path') as managed:
            with self.assertRaisesRegex(ValueError, 'incorrect role'):
                runtime.verify_execution('tick', 'producer')
            for command in ('register', 'stop', 'status'):
                with self.assertRaisesRegex(ValueError, 'requires root'):
                    runtime.verify_execution(command)
            managed.assert_not_called()

    def test_busy_worker_avoids_provider_and_ledger_transition(self):
        with patch.object(runtime, 'verify_execution', return_value=81), \
             patch.object(runtime, 'read_config', return_value={}), \
             patch.object(runtime, 'tick_lock', return_value=nullcontext(False)) as lock, \
             patch.object(runtime.common_runtime, 'read_provider_network') as network, \
             patch.object(runtime.campaign_worker, 'tick') as tick:
            self.assertEqual(runtime.run('tick', 'reviewer'), {'status': 'busy', 'role': 'reviewer'})
            lock.assert_called_once_with(81)
            network.assert_not_called()
            tick.assert_not_called()

    def test_tick_pins_provider_transport_and_only_injects_key_reader(self):
        config = {'campaign_id': 'authorized'}
        with patch.object(runtime, 'verify_execution', return_value=81), \
             patch.object(runtime, 'read_config', return_value=config), \
             patch.object(runtime, 'tick_lock', return_value=nullcontext(True)), \
             patch.object(runtime.common_runtime, 'read_provider_network', return_value='10.0.2.2:7897') as network, \
             patch.object(runtime, 'read_systemd_key') as key, \
             patch.object(runtime, 'complete_bounded') as provider, \
             patch.object(runtime.campaign_worker, 'tick', return_value={'status': 'active'}) as tick:
            self.assertEqual(runtime.run('tick', 'reviewer'), {'status': 'active'})
            network.assert_called_once_with(runtime.NETWORK_CONFIG)
            args, kwargs = tick.call_args
            self.assertEqual(args[0], runtime.LEDGER)
            self.assertIs(args[2], config)
            self.assertEqual(kwargs['role'], 'reviewer')
            self.assertIs(kwargs['credential_reader'], key)
            self.assertIs(kwargs['provider_call'].func, provider)
            self.assertEqual(kwargs['provider_call'].keywords, {'https_connect_proxy': '10.0.2.2:7897'})
            key.assert_not_called()
            provider.assert_not_called()

    def test_status_never_reads_provider_config_or_creates_tick_lock(self):
        with patch.object(runtime, 'verify_execution', return_value=81), \
             patch.object(runtime, 'read_config', return_value={}), \
             patch.object(runtime, 'tick_lock') as lock, \
             patch.object(runtime.common_runtime, 'read_provider_network') as network, \
             patch.object(runtime.campaign_control, 'status', return_value={'status': 'scheduled'}) as status:
            self.assertEqual(runtime.run('status'), {'status': 'scheduled'})
            status.assert_called_once()
            network.assert_not_called()
            lock.assert_not_called()


if __name__ == '__main__':
    unittest.main()
