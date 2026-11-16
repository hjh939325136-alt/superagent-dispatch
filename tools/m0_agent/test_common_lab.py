"""Acceptance helper speaks to the actual HTTP contract, not a guessed envelope."""
import hashlib
from http.client import HTTPConnection
import json
from pathlib import Path
from threading import Thread
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from sa_dispatch.transport import CoordinatorAPI, create_server
from sa_dispatch_hotspot import KINDS
from . import common_lab as lab
from .test_common_worker import CommonWorkerTests


class CommonLabTests(unittest.TestCase):
    def test_new_prepare_requires_explicit_grant_id_before_writing(self):
        f = CommonWorkerTests(); f.setUp(); self.addCleanup(f.doCleanups)
        f.upgrade()
        config_path = f.root / 'grant.json'
        machine_path = f.root / 'machine-id'
        machine_path.write_text(f.config.machine_id, encoding='ascii')
        network_path = f.root / 'provider-network.json'
        paths = {'/etc/machine-id': machine_path, '/etc/sa-dispatch/provider-network.json': network_path}
        group = SimpleNamespace(getgrnam=lambda _: SimpleNamespace(gr_gid=15))
        with patch.object(lab, 'Path', side_effect=paths.__getitem__), \
             patch.object(lab.os, 'geteuid', return_value=0, create=True), \
             patch.object(lab.os, 'chown', create=True), patch.object(lab.os, 'chmod'), \
             patch.dict('sys.modules', {'grp': group}), \
             patch.object(lab, 'LEDGER', f.ledger), patch.object(lab, 'CAS', f.root / 'cas'), \
             patch.object(lab, 'CONFIG', config_path), patch.object(lab, 'write_new') as write:
            with self.assertRaisesRegex(ValueError, 'explicit --grant-id required'):
                lab.prepare(f.config.machine_id)
            write.assert_not_called()
            self.assertTrue(lab.prepare(f.config.machine_id, 'reviewed-fixture-grant')['prepared'])
            path, document = write.call_args_list[0].args
            self.assertEqual(path, config_path)
            self.assertEqual(document['grant']['grant_id'], 'reviewed-fixture-grant')

    def test_prepare_reuses_bound_registered_grant_and_refuses_replacement(self):
        f = CommonWorkerTests(); f.setUp(); self.addCleanup(f.doCleanups)
        f.upgrade(); f.register()
        config_path = f.root / 'grant.json'
        config_path.write_bytes(b'protected fixture')
        machine_path = f.root / 'machine-id'
        machine_path.write_text(f.config.machine_id, encoding='ascii')
        initial = f.ledger.read_bytes()
        with patch.object(lab, 'Path', return_value=machine_path), \
             patch.object(lab.os, 'geteuid', return_value=0, create=True), \
             patch.dict('sys.modules', {'grp': SimpleNamespace()}), \
             patch.object(lab, 'LEDGER', f.ledger), patch.object(lab, 'CAS', f.root / 'cas'), \
             patch.object(lab, 'CONFIG', config_path), patch.object(lab, '_safe_system_path'), \
             patch.object(lab, 'read_config', return_value=f.config), patch.object(lab, 'write_new') as write:
            first = lab.prepare(f.config.machine_id)
            self.assertEqual(first['task_id'], f.status()['task_id'])
            self.assertEqual(lab.prepare(f.config.machine_id, f.config.grant.grant_id), first)
            with self.assertRaisesRegex(ValueError, 'existing grant differs'):
                lab.prepare(f.config.machine_id, 'another-synthetic-grant')
            config_path.unlink()
            with self.assertRaisesRegex(ValueError, 'recover the original configuration'):
                lab.prepare(f.config.machine_id, 'another-synthetic-grant')
            write.assert_not_called()
        self.assertEqual(f.ledger.read_bytes(), initial)

    def test_http_acceptance_contract_and_repeat(self):
        f = CommonWorkerTests(); f.setUp(); self.addCleanup(f.doCleanups)
        f.upgrade(); f.register()
        f.kinds.update(KINDS)
        store = f.store('http_synthetic')
        store.register_device('common-http-synthetic', ['component:mgmtcore'])
        tokens = {'common-http-synthetic': 'a' * 40, 'common-http-dispatcher': 'b' * 40}
        api = CoordinatorAPI(store, {name: hashlib.sha256(value.encode()).hexdigest()
                                     for name, value in tokens.items()},
            synthetic_handoff=True, roles={'common-http-synthetic': ['worker'],
                                           'common-http-dispatcher': ['dispatcher']}, artifact_store=f.cas)
        server = create_server(api)
        thread = Thread(target=server.serve_forever, daemon=True); thread.start()
        self.addCleanup(lambda: (server.shutdown(), server.server_close(), thread.join()))
        def selected_path(value):
            if str(value) == '/var/lib/sa-common-install/http-local-tokens.json':
                return SimpleNamespace(read_bytes=lambda: json.dumps({'tokens': tokens}).encode())
            return Path(value)
        def connection(*args, **kwargs): return HTTPConnection(*server.server_address, timeout=8)
        with patch.object(lab, 'Path', side_effect=selected_path), \
             patch.object(lab, 'HTTPConnection', side_effect=connection), \
             patch.object(lab, 'LEDGER', f.ledger), patch.object(lab, 'CAS', f.root / 'cas'), \
             patch.object(lab, 'read_config', return_value=f.config):
            one = lab.http_probe(); two = lab.http_probe()
        self.assertEqual(one['http']['status'], 'succeeded')
        self.assertEqual(one['http'], two['http'])
        self.assertEqual(one['external_status_denied'], 'E_NOT_FOUND')
        self.assertEqual(f.status()['attempts'], 0)


if __name__ == '__main__': unittest.main()
