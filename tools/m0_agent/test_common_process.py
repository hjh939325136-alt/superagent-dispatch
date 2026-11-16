"""A killed process cannot release an ambiguous provider-request reservation."""
from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import Mock

from .common_runtime import SCHEMA
from .common_worker import tick
from .test_common_worker import CommonWorkerTests


class CommonProcessTests(unittest.TestCase):
    def test_actual_process_exit_leaves_unknown_and_never_calls_again(self):
        fixture = CommonWorkerTests(); fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.upgrade(); fixture.register()
        config = fixture.root / 'grant.json'
        config.write_text(json.dumps({'schema': SCHEMA, **asdict(fixture.config)}), encoding='utf-8')
        counter = fixture.root / 'provider-attempts'
        code = '''
import os,sys
from pathlib import Path
from sa_dispatch.artifacts import ArtifactStore
from sa_dispatch.clock import FakeClock
from tools.m0_agent.common_runtime import read_config
from tools.m0_agent.common_worker import tick
def provider(**kwargs):
    with Path(sys.argv[4]).open('xb') as stream:
        stream.write(b'one provider attempt'); stream.flush(); os.fsync(stream.fileno())
    os._exit(71)
tick(Path(sys.argv[1]),ArtifactStore(sys.argv[2]),read_config(sys.argv[3]),
     credential_reader=lambda:'fixture-not-real-key',provider_call=provider,clock=FakeClock())
'''
        root = Path(__file__).resolve().parents[2]
        env = dict(os.environ, PYTHONPATH=os.pathsep.join((str(root / 'dispatch'), str(root))))
        result = subprocess.run([sys.executable, '-B', '-c', code, str(fixture.ledger),
                                 str(fixture.root / 'cas'), str(config), str(counter)],
                                env=env, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 71, result.stderr.decode(errors='replace'))
        fixture.clock.advance(121)
        credential = Mock(side_effect=AssertionError('must not read credential after process loss'))
        provider = Mock(side_effect=AssertionError('must not repeat ambiguous API request'))
        for _ in range(2):
            status = tick(fixture.ledger, fixture.cas, fixture.config, credential_reader=credential,
                          provider_call=provider, clock=fixture.clock)
            self.assertEqual(status['status'], 'unknown')
            self.assertEqual(status['attempts'], 1)
            self.assertEqual(status['request_units_held'], 1)
        self.assertEqual(counter.read_bytes(), b'one provider attempt')
        self.assertEqual(fixture.local.get_task(fixture.local_task.task_id)['status'], 'queued')


if __name__ == '__main__': unittest.main()
