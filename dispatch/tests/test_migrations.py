"""M0 additive migration must survive failures without changing old tasks."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from sa_dispatch.models import TaskSpec
from sa_dispatch.store_sqlite import SqliteStore


class MigrationTests(unittest.TestCase):
    def legacy(self, path):
        store = SqliteStore(path, lease_ttl_seconds=10)
        task, _ = store.submit(TaskSpec('old', 'legacy.read', 1, {}, (), (), 'synthetic', 'none', 2))
        events = store.events(task.task_id)
        with closing(sqlite3.connect(path)) as db, db:
            for name in ('schema_migrations', 'schedule_events', 'schedule_firings', 'schedules',
                         'unified_events', 'component_capabilities', 'reconciliations'):
                db.execute('DROP TABLE ' + name)
        return task, events

    def test_failed_upgrade_rolls_back_and_next_open_recovers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'legacy.db'
            task, events = self.legacy(path)
            with patch('sa_dispatch.migrations.initialize_events', side_effect=RuntimeError('injected')):
                with self.assertRaises(RuntimeError):
                    SqliteStore(path, lease_ttl_seconds=10)
            with closing(sqlite3.connect(path)) as db:
                names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                self.assertNotIn('schedules', names)
                self.assertNotIn('schema_migrations', names)
            restored = SqliteStore(path, lease_ttl_seconds=10)
            self.assertEqual(restored.get_task(task.task_id), task)
            self.assertEqual(restored.events(task.task_id), events)
            self.assertEqual(restored.snapshot()['schema_migrations'][0]['version'], '0007')

    def test_concurrent_reopen_records_one_migration(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'legacy.db'
            task, events = self.legacy(path)
            with ThreadPoolExecutor(2) as pool:
                stores = list(pool.map(lambda _: SqliteStore(path, lease_ttl_seconds=10), range(2)))
            versions = [row['version'] for row in stores[0].snapshot()['schema_migrations']]
            self.assertEqual(sorted(versions), ['0007', '0008', '0009', '0010', '0011', '0012'])
            self.assertEqual(stores[1].events(task.task_id), events)


if __name__ == '__main__':
    unittest.main()
