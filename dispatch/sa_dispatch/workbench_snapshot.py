"""Read the local dispatch ledger for the workbench without initializing it."""

from argparse import ArgumentParser
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys


def snapshot(path):
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        devices = [dict(row) for row in db.execute(
            "SELECT device_id, last_seen_at FROM devices ORDER BY device_id")]
        tasks = []
        for row in db.execute("""SELECT t.task_id, t.kind, t.track, t.status, t.updated_at,
                   t.result_json, l.device_id, l.expires_at
            FROM tasks t LEFT JOIN leases l ON l.task_id=t.task_id AND l.fence=t.fence
            WHERE t.access <> 'synthetic' ORDER BY t.updated_at DESC,t.task_id LIMIT 30"""):
            task = dict(row)
            result = json.loads(task.pop("result_json") or "{}")
            task["component_state"] = result.get("component_state")
            tasks.append(task)
        return {"observed_at": datetime.now(timezone.utc).isoformat(), "devices": devices, "tasks": tasks}


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("ledger", type=Path)
    try:
        result = snapshot(parser.parse_args().ledger)
    except (OSError, sqlite3.Error, ValueError):
        print("Unable to read the configured dispatch ledger", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(json.dumps(result, ensure_ascii=False).encode("utf-8") + b"\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
