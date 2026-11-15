"""Transactional registration of the compatible M0 schema extension."""
from .scheduler import initialize_schema as initialize_scheduler
from .unified_events import initialize_schema as initialize_events
from .control_policy import initialize_schema as initialize_control
from .host_adapter import initialize_schema as initialize_host
from .claude_adapter import initialize_schema as initialize_claude
from .collector import initialize_schema as initialize_collector


def apply_m0_schema(conn, at):
    """Caller owns BEGIN/COMMIT, so a failed upgrade preserves the prior schema."""
    columns = {row[1] for row in conn.execute('PRAGMA table_info(component_capabilities)')}
    if 'component_fingerprint' not in columns:
        conn.execute('ALTER TABLE component_capabilities ADD COLUMN component_fingerprint TEXT')
    initialize_scheduler(conn)
    initialize_events(conn)
    conn.execute('CREATE TABLE IF NOT EXISTS schema_migrations('
                 'version TEXT PRIMARY KEY,name TEXT NOT NULL,applied_at TEXT NOT NULL)')
    conn.execute('INSERT OR IGNORE INTO schema_migrations VALUES(?,?,?)',
                 ('0007', 'm0_scheduler_events_reconciliation', at))
    initialize_control(conn)
    initialize_host(conn)
    conn.execute('INSERT OR IGNORE INTO schema_migrations VALUES(?,?,?)',
                 ('0008', 'm0_control_host_adapter', at))
    initialize_claude(conn)
    conn.execute('INSERT OR IGNORE INTO schema_migrations VALUES(?,?,?)',
                 ('0009', 'm0_claude_owned_runtime', at))
    conn.execute('INSERT OR IGNORE INTO schema_migrations VALUES(?,?,?)',
                 ('0010', 'm0_bounded_schedules', at))
    initialize_collector(conn)
    conn.execute('INSERT OR IGNORE INTO schema_migrations VALUES(?,?,?)',
                 ('0011', 'm0_persistent_collector', at))
