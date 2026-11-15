"""Transactional registration of the compatible M0 schema extension."""
from .scheduler import initialize_schema as initialize_scheduler
from .unified_events import initialize_schema as initialize_events
from .control_policy import initialize_schema as initialize_control
from .host_adapter import initialize_schema as initialize_host
from .claude_adapter import initialize_schema as initialize_claude
from .collector import initialize_schema as initialize_collector
from .routing import ROUTED_TABLES


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
    apply_routing_schema(conn, at)


def apply_routing_schema(conn, at):
    conn.execute("CREATE TABLE IF NOT EXISTS ledger_mode(id INTEGER PRIMARY KEY CHECK(id=1),"
                 "mode TEXT NOT NULL CHECK(mode IN('isolated','mixed')))")
    conn.execute("INSERT OR IGNORE INTO ledger_mode VALUES(1,'isolated')")
    conn.execute('''CREATE TABLE IF NOT EXISTS route_audit(
        seq INTEGER PRIMARY KEY, action TEXT NOT NULL CHECK(action IN('activate','bind','register','disable')),
        object_table TEXT NOT NULL, object_id TEXT NOT NULL,
        execution_route TEXT CHECK(execution_route IN('http_synthetic','local_synthetic')),
        actor TEXT NOT NULL, at TEXT NOT NULL, evidence_ref TEXT, pre_state_hash TEXT, fence INTEGER)''')
    conn.execute('''CREATE INDEX IF NOT EXISTS route_audit_objects
        ON route_audit(object_table,object_id,execution_route)''')
    for table, key in ROUTED_TABLES.items():
        columns = {row[1] for row in conn.execute(f'PRAGMA table_info({table})')}
        if 'execution_route' not in columns:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN execution_route TEXT '
                         "CHECK(execution_route IN('http_synthetic','local_synthetic'))")
        index = {'tasks': 'status,kind,kind_version,created_at', 'devices': key,
                 'schedules': 'enabled,next_fire_at', 'schedule_firings': 'outcome,scheduled_at'}[table]
        conn.execute(f'CREATE INDEX IF NOT EXISTS {table}_route ON {table}(execution_route,{index})')
        conn.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_route_immutable
            BEFORE UPDATE OF execution_route ON {table}
            WHEN OLD.execution_route IS NOT NEW.execution_route AND
                 (OLD.execution_route IS NOT NULL OR (SELECT mode FROM ledger_mode WHERE id=1)='mixed')
            BEGIN SELECT RAISE(ABORT,'execution route is immutable'); END''')
        conn.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_route_required
            BEFORE INSERT ON {table} WHEN NEW.execution_route IS NULL
                 AND (SELECT mode FROM ledger_mode WHERE id=1)='mixed'
            BEGIN SELECT RAISE(ABORT,'execution route is required'); END''')
    conn.execute('''CREATE TRIGGER IF NOT EXISTS ledger_mode_immutable
        BEFORE UPDATE ON ledger_mode WHEN OLD.mode='mixed' AND NEW.mode!='mixed'
        BEGIN SELECT RAISE(ABORT,'mixed ledger mode is irreversible'); END''')
    conn.execute('''CREATE TRIGGER IF NOT EXISTS ledger_mode_required
        BEFORE DELETE ON ledger_mode
        BEGIN SELECT RAISE(ABORT,'ledger mode row cannot be deleted'); END''')
    conn.execute('''CREATE TRIGGER IF NOT EXISTS ledger_mode_no_replace
        BEFORE INSERT ON ledger_mode WHEN NEW.mode!='mixed' AND
            (SELECT mode FROM ledger_mode WHERE id=1)='mixed'
        BEGIN SELECT RAISE(ABORT,'mixed ledger mode is irreversible'); END''')
    conn.execute('INSERT OR IGNORE INTO schema_migrations VALUES(?,?,?)',
                 ('0012', 'm0_mixed_execution_routes', at))
