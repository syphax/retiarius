"""
Scenario registry database.

A per-org DuckDB that stores scenario configurations (both parsed fields
and raw YAML), tracks run status, and will eventually hold user models.

Data model:
  org
   └── project (= result DB + grouping in registry)
        └── scenario (config in registry, results in project's DB)

  user ──belongs_to──> org
  user ──has_access_to──> project (via role)

Separate from result databases, which hold simulation output.
"""

import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

import duckdb

logger = logging.getLogger(__name__)

REGISTRY_DB_NAME = "scimulator_registry.duckdb"

# Defaults for single-user mode; replaced by real management later
DEFAULT_ORG_ID = "default"
DEFAULT_ORG_NAME = "Default Organization"
DEFAULT_PROJECT_ID = "default"
DEFAULT_PROJECT_NAME = "Default Project"


def _create_schema(conn: duckdb.DuckDBPyConnection):
    """Create the registry schema. Idempotent."""

    conn.execute("""
        CREATE TABLE IF NOT EXISTS organization (
            org_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS project (
            project_id TEXT NOT NULL,
            org_id TEXT NOT NULL DEFAULT 'default',
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            database TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (project_id, org_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS scenario_config (
            scenario_id TEXT NOT NULL,
            project_id TEXT NOT NULL DEFAULT 'default',
            org_id TEXT NOT NULL DEFAULT 'default',
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            yaml_content TEXT,

            -- Parsed config fields for UI display/editing
            currency_code TEXT DEFAULT 'USD',
            time_resolution TEXT DEFAULT 'daily',
            start_date DATE,
            end_date DATE,
            warm_up_days INTEGER DEFAULT 0,
            backorder_probability DECIMAL(5,4) DEFAULT 1.0,
            write_event_log BOOLEAN DEFAULT TRUE,
            write_snapshots BOOLEAN DEFAULT TRUE,
            snapshot_interval_days INTEGER DEFAULT 1,

            -- Entity set references
            dataset_version_id TEXT,
            product_set_id TEXT,
            supply_node_set_id TEXT,
            distribution_node_set_id TEXT,
            demand_node_set_id TEXT,
            edge_set_id TEXT,

            -- Data file paths
            demand_csv TEXT,
            inbound_schedule_csv TEXT,
            initial_inventory_csv TEXT,
            product_csv TEXT,
            customer_csv TEXT,
            distribution_nodes_csv TEXT,

            -- Run tracking
            status TEXT DEFAULT 'draft',
            last_run_at TIMESTAMP,
            run_wall_clock_seconds DECIMAL(10,2),
            run_error TEXT,

            -- Timestamps
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

            notes TEXT DEFAULT '',

            PRIMARY KEY (scenario_id, project_id, org_id)
        )
    """)

    # Seed defaults
    conn.execute("""
        INSERT OR IGNORE INTO organization (org_id, name) VALUES (?, ?)
    """, [DEFAULT_ORG_ID, DEFAULT_ORG_NAME])

    conn.execute("""
        INSERT OR IGNORE INTO project (project_id, org_id, name, database)
        VALUES (?, ?, ?, ?)
    """, [DEFAULT_PROJECT_ID, DEFAULT_ORG_ID, DEFAULT_PROJECT_NAME,
          "default.duckdb"])


def init_registry(data_dir: Path) -> duckdb.DuckDBPyConnection:
    """Initialize (or open) the registry DB. Returns a connection.

    Called once at app startup. The connection is stored on app.state
    and shared across requests.
    """
    db_path = data_dir / REGISTRY_DB_NAME
    conn = duckdb.connect(str(db_path))
    _create_schema(conn)
    logger.info(f"Registry DB ready: {db_path}")
    return conn


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------

def list_projects(
    conn: duckdb.DuckDBPyConnection,
    org_id: str = DEFAULT_ORG_ID,
) -> List[Dict]:
    """List all projects for an org."""
    rows = conn.execute("""
        SELECT p.project_id, p.org_id, p.name, p.description, p.database,
               p.created_at, p.updated_at,
               COUNT(s.scenario_id) as scenario_count
        FROM project p
        LEFT JOIN scenario_config s
            ON p.project_id = s.project_id AND p.org_id = s.org_id
        WHERE p.org_id = ?
        GROUP BY p.project_id, p.org_id, p.name, p.description, p.database,
                 p.created_at, p.updated_at
        ORDER BY p.updated_at DESC
    """, [org_id]).fetchall()

    cols = ['project_id', 'org_id', 'name', 'description', 'database',
            'created_at', 'updated_at', 'scenario_count']
    return [_row_to_dict(cols, r) for r in rows]


def get_project(
    conn: duckdb.DuckDBPyConnection,
    project_id: str,
    org_id: str = DEFAULT_ORG_ID,
) -> Optional[Dict]:
    """Get a project by ID."""
    row = conn.execute("""
        SELECT * FROM project
        WHERE project_id = ? AND org_id = ?
    """, [project_id, org_id]).fetchone()
    if not row:
        return None
    cols = [d[0] for d in conn.description]
    return _row_to_dict(cols, row)


def save_project(
    conn: duckdb.DuckDBPyConnection,
    project_id: str,
    name: str,
    database: str,
    org_id: str = DEFAULT_ORG_ID,
    description: str = "",
) -> Dict:
    """Create or update a project. Returns the saved record."""
    now = datetime.now()
    existing = get_project(conn, project_id, org_id)

    if existing:
        conn.execute("""
            UPDATE project
            SET name = ?, description = ?, database = ?, updated_at = ?
            WHERE project_id = ? AND org_id = ?
        """, [name, description, database, now, project_id, org_id])
    else:
        conn.execute("""
            INSERT INTO project (project_id, org_id, name, description, database,
                                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [project_id, org_id, name, description, database, now, now])

    return get_project(conn, project_id, org_id)


def delete_project(
    conn: duckdb.DuckDBPyConnection,
    project_id: str,
    org_id: str = DEFAULT_ORG_ID,
) -> bool:
    """Delete a project and all its scenarios. Returns True if it existed."""
    existing = get_project(conn, project_id, org_id)
    if not existing:
        return False
    conn.execute("""
        DELETE FROM scenario_config
        WHERE project_id = ? AND org_id = ?
    """, [project_id, org_id])
    conn.execute("""
        DELETE FROM project
        WHERE project_id = ? AND org_id = ?
    """, [project_id, org_id])
    return True


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def list_scenarios(
    conn: duckdb.DuckDBPyConnection,
    project_id: str = DEFAULT_PROJECT_ID,
    org_id: str = DEFAULT_ORG_ID,
    include_archived: bool = False,
) -> List[Dict]:
    """List all scenarios for a project. Excludes archived by default."""
    query = """
        SELECT scenario_id, project_id, name, description,
               start_date, end_date, currency_code, time_resolution,
               backorder_probability, status, last_run_at,
               run_wall_clock_seconds, created_at, updated_at
        FROM scenario_config
        WHERE project_id = ? AND org_id = ?
    """
    if not include_archived:
        query += " AND (status IS NULL OR status != 'archived')"
    query += " ORDER BY updated_at DESC"
    rows = conn.execute(query, [project_id, org_id]).fetchall()

    cols = ['scenario_id', 'project_id', 'name', 'description',
            'start_date', 'end_date', 'currency_code', 'time_resolution',
            'backorder_probability', 'status', 'last_run_at',
            'run_wall_clock_seconds', 'created_at', 'updated_at']

    return [_row_to_dict(cols, r) for r in rows]


def get_scenario(
    conn: duckdb.DuckDBPyConnection,
    scenario_id: str,
    project_id: str = DEFAULT_PROJECT_ID,
    org_id: str = DEFAULT_ORG_ID,
) -> Optional[Dict]:
    """Get full scenario config including YAML."""
    row = conn.execute("""
        SELECT * FROM scenario_config
        WHERE scenario_id = ? AND project_id = ? AND org_id = ?
    """, [scenario_id, project_id, org_id]).fetchone()
    if not row:
        return None
    cols = [d[0] for d in conn.description]
    return _row_to_dict(cols, row)


def save_scenario(
    conn: duckdb.DuckDBPyConnection,
    scenario_id: str,
    name: str,
    project_id: str = DEFAULT_PROJECT_ID,
    yaml_content: Optional[str] = None,
    org_id: str = DEFAULT_ORG_ID,
    **fields,
) -> Dict:
    """Create or update a scenario config. Returns the saved record."""
    now = datetime.now()
    existing = get_scenario(conn, scenario_id, project_id, org_id)

    if existing:
        # Update
        set_clauses = ["name = ?", "updated_at = ?"]
        params = [name, now]

        if yaml_content is not None:
            set_clauses.append("yaml_content = ?")
            params.append(yaml_content)

        for k, v in fields.items():
            set_clauses.append(f"{k} = ?")
            params.append(v)

        params.extend([scenario_id, project_id, org_id])
        conn.execute(f"""
            UPDATE scenario_config
            SET {', '.join(set_clauses)}
            WHERE scenario_id = ? AND project_id = ? AND org_id = ?
        """, params)
    else:
        # Insert
        all_fields = {
            'scenario_id': scenario_id,
            'project_id': project_id,
            'org_id': org_id,
            'name': name,
            'yaml_content': yaml_content,
            'created_at': now,
            'updated_at': now,
            **fields,
        }
        col_names = ', '.join(all_fields.keys())
        placeholders = ', '.join(['?'] * len(all_fields))
        conn.execute(
            f"INSERT INTO scenario_config ({col_names}) VALUES ({placeholders})",
            list(all_fields.values()),
        )

    return get_scenario(conn, scenario_id, project_id, org_id)


def clone_scenario(
    conn: duckdb.DuckDBPyConnection,
    source_scenario_id: str,
    new_scenario_id: str,
    source_project_id: str = DEFAULT_PROJECT_ID,
    target_project_id: Optional[str] = None,
    org_id: str = DEFAULT_ORG_ID,
) -> Optional[Dict]:
    """Clone a scenario, optionally into a different project.

    Creates a full independent copy — no link back to the source.
    Returns the new scenario, or None if source not found.
    """
    source = get_scenario(conn, source_scenario_id, source_project_id, org_id)
    if not source:
        return None

    target_proj = target_project_id or source_project_id

    # Copy all fields except IDs and timestamps
    fields = {k: v for k, v in source.items()
              if k not in ('scenario_id', 'project_id', 'org_id',
                           'created_at', 'updated_at', 'name',
                           'status', 'last_run_at', 'run_wall_clock_seconds',
                           'run_error')}

    return save_scenario(
        conn,
        scenario_id=new_scenario_id,
        name=f"{source['name']} (clone)",
        project_id=target_proj,
        org_id=org_id,
        status='draft',
        **fields,
    )


def update_run_status(
    conn: duckdb.DuckDBPyConnection,
    scenario_id: str,
    status: str,
    project_id: str = DEFAULT_PROJECT_ID,
    org_id: str = DEFAULT_ORG_ID,
    wall_clock_seconds: Optional[float] = None,
    error: Optional[str] = None,
):
    """Update scenario run status after a simulation completes or fails."""
    conn.execute("""
        UPDATE scenario_config
        SET status = ?,
            last_run_at = CURRENT_TIMESTAMP,
            run_wall_clock_seconds = ?,
            run_error = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE scenario_id = ? AND project_id = ? AND org_id = ?
    """, [status, wall_clock_seconds, error, scenario_id, project_id, org_id])


def delete_scenario(
    conn: duckdb.DuckDBPyConnection,
    scenario_id: str,
    project_id: str = DEFAULT_PROJECT_ID,
    org_id: str = DEFAULT_ORG_ID,
) -> bool:
    """Delete a scenario config. Returns True if it existed."""
    existing = get_scenario(conn, scenario_id, project_id, org_id)
    if not existing:
        return False
    conn.execute("""
        DELETE FROM scenario_config
        WHERE scenario_id = ? AND project_id = ? AND org_id = ?
    """, [scenario_id, project_id, org_id])
    return True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row_to_dict(cols: List[str], row: tuple) -> Dict:
    """Convert a DuckDB row to a dict, serializing dates/timestamps."""
    d = {}
    for col, val in zip(cols, row):
        if hasattr(val, 'isoformat'):
            val = val.isoformat()
        elif isinstance(val, float):
            val = round(val, 4)
        d[col] = val
    return d
