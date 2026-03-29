"""
Scenario management API routes.

Scenarios live in two places:
1. Registry DB (scimulator_registry.duckdb) — config, YAML, status, metadata
2. Result DBs (e.g. drawdown_tests.duckdb) — simulation output (events, snapshots, etc.)

The registry is the source of truth for scenario configuration.
Result DBs are populated only when a scenario is run.
"""

import logging
import tempfile
import traceback
from pathlib import Path
from typing import Optional

import yaml
from fastapi import APIRouter, HTTPException, Request, UploadFile, File, Form, Body
from pydantic import BaseModel

from ..services.db_manager import get_connection, list_databases
from ..services import registry
from ...simulator.loader import load_scenario_from_yaml, load_scenario_into_db
from ...simulator.engine import DrawdownEngine
from ...simulator.db import open_database, scenario_has_results, clear_scenario_results

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Database browsing (result DBs)
# ---------------------------------------------------------------------------

@router.get("/databases")
async def list_db_files(request: Request):
    """List available .duckdb files in the data directory (excludes registry)."""
    data_dir = request.app.state.data_dir
    all_dbs = list_databases(data_dir)
    return [db for db in all_dbs if db["name"] != registry.REGISTRY_DB_NAME]


@router.get("/databases/{db_name}/inspect")
async def inspect_database(db_name: str, request: Request):
    """Inspect a database: table row counts and scenario list."""
    db_path = _resolve_db(db_name, request)
    conn = get_connection(db_path, read_only=True)
    try:
        tables = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchall()

        table_counts = {}
        for (table_name,) in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if count > 0:
                table_counts[table_name] = count

        scenarios = conn.execute("""
            SELECT scenario_id, name, start_date, end_date
            FROM scenario
        """).fetchall()

        return {
            "database": db_name,
            "tables": table_counts,
            "scenarios": [
                {"scenario_id": sid, "name": name,
                 "start_date": str(start), "end_date": str(end)}
                for sid, name, start, end in scenarios
            ],
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Scenario list from result DB (legacy — reads from result DB)
# ---------------------------------------------------------------------------

@router.get("/scenarios")
async def list_scenarios(db: str, request: Request):
    """List all scenarios in a result database, with run status."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        rows = conn.execute("""
            SELECT
                s.scenario_id, s.name, s.description,
                s.start_date, s.end_date, s.currency_code,
                s.time_resolution, s.backorder_probability,
                r.status, r.total_steps, r.wall_clock_seconds,
                r.run_started_at, r.run_completed_at
            FROM scenario s
            LEFT JOIN run_metadata r ON s.scenario_id = r.scenario_id
            ORDER BY s.created_at DESC
        """).fetchall()

        return [
            {
                "scenario_id": row[0],
                "name": row[1],
                "description": row[2],
                "start_date": str(row[3]),
                "end_date": str(row[4]),
                "currency_code": row[5],
                "time_resolution": row[6],
                "backorder_probability": float(row[7]) if row[7] else None,
                "status": row[8],
                "total_steps": row[9],
                "wall_clock_seconds": float(row[10]) if row[10] else None,
                "run_started_at": str(row[11]) if row[11] else None,
                "run_completed_at": str(row[12]) if row[12] else None,
            }
            for row in rows
        ]
    finally:
        conn.close()


@router.get("/scenarios/{scenario_id}")
async def get_scenario(scenario_id: str, db: str, request: Request):
    """Get full scenario configuration from result DB."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        row = conn.execute(
            "SELECT * FROM scenario WHERE scenario_id = ?", [scenario_id]
        ).fetchone()
        if not row:
            raise HTTPException(404, f"Scenario not found: {scenario_id}")
        cols = [d[0] for d in conn.description]
        scenario = dict(zip(cols, row))
        for k, v in scenario.items():
            if hasattr(v, 'isoformat'):
                scenario[k] = v.isoformat()
            elif isinstance(v, (float, int, str, bool, type(None))):
                pass
            else:
                scenario[k] = str(v)

        meta_row = conn.execute(
            "SELECT * FROM run_metadata WHERE scenario_id = ?", [scenario_id]
        ).fetchone()
        if meta_row:
            meta_cols = [d[0] for d in conn.description]
            meta = dict(zip(meta_cols, meta_row))
            for k, v in meta.items():
                if hasattr(v, 'isoformat'):
                    meta[k] = v.isoformat()
                elif isinstance(v, (float, int, str, bool, type(None))):
                    pass
                else:
                    meta[k] = str(v)
            scenario['run_metadata'] = meta

        return scenario
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Registry CRUD — projects
# ---------------------------------------------------------------------------

class ProjectCreate(BaseModel):
    project_id: str
    name: str
    database: str
    description: str = ""


class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    database: Optional[str] = None


@router.get("/registry/projects")
async def list_projects(request: Request):
    """List all projects with scenario counts."""
    reg = request.app.state.registry
    return registry.list_projects(reg)


@router.get("/registry/projects/{project_id}")
async def get_project(project_id: str, request: Request):
    """Get a project by ID."""
    reg = request.app.state.registry
    project = registry.get_project(reg, project_id)
    if not project:
        raise HTTPException(404, f"Project not found: {project_id}")
    return project


@router.post("/registry/projects")
async def create_project(body: ProjectCreate, request: Request):
    """Create a new project."""
    reg = request.app.state.registry
    existing = registry.get_project(reg, body.project_id)
    if existing:
        raise HTTPException(409, f"Project already exists: {body.project_id}")
    return registry.save_project(
        reg, body.project_id, body.name, body.database,
        description=body.description,
    )


@router.put("/registry/projects/{project_id}")
async def update_project(project_id: str, body: ProjectUpdate, request: Request):
    """Update a project."""
    reg = request.app.state.registry
    existing = registry.get_project(reg, project_id)
    if not existing:
        raise HTTPException(404, f"Project not found: {project_id}")
    return registry.save_project(
        reg,
        project_id=project_id,
        name=body.name or existing['name'],
        database=body.database or existing['database'],
        description=body.description if body.description is not None else existing['description'],
    )


@router.delete("/registry/projects/{project_id}")
async def delete_project_endpoint(project_id: str, request: Request):
    """Delete a project and all its scenarios from the registry."""
    reg = request.app.state.registry
    if not registry.delete_project(reg, project_id):
        raise HTTPException(404, f"Project not found: {project_id}")
    return {"deleted": project_id}


# ---------------------------------------------------------------------------
# Registry CRUD — scenario configs
# ---------------------------------------------------------------------------

@router.get("/registry/projects/{project_id}/scenarios")
async def list_registry_scenarios(project_id: str, request: Request):
    """List all scenario configs for a project."""
    reg = request.app.state.registry
    return registry.list_scenarios(reg, project_id=project_id)


@router.get("/registry/projects/{project_id}/scenarios/{scenario_id}")
async def get_registry_scenario(project_id: str, scenario_id: str, request: Request):
    """Get a scenario config (including YAML) from the registry."""
    reg = request.app.state.registry
    scenario = registry.get_scenario(reg, scenario_id, project_id=project_id)
    if not scenario:
        raise HTTPException(404, f"Scenario not found in registry: {scenario_id}")
    return scenario


class ScenarioUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    yaml_content: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    currency_code: Optional[str] = None
    time_resolution: Optional[str] = None
    backorder_probability: Optional[float] = None
    notes: Optional[str] = None


@router.put("/registry/projects/{project_id}/scenarios/{scenario_id}")
async def update_registry_scenario(
    project_id: str,
    scenario_id: str,
    body: ScenarioUpdate,
    request: Request,
):
    """Update a scenario config in the registry."""
    reg = request.app.state.registry
    existing = registry.get_scenario(reg, scenario_id, project_id=project_id)
    if not existing:
        raise HTTPException(404, f"Scenario not found in registry: {scenario_id}")

    fields = {k: v for k, v in body.model_dump().items()
              if v is not None and k not in ('name', 'yaml_content')}

    return registry.save_scenario(
        reg,
        scenario_id=scenario_id,
        name=body.name or existing['name'],
        project_id=project_id,
        yaml_content=body.yaml_content,
        **fields,
    )


class ScenarioClone(BaseModel):
    new_scenario_id: str
    target_project_id: Optional[str] = None


@router.post("/registry/projects/{project_id}/scenarios/{scenario_id}/clone")
async def clone_registry_scenario(
    project_id: str,
    scenario_id: str,
    body: ScenarioClone,
    request: Request,
):
    """Clone a scenario, optionally into a different project. Full copy, no link."""
    reg = request.app.state.registry
    result = registry.clone_scenario(
        reg,
        source_scenario_id=scenario_id,
        new_scenario_id=body.new_scenario_id,
        source_project_id=project_id,
        target_project_id=body.target_project_id,
    )
    if not result:
        raise HTTPException(404, f"Scenario not found: {scenario_id}")
    return result


@router.delete("/registry/projects/{project_id}/scenarios/{scenario_id}")
async def delete_registry_scenario(
    project_id: str, scenario_id: str, request: Request,
):
    """Delete a scenario config from the registry."""
    reg = request.app.state.registry
    if not registry.delete_scenario(reg, scenario_id, project_id=project_id):
        raise HTTPException(404, f"Scenario not found in registry: {scenario_id}")
    return {"deleted": scenario_id}


@router.post("/registry/projects/{project_id}/scenarios/{scenario_id}/archive")
async def archive_registry_scenario(
    project_id: str, scenario_id: str, request: Request,
):
    """Archive a scenario (soft delete)."""
    reg = request.app.state.registry
    existing = registry.get_scenario(reg, scenario_id, project_id=project_id)
    if not existing:
        raise HTTPException(404, f"Scenario not found: {scenario_id}")
    registry.update_run_status(reg, scenario_id, 'archived', project_id=project_id)
    return {"archived": scenario_id}


# ---------------------------------------------------------------------------
# Run simulation — registers scenario, then runs
# ---------------------------------------------------------------------------

@router.post("/scenarios/run")
async def run_scenario(
    request: Request,
    scenario_file: UploadFile = File(...),
    demand_file: Optional[UploadFile] = File(None),
    db_name: Optional[str] = Form(None),
    replace: bool = Form(False),
    fork_id: Optional[str] = Form(None),
):
    """Upload a scenario YAML (and optional demand CSV), run the simulation.

    Also registers/updates the scenario in the registry DB.
    The scenario's `database` field in the YAML determines which project it
    belongs to (auto-created if needed). Falls back to 'default' project.
    """
    data_dir = request.app.state.data_dir
    reg = request.app.state.registry

    with tempfile.TemporaryDirectory() as tmpdir:
        # Write scenario YAML
        yaml_path = Path(tmpdir) / scenario_file.filename
        yaml_content = await scenario_file.read()
        yaml_path.write_bytes(yaml_content)

        # Write demand CSV if provided
        if demand_file:
            demand_path = Path(tmpdir) / demand_file.filename
            demand_content = await demand_file.read()
            demand_path.write_bytes(demand_content)

        # Load scenario config
        config = load_scenario_from_yaml(str(yaml_path))

        if fork_id:
            config.scenario_id = fork_id

        scenario_id = config.scenario_id

        # DB path priority: form db_name > config.database > derive from scenario_id
        if db_name:
            result_db_name = db_name
        elif config.database:
            result_db_name = f"{config.database}.duckdb"
        else:
            result_db_name = f"{scenario_id}.duckdb"
        db_path = str(data_dir / result_db_name)

        # Resolve project: database name is the project_id
        project_id = config.database or registry.DEFAULT_PROJECT_ID
        if not registry.get_project(reg, project_id):
            registry.save_project(
                reg, project_id, project_id, result_db_name,
            )

        # Check for existing results
        if Path(db_path).exists():
            check_conn = open_database(db_path)
            has_results = scenario_has_results(check_conn, scenario_id)
            if has_results:
                if replace:
                    clear_scenario_results(check_conn, scenario_id)
                else:
                    check_conn.close()
                    raise HTTPException(
                        409,
                        f"Scenario '{scenario_id}' already has results. "
                        f"Use replace=true or fork_id to run under a different ID."
                    )
            check_conn.close()

        # Register scenario in registry
        reg_fields = {}
        for attr in ('description', 'currency_code', 'time_resolution',
                     'start_date', 'end_date', 'warm_up_days',
                     'backorder_probability', 'write_event_log',
                     'write_snapshots', 'snapshot_interval_days',
                     'dataset_version_id', 'product_set_id',
                     'supply_node_set_id', 'distribution_node_set_id',
                     'demand_node_set_id', 'edge_set_id',
                     'demand_csv', 'inbound_schedule_csv',
                     'initial_inventory_csv', 'product_csv',
                     'customer_csv', 'distribution_nodes_csv',
                     'notes'):
            val = getattr(config, attr, None)
            if val is not None and val != '':
                reg_fields[attr] = val

        registry.save_scenario(
            reg,
            scenario_id=scenario_id,
            name=config.name,
            project_id=project_id,
            yaml_content=yaml_content.decode('utf-8'),
            status='running',
            **reg_fields,
        )

        # Load and run
        try:
            conn = load_scenario_into_db(config, db_path)
            engine = DrawdownEngine(conn, scenario_id)
            engine.run()

            # Read wall_clock from run_metadata
            wall_clock = None
            meta = conn.execute(
                "SELECT wall_clock_seconds FROM run_metadata WHERE scenario_id = ?",
                [scenario_id]
            ).fetchone()
            if meta and meta[0]:
                wall_clock = float(meta[0])

            conn.close()

            registry.update_run_status(
                reg, scenario_id, 'completed',
                project_id=project_id,
                wall_clock_seconds=wall_clock,
            )
        except Exception as e:
            logger.error(f"Simulation failed: {e}\n{traceback.format_exc()}")
            registry.update_run_status(
                reg, scenario_id, 'failed',
                project_id=project_id,
                error=str(e),
            )
            raise HTTPException(500, f"Simulation failed: {str(e)}")

        return {
            "scenario_id": scenario_id,
            "project_id": project_id,
            "database": result_db_name,
            "status": "completed",
        }


@router.post("/scenarios/{scenario_id}/rerun")
async def rerun_scenario(scenario_id: str, db: str, request: Request):
    """Re-run an existing scenario (clears old results)."""
    db_path = _resolve_db(db, request)
    reg = request.app.state.registry

    conn = open_database(db_path)
    if not scenario_has_results(conn, scenario_id):
        conn.close()
        raise HTTPException(404, f"No results to replace for scenario: {scenario_id}")
    clear_scenario_results(conn, scenario_id)

    registry.update_run_status(reg, scenario_id, 'running')

    try:
        engine = DrawdownEngine(conn, scenario_id)
        engine.run()

        wall_clock = None
        meta = conn.execute(
            "SELECT wall_clock_seconds FROM run_metadata WHERE scenario_id = ?",
            [scenario_id]
        ).fetchone()
        if meta and meta[0]:
            wall_clock = float(meta[0])

        registry.update_run_status(
            reg, scenario_id, 'completed',
            wall_clock_seconds=wall_clock,
        )
    except Exception as e:
        conn.close()
        logger.error(f"Re-run failed: {e}")
        registry.update_run_status(reg, scenario_id, 'failed', error=str(e))
        raise HTTPException(500, f"Simulation failed: {str(e)}")

    conn.close()
    return {"scenario_id": scenario_id, "status": "completed"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_db(db_name: str, request: Request) -> str:
    """Resolve a database name to a full path, checking it exists."""
    data_dir = request.app.state.data_dir
    db_path = data_dir / db_name
    if not db_path.exists():
        raise HTTPException(404, f"Database not found: {db_name}")
    return str(db_path)
