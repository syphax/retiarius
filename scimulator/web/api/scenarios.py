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
from ..services.word_pool import generate_scenario_id, next_clone_name
from ...simulator.loader import load_scenario_from_yaml, load_scenario_into_db
from ...simulator.engine import DrawdownEngine
from ...simulator.db import open_database, scenario_has_results, clear_scenario_results, clone_scenario_data

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
    """List all scenarios in a result database, with run status.

    Also backfills any unregistered scenarios into the registry.
    """
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    reg = request.app.state.registry
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

        # Derive project_id from db filename (strip .duckdb)
        project_id = db.replace('.duckdb', '')

        # Ensure project exists in registry
        if not registry.get_project(reg, project_id):
            registry.save_project(reg, project_id, project_id, db)

        # Backfill: register any scenarios not yet in the registry
        for row in rows:
            scenario_id = row[0]
            if not registry.get_scenario(reg, scenario_id, project_id=project_id):
                reg_fields = {}
                if row[2]:
                    reg_fields['description'] = row[2]
                if row[3]:
                    reg_fields['start_date'] = str(row[3])
                if row[4]:
                    reg_fields['end_date'] = str(row[4])
                if row[5]:
                    reg_fields['currency_code'] = row[5]
                if row[6]:
                    reg_fields['time_resolution'] = row[6]
                if row[7] is not None:
                    reg_fields['backorder_probability'] = float(row[7])
                status = row[8] or 'completed'
                registry.save_scenario(
                    reg,
                    scenario_id=scenario_id,
                    name=row[1],
                    project_id=project_id,
                    status=status,
                    **reg_fields,
                )
                if row[10] is not None:
                    registry.update_run_status(
                        reg, scenario_id, status,
                        project_id=project_id,
                        wall_clock_seconds=float(row[10]),
                    )

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


class ProjectClone(BaseModel):
    new_name: str


@router.post("/registry/projects/{project_id}/clone")
async def clone_project_endpoint(
    project_id: str, body: ProjectClone, request: Request,
):
    """Clone a project: copies the DB file and all registry entries."""
    reg = request.app.state.registry
    data_dir = request.app.state.data_dir

    # Generate a project_id from the name (slugify)
    new_project_id = body.new_name.lower().replace(' ', '_')
    new_database = f"{new_project_id}.duckdb"

    if registry.get_project(reg, new_project_id):
        raise HTTPException(409, f"Project already exists: {new_project_id}")

    result = registry.clone_project(
        reg,
        source_project_id=project_id,
        new_project_id=new_project_id,
        new_name=body.new_name,
        new_database=new_database,
        data_dir=data_dir,
    )
    if not result:
        raise HTTPException(404, f"Project not found: {project_id}")
    return result


@router.post("/registry/projects/{project_id}/archive")
async def archive_project_endpoint(project_id: str, request: Request):
    """Archive a project (soft delete)."""
    reg = request.app.state.registry
    if not registry.archive_project(reg, project_id):
        raise HTTPException(404, f"Project not found: {project_id}")
    return {"archived": project_id}


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

    result = registry.save_scenario(
        reg,
        scenario_id=scenario_id,
        name=body.name or existing['name'],
        project_id=project_id,
        yaml_content=body.yaml_content,
        **fields,
    )

    # Write-through: sync name/description to the result DB
    if body.name is not None or body.description is not None:
        data_dir = request.app.state.data_dir
        project = registry.get_project(reg, project_id)
        if project:
            db_path = data_dir / project['database']
            if db_path.exists():
                try:
                    conn = get_connection(str(db_path))
                    if body.name is not None:
                        conn.execute("UPDATE scenario SET name = ? WHERE scenario_id = ?",
                                     [body.name, scenario_id])
                    if body.description is not None:
                        conn.execute("UPDATE scenario SET description = ? WHERE scenario_id = ?",
                                     [body.description, scenario_id])
                    conn.close()
                except Exception as e:
                    logger.warning(f"Write-through to result DB failed: {e}")

    return result


class ScenarioClone(BaseModel):
    new_scenario_id: Optional[str] = None
    new_name: Optional[str] = None
    target_project_id: Optional[str] = None


@router.post("/registry/projects/{project_id}/scenarios/{scenario_id}/clone")
async def clone_registry_scenario(
    project_id: str,
    scenario_id: str,
    body: ScenarioClone,
    request: Request,
):
    """Clone a scenario, optionally into a different project.

    Auto-generates scenario ID (random 5-letter word) and name
    ("<original> clone 01") if not provided.

    Full independent copy. If the source has results in the result DB,
    those are cloned too (so the copy is immediately viewable).
    """
    reg = request.app.state.registry
    data_dir = request.app.state.data_dir
    target_proj = body.target_project_id or project_id

    # Auto-generate scenario ID if not provided
    if body.new_scenario_id:
        new_id = body.new_scenario_id
    else:
        existing = registry.list_scenarios(reg, project_id=target_proj, include_archived=True)
        existing_ids = {s['scenario_id'] for s in existing}
        new_id = generate_scenario_id(existing_ids)

    # Auto-generate clone name if not provided
    if body.new_name:
        new_name = body.new_name
    else:
        source = registry.get_scenario(reg, scenario_id, project_id=project_id)
        if not source:
            raise HTTPException(404, f"Scenario not found: {scenario_id}")
        existing = registry.list_scenarios(reg, project_id=target_proj, include_archived=True)
        existing_names = {s['name'] for s in existing}
        new_name = next_clone_name(source['name'], existing_names)

    # Clone in registry
    result = registry.clone_scenario(
        reg,
        source_scenario_id=scenario_id,
        new_scenario_id=new_id,
        new_name=new_name,
        source_project_id=project_id,
        target_project_id=body.target_project_id,
    )
    if not result:
        raise HTTPException(404, f"Scenario not found: {scenario_id}")

    # Clone result DB data if source has results
    source_project = registry.get_project(reg, project_id)
    if source_project:
        db_path = data_dir / source_project['database']
        if db_path.exists():
            try:
                conn = open_database(str(db_path))
                try:
                    if scenario_has_results(conn, scenario_id):
                        clone_scenario_data(conn, scenario_id, new_id)
                        # Update registry status to match source
                        source_reg = registry.get_scenario(reg, scenario_id, project_id=project_id)
                        if source_reg and source_reg.get('status') == 'completed':
                            registry.update_run_status(
                                reg, new_id, 'completed',
                                project_id=target_proj,
                                wall_clock_seconds=source_reg.get('run_wall_clock_seconds'),
                            )
                finally:
                    conn.close()
            except Exception as e:
                logger.error(f"Failed to clone result DB data: {e}\n{traceback.format_exc()}")
                raise HTTPException(500, f"Failed to clone scenario data: {str(e)}")

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
                     'dataset_version_id', 'demand_version_id',
                     'inbound_version_id', 'inventory_version_id',
                     'product_set_id',
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
    """Re-run a scenario. Works for both existing results (clear + re-execute)
    and draft scenarios (first run from scenario config in result DB).
    """
    db_path = _resolve_db(db, request)
    reg = request.app.state.registry
    project_id = db.replace('.duckdb', '')

    conn = open_database(db_path)

    # Check if scenario exists in result DB at all
    has_scenario = conn.execute(
        "SELECT COUNT(*) FROM scenario WHERE scenario_id = ?", [scenario_id]
    ).fetchone()[0] > 0

    if not has_scenario:
        conn.close()
        raise HTTPException(404, f"Scenario not found in database: {scenario_id}")

    # Clear old results if any
    if scenario_has_results(conn, scenario_id):
        clear_scenario_results(conn, scenario_id)

    registry.update_run_status(reg, scenario_id, 'running', project_id=project_id)

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
            project_id=project_id,
            wall_clock_seconds=wall_clock,
        )
    except Exception as e:
        conn.close()
        logger.error(f"Re-run failed: {e}")
        registry.update_run_status(reg, scenario_id, 'failed',
                                   project_id=project_id, error=str(e))
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
