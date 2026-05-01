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
    tags: Optional[str] = None


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
# Scenario config editing (result DB)
# ---------------------------------------------------------------------------

# Fields in the scenario table that can be edited via the config form
EDITABLE_SCENARIO_FIELDS = {
    'name', 'description', 'currency_code', 'time_resolution',
    'start_date', 'end_date', 'warm_up_days', 'backorder_probability',
    'write_event_log', 'write_snapshots', 'snapshot_interval_days',
    'dataset_version_id', 'demand_version_id', 'inbound_version_id',
    'inventory_version_id',
    'product_set_id', 'supply_node_set_id', 'distribution_node_set_id',
    'demand_node_set_id', 'edge_set_id',
    'fulfillment_logic',
    'reorder_logic', 'reorder_resolution', 'reorder_allocation',
    'forecast_method', 'forecast_bias', 'forecast_error', 'forecast_distribution',
    'order_frequency_days', 'safety_stock_days', 'mrq_days',
    'consolidation_mode', 'min_cube_threshold',
    'notes',
}


@router.put("/scenarios/{scenario_id}/config")
async def update_scenario_config(
    scenario_id: str,
    db: str,
    request: Request,
    body: dict = Body(...),
):
    """Update scenario config fields in the result DB.

    Writes only to the result DB scenario table (the engine's source of truth).
    Also syncs name/description to the registry and sets status to 'modified'.
    """
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path)
    try:
        # Verify scenario exists
        exists = conn.execute(
            "SELECT COUNT(*) FROM scenario WHERE scenario_id = ?", [scenario_id]
        ).fetchone()[0]
        if not exists:
            raise HTTPException(404, f"Scenario not found: {scenario_id}")

        # Filter to only editable fields that are present in the body
        updates = {k: v for k, v in body.items() if k in EDITABLE_SCENARIO_FIELDS}
        if not updates:
            raise HTTPException(400, "No valid fields to update")

        # Build UPDATE statement
        set_clauses = [f"{k} = ?" for k in updates]
        values = list(updates.values())
        values.append(scenario_id)

        conn.execute(
            f"UPDATE scenario SET {', '.join(set_clauses)} WHERE scenario_id = ?",
            values,
        )
    finally:
        conn.close()

    # Sync to registry: set status to 'modified', update name/description
    reg = request.app.state.registry
    project_id = db.replace('.duckdb', '')
    reg_existing = registry.get_scenario(reg, scenario_id, project_id=project_id)
    if reg_existing:
        reg_fields = {}
        for attr in ('description', 'currency_code', 'time_resolution',
                     'start_date', 'end_date', 'backorder_probability'):
            if attr in updates:
                reg_fields[attr] = updates[attr]
        registry.save_scenario(
            reg,
            scenario_id=scenario_id,
            name=updates.get('name', reg_existing['name']),
            project_id=project_id,
            status='modified',
            **reg_fields,
        )

    return {"scenario_id": scenario_id, "status": "modified", "updated_fields": list(updates.keys())}


@router.get("/scenarios/{scenario_id}/config")
async def get_scenario_config(scenario_id: str, db: str, request: Request):
    """Get full scenario config from result DB for editing.

    Returns the scenario row plus available dataset versions.
    """
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

        # Get available dataset versions with scope info
        versions = conn.execute(
            "SELECT dataset_version_id, name, description FROM dataset_version ORDER BY name"
        ).fetchall()
        dataset_versions = [
            {"dataset_version_id": v[0], "name": v[1], "description": v[2]}
            for v in versions
        ]

        # Build per-table version lists using hybrid approach:
        # A version appears for a table if it has rows there, or is explicitly scoped, or is universal (no scopes)
        scoped = {}
        try:
            scope_rows = conn.execute(
                "SELECT dataset_version_id, table_name FROM dataset_version_scope"
            ).fetchall()
            for vid, tbl in scope_rows:
                scoped.setdefault(vid, set()).add(tbl)
        except Exception:
            pass  # table may not exist in older DBs

        data_tables = ['demand', 'inbound_schedule', 'initial_inventory']
        # Check which versions have actual rows in each table
        has_data: dict[str, set[str]] = {tbl: set() for tbl in data_tables}
        for tbl in data_tables:
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT dataset_version_id FROM {tbl}"
                ).fetchall()
                has_data[tbl] = {r[0] for r in rows}
            except Exception:
                pass

        # For each table, a version qualifies if:
        # 1. It has rows in that table, OR
        # 2. It is explicitly scoped to that table, OR
        # 3. It has no scope entries (universal)
        versions_by_table: dict[str, list] = {}
        for tbl in data_tables:
            qualified = []
            for v in dataset_versions:
                vid = v['dataset_version_id']
                is_universal = vid not in scoped
                is_scoped_here = vid in scoped and tbl in scoped[vid]
                has_rows = vid in has_data.get(tbl, set())
                if is_universal or is_scoped_here or has_rows:
                    qualified.append(v)
            versions_by_table[tbl] = qualified

        # Get available entity sets
        entity_sets: dict[str, list] = {}
        for set_table, id_col in [
            ('product_set', 'product_set_id'),
            ('supply_node_set', 'supply_node_set_id'),
            ('distribution_node_set', 'distribution_node_set_id'),
            ('demand_node_set', 'demand_node_set_id'),
            ('edge_set', 'edge_set_id'),
        ]:
            try:
                rows = conn.execute(
                    f"SELECT {id_col}, name, description FROM {set_table} ORDER BY name"
                ).fetchall()
                entity_sets[id_col] = [
                    {"id": r[0], "name": r[1], "description": r[2]}
                    for r in rows
                ]
            except Exception:
                entity_sets[id_col] = []

        return {
            "scenario": scenario,
            "dataset_versions": dataset_versions,
            "dataset_versions_by_table": versions_by_table,
            "entity_sets": entity_sets,
        }
    finally:
        conn.close()


@router.post("/scenarios/{scenario_id}/save-as")
async def save_scenario_as(
    scenario_id: str,
    db: str,
    request: Request,
):
    """Save As: duplicate scenario config in result DB with new ID and name.

    Returns the new scenario_id and name.
    """
    db_path = _resolve_db(db, request)
    reg = request.app.state.registry
    project_id = db.replace('.duckdb', '')

    conn = get_connection(db_path)
    try:
        # Read source scenario
        row = conn.execute(
            "SELECT * FROM scenario WHERE scenario_id = ?", [scenario_id]
        ).fetchone()
        if not row:
            raise HTTPException(404, f"Scenario not found: {scenario_id}")
        cols = [d[0] for d in conn.description]
        scenario = dict(zip(cols, row))

        # Generate new ID
        existing_rows = conn.execute("SELECT scenario_id FROM scenario").fetchall()
        existing_ids = {r[0] for r in existing_rows}
        new_id = generate_scenario_id(existing_ids)

        # Generate new name
        existing_names_rows = conn.execute("SELECT name FROM scenario").fetchall()
        existing_names = {r[0] for r in existing_names_rows}
        new_name = next_clone_name(scenario['name'], existing_names)

        # Insert new scenario row
        scenario['scenario_id'] = new_id
        scenario['name'] = new_name
        scenario['created_at'] = None  # let DB set default

        insert_cols = [c for c in cols if c != 'created_at']
        placeholders = ', '.join(['?'] * len(insert_cols))
        values = [scenario[c] for c in insert_cols]

        conn.execute(
            f"INSERT INTO scenario ({', '.join(insert_cols)}) VALUES ({placeholders})",
            values,
        )
    finally:
        conn.close()

    # Register in registry
    registry.save_scenario(
        reg,
        scenario_id=new_id,
        name=new_name,
        project_id=project_id,
        status='draft',
    )

    return {"scenario_id": new_id, "name": new_name}


@router.get("/scenarios/{scenario_id}/export-yaml")
async def export_scenario_yaml(scenario_id: str, db: str, request: Request):
    """Export a scenario config as YAML."""
    from fastapi.responses import Response

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

        # Convert types for YAML serialization
        for k, v in scenario.items():
            if hasattr(v, 'isoformat'):
                scenario[k] = v.isoformat()
            elif hasattr(v, '__float__'):
                scenario[k] = float(v)

        # Remove internal fields
        for key in ('created_at',):
            scenario.pop(key, None)

        yaml_content = yaml.dump(scenario, default_flow_style=False, sort_keys=False)
    finally:
        conn.close()

    return Response(
        content=yaml_content,
        media_type="application/x-yaml",
        headers={"Content-Disposition": f'attachment; filename="{scenario_id}.yaml"'},
    )


# ---------------------------------------------------------------------------
# Dataset management
# ---------------------------------------------------------------------------

@router.get("/datasets")
async def list_datasets(db: str, request: Request):
    """List all dataset versions with row counts, plus topology tables and entity sets.

    Returns:
    - dataset_versions: versions with per-table row counts and scenario usage
    - topology: row counts for topology tables (product, supply_node, etc.)
    - entity_sets: entity sets with member counts and scenario usage
    """
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        # ── Dataset versions ──────────────────────────────────────
        versions = conn.execute("""
            SELECT dataset_version_id, name, description, parent_version_id,
                   created_at, created_by
            FROM dataset_version
            ORDER BY name
        """).fetchall()

        dataset_versions = []
        for v in versions:
            vid = v[0]
            counts: dict[str, int] = {}

            for table in ('demand', 'inbound_schedule', 'initial_inventory'):
                try:
                    row = conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE dataset_version_id = ?",
                        [vid],
                    ).fetchone()
                    counts[table] = row[0] if row else 0
                except Exception:
                    counts[table] = 0

            scenario_rows = conn.execute("""
                SELECT scenario_id, name FROM scenario
                WHERE dataset_version_id = ?
                   OR demand_version_id = ?
                   OR inbound_version_id = ?
                   OR inventory_version_id = ?
            """, [vid, vid, vid, vid]).fetchall()

            dataset_versions.append({
                'dataset_version_id': vid,
                'name': v[1],
                'description': v[2],
                'parent_version_id': v[3],
                'created_at': str(v[4]) if v[4] else None,
                'created_by': v[5],
                'row_counts': counts,
                'scenarios': [
                    {'scenario_id': s[0], 'name': s[1]}
                    for s in scenario_rows
                ],
            })

        # ── Topology tables ───────────────────────────────────────
        topology_tables = [
            ('product', 'Products'),
            ('supply_node', 'Supply Nodes'),
            ('distribution_node', 'Distribution Nodes'),
            ('demand_node', 'Demand Nodes'),
            ('customer', 'Customers'),
            ('edge', 'Edges'),
        ]
        topology: list[dict] = []
        for table, label in topology_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except Exception:
                count = 0
            topology.append({
                'table': table,
                'label': label,
                'row_count': count,
            })

        # ── Entity sets with member counts and scenario usage ─────
        set_configs = [
            ('product_set', 'product_set_id', 'product_set_member', 'Products'),
            ('supply_node_set', 'supply_node_set_id', 'supply_node_set_member', 'Supply Nodes'),
            ('distribution_node_set', 'distribution_node_set_id', 'distribution_node_set_member', 'Distribution Nodes'),
            ('demand_node_set', 'demand_node_set_id', 'demand_node_set_member', 'Demand Nodes'),
            ('edge_set', 'edge_set_id', 'edge_set_member', 'Edges'),
        ]
        entity_sets: list[dict] = []
        for set_table, id_col, member_table, label in set_configs:
            try:
                rows = conn.execute(f"""
                    SELECT s.{id_col}, s.name, s.description,
                           (SELECT COUNT(*) FROM {member_table} m
                            WHERE m.{id_col} = s.{id_col}) as member_count
                    FROM {set_table} s
                    ORDER BY s.name
                """).fetchall()
            except Exception:
                rows = []

            for r in rows:
                set_id = r[0]
                # Find scenarios using this entity set
                try:
                    scenario_rows = conn.execute(
                        f"SELECT scenario_id, name FROM scenario WHERE {id_col} = ?",
                        [set_id],
                    ).fetchall()
                except Exception:
                    scenario_rows = []

                entity_sets.append({
                    'set_type': label,
                    'set_table': set_table,
                    'id_column': id_col,
                    'set_id': set_id,
                    'name': r[1],
                    'description': r[2],
                    'member_count': r[3],
                    'scenarios': [
                        {'scenario_id': s[0], 'name': s[1]}
                        for s in scenario_rows
                    ],
                })

        return {
            'dataset_versions': dataset_versions,
            'topology': topology,
            'entity_sets': entity_sets,
        }
    finally:
        conn.close()


_TOPOLOGY_TABLES = {'product', 'supply_node', 'distribution_node', 'demand_node', 'customer', 'edge'}


@router.get("/datasets/topology/{table}/schema")
async def topology_schema(table: str, db: str, request: Request):
    """Return column name + DuckDB type for a topology table, plus row count."""
    if table not in _TOPOLOGY_TABLES:
        return {'error': f'Unknown topology table: {table}'}
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        described = conn.execute(f"DESCRIBE {table}").fetchall()
        # DESCRIBE returns: column_name, column_type, null, key, default, extra
        columns = [
            {'name': r[0], 'type': r[1], 'nullable': (r[2] == 'YES'), 'key': r[3]}
            for r in described
        ]
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return {'table': table, 'columns': columns, 'row_count': row_count}
    finally:
        conn.close()


@router.get("/datasets/initial_inventory/summary")
async def initial_inventory_summary(db: str, request: Request):
    """Per-dataset-version initial-inventory summary: row count, distinct nodes, distinct products.

    Inventory value-at-cost is deferred (TODO(value)) until product-version pricing is wired up.
    """
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        versions = conn.execute("""
            SELECT dataset_version_id, name, description, created_at
            FROM dataset_version
            ORDER BY name
        """).fetchall()

        rows = []
        for v in versions:
            vid = v[0]
            try:
                stats = conn.execute("""
                    SELECT
                        COUNT(*) AS row_count,
                        COUNT(DISTINCT dist_node_id) AS node_count,
                        COUNT(DISTINCT product_id) AS product_count
                    FROM initial_inventory
                    WHERE dataset_version_id = ?
                """, [vid]).fetchone()
                row_count, node_count, product_count = stats
            except Exception:
                row_count, node_count, product_count = 0, 0, 0

            scenario_rows = conn.execute("""
                SELECT scenario_id, name FROM scenario
                WHERE dataset_version_id = ? OR inventory_version_id = ?
            """, [vid, vid]).fetchall()

            rows.append({
                'dataset_version_id': vid,
                'name': v[1],
                'description': v[2],
                'created_at': str(v[3]) if v[3] else None,
                'row_count': row_count or 0,
                'node_count': node_count or 0,
                'product_count': product_count or 0,
                'scenarios': [
                    {'scenario_id': s[0], 'name': s[1]} for s in scenario_rows
                ],
            })
        return {'dataset_versions': rows}
    finally:
        conn.close()


@router.get("/datasets/inbound_schedule/summary")
async def inbound_schedule_summary(db: str, request: Request):
    """Per-dataset-version inbound-schedule summary: row count, distinct supply/dest/product, date range."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        versions = conn.execute("""
            SELECT dataset_version_id, name, description, created_at
            FROM dataset_version
            ORDER BY name
        """).fetchall()

        rows = []
        for v in versions:
            vid = v[0]
            try:
                stats = conn.execute("""
                    SELECT
                        COUNT(*) AS row_count,
                        COUNT(DISTINCT supply_node_id) AS supply_count,
                        COUNT(DISTINCT dest_node_id) AS dest_count,
                        COUNT(DISTINCT product_id) AS product_count,
                        MIN(arrival_date) AS start_date,
                        MAX(arrival_date) AS end_date
                    FROM inbound_schedule
                    WHERE dataset_version_id = ?
                """, [vid]).fetchone()
                row_count, supply_count, dest_count, product_count, start_date, end_date = stats
            except Exception:
                row_count, supply_count, dest_count, product_count, start_date, end_date = 0, 0, 0, 0, None, None

            scenario_rows = conn.execute("""
                SELECT scenario_id, name FROM scenario
                WHERE dataset_version_id = ? OR inbound_version_id = ?
            """, [vid, vid]).fetchall()

            rows.append({
                'dataset_version_id': vid,
                'name': v[1],
                'description': v[2],
                'created_at': str(v[3]) if v[3] else None,
                'row_count': row_count or 0,
                'supply_node_count': supply_count or 0,
                'dest_node_count': dest_count or 0,
                'product_count': product_count or 0,
                'start_date': str(start_date) if start_date else None,
                'end_date': str(end_date) if end_date else None,
                'scenarios': [
                    {'scenario_id': s[0], 'name': s[1]} for s in scenario_rows
                ],
            })
        return {'dataset_versions': rows}
    finally:
        conn.close()


@router.get("/datasets/demand/summary")
async def demand_summary(db: str, request: Request):
    """Per-dataset-version demand summary: row count, date range, distinct demand-node count.

    Total Value / Total Qty are deferred (TODO(value)) until product-version pricing is wired up.
    """
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        versions = conn.execute("""
            SELECT dataset_version_id, name, description, created_at
            FROM dataset_version
            ORDER BY name
        """).fetchall()

        rows = []
        for v in versions:
            vid = v[0]
            try:
                stats = conn.execute("""
                    SELECT
                        COUNT(*) AS row_count,
                        MIN(demand_date) AS start_date,
                        MAX(demand_date) AS end_date,
                        COUNT(DISTINCT demand_node_id) AS demand_node_count
                    FROM demand
                    WHERE dataset_version_id = ?
                """, [vid]).fetchone()
                row_count, start_date, end_date, dn_count = stats
            except Exception:
                row_count, start_date, end_date, dn_count = 0, None, None, 0

            scenario_rows = conn.execute("""
                SELECT scenario_id, name FROM scenario
                WHERE dataset_version_id = ? OR demand_version_id = ?
            """, [vid, vid]).fetchall()

            rows.append({
                'dataset_version_id': vid,
                'name': v[1],
                'description': v[2],
                'created_at': str(v[3]) if v[3] else None,
                'row_count': row_count or 0,
                'start_date': str(start_date) if start_date else None,
                'end_date': str(end_date) if end_date else None,
                'demand_node_count': dn_count or 0,
                'scenarios': [
                    {'scenario_id': s[0], 'name': s[1]} for s in scenario_rows
                ],
            })
        return {'dataset_versions': rows}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entity set management
# ---------------------------------------------------------------------------

# Mapping from set_table to (id_col, member_table, member_id_col, source_table, source_id_col, label)
ENTITY_SET_CONFIGS: dict[str, tuple[str, str, str, str, str, str]] = {
    'product_set': ('product_set_id', 'product_set_member', 'product_id', 'product', 'product_id', 'Products'),
    'supply_node_set': ('supply_node_set_id', 'supply_node_set_member', 'supply_node_id', 'supply_node', 'supply_node_id', 'Supply Nodes'),
    'distribution_node_set': ('distribution_node_set_id', 'distribution_node_set_member', 'dist_node_id', 'distribution_node', 'dist_node_id', 'Distribution Nodes'),
    'demand_node_set': ('demand_node_set_id', 'demand_node_set_member', 'demand_node_id', 'demand_node', 'demand_node_id', 'Demand Nodes'),
    'edge_set': ('edge_set_id', 'edge_set_member', 'edge_id', 'edge', 'edge_id', 'Edges'),
}

# Which columns to show for each source table (id + display columns)
ENTITY_TABLE_COLUMNS: dict[str, list[str]] = {
    'product': ['product_id', 'name', 'standard_cost', 'base_price', 'weight', 'weight_uom', 'cube', 'cube_uom'],
    'supply_node': ['supply_node_id', 'supplier_id', 'name', 'latitude', 'longitude'],
    'distribution_node': ['dist_node_id', 'name', 'latitude', 'longitude', 'zip3'],
    'demand_node': ['demand_node_id', 'name', 'latitude', 'longitude', 'zip3'],
    'edge': ['edge_id', 'origin_node_id', 'origin_node_type', 'dest_node_id', 'dest_node_type', 'transport_type'],
}


@router.get("/entity-sets/tables")
async def list_entity_set_tables(db: str, request: Request):
    """List the entity tables that support entity sets, with row counts."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        tables = []
        for set_table, (id_col, member_table, member_id_col, source_table, source_id_col, label) in ENTITY_SET_CONFIGS.items():
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
            except Exception:
                count = 0
            tables.append({
                'set_table': set_table,
                'source_table': source_table,
                'label': label,
                'id_column': id_col,
                'member_id_column': member_id_col,
                'row_count': count,
                'columns': ENTITY_TABLE_COLUMNS.get(source_table, [source_id_col]),
            })
        return {'tables': tables}
    finally:
        conn.close()


@router.get("/entity-sets/items")
async def list_entity_items(db: str, set_table: str, request: Request):
    """List all items from the source table for a given entity set type."""
    db_path = _resolve_db(db, request)
    if set_table not in ENTITY_SET_CONFIGS:
        raise HTTPException(400, f"Unknown set table: {set_table}")
    id_col, member_table, member_id_col, source_table, source_id_col, label = ENTITY_SET_CONFIGS[set_table]
    columns = ENTITY_TABLE_COLUMNS.get(source_table, [source_id_col])

    conn = get_connection(db_path, read_only=True)
    try:
        rows = conn.execute(
            f"SELECT {', '.join(columns)} FROM {source_table} ORDER BY {columns[0]}"
        ).fetchall()
        return {
            'columns': columns,
            'rows': [dict(zip(columns, [str(v) if v is not None else None for v in row])) for row in rows],
        }
    finally:
        conn.close()


class CreateEntitySetRequest(BaseModel):
    set_table: str
    name: str
    description: str = ''
    member_ids: list[str]


@router.post("/entity-sets")
async def create_entity_set(db: str, body: CreateEntitySetRequest, request: Request):
    """Create a new entity set with the given members."""
    db_path = _resolve_db(db, request)
    if body.set_table not in ENTITY_SET_CONFIGS:
        raise HTTPException(400, f"Unknown set table: {body.set_table}")
    if not body.name.strip():
        raise HTTPException(400, "Name is required")
    if not body.member_ids:
        raise HTTPException(400, "At least one member is required")

    id_col, member_table, member_id_col, source_table, source_id_col, label = ENTITY_SET_CONFIGS[body.set_table]

    conn = get_connection(db_path)
    try:
        # Generate unique ID using word pool
        existing = {r[0] for r in conn.execute(f"SELECT {id_col} FROM {body.set_table}").fetchall()}
        set_id = generate_scenario_id(existing)

        conn.execute(
            f"INSERT INTO {body.set_table} ({id_col}, name, description) VALUES (?, ?, ?)",
            [set_id, body.name.strip(), body.description.strip() or None],
        )
        for mid in body.member_ids:
            conn.execute(
                f"INSERT INTO {member_table} ({id_col}, {member_id_col}) VALUES (?, ?)",
                [set_id, mid],
            )
        return {'set_id': set_id, 'name': body.name.strip(), 'member_count': len(body.member_ids)}
    finally:
        conn.close()


@router.delete("/entity-sets")
async def delete_entity_set(db: str, set_table: str, set_id: str, request: Request):
    """Delete an entity set, unless it is used by any scenario."""
    db_path = _resolve_db(db, request)
    if set_table not in ENTITY_SET_CONFIGS:
        raise HTTPException(400, f"Unknown set table: {set_table}")

    id_col, member_table, member_id_col, source_table, source_id_col, label = ENTITY_SET_CONFIGS[set_table]

    conn = get_connection(db_path)
    try:
        # Check if any scenario references this set (including archived)
        scenario_rows = conn.execute(
            f"SELECT scenario_id, name FROM scenario WHERE {id_col} = ?",
            [set_id],
        ).fetchall()
        if scenario_rows:
            names = ', '.join(r[1] for r in scenario_rows)
            raise HTTPException(
                409,
                f"Cannot delete: entity set is used by scenario(s): {names}",
            )

        conn.execute(f"DELETE FROM {member_table} WHERE {id_col} = ?", [set_id])
        conn.execute(f"DELETE FROM {set_table} WHERE {id_col} = ?", [set_id])
        return {'status': 'deleted', 'set_id': set_id}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_db(db_name: str, request: Request) -> str:
    """Resolve a database name to a full path, checking it exists."""
    data_dir = request.app.state.data_dir
    db_path = data_dir / db_name
    if not db_path.exists() and not db_name.endswith('.duckdb'):
        db_path = data_dir / f"{db_name}.duckdb"
    if not db_path.exists():
        raise HTTPException(404, f"Database not found: {db_name}")
    return str(db_path)
