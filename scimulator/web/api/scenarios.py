"""
Scenario management API routes.
"""

import logging
import tempfile
import traceback
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, UploadFile, File, Form

from ..services.db_manager import get_connection, list_databases
from ...simulator.loader import load_scenario_from_yaml, load_scenario_into_db
from ...simulator.engine import DrawdownEngine
from ...simulator.db import open_database, scenario_has_results, clear_scenario_results

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/databases")
async def list_db_files(request: Request):
    """List available .duckdb files in the data directory."""
    data_dir = request.app.state.data_dir
    return list_databases(data_dir)


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


@router.get("/scenarios")
async def list_scenarios(db: str, request: Request):
    """List all scenarios in a database, with run status."""
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
    """Get full scenario configuration."""
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
        # Convert non-serializable types
        for k, v in scenario.items():
            if hasattr(v, 'isoformat'):
                scenario[k] = v.isoformat()
            elif isinstance(v, (float, int, str, bool, type(None))):
                pass
            else:
                scenario[k] = str(v)

        # Get run metadata if exists
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


@router.post("/scenarios/run")
async def run_scenario(
    request: Request,
    scenario_file: UploadFile = File(...),
    demand_file: Optional[UploadFile] = File(None),
    db_name: Optional[str] = Form(None),
    replace: bool = Form(False),
    fork_id: Optional[str] = Form(None),
):
    """Upload a scenario YAML (and optional demand CSV), run the simulation."""
    data_dir = request.app.state.data_dir

    # Save uploaded files to temp directory
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

        # Determine DB path
        if db_name:
            db_path = str(data_dir / db_name)
        else:
            db_path = str(data_dir / f"{scenario_id}.duckdb")

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

        # Load and run
        try:
            conn = load_scenario_into_db(config, db_path)
            engine = DrawdownEngine(conn, scenario_id)
            engine.run()
            conn.close()
        except Exception as e:
            logger.error(f"Simulation failed: {e}\n{traceback.format_exc()}")
            raise HTTPException(500, f"Simulation failed: {str(e)}")

        return {
            "scenario_id": scenario_id,
            "database": Path(db_path).name,
            "status": "completed",
        }


@router.post("/scenarios/{scenario_id}/rerun")
async def rerun_scenario(scenario_id: str, db: str, request: Request):
    """Re-run an existing scenario (clears old results)."""
    db_path = _resolve_db(db, request)

    conn = open_database(db_path)
    if not scenario_has_results(conn, scenario_id):
        conn.close()
        raise HTTPException(404, f"No results to replace for scenario: {scenario_id}")
    clear_scenario_results(conn, scenario_id)

    try:
        engine = DrawdownEngine(conn, scenario_id)
        engine.run()
    except Exception as e:
        conn.close()
        logger.error(f"Re-run failed: {e}")
        raise HTTPException(500, f"Simulation failed: {str(e)}")

    conn.close()
    return {"scenario_id": scenario_id, "status": "completed"}


def _resolve_db(db_name: str, request: Request) -> str:
    """Resolve a database name to a full path, checking it exists."""
    data_dir = request.app.state.data_dir
    db_path = data_dir / db_name
    if not db_path.exists():
        raise HTTPException(404, f"Database not found: {db_name}")
    return str(db_path)
