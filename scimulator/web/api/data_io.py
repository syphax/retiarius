"""
Import/export API routes.
"""

import io
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse, FileResponse

from ..services.db_manager import get_connection

router = APIRouter()


@router.get("/export/{scenario_id}/events.csv")
async def export_events_csv(scenario_id: str, db: str, request: Request):
    """Download the event log as CSV."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        df = conn.execute("""
            SELECT * FROM event_log
            WHERE scenario_id = ?
            ORDER BY sim_step, event_type
        """, [scenario_id]).fetchdf()

        if df.empty:
            raise HTTPException(404, f"No events for scenario: {scenario_id}")

        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        return StreamingResponse(
            iter([buffer.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={scenario_id}_events.csv"},
        )
    finally:
        conn.close()


@router.get("/export/{scenario_id}/snapshots.csv")
async def export_snapshots_csv(scenario_id: str, db: str, request: Request):
    """Download inventory snapshots as CSV."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        df = conn.execute("""
            SELECT * FROM inventory_snapshot
            WHERE scenario_id = ?
            ORDER BY sim_date, dist_node_id, product_id
        """, [scenario_id]).fetchdf()

        if df.empty:
            raise HTTPException(404, f"No snapshots for scenario: {scenario_id}")

        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        return StreamingResponse(
            iter([buffer.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={scenario_id}_snapshots.csv"},
        )
    finally:
        conn.close()


@router.get("/export/database/{db_name}")
async def export_database(db_name: str, request: Request):
    """Download the DuckDB file."""
    db_path = _resolve_db(db_name, request)
    return FileResponse(
        db_path,
        media_type="application/octet-stream",
        filename=db_name,
    )


def _resolve_db(db_name: str, request: Request) -> str:
    db_path = request.app.state.data_dir / db_name
    if not db_path.exists():
        raise HTTPException(404, f"Database not found: {db_name}")
    return str(db_path)
