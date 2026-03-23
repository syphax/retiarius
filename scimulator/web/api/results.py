"""
Results and KPI query API routes.
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, Request, Query
from fastapi.responses import PlainTextResponse

from ..services.db_manager import get_connection
from ..services.flow_data import get_flow_data_csv
from ..services.query import (
    get_run_metadata,
    get_event_summary,
    get_fulfillment_stats,
    get_cost_summary,
    get_inventory_summary,
    get_inventory_timeseries,
    get_event_log_page,
)

router = APIRouter()


@router.get("/results/{scenario_id}/summary")
async def get_results_summary(scenario_id: str, db: str, request: Request):
    """Full results summary: metadata, events, fulfillment, costs, inventory."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        meta = get_run_metadata(conn, scenario_id)
        if not meta:
            raise HTTPException(404, f"No results for scenario: {scenario_id}")

        return {
            "metadata": meta,
            "events": get_event_summary(conn, scenario_id),
            "fulfillment": get_fulfillment_stats(conn, scenario_id),
            "costs": get_cost_summary(conn, scenario_id),
            "inventory": get_inventory_summary(conn, scenario_id),
        }
    finally:
        conn.close()


@router.get("/results/{scenario_id}/events")
async def get_events(
    scenario_id: str,
    db: str,
    request: Request,
    event_type: Optional[str] = None,
    product_id: Optional[str] = None,
    node_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(100, le=10000),
    offset: int = Query(0, ge=0),
):
    """Paginated, filterable event log."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        return get_event_log_page(
            conn, scenario_id,
            event_type=event_type,
            product_id=product_id,
            node_id=node_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )
    finally:
        conn.close()


@router.get("/results/{scenario_id}/inventory")
async def get_inventory_chart_data(
    scenario_id: str,
    db: str,
    request: Request,
    group_by: str = Query("node", pattern="^(node|product|state|total)$"),
    node_id: Optional[str] = None,
    product_id: Optional[str] = None,
):
    """Inventory time-series for charting."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        return get_inventory_timeseries(
            conn, scenario_id,
            group_by=group_by,
            node_id=node_id,
            product_id=product_id,
        )
    finally:
        conn.close()


@router.get("/results/{scenario_id}/flow-data")
async def get_flow_data(
    scenario_id: str,
    db: str,
    request: Request,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """Simulation results in flow_viz CSV format."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        csv_data = get_flow_data_csv(conn, scenario_id, date_from, date_to)
        return PlainTextResponse(csv_data, media_type="text/csv")
    finally:
        conn.close()


def _resolve_db(db_name: str, request: Request) -> str:
    db_path = request.app.state.data_dir / db_name
    if not db_path.exists():
        raise HTTPException(404, f"Database not found: {db_name}")
    return str(db_path)
