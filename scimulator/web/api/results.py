"""
Results and KPI query API routes.
"""

from typing import Optional, List
from fastapi import APIRouter, HTTPException, Request, Query
from fastapi.responses import PlainTextResponse

from ..services.db_manager import get_connection
from ..services.flow_data import get_flow_data_csv
from ..services.query import (
    get_run_metadata,
    get_event_summary,
    get_fulfillment_stats,
    get_fulfillment_by_node,
    get_fulfillment_by_product,
    get_fulfillment_by_days,
    get_fulfillment_csv,
    get_cost_summary,
    get_cost_detail,
    get_inventory_summary,
    get_inventory_timeseries,
    get_inventory_kpis,
    get_avg_inventory_by_node,
    get_avg_inventory_by_product,
    get_node_summary,
    get_transportation_summary,
    get_event_log_page,
    get_event_filter_options,
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


@router.get("/results/{scenario_id}/fulfillment")
async def get_fulfillment_detail(scenario_id: str, db: str, request: Request):
    """Fulfillment breakdown by node, product, and delivery speed."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        stats = get_fulfillment_stats(conn, scenario_id)
        by_node = get_fulfillment_by_node(conn, scenario_id)
        by_product = get_fulfillment_by_product(conn, scenario_id)
        by_days = get_fulfillment_by_days(conn, scenario_id)

        # Add total value_shipped to stats
        total_value = sum(n["value_shipped"] for n in by_node)
        stats["value_shipped"] = total_value

        return {
            "stats": stats,
            "by_days": by_days,
            "by_node": by_node,
            "by_product": by_product,
        }
    finally:
        conn.close()


@router.get("/results/{scenario_id}/fulfillment/csv")
async def export_fulfillment_csv(
    scenario_id: str,
    db: str,
    request: Request,
    view: str = Query("by_node", pattern="^(by_node|by_product)$"),
):
    """Download fulfillment data as CSV."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        csv_data = get_fulfillment_csv(conn, scenario_id, view=view)
        return PlainTextResponse(
            csv_data,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=fulfillment_{view}_{scenario_id}.csv"},
        )
    finally:
        conn.close()


@router.get("/results/{scenario_id}/inventory/kpis")
async def get_inventory_kpi_data(scenario_id: str, db: str, request: Request):
    """Inventory KPIs: avg inventory, MOS, turns, avg by node, and avg by product."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        return {
            "kpis": get_inventory_kpis(conn, scenario_id),
            "by_node": get_avg_inventory_by_node(conn, scenario_id),
            "by_product": get_avg_inventory_by_product(conn, scenario_id),
        }
    finally:
        conn.close()


@router.get("/results/{scenario_id}/nodes")
async def get_nodes_detail(scenario_id: str, db: str, request: Request):
    """Network node summary with simulation activity stats."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        return get_node_summary(conn, scenario_id)
    finally:
        conn.close()


@router.get("/results/{scenario_id}/transportation")
async def get_transportation_detail(scenario_id: str, db: str, request: Request):
    """Transportation edge utilization summary."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        return get_transportation_summary(conn, scenario_id)
    finally:
        conn.close()


@router.get("/results/{scenario_id}/costs")
async def get_costs_detail(scenario_id: str, db: str, request: Request):
    """Detailed cost breakdown by type, node, and product."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        return get_cost_detail(conn, scenario_id)
    finally:
        conn.close()


@router.get("/results/{scenario_id}/events/filters")
async def get_event_filters(scenario_id: str, db: str, request: Request):
    """Distinct values for event log filter dropdowns."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        return get_event_filter_options(conn, scenario_id)
    finally:
        conn.close()


@router.get("/results/{scenario_id}/events")
async def get_events(
    scenario_id: str,
    db: str,
    request: Request,
    event_type: List[str] = Query(None),
    product_id: List[str] = Query(None),
    origin_node_id: List[str] = Query(None),
    dest_node_id: List[str] = Query(None),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: str = Query("asc", pattern="^(asc|desc)$"),
    limit: int = Query(100, le=10000),
    offset: int = Query(0, ge=0),
):
    """Paginated, filterable event log."""
    db_path = _resolve_db(db, request)
    conn = get_connection(db_path, read_only=True)
    try:
        return get_event_log_page(
            conn, scenario_id,
            event_types=event_type or None,
            product_ids=product_id or None,
            origin_node_ids=origin_node_id or None,
            dest_node_ids=dest_node_id or None,
            date_from=date_from,
            date_to=date_to,
            sort_by=sort_by,
            sort_dir=sort_dir,
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
    metric: str = Query("units", pattern="^(units|value|parts)$"),
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
            metric=metric,
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
