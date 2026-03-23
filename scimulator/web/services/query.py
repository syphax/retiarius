"""
DuckDB query helpers for results and KPIs.

Extracts and extends the query logic from cli.py into reusable functions
that return JSON-serializable dictionaries.
"""

from typing import Optional, Dict, List, Any

import duckdb


def get_run_metadata(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> Optional[Dict]:
    """Get run metadata for a scenario."""
    row = conn.execute(
        "SELECT * FROM run_metadata WHERE scenario_id = ?", [scenario_id]
    ).fetchone()
    if not row:
        return None
    cols = [d[0] for d in conn.description]
    meta = dict(zip(cols, row))
    # Serialize timestamps
    for k in ('run_started_at', 'run_completed_at'):
        if meta.get(k) and hasattr(meta[k], 'isoformat'):
            meta[k] = meta[k].isoformat()
    if meta.get('wall_clock_seconds') is not None:
        meta['wall_clock_seconds'] = float(meta['wall_clock_seconds'])
    return meta


def get_event_summary(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> List[Dict]:
    """Event counts by type, with totals."""
    rows = conn.execute("""
        SELECT event_type, COUNT(*) as count,
               SUM(quantity) as total_qty,
               SUM(cost) as total_cost
        FROM event_log
        WHERE scenario_id = ?
        GROUP BY event_type
        ORDER BY count DESC
    """, [scenario_id]).fetchall()

    return [
        {
            "event_type": r[0],
            "count": r[1],
            "total_qty": float(r[2]) if r[2] else None,
            "total_cost": float(r[3]) if r[3] else None,
        }
        for r in rows
    ]


def get_fulfillment_stats(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> Dict:
    """Fulfillment rate and demand statistics."""
    demand = conn.execute("""
        SELECT COUNT(*), SUM(quantity) FROM event_log
        WHERE scenario_id = ? AND event_type = 'demand_received'
    """, [scenario_id]).fetchone()

    fulfilled = conn.execute("""
        SELECT COUNT(*), SUM(quantity) FROM event_log
        WHERE scenario_id = ? AND event_type IN ('demand_fulfilled', 'backorder_fulfilled')
    """, [scenario_id]).fetchone()

    lost = conn.execute("""
        SELECT COUNT(*), SUM(quantity) FROM event_log
        WHERE scenario_id = ? AND event_type = 'lost_sale'
    """, [scenario_id]).fetchone()

    backordered = conn.execute("""
        SELECT COUNT(*), SUM(quantity) FROM event_log
        WHERE scenario_id = ? AND event_type = 'backorder_created'
    """, [scenario_id]).fetchone()

    demand_qty = float(demand[1]) if demand[1] else 0
    fulfilled_qty = float(fulfilled[1]) if fulfilled[1] else 0
    fill_rate = (fulfilled_qty / demand_qty * 100) if demand_qty > 0 else 0

    return {
        "demand_events": demand[0],
        "demand_units": demand_qty,
        "fulfilled_events": fulfilled[0],
        "fulfilled_units": fulfilled_qty,
        "fill_rate_pct": round(fill_rate, 1),
        "lost_sale_events": lost[0],
        "lost_sale_units": float(lost[1]) if lost[1] else 0,
        "backorder_events": backordered[0],
        "backorder_units": float(backordered[1]) if backordered[1] else 0,
    }


def get_cost_summary(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> Dict:
    """Cost breakdown by event type."""
    total = conn.execute("""
        SELECT SUM(cost) FROM event_log
        WHERE scenario_id = ? AND cost IS NOT NULL
    """, [scenario_id]).fetchone()[0]

    by_type = conn.execute("""
        SELECT event_type, SUM(cost) as total
        FROM event_log
        WHERE scenario_id = ? AND cost IS NOT NULL AND cost > 0
        GROUP BY event_type
        ORDER BY total DESC
    """, [scenario_id]).fetchall()

    return {
        "total_cost": float(total) if total else 0,
        "by_event_type": [
            {"event_type": r[0], "cost": float(r[1])}
            for r in by_type
        ],
    }


def get_inventory_summary(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> Optional[Dict]:
    """Final inventory state from the last snapshot."""
    last_date = conn.execute("""
        SELECT MAX(sim_date) FROM inventory_snapshot WHERE scenario_id = ?
    """, [scenario_id]).fetchone()[0]

    if not last_date:
        return None

    rows = conn.execute("""
        SELECT inventory_state, SUM(quantity) as total_qty,
               COUNT(DISTINCT dist_node_id) as nodes,
               COUNT(DISTINCT product_id) as products
        FROM inventory_snapshot
        WHERE scenario_id = ? AND sim_date = ?
        GROUP BY inventory_state
        ORDER BY total_qty DESC
    """, [scenario_id, last_date]).fetchall()

    return {
        "snapshot_date": str(last_date),
        "states": [
            {
                "state": r[0],
                "quantity": float(r[1]),
                "nodes": r[2],
                "products": r[3],
            }
            for r in rows
        ],
    }


def get_inventory_timeseries(
    conn: duckdb.DuckDBPyConnection,
    scenario_id: str,
    group_by: str = "node",
    node_id: Optional[str] = None,
    product_id: Optional[str] = None,
) -> Dict:
    """Inventory quantities over time, grouped for charting."""
    # Build WHERE clause
    conditions = ["scenario_id = ?", "inventory_state = 'saleable'"]
    params: List[Any] = [scenario_id]

    if node_id:
        conditions.append("dist_node_id = ?")
        params.append(node_id)
    if product_id:
        conditions.append("product_id = ?")
        params.append(product_id)

    where = " AND ".join(conditions)

    # Determine GROUP BY column
    group_col_map = {
        "node": "dist_node_id",
        "product": "product_id",
        "state": "inventory_state",
        "total": "'total'",
    }
    group_col = group_col_map.get(group_by, "dist_node_id")

    rows = conn.execute(f"""
        SELECT sim_date, {group_col} as series, SUM(quantity) as qty
        FROM inventory_snapshot
        WHERE {where}
        GROUP BY sim_date, {group_col}
        ORDER BY sim_date, {group_col}
    """, params).fetchall()

    # Pivot into {dates: [...], series: {name: [values]}}
    dates = sorted(set(str(r[0]) for r in rows))
    series_names = sorted(set(r[1] for r in rows))

    # Build lookup
    lookup = {}
    for r in rows:
        lookup[(str(r[0]), r[1])] = float(r[2])

    series = {}
    for name in series_names:
        series[name] = [lookup.get((d, name), 0) for d in dates]

    return {
        "dates": dates,
        "series": series,
        "group_by": group_by,
    }


def get_event_log_page(
    conn: duckdb.DuckDBPyConnection,
    scenario_id: str,
    event_type: Optional[str] = None,
    product_id: Optional[str] = None,
    node_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> Dict:
    """Paginated, filterable event log."""
    conditions = ["scenario_id = ?"]
    params: List[Any] = [scenario_id]

    if event_type:
        conditions.append("event_type = ?")
        params.append(event_type)
    if product_id:
        conditions.append("product_id = ?")
        params.append(product_id)
    if node_id:
        conditions.append("node_id = ?")
        params.append(node_id)
    if date_from:
        conditions.append("sim_date >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("sim_date <= ?")
        params.append(date_to)

    where = " AND ".join(conditions)

    # Get total count
    total = conn.execute(
        f"SELECT COUNT(*) FROM event_log WHERE {where}", params
    ).fetchone()[0]

    # Get page
    rows = conn.execute(f"""
        SELECT * FROM event_log
        WHERE {where}
        ORDER BY sim_step, event_type
        LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchall()

    cols = [d[0] for d in conn.description]
    events = []
    for row in rows:
        event = dict(zip(cols, row))
        for k, v in event.items():
            if hasattr(v, 'isoformat'):
                event[k] = v.isoformat()
            elif isinstance(v, (float, int, str, bool, type(None))):
                pass
            else:
                event[k] = str(v)
        events.append(event)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "events": events,
    }
