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


def get_inventory_kpis(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> Optional[Dict]:
    """Average inventory KPIs: months of supply, turns.

    MOS = avg_inventory / (total_fulfilled / num_months)
    Turns = 12 / MOS
    """
    # Get scenario date range for num_months
    scenario = conn.execute(
        "SELECT start_date, end_date FROM scenario WHERE scenario_id = ?",
        [scenario_id],
    ).fetchone()
    if not scenario:
        return None

    start_date, end_date = scenario
    num_days = (end_date - start_date).days + 1
    num_months = num_days / 30.44  # average days per month

    # Average daily saleable inventory across all snapshots
    avg_inv = conn.execute("""
        SELECT AVG(daily_total) FROM (
            SELECT sim_date, SUM(quantity) as daily_total
            FROM inventory_snapshot
            WHERE scenario_id = ? AND inventory_state = 'saleable'
            GROUP BY sim_date
        )
    """, [scenario_id]).fetchone()[0]

    # Total fulfilled units
    fulfilled = conn.execute("""
        SELECT SUM(quantity) FROM event_log
        WHERE scenario_id = ? AND event_type IN ('demand_fulfilled', 'backorder_fulfilled')
    """, [scenario_id]).fetchone()[0]

    avg_inv = float(avg_inv) if avg_inv else 0
    fulfilled = float(fulfilled) if fulfilled else 0
    monthly_sales = fulfilled / num_months if num_months > 0 else 0

    mos = avg_inv / monthly_sales if monthly_sales > 0 else 0
    turns = 12 / mos if mos > 0 else 0

    return {
        "avg_inventory_units": round(avg_inv, 0),
        "total_fulfilled_units": fulfilled,
        "months_of_supply": round(mos, 2),
        "inventory_turns": round(turns, 2),
        "num_months": round(num_months, 1),
    }


def get_avg_inventory_by_node(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> List[Dict]:
    """Average inventory by distribution node: parts in stock, units, $ at standard_cost."""
    rows = conn.execute("""
        SELECT s.dist_node_id,
               AVG(s.parts_in_stock) as avg_parts,
               AVG(s.total_units) as avg_units,
               AVG(s.total_value) as avg_value
        FROM (
            SELECT inv.dist_node_id, inv.sim_date,
                   COUNT(DISTINCT inv.product_id) as parts_in_stock,
                   SUM(inv.quantity) as total_units,
                   SUM(inv.quantity * COALESCE(p.standard_cost, 0)) as total_value
            FROM inventory_snapshot inv
            LEFT JOIN product p ON inv.product_id = p.product_id
            WHERE inv.scenario_id = ?
              AND inv.inventory_state = 'saleable'
              AND inv.quantity > 0
            GROUP BY inv.dist_node_id, inv.sim_date
        ) s
        GROUP BY s.dist_node_id
        ORDER BY avg_units DESC
    """, [scenario_id]).fetchall()

    return [
        {
            "dist_node_id": r[0],
            "avg_parts_in_stock": round(float(r[1]), 0) if r[1] else 0,
            "avg_units_in_stock": round(float(r[2]), 0) if r[2] else 0,
            "avg_value_in_stock": round(float(r[3]), 2) if r[3] else 0,
        }
        for r in rows
    ]


def get_fulfillment_by_node(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> List[Dict]:
    """Fulfillment volume, cost, and value shipped by distribution node."""
    rows = conn.execute("""
        SELECT el.node_id as dist_node_id,
               COUNT(*) as fulfilled_events,
               SUM(el.quantity) as fulfilled_units,
               SUM(el.cost) as fulfillment_cost,
               SUM(el.quantity * COALESCE(p.base_price, 0)) as value_shipped
        FROM event_log el
        LEFT JOIN product p ON el.product_id = p.product_id
        WHERE el.scenario_id = ?
          AND el.event_type IN ('demand_fulfilled', 'backorder_fulfilled')
        GROUP BY el.node_id
        ORDER BY fulfilled_units DESC
    """, [scenario_id]).fetchall()

    return [
        {
            "dist_node_id": r[0],
            "fulfilled_events": r[1],
            "fulfilled_units": float(r[2]) if r[2] else 0,
            "fulfillment_cost": float(r[3]) if r[3] else 0,
            "value_shipped": float(r[4]) if r[4] else 0,
        }
        for r in rows
    ]


def get_fulfillment_by_product(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> List[Dict]:
    """Demand, fulfillment, and lost sale breakdown by product with value shipped."""
    rows = conn.execute("""
        SELECT el.product_id,
               SUM(CASE WHEN el.event_type = 'demand_received' THEN el.quantity ELSE 0 END) as demand_units,
               SUM(CASE WHEN el.event_type IN ('demand_fulfilled', 'backorder_fulfilled') THEN el.quantity ELSE 0 END) as fulfilled_units,
               SUM(CASE WHEN el.event_type = 'lost_sale' THEN el.quantity ELSE 0 END) as lost_units,
               SUM(CASE WHEN el.event_type = 'backorder_created' THEN el.quantity ELSE 0 END) as backorder_units,
               SUM(CASE WHEN el.event_type IN ('demand_fulfilled', 'backorder_fulfilled')
                   THEN el.quantity * COALESCE(p.base_price, 0) ELSE 0 END) as value_shipped
        FROM event_log el
        LEFT JOIN product p ON el.product_id = p.product_id
        WHERE el.scenario_id = ?
          AND el.event_type IN ('demand_received', 'demand_fulfilled', 'backorder_fulfilled', 'lost_sale', 'backorder_created')
        GROUP BY el.product_id
        ORDER BY demand_units DESC
    """, [scenario_id]).fetchall()

    return [
        {
            "product_id": r[0],
            "demand_units": float(r[1]),
            "fulfilled_units": float(r[2]),
            "lost_units": float(r[3]),
            "backorder_units": float(r[4]),
            "value_shipped": float(r[5]),
            "fill_rate_pct": round(float(r[2]) / float(r[1]) * 100, 1) if r[1] else 0,
        }
        for r in rows
    ]


def get_fulfillment_by_days(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> Dict:
    """Fulfillment breakdown by delivery speed (duration days).

    Groups fulfilled events by duration into buckets:
    0 (same day), 1, 2, 3, 4, 5+. Returns qty, value, and avg/median days.
    Duration is set by the engine (currently = edge mean_transit_time,
    future: O2S + S2D).
    """
    rows = conn.execute("""
        SELECT COALESCE(el.duration, 0) as delivery_days,
               SUM(el.quantity) as qty,
               SUM(el.quantity * COALESCE(p.base_price, 0)) as value
        FROM event_log el
        LEFT JOIN product p ON el.product_id = p.product_id
        WHERE el.scenario_id = ?
          AND el.event_type IN ('demand_fulfilled', 'backorder_fulfilled')
        GROUP BY delivery_days
        ORDER BY delivery_days
    """, [scenario_id]).fetchall()

    # Build day buckets: 0, 1, 2, 3, 4, 5+
    buckets = {0: {"qty": 0, "value": 0}, 1: {"qty": 0, "value": 0},
               2: {"qty": 0, "value": 0}, 3: {"qty": 0, "value": 0},
               4: {"qty": 0, "value": 0}, 5: {"qty": 0, "value": 0}}

    # For median/average calculation
    all_days = []

    for transit_days, qty, value in rows:
        td = float(transit_days)
        q = float(qty) if qty else 0
        v = float(value) if value else 0
        bucket_key = int(td) if td < 5 else 5
        buckets[bucket_key]["qty"] += q
        buckets[bucket_key]["value"] += v
        # Expand for median calc (approximate: use integer days * qty)
        all_days.extend([td] * int(q))

    # Calculate median and average
    avg_days = 0.0
    median_days = 0.0
    if all_days:
        avg_days = round(sum(all_days) / len(all_days), 1)
        sorted_days = sorted(all_days)
        mid = len(sorted_days) // 2
        if len(sorted_days) % 2 == 0:
            median_days = round((sorted_days[mid - 1] + sorted_days[mid]) / 2, 1)
        else:
            median_days = round(sorted_days[mid], 1)

    return {
        "buckets": [
            {"day": k, "label": f"{k}" if k < 5 else "5+",
             "qty": buckets[k]["qty"], "value": buckets[k]["value"]}
            for k in sorted(buckets.keys())
        ],
        "avg_days": avg_days,
        "median_days": median_days,
    }


def get_fulfillment_csv(conn: duckdb.DuckDBPyConnection, scenario_id: str, view: str = "by_node") -> str:
    """Export fulfillment data as CSV string.

    view: 'by_node' or 'by_product'
    """
    import io
    import csv

    output = io.StringIO()
    writer = csv.writer(output)

    if view == "by_product":
        writer.writerow(["product_id", "demand_units", "fulfilled_units", "lost_units",
                         "backorder_units", "value_shipped", "fill_rate_pct"])
        for row in get_fulfillment_by_product(conn, scenario_id):
            writer.writerow([
                row["product_id"], row["demand_units"], row["fulfilled_units"],
                row["lost_units"], row["backorder_units"], row["value_shipped"],
                row["fill_rate_pct"],
            ])
    else:
        writer.writerow(["dist_node_id", "fulfilled_events", "fulfilled_units",
                         "fulfillment_cost", "value_shipped"])
        for row in get_fulfillment_by_node(conn, scenario_id):
            writer.writerow([
                row["dist_node_id"], row["fulfilled_events"], row["fulfilled_units"],
                row["fulfillment_cost"], row["value_shipped"],
            ])

    return output.getvalue()


def get_node_summary(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> Dict:
    """Summary of all network nodes with activity stats from the simulation."""
    # Distribution nodes with config + activity
    dist_nodes = conn.execute("""
        SELECT dn.dist_node_id, dn.name, dn.latitude, dn.longitude,
               dn.storage_capacity, dn.storage_capacity_uom,
               dn.fixed_cost, dn.fixed_cost_basis,
               dn.variable_cost, dn.variable_cost_basis
        FROM distribution_node dn
    """).fetchall()

    # Aggregate event stats per distribution node
    dist_stats = {}
    for r in conn.execute("""
        SELECT el.node_id,
               SUM(CASE WHEN el.event_type IN ('demand_fulfilled', 'backorder_fulfilled') THEN el.quantity ELSE 0 END) as fulfilled_units,
               SUM(CASE WHEN el.event_type IN ('demand_fulfilled', 'backorder_fulfilled') THEN el.cost ELSE 0 END) as fulfillment_cost,
               SUM(CASE WHEN el.event_type = 'fixed_cost' THEN el.cost ELSE 0 END) as fixed_cost_total,
               SUM(CASE WHEN el.event_type = 'capacity_overage' THEN el.cost ELSE 0 END) as overage_cost
        FROM event_log el
        WHERE el.scenario_id = ?
          AND el.node_id IS NOT NULL
        GROUP BY el.node_id
    """, [scenario_id]).fetchall():
        dist_stats[r[0]] = {
            "fulfilled_units": float(r[1]) if r[1] else 0,
            "fulfillment_cost": float(r[2]) if r[2] else 0,
            "fixed_cost_total": float(r[3]) if r[3] else 0,
            "overage_cost": float(r[4]) if r[4] else 0,
        }

    # Final inventory per node (from latest snapshot)
    inv_by_node = {}
    last_date = conn.execute(
        "SELECT MAX(sim_date) FROM inventory_snapshot WHERE scenario_id = ?",
        [scenario_id],
    ).fetchone()[0]
    if last_date:
        for r in conn.execute("""
            SELECT dist_node_id, SUM(quantity) as total_qty
            FROM inventory_snapshot
            WHERE scenario_id = ? AND sim_date = ? AND inventory_state = 'saleable'
            GROUP BY dist_node_id
        """, [scenario_id, last_date]).fetchall():
            inv_by_node[r[0]] = float(r[1])

    distribution = []
    for r in dist_nodes:
        node_id = r[0]
        stats = dist_stats.get(node_id, {})
        distribution.append({
            "node_id": node_id,
            "name": r[1],
            "latitude": float(r[2]) if r[2] else None,
            "longitude": float(r[3]) if r[3] else None,
            "storage_capacity": float(r[4]) if r[4] else None,
            "storage_capacity_uom": r[5],
            "fixed_cost_rate": float(r[6]) if r[6] else None,
            "fixed_cost_basis": r[7],
            "variable_cost_rate": float(r[8]) if r[8] else None,
            "variable_cost_basis": r[9],
            "fulfilled_units": stats.get("fulfilled_units", 0),
            "fulfillment_cost": stats.get("fulfillment_cost", 0),
            "fixed_cost_total": stats.get("fixed_cost_total", 0),
            "overage_cost": stats.get("overage_cost", 0),
            "final_inventory": inv_by_node.get(node_id, 0),
        })

    # Supply nodes
    supply_nodes = conn.execute("""
        SELECT sn.supply_node_id, sn.name, sn.latitude, sn.longitude,
               s.supplier_id, s.name as supplier_name,
               sn.lead_time_days
        FROM supply_node sn
        JOIN supplier s ON sn.supplier_id = s.supplier_id
    """).fetchall()

    supply = [
        {
            "node_id": r[0],
            "name": r[1],
            "latitude": float(r[2]) if r[2] else None,
            "longitude": float(r[3]) if r[3] else None,
            "supplier_id": r[4],
            "supplier_name": r[5],
            "lead_time_days": float(r[6]) if r[6] else None,
        }
        for r in supply_nodes
    ]

    # Demand nodes with volume
    demand_stats = {}
    for r in conn.execute("""
        SELECT el.node_id, SUM(el.quantity) as demand_units
        FROM event_log el
        WHERE el.scenario_id = ? AND el.event_type = 'demand_received'
        GROUP BY el.node_id
    """, [scenario_id]).fetchall():
        demand_stats[r[0]] = float(r[1])

    # Note: demand events use dest_node from edge, but demand_received uses node_id
    # as the demand node itself. Let's also check via edge join.
    demand_nodes = conn.execute("""
        SELECT demand_node_id, name, latitude, longitude
        FROM demand_node
    """).fetchall()

    demand = [
        {
            "node_id": r[0],
            "name": r[1],
            "latitude": float(r[2]) if r[2] else None,
            "longitude": float(r[3]) if r[3] else None,
            "demand_units": demand_stats.get(r[0], 0),
        }
        for r in demand_nodes
    ]

    return {
        "distribution": distribution,
        "supply": supply,
        "demand": demand,
    }


def get_transportation_summary(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> List[Dict]:
    """Edge utilization: shipment counts, volumes, and costs per edge."""
    rows = conn.execute("""
        SELECT e.edge_id, e.origin_node_id, e.origin_node_type,
               e.dest_node_id, e.dest_node_type, e.transport_type,
               e.mean_transit_time, e.distance, e.distance_uom,
               COUNT(el.event_id) as shipments,
               SUM(el.quantity) as total_qty,
               SUM(el.cost) as total_cost
        FROM edge e
        LEFT JOIN event_log el
          ON e.edge_id = el.edge_id
          AND el.scenario_id = ?
          AND el.event_type IN ('demand_fulfilled', 'backorder_fulfilled')
        GROUP BY e.edge_id, e.origin_node_id, e.origin_node_type,
                 e.dest_node_id, e.dest_node_type, e.transport_type,
                 e.mean_transit_time, e.distance, e.distance_uom
        ORDER BY total_qty DESC NULLS LAST
    """, [scenario_id]).fetchall()

    return [
        {
            "edge_id": r[0],
            "origin_node_id": r[1],
            "origin_node_type": r[2],
            "dest_node_id": r[3],
            "dest_node_type": r[4],
            "transport_type": r[5],
            "mean_transit_time": float(r[6]) if r[6] else None,
            "distance": float(r[7]) if r[7] else None,
            "distance_uom": r[8],
            "shipments": r[9],
            "total_qty": float(r[10]) if r[10] else 0,
            "total_cost": float(r[11]) if r[11] else 0,
        }
        for r in rows
    ]


def get_cost_detail(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> Dict:
    """Detailed cost breakdown: by type, by node, and by product."""
    # By event type (already have this, but include for completeness)
    by_type = conn.execute("""
        SELECT event_type, SUM(cost) as total
        FROM event_log
        WHERE scenario_id = ? AND cost IS NOT NULL AND cost > 0
        GROUP BY event_type
        ORDER BY total DESC
    """, [scenario_id]).fetchall()

    # By node
    by_node = conn.execute("""
        SELECT node_id, SUM(cost) as total,
               SUM(CASE WHEN event_type = 'fixed_cost' THEN cost ELSE 0 END) as fixed,
               SUM(CASE WHEN event_type IN ('demand_fulfilled', 'backorder_fulfilled') THEN cost ELSE 0 END) as fulfillment,
               SUM(CASE WHEN event_type = 'capacity_overage' THEN cost ELSE 0 END) as overage
        FROM event_log
        WHERE scenario_id = ? AND cost IS NOT NULL AND cost > 0
        GROUP BY node_id
        ORDER BY total DESC
    """, [scenario_id]).fetchall()

    # By product
    by_product = conn.execute("""
        SELECT product_id, SUM(cost) as total
        FROM event_log
        WHERE scenario_id = ? AND cost IS NOT NULL AND cost > 0 AND product_id IS NOT NULL
        GROUP BY product_id
        ORDER BY total DESC
    """, [scenario_id]).fetchall()

    total = conn.execute("""
        SELECT SUM(cost) FROM event_log
        WHERE scenario_id = ? AND cost IS NOT NULL
    """, [scenario_id]).fetchone()[0]

    return {
        "total_cost": float(total) if total else 0,
        "by_event_type": [
            {"event_type": r[0], "cost": float(r[1])}
            for r in by_type
        ],
        "by_node": [
            {
                "node_id": r[0],
                "total_cost": float(r[1]),
                "fixed_cost": float(r[2]),
                "fulfillment_cost": float(r[3]),
                "overage_cost": float(r[4]),
            }
            for r in by_node
        ],
        "by_product": [
            {"product_id": r[0], "cost": float(r[1])}
            for r in by_product
        ],
    }


_VALID_SORT_COLS = {
    'sim_date': 'el.sim_date',
    'event_type': 'el.event_type',
    'node_id': 'el.node_id',
    'origin_node_id': 'e.origin_node_id',
    'dest_node_id': 'e.dest_node_id',
    'product_id': 'el.product_id',
    'quantity': 'el.quantity',
    'cost': 'el.cost',
    'sim_step': 'el.sim_step',
}


def get_event_filter_options(
    conn: duckdb.DuckDBPyConnection,
    scenario_id: str,
) -> Dict:
    """Distinct values for event log filter dropdowns."""
    event_types = [r[0] for r in conn.execute(
        "SELECT DISTINCT event_type FROM event_log WHERE scenario_id = ? ORDER BY event_type",
        [scenario_id],
    ).fetchall()]

    products = [r[0] for r in conn.execute(
        "SELECT DISTINCT product_id FROM event_log WHERE scenario_id = ? AND product_id IS NOT NULL ORDER BY product_id",
        [scenario_id],
    ).fetchall()]

    # Origin and dest nodes come from the edge table join
    origin_nodes = [r[0] for r in conn.execute("""
        SELECT DISTINCT e.origin_node_id
        FROM event_log el
        JOIN edge e ON el.edge_id = e.edge_id
        WHERE el.scenario_id = ? AND e.origin_node_id IS NOT NULL
        ORDER BY e.origin_node_id
    """, [scenario_id]).fetchall()]

    dest_nodes = [r[0] for r in conn.execute("""
        SELECT DISTINCT e.dest_node_id
        FROM event_log el
        JOIN edge e ON el.edge_id = e.edge_id
        WHERE el.scenario_id = ? AND e.dest_node_id IS NOT NULL
        ORDER BY e.dest_node_id
    """, [scenario_id]).fetchall()]

    return {
        "event_types": event_types,
        "products": products,
        "origin_nodes": origin_nodes,
        "dest_nodes": dest_nodes,
    }


def get_event_log_page(
    conn: duckdb.DuckDBPyConnection,
    scenario_id: str,
    event_types: Optional[List[str]] = None,
    product_ids: Optional[List[str]] = None,
    origin_node_ids: Optional[List[str]] = None,
    dest_node_ids: Optional[List[str]] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: str = "asc",
    limit: int = 100,
    offset: int = 0,
) -> Dict:
    """Paginated, filterable event log."""
    conditions = ["el.scenario_id = ?"]
    params: List[Any] = [scenario_id]

    if event_types:
        placeholders = ", ".join(["?"] * len(event_types))
        conditions.append(f"el.event_type IN ({placeholders})")
        params.extend(event_types)
    if product_ids:
        placeholders = ", ".join(["?"] * len(product_ids))
        conditions.append(f"el.product_id IN ({placeholders})")
        params.extend(product_ids)
    if origin_node_ids:
        placeholders = ", ".join(["?"] * len(origin_node_ids))
        conditions.append(f"e.origin_node_id IN ({placeholders})")
        params.extend(origin_node_ids)
    if dest_node_ids:
        placeholders = ", ".join(["?"] * len(dest_node_ids))
        conditions.append(f"e.dest_node_id IN ({placeholders})")
        params.extend(dest_node_ids)
    if date_from:
        conditions.append("el.sim_date >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("el.sim_date <= ?")
        params.append(date_to)

    where = " AND ".join(conditions)

    # Get total count
    count_conditions = [c for c in conditions]
    total = conn.execute(f"""
        SELECT COUNT(*) FROM event_log el
        LEFT JOIN edge e ON el.edge_id = e.edge_id
        WHERE {where}
    """, params).fetchone()[0]

    # Get page — join edge table to resolve origin/destination nodes
    rows = conn.execute(f"""
        SELECT el.*,
               e.origin_node_id, e.origin_node_type,
               e.dest_node_id, e.dest_node_type
        FROM event_log el
        LEFT JOIN edge e ON el.edge_id = e.edge_id
        WHERE {where}
        ORDER BY {_VALID_SORT_COLS.get(sort_by, 'el.sim_step')} {sort_dir.upper()}, el.sim_step ASC
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
