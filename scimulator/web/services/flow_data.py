"""
Transform simulation event_log data into the flow_viz CSV format.

The flow_viz expects CSV with columns:
  origin_name, dest_name, origin_lat, origin_lng, dest_lat, dest_lng,
  ship_datetime, delivery_datetime, delivery_days, product, value, weight
"""

import io
import csv
from typing import Optional

import duckdb


def get_flow_data_csv(
    conn: duckdb.DuckDBPyConnection,
    scenario_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> str:
    """Generate flow_viz CSV from fulfillment events.

    Joins demand_fulfilled events with edges, distribution nodes, and products.
    """
    conditions = ["el.scenario_id = ?"]
    params = [scenario_id]

    if date_from:
        conditions.append("el.sim_date >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("el.sim_date <= ?")
        params.append(date_to)

    where = " AND ".join(conditions)

    rows = conn.execute(f"""
        SELECT
            dn.name as origin_name,
            dn.latitude as origin_lat,
            dn.longitude as origin_lng,
            dem.name as dest_name,
            dem.latitude as dest_lat,
            dem.longitude as dest_lng,
            el.sim_date as ship_date,
            COALESCE(e.mean_transit_time, 2) as transit_days,
            el.sim_date + CAST(COALESCE(e.mean_transit_time, 2) AS INTEGER) * INTERVAL '1' DAY as delivery_date,
            el.product_id,
            COALESCE(el.quantity * p.base_price, 0) as value,
            COALESCE(el.quantity * p.weight, 0) as weight
        FROM event_log el
        JOIN distribution_node dn ON el.node_id = dn.dist_node_id
        LEFT JOIN edge e ON el.edge_id = e.edge_id
        LEFT JOIN demand_node dem ON e.dest_node_id = dem.demand_node_id
        LEFT JOIN product p ON el.product_id = p.product_id
        WHERE {where}
          AND el.event_type IN ('demand_fulfilled', 'backorder_fulfilled')
          AND dn.latitude IS NOT NULL
        ORDER BY el.sim_date
    """, params).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'origin_name', 'dest_name',
        'origin_lat', 'origin_lng',
        'dest_lat', 'dest_lng',
        'ship_datetime', 'delivery_datetime', 'delivery_days',
        'product', 'value', 'weight',
    ])

    for row in rows:
        (origin_name, origin_lat, origin_lng, dest_name, dest_lat, dest_lng,
         ship_date, transit_days, delivery_date, product_id, value, weight) = row

        # Skip rows where we don't have destination coordinates
        if dest_lat is None or dest_lng is None:
            continue

        # Format dates as ISO datetimes
        ship_dt = f"{ship_date}T12:00:00" if ship_date else ""
        if hasattr(delivery_date, 'isoformat'):
            delivery_dt = delivery_date.isoformat()
        else:
            delivery_dt = str(delivery_date) if delivery_date else ""

        writer.writerow([
            origin_name or "DC",
            dest_name or "",
            float(origin_lat),
            float(origin_lng),
            float(dest_lat),
            float(dest_lng),
            ship_dt,
            delivery_dt,
            round(float(transit_days), 1),
            product_id or "",
            round(float(value), 2) if value else 0,
            round(float(weight), 2) if weight else 0,
        ])

    return output.getvalue()
