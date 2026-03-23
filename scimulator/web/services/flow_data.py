"""
Transform simulation event_log data into the flow_viz CSV format.

The flow_viz expects CSV with columns:
  origin_name, origin_lat, origin_lng, dest_lat, dest_lng,
  ship_datetime, delivery_datetime, value, weight, cube, brand
"""

import io
import csv
import json
from typing import Optional, Dict, Tuple

import duckdb


# Approximate centroids for common US ZIP3 prefixes.
# Used as fallback when demand nodes lack lat/lon.
_ZIP3_CENTROIDS: Dict[str, Tuple[float, float]] = {
    "100": (40.75, -73.99),   # NYC
    "101": (40.82, -73.93),   # Bronx
    "200": (20.88, -156.68),  # Hawaii (200 prefix)
    "300": (33.75, -84.39),   # Atlanta
    "303": (33.80, -84.41),   # Atlanta suburbs
    "606": (41.88, -87.63),   # Chicago
    "750": (32.78, -96.80),   # Dallas
    "900": (34.05, -118.24),  # Los Angeles
    "941": (37.77, -122.42),  # San Francisco
    "981": (47.61, -122.33),  # Seattle
}


def _zip3_coords(demand_node_id: str) -> Tuple[Optional[float], Optional[float]]:
    """Look up approximate coordinates for a ZIP3-based demand node."""
    # Strip the 'Z' prefix if present
    zip3 = demand_node_id.lstrip('Z')
    if zip3 in _ZIP3_CENTROIDS:
        return _ZIP3_CENTROIDS[zip3]
    return None, None


def get_flow_data_csv(
    conn: duckdb.DuckDBPyConnection,
    scenario_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> str:
    """Generate flow_viz CSV from fulfillment events.

    Joins demand_fulfilled events with edges, distribution nodes, and products.
    Falls back to ZIP3 centroid lookup when demand nodes lack coordinates.
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
            dem.latitude as dest_lat,
            dem.longitude as dest_lng,
            dem.demand_node_id as demand_node_id,
            el.sim_date as ship_date,
            el.sim_date + CAST(COALESCE(e.mean_transit_time, 2) AS INTEGER) * INTERVAL '1' DAY as delivery_date,
            COALESCE(el.quantity * p.base_price, 0) as value,
            COALESCE(el.quantity * p.weight, 0) as weight,
            COALESCE(el.quantity * p.cube, 0) as cube,
            COALESCE(pa.attribute_value, p.name) as brand,
            el.detail
        FROM event_log el
        JOIN distribution_node dn ON el.node_id = dn.dist_node_id
        LEFT JOIN edge e ON el.edge_id = e.edge_id
        LEFT JOIN demand_node dem ON e.dest_node_id = dem.demand_node_id
        LEFT JOIN product p ON el.product_id = p.product_id
        LEFT JOIN product_attribute pa ON p.product_id = pa.product_id
            AND pa.attribute_key = 'brand'
        WHERE {where}
          AND el.event_type IN ('demand_fulfilled', 'backorder_fulfilled')
          AND dn.latitude IS NOT NULL
        ORDER BY el.sim_date
    """, params).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'origin_name', 'origin_lat', 'origin_lng',
        'dest_lat', 'dest_lng',
        'ship_datetime', 'delivery_datetime',
        'value', 'weight', 'cube', 'brand',
    ])

    for row in rows:
        (origin_name, origin_lat, origin_lng, dest_lat, dest_lng,
         demand_node_id, ship_date, delivery_date, value, weight, cube,
         brand, detail) = row

        # Fall back to ZIP3 centroid if demand node has no coordinates
        if dest_lat is None or dest_lng is None:
            # Try demand_node_id from the join
            node_id = demand_node_id
            # If not available, try the detail JSON
            if not node_id and detail:
                try:
                    d = json.loads(detail)
                    node_id = d.get('demand_node_id')
                except (json.JSONDecodeError, TypeError):
                    pass
            if node_id:
                dest_lat, dest_lng = _zip3_coords(node_id)

        # Skip rows where we still don't have destination coordinates
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
            float(origin_lat),
            float(origin_lng),
            float(dest_lat),
            float(dest_lng),
            ship_dt,
            delivery_dt,
            round(float(value), 2) if value else 0,
            round(float(weight), 2) if weight else 0,
            round(float(cube), 2) if cube else 0,
            brand or "",
        ])

    return output.getvalue()
