"""
Edge builders: generate edges from abstract relationships (zone tables, distance rules, etc.).

Each builder function takes a DuckDB connection and a ScenarioConfig, queries the
relevant lookup tables and active nodes, and inserts generated edges into the edge table.
"""

import logging
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)


def build_edges_from_zones(conn: duckdb.DuckDBPyConnection,
                           transport_type: str = "parcel",
                           transit_time_distribution: str = "lognormal",
                           cost_variable: float = 0.0,
                           cost_variable_basis: str = "per_unit"):
    """Generate distribution→demand edges by joining active nodes against the zone table.

    For each (distribution_node, demand_node) pair where both have zip3 values
    present in the zone table, creates an edge with transit time and distance
    from the zone lookup.

    Edge IDs are deterministic: ZE_{origin_id}_{dest_id} so re-running is idempotent.
    """
    # Find all distribution_node × demand_node pairs linked by the zone table
    query = """
        SELECT
            dn.dist_node_id,
            dn.zip3 AS origin_zip3,
            dem.demand_node_id,
            dem.zip3 AS dest_zip3,
            zt.zone,
            zt.distance_haversine,
            zt.distance_uom,
            zt.transit_days_base
        FROM distribution_node dn
        JOIN demand_node dem ON TRUE
        JOIN zone_table zt
            ON zt.origin_zip3 = dn.zip3
            AND zt.dest_zip3 = dem.zip3
        WHERE dn.zip3 IS NOT NULL
          AND dem.zip3 IS NOT NULL
    """

    rows = conn.execute(query).fetchall()
    if not rows:
        logger.warning("No zone-based edges generated (no matching zip3 pairs in zone_table)")
        return 0

    count = 0
    for (dist_node_id, origin_zip3, demand_node_id, dest_zip3,
         zone, distance, distance_uom, transit_days) in rows:

        edge_id = f"ZE_{dist_node_id}_{demand_node_id}"

        conn.execute("""
            INSERT OR REPLACE INTO edge VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            edge_id,
            dist_node_id, 'distribution',
            demand_node_id, 'demand',
            transport_type,
            float(transit_days) if transit_days else 2.0,
            'days',
            transit_time_distribution,
            None,  # transit_time_std
            None,  # transit_time_std_uom
            None,  # transit_time_skew
            0.0,   # cost_fixed
            cost_variable,
            cost_variable_basis,
            float(distance) if distance else None,
            distance_uom or 'km',
            'haversine',
        ])
        count += 1

    logger.info(f"Generated {count} edges from zone table "
                f"({transport_type}, {transit_time_distribution})")
    return count
