"""
Edge builders: generate edges from abstract relationships (zone tables, distance rules, etc.).

Each builder function takes a DuckDB connection and parameters describing which
node types and attributes to join, queries the relevant lookup tables and active
nodes, and inserts generated edges into the edge table.
"""

import logging

import duckdb

logger = logging.getLogger(__name__)

# Map node type names to (table_name, id_column)
_NODE_TYPE_TABLE = {
    'supply':       ('supply_node',       'supply_node_id'),
    'distribution': ('distribution_node', 'dist_node_id'),
    'demand':       ('demand_node',       'demand_node_id'),
}


def build_edges_from_zones(conn: duckdb.DuckDBPyConnection,
                           zone_table_name: str,
                           origin_type: str,
                           dest_type: str,
                           origin_node_attribute: str,
                           dest_node_attribute: str,
                           transport_type: str = "parcel",
                           transit_time_distribution: str = "lognormal",
                           cost_variable: float = 0.0,
                           cost_variable_basis: str = "per_unit"):
    """Generate edges by joining active nodes against a named zone table.

    For each (origin_node, dest_node) pair where both have attribute values
    present in the zone table, creates an edge with transit time and distance
    from the zone lookup.

    Edge IDs are deterministic: ZE_{origin_id}_{dest_id} so re-running is idempotent.
    """
    if origin_type not in _NODE_TYPE_TABLE:
        raise ValueError(f"Unknown origin_type '{origin_type}'. "
                         f"Must be one of: {list(_NODE_TYPE_TABLE.keys())}")
    if dest_type not in _NODE_TYPE_TABLE:
        raise ValueError(f"Unknown dest_type '{dest_type}'. "
                         f"Must be one of: {list(_NODE_TYPE_TABLE.keys())}")

    orig_table, orig_id_col = _NODE_TYPE_TABLE[origin_type]
    dest_table, dest_id_col = _NODE_TYPE_TABLE[dest_type]

    query = f"""
        SELECT
            orig.{orig_id_col},
            dest.{dest_id_col},
            zt.zone,
            zt.distance,
            zt.distance_uom,
            zt.transit_days_base
        FROM {orig_table} orig
        JOIN {dest_table} dest ON TRUE
        JOIN zone_table zt
            ON zt.zone_table_name = ?
            AND zt.origin_key = CAST(orig.{origin_node_attribute} AS VARCHAR)
            AND zt.dest_key = CAST(dest.{dest_node_attribute} AS VARCHAR)
        WHERE orig.{origin_node_attribute} IS NOT NULL
          AND dest.{dest_node_attribute} IS NOT NULL
    """

    rows = conn.execute(query, [zone_table_name]).fetchall()
    if not rows:
        logger.warning(f"No zone-based edges generated for '{zone_table_name}' "
                       f"({origin_type}.{origin_node_attribute} → "
                       f"{dest_type}.{dest_node_attribute})")
        return 0

    count = 0
    for (origin_id, dest_id, zone, distance, distance_uom, transit_days) in rows:

        edge_id = f"ZE_{origin_id}_{dest_id}"

        conn.execute("""
            INSERT OR REPLACE INTO edge VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            edge_id,
            origin_id, origin_type,
            dest_id, dest_type,
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

        # Store zone mapping for engine lookup
        if zone is not None:
            conn.execute("""
                INSERT OR REPLACE INTO edge_zone_map VALUES (?, ?, ?)
            """, [edge_id, zone_table_name, str(zone)])

        count += 1

    logger.info(f"Generated {count} edges from zone table '{zone_table_name}' "
                f"({origin_type}→{dest_type}, {transport_type})")
    return count
