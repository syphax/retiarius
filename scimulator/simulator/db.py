"""
DuckDB schema creation and data loading for the Distribution SCimulator.

Implements the full data schema from scimulator-data-schema.md v0.2.
"""

import duckdb
from pathlib import Path
from typing import Optional


def create_database(db_path: str) -> duckdb.DuckDBPyConnection:
    """Create a new DuckDB database with the full SCimulator schema."""
    conn = duckdb.connect(db_path)
    _create_schema(conn)
    _seed_uom(conn)
    return conn


def open_database(db_path: str, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open an existing DuckDB database, applying any schema migrations."""
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    conn = duckdb.connect(db_path, read_only=read_only)
    if not read_only:
        _migrate(conn)
    return conn


def _migrate(conn: duckdb.DuckDBPyConnection):
    """Apply schema migrations to existing databases. Idempotent."""
    # v0.3.1: add duration column to event_log
    cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'event_log'"
    ).fetchall()}
    if 'event_log' and 'duration' not in cols and cols:
        conn.execute("ALTER TABLE event_log ADD COLUMN duration DECIMAL(10,2)")

    # v0.3.2: per-table version IDs on scenario
    scen_cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'scenario'"
    ).fetchall()}
    if scen_cols:
        for col in ('demand_version_id', 'inbound_version_id', 'inventory_version_id'):
            if col not in scen_cols:
                conn.execute(f"ALTER TABLE scenario ADD COLUMN {col} TEXT")


def _create_schema(conn: duckdb.DuckDBPyConnection):
    """Create all tables in the SCimulator schema."""

    # --- Reference Tables ---

    conn.execute("""
        CREATE TABLE IF NOT EXISTS uom (
            uom_code TEXT PRIMARY KEY,
            uom_name TEXT NOT NULL,
            dimension TEXT NOT NULL,
            is_metric_default BOOLEAN NOT NULL DEFAULT FALSE,
            conversion_to_default DECIMAL(18,10) NOT NULL
        )
    """)

    # --- Configuration Tables ---

    conn.execute("""
        CREATE TABLE IF NOT EXISTS scenario (
            scenario_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            dataset_version_id TEXT,
            demand_version_id TEXT,
            inbound_version_id TEXT,
            inventory_version_id TEXT,
            currency_code TEXT NOT NULL DEFAULT 'USD',
            time_resolution TEXT NOT NULL DEFAULT 'daily',
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            warm_up_days INTEGER NOT NULL DEFAULT 0,
            backorder_probability DECIMAL(5,4) NOT NULL DEFAULT 1.0,
            write_event_log BOOLEAN NOT NULL DEFAULT TRUE,
            write_snapshots BOOLEAN NOT NULL DEFAULT TRUE,
            snapshot_interval_days INTEGER NOT NULL DEFAULT 1,
            product_set_id TEXT,
            supply_node_set_id TEXT,
            distribution_node_set_id TEXT,
            demand_node_set_id TEXT,
            edge_set_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS scenario_param (
            scenario_id TEXT NOT NULL,
            param_key TEXT NOT NULL,
            param_value TEXT NOT NULL,
            PRIMARY KEY (scenario_id, param_key)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS dataset_version (
            dataset_version_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            parent_version_id TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT
        )
    """)

    # --- Network Topology Tables ---

    conn.execute("""
        CREATE TABLE IF NOT EXISTS supplier (
            supplier_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            default_lead_time DECIMAL(6,2),
            default_lead_time_uom TEXT DEFAULT 'days',
            default_qty_reliability DECIMAL(5,4) DEFAULT 1.0,
            default_timing_variance DECIMAL(6,2) DEFAULT 0.0,
            default_timing_variance_uom TEXT DEFAULT 'days',
            timing_variance_distribution TEXT DEFAULT 'normal',
            timing_variance_std DECIMAL(6,2),
            timing_variance_std_uom TEXT DEFAULT 'days'
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS supply_node (
            supply_node_id TEXT PRIMARY KEY,
            supplier_id TEXT NOT NULL,
            name TEXT NOT NULL,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            lead_time DECIMAL(6,2),
            lead_time_uom TEXT,
            qty_reliability DECIMAL(5,4),
            timing_variance DECIMAL(6,2),
            timing_variance_uom TEXT,
            timing_variance_distribution TEXT,
            timing_variance_std DECIMAL(6,2),
            timing_variance_std_uom TEXT,
            max_capacity DECIMAL(12,2),
            max_capacity_uom TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS supply_node_tag (
            supply_node_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            PRIMARY KEY (supply_node_id, tag)
        )
    """)

    conn.execute("""
        -- No rows = supply node can supply all products
        CREATE TABLE IF NOT EXISTS supply_node_product (
            supply_node_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            unit_cost DECIMAL(12,4),
            max_units_per_day DECIMAL(12,2),
            PRIMARY KEY (supply_node_id, product_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS distribution_node (
            dist_node_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            zip3 TEXT,
            storage_capacity DECIMAL(14,2),
            storage_capacity_uom TEXT DEFAULT 'm3',
            max_inbound DECIMAL(12,2),
            max_inbound_uom TEXT,
            max_outbound DECIMAL(12,2),
            max_outbound_uom TEXT,
            order_response_time DECIMAL(6,2) NOT NULL DEFAULT 1.0,
            order_response_time_uom TEXT DEFAULT 'days',
            fixed_cost DECIMAL(12,4) DEFAULT 0,
            fixed_cost_basis TEXT NOT NULL DEFAULT 'per_day',
            variable_cost DECIMAL(12,4) DEFAULT 0,
            variable_cost_basis TEXT NOT NULL DEFAULT 'per_unit',
            overage_penalty DECIMAL(12,4),
            overage_penalty_basis TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS distribution_node_tag (
            dist_node_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            PRIMARY KEY (dist_node_id, tag)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS demand_node (
            demand_node_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            zip3 TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS customer (
            customer_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            demand_node_id TEXT NOT NULL,
            street_address TEXT,
            state_province TEXT,
            country TEXT,
            postal_code TEXT,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS edge (
            edge_id TEXT PRIMARY KEY,
            origin_node_id TEXT NOT NULL,
            origin_node_type TEXT NOT NULL,
            dest_node_id TEXT NOT NULL,
            dest_node_type TEXT NOT NULL,
            transport_type TEXT NOT NULL,
            mean_transit_time DECIMAL(8,4) NOT NULL,
            mean_transit_time_uom TEXT DEFAULT 'days',
            transit_time_distribution TEXT DEFAULT 'lognormal',
            transit_time_std DECIMAL(8,4),
            transit_time_std_uom TEXT,
            transit_time_skew DECIMAL(8,4),
            cost_fixed DECIMAL(12,4) DEFAULT 0,
            cost_variable DECIMAL(12,4) DEFAULT 0,
            cost_variable_basis TEXT DEFAULT 'per_unit',
            distance DECIMAL(10,2),
            distance_uom TEXT DEFAULT 'km',
            distance_method TEXT
        )
    """)

    # --- Product Tables ---

    conn.execute("""
        CREATE TABLE IF NOT EXISTS product (
            product_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            standard_cost DECIMAL(12,4) NOT NULL,
            base_price DECIMAL(12,4) NOT NULL,
            weight DECIMAL(10,4) NOT NULL,
            weight_uom TEXT DEFAULT 'kg',
            cube DECIMAL(10,4) NOT NULL,
            cube_uom TEXT DEFAULT 'L',
            orderable_qty INTEGER NOT NULL DEFAULT 1,
            currency TEXT DEFAULT 'USD'
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS product_attribute (
            product_id TEXT NOT NULL,
            attribute_key TEXT NOT NULL,
            value_type TEXT NOT NULL DEFAULT 'text',
            attribute_value TEXT NOT NULL,
            value_numeric DECIMAL(14,4),
            PRIMARY KEY (product_id, attribute_key)
        )
    """)

    # --- Entity Set Tables ---

    conn.execute("""
        CREATE TABLE IF NOT EXISTS product_set (
            product_set_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS product_set_member (
            product_set_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            PRIMARY KEY (product_set_id, product_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS supply_node_set (
            supply_node_set_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS supply_node_set_member (
            supply_node_set_id TEXT NOT NULL,
            supply_node_id TEXT NOT NULL,
            PRIMARY KEY (supply_node_set_id, supply_node_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS distribution_node_set (
            distribution_node_set_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS distribution_node_set_member (
            distribution_node_set_id TEXT NOT NULL,
            dist_node_id TEXT NOT NULL,
            PRIMARY KEY (distribution_node_set_id, dist_node_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS demand_node_set (
            demand_node_set_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS demand_node_set_member (
            demand_node_set_id TEXT NOT NULL,
            demand_node_id TEXT NOT NULL,
            PRIMARY KEY (demand_node_set_id, demand_node_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS edge_set (
            edge_set_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS edge_set_member (
            edge_set_id TEXT NOT NULL,
            edge_id TEXT NOT NULL,
            PRIMARY KEY (edge_set_id, edge_id)
        )
    """)

    # --- Distance / Rate Tables ---

    conn.execute("""
        CREATE TABLE IF NOT EXISTS distance_matrix (
            origin_id TEXT NOT NULL,
            dest_id TEXT NOT NULL,
            method TEXT NOT NULL,
            distance DECIMAL(10,2) NOT NULL,
            distance_uom TEXT DEFAULT 'km',
            PRIMARY KEY (origin_id, dest_id, method)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS zone_table (
            zone_table_name TEXT NOT NULL,
            origin_key TEXT NOT NULL,
            dest_key TEXT NOT NULL,
            zone TEXT,
            distance DECIMAL(10,2),
            distance_uom TEXT DEFAULT 'km',
            transit_days_base DECIMAL(4,1),
            PRIMARY KEY (zone_table_name, origin_key, dest_key)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS edge_zone_map (
            edge_id TEXT PRIMARY KEY,
            zone_table_name TEXT NOT NULL,
            zone TEXT
        )
    """)

    # --- Simulation Input Tables ---

    conn.execute("""
        CREATE TABLE IF NOT EXISTS demand (
            dataset_version_id TEXT NOT NULL,
            demand_id TEXT NOT NULL,
            demand_date DATE NOT NULL,
            demand_datetime TIMESTAMP,
            demand_node_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity DECIMAL(12,2) NOT NULL,
            order_id TEXT,
            PRIMARY KEY (dataset_version_id, demand_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS inbound_schedule (
            dataset_version_id TEXT NOT NULL,
            inbound_id TEXT NOT NULL,
            supply_node_id TEXT NOT NULL,
            dest_node_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity DECIMAL(12,2) NOT NULL,
            ship_date DATE NOT NULL,
            arrival_date DATE NOT NULL,
            PRIMARY KEY (dataset_version_id, inbound_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS initial_inventory (
            dataset_version_id TEXT NOT NULL,
            dist_node_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            inventory_state TEXT NOT NULL,
            quantity DECIMAL(12,2) NOT NULL,
            PRIMARY KEY (dataset_version_id, dist_node_id, product_id, inventory_state)
        )
    """)

    # --- Simulation Output Tables ---

    conn.execute("""
        CREATE TABLE IF NOT EXISTS event_log (
            scenario_id TEXT NOT NULL,
            event_id BIGINT,
            sim_date DATE NOT NULL,
            sim_step INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            node_id TEXT,
            node_type TEXT,
            edge_id TEXT,
            product_id TEXT,
            quantity DECIMAL(12,2),
            from_state TEXT,
            to_state TEXT,
            demand_id TEXT,
            cost DECIMAL(12,4),
            duration DECIMAL(10,2),
            detail TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS inventory_snapshot (
            scenario_id TEXT NOT NULL,
            sim_date DATE NOT NULL,
            dist_node_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            inventory_state TEXT NOT NULL,
            quantity DECIMAL(12,2) NOT NULL,
            total_cube DECIMAL(14,2),
            total_cube_uom TEXT DEFAULT 'L'
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS run_metadata (
            scenario_id TEXT PRIMARY KEY,
            run_started_at TIMESTAMP NOT NULL,
            run_completed_at TIMESTAMP,
            status TEXT NOT NULL,
            total_steps INTEGER,
            wall_clock_seconds DECIMAL(10,2),
            error_message TEXT,
            engine_version TEXT,
            config_snapshot TEXT
        )
    """)


def clear_scenario_results(conn: duckdb.DuckDBPyConnection, scenario_id: str):
    """Delete all output data for a scenario (event_log, snapshots, run_metadata).

    Does NOT delete the scenario config itself or any input data.
    """
    conn.execute("DELETE FROM event_log WHERE scenario_id = ?", [scenario_id])
    conn.execute("DELETE FROM inventory_snapshot WHERE scenario_id = ?", [scenario_id])
    conn.execute("DELETE FROM run_metadata WHERE scenario_id = ?", [scenario_id])


def scenario_has_results(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> bool:
    """Check if a scenario has existing run results."""
    row = conn.execute(
        "SELECT COUNT(*) FROM run_metadata WHERE scenario_id = ?", [scenario_id]
    ).fetchone()
    return row[0] > 0


def clone_scenario_data(conn: duckdb.DuckDBPyConnection, source_id: str, new_id: str):
    """Clone all scenario data (config + results) under a new scenario_id.

    Copies: scenario, scenario_param, event_log, inventory_snapshot, run_metadata.
    """
    # Clone scenario config
    cols = [r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'scenario' ORDER BY ordinal_position"
    ).fetchall()]
    col_list = ', '.join(c for c in cols if c != 'scenario_id')
    conn.execute(f"""
        INSERT INTO scenario (scenario_id, {col_list})
        SELECT ?, {col_list} FROM scenario WHERE scenario_id = ?
    """, [new_id, source_id])

    # Clone scenario params
    conn.execute("""
        INSERT INTO scenario_param (scenario_id, param_key, param_value)
        SELECT ?, param_key, param_value FROM scenario_param WHERE scenario_id = ?
    """, [new_id, source_id])

    # Clone results (if any)
    for table in ('event_log', 'inventory_snapshot', 'run_metadata'):
        cols = [r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table]
        ).fetchall()]
        col_list = ', '.join(c for c in cols if c != 'scenario_id')
        conn.execute(f"""
            INSERT INTO {table} (scenario_id, {col_list})
            SELECT ?, {col_list} FROM {table} WHERE scenario_id = ?
        """, [new_id, source_id])


def _seed_uom(conn: duckdb.DuckDBPyConnection):
    """Seed the UoM reference table with default units."""
    conn.execute("""
        INSERT OR IGNORE INTO uom VALUES
            ('kg',    'kilogram',     'mass',     TRUE,  1.0),
            ('lb',    'pound',        'mass',     FALSE, 0.45359237),
            ('g',     'gram',         'mass',     FALSE, 0.001),
            ('L',     'liter',        'volume',   TRUE,  1.0),
            ('m3',    'cubic meter',  'volume',   FALSE, 1000.0),
            ('cuft',  'cubic foot',   'volume',   FALSE, 28.3168466),
            ('gal',   'gallon (US)',  'volume',   FALSE, 3.78541),
            ('cm',    'centimeter',   'length',   TRUE,  1.0),
            ('in',    'inch',         'length',   FALSE, 2.54),
            ('km',    'kilometer',    'distance', TRUE,  1.0),
            ('mi',    'mile',         'distance', FALSE, 1.609344),
            ('days',  'days',         'time',     TRUE,  1.0),
            ('hours', 'hours',        'time',     FALSE, 0.0416667),
            ('unit',  'unit',         'count',    TRUE,  1.0),
            ('order', 'order',        'count',    FALSE, 1.0)
    """)
