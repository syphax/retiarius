"""
Data loader: loads scenario configuration into DuckDB.

Handles loading YAML scenario configs, demand CSVs from the synthetic demand
engine, and all network topology / product / inventory data.

CSV loading uses DuckDB's native read_csv_auto for bulk performance.
"""

import yaml
import uuid
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

import duckdb
import pandas as pd

from .models import ScenarioConfig, SupplierConfig, SupplyNodeConfig
from .models import DistributionNodeConfig, DemandNodeConfig, EdgeConfig
from .models import ProductConfig, InboundShipment, InitialInventory
from .db import create_database, open_database
from .edge_builders import build_edges_from_zones

logger = logging.getLogger(__name__)


def _resolve_csv(path_str: str) -> str:
    """Resolve a CSV path and verify it exists. Returns absolute path string."""
    csv_path = Path(path_str).expanduser()
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    return str(csv_path.resolve())


def load_scenario_from_yaml(yaml_path: str) -> ScenarioConfig:
    """Load a scenario configuration from a YAML file."""
    with open(yaml_path, 'r') as f:
        raw = yaml.safe_load(f)

    # Parse nested objects
    suppliers = [SupplierConfig(**s) for s in raw.get('suppliers', [])]
    supply_nodes = [SupplyNodeConfig(**s) for s in raw.get('supply_nodes', [])]
    distribution_nodes = [DistributionNodeConfig(**d) for d in raw.get('distribution_nodes', [])]
    demand_nodes = [DemandNodeConfig(**d) for d in raw.get('demand_nodes', [])]
    edges = [EdgeConfig(**e) for e in raw.get('edges', [])]
    products = [ProductConfig(**p) for p in raw.get('products', [])]
    inbound_schedule = [InboundShipment(**i) for i in raw.get('inbound_schedule', [])]
    initial_inventory = [InitialInventory(**i) for i in raw.get('initial_inventory', [])]

    # Build top-level config
    top_keys = {
        'scenario_id', 'name', 'description', 'currency_code', 'time_resolution',
        'start_date', 'end_date', 'warm_up_days', 'backorder_probability',
        'write_event_log', 'write_snapshots', 'snapshot_interval_days',
        'dataset_version_id', 'demand_csv', 'inbound_schedule_csv',
        'initial_inventory_csv', 'product_csv', 'distribution_nodes_csv',
        'edge_csvs', 'zone_table_csv',
        'params', 'notes',
        'product_set_id', 'supply_node_set_id', 'distribution_node_set_id',
        'demand_node_set_id', 'edge_set_id',
    }
    top = {k: v for k, v in raw.items() if k in top_keys}

    return ScenarioConfig(
        **top,
        suppliers=suppliers,
        supply_nodes=supply_nodes,
        distribution_nodes=distribution_nodes,
        demand_nodes=demand_nodes,
        edges=edges,
        products=products,
        inbound_schedule=inbound_schedule,
        initial_inventory=initial_inventory,
    )


def load_scenario_into_db(config: ScenarioConfig, db_path: str) -> duckdb.DuckDBPyConnection:
    """Load a full scenario configuration into a DuckDB database.

    Creates the database if it doesn't exist. Returns the connection.
    """
    if Path(db_path).exists():
        conn = open_database(db_path)
    else:
        conn = create_database(db_path)

    _load_dataset_version(conn, config)
    _load_suppliers(conn, config)
    _load_supply_nodes(conn, config)
    _load_distribution_nodes(conn, config)
    _load_demand_nodes(conn, config)
    _load_edges(conn, config)
    _load_zone_table(conn, config)
    _load_products(conn, config)
    _load_demand(conn, config)
    _load_inbound_schedule(conn, config)
    _load_initial_inventory(conn, config)
    _build_generated_edges(conn, config)
    _load_scenario(conn, config)

    return conn


def _load_dataset_version(conn, config: ScenarioConfig):
    conn.execute("""
        INSERT OR IGNORE INTO dataset_version (dataset_version_id, name, created_by)
        VALUES (?, ?, 'loader')
    """, [config.dataset_version_id, f"Dataset for {config.name}"])


def _load_suppliers(conn, config: ScenarioConfig):
    for s in config.suppliers:
        conn.execute("""
            INSERT OR REPLACE INTO supplier VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            s.supplier_id, s.name, s.default_lead_time, s.default_lead_time_uom,
            s.default_qty_reliability, s.default_timing_variance,
            s.default_timing_variance_uom, s.timing_variance_distribution,
            s.timing_variance_std, s.timing_variance_std_uom,
        ])


def _load_supply_nodes(conn, config: ScenarioConfig):
    for sn in config.supply_nodes:
        conn.execute("""
            INSERT OR REPLACE INTO supply_node VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            sn.supply_node_id, sn.supplier_id, sn.name,
            sn.latitude, sn.longitude,
            sn.lead_time, sn.lead_time_uom, sn.qty_reliability,
            None, None, None, None, None,  # timing variance overrides
            sn.max_capacity, sn.max_capacity_uom,
        ])
        for tag in sn.tags:
            conn.execute(
                "INSERT OR IGNORE INTO supply_node_tag VALUES (?, ?)",
                [sn.supply_node_id, tag]
            )
        for pid in sn.products:
            conn.execute(
                "INSERT OR IGNORE INTO supply_node_product (supply_node_id, product_id) VALUES (?, ?)",
                [sn.supply_node_id, pid]
            )


def _load_distribution_nodes(conn, config: ScenarioConfig):
    # Load from CSV — bulk via DuckDB with column aliasing
    if config.distribution_nodes_csv:
        csv_path = _resolve_csv(config.distribution_nodes_csv)

        # Read into pandas for column aliasing (small table, flexibility matters more)
        df = pd.read_csv(csv_path)
        if 'facility_code' in df.columns and 'dist_node_id' not in df.columns:
            df['dist_node_id'] = df['facility_code']
        if 'lat' in df.columns and 'latitude' not in df.columns:
            df['latitude'] = df['lat']
        if 'lng' in df.columns and 'longitude' not in df.columns:
            df['longitude'] = df['lng']
        if 'zip3' in df.columns:
            df['zip3'] = df['zip3'].astype(str).str.zfill(3)

        insert_df = pd.DataFrame({
            'dist_node_id': df['dist_node_id'],
            'name': df.get('name', df['dist_node_id']),
            'latitude': df.get('latitude'),
            'longitude': df.get('longitude'),
            'zip3': df.get('zip3'),
        })
        conn.execute("""
            INSERT OR REPLACE INTO distribution_node
            SELECT
                dist_node_id, name, latitude, longitude, zip3,
                NULL, 'm3', NULL, NULL, NULL, NULL,
                1.0, 'days', 0.0, 'per_day', 0.0, 'per_unit', NULL, NULL
            FROM insert_df
        """)
        logger.info(f"Loaded {len(df)} distribution nodes from {csv_path}")

    # Load from inline YAML entries
    for dn in config.distribution_nodes:
        conn.execute("""
            INSERT OR REPLACE INTO distribution_node VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            dn.dist_node_id, dn.name, dn.latitude, dn.longitude, dn.zip3,
            dn.storage_capacity, dn.storage_capacity_uom,
            dn.max_inbound, dn.max_inbound_uom,
            dn.max_outbound, dn.max_outbound_uom,
            dn.order_response_time, dn.order_response_time_uom,
            dn.fixed_cost, dn.fixed_cost_basis,
            dn.variable_cost, dn.variable_cost_basis,
            dn.overage_penalty, dn.overage_penalty_basis,
        ])
        for tag in dn.tags:
            conn.execute(
                "INSERT OR IGNORE INTO distribution_node_tag VALUES (?, ?)",
                [dn.dist_node_id, tag]
            )


def _load_demand_nodes(conn, config: ScenarioConfig):
    for d in config.demand_nodes:
        conn.execute("""
            INSERT OR REPLACE INTO demand_node VALUES (?, ?, ?, ?, ?)
        """, [d.demand_node_id, d.name, d.latitude, d.longitude, d.zip3])


def _load_edges(conn, config: ScenarioConfig):
    # Load from inline YAML entries
    for e in config.edges:
        conn.execute("""
            INSERT OR REPLACE INTO edge VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            e.edge_id, e.origin_node_id, e.origin_node_type,
            e.dest_node_id, e.dest_node_type, e.transport_type,
            e.mean_transit_time, e.mean_transit_time_uom,
            e.transit_time_distribution, e.transit_time_std,
            e.transit_time_std_uom, e.transit_time_skew
            if hasattr(e, 'transit_time_skew') else None,
            e.cost_fixed, e.cost_variable, e.cost_variable_basis,
            e.distance, e.distance_uom, e.distance_method,
        ])

    # Load from edge CSVs — bulk via DuckDB
    for csv_file in config.edge_csvs:
        csv_path = _resolve_csv(csv_file)

        conn.execute(f"""
            INSERT OR REPLACE INTO edge
            SELECT
                edge_id,
                origin_node_id,
                origin_node_type,
                dest_node_id,
                dest_node_type,
                COALESCE(transport_type, 'parcel'),
                COALESCE(mean_transit_time, 2.0),
                COALESCE(mean_transit_time_uom, 'days'),
                COALESCE(transit_time_distribution, 'lognormal'),
                transit_time_std,
                transit_time_std_uom,
                transit_time_skew,
                COALESCE(cost_fixed, 0),
                COALESCE(cost_variable, 0),
                COALESCE(cost_variable_basis, 'per_unit'),
                distance,
                COALESCE(distance_uom, 'km'),
                distance_method
            FROM read_csv_auto('{csv_path}', union_by_name=true, all_varchar=false)
        """)
        rows = conn.execute(f"""
            SELECT count(*) FROM read_csv_auto('{csv_path}')
        """).fetchone()[0]
        logger.info(f"Loaded {rows} edges from {csv_path}")


def _load_zone_table(conn, config: ScenarioConfig):
    """Load parcel zone table from CSV — bulk via DuckDB."""
    if not config.zone_table_csv:
        return

    csv_path = _resolve_csv(config.zone_table_csv)

    conn.execute(f"""
        INSERT OR REPLACE INTO zone_table
        SELECT
            lpad(CAST(origin_zip3 AS VARCHAR), 3, '0'),
            lpad(CAST(dest_zip3 AS VARCHAR), 3, '0'),
            CAST(zone AS VARCHAR),
            distance_haversine,
            COALESCE(distance_uom, 'km'),
            transit_days_base
        FROM read_csv_auto('{csv_path}', all_varchar=false)
    """)
    rows = conn.execute(f"""
        SELECT count(*) FROM read_csv_auto('{csv_path}')
    """).fetchone()[0]
    logger.info(f"Loaded {rows} zone table entries from {csv_path}")


def _load_products(conn, config: ScenarioConfig):
    # Load from CSV — pandas for column aliasing, then bulk insert via DuckDB
    if config.product_csv:
        csv_path = _resolve_csv(config.product_csv)
        df = pd.read_csv(csv_path)

        if 'part_number' in df.columns and 'product_id' not in df.columns:
            df['product_id'] = df['part_number']

        insert_df = pd.DataFrame({
            'product_id': df['product_id'],
            'name': df.get('name', df['product_id']),
            'standard_cost': df.get('standard_cost', 0.0).fillna(0.0),
            'base_price': df.get('base_price', 0.0).fillna(0.0),
            'weight': df.get('weight', 0.0).fillna(0.0),
            'weight_uom': df.get('weight_uom', 'kg').fillna('kg'),
            'cube': df.get('cube', 1.0).fillna(1.0),
            'cube_uom': df.get('cube_uom', 'L').fillna('L'),
            'orderable_qty': df.get('orderable_qty', 1).fillna(1).astype(int),
            'currency': df.get('currency', 'USD').fillna('USD'),
        })
        conn.execute("INSERT OR REPLACE INTO product SELECT * FROM insert_df")
        logger.info(f"Loaded {len(df)} products from {csv_path}")

    # Load from inline YAML entries
    for p in config.products:
        conn.execute("""
            INSERT OR REPLACE INTO product VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            p.product_id, p.name, p.standard_cost, p.base_price,
            p.weight, p.weight_uom, p.cube, p.cube_uom, p.orderable_qty,
            p.currency,
        ])
        for key, value in p.attributes.items():
            conn.execute("""
                INSERT OR REPLACE INTO product_attribute
                (product_id, attribute_key, attribute_value) VALUES (?, ?, ?)
            """, [p.product_id, key, value])


def _load_demand(conn, config: ScenarioConfig):
    """Load demand data from the synthetic demand engine CSV.

    This loader does column mapping/transforms in pandas, then bulk-inserts
    via DuckDB's native DataFrame ingestion.
    """
    if not config.demand_csv:
        logger.warning("No demand_csv specified; skipping demand load")
        return

    csv_path = Path(config.demand_csv).expanduser()
    if not csv_path.exists():
        raise FileNotFoundError(f"Demand CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    logger.info(f"Loading {len(df)} demand rows from {csv_path}")

    # Map zip3 to demand_node_id (prefixed with Z for ZIP3 aggregation)
    if 'zip3' in df.columns:
        df['demand_node_id'] = 'Z' + df['zip3'].astype(str).str.zfill(3)
    elif 'demand_node_id' in df.columns:
        pass
    else:
        raise ValueError("Demand CSV must have 'zip3' or 'demand_node_id' column")

    # Auto-create demand nodes from zip3 data
    if 'zip3' in df.columns:
        unique_zip3s = df[['zip3', 'demand_node_id']].drop_duplicates()
        unique_zip3s['name'] = 'ZIP3 ' + unique_zip3s['zip3'].astype(str)
        unique_zip3s['zip3_str'] = unique_zip3s['zip3'].astype(str)
        demand_nodes_df = unique_zip3s[['demand_node_id', 'name', 'zip3_str']].copy()
        demand_nodes_df.columns = ['demand_node_id', 'name', 'zip3']
        conn.execute("""
            INSERT OR IGNORE INTO demand_node (demand_node_id, name, zip3)
            SELECT demand_node_id, name, zip3 FROM demand_nodes_df
        """)

    # Parse timestamps
    if 'timestamp' in df.columns:
        df['demand_datetime'] = pd.to_datetime(df['timestamp'])
        df['demand_date'] = df['demand_datetime'].dt.date
    elif 'demand_date' in df.columns:
        df['demand_date'] = pd.to_datetime(df['demand_date']).dt.date
        df['demand_datetime'] = None
    else:
        raise ValueError("Demand CSV must have 'timestamp' or 'demand_date' column")

    # Map product column
    if 'part_number' in df.columns:
        df['product_id'] = df['part_number']
    elif 'product_id' not in df.columns:
        raise ValueError("Demand CSV must have 'part_number' or 'product_id' column")

    # Generate demand_id if not present
    if 'demand_id' not in df.columns:
        if 'order_id' in df.columns:
            df['demand_id'] = df['order_id']
        else:
            df['demand_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

    # Add dataset_version_id
    df['dataset_version_id'] = config.dataset_version_id

    # Ensure order_id exists
    if 'order_id' not in df.columns:
        df['order_id'] = None

    # Bulk insert via DuckDB DataFrame ingestion
    insert_df = df[['dataset_version_id', 'demand_id', 'demand_date',
                     'demand_datetime', 'demand_node_id', 'product_id',
                     'quantity', 'order_id']].copy()
    insert_df['demand_id'] = insert_df['demand_id'].astype(str)
    insert_df['quantity'] = insert_df['quantity'].astype(float)

    conn.execute("""
        INSERT OR IGNORE INTO demand
        SELECT * FROM insert_df
    """)

    logger.info(f"Loaded {len(df)} demand events for dataset {config.dataset_version_id}")


def _load_inbound_schedule(conn, config: ScenarioConfig):
    # Load from inline YAML entries
    for i in config.inbound_schedule:
        conn.execute("""
            INSERT OR REPLACE INTO inbound_schedule VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            config.dataset_version_id, i.inbound_id,
            i.supply_node_id, i.dest_node_id, i.product_id,
            i.quantity, i.ship_date, i.arrival_date,
        ])

    # Load from CSV — bulk via DuckDB
    if config.inbound_schedule_csv:
        csv_path = _resolve_csv(config.inbound_schedule_csv)
        dsv = config.dataset_version_id

        conn.execute(f"""
            INSERT OR REPLACE INTO inbound_schedule
            SELECT
                '{dsv}' AS dataset_version_id,
                CAST(inbound_id AS VARCHAR),
                supply_node_id,
                dest_node_id,
                product_id,
                quantity,
                ship_date,
                arrival_date
            FROM read_csv_auto('{csv_path}', all_varchar=false)
        """)
        rows = conn.execute(f"""
            SELECT count(*) FROM read_csv_auto('{csv_path}')
        """).fetchone()[0]
        logger.info(f"Loaded {rows} inbound schedule events from {csv_path}")


def _load_initial_inventory(conn, config: ScenarioConfig):
    # Load from inline YAML entries
    for inv in config.initial_inventory:
        conn.execute("""
            INSERT OR REPLACE INTO initial_inventory VALUES (?, ?, ?, ?, ?)
        """, [
            config.dataset_version_id, inv.dist_node_id,
            inv.product_id, inv.inventory_state, inv.quantity,
        ])

    # Load from CSV — pandas for defaults, then bulk insert
    if config.initial_inventory_csv:
        csv_path = _resolve_csv(config.initial_inventory_csv)
        df = pd.read_csv(csv_path)

        if 'inventory_state' not in df.columns:
            df['inventory_state'] = 'saleable'

        df['dataset_version_id'] = config.dataset_version_id
        insert_df = df[['dataset_version_id', 'dist_node_id', 'product_id',
                         'inventory_state', 'quantity']]
        conn.execute("INSERT OR REPLACE INTO initial_inventory SELECT * FROM insert_df")
        logger.info(f"Loaded {len(df)} initial inventory rows from {csv_path}")


def _build_generated_edges(conn, config: ScenarioConfig):
    """Run edge builders that generate edges from abstract relationships.

    Currently supports zone-table-based edge generation. Future builders
    (distance-based cost rules, LTL lane tables, etc.) plug in here.
    """
    if config.zone_table_csv:
        build_edges_from_zones(conn)


def _load_scenario(conn, config: ScenarioConfig):
    conn.execute("""
        INSERT OR REPLACE INTO scenario VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        config.scenario_id, config.name, config.description,
        config.dataset_version_id, config.currency_code,
        config.time_resolution, config.start_date, config.end_date,
        config.warm_up_days, config.backorder_probability,
        config.write_event_log, config.write_snapshots,
        config.snapshot_interval_days,
        config.product_set_id, config.supply_node_set_id,
        config.distribution_node_set_id, config.demand_node_set_id,
        config.edge_set_id,
        datetime.now(), config.notes,
    ])

    for key, value in config.params.items():
        conn.execute("""
            INSERT OR REPLACE INTO scenario_param VALUES (?, ?, ?)
        """, [config.scenario_id, key, value])
