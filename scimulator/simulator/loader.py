"""
Data loader: loads scenario configuration into DuckDB.

Handles loading YAML scenario configs, demand CSVs from the synthetic demand
engine, and all network topology / product / inventory data.
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

logger = logging.getLogger(__name__)


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
        'dataset_version_id', 'demand_csv', 'params', 'notes',
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
    _load_products(conn, config)
    _load_demand(conn, config)
    _load_inbound_schedule(conn, config)
    _load_initial_inventory(conn, config)
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
    for dn in config.distribution_nodes:
        conn.execute("""
            INSERT OR REPLACE INTO distribution_node VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            dn.dist_node_id, dn.name, dn.latitude, dn.longitude,
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


def _load_products(conn, config: ScenarioConfig):
    for p in config.products:
        conn.execute("""
            INSERT OR REPLACE INTO product VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            p.product_id, p.name, p.standard_cost, p.base_price,
            p.weight, p.weight_uom, p.cube, p.cube_uom, p.orderable_qty,
        ])
        for key, value in p.attributes.items():
            conn.execute("""
                INSERT OR REPLACE INTO product_attribute
                (product_id, attribute_key, attribute_value) VALUES (?, ?, ?)
            """, [p.product_id, key, value])


def _load_demand(conn, config: ScenarioConfig):
    """Load demand data from the synthetic demand engine CSV."""
    if not config.demand_csv:
        logger.warning("No demand_csv specified; skipping demand load")
        return

    csv_path = Path(config.demand_csv).expanduser()
    if not csv_path.exists():
        raise FileNotFoundError(f"Demand CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    logger.info(f"Loading {len(df)} demand rows from {csv_path}")

    # The demand engine outputs: order_id, timestamp, part_number, zip3, quantity
    # We need to map to: dataset_version_id, demand_id, demand_date, demand_datetime,
    #                     demand_node_id, product_id, quantity, order_id

    # Map zip3 to demand_node_id (prefixed with Z for ZIP3 aggregation)
    if 'zip3' in df.columns:
        df['demand_node_id'] = 'Z' + df['zip3'].astype(str).str.zfill(3)
    elif 'demand_node_id' in df.columns:
        pass  # already has the right column
    else:
        raise ValueError("Demand CSV must have 'zip3' or 'demand_node_id' column")

    # Auto-create demand nodes from zip3 data if they don't exist
    if 'zip3' in df.columns:
        unique_zip3s = df[['zip3', 'demand_node_id']].drop_duplicates()
        for _, row in unique_zip3s.iterrows():
            conn.execute("""
                INSERT OR IGNORE INTO demand_node (demand_node_id, name, zip3)
                VALUES (?, ?, ?)
            """, [row['demand_node_id'], f"ZIP3 {row['zip3']}", str(row['zip3'])])

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

    # Insert rows
    for _, row in df.iterrows():
        conn.execute("""
            INSERT OR IGNORE INTO demand VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            config.dataset_version_id,
            str(row['demand_id']),
            row['demand_date'],
            row.get('demand_datetime'),
            row['demand_node_id'],
            row['product_id'],
            float(row['quantity']),
            row.get('order_id'),
        ])

    logger.info(f"Loaded {len(df)} demand events for dataset {config.dataset_version_id}")


def _load_inbound_schedule(conn, config: ScenarioConfig):
    for i in config.inbound_schedule:
        conn.execute("""
            INSERT OR REPLACE INTO inbound_schedule VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            config.dataset_version_id, i.inbound_id,
            i.supply_node_id, i.dest_node_id, i.product_id,
            i.quantity, i.ship_date, i.arrival_date,
        ])


def _load_initial_inventory(conn, config: ScenarioConfig):
    for inv in config.initial_inventory:
        conn.execute("""
            INSERT OR REPLACE INTO initial_inventory VALUES (?, ?, ?, ?, ?)
        """, [
            config.dataset_version_id, inv.dist_node_id,
            inv.product_id, inv.inventory_state, inv.quantity,
        ])


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
