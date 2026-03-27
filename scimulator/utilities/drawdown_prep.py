"""Generate initial inventory and inbound schedule CSVs for a drawdown simulation.

Usage:
    python -m scimulator.utilities.drawdown_prep [config_path]

    config_path : Optional path to a YAML config file. Defaults to
                  scimulator/utilities/config/drawdown_prep_config.yaml.

Output:
    Writes two CSVs to scimulator/utilities/output/:

    initial_inventory-{name}.csv
        dist_node_id  — Distribution node identifier (from node_file facility_code)
        product_id    — Product identifier (from demand_file product_id)
        quantity      — Starting inventory units

    inbound_schedule-{name}.csv
        inbound_id     — Unique receipt identifier
        supply_node_id — Source supply node (from config)
        dest_node_id   — Destination distribution node
        product_id     — Product identifier
        quantity       — Receipt quantity
        ship_date      — Ship date (ISO)
        arrival_date   — Arrival date (ISO)

How it works:
    1. Reads the demand CSV to compute monthly demand per product.
    2. Reads the node CSV to get active distribution nodes.
    3. For each product, draws total initial inventory from
       N(inventory_mos, inventory_mos_sd) * monthly_demand, then splits
       evenly across nodes (remainders distributed one per node).
    4. For each product, picks a mean arrival day uniformly from
       [reorder_start, reorder_end]. Per-node arrival dates are scattered
       around this mean with std dev reorder_dt_sd. Reorder quantity is
       drawn from N(reorder_mos, reorder_mos_sd) * monthly_demand, split
       across nodes.

See config/drawdown_prep_config.yaml for parameter documentation.
"""

import argparse
import csv
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yaml


def load_demand(demand_file: str) -> dict[str, float]:
    """Load demand CSV and return monthly demand per product.

    Returns a dict of {product_id: monthly_units}.
    """
    df = pd.read_csv(demand_file)

    # Resolve product column
    if 'product_id' not in df.columns and 'product_id' in df.columns:
        df.rename(columns={'product_id': 'product_id'}, inplace=True)
    elif 'product_id' not in df.columns:
        raise ValueError("Demand CSV must have 'product_id' column")

    # Resolve date column
    if 'timestamp' in df.columns:
        df['date'] = pd.to_datetime(df['timestamp']).dt.date
    elif 'demand_date' in df.columns:
        df['date'] = pd.to_datetime(df['demand_date']).dt.date
    else:
        raise ValueError("Demand CSV must have 'timestamp' or 'demand_date' column")

    # Compute date span in months
    min_date = df['date'].min()
    max_date = df['date'].max()
    span_days = (max_date - min_date).days + 1
    span_months = span_days / 30.4375  # average days per month

    if span_months < 0.5:
        raise ValueError(
            f"Demand date span is only {span_days} days — need at least ~15 days "
            f"to estimate monthly demand"
        )

    # Sum quantity per product, convert to monthly
    totals = df.groupby('product_id')['quantity'].sum()
    monthly = (totals / span_months).to_dict()

    return monthly


def load_nodes(node_file: str) -> list[str]:
    """Load node CSV and return list of facility_code values as node IDs."""
    df = pd.read_csv(node_file)
    if 'facility_code' not in df.columns:
        raise ValueError("Node CSV must have 'facility_code' column")
    return df['facility_code'].tolist()


def distribute_evenly(total: int, n: int) -> list[int]:
    """Distribute total units across n bins as evenly as possible.

    E.g. distribute_evenly(12, 10) -> [2, 2, 1, 1, 1, 1, 1, 1, 1, 1]
    """
    if n == 0:
        return []
    base = total // n
    remainder = total % n
    return [base + (1 if i < remainder else 0) for i in range(n)]


def generate_initial_inventory(
    monthly_demand: dict[str, float],
    nodes: list[str],
    inventory_mos: float,
    inventory_mos_sd: float,
    rng: np.random.Generator,
) -> list[dict]:
    """Generate initial inventory rows."""
    rows = []
    n_nodes = len(nodes)

    for product_id, monthly_units in monthly_demand.items():
        # Draw months of inventory for this product
        mos = rng.normal(inventory_mos, inventory_mos_sd)
        mos = max(mos, 0.5)  # floor at half a month

        total_qty = max(0, round(mos * monthly_units))
        if total_qty == 0:
            continue

        per_node = distribute_evenly(total_qty, n_nodes)
        for node_id, qty in zip(nodes, per_node):
            if qty > 0:
                rows.append({
                    'dist_node_id': node_id,
                    'product_id': product_id,
                    'quantity': qty,
                })

    return rows


def generate_inbound_schedule(
    monthly_demand: dict[str, float],
    nodes: list[str],
    supply_node_id: str,
    start_date: datetime,
    ship_lead_time: int,
    reorder_mos: float,
    reorder_mos_sd: float,
    reorder_start: int,
    reorder_end: int,
    reorder_dt_sd: float,
    rng: np.random.Generator,
) -> list[dict]:
    """Generate inbound schedule rows."""
    rows = []
    n_nodes = len(nodes)
    seq = 0

    for product_id, monthly_units in monthly_demand.items():
        # Draw reorder quantity (network total)
        mos = rng.normal(reorder_mos, reorder_mos_sd)
        mos = max(mos, 0.5)

        total_qty = max(0, round(mos * monthly_units))
        if total_qty == 0:
            continue

        per_node = distribute_evenly(total_qty, n_nodes)

        # Pick mean arrival day for this product
        mean_day = rng.uniform(reorder_start, reorder_end)

        for node_id, qty in zip(nodes, per_node):
            if qty == 0:
                continue

            # Scatter arrival date around mean
            arrival_day = round(rng.normal(mean_day, reorder_dt_sd))
            arrival_day = max(1, arrival_day)  # don't arrive before day 1

            arrival_date = start_date + timedelta(days=arrival_day)
            ship_date = arrival_date - timedelta(days=ship_lead_time)

            seq += 1
            rows.append({
                'inbound_id': f"INB_{seq:06d}",
                'supply_node_id': supply_node_id,
                'dest_node_id': node_id,
                'product_id': product_id,
                'quantity': qty,
                'ship_date': ship_date.strftime('%Y-%m-%d'),
                'arrival_date': arrival_date.strftime('%Y-%m-%d'),
            })

    return rows


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(script_dir, "config", "drawdown_prep_config.yaml")

    parser = argparse.ArgumentParser(
        description="Generate initial inventory and inbound schedule for drawdown simulation"
    )
    parser.add_argument(
        "config",
        nargs="?",
        default=default_config,
        help="Path to YAML config file (default: %(default)s)",
    )
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    # Extract config
    name = config["name"]
    demand_file = config["demand_file"]
    node_file = config["node_file"]
    start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
    supply_node_id = config.get("supply_node_id", "GENERIC_SUPPLY")
    ship_lead_time = config.get("ship_lead_time", 0)
    inventory_mos = config["inventory_mos"]
    inventory_mos_sd = config["inventory_mos_sd"]
    reorder_mos = config["reorder_mos"]
    reorder_mos_sd = config["reorder_mos_sd"]
    reorder_start = config["reorder_start"]
    reorder_end = config["reorder_end"]
    reorder_dt_sd = config["reorder_dt_sd"]
    seed = config.get("seed", None)

    rng = np.random.default_rng(seed)

    # Load inputs
    monthly_demand = load_demand(demand_file)
    nodes = load_nodes(node_file)

    print(f"Loaded demand for {len(monthly_demand)} products")
    print(f"Loaded {len(nodes)} distribution nodes")
    print(f"Demand date span used to compute monthly rates")

    # Generate
    inv_rows = generate_initial_inventory(
        monthly_demand, nodes, inventory_mos, inventory_mos_sd, rng,
    )
    inb_rows = generate_inbound_schedule(
        monthly_demand, nodes, supply_node_id, start_date, ship_lead_time,
        reorder_mos, reorder_mos_sd, reorder_start, reorder_end, reorder_dt_sd, rng,
    )

    # Write output
    output_dir = os.path.join(script_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    inv_path = os.path.join(output_dir, f"initial_inventory-{name}.csv")
    with open(inv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["dist_node_id", "product_id", "quantity"])
        writer.writeheader()
        writer.writerows(inv_rows)

    inb_path = os.path.join(output_dir, f"inbound_schedule-{name}.csv")
    with open(inb_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "inbound_id", "supply_node_id", "dest_node_id",
            "product_id", "quantity", "ship_date", "arrival_date",
        ])
        writer.writeheader()
        writer.writerows(inb_rows)

    # Summary
    total_inv = sum(r['quantity'] for r in inv_rows)
    total_inb = sum(r['quantity'] for r in inb_rows)
    print(f"\nInitial inventory: {len(inv_rows)} rows, {total_inv:,} total units → {inv_path}")
    print(f"Inbound schedule:  {len(inb_rows)} rows, {total_inb:,} total units → {inb_path}")

    # Per-product summary
    if monthly_demand:
        print(f"\nPer-product monthly demand range: "
              f"{min(monthly_demand.values()):.1f} – {max(monthly_demand.values()):.1f} units")
        print(f"Total monthly demand: {sum(monthly_demand.values()):,.0f} units")

    # Inbound date range
    if inb_rows:
        dates = [r['arrival_date'] for r in inb_rows]
        print(f"Inbound arrivals: {min(dates)} to {max(dates)}")


if __name__ == "__main__":
    main()
