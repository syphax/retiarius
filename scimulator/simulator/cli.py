"""
CLI for the Distribution SCimulator.

Usage:
    python -m scimulator.simulator.cli run scenario.yaml [--db path.duckdb]
    python -m scimulator.simulator.cli results path.duckdb scenario_id
    python -m scimulator.simulator.cli inspect path.duckdb
"""

import argparse
import logging
import sys
from pathlib import Path

import duckdb


def cmd_run(args):
    """Load a scenario YAML and run the simulation."""
    from .loader import load_scenario_from_yaml, load_scenario_into_db
    from .engine import DrawdownEngine
    from .db import open_database, scenario_has_results, clear_scenario_results

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    logger = logging.getLogger('scimulator')

    yaml_path = args.scenario
    db_path = args.db or str(Path(yaml_path).with_suffix('.duckdb'))

    logger.info(f"Loading scenario from {yaml_path}")
    config = load_scenario_from_yaml(yaml_path)

    # Handle --fork: override the scenario_id
    scenario_id = config.scenario_id
    if args.fork:
        scenario_id = args.fork
        config.scenario_id = scenario_id
        logger.info(f"Forking scenario as: {scenario_id}")

    # Check for existing results
    if Path(db_path).exists():
        check_conn = open_database(db_path)
        if scenario_has_results(check_conn, scenario_id):
            if args.replace:
                logger.info(f"Replacing existing results for: {scenario_id}")
                clear_scenario_results(check_conn, scenario_id)
            else:
                check_conn.close()
                print(f"\nError: Scenario '{scenario_id}' already has results in {db_path}")
                print(f"\nOptions:")
                print(f"  --replace    Delete old results and re-run")
                print(f"  --fork ID    Run as a new scenario with a different ID")
                sys.exit(1)
        check_conn.close()

    logger.info(f"Loading data into {db_path}")
    conn = load_scenario_into_db(config, db_path)

    logger.info(f"Running simulation: {scenario_id}")
    engine = DrawdownEngine(conn, scenario_id)
    engine.run()

    conn.close()
    logger.info(f"Done. Results in {db_path}")
    print(f"\nSimulation complete. Database: {db_path}")
    print(f"Run: python -m scimulator.simulator.cli results {db_path} {scenario_id}")


def cmd_results(args):
    """Print summary results for a completed simulation."""
    from .db import open_database
    conn = open_database(args.db, read_only=True)
    sid = args.scenario_id

    # Run metadata
    meta = conn.execute(
        "SELECT * FROM run_metadata WHERE scenario_id = ?", [sid]
    ).fetchone()
    if not meta:
        print(f"No run found for scenario: {sid}")
        sys.exit(1)

    cols = [d[0] for d in conn.description]
    meta = dict(zip(cols, meta))

    print(f"\n{'='*60}")
    print(f"  Scenario: {sid}")
    print(f"  Status: {meta['status']}")
    print(f"  Steps: {meta['total_steps']}")
    print(f"  Wall clock: {meta['wall_clock_seconds']}s")
    if meta.get('error_message'):
        print(f"  Error: {meta['error_message']}")
    print(f"{'='*60}")

    # Event summary
    print(f"\n--- Event Summary ---")
    event_counts = conn.execute("""
        SELECT event_type, COUNT(*) as cnt, SUM(quantity) as total_qty,
               SUM(cost) as total_cost
        FROM event_log
        WHERE scenario_id = ?
        GROUP BY event_type
        ORDER BY cnt DESC
    """, [sid]).fetchall()

    if event_counts:
        print(f"{'Event Type':<25} {'Count':>8} {'Total Qty':>12} {'Total Cost':>12}")
        print(f"{'-'*25} {'-'*8} {'-'*12} {'-'*12}")
        for etype, cnt, total_qty, total_cost in event_counts:
            qty_str = f"{total_qty:,.0f}" if total_qty else "-"
            cost_str = f"${total_cost:,.2f}" if total_cost else "-"
            print(f"{etype:<25} {cnt:>8,} {qty_str:>12} {cost_str:>12}")
    else:
        print("  No events recorded.")

    # Fulfillment rate
    demand_count = conn.execute("""
        SELECT COUNT(*), SUM(quantity) FROM event_log
        WHERE scenario_id = ? AND event_type = 'demand_received'
    """, [sid]).fetchone()

    fulfilled_count = conn.execute("""
        SELECT COUNT(*), SUM(quantity) FROM event_log
        WHERE scenario_id = ? AND event_type IN ('demand_fulfilled', 'backorder_fulfilled')
    """, [sid]).fetchone()

    if demand_count[0] > 0:
        fill_rate_qty = (fulfilled_count[1] or 0) / demand_count[1] * 100 if demand_count[1] else 0
        print(f"\n--- Fulfillment ---")
        print(f"  Demand events: {demand_count[0]:,}")
        print(f"  Demand units: {demand_count[1]:,.0f}")
        print(f"  Fulfilled units: {(fulfilled_count[1] or 0):,.0f}")
        print(f"  Fill rate: {fill_rate_qty:.1f}%")

    # Inventory at end of simulation
    last_snap_date = conn.execute("""
        SELECT MAX(sim_date) FROM inventory_snapshot WHERE scenario_id = ?
    """, [sid]).fetchone()[0]

    if last_snap_date:
        print(f"\n--- Final Inventory (as of {last_snap_date}) ---")
        inv_summary = conn.execute("""
            SELECT inventory_state, SUM(quantity) as total_qty,
                   COUNT(DISTINCT dist_node_id) as nodes,
                   COUNT(DISTINCT product_id) as products
            FROM inventory_snapshot
            WHERE scenario_id = ? AND sim_date = ?
            GROUP BY inventory_state
            ORDER BY total_qty DESC
        """, [sid, last_snap_date]).fetchall()

        print(f"{'State':<15} {'Qty':>10} {'Nodes':>8} {'Products':>10}")
        print(f"{'-'*15} {'-'*10} {'-'*8} {'-'*10}")
        for state, qty, nodes, products in inv_summary:
            print(f"{state:<15} {qty:>10,.0f} {nodes:>8} {products:>10}")

    # Cost summary
    total_cost = conn.execute("""
        SELECT SUM(cost) FROM event_log
        WHERE scenario_id = ? AND cost IS NOT NULL
    """, [sid]).fetchone()[0]

    if total_cost:
        print(f"\n--- Cost Summary ---")
        print(f"  Total cost: ${total_cost:,.2f}")

        cost_by_type = conn.execute("""
            SELECT event_type, SUM(cost) as total
            FROM event_log
            WHERE scenario_id = ? AND cost IS NOT NULL AND cost > 0
            GROUP BY event_type
            ORDER BY total DESC
        """, [sid]).fetchall()
        for etype, cost in cost_by_type:
            print(f"  {etype}: ${cost:,.2f}")

    print()
    conn.close()


def cmd_inspect(args):
    """Inspect the contents of a SCimulator database."""
    from .db import open_database
    conn = open_database(args.db, read_only=True)

    print(f"\n--- Database: {args.db} ---\n")

    tables = conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()

    for (table_name,) in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        if count > 0:
            print(f"  {table_name}: {count:,} rows")

    # List scenarios
    scenarios = conn.execute("""
        SELECT scenario_id, name, start_date, end_date
        FROM scenario
    """).fetchall()

    if scenarios:
        print(f"\n--- Scenarios ---")
        for sid, name, start, end in scenarios:
            print(f"  {sid}: {name} ({start} to {end})")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Distribution SCimulator CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m scimulator.simulator.cli run scenario.yaml
  python -m scimulator.simulator.cli run scenario.yaml --db output.duckdb
  python -m scimulator.simulator.cli results output.duckdb my_scenario
  python -m scimulator.simulator.cli inspect output.duckdb
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # run
    run_parser = subparsers.add_parser('run', help='Run a simulation from a YAML scenario')
    run_parser.add_argument('scenario', help='Path to scenario YAML file')
    run_parser.add_argument('--db', help='Output DuckDB path (default: same name as YAML)')
    run_parser.add_argument('-v', '--verbose', action='store_true')
    run_parser.add_argument('--replace', action='store_true',
                            help='Delete existing results and re-run the scenario')
    run_parser.add_argument('--fork', metavar='NEW_ID',
                            help='Run as a new scenario with a different ID')

    # results
    results_parser = subparsers.add_parser('results', help='Show results for a simulation run')
    results_parser.add_argument('db', help='Path to DuckDB database')
    results_parser.add_argument('scenario_id', help='Scenario ID')

    # inspect
    inspect_parser = subparsers.add_parser('inspect', help='Inspect database contents')
    inspect_parser.add_argument('db', help='Path to DuckDB database')

    args = parser.parse_args()

    if args.command == 'run':
        cmd_run(args)
    elif args.command == 'results':
        cmd_results(args)
    elif args.command == 'inspect':
        cmd_inspect(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
