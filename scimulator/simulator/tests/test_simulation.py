"""
End-to-end test for the Distribution SCimulator Phase 1.

Tests the full pipeline: YAML load -> DuckDB -> drawdown simulation -> results.
"""

import os
import sys
import tempfile
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scimulator.simulator.loader import load_scenario_from_yaml, load_scenario_into_db
from scimulator.simulator.engine import DrawdownEngine
from scimulator.simulator.db import open_database


def test_full_pipeline():
    """Test the complete Phase 1 simulation pipeline."""
    test_dir = Path(__file__).parent
    yaml_path = str(test_dir / "sample_scenario.yaml")

    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
        db_path = f.name
    os.unlink(db_path)  # Remove so create_database can create it fresh

    try:
        # 1. Load scenario config
        config = load_scenario_from_yaml(yaml_path)
        assert config.scenario_id == "drawdown_sample_01"
        assert config.start_date == "2024-01-01"
        assert config.end_date == "2024-03-31"
        assert len(config.products) == 4
        assert len(config.distribution_nodes) == 2
        assert len(config.edges) == 15
        assert len(config.initial_inventory) == 8
        assert len(config.inbound_schedule) == 12
        print("PASS: Scenario config loaded correctly")

        # 2. Load into DuckDB
        conn = load_scenario_into_db(config, db_path)

        product_count = conn.execute("SELECT COUNT(*) FROM product").fetchone()[0]
        assert product_count == 4, f"Expected 4 products, got {product_count}"

        demand_count = conn.execute("SELECT COUNT(*) FROM demand").fetchone()[0]
        assert demand_count > 7000, f"Expected >7000 demand rows, got {demand_count}"

        edge_count = conn.execute("SELECT COUNT(*) FROM edge").fetchone()[0]
        assert edge_count == 15, f"Expected 15 edges, got {edge_count}"

        inv_count = conn.execute("SELECT COUNT(*) FROM initial_inventory").fetchone()[0]
        assert inv_count == 8, f"Expected 8 initial inventory rows, got {inv_count}"

        uom_count = conn.execute("SELECT COUNT(*) FROM uom").fetchone()[0]
        assert uom_count == 15, f"Expected 15 UoM entries, got {uom_count}"
        print("PASS: Data loaded into DuckDB correctly")

        # 3. Run simulation
        engine = DrawdownEngine(conn, config.scenario_id)
        engine.run()

        # 4. Verify run metadata
        meta = conn.execute(
            "SELECT status, total_steps FROM run_metadata WHERE scenario_id = ?",
            [config.scenario_id]
        ).fetchone()
        assert meta[0] == 'completed', f"Expected completed, got {meta[0]}"
        assert meta[1] == 91, f"Expected 91 steps, got {meta[1]}"
        print("PASS: Simulation completed successfully")

        # 5. Verify event log
        event_count = conn.execute("SELECT COUNT(*) FROM event_log").fetchone()[0]
        assert event_count > 1000, f"Expected >1000 events, got {event_count}"

        event_types = conn.execute(
            "SELECT DISTINCT event_type FROM event_log ORDER BY event_type"
        ).fetchall()
        event_types = [e[0] for e in event_types]
        assert 'demand_received' in event_types
        assert 'demand_fulfilled' in event_types
        assert 'shipment_arrived' in event_types
        assert 'inventory_state_change' in event_types
        print(f"PASS: Event log has {event_count} events with types: {event_types}")

        # 6. Verify fulfillment
        demand_qty = conn.execute(
            "SELECT SUM(quantity) FROM event_log WHERE event_type='demand_received'"
        ).fetchone()[0]
        fulfilled_qty = conn.execute(
            "SELECT SUM(quantity) FROM event_log WHERE event_type IN ('demand_fulfilled', 'backorder_fulfilled')"
        ).fetchone()[0]
        fill_rate = float(fulfilled_qty) / float(demand_qty) * 100
        assert 80 < fill_rate < 95, f"Unexpected fill rate: {fill_rate:.1f}%"
        print(f"PASS: Fill rate = {fill_rate:.1f}% (expected 80-95%)")

        # 7. Verify backorder/lost sale split
        backorder_count = conn.execute(
            "SELECT COUNT(*) FROM event_log WHERE event_type='demand_backordered'"
        ).fetchone()[0]
        lost_count = conn.execute(
            "SELECT COUNT(*) FROM event_log WHERE event_type='demand_lost'"
        ).fetchone()[0]
        total_unfulfilled = backorder_count + lost_count
        if total_unfulfilled > 0:
            backorder_ratio = backorder_count / total_unfulfilled
            # Configured at 0.8, so expect roughly 80% backorders
            assert 0.6 < backorder_ratio < 0.95, f"Unexpected backorder ratio: {backorder_ratio:.2f}"
            print(f"PASS: Backorder ratio = {backorder_ratio:.2f} (expected ~0.8)")

        # 8. Verify inventory snapshots
        snapshot_count = conn.execute("SELECT COUNT(*) FROM inventory_snapshot").fetchone()[0]
        assert snapshot_count > 0, "No inventory snapshots written"
        snap_dates = conn.execute(
            "SELECT COUNT(DISTINCT sim_date) FROM inventory_snapshot"
        ).fetchone()[0]
        # With snapshot_interval_days=7 and 91 steps, expect ~13 snapshots
        assert 10 <= snap_dates <= 15, f"Expected 10-15 snapshot dates, got {snap_dates}"
        print(f"PASS: {snapshot_count} snapshot rows across {snap_dates} dates")

        # 9. Verify inbound shipments processed
        arrived = conn.execute(
            "SELECT COUNT(*) FROM event_log WHERE event_type='shipment_arrived'"
        ).fetchone()[0]
        assert arrived == 12, f"Expected 12 inbound arrivals, got {arrived}"
        print("PASS: All 12 inbound shipments arrived")

        # 10. Verify costs recorded
        total_cost = conn.execute(
            "SELECT SUM(cost) FROM event_log WHERE cost IS NOT NULL"
        ).fetchone()[0]
        assert float(total_cost) > 0, "No costs recorded"
        print(f"PASS: Total cost = ${float(total_cost):,.2f}")

        conn.close()
        print("\n=== ALL TESTS PASSED ===")

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


if __name__ == '__main__':
    test_full_pipeline()
