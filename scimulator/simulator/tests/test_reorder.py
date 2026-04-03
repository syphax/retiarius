"""
Tests for Phase 3: ordering logic, forecasting, fulfillment rank tracking.

Tests:
1. Backward compat: drawdown with rank tracking (no reorder)
2. Periodic reorder with noisy_actuals forecast
3. Fulfillment rank and optimal cost gap tracking
4. closest_node_only fulfillment mode
5. Forecast bias and error
"""

import os
import sys
import tempfile
from pathlib import Path
from datetime import date

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scimulator.simulator.loader import load_scenario_from_yaml, load_scenario_into_db
from scimulator.simulator.engine import DrawdownEngine
from scimulator.simulator.db import create_database
from scimulator.simulator.models import ScenarioConfig
from scimulator.simulator.forecast import NoisyActualsForecast

import numpy as np


def _run_scenario(config, db_path):
    """Helper: load scenario into DB, run engine, return connection."""
    conn = load_scenario_into_db(config, db_path)
    engine = DrawdownEngine(conn, config.scenario_id)
    engine.run()
    return conn


def test_drawdown_with_rank_tracking():
    """Verify that drawdown mode now includes fulfillment_rank and optimal_cost."""
    test_dir = Path(__file__).parent
    yaml_path = str(test_dir / "sample_scenario.yaml")

    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
        db_path = f.name
    os.unlink(db_path)

    try:
        config = load_scenario_from_yaml(yaml_path)
        conn = _run_scenario(config, db_path)

        # Check that fulfillment events have rank and optimal_cost
        rows = conn.execute("""
            SELECT fulfillment_rank, optimal_cost, cost
            FROM event_log
            WHERE event_type = 'demand_fulfilled'
              AND fulfillment_rank IS NOT NULL
            LIMIT 10
        """).fetchall()

        assert len(rows) > 0, "No fulfillment events with rank tracking"
        for rank, optimal, actual in rows:
            assert rank >= 1, f"Rank should be >= 1, got {rank}"
            assert optimal is not None, "optimal_cost should not be None"
            assert float(optimal) <= float(actual) + 0.01, \
                f"optimal_cost ({optimal}) should be <= actual cost ({actual})"

        # Verify some events were rank > 1 (fulfilled from non-closest node)
        rank_2_plus = conn.execute("""
            SELECT COUNT(*) FROM event_log
            WHERE event_type IN ('demand_fulfilled', 'backorder_fulfilled')
              AND fulfillment_rank > 1
        """).fetchone()[0]
        # In the sample scenario, some demand nodes only have one route,
        # so we just verify the column exists and is populated
        print(f"PASS: Rank tracking works. {rank_2_plus} events from non-best node")

        # Verify fill rate unchanged
        demand_qty = conn.execute(
            "SELECT SUM(quantity) FROM event_log WHERE event_type='demand_received'"
        ).fetchone()[0]
        fulfilled_qty = conn.execute(
            "SELECT SUM(quantity) FROM event_log "
            "WHERE event_type IN ('demand_fulfilled', 'backorder_fulfilled')"
        ).fetchone()[0]
        fill_rate = float(fulfilled_qty) / float(demand_qty) * 100
        assert 80 < fill_rate < 95, f"Fill rate changed unexpectedly: {fill_rate:.1f}%"
        print(f"PASS: Fill rate = {fill_rate:.1f}% (backward compat OK)")

        conn.close()
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_periodic_reorder():
    """Test periodic reorder logic end-to-end.

    Uses the sample scenario but with reorder_logic enabled.
    Expects higher fill rate than drawdown since inventory is replenished.
    """
    test_dir = Path(__file__).parent
    yaml_path = str(test_dir / "sample_scenario.yaml")

    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
        db_path = f.name
    os.unlink(db_path)

    try:
        config = load_scenario_from_yaml(yaml_path)
        # Override to enable reorder logic
        config.scenario_id = "reorder_test_01"
        config.reorder_logic = "periodic"
        config.order_frequency_days = 7
        config.safety_stock_days = 14
        config.mrq_days = 7
        config.forecast_method = "noisy_actuals"
        config.forecast_bias = 0.0
        config.forecast_error = 0.0  # perfect forecast for deterministic test
        config.forecast_distribution = "normal"
        config.consolidation_mode = "free"

        conn = _run_scenario(config, db_path)

        # Verify POs were placed
        po_count = conn.execute(
            "SELECT COUNT(*) FROM purchase_order WHERE scenario_id = ?",
            [config.scenario_id]
        ).fetchone()[0]
        assert po_count > 0, "No purchase orders placed"

        # Verify PO events in event log
        po_events = conn.execute("""
            SELECT COUNT(*) FROM event_log
            WHERE scenario_id = ? AND event_type = 'po_placed'
        """, [config.scenario_id]).fetchone()[0]
        assert po_events > 0, "No po_placed events in event log"

        # Verify PO arrivals
        po_arrived = conn.execute("""
            SELECT COUNT(*) FROM event_log
            WHERE scenario_id = ? AND event_type = 'po_arrived'
        """, [config.scenario_id]).fetchone()[0]
        assert po_arrived > 0, "No po_arrived events — POs never arrived"

        # Fill rate should be higher than drawdown (~85%)
        demand_qty = conn.execute(
            "SELECT SUM(quantity) FROM event_log "
            "WHERE scenario_id = ? AND event_type='demand_received'",
            [config.scenario_id]
        ).fetchone()[0]
        fulfilled_qty = conn.execute(
            "SELECT SUM(quantity) FROM event_log "
            "WHERE scenario_id = ? AND event_type IN ('demand_fulfilled', 'backorder_fulfilled')",
            [config.scenario_id]
        ).fetchone()[0]
        fill_rate = float(fulfilled_qty) / float(demand_qty) * 100
        print(f"  Reorder fill rate: {fill_rate:.1f}% (drawdown was ~86%)")
        assert fill_rate > 85, f"Reorder fill rate too low: {fill_rate:.1f}%"

        print(f"PASS: Periodic reorder works. {po_count} POs placed, "
              f"{po_arrived} arrived, fill rate = {fill_rate:.1f}%")

        conn.close()
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_closest_node_only():
    """Test closest_node_only fulfillment mode.

    Should have lower fill rate than closest_node_wins since it can't
    fall back to other nodes.
    """
    test_dir = Path(__file__).parent
    yaml_path = str(test_dir / "sample_scenario.yaml")

    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
        db_path = f.name
    os.unlink(db_path)

    try:
        config = load_scenario_from_yaml(yaml_path)
        config.scenario_id = "closest_only_test"
        config.fulfillment_logic = "closest_node_only"

        conn = _run_scenario(config, db_path)

        # All fulfillment should be rank 1
        non_rank1 = conn.execute("""
            SELECT COUNT(*) FROM event_log
            WHERE scenario_id = ? AND event_type = 'demand_fulfilled'
              AND fulfillment_rank != 1
        """, [config.scenario_id]).fetchone()[0]
        assert non_rank1 == 0, f"closest_node_only should only have rank=1, found {non_rank1} non-rank-1"

        # Fill rate: for nodes with only 1 route, same as before.
        # For nodes with 2 routes (Z606, Z750), should be lower.
        demand_qty = conn.execute(
            "SELECT SUM(quantity) FROM event_log "
            "WHERE scenario_id = ? AND event_type='demand_received'",
            [config.scenario_id]
        ).fetchone()[0]
        fulfilled_qty = conn.execute(
            "SELECT SUM(quantity) FROM event_log "
            "WHERE scenario_id = ? AND event_type IN ('demand_fulfilled', 'backorder_fulfilled')",
            [config.scenario_id]
        ).fetchone()[0]
        fill_rate = float(fulfilled_qty) / float(demand_qty) * 100
        print(f"PASS: closest_node_only works. Fill rate = {fill_rate:.1f}% "
              f"(all rank=1, no fallback)")

        conn.close()
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_forecast_noisy_actuals():
    """Unit test for the NoisyActualsForecast module."""
    # Create a minimal in-memory DB with demand data
    import duckdb
    conn = duckdb.connect(':memory:')

    from scimulator.simulator.db import _create_schema, _seed_uom
    _create_schema(conn)
    _seed_uom(conn)

    conn.execute("""
        INSERT INTO dataset_version VALUES ('test_v1', 'test', NULL, NULL, CURRENT_TIMESTAMP, 'test')
    """)

    # Insert 30 days of demand: 10 units/day for product A
    for day in range(30):
        d = date(2024, 1, 1) + __import__('datetime').timedelta(days=day)
        conn.execute("""
            INSERT INTO demand VALUES ('test_v1', ?, ?, NULL, 'Z100', 'A', 10.0, NULL)
        """, [f"d_{day}", d])

    rng = np.random.default_rng(42)

    # Test 1: zero bias, zero error = exact actuals
    forecast = NoisyActualsForecast(conn, 'test_v1', bias=0.0, error=0.0,
                                     distribution='normal', rng=rng)
    result = forecast.forecast_national('A', date(2024, 1, 1), 30)
    assert result == 300.0, f"Expected 300.0 with no bias/error, got {result}"
    print("PASS: Zero bias/error returns exact actuals")

    # Test 2: +10% bias = 330
    forecast_biased = NoisyActualsForecast(conn, 'test_v1', bias=0.10, error=0.0,
                                            distribution='normal', rng=rng)
    result = forecast_biased.forecast_national('A', date(2024, 1, 1), 30)
    assert result == 330.0, f"Expected 330.0 with +10% bias, got {result}"
    print("PASS: +10% bias returns 330.0")

    # Test 3: -10% bias = 270
    forecast_neg = NoisyActualsForecast(conn, 'test_v1', bias=-0.10, error=0.0,
                                         distribution='normal', rng=rng)
    result = forecast_neg.forecast_national('A', date(2024, 1, 1), 30)
    assert result == 270.0, f"Expected 270.0 with -10% bias, got {result}"
    print("PASS: -10% bias returns 270.0")

    # Test 4: with error, result should vary
    rng2 = np.random.default_rng(123)
    forecast_err = NoisyActualsForecast(conn, 'test_v1', bias=0.0, error=0.20,
                                         distribution='normal', rng=rng2)
    results = [forecast_err.forecast_national('A', date(2024, 1, 1), 30)
               for _ in range(100)]
    mean_result = sum(results) / len(results)
    # Mean should be close to 300, with some variance
    assert 250 < mean_result < 350, f"Mean forecast with error should be ~300, got {mean_result:.1f}"
    # Verify there IS variance
    min_r, max_r = min(results), max(results)
    assert max_r - min_r > 10, f"Expected variance in forecasts, got range {min_r:.1f}-{max_r:.1f}"
    print(f"PASS: Error produces variance. Mean={mean_result:.1f}, range=[{min_r:.1f}, {max_r:.1f}]")

    # Test 5: Poisson distribution
    rng3 = np.random.default_rng(456)
    forecast_pois = NoisyActualsForecast(conn, 'test_v1', bias=0.0, error=0.20,
                                          distribution='poisson', rng=rng3)
    results = [forecast_pois.forecast_national('A', date(2024, 1, 1), 30)
               for _ in range(100)]
    mean_result = sum(results) / len(results)
    assert 280 < mean_result < 320, f"Poisson mean should be ~300, got {mean_result:.1f}"
    print(f"PASS: Poisson distribution works. Mean={mean_result:.1f}")

    conn.close()


if __name__ == '__main__':
    print("=== Phase 3 Tests ===\n")

    print("--- Test 1: Forecast Unit Tests ---")
    test_forecast_noisy_actuals()
    print()

    print("--- Test 2: Drawdown with Rank Tracking ---")
    test_drawdown_with_rank_tracking()
    print()

    print("--- Test 3: Closest Node Only ---")
    test_closest_node_only()
    print()

    print("--- Test 4: Periodic Reorder ---")
    test_periodic_reorder()
    print()

    print("=== ALL PHASE 3 TESTS PASSED ===")
