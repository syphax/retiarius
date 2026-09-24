"""
Unit tests for the unified cost helper (scimulator/simulator/cost.py).

Verifies each cost basis produces the hand-computed dollar amount, that
value-dependent bases degrade gracefully until value resolution lands, and
that the UoM converter and transport cost calculator behave correctly.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scimulator.simulator.cost import (
    compute_cost, CostContext, UomConverter, TransportCostCalculator)


def approx(a, b, tol=1e-9):
    return abs(a - b) < tol


def test_basis_math():
    """Every non-value basis resolves to a hand-computed amount."""
    ctx = CostContext(
        qty=5, weight_kg=10.0, volume_l=2000.0, distance_km=100.0,
        days=1.0, orders=1.0,
    )
    cases = {
        'per_unit':   2.0 * 5,                       # rate * qty
        'per_order':  2.0 * 1,                       # rate * orders
        'fixed':      2.0,                           # flat
        'per_day':    2.0 * 1,                       # rate * days
        'per_kg':     2.0 * 10.0,                    # rate * weight_kg
        'per_L':      2.0 * 2000.0,                  # rate * volume_l
        'per_m3':     2.0 * (2000.0 / 1000.0),       # rate * m3
        'per_kg_km':  2.0 * 10.0 * 100.0,            # rate * kg * km
        'per_L_km':   2.0 * 2000.0 * 100.0,          # rate * L * km
        'per_m3_km':  2.0 * (2000.0 / 1000.0) * 100.0,
    }
    for basis, expected in cases.items():
        got = compute_cost(2.0, basis, ctx)
        assert approx(got, expected), f"{basis}: got {got}, expected {expected}"
    print("PASS: all non-value bases compute correctly")


def test_pct_value():
    """pct_value needs a per-unit value; degrades to 0 without one."""
    # No value available -> 0 (warns once, pending value resolution)
    got = compute_cost(0.02, 'pct_value', CostContext(qty=3, unit_value=None))
    assert got == 0.0, f"expected 0 without value, got {got}"

    # With value: rate * unit_value * qty
    got = compute_cost(0.02, 'pct_value', CostContext(qty=3, unit_value=10.0))
    assert approx(got, 0.02 * 10.0 * 3), f"pct_value with value: got {got}"
    print("PASS: pct_value degrades without value and computes with it")


def test_edge_cases():
    """None/zero rates and unknown bases never raise, always return 0."""
    ctx = CostContext(qty=5)
    assert compute_cost(None, 'per_unit', ctx) == 0.0
    assert compute_cost(0.0, 'per_unit', ctx) == 0.0
    assert compute_cost(2.0, 'bogus_basis', ctx) == 0.0
    assert compute_cost(2.0, None, CostContext(qty=4)) == 2.0 * 4  # defaults per_unit
    print("PASS: edge cases handled without raising")


def test_uom_converter():
    """conversion_to_default factors map measured values to canonical units."""
    conv = UomConverter({'kg': 1.0, 'lb': 0.45359237, 'L': 1.0, 'm3': 1000.0})
    assert approx(conv.to_default(1, 'm3'), 1000.0)      # m3 -> liters
    assert approx(conv.to_default(2, 'lb'), 0.90718474)  # lb -> kg
    assert conv.to_default(None, 'kg') == 0.0            # missing -> 0
    assert approx(conv.to_default(5, 'unknown'), 5.0)    # unknown -> factor 1
    print("PASS: UoM converter maps to canonical units")


def test_transport_calculator():
    """Transport cost = basis-aware variable + flat per-shipment fixed."""
    dims = {'P1': (2.0, 3.0)}  # 2 kg, 3 L per unit
    calc = TransportCostCalculator(dims)

    # per_unit variable + fixed per shipment
    route = {'cost_variable': 0.10, 'cost_variable_basis': 'per_unit',
             'cost_fixed': 500.0, 'distance_km': 100.0}
    got = calc.edge_cost(route, 'P1', 4)
    assert approx(got, 0.10 * 4 + 500.0), f"per_unit+fixed: got {got}"

    # per_kg_km variable, no fixed: rate * (kg*qty) * km
    route = {'cost_variable': 0.01, 'cost_variable_basis': 'per_kg_km',
             'cost_fixed': 0.0, 'distance_km': 100.0}
    got = calc.edge_cost(route, 'P1', 4)
    assert approx(got, 0.01 * (2.0 * 4) * 100.0), f"per_kg_km: got {got}"

    # Unknown product falls back to zero dims -> only fixed applies
    route = {'cost_variable': 0.10, 'cost_variable_basis': 'per_unit',
             'cost_fixed': 25.0, 'distance_km': 0.0}
    got = calc.edge_cost(route, 'MISSING', 4)
    assert approx(got, 0.10 * 4 + 25.0), f"missing dims: got {got}"
    print("PASS: transport calculator combines variable + fixed correctly")


if __name__ == '__main__':
    test_basis_math()
    test_pct_value()
    test_edge_cases()
    test_uom_converter()
    test_transport_calculator()
    print("\n=== ALL COST TESTS PASSED ===")
