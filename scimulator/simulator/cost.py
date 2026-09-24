"""
Unified cost computation.

Single point of truth for turning a ``(rate, basis)`` pair plus a context
(quantity, weight, volume, distance, value, time) into a dollar amount.
Every cost-event site in the engine calls :func:`compute_cost` rather than
doing its own ``qty * rate`` arithmetic, so basis handling and unit conversion
live in exactly one place.

Canonical units used inside :class:`CostContext`:
    mass     -> kg
    volume   -> L (liters)
    distance -> km

``compute_cost`` derives cubic meters from liters internally (1 m3 = 1000 L).
Callers convert raw measured values to these canonical units with
:class:`UomConverter` before building a context.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# Full basis vocabulary (from scimulator-data-schema.md).
ALL_BASES = frozenset({
    'per_unit', 'per_order', 'per_kg', 'per_L', 'per_m3',
    'per_kg_km', 'per_L_km', 'per_m3_km', 'pct_value', 'per_day', 'fixed',
})

# Bases that require a per-unit product value to resolve. Product value is
# dataset-version-dependent and not yet wired (pending the dataset-versioning
# redesign), so compute_cost returns 0 and warns once when value is missing.
VALUE_BASES = frozenset({'pct_value'})


_warned: set = set()


def warn_once(key: str, msg: str) -> None:
    """Log ``msg`` at WARNING level the first time ``key`` is seen.

    Deduplicated per process so a misconfigured basis doesn't spam the log
    once per event across a multi-thousand-step run.
    """
    if key not in _warned:
        _warned.add(key)
        logger.warning(msg)


@dataclass
class CostContext:
    """Physical/quantity context for a single cost computation.

    All measures are pre-converted to canonical units: weight in kg, volume in
    liters, distance in km. ``qty`` is the number of units being costed;
    ``weight_kg`` / ``volume_l`` are the *totals* for that quantity, not
    per-unit figures.
    """
    qty: int = 0
    weight_kg: float = 0.0
    volume_l: float = 0.0
    distance_km: float = 0.0
    unit_value: Optional[float] = None   # per-unit $ value, for pct_value bases
    days: float = 1.0                    # for per_day bases
    orders: float = 1.0                  # for per_order bases (# of shipments)


def compute_cost(rate, basis: Optional[str], ctx: CostContext) -> float:
    """Return the dollar cost for ``rate`` applied on ``basis`` within ``ctx``.

    Unknown or unsupported bases return 0.0 with a one-time warning rather than
    raising, so a bad config never aborts a simulation run.
    """
    if rate is None:
        return 0.0
    rate = float(rate)
    if rate == 0.0:
        return 0.0

    b = basis or 'per_unit'

    if b == 'per_unit':
        return rate * ctx.qty
    if b == 'per_order':
        return rate * ctx.orders
    if b == 'fixed':
        return rate
    if b == 'per_day':
        return rate * ctx.days
    if b == 'per_kg':
        return rate * ctx.weight_kg
    if b == 'per_L':
        return rate * ctx.volume_l
    if b == 'per_m3':
        return rate * (ctx.volume_l / 1000.0)
    if b == 'per_kg_km':
        return rate * ctx.weight_kg * ctx.distance_km
    if b == 'per_L_km':
        return rate * ctx.volume_l * ctx.distance_km
    if b == 'per_m3_km':
        return rate * (ctx.volume_l / 1000.0) * ctx.distance_km
    if b == 'pct_value':
        if ctx.unit_value is None:
            warn_once(
                'pct_value',
                "compute_cost: 'pct_value' basis requires a per-unit product "
                "value, which is not yet resolved (pending dataset-version "
                "value lookup). Treating as $0 for now.")
            return 0.0
        return rate * ctx.unit_value * ctx.qty

    warn_once(b, f"compute_cost: unknown cost basis '{b}'. Treating as $0.")
    return 0.0


class UomConverter:
    """Converts measured values to their dimension's metric default
    (kg / L / km / days) using the ``uom`` reference table's
    ``conversion_to_default`` factors.
    """

    def __init__(self, conversions: Dict[str, float]):
        # conversions: uom_code -> conversion_to_default
        self._c = conversions

    @classmethod
    def from_conn(cls, conn) -> "UomConverter":
        rows = conn.execute(
            "SELECT uom_code, conversion_to_default FROM uom"
        ).fetchall()
        return cls({code: float(conv) for code, conv in rows})

    def to_default(self, value, uom_code: Optional[str]) -> float:
        """Convert ``value`` (in ``uom_code``) to its dimension's default unit.

        Unknown UoM codes are treated as already-canonical (factor 1.0).
        """
        if value is None:
            return 0.0
        factor = self._c.get(uom_code, 1.0)
        return float(value) * factor


class TransportCostCalculator:
    """Computes the cost of shipping ``qty`` of a product over an edge,
    honoring the edge's ``cost_variable_basis`` plus any per-shipment
    ``cost_fixed``.

    Injected into fulfillment strategies so ranking, gap-analysis optimal
    cost, and the logged event cost all agree on a single basis-aware number.
    """

    def __init__(self, product_dims: Dict[str, tuple],
                 unit_values: Optional[Dict[str, float]] = None):
        # product_dims: product_id -> (weight_kg_per_unit, volume_l_per_unit)
        self._dims = product_dims
        self._values = unit_values or {}

    def edge_cost(self, route: dict, product_id: str, qty: int) -> float:
        """Total transport cost = variable (basis-aware) + per-shipment fixed."""
        w_unit, v_unit = self._dims.get(product_id, (0.0, 0.0))
        ctx = CostContext(
            qty=qty,
            weight_kg=w_unit * qty,
            volume_l=v_unit * qty,
            distance_km=route.get('distance_km', 0.0),
            unit_value=self._values.get(product_id),
            orders=1.0,
        )
        variable = compute_cost(
            route.get('cost_variable'),
            route.get('cost_variable_basis') or 'per_unit',
            ctx)
        # cost_fixed is a flat per-shipment charge (parcel handling, container
        # fee, TL minimum). One fulfillment result == one shipment.
        fixed = compute_cost(route.get('cost_fixed'), 'per_order', ctx)
        return variable + fixed
