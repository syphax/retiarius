"""
Fulfillment strategies for order routing.

Each strategy decides which distribution node(s) fulfill a demand line,
and returns results with rank tracking and optimal cost for gap analysis.
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional


@dataclass
class FulfillmentResult:
    """One fulfillment action (a portion of demand filled from one node)."""
    dist_node_id: str
    edge_id: str
    quantity: int
    cost: float
    rank: int           # 1 = best per active policy
    optimal_cost: float  # min outbound cost ignoring inventory


class FulfillmentStrategy:
    """Base class for fulfillment strategies."""

    def __init__(self, routes: Dict[str, List[Dict]],
                 inventory: Dict[Tuple[str, str, str], int],
                 cost_calculator=None):
        self._routes = routes
        self._inventory = inventory
        # Optional TransportCostCalculator. When present, shipment cost honors
        # each edge's cost_variable_basis plus per-shipment cost_fixed. When
        # absent, falls back to the legacy per-unit proxy (qty * cost_variable).
        self._cost_calc = cost_calculator

    def _route_cost(self, route: Dict, product_id: str, fill_qty: int) -> float:
        """Cost of shipping ``fill_qty`` over ``route`` for ``product_id``."""
        if self._cost_calc is not None:
            return self._cost_calc.edge_cost(route, product_id, fill_qty)
        return fill_qty * route['cost_variable']

    def _optimal_cost(self, demand_node_id: str, product_id: str,
                      fill_qty: int) -> float:
        """Minimum cost to ship ``fill_qty`` ignoring inventory availability.

        This is the unconstrained optimal used for gap analysis.
        """
        routes = self._routes.get(demand_node_id, [])
        if not routes:
            return 0.0
        return min(self._route_cost(r, product_id, fill_qty) for r in routes)

    def fulfill(self, demand_node_id: str, product_id: str,
                qty: int) -> List[FulfillmentResult]:
        raise NotImplementedError


class ClosestNodeWins(FulfillmentStrategy):
    """Ship from the closest node that has inventory. If it can't fill the
    full quantity, continue to the next closest, and so on."""

    def fulfill(self, demand_node_id: str, product_id: str,
                qty: int) -> List[FulfillmentResult]:
        routes = self._routes.get(demand_node_id, [])
        if not routes:
            return []

        results = []
        remaining = qty

        for rank, route in enumerate(routes, start=1):
            if remaining <= 0:
                break

            dist_node_id = route['dist_node_id']
            saleable_key = (dist_node_id, product_id, 'saleable')
            available = self._inventory.get(saleable_key, 0)
            if available <= 0:
                continue

            fill_qty = min(remaining, available)

            # Deduct inventory
            self._inventory[saleable_key] -= fill_qty
            shipped_key = (dist_node_id, product_id, 'shipped')
            self._inventory[shipped_key] = self._inventory.get(shipped_key, 0) + fill_qty

            cost = self._route_cost(route, product_id, fill_qty)
            optimal = self._optimal_cost(demand_node_id, product_id, fill_qty)

            results.append(FulfillmentResult(
                dist_node_id=dist_node_id,
                edge_id=route['edge_id'],
                quantity=fill_qty,
                cost=cost,
                rank=rank,
                optimal_cost=optimal,
            ))
            remaining -= fill_qty

        return results


class ClosestNodeOnly(FulfillmentStrategy):
    """Ship only from the closest (best) node. If it doesn't have inventory,
    the demand is unfulfilled (backorder or lost sale)."""

    def fulfill(self, demand_node_id: str, product_id: str,
                qty: int) -> List[FulfillmentResult]:
        routes = self._routes.get(demand_node_id, [])
        if not routes:
            return []

        route = routes[0]  # best route only
        dist_node_id = route['dist_node_id']

        saleable_key = (dist_node_id, product_id, 'saleable')
        available = self._inventory.get(saleable_key, 0)
        if available <= 0:
            return []

        fill_qty = min(qty, available)

        # Deduct inventory
        self._inventory[saleable_key] -= fill_qty
        shipped_key = (dist_node_id, product_id, 'shipped')
        self._inventory[shipped_key] = self._inventory.get(shipped_key, 0) + fill_qty

        cost = self._route_cost(route, product_id, fill_qty)
        optimal = self._optimal_cost(demand_node_id, product_id, fill_qty)

        return [FulfillmentResult(
            dist_node_id=dist_node_id,
            edge_id=route['edge_id'],
            quantity=fill_qty,
            cost=cost,
            rank=1,
            optimal_cost=optimal,
        )]


def create_strategy(name: str, routes: Dict[str, List[Dict]],
                    inventory: Dict[Tuple[str, str, str], int],
                    cost_calculator=None) -> FulfillmentStrategy:
    """Factory: create a fulfillment strategy by name."""
    strategies = {
        'closest_node_wins': ClosestNodeWins,
        'closest_node_only': ClosestNodeOnly,
    }
    cls = strategies.get(name)
    if cls is None:
        raise ValueError(f"Unknown fulfillment strategy: {name}. "
                         f"Available: {sorted(strategies.keys())}")
    return cls(routes, inventory, cost_calculator=cost_calculator)
