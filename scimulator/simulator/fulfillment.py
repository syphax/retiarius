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
    quantity: float
    cost: float
    rank: int           # 1 = best per active policy
    optimal_cost: float  # min outbound cost ignoring inventory


class FulfillmentStrategy:
    """Base class for fulfillment strategies."""

    def __init__(self, routes: Dict[str, List[Dict]],
                 inventory: Dict[Tuple[str, str, str], float]):
        self._routes = routes
        self._inventory = inventory
        self._optimal_costs = self._precompute_optimal_costs()

    def _precompute_optimal_costs(self) -> Dict[str, float]:
        """For each demand node, compute the minimum outbound cost_variable.

        This is the unconstrained optimal — ignoring inventory availability.
        """
        optimal = {}
        for demand_node_id, routes in self._routes.items():
            if routes:
                optimal[demand_node_id] = min(r['cost_variable'] for r in routes)
        return optimal

    def fulfill(self, demand_node_id: str, product_id: str,
                qty: float) -> List[FulfillmentResult]:
        raise NotImplementedError


class ClosestNodeWins(FulfillmentStrategy):
    """Ship from the closest node that has inventory. If it can't fill the
    full quantity, continue to the next closest, and so on."""

    def fulfill(self, demand_node_id: str, product_id: str,
                qty: float) -> List[FulfillmentResult]:
        routes = self._routes.get(demand_node_id, [])
        if not routes:
            return []

        optimal_unit_cost = self._optimal_costs.get(demand_node_id, 0.0)
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

            cost = fill_qty * route['cost_variable']
            optimal = fill_qty * optimal_unit_cost

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
                qty: float) -> List[FulfillmentResult]:
        routes = self._routes.get(demand_node_id, [])
        if not routes:
            return []

        optimal_unit_cost = self._optimal_costs.get(demand_node_id, 0.0)
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

        cost = fill_qty * route['cost_variable']
        optimal = fill_qty * optimal_unit_cost

        return [FulfillmentResult(
            dist_node_id=dist_node_id,
            edge_id=route['edge_id'],
            quantity=fill_qty,
            cost=cost,
            rank=1,
            optimal_cost=optimal,
        )]


def create_strategy(name: str, routes: Dict[str, List[Dict]],
                    inventory: Dict[Tuple[str, str, str], float]) -> FulfillmentStrategy:
    """Factory: create a fulfillment strategy by name."""
    strategies = {
        'closest_node_wins': ClosestNodeWins,
        'closest_node_only': ClosestNodeOnly,
    }
    cls = strategies.get(name)
    if cls is None:
        raise ValueError(f"Unknown fulfillment strategy: {name}. "
                         f"Available: {sorted(strategies.keys())}")
    return cls(routes, inventory)
