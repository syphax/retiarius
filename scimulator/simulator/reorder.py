"""
Reorder logic: periodic ordering, supplier selection, consolidation, and
fair-share node allocation.

All logic is encapsulated here so it can be swapped out for different
strategies in the future.
"""

import uuid
import logging
import math
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

import duckdb

from .forecast import NoisyActualsForecast

logger = logging.getLogger(__name__)

# Inventory states that count toward inventory position
IP_STATES = {'in_transit', 'received', 'saleable', 'committed'}


@dataclass
class PurchaseOrder:
    """A purchase order line placed with a supplier."""
    po_id: str
    sim_date: date
    supply_node_id: str
    dest_node_id: str
    product_id: str
    quantity: float
    expected_arrival: date
    cube: float
    status: str = 'pending'  # pending, consolidating, in_transit, received
    actual_arrival: Optional[date] = None


@dataclass
class SupplierRoute:
    """Routing info for ordering a product from a supplier."""
    supply_node_id: str
    supplier_id: str
    supplier_lead_time: float   # days
    transit_time: float         # days, supplier -> first DC
    edge_id: str                # supply -> distribution edge
    dest_node_id: str           # first distribution node


class PeriodicReorderPolicy:
    """Periodic reorder: evaluate every R days, order if D > IP.

    National scope: compute total network demand and inventory,
    then allocate to nodes via fair share.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection,
                 scenario_id: str,
                 forecast: NoisyActualsForecast,
                 fulfillment_routes: Dict[str, List[Dict]],
                 order_frequency_days: int,
                 safety_stock_days: int,
                 mrq_days: int,
                 consolidation_mode: str,
                 min_cube_threshold: float,
                 start_date: date,
                 end_date: date,
                 demand_version_id: str,
                 product_set_id: str = None,
                 distribution_node_set_id: str = None):
        self.conn = conn
        self.scenario_id = scenario_id
        self.forecast = forecast
        self.order_frequency_days = order_frequency_days
        self.safety_stock_days = safety_stock_days
        self.mrq_days = mrq_days
        self.consolidation_mode = consolidation_mode
        self.min_cube_threshold = min_cube_threshold
        self.start_date = start_date
        self.end_date = end_date
        self.demand_version_id = demand_version_id

        # Build supplier routing: product_id -> SupplierRoute
        self._supplier_routes: Dict[str, SupplierRoute] = {}
        self._build_supplier_routes(product_set_id, distribution_node_set_id)

        # Build fair-share allocator
        self._allocator = FairShareAllocator(
            conn, fulfillment_routes, demand_version_id,
            start_date, end_date, forecast)

        # Product cubes for consolidation
        self._product_cubes: Dict[str, float] = {}
        rows = conn.execute("SELECT product_id, cube FROM product").fetchall()
        for pid, cube in rows:
            self._product_cubes[pid] = float(cube)

        # Held orders awaiting consolidation: supply_node_id -> [PurchaseOrder]
        self._held_orders: Dict[str, List[PurchaseOrder]] = defaultdict(list)

        # Active distribution nodes
        self._dist_nodes: List[str] = []
        query = "SELECT dist_node_id FROM distribution_node"
        params = []
        if distribution_node_set_id:
            query += """
                WHERE dist_node_id IN (
                    SELECT dist_node_id FROM distribution_node_set_member
                    WHERE distribution_node_set_id = ?
                )
            """
            params.append(distribution_node_set_id)
        self._dist_nodes = [r[0] for r in conn.execute(query, params).fetchall()]

    def _build_supplier_routes(self, product_set_id, distribution_node_set_id):
        """Build product -> preferred supplier routing.

        For each product, find the first supply node that carries it,
        look up the supplier lead time, and find the shortest
        supply->distribution edge for transit time.
        """
        # Get all products
        query = "SELECT product_id FROM product"
        params = []
        if product_set_id:
            query += """
                WHERE product_id IN (
                    SELECT product_id FROM product_set_member
                    WHERE product_set_id = ?
                )
            """
            params.append(product_set_id)
        products = [r[0] for r in self.conn.execute(query, params).fetchall()]

        # Get supply node -> product mappings
        snp_rows = self.conn.execute(
            "SELECT supply_node_id, product_id FROM supply_node_product"
        ).fetchall()
        # If a supply node has entries in supply_node_product, it only supplies those products.
        # If no entries, it supplies all products.
        supply_node_products: Dict[str, set] = defaultdict(set)
        supply_nodes_with_constraints = set()
        for sn_id, pid in snp_rows:
            supply_node_products[sn_id].add(pid)
            supply_nodes_with_constraints.add(sn_id)

        # Get supply nodes with their supplier info
        sn_rows = self.conn.execute("""
            SELECT sn.supply_node_id, sn.supplier_id,
                   COALESCE(sn.lead_time, s.default_lead_time) as lead_time
            FROM supply_node sn
            JOIN supplier s ON sn.supplier_id = s.supplier_id
        """).fetchall()
        supply_nodes = {r[0]: {'supplier_id': r[1], 'lead_time': float(r[2])}
                        for r in sn_rows}

        # Get supply -> distribution edges (shortest transit per supply node)
        edge_query = """
            SELECT edge_id, origin_node_id, dest_node_id, mean_transit_time
            FROM edge
            WHERE origin_node_type = 'supply' AND dest_node_type = 'distribution'
        """
        edge_params = []
        if distribution_node_set_id:
            edge_query += """
                AND dest_node_id IN (
                    SELECT dist_node_id FROM distribution_node_set_member
                    WHERE distribution_node_set_id = ?
                )
            """
            edge_params.append(distribution_node_set_id)
        edge_query += " ORDER BY mean_transit_time ASC"
        edge_rows = self.conn.execute(edge_query, edge_params).fetchall()

        # Best edge per supply node (shortest transit)
        best_edge: Dict[str, tuple] = {}  # supply_node_id -> (edge_id, dest_node_id, transit_time)
        for edge_id, origin_id, dest_id, transit_time in edge_rows:
            if origin_id not in best_edge:
                best_edge[origin_id] = (edge_id, dest_id, float(transit_time))

        # For each product, find preferred supplier
        for product_id in products:
            for sn_id, sn_info in supply_nodes.items():
                # Check if this supply node carries this product
                if sn_id in supply_nodes_with_constraints:
                    if product_id not in supply_node_products[sn_id]:
                        continue

                edge_info = best_edge.get(sn_id)
                if not edge_info:
                    continue

                edge_id, dest_node_id, transit_time = edge_info
                self._supplier_routes[product_id] = SupplierRoute(
                    supply_node_id=sn_id,
                    supplier_id=sn_info['supplier_id'],
                    supplier_lead_time=sn_info['lead_time'],
                    transit_time=transit_time,
                    edge_id=edge_id,
                    dest_node_id=dest_node_id,
                )
                break  # first match = preferred supplier

        logger.info(f"Built supplier routes for {len(self._supplier_routes)} products")

    def should_reorder(self, sim_date: date) -> bool:
        """True if today is a reorder day."""
        days_elapsed = (sim_date - self.start_date).days
        return days_elapsed % self.order_frequency_days == 0

    def compute_orders(self, sim_date: date,
                       inventory: Dict[Tuple[str, str, str], float],
                       pending_pos: List[PurchaseOrder]) -> List[PurchaseOrder]:
        """Compute purchase orders for all products that need reordering.

        Returns new POs (not yet consolidated).
        """
        new_orders = []

        for product_id, route in self._supplier_routes.items():
            # N = supplier lead time + transit time
            n_days = route.supplier_lead_time + route.transit_time
            r_days = self.order_frequency_days
            s_days = self.safety_stock_days

            # Forecast horizon = N + R + S days
            horizon = int(math.ceil(n_days + r_days + s_days))
            d = self.forecast.forecast_national(product_id, sim_date, horizon)

            # Compute inventory position (IP) across all nodes + pipeline
            ip = 0.0
            for (node_id, pid, state), qty in inventory.items():
                if pid == product_id and state in IP_STATES:
                    ip += qty

            # Add pending PO quantities (in_transit or consolidating)
            for po in pending_pos:
                if po.product_id == product_id and po.status in ('in_transit', 'consolidating'):
                    ip += po.quantity

            if d <= ip:
                continue

            # Order quantity = max(MRQ, D - IP)
            order_qty = d - ip
            if horizon > 0:
                daily_rate = d / horizon
                mrq = daily_rate * self.mrq_days
            else:
                mrq = 0
            order_qty = max(mrq, order_qty)

            if order_qty <= 0:
                continue

            # Expected arrival = sim_date + N
            expected_arrival = sim_date + timedelta(days=int(math.ceil(n_days)))

            # Compute cube
            cube_per_unit = self._product_cubes.get(product_id, 1.0)
            total_cube = order_qty * cube_per_unit

            # Allocate to nodes
            allocations = self._allocator.allocate(
                product_id, order_qty, inventory, expected_arrival)

            for dest_node_id, alloc_qty in allocations.items():
                if alloc_qty <= 0:
                    continue

                po = PurchaseOrder(
                    po_id=str(uuid.uuid4())[:12],
                    sim_date=sim_date,
                    supply_node_id=route.supply_node_id,
                    dest_node_id=dest_node_id,
                    product_id=product_id,
                    quantity=alloc_qty,
                    expected_arrival=expected_arrival,
                    cube=alloc_qty * cube_per_unit,
                )
                new_orders.append(po)

        return new_orders

    def apply_consolidation(self, new_orders: List[PurchaseOrder]) -> Tuple[
            List[PurchaseOrder], List[PurchaseOrder]]:
        """Apply consolidation logic to new orders.

        Returns: (orders_to_ship, orders_still_held)
        - 'free' mode: all orders ship immediately
        - 'constrained' mode: group by supplier, hold until cumulative
          cube >= min_cube_threshold, then release all held orders for that supplier
        """
        if self.consolidation_mode == 'free':
            for po in new_orders:
                po.status = 'in_transit'
            return new_orders, []

        # Constrained mode
        for po in new_orders:
            po.status = 'consolidating'
            self._held_orders[po.supply_node_id].append(po)

        orders_to_ship = []
        orders_still_held = []

        for supply_node_id, held in self._held_orders.items():
            total_cube = sum(po.cube for po in held)
            if total_cube >= self.min_cube_threshold:
                for po in held:
                    po.status = 'in_transit'
                orders_to_ship.extend(held)
                self._held_orders[supply_node_id] = []
            else:
                orders_still_held.extend(held)

        return orders_to_ship, orders_still_held


class FairShareAllocator:
    """Allocate order quantities to nodes to equalize days-of-supply at arrival.

    Demand fractions are computed once at init using closest-node-wins logic
    on historical or forward demand data.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection,
                 fulfillment_routes: Dict[str, List[Dict]],
                 demand_version_id: str,
                 start_date: date, end_date: date,
                 forecast: NoisyActualsForecast):
        self.forecast = forecast
        self._fulfillment_routes = fulfillment_routes

        # Compute demand fractions: {product_id: {dist_node_id: fraction}}
        self._demand_fractions: Dict[str, Dict[str, float]] = {}
        self._build_demand_fractions(conn, demand_version_id, start_date, end_date)

    def _build_demand_fractions(self, conn, demand_version_id, start_date, end_date):
        """Compute what fraction of each product's demand each dist node should serve.

        Uses closest-node-wins logic: for each demand line, attribute it to the
        closest dist node. Prefer historical (pre-start) data if available.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)

        # Check for historical demand (before start_date)
        hist_count = conn.execute("""
            SELECT COUNT(*) FROM demand
            WHERE dataset_version_id = ? AND demand_date < ?
        """, [demand_version_id, start_date]).fetchone()[0]

        if hist_count > 0:
            # Use historical demand
            demand_rows = conn.execute("""
                SELECT demand_node_id, product_id, SUM(quantity) as qty
                FROM demand
                WHERE dataset_version_id = ? AND demand_date < ?
                GROUP BY demand_node_id, product_id
            """, [demand_version_id, start_date]).fetchall()
        else:
            # Fall back to forward demand
            demand_rows = conn.execute("""
                SELECT demand_node_id, product_id, SUM(quantity) as qty
                FROM demand
                WHERE dataset_version_id = ?
                  AND demand_date >= ? AND demand_date <= ?
                GROUP BY demand_node_id, product_id
            """, [demand_version_id, start_date, end_date]).fetchall()

        # For each demand row, assign to closest dist node
        # product_id -> {dist_node_id -> qty}
        node_demand: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))

        for demand_node_id, product_id, qty in demand_rows:
            qty = float(qty)
            routes = self._fulfillment_routes.get(demand_node_id, [])
            if routes:
                # First route = closest node
                closest = routes[0]['dist_node_id']
                node_demand[product_id][closest] += qty

        # Convert to fractions
        for product_id, node_qtys in node_demand.items():
            total = sum(node_qtys.values())
            if total > 0:
                self._demand_fractions[product_id] = {
                    node_id: qty / total for node_id, qty in node_qtys.items()
                }

        logger.info(f"Computed demand fractions for {len(self._demand_fractions)} products")

    def allocate(self, product_id: str, order_qty: float,
                 inventory: Dict[Tuple[str, str, str], float],
                 expected_arrival: date) -> Dict[str, float]:
        """Allocate order_qty across nodes to equalize days-of-supply at arrival.

        1. For each node, estimate inventory at arrival (current - forecasted demand)
        2. Compute target DoS = (total_inv + order_qty) / total_daily_demand
        3. Each node gets: max(0, target_dos * node_daily_demand - node_inv_at_arrival)
        4. Normalize so allocations sum to order_qty
        """
        fractions = self._demand_fractions.get(product_id, {})
        if not fractions:
            # No demand data: can't allocate. Return empty.
            return {}

        daily_rate = self.forecast.get_daily_demand_rate(product_id)
        if daily_rate <= 0:
            # Equal split
            n = len(fractions)
            return {node_id: order_qty / n for node_id in fractions} if n > 0 else {}

        # Node-level daily demand rates
        node_rates = {node_id: frac * daily_rate for node_id, frac in fractions.items()}

        # Current inventory per node (saleable + received states, approximation)
        node_inv = {}
        for node_id in fractions:
            inv = 0.0
            for state in ('saleable', 'received', 'in_transit', 'committed'):
                inv += inventory.get((node_id, product_id, state), 0)
            node_inv[node_id] = inv

        total_inv = sum(node_inv.values())
        total_rate = sum(node_rates.values())

        if total_rate <= 0:
            n = len(fractions)
            return {node_id: order_qty / n for node_id in fractions}

        # Target days of supply after order arrives
        target_dos = (total_inv + order_qty) / total_rate

        # Allocation: fill each node to target DoS
        raw_alloc = {}
        for node_id in fractions:
            needed = target_dos * node_rates[node_id] - node_inv[node_id]
            raw_alloc[node_id] = max(0, needed)

        # Normalize to sum to order_qty
        total_raw = sum(raw_alloc.values())
        if total_raw <= 0:
            # All nodes already above target; distribute proportionally to demand
            return {node_id: order_qty * frac for node_id, frac in fractions.items()}

        scale = order_qty / total_raw
        return {node_id: qty * scale for node_id, qty in raw_alloc.items()}


def create_reorder_policy(name: str, **kwargs):
    """Factory: create a reorder policy by name."""
    if name == 'periodic':
        return PeriodicReorderPolicy(**kwargs)
    raise ValueError(f"Unknown reorder policy: {name}")
