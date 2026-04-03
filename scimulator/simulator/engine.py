"""
Time-stepped simulation engine.

Supports both drawdown-only (Phase 1/2) and active ordering (Phase 3+).
Fulfillment uses pluggable strategies. Reorder logic is optional.

The engine is stateless — all state lives in DuckDB.
"""

import json
import logging
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional

import duckdb
import numpy as np
import polars as pl

from . import __version__
from .fulfillment import create_strategy
from .forecast import create_forecast
from .reorder import create_reorder_policy, PurchaseOrder

logger = logging.getLogger(__name__)


# Valid inventory state transitions
VALID_TRANSITIONS = {
    'in_transit': {'received'},
    'received': {'saleable', 'damaged'},
    'saleable': {'committed', 'in_transit', 'damaged'},
    'committed': {'shipped'},
    'damaged': {'disposed', 'saleable'},
}
TERMINAL_STATES = {'shipped', 'disposed'}


class DrawdownEngine:
    """Time-stepped simulation engine.

    Processes all events for time step T before advancing to T+1.
    When reorder_logic is None (default), operates in drawdown mode:
    pre-loaded inventory consumed by demand, no ordering decisions.
    When reorder_logic is set, forecast and ordering modules are activated.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, scenario_id: str):
        self.conn = conn
        self.scenario_id = scenario_id
        self._event_counter = 0

        # Load scenario config
        row = conn.execute(
            "SELECT * FROM scenario WHERE scenario_id = ?", [scenario_id]
        ).fetchone()
        if not row:
            raise ValueError(f"Scenario not found: {scenario_id}")

        cols = [desc[0] for desc in conn.description]
        self.scenario = dict(zip(cols, row))

        self.dataset_version_id = self.scenario['dataset_version_id']
        self.demand_version_id = self.scenario.get('demand_version_id') or self.dataset_version_id
        self.inbound_version_id = self.scenario.get('inbound_version_id') or self.dataset_version_id
        self.inventory_version_id = self.scenario.get('inventory_version_id') or self.dataset_version_id
        self.start_date = self.scenario['start_date']
        self.end_date = self.scenario['end_date']
        self.backorder_prob = float(self.scenario['backorder_probability'])
        self.write_event_log = self.scenario['write_event_log']
        self.write_snapshots = self.scenario['write_snapshots']
        self.snapshot_interval = self.scenario['snapshot_interval_days']

        # Entity set filters (None = use all)
        self.product_set_id = self.scenario.get('product_set_id')
        self.supply_node_set_id = self.scenario.get('supply_node_set_id')
        self.distribution_node_set_id = self.scenario.get('distribution_node_set_id')
        self.demand_node_set_id = self.scenario.get('demand_node_set_id')
        self.edge_set_id = self.scenario.get('edge_set_id')

        # Phase 3 config
        self.fulfillment_logic = self.scenario.get('fulfillment_logic') or 'closest_node_wins'
        self.reorder_logic = self.scenario.get('reorder_logic')

        # RNG for stochastic elements (backorder/lost-sale coin flip, forecast noise)
        self.rng = np.random.default_rng(42)

        # In-memory inventory state: {(dist_node_id, product_id, state): quantity}
        self._inventory: Dict[Tuple[str, str, str], float] = {}

        # Backorder queue: [(demand_id, demand_node_id, product_id, quantity, sim_date)]
        self._backorders: List[Tuple] = []

        # Buffered event log rows for batch insert
        self._event_buffer: List[Tuple] = []
        self._snapshot_buffer: List[Tuple] = []

        # Pre-load edge routing info for fulfillment
        self._fulfillment_routes: Dict[str, List[Dict]] = {}

        # Zone lookup: edge_id -> zone (populated from zone_table for zone-derived edges)
        self._edge_zones: Dict[str, str] = {}

        # Purchase orders (Phase 3)
        self._purchase_orders: List[PurchaseOrder] = []

        # Strategy and policy objects (initialized in run())
        self._fulfillment_strategy = None
        self._reorder_policy = None

    def run(self):
        """Execute the full simulation."""
        start_time = time.time()
        logger.info(f"Starting simulation: {self.scenario_id}")
        logger.info(f"  Period: {self.start_date} to {self.end_date}")
        if self.reorder_logic:
            logger.info(f"  Reorder logic: {self.reorder_logic}")
        logger.info(f"  Fulfillment logic: {self.fulfillment_logic}")

        # Record run start
        self.conn.execute("""
            INSERT OR REPLACE INTO run_metadata
            (scenario_id, run_started_at, status, engine_version, config_snapshot)
            VALUES (?, ?, 'running', ?, ?)
        """, [
            self.scenario_id, datetime.now(), __version__,
            json.dumps({k: str(v) for k, v in self.scenario.items()}),
        ])

        try:
            self._initialize_inventory()
            self._build_fulfillment_routes()
            self._initialize_strategies()
            self._run_time_steps()

            elapsed = time.time() - start_time
            total_steps = self._compute_total_steps()

            self.conn.execute("""
                UPDATE run_metadata
                SET run_completed_at = ?, status = 'completed',
                    total_steps = ?, wall_clock_seconds = ?
                WHERE scenario_id = ?
            """, [datetime.now(), total_steps, round(elapsed, 2), self.scenario_id])

            logger.info(f"Simulation completed in {elapsed:.2f}s ({total_steps} steps)")

        except Exception as e:
            elapsed = time.time() - start_time
            self.conn.execute("""
                UPDATE run_metadata
                SET run_completed_at = ?, status = 'failed',
                    wall_clock_seconds = ?, error_message = ?
                WHERE scenario_id = ?
            """, [datetime.now(), round(elapsed, 2), str(e), self.scenario_id])
            logger.error(f"Simulation failed: {e}")
            raise

    def _initialize_strategies(self):
        """Initialize fulfillment strategy and (optionally) reorder policy."""
        # Fulfillment strategy
        self._fulfillment_strategy = create_strategy(
            self.fulfillment_logic, self._fulfillment_routes, self._inventory)

        # Reorder logic (only if configured)
        if not self.reorder_logic:
            return

        start = self.start_date
        if isinstance(start, str):
            start = date.fromisoformat(start)
        end = self.end_date
        if isinstance(end, str):
            end = date.fromisoformat(end)

        forecast = create_forecast(
            method=self.scenario.get('forecast_method') or 'noisy_actuals',
            conn=self.conn,
            demand_version_id=self.demand_version_id,
            bias=float(self.scenario.get('forecast_bias') or 0),
            error=float(self.scenario.get('forecast_error') or 0),
            distribution=self.scenario.get('forecast_distribution') or 'normal',
            rng=self.rng,
            product_set_id=self.product_set_id,
            demand_node_set_id=self.demand_node_set_id,
        )

        self._reorder_policy = create_reorder_policy(
            name=self.reorder_logic,
            conn=self.conn,
            scenario_id=self.scenario_id,
            forecast=forecast,
            fulfillment_routes=self._fulfillment_routes,
            order_frequency_days=int(self.scenario.get('order_frequency_days') or 7),
            safety_stock_days=int(self.scenario.get('safety_stock_days') or 14),
            mrq_days=int(self.scenario.get('mrq_days') or 14),
            consolidation_mode=self.scenario.get('consolidation_mode') or 'free',
            min_cube_threshold=float(self.scenario.get('min_cube_threshold') or 0),
            start_date=start,
            end_date=end,
            demand_version_id=self.demand_version_id,
            product_set_id=self.product_set_id,
            distribution_node_set_id=self.distribution_node_set_id,
        )

    def _compute_total_steps(self) -> int:
        d = self.start_date
        if isinstance(d, str):
            d = date.fromisoformat(d)
        e = self.end_date
        if isinstance(e, str):
            e = date.fromisoformat(e)
        return (e - d).days + 1

    def _initialize_inventory(self):
        """Load initial inventory from the database into memory.

        Filters by active distribution_node_set and product_set if specified.
        """
        query = """
            SELECT i.dist_node_id, i.product_id, i.inventory_state, i.quantity
            FROM initial_inventory i
            WHERE i.dataset_version_id = ?
        """
        params = [self.inventory_version_id]

        if self.distribution_node_set_id:
            query += """
                AND i.dist_node_id IN (
                    SELECT dist_node_id FROM distribution_node_set_member
                    WHERE distribution_node_set_id = ?
                )
            """
            params.append(self.distribution_node_set_id)

        if self.product_set_id:
            query += """
                AND i.product_id IN (
                    SELECT product_id FROM product_set_member
                    WHERE product_set_id = ?
                )
            """
            params.append(self.product_set_id)

        rows = self.conn.execute(query, params).fetchall()

        for dist_node_id, product_id, state, qty in rows:
            key = (dist_node_id, product_id, state)
            self._inventory[key] = float(qty)

        total_units = sum(self._inventory.values())
        logger.info(f"Initialized inventory: {len(rows)} positions, {total_units:.0f} total units")

    def _build_fulfillment_routes(self):
        """Build routing tables: for each demand node, ranked list of fulfillment options.

        Each route is a dict with edge attributes (distance, cost, zone, etc.)
        so fulfillment logic can use any combination for ranking.

        Current ranking: shortest haversine distance first.
        """
        query = """
            SELECT e.edge_id, e.origin_node_id, e.dest_node_id,
                   e.cost_variable, e.distance, e.mean_transit_time,
                   ezm.zone
            FROM edge e
            LEFT JOIN edge_zone_map ezm ON e.edge_id = ezm.edge_id
            WHERE e.dest_node_type = 'demand' AND e.origin_node_type = 'distribution'
        """
        params = []

        if self.edge_set_id:
            query += """
                AND e.edge_id IN (
                    SELECT edge_id FROM edge_set_member WHERE edge_set_id = ?
                )
            """
            params.append(self.edge_set_id)

        if self.distribution_node_set_id:
            query += """
                AND e.origin_node_id IN (
                    SELECT dist_node_id FROM distribution_node_set_member
                    WHERE distribution_node_set_id = ?
                )
            """
            params.append(self.distribution_node_set_id)

        if self.demand_node_set_id:
            query += """
                AND e.dest_node_id IN (
                    SELECT demand_node_id FROM demand_node_set_member
                    WHERE demand_node_set_id = ?
                )
            """
            params.append(self.demand_node_set_id)

        rows = self.conn.execute(query, params).fetchall()

        for edge_id, origin_id, dest_id, cost_var, distance, mean_tt, zone in rows:
            route = {
                'dist_node_id': origin_id,
                'edge_id': edge_id,
                'cost_variable': float(cost_var or 0),
                'distance': float(distance) if distance is not None else float('inf'),
                'mean_transit_time': float(mean_tt) if mean_tt is not None else None,
                'zone': str(zone) if zone is not None else None,
            }
            if dest_id not in self._fulfillment_routes:
                self._fulfillment_routes[dest_id] = []
            self._fulfillment_routes[dest_id].append(route)

        # Rank routes — currently: nearest first
        for dest_id in self._fulfillment_routes:
            self._fulfillment_routes[dest_id].sort(key=self._route_sort_key)

        # Pre-load zone data for zone-derived edges
        zone_rows = self.conn.execute("""
            SELECT edge_id, zone FROM edge_zone_map WHERE zone IS NOT NULL
        """).fetchall()
        for edge_id, zone in zone_rows:
            self._edge_zones[edge_id] = str(zone)
        if self._edge_zones:
            logger.info(f"Loaded zone data for {len(self._edge_zones)} edges")

        logger.info(f"Built fulfillment routes for {len(self._fulfillment_routes)} demand nodes")

    @staticmethod
    def _route_sort_key(route: dict) -> tuple:
        """Sort key for ranking fulfillment routes.

        Currently: shortest distance first, then lowest cost as tiebreaker.
        This method is the single point of control for fulfillment priority.
        """
        return (route['distance'], route['cost_variable'])

    def _run_time_steps(self):
        """Main simulation loop: iterate day by day."""
        current = self.start_date
        if isinstance(current, str):
            current = date.fromisoformat(current)
        end = self.end_date
        if isinstance(end, str):
            end = date.fromisoformat(end)

        step = 0
        while current <= end:
            self._process_day(current, step)
            step += 1
            current += timedelta(days=1)

        # Flush any remaining buffered events
        self._flush_events()
        self._flush_snapshots()

    def _process_day(self, sim_date: date, sim_step: int):
        """Process all events for a single day."""

        # 1. Process PO arrivals (Phase 3: dynamic purchase orders)
        self._process_po_arrivals(sim_date, sim_step)

        # 2. Process pre-scheduled inbound arrivals
        self._process_inbound_arrivals(sim_date, sim_step)

        # 3. Process received -> saleable transitions
        self._process_receiving(sim_date, sim_step)

        # 4. Try to fulfill backorders first (FIFO)
        self._process_backorders(sim_date, sim_step)

        # 5. Process new demand for this day
        self._process_demand(sim_date, sim_step)

        # 6. Reorder check (Phase 3: if reorder logic is configured)
        if self._reorder_policy:
            self._process_reorder(sim_date, sim_step)

        # 7. Record daily fixed costs for distribution nodes
        self._record_fixed_costs(sim_date, sim_step)

        # 8. Check for storage capacity overages (soft constraint penalties)
        self._check_capacity_overages(sim_date, sim_step)

        # 9. Write inventory snapshot (if enabled and on schedule)
        if self.write_snapshots and sim_step % self.snapshot_interval == 0:
            self._write_snapshot(sim_date)

    def _process_po_arrivals(self, sim_date: date, sim_step: int):
        """Process purchase orders arriving today."""
        for po in self._purchase_orders:
            if po.status != 'in_transit':
                continue
            if po.expected_arrival is not None and po.expected_arrival <= sim_date:
                # PO arrives: add to received inventory
                key = (po.dest_node_id, po.product_id, 'received')
                self._inventory[key] = self._inventory.get(key, 0) + po.quantity

                po.status = 'received'
                po.actual_arrival = sim_date

                # Update DB record
                self.conn.execute("""
                    UPDATE purchase_order
                    SET status = 'received', actual_arrival = ?
                    WHERE scenario_id = ? AND po_id = ?
                """, [sim_date, self.scenario_id, po.po_id])

                self._log_event(sim_date, sim_step, 'po_arrived',
                                node_id=po.dest_node_id, node_type='distribution',
                                product_id=po.product_id, quantity=po.quantity,
                                from_state='in_transit', to_state='received',
                                detail=json.dumps({
                                    'po_id': po.po_id,
                                    'supply_node_id': po.supply_node_id,
                                }))

    def _process_inbound_arrivals(self, sim_date: date, sim_step: int):
        """Process scheduled inbound shipments arriving today.

        Filters by active supply_node_set, distribution_node_set, and product_set.
        """
        query = """
            SELECT inbound_id, supply_node_id, dest_node_id, product_id, quantity
            FROM inbound_schedule
            WHERE dataset_version_id = ? AND arrival_date = ?
        """
        params = [self.inbound_version_id, sim_date]

        if self.supply_node_set_id:
            query += """
                AND supply_node_id IN (
                    SELECT supply_node_id FROM supply_node_set_member
                    WHERE supply_node_set_id = ?
                )
            """
            params.append(self.supply_node_set_id)

        if self.distribution_node_set_id:
            query += """
                AND dest_node_id IN (
                    SELECT dist_node_id FROM distribution_node_set_member
                    WHERE distribution_node_set_id = ?
                )
            """
            params.append(self.distribution_node_set_id)

        if self.product_set_id:
            query += """
                AND product_id IN (
                    SELECT product_id FROM product_set_member
                    WHERE product_set_id = ?
                )
            """
            params.append(self.product_set_id)

        rows = self.conn.execute(query, params).fetchall()

        for inbound_id, supply_node_id, dest_node_id, product_id, qty in rows:
            qty = float(qty)

            # Add to received inventory
            key = (dest_node_id, product_id, 'received')
            self._inventory[key] = self._inventory.get(key, 0) + qty

            self._log_event(sim_date, sim_step, 'shipment_arrived',
                            node_id=dest_node_id, node_type='distribution',
                            product_id=product_id, quantity=qty,
                            from_state='in_transit', to_state='received',
                            detail=json.dumps({'inbound_id': inbound_id,
                                               'supply_node_id': supply_node_id}))

    def _process_receiving(self, sim_date: date, sim_step: int):
        """Transition received inventory to saleable.

        For simplicity, all received inventory becomes saleable on the same day.
        """
        received_keys = [k for k in self._inventory
                         if k[2] == 'received' and self._inventory[k] > 0]

        for key in received_keys:
            dist_node_id, product_id, _ = key
            qty = self._inventory[key]

            # Move received -> saleable
            self._inventory[key] = 0
            saleable_key = (dist_node_id, product_id, 'saleable')
            self._inventory[saleable_key] = self._inventory.get(saleable_key, 0) + qty

            self._log_event(sim_date, sim_step, 'inventory_state_change',
                            node_id=dist_node_id, node_type='distribution',
                            product_id=product_id, quantity=qty,
                            from_state='received', to_state='saleable')

    def _process_backorders(self, sim_date: date, sim_step: int):
        """Attempt to fulfill backorders from available inventory (FIFO)."""
        remaining_backorders = []

        for demand_id, demand_node_id, product_id, qty, original_date in self._backorders:
            fulfilled_qty = self._try_fulfill(
                sim_date, sim_step, demand_id, demand_node_id,
                product_id, qty, is_backorder=True
            )
            unfulfilled = qty - fulfilled_qty
            if unfulfilled > 0:
                remaining_backorders.append(
                    (demand_id, demand_node_id, product_id, unfulfilled, original_date)
                )

        self._backorders = remaining_backorders

    def _process_demand(self, sim_date: date, sim_step: int):
        """Process all demand events for this day.

        Filters by active demand_node_set and product_set.
        """
        query = """
            SELECT demand_id, demand_node_id, product_id, quantity
            FROM demand
            WHERE dataset_version_id = ? AND demand_date = ?
        """
        params = [self.demand_version_id, sim_date]

        if self.demand_node_set_id:
            query += """
                AND demand_node_id IN (
                    SELECT demand_node_id FROM demand_node_set_member
                    WHERE demand_node_set_id = ?
                )
            """
            params.append(self.demand_node_set_id)

        if self.product_set_id:
            query += """
                AND product_id IN (
                    SELECT product_id FROM product_set_member
                    WHERE product_set_id = ?
                )
            """
            params.append(self.product_set_id)

        query += " ORDER BY demand_datetime ASC NULLS LAST"
        rows = self.conn.execute(query, params).fetchall()

        for demand_id, demand_node_id, product_id, qty in rows:
            qty = float(qty)

            # Log demand received
            self._log_event(sim_date, sim_step, 'demand_received',
                            node_id=demand_node_id, node_type='demand',
                            product_id=product_id, quantity=qty,
                            demand_id=demand_id)

            # Attempt fulfillment
            fulfilled_qty = self._try_fulfill(
                sim_date, sim_step, demand_id, demand_node_id,
                product_id, qty
            )

            unfulfilled = qty - fulfilled_qty
            if unfulfilled > 0:
                self._handle_unfulfilled(
                    sim_date, sim_step, demand_id, demand_node_id,
                    product_id, unfulfilled
                )

    def _try_fulfill(self, sim_date: date, sim_step: int, demand_id: str,
                     demand_node_id: str, product_id: str, qty: float,
                     is_backorder: bool = False) -> float:
        """Try to fulfill demand using the active fulfillment strategy.

        Returns the quantity successfully fulfilled.
        """
        results = self._fulfillment_strategy.fulfill(
            demand_node_id, product_id, qty)

        total_fulfilled = 0.0
        for r in results:
            event_type = 'backorder_fulfilled' if is_backorder else 'demand_fulfilled'
            routes = self._fulfillment_routes.get(demand_node_id, [])
            route = next((rt for rt in routes if rt['edge_id'] == r.edge_id), {})
            detail = {
                'demand_node_id': demand_node_id,
                'variable_cost_per_unit': r.cost / r.quantity if r.quantity > 0 else 0,
                'distance': route.get('distance', 0),
            }
            zone = route.get('zone') or self._edge_zones.get(r.edge_id)
            if zone is not None:
                detail['zone'] = zone

            self._log_event(sim_date, sim_step, event_type,
                            node_id=r.dist_node_id, node_type='distribution',
                            edge_id=r.edge_id, product_id=product_id,
                            quantity=r.quantity,
                            from_state='saleable', to_state='shipped',
                            demand_id=demand_id, cost=r.cost,
                            duration=route.get('mean_transit_time'),
                            detail=json.dumps(detail),
                            fulfillment_rank=r.rank,
                            optimal_cost=r.optimal_cost)

            total_fulfilled += r.quantity

        return total_fulfilled

    def _handle_unfulfilled(self, sim_date: date, sim_step: int,
                            demand_id: str, demand_node_id: str,
                            product_id: str, qty: float):
        """Handle demand that couldn't be fulfilled: backorder or lost sale."""
        if self.rng.random() < self.backorder_prob:
            # Backorder
            self._backorders.append(
                (demand_id, demand_node_id, product_id, qty, sim_date)
            )
            self._log_event(sim_date, sim_step, 'demand_backordered',
                            node_id=demand_node_id, node_type='demand',
                            product_id=product_id, quantity=qty,
                            demand_id=demand_id)
        else:
            # Lost sale
            self._log_event(sim_date, sim_step, 'demand_lost',
                            node_id=demand_node_id, node_type='demand',
                            product_id=product_id, quantity=qty,
                            demand_id=demand_id)

    def _process_reorder(self, sim_date: date, sim_step: int):
        """Run the reorder policy for this day."""
        if not self._reorder_policy.should_reorder(sim_date):
            return

        # Compute new orders
        new_orders = self._reorder_policy.compute_orders(
            sim_date, self._inventory, self._purchase_orders)

        if not new_orders:
            return

        # Apply consolidation
        shipped, held = self._reorder_policy.apply_consolidation(new_orders)

        # Record all orders (shipped + held)
        for po in shipped + held:
            self._purchase_orders.append(po)

            # Write to DB
            self.conn.execute("""
                INSERT INTO purchase_order
                (scenario_id, po_id, sim_date, supply_node_id, product_id,
                 quantity, expected_arrival, dest_node_id, status, cube)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                self.scenario_id, po.po_id, po.sim_date,
                po.supply_node_id, po.product_id, po.quantity,
                po.expected_arrival, po.dest_node_id, po.status, po.cube,
            ])

            event_type = 'po_placed' if po.status == 'in_transit' else 'po_consolidating'
            self._log_event(sim_date, sim_step, event_type,
                            node_id=po.dest_node_id, node_type='distribution',
                            product_id=po.product_id, quantity=po.quantity,
                            cost=0,
                            detail=json.dumps({
                                'po_id': po.po_id,
                                'supply_node_id': po.supply_node_id,
                                'expected_arrival': str(po.expected_arrival),
                                'cube': po.cube,
                            }))

        logger.info(f"Day {sim_date}: placed {len(shipped)} POs, "
                     f"{len(held)} held for consolidation")

    def _record_fixed_costs(self, sim_date: date, sim_step: int):
        """Record daily fixed costs for active distribution nodes."""
        query = """
            SELECT dn.dist_node_id, dn.fixed_cost, dn.fixed_cost_basis
            FROM distribution_node dn
            WHERE dn.fixed_cost > 0
        """
        params = []

        if self.distribution_node_set_id:
            query += """
                AND dn.dist_node_id IN (
                    SELECT dist_node_id FROM distribution_node_set_member
                    WHERE distribution_node_set_id = ?
                )
            """
            params.append(self.distribution_node_set_id)

        rows = self.conn.execute(query, params).fetchall()

        for dist_node_id, fixed_cost, basis in rows:
            if basis == 'per_day':
                self._log_event(sim_date, sim_step, 'fixed_cost',
                                node_id=dist_node_id, node_type='distribution',
                                cost=float(fixed_cost),
                                detail=json.dumps({
                                    'cost_type': 'fixed_cost',
                                    'basis': basis,
                                }))

    def _check_capacity_overages(self, sim_date: date, sim_step: int):
        """Check if any distribution node exceeds storage capacity.

        Capacity uses soft constraints: overages are allowed but incur
        penalty costs. The penalty is overage_penalty if set on the node,
        otherwise 2x the node's variable_cost.
        """
        query = """
            SELECT dn.dist_node_id, dn.storage_capacity, dn.storage_capacity_uom,
                   dn.variable_cost, dn.variable_cost_basis,
                   dn.overage_penalty, dn.overage_penalty_basis
            FROM distribution_node dn
            WHERE dn.storage_capacity IS NOT NULL
        """
        params = []

        if self.distribution_node_set_id:
            query += """
                AND dn.dist_node_id IN (
                    SELECT dist_node_id FROM distribution_node_set_member
                    WHERE distribution_node_set_id = ?
                )
            """
            params.append(self.distribution_node_set_id)

        nodes = self.conn.execute(query, params).fetchall()
        if not nodes:
            return

        # Pre-load product cube data (cached after first call)
        if not hasattr(self, '_product_cubes'):
            self._product_cubes = {}
            rows = self.conn.execute(
                "SELECT product_id, cube, cube_uom FROM product"
            ).fetchall()
            for pid, cube, cube_uom in rows:
                self._product_cubes[pid] = (float(cube), cube_uom)

        for (dist_node_id, capacity, capacity_uom,
             variable_cost, variable_cost_basis,
             overage_penalty, overage_penalty_basis) in nodes:

            capacity = float(capacity)
            if capacity <= 0:
                continue

            # Sum current inventory volume at this node (non-terminal states)
            total_cube = 0.0
            for (nid, pid, state), qty in self._inventory.items():
                if nid != dist_node_id or qty <= 0 or state in TERMINAL_STATES:
                    continue
                cube_per_unit, _ = self._product_cubes.get(pid, (0, 'L'))
                total_cube += qty * cube_per_unit

            # Convert product cube (liters) to capacity UoM.
            # storage_capacity is typically m3; product cube is in liters.
            # 1 m3 = 1000 L
            if capacity_uom == 'm3':
                total_cube_in_capacity_uom = total_cube / 1000.0
            else:
                # If same UoM or unknown, assume no conversion needed
                total_cube_in_capacity_uom = total_cube

            overage = total_cube_in_capacity_uom - capacity
            if overage <= 0:
                continue

            # Calculate penalty cost
            # Default: 2x variable cost per unit of overage (in capacity UoM)
            if overage_penalty is not None:
                penalty_rate = float(overage_penalty)
            elif variable_cost is not None:
                penalty_rate = float(variable_cost) * 2.0
            else:
                penalty_rate = 0.0

            if penalty_rate <= 0:
                continue

            penalty_cost = overage * penalty_rate

            self._log_event(sim_date, sim_step, 'capacity_overage',
                            node_id=dist_node_id, node_type='distribution',
                            cost=penalty_cost,
                            detail=json.dumps({
                                'total_volume': round(total_cube_in_capacity_uom, 2),
                                'capacity': capacity,
                                'capacity_uom': capacity_uom or 'm3',
                                'overage': round(overage, 2),
                                'penalty_rate': penalty_rate,
                            }))

    def _log_event(self, sim_date: date, sim_step: int, event_type: str,
                   node_id: str = None, node_type: str = None,
                   edge_id: str = None, product_id: str = None,
                   quantity: float = None, from_state: str = None,
                   to_state: str = None, demand_id: str = None,
                   cost: float = None, duration: float = None,
                   detail: str = None,
                   fulfillment_rank: int = None,
                   optimal_cost: float = None):
        """Buffer an event for batch insertion."""
        if not self.write_event_log:
            return

        self._event_counter += 1
        self._event_buffer.append((
            self.scenario_id, self._event_counter, sim_date, sim_step,
            event_type, node_id, node_type, edge_id, product_id,
            quantity, from_state, to_state, demand_id, cost, duration, detail,
            fulfillment_rank, optimal_cost,
        ))

    def _flush_events(self):
        """Bulk insert all buffered events into DuckDB via Polars."""
        if not self._event_buffer:
            return

        df = pl.DataFrame(
            self._event_buffer,
            schema={
                'scenario_id': pl.Utf8, 'event_id': pl.Int64,
                'sim_date': pl.Date, 'sim_step': pl.Int32,
                'event_type': pl.Utf8, 'node_id': pl.Utf8,
                'node_type': pl.Utf8, 'edge_id': pl.Utf8,
                'product_id': pl.Utf8, 'quantity': pl.Float64,
                'from_state': pl.Utf8, 'to_state': pl.Utf8,
                'demand_id': pl.Utf8, 'cost': pl.Float64,
                'duration': pl.Float64, 'detail': pl.Utf8,
                'fulfillment_rank': pl.Int32, 'optimal_cost': pl.Float64,
            },
            orient='row',
        )
        self.conn.execute("""
            INSERT INTO event_log (
                scenario_id, event_id, sim_date, sim_step, event_type,
                node_id, node_type, edge_id, product_id, quantity,
                from_state, to_state, demand_id, cost, duration, detail,
                fulfillment_rank, optimal_cost
            ) SELECT * FROM df
        """)
        logger.info(f"Flushed {len(self._event_buffer)} events")
        self._event_buffer.clear()

    def _write_snapshot(self, sim_date: date):
        """Write current inventory state as a snapshot."""
        # Get product cube data for total_cube calculation
        product_cubes = {}
        rows = self.conn.execute("SELECT product_id, cube, cube_uom FROM product").fetchall()
        for pid, cube, cube_uom in rows:
            product_cubes[pid] = (float(cube), cube_uom)

        for (dist_node_id, product_id, state), qty in self._inventory.items():
            if qty <= 0:
                continue
            if state in TERMINAL_STATES:
                continue  # Don't snapshot shipped/disposed

            cube_per_unit, cube_uom = product_cubes.get(product_id, (0, 'L'))
            total_cube = qty * cube_per_unit

            self._snapshot_buffer.append((
                self.scenario_id, sim_date, dist_node_id, product_id,
                state, qty, total_cube, cube_uom,
            ))

    def _flush_snapshots(self):
        """Bulk insert all buffered snapshots into DuckDB via Polars."""
        if not self._snapshot_buffer:
            return

        df = pl.DataFrame(
            self._snapshot_buffer,
            schema={
                'scenario_id': pl.Utf8, 'sim_date': pl.Date,
                'dist_node_id': pl.Utf8, 'product_id': pl.Utf8,
                'inventory_state': pl.Utf8, 'quantity': pl.Float64,
                'total_cube': pl.Float64, 'total_cube_uom': pl.Utf8,
            },
            orient='row',
        )
        self.conn.execute("INSERT INTO inventory_snapshot SELECT * FROM df")
        logger.info(f"Flushed {len(self._snapshot_buffer)} snapshot rows")
        self._snapshot_buffer.clear()
