"""
Forecast methods for the reorder logic.

Each method produces a national-level demand forecast for a given product
over a specified horizon. Per-node allocation is handled separately.
"""

import math
from datetime import date, timedelta
from typing import Dict
from collections import defaultdict

import duckdb
import numpy as np


class NoisyActualsForecast:
    """Forecast by taking actual future demand and adding bias + noise.

    Parameters:
        bias: percentage adjustment (e.g. 0.10 = +10% bias)
        error: 2-sigma error width at a monthly (30-day) horizon
        distribution: 'normal', 'lognormal', or 'poisson'
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection,
                 demand_version_id: str,
                 bias: float, error: float, distribution: str,
                 rng: np.random.Generator,
                 product_set_id: str = None,
                 demand_node_set_id: str = None):
        self.bias = bias
        self.error = error
        self.distribution = distribution
        self.rng = rng

        # Pre-load national demand by (product_id, date) -> total qty
        self._demand: Dict[str, Dict[date, float]] = defaultdict(lambda: defaultdict(float))
        self._load_demand(conn, demand_version_id, product_set_id, demand_node_set_id)

    def _load_demand(self, conn, demand_version_id, product_set_id, demand_node_set_id):
        query = """
            SELECT product_id, demand_date, SUM(quantity) as total_qty
            FROM demand
            WHERE dataset_version_id = ?
        """
        params = [demand_version_id]

        if product_set_id:
            query += """
                AND product_id IN (
                    SELECT product_id FROM product_set_member
                    WHERE product_set_id = ?
                )
            """
            params.append(product_set_id)

        if demand_node_set_id:
            query += """
                AND demand_node_id IN (
                    SELECT demand_node_id FROM demand_node_set_member
                    WHERE demand_node_set_id = ?
                )
            """
            params.append(demand_node_set_id)

        query += " GROUP BY product_id, demand_date"
        rows = conn.execute(query, params).fetchall()

        for product_id, demand_date, total_qty in rows:
            if isinstance(demand_date, str):
                demand_date = date.fromisoformat(demand_date)
            self._demand[product_id][demand_date] = float(total_qty)

    def forecast_national(self, product_id: str, start_date: date,
                          horizon_days: int) -> float:
        """Forecast national demand for a product over a horizon.

        1. Sum actual demand from input data for [start_date, start_date + horizon_days)
        2. Apply bias: biased = actual * (1 + bias)
        3. Apply error: scale by sqrt(horizon_days / 30) from monthly base
        4. Draw from distribution
        5. Floor at 0
        """
        # Sum actuals over horizon
        product_demand = self._demand.get(product_id, {})
        actual = 0.0
        for day_offset in range(horizon_days):
            d = start_date + timedelta(days=day_offset)
            actual += product_demand.get(d, 0.0)

        # Apply bias
        biased = actual * (1.0 + self.bias)

        if biased <= 0:
            return 0.0

        # Apply error and draw from distribution
        if self.distribution == 'poisson':
            # Single-parameter: mean = biased, error param ignored
            forecast = float(self.rng.poisson(lam=max(biased, 0)))
        elif self.error <= 0:
            # No error: return biased value directly
            forecast = biased
        else:
            # Scale error from monthly (30-day) base to actual horizon
            scale_factor = math.sqrt(horizon_days / 30.0)
            # error is 2-sigma width as a fraction; sigma = biased * error / 2
            sigma = biased * self.error * scale_factor / 2.0

            if self.distribution == 'normal':
                forecast = self.rng.normal(loc=biased, scale=sigma)
            elif self.distribution == 'lognormal':
                # Convert mean/sigma to lognormal parameters
                # mu_ln = ln(mean^2 / sqrt(sigma^2 + mean^2))
                # sigma_ln = sqrt(ln(1 + sigma^2/mean^2))
                variance = sigma ** 2
                mu_ln = math.log(biased ** 2 / math.sqrt(variance + biased ** 2))
                sigma_ln = math.sqrt(math.log(1 + variance / biased ** 2))
                forecast = float(self.rng.lognormal(mean=mu_ln, sigma=sigma_ln))
            else:
                raise ValueError(f"Unknown forecast distribution: {self.distribution}")

        return max(forecast, 0.0)

    def get_daily_demand_rate(self, product_id: str) -> float:
        """Average daily demand across all dates in the dataset."""
        product_demand = self._demand.get(product_id, {})
        if not product_demand:
            return 0.0
        return sum(product_demand.values()) / len(product_demand)

    def get_demand_by_node(self, conn: duckdb.DuckDBPyConnection,
                           demand_version_id: str,
                           product_id: str,
                           start_date: date = None,
                           end_date: date = None) -> Dict[str, float]:
        """Get total demand per demand_node for a product over a date range.

        Used by the allocator for fair-share computation.
        """
        query = """
            SELECT demand_node_id, SUM(quantity) as total_qty
            FROM demand
            WHERE dataset_version_id = ? AND product_id = ?
        """
        params = [demand_version_id, product_id]

        if start_date:
            query += " AND demand_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND demand_date <= ?"
            params.append(end_date)

        query += " GROUP BY demand_node_id"
        rows = conn.execute(query, params).fetchall()
        return {node_id: float(qty) for node_id, qty in rows}


def create_forecast(method: str, conn: duckdb.DuckDBPyConnection,
                    demand_version_id: str,
                    bias: float, error: float, distribution: str,
                    rng: np.random.Generator,
                    product_set_id: str = None,
                    demand_node_set_id: str = None):
    """Factory: create a forecast method by name."""
    if method == 'noisy_actuals':
        return NoisyActualsForecast(
            conn, demand_version_id, bias, error, distribution, rng,
            product_set_id, demand_node_set_id)
    raise ValueError(f"Unknown forecast method: {method}")
