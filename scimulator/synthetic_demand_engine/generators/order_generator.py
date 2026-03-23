"""
Discrete order event generator using Poisson-based arrival and quantity models.

Implements the spec from demand_simulator.md Sections 3.1-3.3:
- Order arrivals: Poisson(annual_orders / 365) per product per day
- Order timestamps: Uniform between 07:00 and 22:00
- Order quantities: Zero-Truncated Poisson(annual_units / annual_orders)
- Geographic assignment: Weighted random sample from ZIP3 weights
"""

import uuid
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional

from ..models import ProductConfig, GeographicWeight


class OrderEventGenerator:
    """Generates discrete order events with Poisson-based stochastic modeling."""

    # Operational window: 07:00 to 22:00 (15 hours = 900 minutes)
    OP_START_HOUR = 7
    OP_END_HOUR = 22
    OP_WINDOW_MINUTES = (OP_END_HOUR - OP_START_HOUR) * 60  # 900

    def __init__(self, seed: Optional[int] = None):
        self.rng = np.random.default_rng(seed)

    def generate(
        self,
        products: List[ProductConfig],
        geographic_weights: List[GeographicWeight],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Generate a complete order ledger for all products across the date range.

        Returns a DataFrame with columns: order_id, timestamp, part_number, zip3, quantity
        """
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        days = pd.date_range(start=start, end=end, freq='D')
        n_days = len(days)

        # Prepare geographic weights for weighted sampling
        zip3s = np.array([gw.zip3 for gw in geographic_weights])
        weights = np.array([gw.weight for gw in geographic_weights])

        all_orders = []

        for product in products:
            if product.annual_units is None or product.annual_orders is None:
                continue

            daily_order_rate = product.annual_orders / 365.0
            units_per_order = product.annual_units / product.annual_orders

            # Sample order counts for all days at once: Poisson(λ = daily_order_rate)
            order_counts = self.rng.poisson(lam=daily_order_rate, size=n_days)
            total_orders = int(order_counts.sum())

            if total_orders == 0:
                continue

            # Generate quantities: Zero-Truncated Poisson
            quantities = self._zero_truncated_poisson(lam=units_per_order, size=total_orders)

            # Generate ZIP3 assignments: weighted random sample
            zip3_assignments = self.rng.choice(zip3s, size=total_orders, p=weights)

            # Generate timestamps: one per order
            # For each day, we know how many orders occur; assign uniform times within 07:00-22:00
            timestamps = self._generate_timestamps(days, order_counts, total_orders)

            # Generate UUIDs
            order_ids = [str(uuid.uuid4()) for _ in range(total_orders)]

            # Build partial ledger for this product
            product_orders = pd.DataFrame({
                'order_id': order_ids,
                'timestamp': timestamps,
                'part_number': product.product_id,
                'zip3': zip3_assignments,
                'quantity': quantities,
            })

            all_orders.append(product_orders)

        if not all_orders:
            return pd.DataFrame(columns=['order_id', 'timestamp', 'part_number', 'zip3', 'quantity'])

        ledger = pd.concat(all_orders, ignore_index=True)
        ledger = ledger.sort_values('timestamp').reset_index(drop=True)
        return ledger

    def _zero_truncated_poisson(self, lam: float, size: int) -> np.ndarray:
        """Sample from a Zero-Truncated Poisson distribution.

        Ensures all values >= 1 (no ghost orders).
        Uses rejection sampling: draw from Poisson, resample any zeros.
        """
        samples = self.rng.poisson(lam=lam, size=size)
        zeros = samples == 0
        while zeros.any():
            samples[zeros] = self.rng.poisson(lam=lam, size=zeros.sum())
            zeros = samples == 0
        return samples

    def _generate_timestamps(
        self,
        days: pd.DatetimeIndex,
        order_counts: np.ndarray,
        total_orders: int,
    ) -> np.ndarray:
        """Generate random timestamps within the 07:00-22:00 operational window.

        For each day with N orders, sample N uniform times in [07:00, 22:00).
        """
        # Random minute offsets from 07:00, uniform in [0, 900) minutes
        minute_offsets = self.rng.uniform(0, self.OP_WINDOW_MINUTES, size=total_orders)

        # Expand days by order_counts: day_indices[i] is the day index for order i
        day_indices = np.repeat(np.arange(len(days)), order_counts)

        # Base date (midnight) for each order
        base_dates = days[day_indices]

        # Convert minute offsets to timedeltas and add to base dates + 07:00
        timestamps = (
            base_dates
            + pd.Timedelta(hours=self.OP_START_HOUR)
            + pd.to_timedelta(minute_offsets, unit='m')
        )

        # Truncate to second precision
        timestamps = timestamps.floor('s')

        return timestamps.values
