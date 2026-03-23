"""
Main orchestrator for demand generation pipeline.
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Union
from datetime import datetime

from .models import (
    GenerationConfig, ProductConfig, DemandPattern,
    SeasonalityConfig, TrendConfig, NoiseConfig
)
from .generators.patterns import SeasonalityGenerator, TrendGenerator, BaselineGenerator
from .generators.noise import NoiseGenerator, AnomalyGenerator
from .generators.correlations import CorrelationEngine
from .generators.order_generator import OrderEventGenerator


class DemandOrchestrator:
    """Orchestrates the entire demand generation pipeline.

    Supports two modes:
    - Signal mode: continuous time-series patterns (YAML-defined products)
    - Order ledger mode: discrete stochastic order events (CSV-defined products
      with annual_units/annual_orders and geographic weights)
    """

    def __init__(self, config: GenerationConfig):
        """Initialize orchestrator with configuration."""
        self.config = config
        self.seed = config.seed

        # Detect mode: order ledger if products have annual_units/annual_orders
        # and geographic weights are provided
        self._order_mode = (
            config.geographic_weights
            and all(
                p.annual_units is not None and p.annual_orders is not None
                for p in config.products
            )
        )

        if self._order_mode:
            self.order_gen = OrderEventGenerator(seed=self.seed)
        else:
            # Initialize signal-mode generators
            self.noise_gen = NoiseGenerator(seed=self.seed)
            self.anomaly_gen = AnomalyGenerator(seed=self.seed + 1 if self.seed else None)
            self.correlation_engine = CorrelationEngine(seed=self.seed + 2 if self.seed else None)

            # Generate time index
            self.timestamps = pd.date_range(
                start=config.start_date,
                end=config.end_date,
                freq=config.frequency
            )
            self.n_timesteps = len(self.timestamps)

        # Stores the order ledger when in order mode
        self._order_ledger: Optional[pd.DataFrame] = None

    def generate(self) -> Dict[str, DemandPattern]:
        """Generate demand patterns for all products.

        In order ledger mode, this generates the order ledger internally
        and returns an empty dict (use export() to write the ledger).

        Returns:
            Dictionary mapping product_id to DemandPattern (empty in order mode)
        """
        if self._order_mode:
            return self._generate_order_ledger()

        return self._generate_signals()

    def _generate_order_ledger(self) -> Dict[str, DemandPattern]:
        """Generate discrete order events via Poisson-based model."""
        self._order_ledger = self.order_gen.generate(
            products=self.config.products,
            geographic_weights=self.config.geographic_weights,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
        )
        # Return empty dict; the ledger is accessed via export()
        return {}

    def _generate_signals(self) -> Dict[str, DemandPattern]:
        """Generate continuous time-series signal patterns."""
        self._validate_config()

        demands = {}
        for product_config in self.config.products:
            pattern = self._generate_product(product_config)
            demands[product_config.product_id] = pattern

        if self.config.correlations:
            demands = self._apply_correlations(demands)

        return demands

    def _generate_product(self, config: ProductConfig) -> DemandPattern:
        """Generate demand pattern for a single product."""
        n = self.n_timesteps
        components = {}

        # 1. Baseline
        baseline = BaselineGenerator.generate(n, config.baseline_demand)
        components['baseline'] = baseline.copy()
        demand = baseline.copy()

        # 2. Seasonality
        if config.seasonality:
            seasonality = np.zeros(n)
            for season_config in config.seasonality:
                season_component = SeasonalityGenerator.generate(
                    np.arange(n), season_config
                )
                seasonality += season_component
            components['seasonality'] = seasonality
            demand += seasonality

        # 3. Trend
        if config.trend:
            trend = TrendGenerator.generate(np.arange(n), config.trend)
            components['trend'] = trend
            demand += trend

        # 4. Noise
        if config.noise:
            noise = self.noise_gen.generate(n, config.noise)
            components['noise'] = noise
            demand += noise

        # 5. Anomalies
        anomaly_mask = None
        if config.anomalies:
            demand, anomaly_mask = self.anomaly_gen.generate_multiple(
                demand, config.anomalies
            )

        # 6. Apply constraints
        demand = np.clip(demand, config.min_demand, config.max_demand)

        metadata = config.metadata.copy()
        if config.annual_units is not None:
            metadata['annual_units'] = config.annual_units
        if config.annual_orders is not None:
            metadata['annual_orders'] = config.annual_orders

        return DemandPattern(
            product_id=config.product_id,
            timestamps=self.timestamps.values,
            values=demand,
            components=components,
            anomaly_mask=anomaly_mask,
            metadata=metadata
        )

    def _apply_correlations(self, demands: Dict[str, DemandPattern]) -> Dict[str, DemandPattern]:
        """Apply cross-product correlations."""
        demand_arrays = {pid: pattern.values for pid, pattern in demands.items()}
        correlated = self.correlation_engine.apply_correlations(
            demand_arrays, self.config.correlations
        )

        result = {}
        for pid, pattern in demands.items():
            new_pattern = DemandPattern(
                product_id=pattern.product_id,
                timestamps=pattern.timestamps,
                values=correlated[pid],
                components=pattern.components,
                anomaly_mask=pattern.anomaly_mask,
                metadata=pattern.metadata
            )
            result[pid] = new_pattern
        return result

    def _validate_config(self):
        """Validate configuration before generation."""
        if self.n_timesteps == 0:
            raise ValueError("Invalid date range: no timestamps generated")

        if self.config.correlations:
            product_ids = [p.product_id for p in self.config.products]
            errors = self.correlation_engine.validate_correlations(
                self.config.correlations, product_ids
            )
            if errors:
                raise ValueError(f"Correlation validation failed: {errors}")

    def export(self, demands: Dict[str, DemandPattern], output_path: str):
        """Export generated demands to file."""
        fmt = self.config.output_format

        if self._order_mode and self._order_ledger is not None:
            self._export_order_ledger(output_path, fmt)
        elif fmt == "csv":
            self._export_signal_csv(demands, output_path)
        elif fmt == "parquet":
            self._export_signal_parquet(demands, output_path)
        elif fmt == "json":
            self._export_signal_json(demands, output_path)
        else:
            raise ValueError(f"Unsupported output format: {fmt}")

    def _export_order_ledger(self, output_path: str, fmt: str):
        """Export the order ledger DataFrame."""
        if fmt == "csv":
            self._order_ledger.to_csv(output_path, index=False)
        elif fmt == "parquet":
            self._order_ledger.to_parquet(output_path, index=False)
        else:
            raise ValueError(f"Unsupported output format for order ledger: {fmt}")

    def _export_signal_csv(self, demands: Dict[str, DemandPattern], output_path: str):
        """Export signal-mode data to CSV."""
        data = {'timestamp': self.timestamps}
        for pid, pattern in demands.items():
            data[f'{pid}_demand'] = pattern.values
            if pattern.anomaly_mask is not None:
                data[f'{pid}_anomaly'] = pattern.anomaly_mask.astype(int)
        pd.DataFrame(data).to_csv(output_path, index=False)

    def _export_signal_parquet(self, demands: Dict[str, DemandPattern], output_path: str):
        """Export signal-mode data to Parquet."""
        data = {'timestamp': self.timestamps}
        for pid, pattern in demands.items():
            data[f'{pid}_demand'] = pattern.values
            if pattern.anomaly_mask is not None:
                data[f'{pid}_anomaly'] = pattern.anomaly_mask.astype(int)
        pd.DataFrame(data).to_parquet(output_path, index=False)

    def _export_signal_json(self, demands: Dict[str, DemandPattern], output_path: str):
        """Export signal-mode data to JSON."""
        import json
        output = {
            'metadata': self.config.metadata,
            'products': {pid: pattern.to_dict() for pid, pattern in demands.items()}
        }
        with open(output_path, 'w') as f:
            json.dump(output, f, indent=2, default=str)

    def get_order_ledger(self) -> Optional[pd.DataFrame]:
        """Access the generated order ledger (order mode only)."""
        return self._order_ledger
