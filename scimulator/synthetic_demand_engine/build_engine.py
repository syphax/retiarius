#!/usr/bin/env python3
"""
Build script to generate the complete Synthetic Demand Generation Engine.
This script creates all necessary files, modules, and configurations.
"""

import os
import pathlib
from textwrap import dedent

BASE_DIR = pathlib.Path(__file__).parent

def create_file(path, content):
    """Create a file with given content."""
    filepath = BASE_DIR / path
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(dedent(content).strip() + '\n')
    print(f"Created: {path}")

def build_engine():
    """Build all components of the engine."""

    # 1. Package init files
    create_file("config/__init__.py", "")
    create_file("generators/__init__.py", "")
    create_file("utils/__init__.py", "")
    create_file("tests/__init__.py", "")

    # 2. Pattern Generators
    create_file("generators/patterns.py", '''
    """
    Demand pattern generators for seasonality and trends.
    """

    import numpy as np
    from typing import List, Optional
    from ..models import (
        SeasonalityConfig, SeasonalityType,
        TrendConfig, TrendType
    )


    class SeasonalityGenerator:
        """Generates seasonal patterns."""

        @staticmethod
        def generate(timestamps: np.ndarray, config: SeasonalityConfig) -> np.ndarray:
            """Generate seasonality component."""
            if not config.enabled:
                return np.zeros_like(timestamps, dtype=float)

            n = len(timestamps)
            result = np.zeros(n)

            # Determine period in hours
            if config.type == SeasonalityType.DAILY:
                period = 24
            elif config.type == SeasonalityType.WEEKLY:
                period = 24 * 7
            elif config.type == SeasonalityType.MONTHLY:
                period = 24 * 30
            elif config.type == SeasonalityType.YEARLY:
                period = 24 * 365
            else:  # CUSTOM
                period = config.period

            # Generate harmonic components
            for h in range(1, config.harmonics + 1):
                phase = 2 * np.pi * h * timestamps / period + config.phase_shift
                result += (config.amplitude / h) * np.sin(phase)

            return result


    class TrendGenerator:
        """Generates trend patterns."""

        @staticmethod
        def generate(timestamps: np.ndarray, config: TrendConfig) -> np.ndarray:
            """Generate trend component."""
            if not config.enabled or config.type == TrendType.NONE:
                return np.zeros_like(timestamps, dtype=float)

            n = len(timestamps)
            t = np.arange(n)

            if config.type == TrendType.LINEAR:
                return config.coefficient * t

            elif config.type == TrendType.EXPONENTIAL:
                return config.coefficient * (config.exponent ** t - 1)

            elif config.type == TrendType.LOGARITHMIC:
                return config.coefficient * np.log1p(t)

            elif config.type == TrendType.POLYNOMIAL:
                return config.coefficient * (t ** config.exponent)

            elif config.type == TrendType.STEP:
                result = np.zeros(n)
                for cp in config.change_points:
                    idx = cp.get('index', 0)
                    value = cp.get('value', 0)
                    if idx < n:
                        result[idx:] += value
                return result

            return np.zeros(n)


    class BaselineGenerator:
        """Generates baseline demand."""

        @staticmethod
        def generate(n: int, baseline: float) -> np.ndarray:
            """Generate constant baseline."""
            return np.full(n, baseline, dtype=float)
    ''')

    # 3. Noise and Anomaly Generators
    create_file("generators/noise.py", '''
    """
    Noise and anomaly injection for demand patterns.
    """

    import numpy as np
    from typing import List, Optional
    from ..models import NoiseConfig, NoiseType, AnomalyConfig, AnomalyType


    class NoiseGenerator:
        """Generates various types of noise."""

        def __init__(self, seed: Optional[int] = None):
            self.rng = np.random.default_rng(seed)

        def generate(self, n: int, config: NoiseConfig) -> np.ndarray:
            """Generate noise component."""
            if not config.enabled:
                return np.zeros(n)

            if config.type == NoiseType.GAUSSIAN:
                return self.rng.normal(config.mean, config.std_dev, n)

            elif config.type == NoiseType.UNIFORM:
                low = config.min_value if config.min_value is not None else -config.std_dev
                high = config.max_value if config.max_value is not None else config.std_dev
                return self.rng.uniform(low, high, n)

            elif config.type == NoiseType.POISSON:
                return self.rng.poisson(config.lambda_param, n) - config.lambda_param

            elif config.type == NoiseType.LOGNORMAL:
                sigma = config.sigma if config.sigma is not None else config.std_dev
                return self.rng.lognormal(config.mean, sigma, n) - np.exp(config.mean + sigma**2 / 2)

            return np.zeros(n)


    class AnomalyGenerator:
        """Generates anomalies in demand patterns."""

        def __init__(self, seed: Optional[int] = None):
            self.rng = np.random.default_rng(seed)

        def generate(self, values: np.ndarray, config: AnomalyConfig) -> tuple[np.ndarray, np.ndarray]:
            """
            Inject anomalies into values.

            Returns:
                Modified values and anomaly mask (1 where anomaly exists)
            """
            if not config.enabled:
                return values.copy(), np.zeros_like(values, dtype=bool)

            n = len(values)
            result = values.copy()
            mask = np.zeros(n, dtype=bool)

            # Determine anomaly locations
            if config.locations:
                locations = [loc for loc in config.locations if loc < n]
            else:
                # Random locations based on probability
                locations = np.where(self.rng.random(n) < config.probability)[0]

            # Apply anomalies
            for loc in locations:
                end = min(loc + config.duration, n)
                mask[loc:end] = True

                if config.type == AnomalyType.SPIKE:
                    result[loc:end] *= config.magnitude

                elif config.type == AnomalyType.DROP:
                    result[loc:end] *= config.magnitude

                elif config.type == AnomalyType.LEVEL_SHIFT:
                    result[loc:] += config.magnitude * np.mean(values)

                elif config.type == AnomalyType.TREND_CHANGE:
                    trend = np.linspace(0, config.magnitude * np.mean(values), end - loc)
                    result[loc:end] += trend

                elif config.type == AnomalyType.MISSING_DATA:
                    result[loc:end] = np.nan

            return result, mask

        def generate_multiple(self, values: np.ndarray, configs: List[AnomalyConfig]) -> tuple[np.ndarray, np.ndarray]:
            """Apply multiple anomaly configurations."""
            result = values.copy()
            combined_mask = np.zeros_like(values, dtype=bool)

            for config in configs:
                result, mask = self.generate(result, config)
                combined_mask |= mask

            return result, combined_mask
    ''')

    # 4. Correlation Engine
    create_file("generators/correlations.py", '''
    """
    Multi-product correlation engine.
    """

    import numpy as np
    from typing import List, Dict
    from ..models import CorrelationConfig


    class CorrelationEngine:
        """Handles correlations between product demands."""

        def __init__(self, seed: Optional[int] = None):
            self.rng = np.random.default_rng(seed)

        def apply_correlations(
            self,
            demands: Dict[str, np.ndarray],
            correlations: List[CorrelationConfig]
        ) -> Dict[str, np.ndarray]:
            """
            Apply correlations between products.

            Args:
                demands: Dictionary mapping product_id to demand arrays
                correlations: List of correlation configurations

            Returns:
                Modified demand dictionary with correlations applied
            """
            result = {k: v.copy() for k, v in demands.items()}

            # Build dependency graph
            dependencies = self._build_dependency_graph(correlations)

            # Apply correlations in topological order
            processed = set()

            for config in correlations:
                if not config.enabled:
                    continue

                source = config.source_product
                target = config.target_product

                if source not in demands or target not in demands:
                    continue

                # Get source demand
                source_demand = result[source]

                # Apply lag if specified
                if config.lag > 0:
                    source_demand = np.roll(source_demand, config.lag)
                    source_demand[:config.lag] = source_demand[config.lag]

                # Apply correlation
                if config.type == "linear":
                    influence = config.coefficient * (source_demand - np.mean(source_demand))
                    result[target] += influence

                elif config.type == "exponential":
                    normalized = (source_demand - np.mean(source_demand)) / (np.std(source_demand) + 1e-10)
                    influence = config.coefficient * np.mean(result[target]) * (np.exp(normalized) - 1)
                    result[target] += influence

                processed.add(target)

            return result

        def _build_dependency_graph(self, correlations: List[CorrelationConfig]) -> Dict[str, List[str]]:
            """Build dependency graph from correlations."""
            graph = {}
            for config in correlations:
                if config.enabled:
                    if config.target_product not in graph:
                        graph[config.target_product] = []
                    graph[config.target_product].append(config.source_product)
            return graph

        def validate_correlations(self, correlations: List[CorrelationConfig], product_ids: List[str]) -> List[str]:
            """
            Validate correlation configurations.

            Returns:
                List of error messages (empty if valid)
            """
            errors = []
            product_set = set(product_ids)

            for i, config in enumerate(correlations):
                if config.source_product not in product_set:
                    errors.append(f"Correlation {i}: source product '{config.source_product}' not found")

                if config.target_product not in product_set:
                    errors.append(f"Correlation {i}: target product '{config.target_product}' not found")

                if config.source_product == config.target_product:
                    errors.append(f"Correlation {i}: source and target cannot be the same product")

            # Check for circular dependencies
            if self._has_circular_dependency(correlations):
                errors.append("Circular dependency detected in correlations")

            return errors

        def _has_circular_dependency(self, correlations: List[CorrelationConfig]) -> bool:
            """Check for circular dependencies in correlations."""
            graph = self._build_dependency_graph(correlations)
            visited = set()
            rec_stack = set()

            def has_cycle(node):
                visited.add(node)
                rec_stack.add(node)

                for neighbor in graph.get(node, []):
                    if neighbor not in visited:
                        if has_cycle(neighbor):
                            return True
                    elif neighbor in rec_stack:
                        return True

                rec_stack.remove(node)
                return False

            for node in graph:
                if node not in visited:
                    if has_cycle(node):
                        return True

            return False
    ''')

    # 5. Main Orchestrator
    create_file("orchestrator.py", '''
    """
    Main orchestrator for demand generation pipeline.
    """

    import numpy as np
    import pandas as pd
    from typing import List, Dict, Optional
    from datetime import datetime

    from .models import (
        GenerationConfig, ProductConfig, DemandPattern,
        SeasonalityConfig, TrendConfig, NoiseConfig
    )
    from .generators.patterns import SeasonalityGenerator, TrendGenerator, BaselineGenerator
    from .generators.noise import NoiseGenerator, AnomalyGenerator
    from .generators.correlations import CorrelationEngine


    class DemandOrchestrator:
        """Orchestrates the entire demand generation pipeline."""

        def __init__(self, config: GenerationConfig):
            """Initialize orchestrator with configuration."""
            self.config = config
            self.seed = config.seed

            # Initialize generators
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

        def generate(self) -> Dict[str, DemandPattern]:
            """
            Generate demand patterns for all products.

            Returns:
                Dictionary mapping product_id to DemandPattern
            """
            # Validate configuration
            self._validate_config()

            # Generate individual products
            demands = {}
            for product_config in self.config.products:
                pattern = self._generate_product(product_config)
                demands[product_config.product_id] = pattern

            # Apply cross-product correlations
            if self.config.correlations:
                demands = self._apply_correlations(demands)

            return demands

        def _generate_product(self, config: ProductConfig) -> DemandPattern:
            """Generate demand pattern for a single product."""
            n = self.n_timesteps

            # Initialize components dictionary
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

            # Create pattern object
            pattern = DemandPattern(
                product_id=config.product_id,
                timestamps=self.timestamps.values,
                values=demand,
                components=components,
                anomaly_mask=anomaly_mask,
                metadata=config.metadata.copy()
            )

            return pattern

        def _apply_correlations(self, demands: Dict[str, DemandPattern]) -> Dict[str, DemandPattern]:
            """Apply cross-product correlations."""
            # Extract demand arrays
            demand_arrays = {pid: pattern.values for pid, pattern in demands.items()}

            # Apply correlations
            correlated = self.correlation_engine.apply_correlations(
                demand_arrays, self.config.correlations
            )

            # Update patterns with correlated values
            result = {}
            for pid, pattern in demands.items():
                # Create new pattern with correlated values
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

            # Validate correlations
            if self.config.correlations:
                product_ids = [p.product_id for p in self.config.products]
                errors = self.correlation_engine.validate_correlations(
                    self.config.correlations, product_ids
                )
                if errors:
                    raise ValueError(f"Correlation validation failed: {errors}")

        def export(self, demands: Dict[str, DemandPattern], output_path: str):
            """Export generated demands to file."""
            if self.config.output_format == "csv":
                self._export_csv(demands, output_path)
            elif self.config.output_format == "parquet":
                self._export_parquet(demands, output_path)
            elif self.config.output_format == "json":
                self._export_json(demands, output_path)
            else:
                raise ValueError(f"Unsupported output format: {self.config.output_format}")

        def _export_csv(self, demands: Dict[str, DemandPattern], output_path: str):
            """Export to CSV format."""
            # Create DataFrame
            data = {'timestamp': self.timestamps}
            for pid, pattern in demands.items():
                data[f'{pid}_demand'] = pattern.values
                if pattern.anomaly_mask is not None:
                    data[f'{pid}_anomaly'] = pattern.anomaly_mask.astype(int)

            df = pd.DataFrame(data)
            df.to_csv(output_path, index=False)

        def _export_parquet(self, demands: Dict[str, DemandPattern], output_path: str):
            """Export to Parquet format."""
            data = {'timestamp': self.timestamps}
            for pid, pattern in demands.items():
                data[f'{pid}_demand'] = pattern.values
                if pattern.anomaly_mask is not None:
                    data[f'{pid}_anomaly'] = pattern.anomaly_mask.astype(int)

            df = pd.DataFrame(data)
            df.to_parquet(output_path, index=False)

        def _export_json(self, demands: Dict[str, DemandPattern], output_path: str):
            """Export to JSON format."""
            import json

            output = {
                'metadata': self.config.metadata,
                'products': {pid: pattern.to_dict() for pid, pattern in demands.items()}
            }

            with open(output_path, 'w') as f:
                json.dump(output, f, indent=2, default=str)
    ''')

    # 6. Configuration Loader
    create_file("config/loader.py", '''
    """
    Configuration loader for YAML files.
    """

    import yaml
    from pathlib import Path
    from typing import Union, Dict, Any
    from ..models import (
        GenerationConfig, ProductConfig, CorrelationConfig,
        SeasonalityConfig, TrendConfig, NoiseConfig, AnomalyConfig,
        SeasonalityType, TrendType, NoiseType, AnomalyType
    )


    class ConfigLoader:
        """Loads and parses YAML configuration files."""

        @staticmethod
        def load(path: Union[str, Path]) -> GenerationConfig:
            """Load configuration from YAML file."""
            with open(path, 'r') as f:
                data = yaml.safe_load(f)

            return ConfigLoader.from_dict(data)

        @staticmethod
        def from_dict(data: Dict[str, Any]) -> GenerationConfig:
            """Parse configuration from dictionary."""
            # Parse products
            products = [
                ConfigLoader._parse_product(p) for p in data.get('products', [])
            ]

            # Parse correlations
            correlations = [
                ConfigLoader._parse_correlation(c) for c in data.get('correlations', [])
            ]

            return GenerationConfig(
                start_date=data['start_date'],
                end_date=data['end_date'],
                frequency=data.get('frequency', 'H'),
                products=products,
                correlations=correlations,
                seed=data.get('seed'),
                output_format=data.get('output_format', 'csv'),
                metadata=data.get('metadata', {})
            )

        @staticmethod
        def _parse_product(data: Dict[str, Any]) -> ProductConfig:
            """Parse product configuration."""
            seasonality = [
                ConfigLoader._parse_seasonality(s) for s in data.get('seasonality', [])
            ]

            trend = None
            if 'trend' in data:
                trend = ConfigLoader._parse_trend(data['trend'])

            noise = None
            if 'noise' in data:
                noise = ConfigLoader._parse_noise(data['noise'])

            anomalies = [
                ConfigLoader._parse_anomaly(a) for a in data.get('anomalies', [])
            ]

            return ProductConfig(
                product_id=data['product_id'],
                baseline_demand=data['baseline_demand'],
                seasonality=seasonality,
                trend=trend,
                noise=noise,
                anomalies=anomalies,
                min_demand=data.get('min_demand', 0.0),
                max_demand=data.get('max_demand'),
                metadata=data.get('metadata', {})
            )

        @staticmethod
        def _parse_seasonality(data: Dict[str, Any]) -> SeasonalityConfig:
            """Parse seasonality configuration."""
            return SeasonalityConfig(
                type=SeasonalityType(data['type']),
                amplitude=data['amplitude'],
                period=data.get('period'),
                phase_shift=data.get('phase_shift', 0.0),
                harmonics=data.get('harmonics', 1),
                enabled=data.get('enabled', True)
            )

        @staticmethod
        def _parse_trend(data: Dict[str, Any]) -> TrendConfig:
            """Parse trend configuration."""
            return TrendConfig(
                type=TrendType(data['type']),
                coefficient=data.get('coefficient', 0.0),
                exponent=data.get('exponent', 1.0),
                change_points=data.get('change_points', []),
                enabled=data.get('enabled', True)
            )

        @staticmethod
        def _parse_noise(data: Dict[str, Any]) -> NoiseConfig:
            """Parse noise configuration."""
            return NoiseConfig(
                type=NoiseType(data['type']),
                std_dev=data.get('std_dev'),
                mean=data.get('mean', 0.0),
                min_value=data.get('min_value'),
                max_value=data.get('max_value'),
                lambda_param=data.get('lambda_param'),
                sigma=data.get('sigma'),
                enabled=data.get('enabled', True)
            )

        @staticmethod
        def _parse_anomaly(data: Dict[str, Any]) -> AnomalyConfig:
            """Parse anomaly configuration."""
            return AnomalyConfig(
                type=AnomalyType(data['type']),
                probability=data['probability'],
                magnitude=data['magnitude'],
                duration=data.get('duration', 1),
                locations=data.get('locations'),
                enabled=data.get('enabled', True)
            )

        @staticmethod
        def _parse_correlation(data: Dict[str, Any]) -> CorrelationConfig:
            """Parse correlation configuration."""
            return CorrelationConfig(
                source_product=data['source_product'],
                target_product=data['target_product'],
                coefficient=data['coefficient'],
                lag=data.get('lag', 0),
                type=data.get('type', 'linear'),
                enabled=data.get('enabled', True)
            )
    ''')

    # 7. Utilities
    create_file("utils/validation.py", '''
    """
    Validation utilities for generated demand patterns.
    """

    import numpy as np
    from typing import Dict, List, Tuple
    from ..models import DemandPattern


    class DemandValidator:
        """Validates generated demand patterns."""

        @staticmethod
        def validate_pattern(pattern: DemandPattern) -> Tuple[bool, List[str]]:
            """
            Validate a single demand pattern.

            Returns:
                (is_valid, error_messages)
            """
            errors = []

            # Check for NaN values
            if np.any(np.isnan(pattern.values)):
                nan_count = np.sum(np.isnan(pattern.values))
                errors.append(f"Contains {nan_count} NaN values")

            # Check for negative values
            if np.any(pattern.values < 0):
                neg_count = np.sum(pattern.values < 0)
                errors.append(f"Contains {neg_count} negative values")

            # Check for infinite values
            if np.any(np.isinf(pattern.values)):
                errors.append("Contains infinite values")

            # Check timestamp-value alignment
            if len(pattern.timestamps) != len(pattern.values):
                errors.append("Timestamp and value arrays have different lengths")

            # Statistical checks
            if np.std(pattern.values) == 0:
                errors.append("Zero variance - pattern is constant")

            return len(errors) == 0, errors

        @staticmethod
        def validate_all(demands: Dict[str, DemandPattern]) -> Dict[str, Tuple[bool, List[str]]]:
            """Validate all demand patterns."""
            results = {}
            for pid, pattern in demands.items():
                results[pid] = DemandValidator.validate_pattern(pattern)
            return results

        @staticmethod
        def check_quality_metrics(pattern: DemandPattern) -> Dict[str, float]:
            """Calculate quality metrics for a demand pattern."""
            values = pattern.values[~np.isnan(pattern.values)]  # Remove NaN

            if len(values) == 0:
                return {
                    'mean': 0.0,
                    'std': 0.0,
                    'cv': 0.0,
                    'completeness': 0.0
                }

            mean = np.mean(values)
            std = np.std(values)
            cv = std / mean if mean != 0 else 0.0
            completeness = len(values) / len(pattern.values)

            return {
                'mean': float(mean),
                'std': float(std),
                'cv': float(cv),
                'completeness': float(completeness)
            }
    ''')

    create_file("utils/visualization.py", '''
    """
    Visualization utilities for demand patterns.
    """

    import numpy as np
    import matplotlib.pyplot as plt
    from typing import Dict, Optional, List
    from ..models import DemandPattern


    class DemandVisualizer:
        """Visualizes demand patterns and components."""

        @staticmethod
        def plot_pattern(
            pattern: DemandPattern,
            show_components: bool = True,
            show_anomalies: bool = True,
            figsize: tuple = (15, 8)
        ):
            """Plot a single demand pattern with components."""
            n_plots = 1 + (1 if show_components and pattern.components else 0)

            fig, axes = plt.subplots(n_plots, 1, figsize=figsize)
            if n_plots == 1:
                axes = [axes]

            # Plot main demand
            ax = axes[0]
            ax.plot(pattern.timestamps, pattern.values, label='Total Demand', linewidth=1)

            # Highlight anomalies
            if show_anomalies and pattern.anomaly_mask is not None:
                anomaly_idx = np.where(pattern.anomaly_mask)[0]
                if len(anomaly_idx) > 0:
                    ax.scatter(
                        pattern.timestamps[anomaly_idx],
                        pattern.values[anomaly_idx],
                        color='red', s=20, label='Anomalies', zorder=5
                    )

            ax.set_xlabel('Time')
            ax.set_ylabel('Demand')
            ax.set_title(f'Demand Pattern: {pattern.product_id}')
            ax.legend()
            ax.grid(True, alpha=0.3)

            # Plot components
            if show_components and pattern.components and len(axes) > 1:
                ax = axes[1]
                for comp_name, comp_values in pattern.components.items():
                    ax.plot(pattern.timestamps, comp_values, label=comp_name, alpha=0.7)

                ax.set_xlabel('Time')
                ax.set_ylabel('Value')
                ax.set_title('Demand Components')
                ax.legend()
                ax.grid(True, alpha=0.3)

            plt.tight_layout()
            return fig

        @staticmethod
        def plot_multiple(
            demands: Dict[str, DemandPattern],
            product_ids: Optional[List[str]] = None,
            figsize: tuple = (15, 10)
        ):
            """Plot multiple demand patterns."""
            if product_ids is None:
                product_ids = list(demands.keys())

            n = len(product_ids)
            fig, axes = plt.subplots(n, 1, figsize=figsize, sharex=True)
            if n == 1:
                axes = [axes]

            for ax, pid in zip(axes, product_ids):
                pattern = demands[pid]
                ax.plot(pattern.timestamps, pattern.values, linewidth=1)

                if pattern.anomaly_mask is not None:
                    anomaly_idx = np.where(pattern.anomaly_mask)[0]
                    if len(anomaly_idx) > 0:
                        ax.scatter(
                            pattern.timestamps[anomaly_idx],
                            pattern.values[anomaly_idx],
                            color='red', s=10, zorder=5
                        )

                ax.set_ylabel('Demand')
                ax.set_title(f'Product: {pid}')
                ax.grid(True, alpha=0.3)

            axes[-1].set_xlabel('Time')
            plt.tight_layout()
            return fig

        @staticmethod
        def plot_statistics(demands: Dict[str, DemandPattern], figsize: tuple = (12, 6)):
            """Plot statistical summary of all patterns."""
            stats = {pid: pattern.get_statistics() for pid, pattern in demands.items()}

            fig, axes = plt.subplots(2, 2, figsize=figsize)

            # Mean demand
            ax = axes[0, 0]
            products = list(stats.keys())
            means = [stats[p]['mean'] for p in products]
            ax.bar(products, means)
            ax.set_title('Mean Demand by Product')
            ax.set_ylabel('Mean Demand')
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            # Standard deviation
            ax = axes[0, 1]
            stds = [stats[p]['std'] for p in products]
            ax.bar(products, stds)
            ax.set_title('Demand Variability by Product')
            ax.set_ylabel('Std Dev')
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            # Min/Max range
            ax = axes[1, 0]
            mins = [stats[p]['min'] for p in products]
            maxs = [stats[p]['max'] for p in products]
            x = np.arange(len(products))
            ax.bar(x - 0.2, mins, 0.4, label='Min')
            ax.bar(x + 0.2, maxs, 0.4, label='Max')
            ax.set_xticks(x)
            ax.set_xticklabels(products)
            ax.set_title('Demand Range by Product')
            ax.set_ylabel('Value')
            ax.legend()
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            # Anomaly count
            ax = axes[1, 1]
            anomaly_counts = [stats[p]['anomaly_count'] for p in products]
            ax.bar(products, anomaly_counts)
            ax.set_title('Anomaly Count by Product')
            ax.set_ylabel('Count')
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            plt.tight_layout()
            return fig
    ''')

    # 8. CLI
    create_file("cli.py", '''
    """
    Command-line interface for synthetic demand generation.
    """

    import argparse
    import sys
    from pathlib import Path

    from .config.loader import ConfigLoader
    from .orchestrator import DemandOrchestrator
    from .utils.validation import DemandValidator
    from .utils.visualization import DemandVisualizer


    def main():
        """Main CLI entry point."""
        parser = argparse.ArgumentParser(
            description='Synthetic Demand Generation Engine',
            formatter_class=argparse.RawDescriptionHelpFormatter
        )

        parser.add_argument(
            'config',
            type=str,
            help='Path to YAML configuration file'
        )

        parser.add_argument(
            '-o', '--output',
            type=str,
            required=True,
            help='Output file path'
        )

        parser.add_argument(
            '--validate',
            action='store_true',
            help='Validate generated patterns'
        )

        parser.add_argument(
            '--plot',
            type=str,
            help='Save plots to this path'
        )

        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Verbose output'
        )

        args = parser.parse_args()

        try:
            # Load configuration
            if args.verbose:
                print(f"Loading configuration from {args.config}...")

            config = ConfigLoader.load(args.config)

            # Generate demands
            if args.verbose:
                print(f"Generating demand patterns for {len(config.products)} products...")

            orchestrator = DemandOrchestrator(config)
            demands = orchestrator.generate()

            if args.verbose:
                print(f"Generated {len(demands)} demand patterns")

            # Validate
            if args.validate:
                if args.verbose:
                    print("Validating patterns...")

                validation_results = DemandValidator.validate_all(demands)

                all_valid = True
                for pid, (is_valid, errors) in validation_results.items():
                    if not is_valid:
                        all_valid = False
                        print(f"Validation failed for {pid}:")
                        for error in errors:
                            print(f"  - {error}")

                if all_valid and args.verbose:
                    print("All patterns validated successfully")

            # Export
            if args.verbose:
                print(f"Exporting to {args.output}...")

            orchestrator.export(demands, args.output)

            if args.verbose:
                print("Export complete")

            # Plot
            if args.plot:
                if args.verbose:
                    print(f"Generating plots to {args.plot}...")

                import matplotlib
                matplotlib.use('Agg')

                fig = DemandVisualizer.plot_multiple(demands)
                fig.savefig(args.plot, dpi=150, bbox_inches='tight')

                if args.verbose:
                    print("Plots saved")

            print("Success!")
            return 0

        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1


    if __name__ == '__main__':
        sys.exit(main())
    ''')

    # 9. Requirements and setup
    requirements = """
numpy>=1.24.0
pandas>=2.0.0
matplotlib>=3.7.0
pyyaml>=6.0
pytest>=7.4.0
""".strip()

    create_file("requirements.txt", requirements)

    setup_py = """
from setuptools import setup, find_packages

setup(
    name="synthetic-demand-engine",
    version="1.0.0",
    description="Production-grade synthetic demand generation engine",
    packages=find_packages(),
    install_requires=[
        "numpy>=1.24.0",
        "pandas>=2.0.0",
        "matplotlib>=3.7.0",
        "pyyaml>=6.0",
    ],
    entry_points={
        'console_scripts': [
            'synth-demand=synthetic_demand_engine.cli:main',
        ],
    },
    python_requires=">=3.9",
)
""".strip()

    create_file("setup.py", setup_py)

    print("\\nBuild complete!")
    print(f"Created {BASE_DIR} with all modules")

if __name__ == "__main__":
    build_engine()
