"""
Configuration loader for YAML and CSV files.
"""

import csv
import yaml
from pathlib import Path
from typing import Union, Dict, Any, List, Optional
from ..models import (
    GenerationConfig, ProductConfig, CorrelationConfig, GeographicWeight,
    SeasonalityConfig, TrendConfig, NoiseConfig, AnomalyConfig,
    SeasonalityType, TrendType, NoiseType, AnomalyType
)


class ConfigLoader:
    """Loads and parses YAML configuration files with optional CSV data inputs."""

    @staticmethod
    def load(
        path: Union[str, Path],
        products_csv: Optional[Union[str, Path]] = None,
        geo_weights_csv: Optional[Union[str, Path]] = None,
        correlations_csv: Optional[Union[str, Path]] = None,
    ) -> GenerationConfig:
        """Load configuration from YAML file, with optional CSV overrides.

        Args:
            path: Path to YAML configuration file.
            products_csv: Path to products CSV file. Overrides products in YAML.
            geo_weights_csv: Path to geographic weights CSV file.
            correlations_csv: Path to correlations CSV file. Overrides correlations in YAML.
        """
        with open(path, 'r') as f:
            data = yaml.safe_load(f)

        # CSV paths can come from YAML or from method arguments (args take precedence)
        products_csv = products_csv or data.get('products_csv')
        geo_weights_csv = geo_weights_csv or data.get('geo_weights_csv')
        correlations_csv = correlations_csv or data.get('correlations_csv')

        return ConfigLoader.from_dict(
            data,
            products_csv=products_csv,
            geo_weights_csv=geo_weights_csv,
            correlations_csv=correlations_csv,
        )

    @staticmethod
    def from_dict(
        data: Dict[str, Any],
        products_csv: Optional[Union[str, Path]] = None,
        geo_weights_csv: Optional[Union[str, Path]] = None,
        correlations_csv: Optional[Union[str, Path]] = None,
    ) -> GenerationConfig:
        """Parse configuration from dictionary, with optional CSV data."""
        # Load products from CSV or YAML
        if products_csv:
            products = ConfigLoader._load_products_csv(products_csv, data.get('frequency', 'H'))
        else:
            products = [
                ConfigLoader._parse_product(p) for p in data.get('products', [])
            ]

        # Load correlations from CSV or YAML
        if correlations_csv:
            correlations = ConfigLoader._load_correlations_csv(correlations_csv)
        else:
            correlations = [
                ConfigLoader._parse_correlation(c) for c in data.get('correlations', [])
            ]

        # Load geographic weights from CSV
        geographic_weights = []
        if geo_weights_csv:
            geographic_weights = ConfigLoader._load_geo_weights_csv(geo_weights_csv)

        return GenerationConfig(
            start_date=data['start_date'],
            end_date=data['end_date'],
            frequency=data.get('frequency', 'H'),
            products=products,
            correlations=correlations,
            geographic_weights=geographic_weights,
            seed=data.get('seed'),
            output_format=data.get('output_format', 'csv'),
            metadata=data.get('metadata', {})
        )

    @staticmethod
    def _load_products_csv(path: Union[str, Path], frequency: str = 'H') -> List[ProductConfig]:
        """Load products from CSV file.

        Required columns: part_number, annual_units, annual_orders
        All other columns are optional.
        """
        path = Path(path).expanduser()
        products = []

        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Required fields
                part_number = row['part_number'].strip()
                annual_units = float(row['annual_units'])
                annual_orders = int(float(row['annual_orders']))

                # Convert annual_units to baseline_demand per time period
                baseline_demand = ConfigLoader._annual_to_baseline(annual_units, frequency)

                product = ProductConfig(
                    product_id=part_number,
                    baseline_demand=baseline_demand,
                    annual_units=annual_units,
                    annual_orders=annual_orders,
                    min_demand=0.0,
                )

                products.append(product)

        return products

    @staticmethod
    def _annual_to_baseline(annual_units: float, frequency: str) -> float:
        """Convert annual units to per-period baseline demand.

        Supports pandas frequency strings: H (hourly), D (daily), W (weekly), etc.
        """
        periods_per_year = {
            'h': 8760,       # 365 * 24
            'H': 8760,
            'd': 365,
            'D': 365,
            'W': 52,
            'w': 52,
            'M': 12,
            'MS': 12,
        }

        periods = periods_per_year.get(frequency)
        if periods is None:
            # Default to daily if unknown frequency
            periods = 365

        return annual_units / periods

    @staticmethod
    def _load_geo_weights_csv(path: Union[str, Path]) -> List[GeographicWeight]:
        """Load geographic weights from CSV file.

        Required columns: zip3, weight
        Weights are normalized to sum to 1.0.
        """
        path = Path(path).expanduser()
        raw_weights = []

        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                zip3 = str(row['zip3']).strip().zfill(3)
                weight = float(row['weight'])
                if weight < 0:
                    raise ValueError(f"Geographic weight for zip3 {zip3} must be positive, got {weight}")
                raw_weights.append((zip3, weight))

        # Normalize weights to sum to 1.0
        total = sum(w for _, w in raw_weights)
        if total == 0:
            raise ValueError("Geographic weights sum to zero")

        return [
            GeographicWeight(zip3=z, weight=w / total)
            for z, w in raw_weights
        ]

    @staticmethod
    def _load_correlations_csv(path: Union[str, Path]) -> List[CorrelationConfig]:
        """Load correlations from CSV file.

        Required columns: source_product, target_product, coefficient
        Optional columns: lag, type, enabled
        """
        path = Path(path).expanduser()
        correlations = []

        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                enabled_val = row.get('enabled', 'true').strip().lower()
                enabled = enabled_val in ('true', '1', 'yes')

                correlation = CorrelationConfig(
                    source_product=row['source_product'].strip(),
                    target_product=row['target_product'].strip(),
                    coefficient=float(row['coefficient']),
                    lag=int(float(row.get('lag', 0))),
                    type=row.get('type', 'linear').strip(),
                    enabled=enabled,
                )
                correlations.append(correlation)

        return correlations

    @staticmethod
    def _parse_product(data: Dict[str, Any]) -> ProductConfig:
        """Parse product configuration from YAML dict."""
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
            annual_units=data.get('annual_units'),
            annual_orders=data.get('annual_orders'),
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
