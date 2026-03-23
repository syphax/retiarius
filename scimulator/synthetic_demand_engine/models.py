"""
Data models and configurations for synthetic demand generation.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum
from datetime import datetime
import numpy as np


class SeasonalityType(Enum):
    """Types of seasonality patterns."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"
    CUSTOM = "custom"


class TrendType(Enum):
    """Types of trend patterns."""
    NONE = "none"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    LOGARITHMIC = "logarithmic"
    POLYNOMIAL = "polynomial"
    STEP = "step"


class NoiseType(Enum):
    """Types of noise distributions."""
    GAUSSIAN = "gaussian"
    UNIFORM = "uniform"
    POISSON = "poisson"
    LOGNORMAL = "lognormal"


class AnomalyType(Enum):
    """Types of anomalies."""
    SPIKE = "spike"
    DROP = "drop"
    LEVEL_SHIFT = "level_shift"
    TREND_CHANGE = "trend_change"
    MISSING_DATA = "missing_data"


@dataclass
class SeasonalityConfig:
    """Configuration for seasonality patterns."""
    type: SeasonalityType
    amplitude: float
    period: Optional[int] = None  # For custom seasonality
    phase_shift: float = 0.0
    harmonics: int = 1
    enabled: bool = True


@dataclass
class TrendConfig:
    """Configuration for trend patterns."""
    type: TrendType
    coefficient: float = 0.0
    exponent: float = 1.0
    change_points: List[Dict[str, Any]] = field(default_factory=list)
    enabled: bool = True


@dataclass
class NoiseConfig:
    """Configuration for noise injection."""
    type: NoiseType
    std_dev: Optional[float] = None
    mean: float = 0.0
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    lambda_param: Optional[float] = None  # For Poisson
    sigma: Optional[float] = None  # For lognormal
    enabled: bool = True


@dataclass
class AnomalyConfig:
    """Configuration for anomaly injection."""
    type: AnomalyType
    probability: float
    magnitude: float
    duration: int = 1
    locations: Optional[List[int]] = None
    enabled: bool = True


@dataclass
class CorrelationConfig:
    """Configuration for cross-product correlations."""
    source_product: str
    target_product: str
    coefficient: float
    lag: int = 0
    type: str = "linear"  # or "exponential"
    enabled: bool = True


@dataclass
class GeographicWeight:
    """Geographic weight for demand distribution across ZIP3 regions."""
    zip3: str
    weight: float


@dataclass
class ProductConfig:
    """Configuration for a single product's demand pattern."""
    product_id: str
    baseline_demand: float
    seasonality: List[SeasonalityConfig] = field(default_factory=list)
    trend: Optional[TrendConfig] = None
    noise: Optional[NoiseConfig] = None
    anomalies: List[AnomalyConfig] = field(default_factory=list)
    min_demand: float = 0.0
    max_demand: Optional[float] = None
    annual_units: Optional[float] = None
    annual_orders: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GenerationConfig:
    """Main configuration for demand generation."""
    start_date: str
    end_date: str
    frequency: str = "H"  # Pandas frequency string
    products: List[ProductConfig] = field(default_factory=list)
    correlations: List[CorrelationConfig] = field(default_factory=list)
    geographic_weights: List[GeographicWeight] = field(default_factory=list)
    seed: Optional[int] = None
    output_format: str = "csv"  # csv, parquet, json
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DemandPattern:
    """Generated demand pattern for a product."""
    product_id: str
    timestamps: np.ndarray
    values: np.ndarray
    components: Dict[str, np.ndarray] = field(default_factory=dict)
    anomaly_mask: Optional[np.ndarray] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            'product_id': self.product_id,
            'timestamps': self.timestamps.tolist() if isinstance(self.timestamps, np.ndarray) else self.timestamps,
            'values': self.values.tolist() if isinstance(self.values, np.ndarray) else self.values,
            'components': {k: v.tolist() for k, v in self.components.items()},
            'anomaly_mask': self.anomaly_mask.tolist() if self.anomaly_mask is not None else None,
            'metadata': self.metadata,
            'statistics': self.get_statistics()
        }

    def get_statistics(self) -> Dict[str, float]:
        """Calculate basic statistics."""
        valid_values = self.values[~np.isnan(self.values)]
        return {
            'mean': float(np.mean(valid_values)),
            'std': float(np.std(valid_values)),
            'min': float(np.min(valid_values)),
            'max': float(np.max(valid_values)),
            'median': float(np.median(valid_values)),
            'anomaly_count': int(np.sum(self.anomaly_mask)) if self.anomaly_mask is not None else 0
        }
