"""
Synthetic Demand Generation Engine

A production-grade engine for generating realistic synthetic demand patterns
with seasonality, trends, noise, anomalies, and cross-product correlations.
"""

__version__ = "1.0.0"

from .models import (
    SeasonalityType,
    TrendType,
    NoiseType,
    AnomalyType,
    SeasonalityConfig,
    TrendConfig,
    NoiseConfig,
    AnomalyConfig,
    CorrelationConfig,
    GeographicWeight,
    ProductConfig,
    GenerationConfig,
    DemandPattern,
)

from .orchestrator import DemandOrchestrator
from .config.loader import ConfigLoader

__all__ = [
    'SeasonalityType',
    'TrendType',
    'NoiseType',
    'AnomalyType',
    'SeasonalityConfig',
    'TrendConfig',
    'NoiseConfig',
    'AnomalyConfig',
    'CorrelationConfig',
    'GeographicWeight',
    'ProductConfig',
    'GenerationConfig',
    'DemandPattern',
    'DemandOrchestrator',
    'ConfigLoader',
]
