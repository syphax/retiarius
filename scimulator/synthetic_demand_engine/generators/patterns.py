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
