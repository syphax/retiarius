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
