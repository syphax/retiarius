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
