"""
Multi-product correlation engine.
"""

import numpy as np
from typing import List, Dict, Optional
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
