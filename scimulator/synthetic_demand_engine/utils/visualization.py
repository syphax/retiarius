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
