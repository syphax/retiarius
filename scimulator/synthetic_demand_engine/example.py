#!/usr/bin/env python3
"""
Example demonstration of the Synthetic Demand Generation Engine.

This script demonstrates all major features:
- Basic demand generation
- Multiple seasonality types
- Trends and noise
- Anomaly injection
- Cross-product correlations
- Validation and visualization
- Export to various formats
"""

import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from synthetic_demand_engine import (
    GenerationConfig, ProductConfig, DemandOrchestrator,
    SeasonalityConfig, SeasonalityType,
    TrendConfig, TrendType,
    NoiseConfig, NoiseType,
    AnomalyConfig, AnomalyType,
    CorrelationConfig,
    ConfigLoader
)
from synthetic_demand_engine.utils.validation import DemandValidator
from synthetic_demand_engine.utils.visualization import DemandVisualizer


def example_1_basic():
    """Example 1: Basic demand pattern with seasonality."""
    print("\n" + "="*70)
    print("EXAMPLE 1: Basic Demand Pattern")
    print("="*70)

    config = GenerationConfig(
        start_date="2024-01-01",
        end_date="2024-01-31",
        frequency="H",
        products=[
            ProductConfig(
                product_id="BASIC_PRODUCT",
                baseline_demand=100.0,
                seasonality=[
                    SeasonalityConfig(
                        type=SeasonalityType.DAILY,
                        amplitude=30.0,
                        harmonics=2
                    )
                ],
                min_demand=0.0,
                max_demand=500.0
            )
        ],
        seed=42
    )

    orchestrator = DemandOrchestrator(config)
    demands = orchestrator.generate()

    pattern = demands["BASIC_PRODUCT"]
    stats = pattern.get_statistics()

    print(f"Generated {len(pattern.values)} hourly data points")
    print(f"Mean demand: {stats['mean']:.2f}")
    print(f"Std deviation: {stats['std']:.2f}")
    print(f"Min demand: {stats['min']:.2f}")
    print(f"Max demand: {stats['max']:.2f}")

    # Visualize
    fig = DemandVisualizer.plot_pattern(pattern, show_components=True)
    plt.savefig("example_1_basic.png", dpi=150, bbox_inches='tight')
    print("Saved plot: example_1_basic.png")

    # Export
    orchestrator.export(demands, "example_1_output.csv")
    print("Saved data: example_1_output.csv")

    plt.close(fig)


def example_2_complex_pattern():
    """Example 2: Complex pattern with multiple components."""
    print("\n" + "="*70)
    print("EXAMPLE 2: Complex Pattern with All Components")
    print("="*70)

    config = GenerationConfig(
        start_date="2024-01-01",
        end_date="2024-03-31",
        frequency="H",
        products=[
            ProductConfig(
                product_id="COMPLEX_PRODUCT",
                baseline_demand=200.0,
                seasonality=[
                    SeasonalityConfig(
                        type=SeasonalityType.WEEKLY,
                        amplitude=50.0,
                        harmonics=3
                    ),
                    SeasonalityConfig(
                        type=SeasonalityType.DAILY,
                        amplitude=30.0,
                        harmonics=2,
                        phase_shift=1.57  # Peak in afternoon
                    )
                ],
                trend=TrendConfig(
                    type=TrendType.LINEAR,
                    coefficient=0.1
                ),
                noise=NoiseConfig(
                    type=NoiseType.GAUSSIAN,
                    std_dev=15.0
                ),
                anomalies=[
                    AnomalyConfig(
                        type=AnomalyType.SPIKE,
                        probability=0.01,
                        magnitude=2.5,
                        duration=3
                    ),
                    AnomalyConfig(
                        type=AnomalyType.DROP,
                        probability=0.005,
                        magnitude=0.5,
                        duration=4
                    )
                ],
                min_demand=0.0,
                max_demand=1000.0
            )
        ],
        seed=123
    )

    orchestrator = DemandOrchestrator(config)
    demands = orchestrator.generate()

    pattern = demands["COMPLEX_PRODUCT"]
    stats = pattern.get_statistics()

    print(f"Generated {len(pattern.values)} hourly data points over 3 months")
    print(f"\nDemand Statistics:")
    print(f"  Mean: {stats['mean']:.2f}")
    print(f"  Std Dev: {stats['std']:.2f}")
    print(f"  Range: [{stats['min']:.2f}, {stats['max']:.2f}]")
    print(f"  Anomalies detected: {stats['anomaly_count']}")

    print(f"\nComponent Analysis:")
    for comp_name, comp_values in pattern.components.items():
        comp_var = np.var(comp_values)
        print(f"  {comp_name.capitalize()}: variance = {comp_var:.2f}")

    # Visualize
    fig = DemandVisualizer.plot_pattern(
        pattern,
        show_components=True,
        show_anomalies=True
    )
    plt.savefig("example_2_complex.png", dpi=150, bbox_inches='tight')
    print("\nSaved plot: example_2_complex.png")

    plt.close(fig)


def example_3_multiple_products():
    """Example 3: Multiple products with correlations."""
    print("\n" + "="*70)
    print("EXAMPLE 3: Multiple Products with Correlations")
    print("="*70)

    config = GenerationConfig(
        start_date="2024-01-01",
        end_date="2024-02-29",
        frequency="H",
        products=[
            # Main product
            ProductConfig(
                product_id="PRODUCT_A",
                baseline_demand=250.0,
                seasonality=[
                    SeasonalityConfig(
                        type=SeasonalityType.WEEKLY,
                        amplitude=60.0,
                        harmonics=2
                    )
                ],
                trend=TrendConfig(
                    type=TrendType.EXPONENTIAL,
                    coefficient=0.0001,
                    exponent=1.002
                ),
                noise=NoiseConfig(
                    type=NoiseType.GAUSSIAN,
                    std_dev=20.0
                ),
                min_demand=50.0,
                max_demand=800.0
            ),
            # Complementary product
            ProductConfig(
                product_id="PRODUCT_B",
                baseline_demand=100.0,
                seasonality=[
                    SeasonalityConfig(
                        type=SeasonalityType.WEEKLY,
                        amplitude=25.0,
                        harmonics=1
                    )
                ],
                noise=NoiseConfig(
                    type=NoiseType.GAUSSIAN,
                    std_dev=10.0
                ),
                min_demand=10.0,
                max_demand=400.0
            ),
            # Accessory product
            ProductConfig(
                product_id="PRODUCT_C",
                baseline_demand=50.0,
                noise=NoiseConfig(
                    type=NoiseType.UNIFORM,
                    min_value=-5.0,
                    max_value=5.0
                ),
                min_demand=5.0,
                max_demand=250.0
            )
        ],
        correlations=[
            # B follows A with 40% correlation
            CorrelationConfig(
                source_product="PRODUCT_A",
                target_product="PRODUCT_B",
                coefficient=0.4,
                lag=0,
                type="linear"
            ),
            # C follows A with 24-hour lag
            CorrelationConfig(
                source_product="PRODUCT_A",
                target_product="PRODUCT_C",
                coefficient=0.25,
                lag=24,
                type="linear"
            )
        ],
        seed=999
    )

    orchestrator = DemandOrchestrator(config)
    demands = orchestrator.generate()

    print(f"Generated demand for {len(demands)} products")

    for product_id, pattern in demands.items():
        stats = pattern.get_statistics()
        print(f"\n{product_id}:")
        print(f"  Mean: {stats['mean']:.2f}")
        print(f"  Std Dev: {stats['std']:.2f}")
        print(f"  Range: [{stats['min']:.2f}, {stats['max']:.2f}]")

    # Calculate correlations between products
    print("\nEmpirical Correlations:")
    values_a = demands["PRODUCT_A"].values
    values_b = demands["PRODUCT_B"].values
    values_c = demands["PRODUCT_C"].values

    corr_ab = np.corrcoef(values_a, values_b)[0, 1]
    corr_ac = np.corrcoef(values_a[:-24], values_c[24:])[0, 1]  # Account for lag

    print(f"  A-B correlation: {corr_ab:.3f} (configured: 0.4)")
    print(f"  A-C correlation (24h lag): {corr_ac:.3f} (configured: 0.25)")

    # Visualize
    fig = DemandVisualizer.plot_multiple(demands)
    plt.savefig("example_3_multi.png", dpi=150, bbox_inches='tight')
    print("\nSaved plot: example_3_multi.png")

    # Statistics plot
    fig2 = DemandVisualizer.plot_statistics(demands)
    plt.savefig("example_3_stats.png", dpi=150, bbox_inches='tight')
    print("Saved statistics plot: example_3_stats.png")

    plt.close('all')


def example_4_yaml_config():
    """Example 4: Loading from YAML configuration."""
    print("\n" + "="*70)
    print("EXAMPLE 4: Loading from YAML Configuration")
    print("="*70)

    config_path = Path("config/example_basic.yaml")

    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        print("Make sure you're running from the project root directory.")
        return

    print(f"Loading configuration from: {config_path}")

    config = ConfigLoader.load(config_path)

    print(f"\nConfiguration details:")
    print(f"  Date range: {config.start_date} to {config.end_date}")
    print(f"  Frequency: {config.frequency}")
    print(f"  Products: {len(config.products)}")
    print(f"  Seed: {config.seed}")

    orchestrator = DemandOrchestrator(config)
    demands = orchestrator.generate()

    # Validate
    print("\nValidating generated patterns...")
    results = DemandValidator.validate_all(demands)

    all_valid = True
    for product_id, (is_valid, errors) in results.items():
        if is_valid:
            print(f"  {product_id}: VALID")
        else:
            print(f"  {product_id}: INVALID")
            for error in errors:
                print(f"    - {error}")
            all_valid = False

    if all_valid:
        print("\nAll patterns validated successfully!")

    # Export
    orchestrator.export(demands, "example_4_output.csv")
    print("\nSaved data: example_4_output.csv")


def example_5_anomalies():
    """Example 5: Different types of anomalies."""
    print("\n" + "="*70)
    print("EXAMPLE 5: Anomaly Types Demonstration")
    print("="*70)

    config = GenerationConfig(
        start_date="2024-01-01",
        end_date="2024-01-15",
        frequency="H",
        products=[
            ProductConfig(
                product_id="SPIKE_DEMO",
                baseline_demand=100.0,
                anomalies=[
                    AnomalyConfig(
                        type=AnomalyType.SPIKE,
                        locations=[100],
                        magnitude=3.0,
                        duration=6,
                        probability=1.0
                    )
                ]
            ),
            ProductConfig(
                product_id="DROP_DEMO",
                baseline_demand=100.0,
                anomalies=[
                    AnomalyConfig(
                        type=AnomalyType.DROP,
                        locations=[100],
                        magnitude=0.3,
                        duration=6,
                        probability=1.0
                    )
                ]
            ),
            ProductConfig(
                product_id="LEVEL_SHIFT_DEMO",
                baseline_demand=100.0,
                anomalies=[
                    AnomalyConfig(
                        type=AnomalyType.LEVEL_SHIFT,
                        locations=[168],  # One week in
                        magnitude=0.5,
                        duration=1,
                        probability=1.0
                    )
                ]
            )
        ],
        seed=777
    )

    orchestrator = DemandOrchestrator(config)
    demands = orchestrator.generate()

    print("Generated patterns with specific anomalies:")
    for product_id, pattern in demands.items():
        anomaly_count = np.sum(pattern.anomaly_mask) if pattern.anomaly_mask is not None else 0
        print(f"  {product_id}: {anomaly_count} anomalous points")

    # Visualize each anomaly type
    fig, axes = plt.subplots(3, 1, figsize=(15, 10))

    for idx, (product_id, ax) in enumerate(zip(demands.keys(), axes)):
        pattern = demands[product_id]
        ax.plot(pattern.timestamps, pattern.values, linewidth=1, label='Demand')

        if pattern.anomaly_mask is not None:
            anomaly_idx = np.where(pattern.anomaly_mask)[0]
            if len(anomaly_idx) > 0:
                ax.scatter(
                    pattern.timestamps[anomaly_idx],
                    pattern.values[anomaly_idx],
                    color='red', s=30, label='Anomaly', zorder=5
                )

        ax.axhline(y=100, color='gray', linestyle='--', alpha=0.5, label='Baseline')
        ax.set_ylabel('Demand')
        ax.set_title(product_id.replace('_', ' '))
        ax.legend()
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel('Time')
    plt.tight_layout()
    plt.savefig("example_5_anomalies.png", dpi=150, bbox_inches='tight')
    print("\nSaved plot: example_5_anomalies.png")

    plt.close(fig)


def example_6_step_trend():
    """Example 6: Step trend with marketing campaigns."""
    print("\n" + "="*70)
    print("EXAMPLE 6: Step Trend (Marketing Campaigns)")
    print("="*70)

    config = GenerationConfig(
        start_date="2024-01-01",
        end_date="2024-12-31",
        frequency="D",  # Daily for full year
        products=[
            ProductConfig(
                product_id="CAMPAIGN_PRODUCT",
                baseline_demand=500.0,
                seasonality=[
                    SeasonalityConfig(
                        type=SeasonalityType.WEEKLY,
                        amplitude=50.0,
                        harmonics=1
                    )
                ],
                trend=TrendConfig(
                    type=TrendType.STEP,
                    change_points=[
                        {'index': 90, 'value': 100.0},    # Q2 campaign
                        {'index': 180, 'value': 150.0},   # Q3 campaign
                        {'index': 270, 'value': -50.0}    # Competition impact
                    ]
                ),
                noise=NoiseConfig(
                    type=NoiseType.GAUSSIAN,
                    std_dev=30.0
                ),
                min_demand=100.0,
                max_demand=2000.0
            )
        ],
        seed=2024
    )

    orchestrator = DemandOrchestrator(config)
    demands = orchestrator.generate()

    pattern = demands["CAMPAIGN_PRODUCT"]
    stats = pattern.get_statistics()

    print(f"Generated {len(pattern.values)} days of demand data")
    print(f"\nStatistics:")
    print(f"  Mean: {stats['mean']:.2f}")
    print(f"  Std Dev: {stats['std']:.2f}")
    print(f"  Range: [{stats['min']:.2f}, {stats['max']:.2f}]")

    # Visualize with change points marked
    fig, ax = plt.subplots(1, 1, figsize=(15, 6))
    ax.plot(pattern.timestamps, pattern.values, linewidth=1, label='Demand')

    # Mark change points
    change_points = [90, 180, 270]
    for cp in change_points:
        if cp < len(pattern.timestamps):
            ax.axvline(x=pattern.timestamps[cp], color='red', linestyle='--',
                      alpha=0.7, linewidth=2)

    ax.set_xlabel('Date')
    ax.set_ylabel('Demand')
    ax.set_title('Product Demand with Marketing Campaigns (Step Trend)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("example_6_step_trend.png", dpi=150, bbox_inches='tight')
    print("\nSaved plot: example_6_step_trend.png")

    plt.close(fig)


def main():
    """Run all examples."""
    print("\n" + "="*70)
    print("SYNTHETIC DEMAND GENERATION ENGINE - EXAMPLES")
    print("="*70)

    try:
        example_1_basic()
        example_2_complex_pattern()
        example_3_multiple_products()
        example_4_yaml_config()
        example_5_anomalies()
        example_6_step_trend()

        print("\n" + "="*70)
        print("ALL EXAMPLES COMPLETED SUCCESSFULLY!")
        print("="*70)
        print("\nGenerated files:")
        print("  - example_1_basic.png")
        print("  - example_1_output.csv")
        print("  - example_2_complex.png")
        print("  - example_3_multi.png")
        print("  - example_3_stats.png")
        print("  - example_4_output.csv")
        print("  - example_5_anomalies.png")
        print("  - example_6_step_trend.png")
        print("\nCheck these files to see the generated demand patterns!")

    except Exception as e:
        print(f"\nError running examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
