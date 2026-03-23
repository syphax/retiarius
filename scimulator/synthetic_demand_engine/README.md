# Synthetic Demand Generation Engine

A production-grade engine for generating realistic synthetic demand patterns with seasonality, trends, noise, anomalies, and cross-product correlations.

## Features

- **Multiple Seasonality Types**: Daily, weekly, monthly, yearly, and custom periods
- **Flexible Trends**: Linear, exponential, logarithmic, polynomial, and step trends with change points
- **Realistic Noise**: Gaussian, uniform, Poisson, and lognormal distributions
- **Anomaly Injection**: Spikes, drops, level shifts, trend changes, and missing data
- **Cross-Product Correlations**: Model relationships between products with configurable lags
- **Component Decomposition**: Track individual contributions (baseline, seasonality, trend, noise)
- **Multiple Export Formats**: CSV, Parquet, and JSON
- **Validation & Visualization**: Built-in quality checks and plotting utilities

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Optional: Install as package
pip install -e .
```

### Requirements

- Python >= 3.9
- numpy >= 1.24.0
- pandas >= 2.0.0
- matplotlib >= 3.7.0
- pyyaml >= 6.0
- pytest >= 7.4.0 (for testing)

## Quick Start

### 1. Using Python API

```python
from synthetic_demand_engine import (
    GenerationConfig, ProductConfig, DemandOrchestrator,
    SeasonalityConfig, SeasonalityType, TrendConfig, TrendType,
    NoiseConfig, NoiseType
)

# Define configuration
config = GenerationConfig(
    start_date="2024-01-01",
    end_date="2024-03-31",
    frequency="H",  # Hourly data
    products=[
        ProductConfig(
            product_id="PRODUCT_001",
            baseline_demand=100.0,
            seasonality=[
                SeasonalityConfig(
                    type=SeasonalityType.DAILY,
                    amplitude=30.0,
                    harmonics=2
                )
            ],
            trend=TrendConfig(
                type=TrendType.LINEAR,
                coefficient=0.05
            ),
            noise=NoiseConfig(
                type=NoiseType.GAUSSIAN,
                std_dev=10.0
            )
        )
    ],
    seed=42
)

# Generate demand patterns
orchestrator = DemandOrchestrator(config)
demands = orchestrator.generate()

# Access results
pattern = demands["PRODUCT_001"]
print(f"Generated {len(pattern.values)} data points")
print(f"Statistics: {pattern.get_statistics()}")

# Export to CSV
orchestrator.export(demands, "output.csv")
```

### 2. Using YAML Configuration

Create a configuration file (e.g., `config.yaml`):

```yaml
start_date: "2024-01-01"
end_date: "2024-03-31"
frequency: "H"
seed: 42
output_format: "csv"

products:
  - product_id: "PRODUCT_001"
    baseline_demand: 100.0
    seasonality:
      - type: "daily"
        amplitude: 30.0
        harmonics: 2
    trend:
      type: "linear"
      coefficient: 0.05
    noise:
      type: "gaussian"
      std_dev: 10.0
```

Then load and generate:

```python
from synthetic_demand_engine import ConfigLoader, DemandOrchestrator

# Load configuration
config = ConfigLoader.load("config.yaml")

# Generate and export
orchestrator = DemandOrchestrator(config)
demands = orchestrator.generate()
orchestrator.export(demands, "output.csv")
```

### 3. Using Command Line Interface

```bash
# Generate from YAML config
python -m synthetic_demand_engine.cli config.yaml -o output.csv --validate --verbose

# With visualization
python -m synthetic_demand_engine.cli config.yaml -o output.csv --plot output.png
```

## Configuration Examples

### Basic Single Product

See `config/example_basic.yaml` for a simple configuration with:
- Daily seasonality
- Linear trend
- Gaussian noise
- Occasional spikes

### Multiple Products with Correlations

See `config/example_multi.yaml` for:
- Multiple products
- Cross-product correlations
- Different seasonality patterns
- Lagged relationships

### Complex Scenario

See `config/example_complex.yaml` for an advanced example with:
- Multiple overlapping seasonalities
- Step trends with change points
- Multiple anomaly types
- Complex correlation networks
- Specific anomaly locations

## Configuration Reference

### Seasonality

```yaml
seasonality:
  - type: "daily" | "weekly" | "monthly" | "yearly" | "custom"
    amplitude: 30.0          # Peak amplitude
    period: 24               # Required for custom type
    phase_shift: 0.0         # Shift pattern in radians
    harmonics: 2             # Number of harmonic components
    enabled: true
```

### Trends

```yaml
trend:
  type: "linear" | "exponential" | "logarithmic" | "polynomial" | "step"
  coefficient: 0.05          # Trend strength
  exponent: 1.0              # For exponential/polynomial
  change_points:             # For step trends
    - index: 720             # Timestep
      value: 50.0            # Change amount
  enabled: true
```

### Noise

```yaml
noise:
  type: "gaussian" | "uniform" | "poisson" | "lognormal"
  std_dev: 10.0              # Standard deviation
  mean: 0.0                  # Mean value
  min_value: -20.0           # For uniform
  max_value: 20.0            # For uniform
  lambda_param: 10.0         # For Poisson
  sigma: 0.3                 # For lognormal
  enabled: true
```

### Anomalies

```yaml
anomalies:
  - type: "spike" | "drop" | "level_shift" | "trend_change" | "missing_data"
    probability: 0.01        # Random occurrence probability
    magnitude: 2.0           # Anomaly strength
    duration: 3              # Length in timesteps
    locations: [100, 500]    # Specific locations (optional)
    enabled: true
```

### Correlations

```yaml
correlations:
  - source_product: "PRODUCT_A"
    target_product: "PRODUCT_B"
    coefficient: 0.4         # Correlation strength
    lag: 24                  # Time lag in timesteps
    type: "linear" | "exponential"
    enabled: true
```

## Usage Examples

### Visualizing Patterns

```python
from synthetic_demand_engine.utils.visualization import DemandVisualizer
import matplotlib.pyplot as plt

# Plot single pattern with components
fig = DemandVisualizer.plot_pattern(
    pattern,
    show_components=True,
    show_anomalies=True
)
plt.savefig("demand_pattern.png")

# Plot multiple products
fig = DemandVisualizer.plot_multiple(demands)
plt.savefig("all_products.png")

# Plot statistics summary
fig = DemandVisualizer.plot_statistics(demands)
plt.savefig("statistics.png")
```

### Validating Generated Data

```python
from synthetic_demand_engine.utils.validation import DemandValidator

# Validate all patterns
results = DemandValidator.validate_all(demands)

for product_id, (is_valid, errors) in results.items():
    if not is_valid:
        print(f"{product_id} validation failed:")
        for error in errors:
            print(f"  - {error}")

# Get quality metrics
metrics = DemandValidator.check_quality_metrics(pattern)
print(f"Mean: {metrics['mean']:.2f}")
print(f"Std Dev: {metrics['std']:.2f}")
print(f"Coefficient of Variation: {metrics['cv']:.2f}")
print(f"Data Completeness: {metrics['completeness']:.2%}")
```

### Component Analysis

```python
# Access individual components
pattern = demands["PRODUCT_001"]

baseline = pattern.components['baseline']
seasonality = pattern.components['seasonality']
trend = pattern.components['trend']
noise = pattern.components['noise']

# Analyze contributions
import numpy as np
total_variance = np.var(pattern.values)
seasonality_variance = np.var(seasonality)
trend_variance = np.var(trend)

print(f"Seasonality explains {seasonality_variance/total_variance:.1%} of variance")
print(f"Trend explains {trend_variance/total_variance:.1%} of variance")
```

### Custom Time Ranges

```python
# Generate hourly data for Q1 2024
config = GenerationConfig(
    start_date="2024-01-01",
    end_date="2024-03-31",
    frequency="H",  # Hourly
    products=[...]
)

# Generate daily data for full year
config = GenerationConfig(
    start_date="2024-01-01",
    end_date="2024-12-31",
    frequency="D",  # Daily
    products=[...]
)

# Generate 15-minute intervals
config = GenerationConfig(
    start_date="2024-01-01",
    end_date="2024-01-07",
    frequency="15T",  # 15 minutes
    products=[...]
)
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest tests/test_engine.py -v

# Run specific test class
pytest tests/test_engine.py::TestSeasonalityGenerator -v

# Run with coverage
pytest tests/test_engine.py --cov=synthetic_demand_engine --cov-report=html
```

## Architecture

```
synthetic_demand_engine/
├── models.py                    # Data models and configurations
├── orchestrator.py              # Main generation pipeline
├── config/
│   └── loader.py               # YAML configuration loader
├── generators/
│   ├── patterns.py             # Seasonality and trend generators
│   ├── noise.py                # Noise and anomaly generators
│   └── correlations.py         # Cross-product correlation engine
├── utils/
│   ├── validation.py           # Validation utilities
│   └── visualization.py        # Plotting utilities
└── cli.py                      # Command-line interface
```

## Best Practices

1. **Reproducibility**: Always set a `seed` value for reproducible results
2. **Validation**: Use `--validate` flag to check generated patterns
3. **Constraints**: Set appropriate `min_demand` and `max_demand` bounds
4. **Correlations**: Avoid circular dependencies between products
5. **Performance**: Use `frequency="D"` for long time ranges instead of "H"
6. **Anomalies**: Use specific `locations` for known events, `probability` for random
7. **Export Format**: Use `parquet` for large datasets (more efficient than CSV)

## Common Patterns

### E-commerce Product

```yaml
baseline_demand: 500.0
seasonality:
  - type: "yearly"
    amplitude: 200.0
  - type: "weekly"
    amplitude: 100.0
  - type: "daily"
    amplitude: 50.0
    phase_shift: 1.57  # Peak in afternoon
trend:
  type: "step"
  change_points:
    - {index: 4320, value: 100.0}  # Marketing campaign
anomalies:
  - type: "spike"
    locations: [8640]  # Black Friday
    magnitude: 5.0
    duration: 48
```

### Seasonal Product

```yaml
baseline_demand: 100.0
seasonality:
  - type: "yearly"
    amplitude: 300.0
    phase_shift: 0.0  # Peak at year start
trend:
  type: "linear"
  coefficient: 0.02
```

### Complementary Products

```yaml
products:
  - product_id: "MAIN"
    baseline_demand: 200.0
  - product_id: "ACCESSORY"
    baseline_demand: 50.0

correlations:
  - source_product: "MAIN"
    target_product: "ACCESSORY"
    coefficient: 0.6
    lag: 48  # Accessories purchased 2 days later
```

## Troubleshooting

### Issue: Negative Demand Values

**Solution**: Set appropriate `min_demand` constraint:
```yaml
min_demand: 0.0
```

### Issue: Correlation Validation Errors

**Solution**: Check for:
- Circular dependencies between products
- Non-existent product IDs in correlations
- Source and target being the same product

### Issue: Memory Issues with Large Time Ranges

**Solution**: Use coarser frequency or split generation:
```yaml
frequency: "D"  # Daily instead of hourly
```

## Contributing

This is a self-contained demand generation engine. To extend:

1. Add new pattern types in `generators/patterns.py`
2. Add new noise distributions in `generators/noise.py`
3. Add new anomaly types in `generators/noise.py`
4. Update models in `models.py` with new configurations

## License

This project is provided as-is for demand forecasting and simulation purposes.

## Support

For questions or issues:
1. Check the example configurations in `config/`
2. Review the test suite in `tests/test_engine.py`
3. Run with `--verbose` flag for detailed output

## Examples

See the `example.py` script for a complete demonstration of all features.
