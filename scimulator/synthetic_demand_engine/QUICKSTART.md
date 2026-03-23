# Quick Start Guide

## Installation

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Verify installation:**
   ```bash
   python verify_installation.py
   ```

## Run Your First Example

### Option 1: Run the Example Script

```bash
python example.py
```

This will generate 6 different examples showcasing various features of the engine and create visualization plots.

### Option 2: Use a Configuration File

```bash
python -m synthetic_demand_engine.cli config/example_basic.yaml -o output.csv --verbose
```

### Option 3: Python API

```python
from synthetic_demand_engine import (
    GenerationConfig, ProductConfig, DemandOrchestrator,
    SeasonalityConfig, SeasonalityType, NoiseConfig, NoiseType
)

# Create configuration
config = GenerationConfig(
    start_date="2024-01-01",
    end_date="2024-01-31",
    frequency="H",
    products=[
        ProductConfig(
            product_id="MY_PRODUCT",
            baseline_demand=100.0,
            seasonality=[
                SeasonalityConfig(
                    type=SeasonalityType.DAILY,
                    amplitude=20.0
                )
            ],
            noise=NoiseConfig(
                type=NoiseType.GAUSSIAN,
                std_dev=5.0
            )
        )
    ],
    seed=42
)

# Generate demand
orchestrator = DemandOrchestrator(config)
demands = orchestrator.generate()

# Export results
orchestrator.export(demands, "my_output.csv")
```

## What's Included

- **3 Example YAML Configs**: basic, multi-product, and complex scenarios
- **Comprehensive Tests**: Full test suite covering all features
- **6 Example Scripts**: Demonstrating different use cases
- **Documentation**: Complete README with API reference

## Next Steps

1. Explore the example configurations in `config/`
2. Read the full documentation in `README.md`
3. Run the test suite: `pytest tests/test_engine.py -v`
4. Customize configurations for your use case

## Getting Help

- Check `README.md` for detailed documentation
- Review `example.py` for code examples
- Look at YAML configs in `config/` for configuration examples
- Run tests to see how components work: `pytest tests/test_engine.py -v -k test_name`
