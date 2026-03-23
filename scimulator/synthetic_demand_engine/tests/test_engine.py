"""
Comprehensive test suite for Synthetic Demand Generation Engine.
"""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path
import tempfile
import os

# Import engine components
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from synthetic_demand_engine.models import (
    SeasonalityType, TrendType, NoiseType, AnomalyType,
    SeasonalityConfig, TrendConfig, NoiseConfig, AnomalyConfig,
    CorrelationConfig, ProductConfig, GenerationConfig, DemandPattern
)
from synthetic_demand_engine.generators.patterns import (
    SeasonalityGenerator, TrendGenerator, BaselineGenerator
)
from synthetic_demand_engine.generators.noise import NoiseGenerator, AnomalyGenerator
from synthetic_demand_engine.generators.correlations import CorrelationEngine
from synthetic_demand_engine.orchestrator import DemandOrchestrator
from synthetic_demand_engine.config.loader import ConfigLoader
from synthetic_demand_engine.utils.validation import DemandValidator


class TestSeasonalityGenerator:
    """Test seasonality pattern generation."""

    def test_daily_seasonality(self):
        """Test daily seasonality generation."""
        config = SeasonalityConfig(
            type=SeasonalityType.DAILY,
            amplitude=10.0,
            harmonics=1
        )
        timestamps = np.arange(48)  # 2 days
        result = SeasonalityGenerator.generate(timestamps, config)

        assert len(result) == 48
        assert not np.all(result == 0)
        # Check periodicity: values at 24 hours apart should be similar
        assert np.allclose(result[0], result[24], rtol=0.1)

    def test_weekly_seasonality(self):
        """Test weekly seasonality generation."""
        config = SeasonalityConfig(
            type=SeasonalityType.WEEKLY,
            amplitude=20.0,
            harmonics=2
        )
        timestamps = np.arange(24 * 7 * 2)  # 2 weeks
        result = SeasonalityGenerator.generate(timestamps, config)

        assert len(result) == 24 * 7 * 2
        assert np.max(np.abs(result)) > 0

    def test_disabled_seasonality(self):
        """Test that disabled seasonality returns zeros."""
        config = SeasonalityConfig(
            type=SeasonalityType.DAILY,
            amplitude=10.0,
            enabled=False
        )
        timestamps = np.arange(24)
        result = SeasonalityGenerator.generate(timestamps, config)

        assert np.all(result == 0)

    def test_custom_seasonality(self):
        """Test custom period seasonality."""
        config = SeasonalityConfig(
            type=SeasonalityType.CUSTOM,
            amplitude=15.0,
            period=12,  # 12-hour period
            harmonics=1
        )
        timestamps = np.arange(48)
        result = SeasonalityGenerator.generate(timestamps, config)

        assert len(result) == 48
        # Check custom periodicity
        assert np.allclose(result[0], result[12], rtol=0.1)


class TestTrendGenerator:
    """Test trend pattern generation."""

    def test_linear_trend(self):
        """Test linear trend generation."""
        config = TrendConfig(
            type=TrendType.LINEAR,
            coefficient=1.0
        )
        timestamps = np.arange(100)
        result = TrendGenerator.generate(timestamps, config)

        assert len(result) == 100
        # Linear trend: check that differences are constant
        diffs = np.diff(result)
        assert np.allclose(diffs, 1.0)

    def test_exponential_trend(self):
        """Test exponential trend generation."""
        config = TrendConfig(
            type=TrendType.EXPONENTIAL,
            coefficient=0.1,
            exponent=1.01
        )
        timestamps = np.arange(100)
        result = TrendGenerator.generate(timestamps, config)

        assert len(result) == 100
        assert result[-1] > result[0]

    def test_step_trend(self):
        """Test step trend with change points."""
        config = TrendConfig(
            type=TrendType.STEP,
            change_points=[
                {'index': 25, 'value': 10.0},
                {'index': 50, 'value': 20.0},
                {'index': 75, 'value': -5.0}
            ]
        )
        timestamps = np.arange(100)
        result = TrendGenerator.generate(timestamps, config)

        assert len(result) == 100
        assert result[24] == 0.0  # Before first change
        assert result[25] == 10.0  # At first change
        assert result[50] == 30.0  # Cumulative: 10 + 20
        assert result[75] == 25.0  # Cumulative: 10 + 20 - 5

    def test_no_trend(self):
        """Test that NONE trend returns zeros."""
        config = TrendConfig(type=TrendType.NONE)
        timestamps = np.arange(100)
        result = TrendGenerator.generate(timestamps, config)

        assert np.all(result == 0)


class TestNoiseGenerator:
    """Test noise generation."""

    def test_gaussian_noise(self):
        """Test Gaussian noise generation."""
        config = NoiseConfig(
            type=NoiseType.GAUSSIAN,
            mean=0.0,
            std_dev=5.0
        )
        gen = NoiseGenerator(seed=42)
        result = gen.generate(1000, config)

        assert len(result) == 1000
        # Check approximate mean and std
        assert abs(np.mean(result)) < 1.0
        assert abs(np.std(result) - 5.0) < 1.0

    def test_uniform_noise(self):
        """Test uniform noise generation."""
        config = NoiseConfig(
            type=NoiseType.UNIFORM,
            min_value=-10.0,
            max_value=10.0
        )
        gen = NoiseGenerator(seed=42)
        result = gen.generate(1000, config)

        assert len(result) == 1000
        assert np.min(result) >= -10.0
        assert np.max(result) <= 10.0

    def test_poisson_noise(self):
        """Test Poisson noise generation."""
        config = NoiseConfig(
            type=NoiseType.POISSON,
            lambda_param=10.0
        )
        gen = NoiseGenerator(seed=42)
        result = gen.generate(1000, config)

        assert len(result) == 1000
        # Mean should be near zero (centered)
        assert abs(np.mean(result)) < 1.0


class TestAnomalyGenerator:
    """Test anomaly injection."""

    def test_spike_anomaly(self):
        """Test spike anomaly generation."""
        values = np.ones(100) * 100.0
        config = AnomalyConfig(
            type=AnomalyType.SPIKE,
            probability=0.0,
            magnitude=2.0,
            duration=3,
            locations=[50]
        )
        gen = AnomalyGenerator(seed=42)
        result, mask = gen.generate(values, config)

        assert len(result) == 100
        assert len(mask) == 100
        assert np.sum(mask) == 3  # Duration
        assert result[50] == 200.0  # Spike magnitude

    def test_drop_anomaly(self):
        """Test drop anomaly generation."""
        values = np.ones(100) * 100.0
        config = AnomalyConfig(
            type=AnomalyType.DROP,
            probability=0.0,
            magnitude=0.5,
            duration=2,
            locations=[40]
        )
        gen = AnomalyGenerator(seed=42)
        result, mask = gen.generate(values, config)

        assert result[40] == 50.0  # Drop magnitude

    def test_missing_data_anomaly(self):
        """Test missing data anomaly."""
        values = np.ones(100) * 100.0
        config = AnomalyConfig(
            type=AnomalyType.MISSING_DATA,
            probability=0.0,
            magnitude=0.0,
            duration=5,
            locations=[30]
        )
        gen = AnomalyGenerator(seed=42)
        result, mask = gen.generate(values, config)

        assert np.isnan(result[30:35]).all()

    def test_multiple_anomalies(self):
        """Test applying multiple anomaly configurations."""
        values = np.ones(100) * 100.0
        configs = [
            AnomalyConfig(
                type=AnomalyType.SPIKE,
                probability=0.0,
                magnitude=2.0,
                duration=1,
                locations=[20]
            ),
            AnomalyConfig(
                type=AnomalyType.DROP,
                probability=0.0,
                magnitude=0.5,
                duration=1,
                locations=[50]
            )
        ]
        gen = AnomalyGenerator(seed=42)
        result, mask = gen.generate_multiple(values, configs)

        assert result[20] == 200.0
        assert result[50] == 50.0
        assert np.sum(mask) == 2


class TestCorrelationEngine:
    """Test correlation handling."""

    def test_linear_correlation(self):
        """Test linear correlation between products."""
        demands = {
            'A': np.ones(100) * 100.0,
            'B': np.ones(100) * 50.0
        }
        config = CorrelationConfig(
            source_product='A',
            target_product='B',
            coefficient=0.5,
            type='linear'
        )
        engine = CorrelationEngine(seed=42)
        result = engine.apply_correlations(demands, [config])

        assert 'A' in result
        assert 'B' in result
        # B should be modified based on A's deviations from mean
        assert not np.array_equal(result['B'], demands['B'])

    def test_lagged_correlation(self):
        """Test correlation with time lag."""
        demands = {
            'A': np.arange(100, dtype=float),
            'B': np.ones(100) * 50.0
        }
        config = CorrelationConfig(
            source_product='A',
            target_product='B',
            coefficient=0.3,
            lag=5,
            type='linear'
        )
        engine = CorrelationEngine(seed=42)
        result = engine.apply_correlations(demands, [config])

        assert len(result['B']) == 100

    def test_correlation_validation(self):
        """Test correlation validation."""
        product_ids = ['A', 'B', 'C']

        # Valid correlation
        valid_config = CorrelationConfig(
            source_product='A',
            target_product='B',
            coefficient=0.5
        )

        # Invalid: source doesn't exist
        invalid_config = CorrelationConfig(
            source_product='X',
            target_product='B',
            coefficient=0.5
        )

        engine = CorrelationEngine()
        errors = engine.validate_correlations([valid_config], product_ids)
        assert len(errors) == 0

        errors = engine.validate_correlations([invalid_config], product_ids)
        assert len(errors) > 0


class TestDemandOrchestrator:
    """Test main orchestration."""

    def test_simple_generation(self):
        """Test basic demand generation."""
        config = GenerationConfig(
            start_date="2024-01-01",
            end_date="2024-01-07",
            frequency="H",
            products=[
                ProductConfig(
                    product_id="TEST_001",
                    baseline_demand=100.0,
                    min_demand=0.0,
                    max_demand=500.0
                )
            ],
            seed=42
        )

        orchestrator = DemandOrchestrator(config)
        demands = orchestrator.generate()

        assert 'TEST_001' in demands
        pattern = demands['TEST_001']
        assert len(pattern.values) == len(pattern.timestamps)
        assert pattern.product_id == "TEST_001"

    def test_generation_with_all_components(self):
        """Test generation with all components."""
        config = GenerationConfig(
            start_date="2024-01-01",
            end_date="2024-01-31",
            frequency="H",
            products=[
                ProductConfig(
                    product_id="COMPLEX_001",
                    baseline_demand=200.0,
                    seasonality=[
                        SeasonalityConfig(
                            type=SeasonalityType.DAILY,
                            amplitude=20.0
                        )
                    ],
                    trend=TrendConfig(
                        type=TrendType.LINEAR,
                        coefficient=0.1
                    ),
                    noise=NoiseConfig(
                        type=NoiseType.GAUSSIAN,
                        std_dev=10.0
                    ),
                    anomalies=[
                        AnomalyConfig(
                            type=AnomalyType.SPIKE,
                            probability=0.01,
                            magnitude=2.0
                        )
                    ],
                    min_demand=0.0,
                    max_demand=1000.0
                )
            ],
            seed=42
        )

        orchestrator = DemandOrchestrator(config)
        demands = orchestrator.generate()

        pattern = demands['COMPLEX_001']
        assert 'baseline' in pattern.components
        assert 'seasonality' in pattern.components
        assert 'trend' in pattern.components
        assert 'noise' in pattern.components
        assert pattern.anomaly_mask is not None

    def test_export_csv(self):
        """Test CSV export."""
        config = GenerationConfig(
            start_date="2024-01-01",
            end_date="2024-01-02",
            frequency="H",
            products=[
                ProductConfig(
                    product_id="EXPORT_TEST",
                    baseline_demand=100.0
                )
            ],
            output_format="csv"
        )

        orchestrator = DemandOrchestrator(config)
        demands = orchestrator.generate()

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name

        try:
            orchestrator.export(demands, temp_path)
            assert os.path.exists(temp_path)

            # Verify CSV content
            df = pd.read_csv(temp_path)
            assert 'timestamp' in df.columns
            assert 'EXPORT_TEST_demand' in df.columns
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


class TestConfigLoader:
    """Test YAML configuration loading."""

    def test_load_basic_config(self):
        """Test loading basic configuration."""
        yaml_content = """
start_date: "2024-01-01"
end_date: "2024-01-31"
frequency: "H"
seed: 42

products:
  - product_id: "TEST_PRODUCT"
    baseline_demand: 100.0
    seasonality:
      - type: "daily"
        amplitude: 10.0
        harmonics: 1
    noise:
      type: "gaussian"
      std_dev: 5.0
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            config = ConfigLoader.load(temp_path)
            assert config.start_date == "2024-01-01"
            assert config.end_date == "2024-01-31"
            assert config.seed == 42
            assert len(config.products) == 1
            assert config.products[0].product_id == "TEST_PRODUCT"
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


class TestDemandValidator:
    """Test validation utilities."""

    def test_valid_pattern(self):
        """Test validation of valid pattern."""
        pattern = DemandPattern(
            product_id="TEST",
            timestamps=np.arange(100),
            values=np.random.rand(100) * 100 + 50,
            components={}
        )

        is_valid, errors = DemandValidator.validate_pattern(pattern)
        assert is_valid
        assert len(errors) == 0

    def test_invalid_pattern_with_nan(self):
        """Test validation catches NaN values."""
        values = np.ones(100) * 100.0
        values[50] = np.nan

        pattern = DemandPattern(
            product_id="TEST",
            timestamps=np.arange(100),
            values=values,
            components={}
        )

        is_valid, errors = DemandValidator.validate_pattern(pattern)
        assert not is_valid
        assert any('NaN' in error for error in errors)

    def test_invalid_pattern_with_negatives(self):
        """Test validation catches negative values."""
        values = np.ones(100) * 100.0
        values[25] = -10.0

        pattern = DemandPattern(
            product_id="TEST",
            timestamps=np.arange(100),
            values=values,
            components={}
        )

        is_valid, errors = DemandValidator.validate_pattern(pattern)
        assert not is_valid
        assert any('negative' in error for error in errors)

    def test_quality_metrics(self):
        """Test quality metrics calculation."""
        pattern = DemandPattern(
            product_id="TEST",
            timestamps=np.arange(100),
            values=np.random.rand(100) * 100 + 50,
            components={}
        )

        metrics = DemandValidator.check_quality_metrics(pattern)
        assert 'mean' in metrics
        assert 'std' in metrics
        assert 'cv' in metrics
        assert 'completeness' in metrics
        assert metrics['completeness'] == 1.0


class TestIntegration:
    """Integration tests."""

    def test_end_to_end_basic(self):
        """Test complete workflow from config to export."""
        yaml_content = """
start_date: "2024-01-01"
end_date: "2024-01-07"
frequency: "H"
seed: 100
output_format: "csv"

products:
  - product_id: "INTEGRATION_TEST"
    baseline_demand: 150.0
    min_demand: 50.0
    max_demand: 500.0
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
    anomalies:
      - type: "spike"
        probability: 0.01
        magnitude: 2.0
        duration: 2
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        output_path = tempfile.mktemp(suffix='.csv')

        try:
            # Load config
            config = ConfigLoader.load(config_path)

            # Generate
            orchestrator = DemandOrchestrator(config)
            demands = orchestrator.generate()

            # Validate
            results = DemandValidator.validate_all(demands)
            assert all(valid for valid, _ in results.values())

            # Export
            orchestrator.export(demands, output_path)
            assert os.path.exists(output_path)

            # Verify export
            df = pd.read_csv(output_path)
            assert len(df) > 0
            assert 'timestamp' in df.columns
            assert 'INTEGRATION_TEST_demand' in df.columns

        finally:
            if os.path.exists(config_path):
                os.unlink(config_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_end_to_end_with_correlations(self):
        """Test complete workflow with correlated products."""
        yaml_content = """
start_date: "2024-01-01"
end_date: "2024-01-14"
frequency: "H"
seed: 200
output_format: "csv"

products:
  - product_id: "PRODUCT_A"
    baseline_demand: 200.0
    seasonality:
      - type: "weekly"
        amplitude: 50.0
    noise:
      type: "gaussian"
      std_dev: 15.0

  - product_id: "PRODUCT_B"
    baseline_demand: 100.0
    noise:
      type: "gaussian"
      std_dev: 10.0

correlations:
  - source_product: "PRODUCT_A"
    target_product: "PRODUCT_B"
    coefficient: 0.4
    lag: 0
    type: "linear"
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            config = ConfigLoader.load(config_path)
            orchestrator = DemandOrchestrator(config)
            demands = orchestrator.generate()

            assert 'PRODUCT_A' in demands
            assert 'PRODUCT_B' in demands
            assert len(demands['PRODUCT_A'].values) == len(demands['PRODUCT_B'].values)

        finally:
            if os.path.exists(config_path):
                os.unlink(config_path)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
