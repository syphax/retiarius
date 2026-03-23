from setuptools import setup, find_packages

setup(
    name="synthetic-demand-engine",
    version="1.0.0",
    description="Production-grade synthetic demand generation engine",
    packages=find_packages(),
    install_requires=[
        "numpy>=1.24.0",
        "pandas>=2.0.0",
        "matplotlib>=3.7.0",
        "pyyaml>=6.0",
    ],
    entry_points={
        'console_scripts': [
            'synth-demand=synthetic_demand_engine.cli:main',
        ],
    },
    python_requires=">=3.9",
)
