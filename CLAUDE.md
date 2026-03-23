# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Repository Purpose

This is the **Distribution SCimulator** (codename: Retiarius) — a distribution network simulator that models product flows from suppliers to customers through N distribution layers. Designed for scenario-based exploration of network configurations, inventory policies, and fulfillment strategies.

## Project Structure

```
.
├── scimulator/                  # Main Python package
│   ├── simulator/               # Simulation engine (Phase 1)
│   │   ├── cli.py               # CLI: run, results, inspect
│   │   ├── db.py                # DuckDB schema (40+ tables)
│   │   ├── engine.py            # Drawdown simulation engine
│   │   ├── loader.py            # YAML + CSV → DuckDB loader
│   │   ├── models.py            # Scenario config dataclasses
│   │   └── tests/               # End-to-end tests + sample scenario
│   ├── web/                     # Web UI (Phase 2)
│   │   ├── app.py               # FastAPI application
│   │   ├── api/                 # REST API routes
│   │   ├── services/            # Query helpers, flow data transform
│   │   └── frontend/            # React + Vite + TypeScript
│   ├── synthetic_demand_engine/ # Demand generation engine
│   ├── flow_viz/                # Flow visualization (Deck.GL + MapLibre)
│   ├── data/                    # Shared input data (CSVs, gitignored)
│   └── utilities/               # Utility scripts
└── retiarius-private/           # Private submodule (separate repo)
    └── prompts/                 # Design specs and prompts
```

## Development Setup

### Language and Stack
- **Python 3.13** — simulation engine and API
- **DuckDB 1.4.4** — database (1.5+ has segfault on Python 3.13/3.14)
- **FastAPI + Uvicorn** — web backend
- **React 18 + TypeScript + Vite** — web frontend
- **Deck.GL + MapLibre** — flow visualization

### Environment Setup

```bash
python3.13 -m venv venv-3.13
source venv-3.13/bin/activate
pip install duckdb==1.4.4 pyyaml numpy pandas fastapi uvicorn python-multipart
```

### Running the Simulator

```bash
# CLI
python -m scimulator.simulator.cli run scimulator/simulator/tests/sample_scenario.yaml -v
python -m scimulator.simulator.cli results <db_path> <scenario_id>

# Web UI
python -m scimulator.web --data-dir scimulator/simulator/tests  # backend on :8000
cd scimulator/web/frontend && npm run dev                        # frontend on :5173
```

### Running Tests

```bash
python scimulator/simulator/tests/test_simulation.py
```

## Key Design Decisions

- **Simulation engine is stateless** — all state lives in DuckDB
- **Scenario-centric** — a scenario fully defines a simulation run
- **Sparse storage** — only non-zero inventory values are stored
- **Soft capacity constraints** — overages are allowed but penalized
- **Entity sets** — named subsets control which entities participate per scenario
- **Dataset versions** — immutable, named input data variants
- **Modular policies** — fulfillment/ordering logic are swappable (Phase 3+)

## Data Format

- **Parquet/CSV** for input data, **DuckDB** for everything else
- Scenario config is YAML
- All monetary values: DECIMAL(12,4)
- Explicit UoM columns on all measured values
- Cost fields use `_basis` columns (per_unit, per_day, pct_value, etc.)

## Development Phases

- **Phase 1** (complete): DuckDB schema, drawdown engine, CLI
- **Phase 2** (complete): FastAPI + React web UI, results dashboard, charts
- **Phase 3** (next): Order fulfillment logic, supplier ordering, transport mode selection
- **Phase 4**: Batch runs, scenario comparison
- **Phase 5**: AI agent interface, mobile UI, Docker deployment
