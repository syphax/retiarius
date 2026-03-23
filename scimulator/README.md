# Distribution SCimulator

A distribution network simulator that models product flows from suppliers to customers through N distribution layers. Designed for scenario-based exploration of network configurations, inventory policies, and fulfillment strategies.

**Current status:** Phase 2 (Visibility) — drawdown simulation with web UI dashboard.

## Setup

Requires **Python 3.13** and DuckDB 1.4.x. (DuckDB 1.5+ has a segfault bug on Python 3.13/3.14.)

```bash
# From the repo root:
# Create the venv (one-time)
python3.13 -m venv venv-3.13
source venv-3.13/bin/activate
pip install duckdb==1.4.4 pyyaml numpy pandas fastapi uvicorn python-multipart
```

If Python 3.13 isn't installed: `brew install python@3.13`

## Quick Start

```bash
# Activate the venv
source venv-3.13/bin/activate

# Run the sample scenario
python -m scimulator.simulator.cli run scimulator/simulator/tests/sample_scenario.yaml -v

# View results (CLI)
python -m scimulator.simulator.cli results scimulator/simulator/tests/sample_scenario.duckdb drawdown_sample_01

# Inspect what's in a database
python -m scimulator.simulator.cli inspect scimulator/simulator/tests/sample_scenario.duckdb

# Or use the web UI (see Web UI section below)
```

## How It Works

A **scenario** defines a simulation run by combining network topology (via entity sets) with simulation input data (via dataset versions). The CLI loads a scenario YAML, populates a DuckDB database, runs the simulation, and writes results back to DuckDB.

```
scenario.yaml ──> loader ──> DuckDB ──> engine ──> DuckDB (event_log, snapshots)
                               ↑
                          demand CSV
```

**Entity sets** select which suppliers, nodes, edges, and products participate in a scenario. **Dataset versions** select which variant of the demand, inventory, and inbound data to use. The engine filters simulation inputs against the active sets automatically.

## Scenario YAML Reference

A scenario file defines the complete simulation: network topology, products, demand source, initial inventory, and inbound shipments. See [simulator/tests/sample_scenario.yaml](simulator/tests/sample_scenario.yaml) for a working example.

### Top-level settings

```yaml
scenario_id: "my_scenario"          # Unique ID
name: "Q1 2024 Drawdown"           # Human-readable name
start_date: "2024-01-01"           # Simulation start
end_date: "2024-03-31"             # Simulation end
currency_code: "USD"                # ISO 4217
time_resolution: "daily"           # "daily" or "hourly"
backorder_probability: 0.8          # 0.0-1.0: probability unfulfilled demand is backordered vs. lost
write_event_log: true               # Write every event to the event_log table
write_snapshots: true               # Write periodic inventory snapshots
snapshot_interval_days: 7           # Snapshot every N days (1 = daily)
dataset_version_id: "v1"           # Identifier for this input dataset
demand_csv: "path/to/demand.csv"   # Path to demand data (from synthetic demand engine or manual)

# Entity set references (optional — NULL means "use all")
product_set_id: "my_products"              # Which products to include
supply_node_set_id: "domestic_suppliers"    # Which supply nodes to include
distribution_node_set_id: "east_coast"     # Which distribution nodes to include
demand_node_set_id: "northeast_customers"  # Which demand nodes to include
edge_set_id: null                          # Which edges (auto-filtered by active node sets)
```

### Network topology

**Suppliers** own one or more **supply nodes**. Supply nodes have a product catalog, lead times, and reliability attributes.

```yaml
suppliers:
  - supplier_id: "SUP_01"
    name: "Domestic Supplier"
    default_lead_time: 3.0            # days
    default_qty_reliability: 0.95     # probability of shipping full quantity

supply_nodes:
  - supply_node_id: "SN_01"
    supplier_id: "SUP_01"
    name: "Ohio Factory"
    latitude: 40.0
    longitude: -82.5
    tags: ["domestic"]
    products: ["SKU_A", "SKU_B"]      # Which products this node can supply
```

**Distribution nodes** are warehouses/DCs that hold inventory and fulfill orders. They have capacity limits (soft constraints — overages incur penalty costs) and cost structures.

```yaml
distribution_nodes:
  - dist_node_id: "DC_EAST"
    name: "East Coast DC"
    latitude: 40.31
    longitude: -74.51
    tags: ["national_dc"]
    storage_capacity: 5000.0          # m3 (null = unlimited)
    max_outbound: 500.0               # units/day (null = unlimited)
    max_outbound_uom: "unit"
    order_response_time: 1.0          # days from order to shipment
    fixed_cost: 2500.0                # per day
    fixed_cost_basis: "per_day"
    variable_cost: 1.50               # per unit shipped
    variable_cost_basis: "per_unit"
```

**Demand nodes** represent customers or customer aggregations (e.g., ZIP3 regions). If your demand CSV has a `zip3` column, demand nodes are auto-created — you don't need to list them in the YAML.

**Edges** define transportation links between nodes. The engine uses these to route fulfillment: for each demand node, it tries distribution nodes in order of lowest variable cost.

```yaml
edges:
  # Supply → Distribution
  - edge_id: "E_SUP_DC"
    origin_node_id: "SN_01"
    origin_node_type: "supply"        # "supply" or "distribution"
    dest_node_id: "DC_EAST"
    dest_node_type: "distribution"    # "distribution" or "demand"
    transport_type: "tl"              # parcel, air, tl, ltl, flex, ocean_dray, etc.
    mean_transit_time: 2.0            # days
    cost_fixed: 500.0
    cost_variable: 0.10
    cost_variable_basis: "per_unit"

  # Distribution → Demand
  - edge_id: "E_DC_Z100"
    origin_node_id: "DC_EAST"
    origin_node_type: "distribution"
    dest_node_id: "Z100"
    dest_node_type: "demand"
    transport_type: "parcel"
    mean_transit_time: 2.0
    cost_variable: 5.50
    cost_variable_basis: "per_unit"
```

### Products

```yaml
products:
  - product_id: "SKU_A"
    name: "Widget A"
    standard_cost: 10.0               # unit cost in scenario currency
    base_price: 20.0                  # selling price
    weight: 10.0                      # kg
    cube: 2.0                         # liters
    attributes:                       # optional key-value pairs
      brand: "Acme"
      category: "Widgets"
```

### Initial inventory

Pre-loaded inventory at distribution nodes. This is what gets consumed during a drawdown simulation.

```yaml
initial_inventory:
  - dist_node_id: "DC_EAST"
    product_id: "SKU_A"
    inventory_state: "saleable"       # saleable, received, damaged, etc.
    quantity: 400
```

### Inbound schedule

Pre-determined shipments arriving at distribution nodes during the simulation. In drawdown mode, this is the only way new inventory enters the network.

```yaml
inbound_schedule:
  - inbound_id: "INB_001"
    supply_node_id: "SN_01"
    dest_node_id: "DC_EAST"
    product_id: "SKU_A"
    quantity: 300
    ship_date: "2024-01-28"
    arrival_date: "2024-01-31"
```

## Getting Data In

### Demand data

The simulator reads demand from a CSV file (pointed to by `demand_csv` in the scenario YAML). Two formats are supported:

**From the synthetic demand engine** (order ledger mode):

| order_id | timestamp | part_number | zip3 | quantity |
|----------|-----------|-------------|------|----------|
| uuid... | 2024-01-01 08:23:02 | SKU_A | 606 | 1 |

**Manual/custom format:**

| demand_id | demand_date | demand_node_id | product_id | quantity |
|-----------|-------------|----------------|------------|----------|
| D001 | 2024-01-01 | Z606 | SKU_A | 1 |

Column mapping:
- `part_number` or `product_id` — product identifier (must match a product in the YAML)
- `zip3` or `demand_node_id` — customer location (zip3 values get auto-prefixed with "Z")
- `timestamp` or `demand_date` — when the demand occurs
- `order_id` or `demand_id` — unique event identifier (auto-generated if missing)

#### Generating demand with the synthetic demand engine

```bash
python -m scimulator.synthetic_demand_engine.cli \
  scimulator/synthetic_demand_engine/config/example_csv.yaml \
  -o my_demand.csv \
  --products-csv scimulator/data/products_demand.csv \
  --geo-weights-csv scimulator/data/geo_weights.csv \
  --verbose
```

See `scimulator/data/products_demand.csv` for the product demand config format and `scimulator/data/geo_weights.csv` for geographic weight distribution.

### Everything else

Network entities (suppliers, DCs, edges, products) can be loaded into the database and selected via **entity sets**, or defined inline in the scenario YAML for convenience (useful for testing and small scenarios). Initial inventory and inbound schedules are scoped to a dataset version and can also be inlined in the YAML.

### Querying the database directly

After a simulation run, the output is a DuckDB file you can query with any DuckDB client:

```bash
# Interactive SQL shell
duckdb my_scenario.duckdb

# Or from Python
python -c "
import duckdb
conn = duckdb.connect('my_scenario.duckdb', read_only=True)
print(conn.execute('SELECT * FROM event_log LIMIT 10').df())
"
```

Key output tables:
- **event_log** — every simulation event (demand, fulfillment, shipments, backorders, costs)
- **inventory_snapshot** — periodic inventory positions by node/product/state
- **run_metadata** — run timing, status, config snapshot

Key input tables (also in the DB after loading):
- **demand** — all demand events (scoped to dataset version)
- **product** — product master
- **distribution_node**, **supply_node**, **edge** — network topology
- **\*_set**, **\*_set_member** — entity sets defining which entities participate in a scenario

## Web UI

Phase 2 adds a FastAPI backend + React frontend for viewing simulation results in the browser.

```bash
# Terminal 1: Backend
source venv-3.13/bin/activate
python -m scimulator.web --data-dir scimulator/simulator/tests

# Terminal 2: Frontend
cd scimulator/web/frontend
npm install && npm run dev

# Terminal 3 (optional): Flow visualizer
cd scimulator/flow_viz
npm install && npm run dev

# Open http://localhost:5173
```

The dashboard shows scenario KPIs, inventory charts, paginated event logs, and export options.

## Project Structure

```
scimulator/
├── simulator/                    # Simulation engine
│   ├── cli.py                    # CLI entry point (run, results, inspect)
│   ├── db.py                     # DuckDB schema creation
│   ├── engine.py                 # Drawdown simulation engine
│   ├── loader.py                 # YAML + CSV → DuckDB loader
│   ├── models.py                 # Scenario config dataclasses
│   └── tests/
│       ├── sample_scenario.yaml  # Working example scenario
│       ├── sample_demand.csv     # Generated demand data (7,747 events)
│       └── test_simulation.py    # End-to-end test
├── web/                          # Web UI (FastAPI + React)
│   ├── app.py                    # FastAPI application
│   ├── api/                      # API route handlers
│   ├── services/                 # Query helpers, flow data transform
│   └── frontend/                 # React + Vite + TypeScript
├── synthetic_demand_engine/      # Demand generation engine
├── flow_viz/                     # Flow visualization (Deck.GL + MapLibre)
├── data/                         # Shared input data (CSVs)
└── utilities/                    # Utility scripts
```

## Running Tests

```bash
python scimulator/simulator/tests/test_simulation.py
```

## Design Spec

Design specs are in the `retiarius-private` repo under `prompts/`.
