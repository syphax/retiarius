# %% [markdown]
# # SCimulator DB Inspector
# Inspect tables, schemas, and row counts in a SCimulator DuckDB database.

# %%
import os
import duckdb

# Resolve path relative to repo root, regardless of cwd
_this_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()
_repo_root = os.path.normpath(os.path.join(_this_dir, '..', '..'))
db_path = os.path.join(_repo_root, 'scimulator', 'scenarios', 'scenario_test_static_10_100.duckdb')
conn = duckdb.connect(db_path, read_only=True)

# %% [markdown]
# ## Table summary

# %%
conn.execute("""
    SELECT
        t.table_name,
        (SELECT count(*) FROM information_schema.columns c
         WHERE c.table_name = t.table_name AND c.table_schema = 'main') AS columns
    FROM information_schema.tables t
    WHERE t.table_schema = 'main'
    ORDER BY t.table_name
""").df()

# %% [markdown]
# ## Row counts

# %%
tables = conn.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'main'
    ORDER BY table_name
""").fetchall()

rows = []
for (table_name,) in tables:
    count = conn.execute(f'SELECT count(*) FROM "{table_name}"').fetchone()[0]
    rows.append((table_name, count))

import pandas as pd
pd.DataFrame(rows, columns=['table', 'rows']).set_index('table')

# %% [markdown]
# ## Detailed schemas

# %%
for (table_name,) in tables:
    count = conn.execute(f'SELECT count(*) FROM "{table_name}"').fetchone()[0]
    schema = conn.execute(f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND table_schema = 'main'
        ORDER BY ordinal_position
    """).df()
    print(f"\n{'─' * 60}")
    print(f"  {table_name}  ({count:,} rows)")
    print(f"{'─' * 60}")
    print(schema.to_string(index=False))

# %% [markdown]
# ## Quick queries
# Use the cells below to explore interactively.

# %%
# Event log summary
conn.execute("""
    SELECT event_type, count(*) AS cnt, sum(quantity) AS total_qty
    FROM event_log
    GROUP BY event_type
    ORDER BY cnt DESC
""").df()

# %%
# Inventory snapshots over time
conn.execute("""
    SELECT sim_date, inventory_state, sum(quantity) AS total_qty, count(DISTINCT product_id) AS products
    FROM inventory_snapshot
    GROUP BY sim_date, inventory_state
    ORDER BY sim_date
""").df()

# %%
# Zone distribution of fulfilled demand
conn.execute("""
    SELECT
        detail::JSON->>'zone' AS zone,
        count(*) AS shipments,
        sum(quantity) AS units
    FROM event_log
    WHERE event_type IN ('demand_fulfilled', 'backorder_fulfilled')
      AND detail::JSON->>'zone' IS NOT NULL
    GROUP BY zone
    ORDER BY zone
""").df()

# %%
# Top 10 products by fulfilled units
conn.execute("""
    SELECT product_id, sum(quantity) AS fulfilled_units, count(*) AS shipments
    FROM event_log
    WHERE event_type = 'demand_fulfilled'
    GROUP BY product_id
    ORDER BY fulfilled_units DESC
    LIMIT 10
""").df()

# %%
# Distribution node utilization
conn.execute("""
    SELECT node_id, count(*) AS shipments, sum(quantity) AS units_shipped
    FROM event_log
    WHERE event_type IN ('demand_fulfilled', 'backorder_fulfilled')
    GROUP BY node_id
    ORDER BY units_shipped DESC
""").df()

# %%
# Inspect destinations

df_orders_by_dest = conn.execute("""
    SELECT
        edge_id,
        count(*) AS shipments,
        sum(quantity) AS units
    FROM event_log
    WHERE event_type IN ('demand_fulfilled', 'backorder_fulfilled')
    GROUP BY edge_id,
    ORDER BY edge_id
""").df()

display(df_orders_by_dest)

#df_orders_by_dest.plot(kind='bar', x='zone', y='units', title='Units shipped by zone')

# %%
