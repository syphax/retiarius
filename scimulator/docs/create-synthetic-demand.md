# How to Create Synthetic Demand

## Create Product Master

If you don't already have a product master (which has product_id, annual demand, and annual orders), create one using the product master generator:

TODO: Add product master generator command

## Create Demand

Use the synthetic demand engine to create a demand dataset:

Example command:

`python -m scimulator.synthetic_demand_engine.cli -o ./data/dmd_model_test_100.csv  ./scimulator/synthetic_demand_engine/config/model_test_100.yaml`