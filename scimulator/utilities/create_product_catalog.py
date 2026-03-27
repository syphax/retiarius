"""Create an artificial product catalog with configurable demand distributions.

Usage:
    python -m scimulator.utilities.create_product_catalog [config_path]

    config_path : Optional path to a YAML config file. Defaults to
                  scimulator/utilities/config/product_catalog_config.yaml.

Output:
    Writes a CSV to scimulator/utilities/output/products-{name}.csv with columns:
        product_id   — Sequential alphabetic identifier (e.g. AAAA, AAAB, ...)
        annual_units  — Simulated annual unit demand for this product
        annual_orders — Simulated annual order count for this product

    Products are sorted by descending demand (highest-demand product first),
    following the shape of the configured demand curve. After writing, the script
    prints summary statistics including demand concentration metrics (e.g. "top
    10% of products account for X% of units").

How it works:
    1. Generates sequential part numbers of configurable length.
    2. Distributes total annual units across products using a demand curve PDF
       (log-logistic or lognormal), so a small number of products capture most
       demand (long-tail distribution).
    3. Optionally applies Gaussian noise to add per-product variation.
    4. Derives order counts from unit demand using a configurable units-per-order
       ratio.

See config/product_catalog_config.yaml for parameter documentation.
"""

import argparse
import csv
import os
import string

import numpy as np
import yaml


def generate_product_ids(count: int, length: int) -> list[str]:
    """Generate sequential alphabetic part numbers (AAA, AAB, AAC, ...)."""
    max_parts = 26 ** length
    if count > max_parts:
        raise ValueError(
            f"Cannot generate {count} part numbers with length {length} "
            f"(max {max_parts})"
        )

    product_ids = []
    for i in range(count):
        chars = []
        n = i
        for _ in range(length):
            chars.append(string.ascii_uppercase[n % 26])
            n //= 26
        product_ids.append("".join(reversed(chars)))
    return product_ids


def log_logistic_pdf(x: np.ndarray, k: float, x0: float) -> np.ndarray:
    """Log-logistic PDF: (k/x0)(x/x0)^(k-1) / (1 + (x/x0)^k)^2."""
    ratio = x / x0
    return (k / x0) * ratio ** (k - 1) / (1 + ratio**k) ** 2


def lognormal_pdf(x: np.ndarray, mu: float, sigma: float) -> np.ndarray:
    """Lognormal PDF: (1/(x*sigma*sqrt(2pi))) * exp(-(ln(x)-mu)^2 / (2*sigma^2))."""
    return (
        1 / (x * sigma * np.sqrt(2 * np.pi))
        * np.exp(-((np.log(x) - mu) ** 2) / (2 * sigma**2))
    )


def compute_demand(
    count: int,
    total_units: int,
    curve_shape: str,
    shape_params: dict,
    noise_factor: float,
    rng: np.random.Generator,
) -> np.ndarray:
    """Compute demand per product using a PDF, then apply noise and rounding."""
    x = np.linspace(1 / count, 1.0, count)

    if curve_shape == "log-logistic":
        raw = log_logistic_pdf(x, k=shape_params["k"], x0=shape_params["x0"])
    elif curve_shape == "lognormal":
        raw = lognormal_pdf(x, mu=shape_params["mu"], sigma=shape_params["sigma"])
    else:
        raise ValueError(f"Unknown demand curve shape: {curve_shape}")

    # Normalize to total units
    demand = raw / raw.sum() * total_units

    # Apply noise: demand *= (1 + noise_factor/2 * z)
    if noise_factor > 0:
        z = rng.standard_normal(count)
        demand = demand * (1 + (noise_factor / 2) * z)

    # Clamp negatives to 0, then round
    demand = np.maximum(demand, 0)
    demand = np.rint(demand).astype(int)

    return demand


def compute_orders(units: np.ndarray, mode: str, ratio: float) -> np.ndarray:
    """Compute order counts from unit demand."""
    if mode == "constant":
        orders = np.rint(units / ratio).astype(int)
        # Ensure: if units > 0 then orders >= 1; if units == 0 then orders == 0
        orders = np.where((units > 0) & (orders < 1), 1, orders)
        orders = np.where(units == 0, 0, orders)
        return orders
    else:
        raise ValueError(f"Unknown qty_per_order_shape: {mode}")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(script_dir, "config", "product_catalog_config.yaml")

    parser = argparse.ArgumentParser(description="Create an artificial product catalog")
    parser.add_argument(
        "config",
        nargs="?",
        default=default_config,
        help="Path to YAML config file (default: %(default)s)",
    )
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    # Set up RNG
    seed = config.get("seed", None)
    rng = np.random.default_rng(seed)

    name = config["name"]
    count = config["cnt_products"]
    length = config["length_pn"]
    total_units = config["sum_units"]
    noise_factor = config.get("noise_factor", 0.0)
    curve_shape = config["demand_curve_shape"]
    shape_params = config["demand_curve_params"]
    order_mode = config.get("qty_per_order_shape", "constant")
    order_ratio = config.get("qty_per_order_ratio", 1.0)

    # Generate
    product_ids = generate_product_ids(count, length)
    demand = compute_demand(count, total_units, curve_shape, shape_params, noise_factor, rng)
    orders = compute_orders(demand, order_mode, order_ratio)

    # Write output
    output_dir = os.path.join(script_dir, "output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"products-{name}.csv")

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "annual_units", "annual_orders"])
        for pn, units, ords in zip(product_ids, demand, orders):
            writer.writerow([pn, units, ords])

    print(f"Created {count} products in {output_path}")
    print(f"Total units: {demand.sum():,} (target: {total_units:,})")
    print(f"Total orders: {orders.sum():,}")

    # Concentration metrics
    total = demand.sum()
    if total > 0:
        print("\nDemand concentration:")
        for pct in [1, 10, 20, 50]:
            idx = max(1, int(count * pct / 100))
            share = demand[:idx].sum() / total * 100
            print(f"  Top {pct:2d}% ({idx:>4d} products): {share:5.1f}% of units")


if __name__ == "__main__":
    main()
