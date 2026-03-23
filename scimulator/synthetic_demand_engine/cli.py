"""
Command-line interface for synthetic demand generation.
"""

import argparse
import sys
from pathlib import Path

from .config.loader import ConfigLoader
from .orchestrator import DemandOrchestrator
from .utils.validation import DemandValidator
from .utils.visualization import DemandVisualizer


def _plot_order_ledger(ledger):
    """Generate summary plots for an order ledger DataFrame."""
    import matplotlib.pyplot as plt
    import pandas as pd

    parts = sorted(ledger['part_number'].unique())
    fig, axes = plt.subplots(len(parts), 1, figsize=(14, 3 * len(parts)), sharex=True)
    if len(parts) == 1:
        axes = [axes]

    ts = pd.to_datetime(ledger['timestamp'])
    for ax, pn in zip(axes, parts):
        mask = ledger['part_number'] == pn
        daily = ts[mask].dt.date.value_counts().sort_index()
        ax.bar(daily.index, daily.values, width=1.0, alpha=0.7)
        ax.set_ylabel('Orders/day')
        ax.set_title(f'{pn} — {mask.sum()} orders, {ledger.loc[mask, "quantity"].sum()} units')

    axes[-1].set_xlabel('Date')
    fig.suptitle('Order Ledger: Daily Order Counts by Product', fontsize=14)
    fig.tight_layout()
    return fig


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Synthetic Demand Generation Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        'config',
        type=str,
        help='Path to YAML configuration file'
    )

    parser.add_argument(
        '-o', '--output',
        type=str,
        required=True,
        help='Output file path'
    )

    parser.add_argument(
        '--products-csv',
        type=str,
        help='Path to products CSV file (overrides products in YAML)'
    )

    parser.add_argument(
        '--geo-weights-csv',
        type=str,
        help='Path to geographic weights CSV file (zip3, weight)'
    )

    parser.add_argument(
        '--correlations-csv',
        type=str,
        help='Path to correlations CSV file (overrides correlations in YAML)'
    )

    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate generated patterns'
    )

    parser.add_argument(
        '--plot',
        type=str,
        help='Save plots to this path'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Verbose output'
    )

    args = parser.parse_args()

    try:
        # Load configuration
        if args.verbose:
            print(f"Loading configuration from {args.config}...")

        config = ConfigLoader.load(
            args.config,
            products_csv=args.products_csv,
            geo_weights_csv=args.geo_weights_csv,
            correlations_csv=args.correlations_csv,
        )

        if args.verbose:
            if args.products_csv:
                print(f"  Products loaded from CSV: {args.products_csv}")
            if args.geo_weights_csv:
                print(f"  Geographic weights loaded from CSV: {args.geo_weights_csv}")
            if args.correlations_csv:
                print(f"  Correlations loaded from CSV: {args.correlations_csv}")

        # Generate demands
        if args.verbose:
            print(f"Generating demand patterns for {len(config.products)} products...")
            if config.geographic_weights:
                print(f"  Distributing across {len(config.geographic_weights)} ZIP3 regions")

        orchestrator = DemandOrchestrator(config)
        demands = orchestrator.generate()

        # Print generation summary
        ledger = orchestrator.get_order_ledger()
        if ledger is not None and args.verbose:
            print(f"Generated {len(ledger)} order events")
            for pn in ledger['part_number'].unique():
                part_rows = ledger[ledger['part_number'] == pn]
                print(f"  {pn}: {len(part_rows)} orders, {part_rows['quantity'].sum()} total units")
        elif args.verbose:
            print(f"Generated {len(demands)} demand patterns")

        # Validate (signal mode only)
        if args.validate and demands:
            if args.verbose:
                print("Validating patterns...")

            validation_results = DemandValidator.validate_all(demands)

            all_valid = True
            for pid, (is_valid, errors) in validation_results.items():
                if not is_valid:
                    all_valid = False
                    print(f"Validation failed for {pid}:")
                    for error in errors:
                        print(f"  - {error}")

            if all_valid and args.verbose:
                print("All patterns validated successfully")

        # Export
        if args.verbose:
            print(f"Exporting to {args.output}...")

        orchestrator.export(demands, args.output)

        if args.verbose:
            print("Export complete")

        # Plot
        if args.plot:
            if args.verbose:
                print(f"Generating plots to {args.plot}...")

            import matplotlib
            matplotlib.use('Agg')

            if ledger is not None and not ledger.empty:
                fig = _plot_order_ledger(ledger)
            else:
                fig = DemandVisualizer.plot_multiple(demands)

            fig.savefig(args.plot, dpi=150, bbox_inches='tight')

            if args.verbose:
                print("Plots saved")

        print("Success!")
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
