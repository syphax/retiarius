"""Create a parcel zone table from ZIP3 centroids and mileage breaks.

Usage:
    python -m scimulator.utilities.create_zone_table [config_path]

    config_path : Optional path to a YAML config file. Defaults to
                  scimulator/utilities/config/zone_table_config.yaml.

Output:
    Writes a CSV to scimulator/utilities/output/zone_table-{name}.csv with columns:
        origin_zip3        — Origin ZIP3 (zero-padded)
        dest_zip3          — Destination ZIP3 (zero-padded)
        zone               — Zone label assigned from mileage breaks
        distance_haversine — Haversine distance between centroids
        distance_uom       — Unit of measure for distance
        transit_days_base  — Estimated transit days (distance / speed, rounded up)

How it works:
    1. Loads ZIP3 centroids from a reference file.
    2. Determines origin ZIP3s (from node_file or explicit list) and destination
       ZIP3s (explicit list or all in reference file).
    3. Computes haversine distance for each origin×destination pair.
    4. Assigns zones based on mileage break thresholds.
    5. Computes transit days from distance and average speed.

See config/zone_table_config.yaml for parameter documentation.
"""

import argparse
import csv
import math
import os

import pandas as pd
import yaml


# Earth radius in km
EARTH_RADIUS_KM = 6371.0

# Conversion factors to km
DISTANCE_TO_KM = {
    'km': 1.0,
    'mi': 1.609344,
}

KM_TO_DISTANCE = {k: 1.0 / v for k, v in DISTANCE_TO_KM.items()}

SPEED_UOM_MAP = {
    'km/day': 'km',
    'mi/day': 'mi',
}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute haversine distance in km between two lat/lon points."""
    lat1, lon1, lat2, lon2 = (math.radians(v) for v in (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def assign_zone(distance: float, mileage_breaks: dict) -> str | None:
    """Assign a zone based on distance and sorted mileage break thresholds."""
    for zone, max_dist in sorted(mileage_breaks.items(), key=lambda x: x[1]):
        if distance <= max_dist:
            return str(zone)
    return None


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(script_dir, "config", "zone_table_config.yaml")

    parser = argparse.ArgumentParser(description="Create a parcel zone table")
    parser.add_argument(
        "config",
        nargs="?",
        default=default_config,
        help="Path to YAML config file (default: %(default)s)",
    )
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    name = config["name"]
    distance_uom = config.get("distance_uom", "km")
    avg_speed = config["avg_transit_speed"]
    speed_uom = config.get("speed_uom", f"{distance_uom}/day")
    mileage_breaks = {str(k): float(v) for k, v in config["mileage_breaks"].items()}

    # Validate UoM
    if distance_uom not in DISTANCE_TO_KM:
        raise ValueError(f"Unsupported distance_uom: {distance_uom}. Use: {list(DISTANCE_TO_KM.keys())}")
    speed_distance_uom = SPEED_UOM_MAP.get(speed_uom)
    if speed_distance_uom is None:
        raise ValueError(f"Unsupported speed_uom: {speed_uom}. Use: {list(SPEED_UOM_MAP.keys())}")

    # Load ZIP3 centroids
    zip3_df = pd.read_csv(config["zip3_file"])
    zip3_df['zip3'] = zip3_df['zip3'].astype(str).str.zfill(3)
    zip3_lookup = {}
    for _, row in zip3_df.iterrows():
        lat, lon = row['latitude'], row['longitude']
        if pd.notna(lat) and pd.notna(lon):
            zip3_lookup[row['zip3']] = (float(lat), float(lon))
    print(f"Loaded {len(zip3_lookup)} ZIP3 centroids")

    # Determine origin ZIP3s
    origin_zip3s = config.get("origin_zip3s", [])
    if not origin_zip3s and config.get("node_file"):
        node_df = pd.read_csv(config["node_file"])
        node_df['zip3'] = node_df['zip3'].astype(str).str.zfill(3)
        origin_zip3s = sorted(node_df['zip3'].unique().tolist())
        print(f"Using {len(origin_zip3s)} origin ZIP3s from node file")
    elif origin_zip3s:
        origin_zip3s = [str(z).zfill(3) for z in origin_zip3s]
    else:
        origin_zip3s = sorted(zip3_lookup.keys())

    # Determine destination ZIP3s
    dest_zip3s = config.get("dest_zip3s", [])
    if not dest_zip3s:
        dest_zip3s = sorted(zip3_lookup.keys())
    else:
        dest_zip3s = [str(z).zfill(3) for z in dest_zip3s]

    # Validate all ZIP3s exist in the reference
    missing_origins = [z for z in origin_zip3s if z not in zip3_lookup]
    missing_dests = [z for z in dest_zip3s if z not in zip3_lookup]
    if missing_origins:
        print(f"WARNING: {len(missing_origins)} origin ZIP3s not in reference file, skipping: {missing_origins[:5]}...")
        origin_zip3s = [z for z in origin_zip3s if z in zip3_lookup]
    if missing_dests:
        print(f"WARNING: {len(missing_dests)} dest ZIP3s not in reference file, skipping: {missing_dests[:5]}...")
        dest_zip3s = [z for z in dest_zip3s if z in zip3_lookup]

    # Compute zone table
    km_to_output = KM_TO_DISTANCE[distance_uom]
    speed_km_per_day = avg_speed * DISTANCE_TO_KM[speed_distance_uom]

    output_dir = os.path.join(script_dir, "output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"zone_table-{name}.csv")

    row_count = 0
    zone_counts = {}
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["origin_zip3", "dest_zip3", "zone",
                         "distance_haversine", "distance_uom", "transit_days_base"])

        for o_zip3 in origin_zip3s:
            o_lat, o_lon = zip3_lookup[o_zip3]
            for d_zip3 in dest_zip3s:
                d_lat, d_lon = zip3_lookup[d_zip3]

                dist_km = haversine_km(o_lat, o_lon, d_lat, d_lon)
                dist_output = round(dist_km * km_to_output, 2)

                zone = assign_zone(dist_output, mileage_breaks)
                transit_days = math.ceil(dist_km / speed_km_per_day) if speed_km_per_day > 0 else None

                writer.writerow([o_zip3, d_zip3, zone,
                                 dist_output, distance_uom, transit_days])
                row_count += 1
                if zone:
                    zone_counts[zone] = zone_counts.get(zone, 0) + 1

    print(f"\nCreated {row_count} zone table entries in {output_path}")
    print(f"  Origins: {len(origin_zip3s)}, Destinations: {len(dest_zip3s)}")
    if zone_counts:
        print("\nZone distribution:")
        for zone in sorted(zone_counts.keys(), key=lambda z: (len(z), z)):
            print(f"  Zone {zone}: {zone_counts[zone]:>6,} lanes ({zone_counts[zone]/row_count*100:.1f}%)")


if __name__ == "__main__":
    main()
