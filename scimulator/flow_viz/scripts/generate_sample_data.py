#!/usr/bin/env python3
"""Generate sample outbound order CSV data for the flow visualization."""

import csv
import random
import math
from datetime import datetime, timedelta

# US warehouse/DC locations
ORIGINS = [
    {"name": "Edison NJ DC", "lat": 40.5187, "lng": -74.4121},
    {"name": "Atlanta GA DC", "lat": 33.6407, "lng": -84.4277},
    {"name": "Dallas TX DC", "lat": 32.8998, "lng": -96.8379},
    {"name": "Chicago IL DC", "lat": 41.8500, "lng": -87.8820},
    {"name": "Reno NV DC", "lat": 39.5296, "lng": -119.8138},
    {"name": "Memphis TN DC", "lat": 35.1175, "lng": -89.9711},
]

# Major US metro areas as delivery destinations (weighted by population)
DESTINATIONS = [
    (40.7128, -74.0060, 8.3),   # New York
    (34.0522, -118.2437, 4.0),  # Los Angeles
    (41.8781, -87.6298, 2.7),   # Chicago
    (29.7604, -95.3698, 2.3),   # Houston
    (33.4484, -112.0740, 1.7),  # Phoenix
    (29.4241, -98.4936, 1.5),   # San Antonio
    (32.7157, -117.1611, 1.4),  # San Diego
    (32.7767, -96.7970, 1.3),   # Dallas
    (37.3382, -121.8863, 1.0),  # San Jose
    (30.2672, -97.7431, 1.0),   # Austin
    (39.7392, -104.9903, 0.7),  # Denver
    (47.6062, -122.3321, 0.7),  # Seattle
    (42.3601, -71.0589, 0.7),   # Boston
    (25.7617, -80.1918, 0.5),   # Miami
    (33.7490, -84.3880, 0.5),   # Atlanta
    (38.9072, -77.0369, 0.7),   # Washington DC
    (35.2271, -80.8431, 0.9),   # Charlotte
    (36.1627, -86.7816, 0.7),   # Nashville
    (39.9612, -82.9988, 0.9),   # Columbus OH
    (44.9778, -93.2650, 0.4),   # Minneapolis
    (27.9506, -82.4572, 0.4),   # Tampa
    (38.6270, -90.1994, 0.3),   # St Louis
    (39.7684, -86.1581, 0.9),   # Indianapolis
    (35.1495, -90.0490, 0.7),   # Memphis
    (36.1540, -95.9928, 0.4),   # Tulsa
    (41.2524, -95.9980, 0.5),   # Omaha
    (43.0389, -87.9065, 0.6),   # Milwaukee
    (28.5383, -81.3792, 0.3),   # Orlando
]

BRANDS = ["Alpine", "Cascade", "Horizon", "Summit", "Pacific", "Metro", "Atlas", "Ridge"]

def haversine_miles(lat1, lng1, lat2, lng2):
    R = 3959  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def pick_destination(weights):
    total = sum(w for _, _, w in weights)
    r = random.uniform(0, total)
    cumulative = 0
    for lat, lng, w in weights:
        cumulative += w
        if r <= cumulative:
            # Add some jitter (simulate different addresses in the metro)
            return lat + random.gauss(0, 0.15), lng + random.gauss(0, 0.15)
    return weights[-1][0], weights[-1][1]

def pick_nearest_origin(dest_lat, dest_lng):
    """Pick origin weighted toward nearer DCs (but not exclusively nearest)."""
    distances = []
    for o in ORIGINS:
        d = haversine_miles(o["lat"], o["lng"], dest_lat, dest_lng)
        distances.append((o, d))
    # Weight inversely by distance (with a floor to avoid extreme concentration)
    weights = [(o, 1.0 / max(d, 100)) for o, d in distances]
    total = sum(w for _, w in weights)
    r = random.uniform(0, total)
    cumulative = 0
    for o, w in weights:
        cumulative += w
        if r <= cumulative:
            return o
    return weights[0][0]

def generate_orders(n_orders=5000, start_date=datetime(2026, 1, 1), days=30):
    orders = []
    for _ in range(n_orders):
        dest_lat, dest_lng = pick_destination(DESTINATIONS)
        origin = pick_nearest_origin(dest_lat, dest_lng)

        distance = haversine_miles(origin["lat"], origin["lng"], dest_lat, dest_lng)

        # Ship datetime: random within the date range, biased toward business hours
        ship_day = start_date + timedelta(days=random.uniform(0, days))
        ship_hour = random.gauss(14, 3)  # Peak around 2 PM
        ship_hour = max(6, min(22, ship_hour))
        ship_time = ship_day.replace(hour=int(ship_hour), minute=random.randint(0, 59))

        # Transit time: based on distance, with some randomness
        # ~450 mph average but with realistic variance
        speed_mph = random.gauss(450, 100)
        speed_mph = max(200, speed_mph)
        transit_hours = distance / speed_mph
        # Add handling time (4-12 hours)
        transit_hours += random.uniform(4, 12)
        # Ensure delivery is during business hours (6 AM - 6 PM)
        delivery_time = ship_time + timedelta(hours=transit_hours)
        if delivery_time.hour < 6:
            delivery_time = delivery_time.replace(hour=random.randint(8, 12))
        elif delivery_time.hour > 18:
            delivery_time = delivery_time.replace(hour=random.randint(14, 18))

        value = round(random.lognormvariate(3.5, 1.0), 2)  # $10-$500 range mostly
        weight = round(random.lognormvariate(1.5, 0.8), 1)  # 1-50 lbs mostly
        cube = round(weight * random.uniform(0.05, 0.3), 2)  # cubic feet
        brand = random.choice(BRANDS)

        orders.append({
            "origin_name": origin["name"],
            "origin_lat": round(origin["lat"], 4),
            "origin_lng": round(origin["lng"], 4),
            "dest_lat": round(dest_lat, 4),
            "dest_lng": round(dest_lng, 4),
            "ship_datetime": ship_time.strftime("%Y-%m-%d %H:%M:%S"),
            "delivery_datetime": delivery_time.strftime("%Y-%m-%d %H:%M:%S"),
            "value": value,
            "weight": weight,
            "cube": cube,
            "brand": brand,
        })

    return orders

if __name__ == "__main__":
    random.seed(42)
    orders = generate_orders(5000)

    output_path = "../public/sample_data.csv"
    fieldnames = [
        "origin_name", "origin_lat", "origin_lng",
        "dest_lat", "dest_lng",
        "ship_datetime", "delivery_datetime",
        "value", "weight", "cube", "brand",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(orders)

    print(f"Generated {len(orders)} orders -> {output_path}")
