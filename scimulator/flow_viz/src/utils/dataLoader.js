import Papa from 'papaparse';
import { generateArcWaypoints } from './greatCircle';

/**
 * Load and parse CSV data, then build trip objects for visualization.
 * @param {string} url - URL to the CSV file
 * @returns {Promise<{trips: Array, origins: Array, timeRange: [number, number], stats: Object}>}
 */
export async function loadFlowData(url) {
  const response = await fetch(url);
  const csvText = await response.text();

  const { data: rows } = Papa.parse(csvText, {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: true,
  });

  let minTime = Infinity;
  let maxTime = -Infinity;
  const originMap = new Map();

  const trips = rows.map((row, i) => {
    const shipTime = new Date(row.ship_datetime).getTime();
    const deliveryTime = new Date(row.delivery_datetime).getTime();

    minTime = Math.min(minTime, shipTime);
    maxTime = Math.max(maxTime, deliveryTime);

    // Track unique origins
    const originKey = `${row.origin_lat},${row.origin_lng}`;
    if (!originMap.has(originKey)) {
      originMap.set(originKey, {
        name: row.origin_name || `Origin ${originMap.size + 1}`,
        latitude: row.origin_lat,
        longitude: row.origin_lng,
      });
    }

    const waypoints = generateArcWaypoints(
      row.origin_lat, row.origin_lng,
      row.dest_lat, row.dest_lng,
      shipTime, deliveryTime,
      30
    );

    const deliveryDays = (deliveryTime - shipTime) / (1000 * 60 * 60 * 24);

    return {
      id: i,
      waypoints,
      startTime: shipTime,
      endTime: deliveryTime,
      value: row.value || 0,
      weight: row.weight || 0,
      cube: row.cube || 0,
      brand: row.brand || '',
      deliveryDays,
      originLat: row.origin_lat,
      originLng: row.origin_lng,
      destLat: row.dest_lat,
      destLng: row.dest_lng,
    };
  });

  const origins = Array.from(originMap.values());

  // Compute attribute ranges for color/size scaling
  const stats = {
    value: { min: Infinity, max: -Infinity },
    weight: { min: Infinity, max: -Infinity },
    cube: { min: Infinity, max: -Infinity },
    deliveryDays: { min: Infinity, max: -Infinity },
    brands: new Set(),
  };

  for (const trip of trips) {
    stats.value.min = Math.min(stats.value.min, trip.value);
    stats.value.max = Math.max(stats.value.max, trip.value);
    stats.weight.min = Math.min(stats.weight.min, trip.weight);
    stats.weight.max = Math.max(stats.weight.max, trip.weight);
    stats.cube.min = Math.min(stats.cube.min, trip.cube);
    stats.cube.max = Math.max(stats.cube.max, trip.cube);
    stats.deliveryDays.min = Math.min(stats.deliveryDays.min, trip.deliveryDays);
    stats.deliveryDays.max = Math.max(stats.deliveryDays.max, trip.deliveryDays);
    if (trip.brand) stats.brands.add(trip.brand);
  }

  stats.brands = Array.from(stats.brands);

  // Precompute OD-pair routes for cumulative path mode
  const odMap = new Map();
  for (const trip of trips) {
    // Round to 2 decimals to group nearby OD pairs
    const key = `${trip.originLat.toFixed(2)},${trip.originLng.toFixed(2)}->${trip.destLat.toFixed(2)},${trip.destLng.toFixed(2)}`;
    if (!odMap.has(key)) {
      odMap.set(key, {
        key,
        originLat: trip.originLat,
        originLng: trip.originLng,
        destLat: trip.destLat,
        destLng: trip.destLng,
        path: trip.waypoints.map((wp) => wp.coordinates),
        // Sorted delivery times for binary-search counting
        deliveryTimes: [],
      });
    }
    odMap.get(key).deliveryTimes.push(trip.endTime);
  }
  // Sort delivery times so we can count completions up to currentTime
  const odRoutes = Array.from(odMap.values());
  for (const route of odRoutes) {
    route.deliveryTimes.sort((a, b) => a - b);
  }

  return { trips, origins, odRoutes, timeRange: [minTime, maxTime], stats };
}
