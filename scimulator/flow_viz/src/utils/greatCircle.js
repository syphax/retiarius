const DEG2RAD = Math.PI / 180;
const RAD2DEG = 180 / Math.PI;

/**
 * Interpolate a point along a great-circle arc.
 * @param {number} lat1 - Origin latitude (degrees)
 * @param {number} lng1 - Origin longitude (degrees)
 * @param {number} lat2 - Destination latitude (degrees)
 * @param {number} lng2 - Destination longitude (degrees)
 * @param {number} t - Fraction along the arc (0 = origin, 1 = destination)
 * @returns {[number, number]} [longitude, latitude] in degrees
 */
export function interpolateGreatCircle(lat1, lng1, lat2, lng2, t) {
  const φ1 = lat1 * DEG2RAD;
  const λ1 = lng1 * DEG2RAD;
  const φ2 = lat2 * DEG2RAD;
  const λ2 = lng2 * DEG2RAD;

  const d = Math.acos(
    Math.sin(φ1) * Math.sin(φ2) +
    Math.cos(φ1) * Math.cos(φ2) * Math.cos(λ2 - λ1)
  );

  // If points are essentially the same, just return the midpoint
  if (d < 1e-10) {
    return [lng1, lat1];
  }

  const sinD = Math.sin(d);
  const a = Math.sin((1 - t) * d) / sinD;
  const b = Math.sin(t * d) / sinD;

  const x = a * Math.cos(φ1) * Math.cos(λ1) + b * Math.cos(φ2) * Math.cos(λ2);
  const y = a * Math.cos(φ1) * Math.sin(λ1) + b * Math.cos(φ2) * Math.sin(λ2);
  const z = a * Math.sin(φ1) + b * Math.sin(φ2);

  const lat = Math.atan2(z, Math.sqrt(x * x + y * y)) * RAD2DEG;
  const lng = Math.atan2(y, x) * RAD2DEG;

  return [lng, lat];
}

/**
 * Generate waypoints along a great-circle arc for use with TripsLayer.
 * @param {number} lat1 - Origin latitude
 * @param {number} lng1 - Origin longitude
 * @param {number} lat2 - Destination latitude
 * @param {number} lng2 - Destination longitude
 * @param {number} startTime - Start timestamp (ms)
 * @param {number} endTime - End timestamp (ms)
 * @param {number} numSegments - Number of segments to divide the arc into
 * @returns {Array<{coordinates: [number, number], timestamp: number}>}
 */
export function generateArcWaypoints(lat1, lng1, lat2, lng2, startTime, endTime, numSegments = 30) {
  const waypoints = [];
  for (let i = 0; i <= numSegments; i++) {
    const t = i / numSegments;
    const [lng, lat] = interpolateGreatCircle(lat1, lng1, lat2, lng2, t);
    const timestamp = startTime + t * (endTime - startTime);
    waypoints.push({ coordinates: [lng, lat], timestamp });
  }
  return waypoints;
}
