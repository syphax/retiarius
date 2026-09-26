// Geometry on the unit sphere. Points are 3-D unit vectors, which keeps the math correct at any
// latitude and across the antimeridian (so non-US data works unchanged).

export type Vec3 = [number, number, number];

export const EARTH_RADIUS = { mi: 3958.8, km: 6371.0 } as const;

const RAD = Math.PI / 180;

export function toVec(lat: number, lon: number): Vec3 {
  const phi = lat * RAD;
  const lam = lon * RAD;
  const c = Math.cos(phi);
  return [c * Math.cos(lam), c * Math.sin(lam), Math.sin(phi)];
}

export function toLatLon([x, y, z]: Vec3): [number, number] {
  return [Math.atan2(z, Math.hypot(x, y)) / RAD, Math.atan2(y, x) / RAD];
}

/** Central angle between two unit vectors, in radians. */
export function angle(a: Vec3, b: Vec3): number {
  const d = a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
  return Math.acos(d > 1 ? 1 : d < -1 ? -1 : d);
}

export function normalize([x, y, z]: Vec3): Vec3 {
  const n = Math.hypot(x, y, z);
  return n === 0 ? [1, 0, 0] : [x / n, y / n, z / n];
}
