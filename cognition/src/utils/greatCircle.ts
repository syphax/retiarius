// Ported from scimulator/flow_viz/src/utils/greatCircle.js.
const DEG2RAD = Math.PI / 180;
const RAD2DEG = 180 / Math.PI;

/** [lon, lat] points along the great circle from (lat1, lon1) to (lat2, lon2). */
export function greatCirclePath(lat1: number, lon1: number, lat2: number, lon2: number, segments = 24): [number, number][] {
  const φ1 = lat1 * DEG2RAD;
  const λ1 = lon1 * DEG2RAD;
  const φ2 = lat2 * DEG2RAD;
  const λ2 = lon2 * DEG2RAD;
  const d = Math.acos(
    Math.min(1, Math.sin(φ1) * Math.sin(φ2) + Math.cos(φ1) * Math.cos(φ2) * Math.cos(λ2 - λ1)),
  );
  if (d < 1e-10) return [[lon1, lat1], [lon2, lat2]];
  const sinD = Math.sin(d);
  const path: [number, number][] = [];
  for (let s = 0; s <= segments; s++) {
    const t = s / segments;
    const a = Math.sin((1 - t) * d) / sinD;
    const b = Math.sin(t * d) / sinD;
    const x = a * Math.cos(φ1) * Math.cos(λ1) + b * Math.cos(φ2) * Math.cos(λ2);
    const y = a * Math.cos(φ1) * Math.sin(λ1) + b * Math.cos(φ2) * Math.sin(λ2);
    const z = a * Math.sin(φ1) + b * Math.sin(φ2);
    path.push([Math.atan2(y, x) * RAD2DEG, Math.atan2(z, Math.hypot(x, y)) * RAD2DEG]);
  }
  return path;
}
