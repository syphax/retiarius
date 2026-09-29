import type { Point } from '../data/types';
import type { Params } from '../params';
import { EARTH_RADIUS, angle, toVec, type Vec3 } from './sphere';

/**
 * The solver's view of a dataset. Distances inside the solver are central angles (radians);
 * `distScale` converts them to the user's units, including circuity. Circuity scales every
 * distance equally, so it changes reported costs but never where nodes end up.
 */
export interface Model {
  demand: Vec3[];
  demandWeight: Float64Array;
  totalDemand: number;
  sources: Vec3[];
  /** Each source's share of total supply (sums to 1). Supply is normalized so in = out. */
  sourceShare: Float64Array;
  /** Inbound cost per unit-distance relative to outbound; 0 when there are no sources. */
  inboundRatio: number;
  /** Share of inbound drawn from all sources by supply share; the rest comes from the nearest source. */
  proportional: number;
  fixed: Vec3[];
  distScale: number;
  serviceDistance: number;
}

export function buildModel(points: Point[], params: Params): Model {
  const demandPts = points.filter((p) => p.type === 'demand');
  const sourcePts = points.filter((p) => p.type === 'source');
  const fixedPts = params.useFixedNodes ? points.filter((p) => p.type === 'fixed') : [];
  const demandWeight = Float64Array.from(demandPts, (p) => p.volume);
  const totalSupply = sourcePts.reduce((s, p) => s + p.volume, 0);
  return {
    demand: demandPts.map((p) => toVec(p.lat, p.lon)),
    demandWeight,
    totalDemand: demandWeight.reduce((s, w) => s + w, 0),
    sources: sourcePts.map((p) => toVec(p.lat, p.lon)),
    sourceShare: Float64Array.from(sourcePts, (p) => p.volume / totalSupply),
    inboundRatio: sourcePts.length ? params.inboundRatio : 0,
    proportional: Math.min(1, Math.max(0, params.proportionalSourcing ?? 1)),
    fixed: fixedPts.map((p) => toVec(p.lat, p.lon)),
    distScale: EARTH_RADIUS[params.units] * params.circuity,
    serviceDistance: params.serviceDistance,
  };
}

/**
 * Where a node's inbound comes from: the fraction of its volume from each source (sums to 1).
 * Proportional part by supply share, the rest from the nearest source. Source volumes are not
 * capacities, so a nearby source can supply more than its stated share.
 */
export function sourceMix(m: Model, node: Vec3, dist?: Float64Array): Float64Array {
  const S = m.sources.length;
  const d = dist ?? Float64Array.from(m.sources, (s) => angle(s, node));
  let nearest = 0;
  for (let k = 1; k < S; k++) if (d[k] < d[nearest]) nearest = k;
  const mix = new Float64Array(S);
  for (let k = 0; k < S; k++) mix[k] = m.proportional * m.sourceShare[k];
  if (S) mix[nearest] += 1 - m.proportional;
  return mix;
}

/** Inbound cost per unit of throughput at a node location (in angle units, already × ratio). */
export function inboundPerUnit(m: Model, node: Vec3): number {
  if (m.inboundRatio === 0) return 0;
  const d = Float64Array.from(m.sources, (s) => angle(s, node));
  const mix = sourceMix(m, node, d);
  let s = 0;
  for (let k = 0; k < d.length; k++) s += mix[k] * d[k];
  return m.inboundRatio * s;
}

/**
 * Assign each demand point to the node with the lowest landed cost per unit:
 * outbound distance + that node's inbound cost per unit.
 */
export function allocate(m: Model, nodes: Vec3[], out = new Int32Array(m.demand.length)): Int32Array {
  const inbound = nodes.map((n) => inboundPerUnit(m, n));
  for (let i = 0; i < m.demand.length; i++) {
    let best = 0;
    let bestCost = Infinity;
    for (let j = 0; j < nodes.length; j++) {
      const c = angle(m.demand[i], nodes[j]) + inbound[j];
      if (c < bestCost) {
        bestCost = c;
        best = j;
      }
    }
    out[i] = best;
  }
  return out;
}

export interface NodeStats {
  lat: number;
  lon: number;
  fixed: boolean;
  throughput: number;
  outboundCost: number;
  inboundCost: number;
}

export interface Metrics {
  totalCost: number;
  outboundCost: number;
  inboundCost: number;
  /** Volume-weighted average outbound distance. */
  avgDistance: number;
  /** Share of demand volume within the service distance (0–1). */
  pctWithin: number;
  /** Realized share of inbound volume from each source (same order as the source points). */
  sourceShares: number[];
}

/** Cost and service metrics, in the user's units. */
export function evaluate(m: Model, nodes: Vec3[], alloc: Int32Array): { metrics: Metrics; perNode: Omit<NodeStats, 'lat' | 'lon' | 'fixed'>[] } {
  const perNode = nodes.map(() => ({ throughput: 0, outboundCost: 0, inboundCost: 0 }));
  let outbound = 0;
  let within = 0;
  for (let i = 0; i < m.demand.length; i++) {
    const j = alloc[i];
    const w = m.demandWeight[i];
    const d = angle(m.demand[i], nodes[j]) * m.distScale;
    outbound += w * d;
    perNode[j].throughput += w;
    perNode[j].outboundCost += w * d;
    if (d <= m.serviceDistance) within += w;
  }
  let inbound = 0;
  const sourceVolume = new Array<number>(m.sources.length).fill(0);
  nodes.forEach((n, j) => {
    const c = perNode[j].throughput * inboundPerUnit(m, n) * m.distScale;
    perNode[j].inboundCost = c;
    inbound += c;
    sourceMix(m, n).forEach((f, k) => (sourceVolume[k] += f * perNode[j].throughput));
  });
  const total = m.totalDemand || 1;
  return {
    metrics: {
      totalCost: outbound + inbound,
      outboundCost: outbound,
      inboundCost: inbound,
      avgDistance: outbound / total,
      pctWithin: within / total,
      sourceShares: sourceVolume.map((v) => v / total),
    },
    perNode,
  };
}
