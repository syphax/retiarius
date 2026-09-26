import { describe, expect, it } from 'vitest';
import type { Point } from '../data/types';
import { DEFAULT_PARAMS } from '../params';
import { buildModel } from '../solver/model';
import { mulberry32, solveN } from '../solver/solve';
import { buildFrameView } from './frameView';

const r = mulberry32(5);
const points: Point[] = [
  ...Array.from({ length: 150 }, () => ({ type: 'demand' as const, lat: 28 + r() * 18, lon: -120 + r() * 45, volume: 1 + r() * 50, rows: [] })),
  { type: 'source', lat: 33.75, lon: -118.2, volume: 1, rows: [] },
  { type: 'fixed', lat: 32.79, lon: -96.8, volume: 0, rows: [] },
];
const model = buildModel(points, { ...DEFAULT_PARAMS, useFixedNodes: true });
const demand = points.filter((p) => p.type === 'demand');
const sources = [{ point: points[150], share: 1 }];
const sol = solveN(model, 4);
const view = (t: number) => buildFrameView(model, points, demand, sources, sol, t);

describe('buildFrameView', () => {
  it('starts at the starting guess and ends at the final solution', () => {
    const start = view(0);
    expect(start.iteration).toBe(0);
    expect(start.nodes.map((n) => [n.lat, n.lon])).toEqual(sol.frames[0].nodes);
    const end = view(sol.frames.length - 1);
    expect(end.iteration).toBe(sol.frames.length - 1);
    expect(end.alloc).toBe(sol.alloc);
    expect(end.metrics.totalCost).toBeCloseTo(sol.metrics.totalCost, 6);
    expect(end.nodes.map((n) => n.lat)).toEqual(sol.nodes.map((n) => n.lat));
  });

  it('interpolates between iterations and clamps past the end', () => {
    const mid = view(0.5);
    const [a, b] = [sol.frames[0].nodes[1], sol.frames[1].nodes[1]];
    expect(mid.nodes[1].lat).toBeCloseTo((a[0] + b[0]) / 2, 9);
    expect(mid.iteration).toBe(0);
    expect(view(1e6).iteration).toBe(sol.frames.length - 1);
  });

  it('builds trails from the start through the current position, and keeps fixed nodes still', () => {
    const v = view(2.5);
    for (const trail of v.trails) expect(trail).toHaveLength(4); // frames 0..2 + current
    expect(v.nodes[0].fixed).toBe(true);
    expect(new Set(v.trails[0].map(([lon, lat]) => `${lon},${lat}`)).size).toBe(1);
  });

  it('throughput always sums to total demand', () => {
    for (const t of [0, 1, 3.3, sol.frames.length - 1]) {
      expect(view(t).nodes.reduce((s, n) => s + n.throughput, 0)).toBeCloseTo(model.totalDemand, 6);
    }
  });
});
