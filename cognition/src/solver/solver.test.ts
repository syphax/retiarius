import { describe, expect, it } from 'vitest';
import type { Point } from '../data/types';
import { DEFAULT_PARAMS, type Params } from '../params';
import { buildModel } from './model';
import { DEFAULT_OPTIONS, locationAllocation, mulberry32, polish, polishBudget, seedNodes, solveN, sweep } from './solve';
import { angle, toLatLon, toVec } from './sphere';

const pt = (type: Point['type'], lat: number, lon: number, volume = 1): Point => ({ type, lat, lon, volume, rows: [] });
const params = (o: Partial<Params> = {}): Params => ({ ...DEFAULT_PARAMS, ...o });
const miles = (a: [number, number], b: [number, number]) => angle(toVec(...a), toVec(...b)) * 3958.8;

/** Random US-ish demand cloud. */
function cloud(n: number, seed = 3): Point[] {
  const r = mulberry32(seed);
  return Array.from({ length: n }, () => pt('demand', 26 + r() * 22, -122 + r() * 50, 1 + Math.floor(r() * 100)));
}

describe('sphere', () => {
  it('round-trips lat/lon', () => {
    const [lat, lon] = toLatLon(toVec(39.5, -119.8));
    expect(lat).toBeCloseTo(39.5, 9);
    expect(lon).toBeCloseTo(-119.8, 9);
  });

  it('matches a known great-circle distance (LAX–JFK ≈ 2475 mi)', () => {
    expect(Math.abs(miles([33.9425, -118.4081], [40.6398, -73.7789]) - 2475)).toBeLessThan(10);
  });
});

describe('single node', () => {
  it('lands at the center of a symmetric set', () => {
    const pts = [pt('demand', 39, -101), pt('demand', 39, -99), pt('demand', 41, -101), pt('demand', 41, -99)];
    const s = solveN(buildModel(pts, params({ circuity: 1 })), 1);
    // On a sphere the 39°/41° "square" is narrower in the north, so the true median is ~40.019°N.
    expect(miles([s.nodes[0].lat, s.nodes[0].lon], [40.019, -100])).toBeLessThan(0.5);
  });

  it('sits on a point that outweighs all others combined (geometric median, not mean)', () => {
    const pts = [pt('demand', 40, -100, 10), pt('demand', 35, -90, 3), pt('demand', 45, -110, 3)];
    const s = solveN(buildModel(pts, params()), 1);
    expect(miles([s.nodes[0].lat, s.nodes[0].lon], [40, -100])).toBeLessThan(1);
  });

  it('is pulled toward sources, more strongly as the inbound ratio rises', () => {
    const pts = [...cloud(200), pt('source', 40.7, -74.0, 1)];
    const dist = (ratio: number) => {
      const s = solveN(buildModel(pts, params({ inboundRatio: ratio })), 1);
      return miles([s.nodes[0].lat, s.nodes[0].lon], [40.7, -74.0]);
    };
    const [d0, d1, d3] = [dist(0), dist(0.5), dist(3)];
    expect(d1).toBeLessThan(d0);
    expect(d3).toBeLessThan(d1);
    expect(d3).toBeLessThan(1); // inbound dominates: sit on the source
  });
});

describe('costs and metrics', () => {
  it('has no inbound cost without sources, and ignores the ratio', () => {
    const s = solveN(buildModel(cloud(100), params({ inboundRatio: 5 })), 3);
    expect(s.metrics.inboundCost).toBe(0);
    expect(s.metrics.totalCost).toBeCloseTo(s.metrics.outboundCost, 6);
  });

  it('normalizes supply so inbound volume equals demand volume', () => {
    const base = [...cloud(100), pt('source', 34, -118, 1)];
    const big = [...cloud(100), pt('source', 34, -118, 1e6)];
    const a = solveN(buildModel(base, params()), 2);
    const b = solveN(buildModel(big, params()), 2);
    expect(b.metrics.inboundCost).toBeCloseTo(a.metrics.inboundCost, 6);
  });

  it('reports consistent metrics', () => {
    const m = buildModel(cloud(150), params({ serviceDistance: 1e6 }));
    const s = solveN(m, 4);
    expect(s.metrics.pctWithin).toBe(1);
    expect(s.metrics.avgDistance).toBeCloseTo(s.metrics.outboundCost / m.totalDemand, 6);
    expect(s.nodes.reduce((t, n) => t + n.throughput, 0)).toBeCloseTo(m.totalDemand, 6);
    expect(s.frames.at(-1)!.totalCost).toBeCloseTo(s.metrics.totalCost, 3);
  });

  it('scales cost with circuity but leaves node locations unchanged', () => {
    const pts = cloud(120);
    const a = solveN(buildModel(pts, params({ circuity: 1 })), 3);
    const b = solveN(buildModel(pts, params({ circuity: 1.2 })), 3);
    expect(b.metrics.totalCost / a.metrics.totalCost).toBeCloseTo(1.2, 6);
    expect(b.nodes.map((n) => n.lat)).toEqual(a.nodes.map((n) => n.lat));
  });
});

describe('sweep', () => {
  const pts = [...cloud(400), pt('source', 33.75, -118.2, 2), pt('source', 40.7, -74.1, 1)];

  it('cost falls as N rises, and later N are warm-started', () => {
    const sols = sweep(buildModel(pts, params()), 1, 8);
    expect(sols.map((s) => s.n)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    for (let i = 1; i < sols.length; i++) {
      expect(sols[i].metrics.totalCost).toBeLessThanOrEqual(sols[i - 1].metrics.totalCost * (1 + 1e-9));
    }
    expect(sols.slice(1).some((s) => s.warmStarted)).toBe(true);
  });

  it('never moves fixed nodes, counts them in N, and starts at N = #fixed', () => {
    const fixed = [pt('fixed', 32.79, -96.8, 0), pt('fixed', 39.49, -119.74, 0)];
    const m = buildModel([...pts, ...fixed], params({ useFixedNodes: true }));
    const sols = sweep(m, 1, 5);
    expect(sols.map((s) => s.n)).toEqual([2, 3, 4, 5]);
    for (const s of sols) {
      expect(s.nodes).toHaveLength(s.n);
      expect(s.nodes.filter((n) => n.fixed)).toHaveLength(2);
      expect(s.nodes[0].lat).toBeCloseTo(32.79, 9);
      expect(s.nodes[1].lon).toBeCloseTo(-119.74, 9);
      for (const f of s.frames) expect(f.nodes[0][0]).toBeCloseTo(32.79, 9);
    }
  });

  it('ignores fixed rows when fixed nodes are off', () => {
    const m = buildModel([...pts, pt('fixed', 32.79, -96.8, 0)], params({ useFixedNodes: false }));
    expect(sweep(m, 1, 2).map((s) => s.nodes.some((n) => n.fixed))).toEqual([false, false]);
  });

  it('records a search that starts at the initial guess and improves', () => {
    const s = solveN(buildModel(pts, params()), 5);
    expect(s.frames.length).toBeGreaterThan(2);
    expect(s.frames.at(-1)!.totalCost).toBeLessThanOrEqual(s.frames[0].totalCost);
  });

  it('is reproducible for a given seed', () => {
    const m = buildModel(pts, params());
    expect(solveN(m, 4).metrics.totalCost).toBe(solveN(m, 4).metrics.totalCost);
  });

  it('is fast enough for ~900 points', () => {
    const m = buildModel([...cloud(900, 11), pt('source', 33.75, -118.2)], params());
    const t = performance.now();
    sweep(m, 1, 10, DEFAULT_OPTIONS);
    expect(performance.now() - t).toBeLessThan(5000);
  });

  it('re-solves one N warm-started from an earlier solution with new parameters (ad-hoc re-run)', () => {
    const base = solveN(buildModel(pts, params({ inboundRatio: 0.4 })), 5);
    const warm = base.nodes.filter((n) => !n.fixed).map((n) => toVec(n.lat, n.lon));
    const m = buildModel(pts, params({ inboundRatio: 2 }));
    const re = solveN(m, 5, { ...DEFAULT_OPTIONS, restarts: 3 }, warm);
    expect(re.n).toBe(5);
    expect(re.warmStarted).toBe(true);
    re.frames[0].nodes.forEach(([lat, lon], j) => {
      // Starts where the base ended.
      expect(lat).toBeCloseTo(base.nodes[j].lat, 9);
      expect(lon).toBeCloseTo(base.nodes[j].lon, 9);
    });
    // A higher inbound ratio pulls the network toward the sources.
    const toSources = (s: typeof re) =>
      s.nodes.reduce((t, n) => t + n.throughput * Math.min(miles([n.lat, n.lon], [33.75, -118.2]), miles([n.lat, n.lon], [40.7, -74.1])), 0);
    expect(toSources(re)).toBeLessThan(toSources(base));
  });

  it('adds fixed nodes in a re-run by trimming the warm start to fit N', () => {
    const base = solveN(buildModel(pts, params()), 4);
    const warm = base.nodes.map((n) => toVec(n.lat, n.lon));
    const m = buildModel([...pts, pt('fixed', 32.79, -96.8, 0)], params({ useFixedNodes: true }));
    const re = solveN(m, 4, DEFAULT_OPTIONS, warm);
    expect(re.nodes).toHaveLength(4);
    expect(re.nodes.filter((n) => n.fixed)).toHaveLength(1);
  });
});

describe('inbound sourcing blend', () => {
  // Two coastal ports with equal supply, demand spread across the country.
  const la: [number, number] = [33.75, -118.2];
  const ny: [number, number] = [40.7, -74.1];
  const pts = [...cloud(300, 21), pt('source', ...la, 1), pt('source', ...ny, 1)];
  const at = (sourcing: number, ratio = 0.4) => buildModel(pts, params({ inboundRatio: ratio, proportionalSourcing: sourcing }));

  it('realized source shares match supply when fully proportional and follow proximity when nearest', () => {
    const prop = solveN(at(1), 4);
    expect(prop.metrics.sourceShares[0]).toBeCloseTo(0.5, 9);
    expect(prop.metrics.sourceShares[1]).toBeCloseTo(0.5, 9);
    const near = solveN(at(0), 4);
    const nearestIsLA = (n: { lat: number; lon: number }) => miles([n.lat, n.lon], la) < miles([n.lat, n.lon], ny);
    const laShare = near.nodes.filter(nearestIsLA).reduce((t, n) => t + n.throughput, 0) / at(0).totalDemand;
    expect(near.metrics.sourceShares[0]).toBeCloseTo(laShare, 9);
    expect(near.metrics.sourceShares.reduce((a, b) => a + b, 0)).toBeCloseTo(1, 9);
  });

  it('blends linearly between the two ends', () => {
    const blend = solveN(at(0.6), 4);
    // Each node draws 60% by share (30% / 30%) plus 40% from its nearest port.
    const nearLA = blend.nodes
      .filter((n) => miles([n.lat, n.lon], la) < miles([n.lat, n.lon], ny))
      .reduce((t, n) => t + n.throughput, 0) / at(0.6).totalDemand;
    expect(blend.metrics.sourceShares[0]).toBeCloseTo(0.3 + 0.4 * nearLA, 9);
    expect(blend.metrics.sourceShares[1]).toBeCloseTo(0.3 + 0.4 * (1 - nearLA), 9);
  });

  it('nearest sourcing lowers inbound cost, and inbound falls with N', () => {
    const prop = sweep(at(1), 1, 6);
    const near = sweep(at(0), 1, 6);
    for (let i = 0; i < prop.length; i++) {
      expect(near[i].metrics.inboundCost).toBeLessThan(prop[i].metrics.inboundCost);
    }
    // More nodes can sit nearer their ports (proportional inbound stays roughly flat instead).
    expect(near.at(-1)!.metrics.inboundCost).toBeLessThan(near[0].metrics.inboundCost);
  });

  it('with dominant inbound cost, 2 nodes sit on the 2 ports when nearest, but not when proportional', () => {
    const onPorts = (s: ReturnType<typeof solveN>) =>
      [la, ny].every((port) => s.nodes.some((n) => miles([n.lat, n.lon], port) < 5));
    expect(onPorts(solveN(at(0, 5), 2))).toBe(true);
    expect(onPorts(solveN(at(1, 5), 2))).toBe(false);
  });

  it('is identical to the old model at 100% (the default)', () => {
    const a = solveN(buildModel(pts, params()), 3);
    const b = solveN(at(1), 3);
    expect(b.metrics.totalCost).toBe(a.metrics.totalCost);
  });
});

describe('polish pass', () => {
  // Clustered demand (metro areas) has the local optima the alternating heuristic gets stuck in.
  function metros(seed: number): Point[] {
    const r = mulberry32(seed);
    const centers = Array.from({ length: 9 }, () => [28 + r() * 18, -120 + r() * 46]);
    return Array.from({ length: 450 }, (_, i) => {
      const [lat, lon] = centers[i % centers.length];
      return pt('demand', lat + (r() - 0.5) * 3, lon + (r() - 0.5) * 3, 1 + Math.floor(r() * 50));
    });
  }
  const noPolish = { ...DEFAULT_OPTIONS, polishAttempts: 0 };

  it('never makes a run worse, and escapes local optima on clustered data', () => {
    let improved = 0;
    let runs = 0;
    for (const seed of [1, 2, 3]) {
      const m = buildModel([...metros(seed), pt('source', 33.75, -118.2)], params());
      const rand = mulberry32(seed);
      for (const n of [3, 5, 7]) {
        for (let r = 0; r < 4; r++) {
          const run = locationAllocation(m, seedNodes(m, n, rand, []), DEFAULT_OPTIONS);
          const p = polish(m, run, DEFAULT_OPTIONS);
          expect(p.cost).toBeLessThanOrEqual(run.cost);
          runs++;
          if (p.cost < run.cost * (1 - 1e-4)) improved++;
        }
      }
    }
    expect(improved).toBeGreaterThan(runs / 4); // plain runs get stuck often enough to matter
  });

  it('keeps sweeps within the continuity tolerance of unpolished results, or better', () => {
    const m = buildModel([...metros(1), pt('source', 33.75, -118.2)], params());
    const plain = sweep(m, 1, 7, noPolish);
    sweep(m, 1, 7).forEach((s, i) => {
      expect(s.metrics.totalCost).toBeLessThanOrEqual(plain[i].metrics.totalCost * (1 + DEFAULT_OPTIONS.continuityTolerance));
    });
  });

  it('appends kept moves to the playback and keeps fixed nodes still', () => {
    const fixed = pt('fixed', 39.1, -94.6, 0);
    const m = buildModel([...metros(2), fixed], params({ useFixedNodes: true }));
    const sols = sweep(m, 1, 7);
    const moved = sols.find((s) => s.polishMoves > 0);
    expect(moved).toBeDefined();
    for (const s of sols) {
      expect(s.frames.at(-1)!.totalCost).toBeCloseTo(s.metrics.totalCost, 3);
      for (const f of s.frames) expect(f.nodes[0][0]).toBeCloseTo(39.1, 9);
    }
  });

  it('scales its attempt budget down for large datasets', () => {
    const small = buildModel(cloud(500), params());
    const big = buildModel(cloud(30000, 9), params());
    expect(polishBudget(small, DEFAULT_OPTIONS)).toBe(DEFAULT_OPTIONS.polishAttempts);
    expect(polishBudget(big, DEFAULT_OPTIONS)).toBe(3);
    expect(polishBudget(big, noPolish)).toBe(0);
  });
});

