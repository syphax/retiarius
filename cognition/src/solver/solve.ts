import { evaluate, inboundPerUnit, sourceMix, type Metrics, type Model, type NodeStats } from './model';
import { angle, normalize, toLatLon, toVec, type Vec3 } from './sphere';

/**
 * Location–allocation (Cooper's alternating heuristic): allocate demand to the cheapest node,
 * move each free node one Weiszfeld step toward the weighted geometric median of its demand and
 * the sources, repeat. Each iteration is recorded as an animation frame.
 */

export interface Frame {
  /** [lat, lon] per node; fixed nodes first. */
  nodes: [number, number][];
  totalCost: number;
}

export interface Solution {
  n: number;
  nodes: NodeStats[];
  metrics: Metrics;
  /** Demand index → node index, for the final positions. */
  alloc: Int32Array;
  /** The winning run's search, from its starting guess to the final positions. */
  frames: Frame[];
  /** True when the winning run was warm-started from the previous solution. */
  warmStarted: boolean;
  runs: number;
}

export interface SolveOptions {
  /** Cold (k-means++) restarts. */
  restarts: number;
  seed: number;
  maxIter: number;
  /** Stop when no node moves more than this (radians; 2e-5 ≈ 0.1 mi). */
  tolerance: number;
  /**
   * Keep the warm start unless a cold restart beats it by more than this fraction, so N+1 stays
   * visually continuous with N. They usually find the same solution; genuine wins are ≥ ~0.5%.
   */
  continuityTolerance: number;
}

export const DEFAULT_OPTIONS: SolveOptions = {
  restarts: 8,
  seed: 1,
  maxIter: 150,
  tolerance: 2e-5,
  continuityTolerance: 0.0025,
};

/** Points closer than this (radians, ~6 mm) count as coincident with the node. */
const EPS = 1e-9;

/**
 * One Weiszfeld step toward the weighted geometric median, with the Vardi–Zhang fix: a point the
 * node sits on is left out of the average, and the node stays put only if that point's weight
 * outweighs the pull of all the others. Plain Weiszfeld would stick to any point it starts on.
 */
class Weiszfeld {
  private acc: Vec3 = [0, 0, 0];
  private sumW = 0;
  private coincident = 0;

  add(p: Vec3, w: number, x: Vec3) {
    if (w <= 0) return;
    const d = angle(p, x);
    if (d < EPS) {
      this.coincident += w;
      return;
    }
    const f = w / d;
    this.acc[0] += f * p[0];
    this.acc[1] += f * p[1];
    this.acc[2] += f * p[2];
    this.sumW += f;
  }

  step(x: Vec3): Vec3 {
    if (this.sumW === 0) return x;
    const t: Vec3 = [this.acc[0] / this.sumW, this.acc[1] / this.sumW, this.acc[2] / this.sumW];
    if (this.coincident === 0) return normalize(t);
    // Net pull of the other points, projected onto the tangent plane at x.
    const r: Vec3 = [this.acc[0] - this.sumW * x[0], this.acc[1] - this.sumW * x[1], this.acc[2] - this.sumW * x[2]];
    const radial = r[0] * x[0] + r[1] * x[1] + r[2] * x[2];
    const pull = Math.hypot(r[0] - radial * x[0], r[1] - radial * x[1], r[2] - radial * x[2]);
    if (pull <= this.coincident) return x;
    const g = this.coincident / pull;
    return normalize([(1 - g) * t[0] + g * x[0], (1 - g) * t[1] + g * x[1], (1 - g) * t[2] + g * x[2]]);
  }
}

/** Small deterministic PRNG so runs are reproducible. */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function pickWeighted(weights: Float64Array, rand: () => number): number {
  let total = 0;
  for (let i = 0; i < weights.length; i++) total += weights[i];
  if (total <= 0) return Math.floor(rand() * weights.length);
  let r = rand() * total;
  for (let i = 0; i < weights.length; i++) {
    r -= weights[i];
    if (r <= 0) return i;
  }
  return weights.length - 1;
}

/** Weighted k-means++: add demand points as nodes until there are k, favoring heavy, poorly served demand. */
export function seedNodes(m: Model, k: number, rand: () => number, initial: Vec3[]): Vec3[] {
  const nodes = initial.slice();
  const P = m.demand.length;
  const d2 = new Float64Array(P).fill(Infinity);
  const update = (c: Vec3) => {
    for (let i = 0; i < P; i++) d2[i] = Math.min(d2[i], angle(m.demand[i], c) ** 2);
  };
  nodes.forEach(update);
  const w = new Float64Array(P);
  while (nodes.length < k) {
    for (let i = 0; i < P; i++) w[i] = m.demandWeight[i] * (nodes.length ? d2[i] : 1);
    const c = m.demand[pickWeighted(w, rand)];
    nodes.push(c);
    update(c);
  }
  return nodes;
}

/** Allocate demand and return the total landed cost (angle units). Fills `alloc` and `landed`. */
function allocateWithCost(m: Model, nodes: Vec3[], alloc: Int32Array, landed: Float64Array): number {
  const inbound = nodes.map((n) => inboundPerUnit(m, n));
  let total = 0;
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
    alloc[i] = best;
    landed[i] = bestCost;
    total += m.demandWeight[i] * bestCost;
  }
  return total;
}

const snapshot = (nodes: Vec3[], cost: number, m: Model): Frame => ({
  nodes: nodes.map(toLatLon),
  totalCost: cost * m.distScale,
});

interface Run {
  nodes: Vec3[];
  alloc: Int32Array;
  cost: number;
  frames: Frame[];
}

/** Run location–allocation from a starting set. The first `m.fixed.length` nodes never move. */
export function locationAllocation(m: Model, start: Vec3[], opts: SolveOptions): Run {
  const nFixed = m.fixed.length;
  const P = m.demand.length;
  const alloc = new Int32Array(P);
  const landed = new Float64Array(P);
  let nodes = start.slice();
  let cost = allocateWithCost(m, nodes, alloc, landed);
  const frames = [snapshot(nodes, cost, m)];

  for (let it = 0; it < opts.maxIter; it++) {
    const acc = nodes.map(() => new Weiszfeld());
    const throughput = new Float64Array(nodes.length);
    for (let i = 0; i < P; i++) {
      const j = alloc[i];
      throughput[j] += m.demandWeight[i];
      if (j >= nFixed) acc[j].add(m.demand[i], m.demandWeight[i], nodes[j]);
    }

    const next = nodes.slice();
    const taken = new Set<number>();
    for (let j = nFixed; j < nodes.length; j++) {
      if (throughput[j] === 0) {
        // Empty node: move it to the worst-served demand point so it becomes useful.
        let worst = -1;
        for (let i = 0; i < P; i++) {
          if (!taken.has(i) && (worst < 0 || m.demandWeight[i] * landed[i] > m.demandWeight[worst] * landed[worst])) worst = i;
        }
        if (worst >= 0) {
          taken.add(worst);
          next[j] = m.demand[worst];
        }
        continue;
      }
      // Sources pull on the node in proportion to its throughput and how much it draws from each.
      if (m.inboundRatio > 0) {
        const mix = sourceMix(m, nodes[j]);
        for (let k = 0; k < m.sources.length; k++) {
          acc[j].add(m.sources[k], m.inboundRatio * throughput[j] * mix[k], nodes[j]);
        }
      }
      next[j] = acc[j].step(nodes[j]);
    }

    let moved = 0;
    for (let j = nFixed; j < nodes.length; j++) moved = Math.max(moved, angle(nodes[j], next[j]));
    nodes = next;
    cost = allocateWithCost(m, nodes, alloc, landed);
    frames.push(snapshot(nodes, cost, m));
    if (moved < opts.tolerance) break;
  }
  return { nodes, alloc, cost, frames };
}

/**
 * Add one node by splitting the most expensive cluster: the new node starts at that cluster's
 * worst-served demand point, so N+1 looks like N plus one node.
 */
export function splitWorstCluster(m: Model, nodes: Vec3[]): Vec3[] {
  const P = m.demand.length;
  const alloc = new Int32Array(P);
  const landed = new Float64Array(P);
  allocateWithCost(m, nodes, alloc, landed);
  const clusterCost = new Float64Array(nodes.length);
  for (let i = 0; i < P; i++) clusterCost[alloc[i]] += m.demandWeight[i] * landed[i];
  let worstCluster = 0;
  for (let j = 1; j < nodes.length; j++) if (clusterCost[j] > clusterCost[worstCluster]) worstCluster = j;
  let pick = -1;
  for (let i = 0; i < P; i++) {
    if (alloc[i] !== worstCluster) continue;
    const c = m.demandWeight[i] * angle(m.demand[i], nodes[worstCluster]);
    if (pick < 0 || c > m.demandWeight[pick] * angle(m.demand[pick], nodes[worstCluster])) pick = i;
  }
  return [...nodes, m.demand[pick >= 0 ? pick : 0]];
}

function toSolution(m: Model, n: number, run: Run, warmStarted: boolean, runs: number): Solution {
  const { metrics, perNode } = evaluate(m, run.nodes, run.alloc);
  return {
    n,
    nodes: run.nodes.map((v, j) => {
      const [lat, lon] = toLatLon(v);
      return { lat, lon, fixed: j < m.fixed.length, ...perNode[j] };
    }),
    metrics,
    alloc: run.alloc,
    frames: run.frames,
    warmStarted,
    runs,
  };
}

/**
 * Best solution for N total nodes (fixed nodes included). `warm` is an earlier solution's free
 * nodes: it is trimmed or split up to N and run alongside the cold restarts.
 */
export function solveN(m: Model, n: number, opts: SolveOptions = DEFAULT_OPTIONS, warm?: Vec3[]): Solution {
  const nFixed = m.fixed.length;
  if (n < nFixed) throw new Error(`N = ${n} is less than the ${nFixed} fixed nodes.`);
  if (m.demand.length === 0) throw new Error('No demand points.');
  const rand = mulberry32(opts.seed * 7919 + n);

  const candidates: { run: Run; warm: boolean }[] = [];
  if (warm) {
    let start = [...m.fixed, ...warm.slice(0, n - nFixed)];
    while (start.length < n) start = splitWorstCluster(m, start);
    candidates.push({ run: locationAllocation(m, start, opts), warm: true });
  }
  const cold = n === nFixed ? (warm ? 0 : 1) : opts.restarts;
  for (let r = 0; r < cold; r++) {
    candidates.push({ run: locationAllocation(m, seedNodes(m, n, rand, m.fixed), opts), warm: false });
  }
  const bestCold = candidates.filter((c) => !c.warm).reduce<(typeof candidates)[number] | undefined>(
    (a, b) => (!a || b.run.cost < a.run.cost ? b : a),
    undefined,
  );
  const warmRun = candidates.find((c) => c.warm);
  const best =
    warmRun && (!bestCold || bestCold.run.cost >= warmRun.run.cost * (1 - opts.continuityTolerance)) ? warmRun : bestCold!;
  return toSolution(m, n, best.run, best.warm, candidates.length);
}

/**
 * Solve N = minN…maxN. The first N is cold-started; each later N is warm-started from the
 * previous best (plus a few cold restarts).
 */
export function sweep(
  m: Model,
  minN: number,
  maxN: number,
  opts: SolveOptions = DEFAULT_OPTIONS,
  onSolution?: (s: Solution) => void,
): Solution[] {
  const lo = Math.max(minN, m.fixed.length, 1);
  const hi = Math.min(maxN, m.demand.length + m.fixed.length);
  const out: Solution[] = [];
  let prev: Vec3[] | undefined;
  for (let n = lo; n <= hi; n++) {
    const restarts = prev ? Math.max(2, Math.ceil(opts.restarts / 3)) : opts.restarts;
    const s = solveN(m, n, { ...opts, restarts }, prev);
    out.push(s);
    onSolution?.(s);
    prev = s.nodes.filter((nd) => !nd.fixed).map((nd) => toVec(nd.lat, nd.lon));
  }
  return out;
}

