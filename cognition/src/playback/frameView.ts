import { useMemo } from 'react';
import type { Point } from '../data/types';
import { allocate, evaluate, type Metrics, type Model } from '../solver/model';
import type { Solution } from '../solver/solve';
import { toVec } from '../solver/sphere';

export interface NodeView {
  j: number;
  lat: number;
  lon: number;
  fixed: boolean;
  throughput: number;
}

/** What the map draws for one moment of a search. */
export interface FrameView {
  /** The data the solution was computed on. */
  points: Point[];
  /** Demand points in model order (aligned with `alloc`). */
  demand: Point[];
  /** Sources in model order, with their share of supply. */
  sources: { point: Point; share: number }[];
  alloc: Int32Array;
  nodes: NodeView[];
  /** Per node, [lon, lat] positions from the start of the search to now. */
  trails: [number, number][][];
  metrics: Metrics;
  /** Solver iteration shown (0 = starting guess). */
  iteration: number;
  iterations: number;
}

interface FrameData {
  alloc: Int32Array;
  throughput: number[];
  metrics: Metrics;
}

export type FrameCache = Map<number, FrameData>;

/**
 * Interpolated node positions at playhead `t`, with allocation and metrics for the current solver
 * iteration. Allocation is recomputed per iteration (not per animation tick) and cached.
 */
export function buildFrameView(
  model: Model,
  points: Point[],
  demand: Point[],
  sources: FrameView['sources'],
  solution: Solution,
  t: number,
  cache: FrameCache = new Map(),
): FrameView {
  const frames = solution.frames;
  const last = frames.length - 1;
  const i = Math.max(0, Math.min(last, Math.floor(t)));
  const f = Math.max(0, Math.min(1, t - i));

  let data = cache.get(i);
  if (!data) {
    const vecs = frames[i].nodes.map(([lat, lon]) => toVec(lat, lon));
    const alloc = i === last ? solution.alloc : allocate(model, vecs);
    const { metrics, perNode } = evaluate(model, vecs, alloc);
    data = { alloc, throughput: perNode.map((p) => p.throughput), metrics };
    cache.set(i, data);
  }
  const { alloc, throughput, metrics } = data;

  const from = frames[i].nodes;
  const to = frames[Math.min(last, i + 1)].nodes;
  const nodes: NodeView[] = from.map(([lat, lon], j) => ({
    j,
    lat: lat + (to[j][0] - lat) * f,
    lon: lon + (to[j][1] - lon) * f,
    fixed: j < model.fixed.length,
    throughput: throughput[j],
  }));
  const trails = nodes.map((n, j) => {
    const path: [number, number][] = [];
    for (let k = 0; k <= i; k++) path.push([frames[k].nodes[j][1], frames[k].nodes[j][0]]);
    path.push([n.lon, n.lat]);
    return path;
  });

  return { points, demand, sources, alloc, nodes, trails, metrics, iteration: i, iterations: last };
}

export function useFrameView(
  model: Model | null,
  points: Point[],
  solution: Solution | undefined,
  t: number,
): FrameView | null {
  const demand = useMemo(() => points.filter((p) => p.type === 'demand'), [points]);
  const sources = useMemo(
    () => points.filter((p) => p.type === 'source').map((point, k) => ({ point, share: model?.sourceShare[k] ?? 0 })),
    [points, model],
  );
  const cache = useMemo<FrameCache>(() => new Map(), [model, solution]); // eslint-disable-line react-hooks/exhaustive-deps
  if (!model || !solution) return null;
  return buildFrameView(model, points, demand, sources, solution, t, cache);
}
