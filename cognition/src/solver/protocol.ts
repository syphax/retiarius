import type { Point } from '../data/types';
import type { Params } from '../params';
import type { Solution } from './solve';

export type SolveRequest =
  /** Solve N = params.minNodes … params.maxNodes. */
  | { id: number; kind: 'sweep'; points: Point[]; params: Params }
  /** Solve one N, warm-started from an earlier solution's free nodes ([lat, lon]). */
  | { id: number; kind: 'single'; points: Point[]; params: Params; n: number; warm: [number, number][] };

export type SolveMessage =
  | { id: number; type: 'solution'; solution: Solution }
  | { id: number; type: 'done' }
  | { id: number; type: 'error'; message: string };
