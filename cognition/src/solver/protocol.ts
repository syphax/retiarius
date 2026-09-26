import type { Point } from '../data/types';
import type { Params } from '../params';
import type { Solution } from './solve';

export interface SweepRequest {
  id: number;
  points: Point[];
  params: Params;
}

export type SweepMessage =
  | { id: number; type: 'solution'; solution: Solution }
  | { id: number; type: 'done' }
  | { id: number; type: 'error'; message: string };
