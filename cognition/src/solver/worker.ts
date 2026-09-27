/// <reference lib="webworker" />
import { buildModel } from './model';
import type { SolveMessage, SolveRequest } from './protocol';
import { DEFAULT_OPTIONS, solveN, sweep, type Solution } from './solve';
import { toVec } from './sphere';

const post = (msg: SolveMessage, transfer: Transferable[] = []) => self.postMessage(msg, { transfer });

self.onmessage = (e: MessageEvent<SolveRequest>) => {
  const req = e.data;
  const { id } = req;
  const send = (solution: Solution) => post({ id, type: 'solution', solution }, [solution.alloc.buffer]);
  try {
    const model = buildModel(req.points, req.params);
    if (req.kind === 'sweep') {
      sweep(model, req.params.minNodes, req.params.maxNodes, DEFAULT_OPTIONS, send);
    } else {
      const warm = req.warm.map(([lat, lon]) => toVec(lat, lon));
      send(solveN(model, req.n, { ...DEFAULT_OPTIONS, restarts: 3 }, warm));
    }
    post({ id, type: 'done' });
  } catch (err) {
    post({ id, type: 'error', message: err instanceof Error ? err.message : String(err) });
  }
};
