/// <reference lib="webworker" />
import { buildModel } from './model';
import type { SweepMessage, SweepRequest } from './protocol';
import { DEFAULT_OPTIONS, sweep } from './solve';

const post = (msg: SweepMessage, transfer: Transferable[] = []) => self.postMessage(msg, { transfer });

self.onmessage = (e: MessageEvent<SweepRequest>) => {
  const { id, points, params } = e.data;
  try {
    const model = buildModel(points, params);
    sweep(model, params.minNodes, params.maxNodes, DEFAULT_OPTIONS, (solution) =>
      post({ id, type: 'solution', solution }, [solution.alloc.buffer]),
    );
    post({ id, type: 'done' });
  } catch (err) {
    post({ id, type: 'error', message: err instanceof Error ? err.message : String(err) });
  }
};
