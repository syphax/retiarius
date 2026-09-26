import { useCallback, useEffect, useRef, useState } from 'react';
import type { Point } from '../data/types';
import type { Params } from '../params';
import type { SweepMessage, SweepRequest } from './protocol';
import type { Solution } from './solve';

export interface SweepState {
  status: 'idle' | 'running' | 'done' | 'error';
  solutions: Solution[];
  /** Inputs the solutions were computed from (allocation indexes into these demand points). */
  points: Point[];
  params: Params | null;
  error?: string;
}

const IDLE: SweepState = { status: 'idle', solutions: [], points: [], params: null };

/** Runs N-sweeps in a web worker. Starting a new sweep cancels the one in flight. */
export function useSweep() {
  const [state, setState] = useState<SweepState>(IDLE);
  const worker = useRef<Worker | null>(null);
  const runId = useRef(0);

  const stop = useCallback(() => {
    worker.current?.terminate();
    worker.current = null;
  }, []);

  useEffect(() => stop, [stop]);

  const run = useCallback(
    (points: Point[], params: Params) => {
      stop();
      const id = ++runId.current;
      const w = new Worker(new URL('./worker.ts', import.meta.url), { type: 'module' });
      worker.current = w;
      setState({ status: 'running', solutions: [], points, params });
      w.onmessage = (e: MessageEvent<SweepMessage>) => {
        const msg = e.data;
        if (msg.id !== id) return;
        if (msg.type === 'solution') {
          setState((s) => ({ ...s, solutions: [...s.solutions, msg.solution] }));
        } else if (msg.type === 'done') {
          setState((s) => ({ ...s, status: 'done' }));
          stop();
        } else {
          setState((s) => ({ ...s, status: 'error', error: msg.message }));
          stop();
        }
      };
      const req: SweepRequest = { id, points, params };
      w.postMessage(req);
    },
    [stop],
  );

  const cancel = useCallback(() => {
    stop();
    setState((s) => (s.status === 'running' ? { ...s, status: s.solutions.length ? 'done' : 'idle' } : s));
  }, [stop]);

  return { sweep: state, runSweep: run, cancelSweep: cancel };
}
