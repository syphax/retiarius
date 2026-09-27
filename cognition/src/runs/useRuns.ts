import { useCallback, useEffect, useRef, useState } from 'react';
import type { Point } from '../data/types';
import type { Params } from '../params';
import type { SolveMessage, SolveRequest } from '../solver/protocol';
import type { Solution } from '../solver/solve';
import { describeScenario, diffParams } from './describe';

/** A solve request before it gets an id (Omit that distributes over the union). */
type RequestBody = SolveRequest extends infer R ? (R extends unknown ? Omit<R, 'id'> : never) : never;

export interface Run {
  id: number;
  /** A sweep over N (charted), or a single-N ad-hoc re-run (tabled only). */
  kind: 'sweep' | 'adhoc';
  label: string;
  /** Scenario summary (sweeps) or the parameter changes vs. the base run (ad-hoc). */
  detail: string;
  status: 'running' | 'done' | 'error' | 'cancelled';
  solutions: Solution[];
  /** Inputs the solutions were computed from (allocation indexes into these demand points). */
  points: Point[];
  params: Params;
  error?: string;
  /** For ad-hoc runs: the solution it was warm-started from. */
  base?: { runId: number; runLabel: string; solution: Solution; params: Params };
  /** Sweeps only: shown on the results charts. */
  onChart: boolean;
}

/**
 * All runs this session, newest last. Solving happens in a web worker, one request at a time;
 * starting a new one cancels whatever is in flight.
 */
export function useRuns() {
  const [runs, setRuns] = useState<Run[]>([]);
  const [activeId, setActiveId] = useState<number | null>(null);
  const worker = useRef<Worker | null>(null);
  const nextId = useRef(1);
  const sweepCount = useRef(0);
  const adhocCount = useRef(0);

  const patch = useCallback((id: number, p: Partial<Run> | ((r: Run) => Partial<Run>)) => {
    setRuns((rs) => rs.map((r) => (r.id === id ? { ...r, ...(typeof p === 'function' ? p(r) : p) } : r)));
  }, []);

  const stopWorker = useCallback(() => {
    worker.current?.terminate();
    worker.current = null;
    // Anything still running was interrupted.
    setRuns((rs) => rs.map((r) => (r.status === 'running' ? { ...r, status: 'cancelled' } : r)));
  }, []);

  useEffect(() => () => worker.current?.terminate(), []);

  const start = useCallback(
    (run: Omit<Run, 'id' | 'status' | 'solutions'>, req: RequestBody) => {
      stopWorker();
      const id = nextId.current++;
      setRuns((rs) => [...rs, { ...run, id, status: 'running', solutions: [] }]);
      setActiveId(id);
      const w = new Worker(new URL('../solver/worker.ts', import.meta.url), { type: 'module' });
      worker.current = w;
      w.onmessage = (e: MessageEvent<SolveMessage>) => {
        const msg = e.data;
        if (msg.id !== id) return;
        if (msg.type === 'solution') {
          patch(id, (r) => ({ solutions: [...r.solutions, msg.solution] }));
          return;
        }
        patch(id, msg.type === 'done' ? { status: 'done' } : { status: 'error', error: msg.message });
        w.terminate();
        if (worker.current === w) worker.current = null;
      };
      w.postMessage({ ...req, id } as SolveRequest);
    },
    [patch, stopWorker],
  );

  const runSweep = useCallback(
    (points: Point[], params: Params) => {
      const fixed = points.filter((p) => p.type === 'fixed').length;
      start(
        {
          kind: 'sweep',
          label: `Run ${++sweepCount.current}`,
          detail: describeScenario(params, fixed),
          points,
          params,
          onChart: true,
        },
        { kind: 'sweep', points, params },
      );
    },
    [start],
  );

  /** Re-solve one N from `base` with new data/parameters, warm-started from its node locations. */
  const runAdhoc = useCallback(
    (base: Run, solution: Solution, points: Point[], params: Params) => {
      const changes = diffParams(base.params, params);
      start(
        {
          kind: 'adhoc',
          label: `Ad-hoc ${++adhocCount.current}`,
          detail: changes.length ? changes.join(', ') : 'no parameter changes',
          points,
          params,
          base: { runId: base.id, runLabel: base.label, solution, params: base.params },
          onChart: false,
        },
        {
          kind: 'single',
          points,
          params,
          n: solution.n,
          warm: solution.nodes.filter((nd) => !nd.fixed).map((nd) => [nd.lat, nd.lon]),
        },
      );
    },
    [start],
  );

  const remove = useCallback(
    (id: number) => {
      const r = runs.find((x) => x.id === id);
      if (r?.status === 'running') stopWorker();
      setRuns((rs) => rs.filter((x) => x.id !== id));
      setActiveId((a) => (a === id ? null : a));
    },
    [runs, stopWorker],
  );

  return {
    runs,
    active: runs.find((r) => r.id === activeId),
    setActive: setActiveId,
    runSweep,
    runAdhoc,
    cancel: stopWorker,
    remove,
    toggleChart: useCallback((id: number) => patch(id, (r) => ({ onChart: !r.onChart })), [patch]),
  };
}
