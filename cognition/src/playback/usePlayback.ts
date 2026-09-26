import { useCallback, useEffect, useRef, useState } from 'react';
import type { Solution } from '../solver/solve';

/** Solver iterations shown per second at 1× speed. */
const FRAMES_PER_SECOND = 10;
/** Pause on each N's final positions before moving to the next N (seconds at 1×). */
const HOLD_SECONDS = 1.2;

export interface Playback {
  /** N being shown (null before the first solution arrives). */
  n: number | null;
  /** Playhead in frames; fractional values interpolate between iterations. */
  t: number;
  playing: boolean;
  /** Advance through every N in turn. */
  playAll: boolean;
  speed: number;
}

/**
 * Drives the search animation. Time lives in refs so the animation loop never reads stale state;
 * React state mirrors it for rendering.
 */
export function usePlayback(solutions: Solution[], sweepRunning: boolean) {
  const [state, setState] = useState<Playback>({ n: null, t: 0, playing: false, playAll: true, speed: 1 });
  const live = useRef({ state, solutions, sweepRunning });
  live.current = { state, solutions, sweepRunning };

  const update = useCallback((patch: Partial<Playback>) => {
    live.current.state = { ...live.current.state, ...patch };
    setState(live.current.state);
  }, []);

  useEffect(() => {
    if (!state.playing) return;
    let raf = 0;
    let last = performance.now();
    let hold = 0;
    const tick = (now: number) => {
      const dt = Math.min(0.1, (now - last) / 1000); // don't leap after a background-tab stall
      last = now;
      const { state: s, solutions: sols, sweepRunning: running } = live.current;
      if (s.n === null) {
        // Waiting for the sweep's first N.
        if (sols.length) update({ n: sols[0].n, t: 0 });
        raf = requestAnimationFrame(tick);
        return;
      }
      const sol = sols.find((x) => x.n === s.n);
      if (!sol) {
        raf = requestAnimationFrame(tick);
        return;
      }
      const end = sol.frames.length - 1;
      if (s.t < end) {
        update({ t: Math.min(end, s.t + dt * FRAMES_PER_SECOND * s.speed) });
      } else if ((hold += dt * s.speed) >= HOLD_SECONDS) {
        hold = 0;
        const next = sols.find((x) => x.n > sol.n);
        if (s.playAll && next) update({ n: next.n, t: 0 });
        else if (!(s.playAll && running)) {
          update({ playing: false });
          return;
        }
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [state.playing, update]);

  const endOf = (n: number) => (live.current.solutions.find((s) => s.n === n)?.frames.length ?? 1) - 1;

  return {
    playback: state,
    /** Start playing a new sweep from its first N as results arrive. */
    startSweep: useCallback(() => update({ n: null, t: 0, playing: true, playAll: true }), [update]),
    /** Jump to N's final positions. */
    select: useCallback((n: number) => update({ n, t: endOf(n), playing: false }), [update]), // eslint-disable-line react-hooks/exhaustive-deps
    togglePlay: useCallback(() => {
      const s = live.current.state;
      if (s.playing) return update({ playing: false });
      // At the end of a single N, play restarts it.
      if (s.n !== null && s.t >= endOf(s.n) && !s.playAll) return update({ t: 0, playing: true });
      update({ playing: true });
    }, [update]), // eslint-disable-line react-hooks/exhaustive-deps
    replay: useCallback(() => update({ t: 0, playing: true, playAll: false }), [update]),
    scrub: useCallback((t: number) => update({ t, playing: false }), [update]),
    setSpeed: useCallback((speed: number) => update({ speed }), [update]),
    setPlayAll: useCallback((playAll: boolean) => update({ playAll }), [update]),
  };
}
