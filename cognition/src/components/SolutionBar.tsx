import type { FrameView } from '../playback/frameView';
import type { usePlayback } from '../playback/usePlayback';
import type { Run } from '../runs/useRuns';
import { fmtCost, fmtDist, fmtPct } from '../utils/format';
import type { DisplayOptions } from './MapView';

interface Props {
  run: Run | undefined;
  view: FrameView | null;
  player: ReturnType<typeof usePlayback>;
  display: DisplayOptions;
  onDisplay: (d: DisplayOptions) => void;
  onCancel: () => void;
}

const SPEEDS = [0.5, 1, 2, 4];

/** Bottom strip on the map: pick N, play the search, toggle layers, see metrics. */
export default function SolutionBar({ run, view, player, display, onDisplay, onCancel }: Props) {
  if (!run) return null;
  const { playback } = player;
  const units = run.params.units;
  const band = `${run.params.serviceDistance} ${units}`;
  const lastN = run.solutions.at(-1)?.n;
  const atEnd = view ? view.iteration === view.iterations && playback.t >= view.iterations : false;
  const toggle = (k: keyof DisplayOptions) => onDisplay({ ...display, [k]: !display[k] });

  return (
    <div className="solution-bar">
      <div className="run-label">
        <b>{run.label}</b>
        {run.base && ` · from ${run.base.runLabel}, N = ${run.base.solution.n}`} · {run.detail}
      </div>
      <div className="n-chips">
        <span className="label">N</span>
        {run.solutions.map((s) => (
          <button key={s.n} className={s.n === playback.n ? 'chip active' : 'chip'} onClick={() => player.select(s.n)}>
            {s.n}
          </button>
        ))}
        {run.status === 'running' && (
          <>
            <span className="solving">
              Solving N = {run.kind === 'adhoc' ? run.base!.solution.n : lastN === undefined ? '…' : lastN + 1}…
            </span>
            <button onClick={onCancel}>Cancel</button>
          </>
        )}
        {run.status === 'error' && <span className="error">{run.error}</span>}
        {run.status === 'cancelled' && <span className="warn">cancelled</span>}
      </div>

      {view && (
        <div className="transport">
          <button className="play" onClick={player.togglePlay} title={playback.playing ? 'Pause' : 'Play'}>
            {playback.playing ? '❚❚' : '▶'}
          </button>
          <button onClick={player.replay} title="Replay this N's search from its starting guess">
            ↺
          </button>
          <input
            type="range"
            className="scrubber"
            min={0}
            max={view.iterations}
            step={0.01}
            value={Math.min(playback.t, view.iterations)}
            onChange={(e) => player.scrub(e.target.valueAsNumber)}
          />
          <span className="iteration">
            {view.iteration === 0 ? 'start' : `iter ${view.iteration}`} / {view.iterations}
            {atEnd && ' · final'}
          </span>
          <select value={playback.speed} onChange={(e) => player.setSpeed(Number(e.target.value))} title="Speed">
            {SPEEDS.map((s) => (
              <option key={s} value={s}>
                {s}×
              </option>
            ))}
          </select>
          {run.kind === 'sweep' && <label className="toggle" title="After each N, continue to the next">
            <input type="checkbox" checked={playback.playAll} onChange={(e) => player.setPlayAll(e.target.checked)} />
            All N
          </label>}
        </div>
      )}

      {view && (
        <div className="display-toggles">
          <label className="toggle">
            <input type="checkbox" checked={display.paths} onChange={() => toggle('paths')} />
            Node paths
          </label>
          <label className="toggle">
            <input type="checkbox" checked={display.demandLines} onChange={() => toggle('demandLines')} />
            Demand lines
          </label>
          <label className="toggle">
            <input type="checkbox" checked={display.sourceLines} onChange={() => toggle('sourceLines')} />
            Source lines
          </label>
        </div>
      )}

      {view && (
        <div className="metrics">
          <span>
            Cost <b>{fmtCost(view.metrics.totalCost)}</b>
            <small>
              {' '}
              (in {fmtCost(view.metrics.inboundCost)} · out {fmtCost(view.metrics.outboundCost)})
            </small>
          </span>
          <span>
            Avg distance <b>{fmtDist(view.metrics.avgDistance, units)}</b>
          </span>
          <span>
            Within {band} <b>{fmtPct(view.metrics.pctWithin)}</b>
          </span>
        </div>
      )}
    </div>
  );
}
