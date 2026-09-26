import type { Solution } from '../solver/solve';
import type { SweepState } from '../solver/useSweep';
import { fmtCost, fmtDist, fmtPct } from '../utils/format';

interface Props {
  sweep: SweepState;
  selected: Solution | undefined;
  onSelect: (n: number) => void;
  onCancel: () => void;
}

/** Bottom strip on the map: pick N, see its headline metrics. */
export default function SolutionBar({ sweep, selected, onSelect, onCancel }: Props) {
  if (sweep.status === 'idle') return null;
  const units = sweep.params?.units ?? 'mi';
  const band = sweep.params ? `${sweep.params.serviceDistance} ${units}` : '';
  const lastN = sweep.solutions.at(-1)?.n;
  return (
    <div className="solution-bar">
      <div className="n-chips">
        <span className="label">N</span>
        {sweep.solutions.map((s) => (
          <button key={s.n} className={s.n === selected?.n ? 'chip active' : 'chip'} onClick={() => onSelect(s.n)}>
            {s.n}
          </button>
        ))}
        {sweep.status === 'running' && (
          <>
            <span className="solving">Solving N = {lastN === undefined ? '…' : lastN + 1}…</span>
            <button onClick={onCancel}>Cancel</button>
          </>
        )}
        {sweep.status === 'error' && <span className="error">{sweep.error}</span>}
      </div>
      {selected && (
        <div className="metrics">
          <span>
            Cost <b>{fmtCost(selected.metrics.totalCost)}</b>
            <small>
              {' '}
              (in {fmtCost(selected.metrics.inboundCost)} · out {fmtCost(selected.metrics.outboundCost)})
            </small>
          </span>
          <span>
            Avg distance <b>{fmtDist(selected.metrics.avgDistance, units)}</b>
          </span>
          <span>
            Within {band} <b>{fmtPct(selected.metrics.pctWithin)}</b>
          </span>
        </div>
      )}
    </div>
  );
}
