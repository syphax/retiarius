import type { SweepState } from '../solver/useSweep';
import { fmtCost, fmtDist, fmtPct } from '../utils/format';

interface Props {
  sweep: SweepState;
  onShowOnMap: (n: number) => void;
}

/** Sweep results by N. (Charts arrive in milestone 4.) */
export default function ResultsTab({ sweep, onShowOnMap }: Props) {
  if (sweep.solutions.length === 0) {
    return <div className="placeholder">Run all scenarios to see cost and service results by number of nodes.</div>;
  }
  const units = sweep.params?.units ?? 'mi';
  const base = sweep.solutions[0].metrics.totalCost;
  return (
    <div className="results-tab">
      <table className="results-table">
        <thead>
          <tr>
            <th>N</th>
            <th>Total cost</th>
            <th>vs N = {sweep.solutions[0].n}</th>
            <th>Inbound</th>
            <th>Outbound</th>
            <th>Avg distance</th>
            <th>Within {sweep.params?.serviceDistance} {units}</th>
            <th>Start</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {sweep.solutions.map((s) => (
            <tr key={s.n}>
              <td>{s.n}</td>
              <td>{fmtCost(s.metrics.totalCost)}</td>
              <td>{fmtPct(s.metrics.totalCost / base - 1)}</td>
              <td>{fmtCost(s.metrics.inboundCost)}</td>
              <td>{fmtCost(s.metrics.outboundCost)}</td>
              <td>{fmtDist(s.metrics.avgDistance, units)}</td>
              <td>{fmtPct(s.metrics.pctWithin)}</td>
              <td className="muted">{s.warmStarted ? `warm (best of ${s.runs})` : `cold (best of ${s.runs})`}</td>
              <td>
                <button onClick={() => onShowOnMap(s.n)}>Map</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="hint">
        Costs are relative: volume × distance ({units}), with inbound weighted by the inbound : outbound ratio. Charts
        come in milestone 4.
      </p>
    </div>
  );
}
