import { useState } from 'react';
import type { Run } from '../runs/useRuns';
import { fmtCost, fmtDist, fmtPct } from '../utils/format';
import ResultsCharts, { runColor } from './ResultsCharts';

interface Props {
  runs: Run[];
  activeId: number | undefined;
  onShowOnMap: (runId: number, n: number) => void;
  onToggleChart: (runId: number) => void;
  onRemove: (runId: number) => void;
}

const statusText = (r: Run) =>
  r.status === 'running' ? 'solving…' : r.status === 'error' ? `error: ${r.error}` : r.status === 'cancelled' ? 'cancelled' : '';

export default function ResultsTab({ runs, activeId, onShowOnMap, onToggleChart, onRemove }: Props) {
  const sweeps = runs.filter((r) => r.kind === 'sweep');
  const adhocs = runs.filter((r) => r.kind === 'adhoc');
  const [tableRunId, setTableRunId] = useState<number | null>(null);
  const tableRun =
    sweeps.find((r) => r.id === tableRunId) ?? sweeps.find((r) => r.id === activeId) ?? sweeps.at(-1);

  if (runs.length === 0) {
    return <div className="placeholder">Run all scenarios to see cost and service results by number of nodes.</div>;
  }

  return (
    <div className="results-tab">
      <section>
        <h2>Runs</h2>
        <table className="results-table runs-table">
          <thead>
            <tr>
              <th />
              <th className="left">Run</th>
              <th className="left">Scenario</th>
              <th>N</th>
              <th>Chart</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {sweeps.map((r) => (
              <tr key={r.id} className={r.id === tableRun?.id ? 'selected' : ''} onClick={() => setTableRunId(r.id)}>
                <td>
                  <span className="run-swatch" style={{ background: runColor(runs, r.id) }} />
                </td>
                <td className="left">{r.label}</td>
                <td className="left muted">
                  {r.detail} <span className="warn">{statusText(r)}</span>
                </td>
                <td>
                  {r.solutions.length ? `${r.solutions[0].n}–${r.solutions.at(-1)!.n}` : '—'}
                </td>
                <td>
                  <input type="checkbox" checked={r.onChart} onChange={() => onToggleChart(r.id)} onClick={(e) => e.stopPropagation()} />
                </td>
                <td>
                  <button onClick={(e) => (e.stopPropagation(), onRemove(r.id))} title="Delete run">
                    ×
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section>
        <ResultsCharts runs={runs} />
      </section>

      {tableRun && tableRun.solutions.length > 0 && (
        <section>
          <h2>
            {tableRun.label} by N <small className="muted">{tableRun.detail}</small>
          </h2>
          <SweepTable run={tableRun} onShowOnMap={onShowOnMap} />
        </section>
      )}

      {adhocs.length > 0 && (
        <section>
          <h2>Ad-hoc re-runs</h2>
          <table className="results-table">
            <thead>
              <tr>
                <th className="left">Run</th>
                <th className="left">From</th>
                <th className="left">Changes</th>
                <th>Total cost</th>
                <th>vs base</th>
                <th>Inbound</th>
                <th>Outbound</th>
                <th>Avg distance</th>
                <th>Within band</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {adhocs.map((r) => {
                const s = r.solutions[0];
                const base = r.base!.solution;
                return (
                  <tr key={r.id}>
                    <td className="left">{r.label}</td>
                    <td className="left">
                      {r.base!.runLabel}, N = {base.n}
                    </td>
                    <td className="left muted">
                      {r.detail} <span className="warn">{statusText(r)}</span>
                    </td>
                    {s ? (
                      <>
                        <td>{fmtCost(s.metrics.totalCost)}</td>
                        <td>{fmtPct(s.metrics.totalCost / base.metrics.totalCost - 1)}</td>
                        <td>{fmtCost(s.metrics.inboundCost)}</td>
                        <td>{fmtCost(s.metrics.outboundCost)}</td>
                        <td>{fmtDist(s.metrics.avgDistance, r.params.units)}</td>
                        <td>
                          {fmtPct(s.metrics.pctWithin)} <small className="muted">≤ {r.params.serviceDistance}</small>
                        </td>
                      </>
                    ) : (
                      <td colSpan={6} />
                    )}
                    <td className="actions">
                      {s && <button onClick={() => onShowOnMap(r.id, s.n)}>Map</button>}
                      <button onClick={() => onRemove(r.id)} title="Delete run">
                        ×
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </section>
      )}

      <p className="hint">
        Costs are relative: volume × distance, with inbound weighted by the inbound : outbound ratio. Runs with
        different ratios or units aren't directly comparable in cost. Ad-hoc re-runs are warm-started from the
        base solution and aren't plotted; re-run all scenarios with the new settings to chart them.
      </p>
    </div>
  );
}

/** Realized source shares, e.g. "72 / 18 / 10 / 0". */
const sharesText = (shares: number[]) => shares.map((x) => Math.round(x * 100)).join(' / ');

function SweepTable({ run, onShowOnMap }: { run: Run; onShowOnMap: (runId: number, n: number) => void }) {
  const units = run.params.units;
  const first = run.solutions[0];
  const sources = run.points.filter((p) => p.type === 'source');
  const total = sources.reduce((t, p) => t + p.volume, 0);
  const stated = sources.map((p) => p.volume / total);
  return (
    <table className="results-table">
      <thead>
        <tr>
          <th>N</th>
          <th>Total cost</th>
          <th>vs N = {first.n}</th>
          <th>Inbound</th>
          <th>Outbound</th>
          <th>Avg distance</th>
          <th>
            Within {run.params.serviceDistance} {units}
          </th>
          {sources.length > 1 && (
            <th title={`Share of inbound from each source, in data order. Stated: ${sharesText(stated)}%`}>
              Source shares %<br />
              <small className="muted">stated {sharesText(stated)}</small>
            </th>
          )}
          <th>Start</th>
          <th />
        </tr>
      </thead>
      <tbody>
        {run.solutions.map((s) => (
          <tr key={s.n}>
            <td>{s.n}</td>
            <td>{fmtCost(s.metrics.totalCost)}</td>
            <td>{fmtPct(s.metrics.totalCost / first.metrics.totalCost - 1)}</td>
            <td>{fmtCost(s.metrics.inboundCost)}</td>
            <td>{fmtCost(s.metrics.outboundCost)}</td>
            <td>{fmtDist(s.metrics.avgDistance, units)}</td>
            <td>{fmtPct(s.metrics.pctWithin)}</td>
            {sources.length > 1 && <td>{sharesText(s.metrics.sourceShares)}</td>}
            <td className="muted" title="Warm = continued from the previous N; cold = fresh random start. Moves = node relocations kept by the polish pass.">
              {s.warmStarted ? 'warm' : 'cold'}
              {s.polishMoves > 0 && ` + ${s.polishMoves} move${s.polishMoves > 1 ? 's' : ''}`} (best of {s.runs})
            </td>
            <td>
              <button onClick={() => onShowOnMap(run.id, s.n)}>Map</button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
