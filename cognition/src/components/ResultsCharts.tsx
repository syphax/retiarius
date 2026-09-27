import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { Run } from '../runs/useRuns';
import { fmtCost, fmtPct } from '../utils/format';

/** One color per charted run; inbound is drawn as a lighter shade of the run's color. */
export const RUN_COLORS = ['#00c8ff', '#ff9f43', '#a78bfa', '#4ade80', '#f472b6', '#facc15'];

export const runColor = (runs: Run[], id: number) => {
  const sweeps = runs.filter((r) => r.kind === 'sweep');
  return RUN_COLORS[Math.max(0, sweeps.findIndex((r) => r.id === id)) % RUN_COLORS.length];
};

/** Blend a hex color toward the card background (so the legend swatch matches the bar). */
function shade(hex: string, amount = 0.55): string {
  const bg = [0x14, 0x14, 0x1e];
  const c = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));
  return `#${c.map((v, i) => Math.round(v + (bg[i] - v) * amount).toString(16).padStart(2, '0')).join('')}`;
}

const AXIS = { stroke: '#707078', fontSize: 11 };
const GRID = 'rgba(255, 255, 255, 0.06)';
const TOOLTIP = {
  contentStyle: { background: '#14141e', border: '1px solid rgba(255,255,255,0.15)', borderRadius: 4, fontSize: 12 },
  labelStyle: { color: '#e0e0e0' },
  labelFormatter: (n: unknown) => `N = ${n}`,
  cursor: { fill: 'rgba(255,255,255,0.04)' },
};

interface Props {
  runs: Run[];
}

/** Cost (stacked inbound/outbound), avg distance, and % within X vs N, one series per charted sweep. */
export default function ResultsCharts({ runs }: Props) {
  const charted = runs.filter((r) => r.kind === 'sweep' && r.onChart && r.solutions.length > 0);
  if (charted.length === 0) return <p className="hint">Tick "Chart" on a run to plot it.</p>;

  const ns = [...new Set(charted.flatMap((r) => r.solutions.map((s) => s.n)))].sort((a, b) => a - b);
  const data = ns.map((n) => {
    const row: Record<string, number> = { n };
    for (const r of charted) {
      const s = r.solutions.find((x) => x.n === n);
      if (!s) continue;
      row[`in_${r.id}`] = s.metrics.inboundCost;
      row[`out_${r.id}`] = s.metrics.outboundCost;
      row[`avg_${r.id}`] = s.metrics.avgDistance;
      row[`pct_${r.id}`] = s.metrics.pctWithin;
    }
    return row;
  });
  const units = [...new Set(charted.map((r) => r.params.units))];
  const bands = [...new Set(charted.map((r) => `${r.params.serviceDistance} ${r.params.units}`))];

  return (
    <div className="charts">
      <div className="chart-card wide">
        <h3>Total cost by N <small>stacked: outbound (bright) + inbound (dark)</small></h3>
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={data} barGap={2}>
            <CartesianGrid stroke={GRID} vertical={false} />
            <XAxis dataKey="n" {...AXIS} />
            <YAxis tickFormatter={fmtCost} {...AXIS} width={56} />
            <Tooltip {...TOOLTIP} formatter={(v) => fmtCost(Number(v))} />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            {charted.flatMap((r) => [
              <Bar key={`out_${r.id}`} dataKey={`out_${r.id}`} name={`${r.label} outbound`} stackId={`s${r.id}`} fill={runColor(runs, r.id)} isAnimationActive={false} />,
              <Bar
                key={`in_${r.id}`}
                dataKey={`in_${r.id}`}
                name={`${r.label} inbound`}
                stackId={`s${r.id}`}
                fill={shade(runColor(runs, r.id))}
                isAnimationActive={false}
              />,
            ])}
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="chart-card">
        <h3>Average outbound distance <small>{units.join(' / ')}</small></h3>
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={data}>
            <CartesianGrid stroke={GRID} vertical={false} />
            <XAxis dataKey="n" {...AXIS} />
            <YAxis {...AXIS} width={48} />
            <Tooltip {...TOOLTIP} formatter={(v) => Number(v).toFixed(0)} />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            {charted.map((r) => (
              <Line key={r.id} dataKey={`avg_${r.id}`} name={r.label} stroke={runColor(runs, r.id)} strokeWidth={2} dot={{ r: 3 }} connectNulls isAnimationActive={false} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="chart-card">
        <h3>Demand within service distance <small>{bands.join(' / ')}</small></h3>
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={data}>
            <CartesianGrid stroke={GRID} vertical={false} />
            <XAxis dataKey="n" {...AXIS} />
            <YAxis tickFormatter={(v) => `${Math.round(v * 100)}%`} domain={[0, 1]} {...AXIS} width={44} />
            <Tooltip {...TOOLTIP} formatter={(v) => fmtPct(Number(v))} />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            {charted.map((r) => (
              <Line key={r.id} dataKey={`pct_${r.id}`} name={r.label} stroke={runColor(runs, r.id)} strokeWidth={2} dot={{ r: 3 }} connectNulls isAnimationActive={false} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
