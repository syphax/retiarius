import { useState } from 'react';
import type { ValidationResult } from '../data/types';
import type { Params } from '../params';

interface Props {
  params: Params;
  onChange: (p: Params) => void;
  validation: ValidationResult;
  running: boolean;
  /** Data or parameters changed since the results on screen were computed. */
  stale: boolean;
  onRunAll: () => void;
  /** The solution on the map, which an ad-hoc re-run would start from. */
  rerunFrom: { label: string; n: number } | null;
  onRerun: () => void;
}

function NumberField({
  label,
  value,
  onChange,
  min,
  max,
  step = 1,
  suffix,
}: {
  label: string;
  value: number;
  onChange: (v: number) => void;
  min?: number;
  max?: number;
  step?: number;
  suffix?: string;
}) {
  return (
    <label className="field">
      <span>{label}</span>
      <span className="field-input">
        <input
          type="number"
          value={value}
          min={min}
          max={max}
          step={step}
          onChange={(e) => {
            const v = e.target.valueAsNumber;
            if (Number.isFinite(v)) onChange(v);
          }}
        />
        {suffix && <span className="suffix">{suffix}</span>}
      </span>
    </label>
  );
}

export default function ParamsPane({ params, onChange, validation, running, stale, onRunAll, rerunFrom, onRerun }: Props) {
  const [open, setOpen] = useState(true);
  const [advanced, setAdvanced] = useState(false);
  const set = <K extends keyof Params>(k: K, v: Params[K]) => onChange({ ...params, [k]: v });

  const counts = { demand: 0, source: 0, fixed: 0 };
  for (const p of validation.points) counts[p.type] += 1;
  const fixedInUse = params.useFixedNodes ? counts.fixed : 0;

  const problems: string[] = [];
  if (params.minNodes < 1) problems.push('Min nodes must be at least 1.');
  if (params.maxNodes < params.minNodes) problems.push('Max nodes must be ≥ min nodes.');
  if (fixedInUse > params.maxNodes) problems.push(`Max nodes must be ≥ the ${fixedInUse} fixed nodes.`);
  if (counts.demand === 0) problems.push('No valid demand points.');

  if (!open) {
    return (
      <button className="pane-toggle collapsed" onClick={() => setOpen(true)} title="Show parameters">
        ☰ Parameters
      </button>
    );
  }

  return (
    <aside className="params-pane">
      <div className="pane-header">
        <h2>Parameters</h2>
        <button className="icon" onClick={() => setOpen(false)} title="Hide parameters">
          ‹
        </button>
      </div>

      <section>
        <NumberField label="Min nodes" value={params.minNodes} min={1} max={50} onChange={(v) => set('minNodes', v)} />
        <NumberField label="Max nodes" value={params.maxNodes} min={1} max={50} onChange={(v) => set('maxNodes', v)} />
        <label className="field checkbox">
          <input
            type="checkbox"
            checked={params.useFixedNodes}
            disabled={counts.fixed === 0}
            onChange={(e) => set('useFixedNodes', e.target.checked)}
          />
          <span>
            Use fixed nodes ({counts.fixed})
            {params.useFixedNodes && counts.fixed > 0 && (
              <small> — N includes these; runs start at N = {Math.max(params.minNodes, counts.fixed)}</small>
            )}
          </span>
        </label>
      </section>

      <section>
        <label className="field">
          <span>Objective</span>
          <select value="cost" disabled>
            <option value="cost">Min weighted cost</option>
            <option value="coverage">% within distance (coming soon)</option>
          </select>
        </label>
        <NumberField
          label="Inbound : outbound cost"
          value={params.inboundRatio}
          min={0}
          step={0.05}
          onChange={(v) => set('inboundRatio', v)}
        />
        {counts.source === 0 && <p className="hint">No sources in the data, so inbound cost is ignored.</p>}
        <NumberField
          label="Service distance"
          value={params.serviceDistance}
          min={0}
          step={25}
          suffix={params.units}
          onChange={(v) => set('serviceDistance', v)}
        />
        <label className="field">
          <span>Distance units</span>
          <select value={params.units} onChange={(e) => set('units', e.target.value as Params['units'])}>
            <option value="mi">Miles</option>
            <option value="km">Kilometers</option>
          </select>
        </label>
      </section>

      <section>
        <button className="disclosure" onClick={() => setAdvanced(!advanced)}>
          {advanced ? '▾' : '▸'} Advanced
        </button>
        {advanced && (
          <NumberField
            label="Circuity factor"
            value={params.circuity}
            min={1}
            step={0.05}
            onChange={(v) => set('circuity', v)}
          />
        )}
      </section>

      <div className="pane-footer">
        <div className="data-summary">
          {counts.demand} demand · {counts.source} source · {counts.fixed} fixed
          {validation.issues.size > 0 && <span className="warn"> · {validation.issues.size} flagged rows excluded</span>}
        </div>
        {problems.map((p) => (
          <div key={p} className="error">
            {p}
          </div>
        ))}
        {stale && !running && <div className="warn">Data or parameters changed since the last run.</div>}
        {rerunFrom && (
          <button
            className="rerun"
            disabled={counts.demand === 0 || running}
            onClick={onRerun}
            title="Solve this one N again with the current data and parameters, starting from the solution on the map"
          >
            Re-run N = {rerunFrom.n} from {rerunFrom.label}
          </button>
        )}
        <button className="primary run-all" disabled={problems.length > 0 || running} onClick={onRunAll}>
          {running ? 'Running…' : 'Run All Scenarios'}
        </button>
      </div>
    </aside>
  );
}
