import type { Params } from '../params';

/** Short scenario description, e.g. "brownfield (2 fixed) · in:out 0.4". */
export function describeScenario(params: Params, fixedCount: number): string {
  const field = params.useFixedNodes && fixedCount > 0 ? `brownfield (${fixedCount} fixed)` : 'greenfield';
  return `${field} · in:out ${params.inboundRatio}`;
}

const LABELS: Record<keyof Params, string> = {
  minNodes: 'min N',
  maxNodes: 'max N',
  inboundRatio: 'in:out',
  serviceDistance: 'service distance',
  units: 'units',
  circuity: 'circuity',
  useFixedNodes: 'fixed nodes',
};

const show = (v: Params[keyof Params]) => (typeof v === 'boolean' ? (v ? 'on' : 'off') : String(v));

/**
 * What changed between two parameter sets, e.g. ["in:out 0.4 → 1"]. N bounds are left out:
 * an ad-hoc re-run solves a single N, so they don't apply.
 */
export function diffParams(from: Params, to: Params): string[] {
  return (Object.keys(LABELS) as (keyof Params)[])
    .filter((k) => k !== 'minNodes' && k !== 'maxNodes' && from[k] !== to[k])
    .map((k) => `${LABELS[k]} ${show(from[k])} → ${show(to[k])}`);
}
