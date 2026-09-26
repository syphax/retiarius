/** Compact number for costs (volume × distance can get large). */
export function fmtCost(x: number): string {
  const a = Math.abs(x);
  if (a >= 1e9) return `${(x / 1e9).toFixed(2)}B`;
  if (a >= 1e6) return `${(x / 1e6).toFixed(2)}M`;
  if (a >= 1e3) return `${(x / 1e3).toFixed(1)}K`;
  return x.toFixed(1);
}

export const fmtPct = (x: number) => `${(x * 100).toFixed(1)}%`;
export const fmtDist = (x: number, units: string) => `${x.toFixed(0)} ${units}`;
