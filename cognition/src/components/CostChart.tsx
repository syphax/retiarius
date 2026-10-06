import { useEffect, useRef, useState } from 'react';
import type { Frame } from '../solver/solve';
import { fmtCost } from '../utils/format';

interface Props {
  frames: Frame[];
  /** Playhead in frames; the line is drawn up to here. */
  t: number;
  onSeek: (t: number) => void;
}

const HEIGHT = 72;
const PAD = { top: 6, right: 8, bottom: 14, left: 48 };

/**
 * Total cost vs solver iteration for one N, drawn up to the playhead so it grows as the search
 * plays. The y-axis spans the whole search so it doesn't rescale while drawing. Polish relocations
 * show as small markers. Click or drag to scrub.
 */
export default function CostChart({ frames, t, onSeek }: Props) {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  const [hover, setHover] = useState<number | null>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([e]) => setWidth(e.contentRect.width));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const last = frames.length - 1;
  const costs = frames.map((f) => f.totalCost);
  let lo = Math.min(...costs);
  let hi = Math.max(...costs);
  if (hi - lo < 1e-9 * Math.max(1, hi)) [lo, hi] = [lo * 0.999, hi * 1.001 + 1e-9];

  const plotW = Math.max(1, width - PAD.left - PAD.right);
  const plotH = HEIGHT - PAD.top - PAD.bottom;
  const x = (k: number) => PAD.left + (last > 0 ? (k / last) * plotW : 0);
  const y = (c: number) => PAD.top + ((hi - c) / (hi - lo)) * plotH;

  const tt = Math.max(0, Math.min(last, t));
  const i = Math.floor(tt);
  const tipCost = i < last ? costs[i] + (costs[i + 1] - costs[i]) * (tt - i) : costs[last];
  let path = '';
  for (let k = 0; k <= i; k++) path += `${k ? 'L' : 'M'}${x(k).toFixed(1)},${y(costs[k]).toFixed(1)}`;
  if (tt > i) path += `L${x(tt).toFixed(1)},${y(tipCost).toFixed(1)}`;

  const frameAt = (clientX: number) => {
    const r = ref.current!.getBoundingClientRect();
    const k = ((clientX - r.left - PAD.left) / plotW) * last;
    return Math.max(0, Math.min(last, k));
  };
  const shown = hover !== null && hover <= tt ? hover : null;
  // Put the readout left of the crosshair near the right edge.
  const flip = shown !== null && x(shown) > PAD.left + plotW * 0.6;

  return (
    <div
      ref={ref}
      className="cost-chart"
      onPointerDown={(e) => {
        e.currentTarget.setPointerCapture(e.pointerId);
        onSeek(frameAt(e.clientX));
      }}
      onPointerMove={(e) => {
        const k = frameAt(e.clientX);
        setHover(Math.round(k));
        if (e.buttons & 1) onSeek(k);
      }}
      onPointerLeave={() => setHover(null)}
    >
      {width > 0 && (
        <svg width={width} height={HEIGHT} role="img" aria-label={`Total cost by iteration, ${fmtCost(costs[0])} to ${fmtCost(costs[last])}`}>
          <line className="grid" x1={PAD.left} x2={PAD.left + plotW} y1={y(hi)} y2={y(hi)} />
          <line className="grid" x1={PAD.left} x2={PAD.left + plotW} y1={y(lo)} y2={y(lo)} />
          <text className="tick" x={PAD.left - 6} y={y(hi)} dy="0.35em" textAnchor="end">{fmtCost(hi)}</text>
          <text className="tick" x={PAD.left - 6} y={y(lo)} dy="0.35em" textAnchor="end">{fmtCost(lo)}</text>
          <text className="tick" x={PAD.left} y={HEIGHT - 2}>0</text>
          <text className="tick" x={PAD.left + plotW} y={HEIGHT - 2} textAnchor="end">{last}</text>

          <path className="cost-line" d={path} />
          {frames.map((f, k) =>
            f.jump && k <= tt ? <circle key={k} className="jump" cx={x(k)} cy={y(costs[k])} r={2.5} /> : null,
          )}

          {shown !== null && (
            <>
              <line className="crosshair" x1={x(shown)} x2={x(shown)} y1={PAD.top} y2={PAD.top + plotH} />
              <text
                className="readout"
                x={x(shown) + (flip ? -6 : 6)}
                y={PAD.top + 8}
                textAnchor={flip ? 'end' : 'start'}
              >
                {shown === 0 ? 'start' : `iter ${shown}`} · {fmtCost(costs[shown])}
                {frames[shown].jump ? ' · polish move' : ''}
              </text>
            </>
          )}
          <circle className="playhead" cx={x(tt)} cy={y(tipCost)} r={4} />
        </svg>
      )}
    </div>
  );
}
