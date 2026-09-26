import { useMemo } from 'react';
import DeckGL from '@deck.gl/react';
import { WebMercatorViewport } from '@deck.gl/core';
import { ScatterplotLayer } from '@deck.gl/layers';
import { Map as BaseMap } from 'react-map-gl/maplibre';
import type { Point } from '../data/types';
import type { Solution } from '../solver/solve';
import type { NodeStats } from '../solver/model';
import { NODE_PALETTE, POINT_COLORS, cssColor, type RGB } from '../utils/colors';

const DEFAULT_VIEW_STATE = { longitude: -98.5, latitude: 39.8, zoom: 3.6, pitch: 0, bearing: 0 };
/** Left padding keeps points clear of the parameters pane. */
const FIT_PADDING = { left: 330, right: 40, top: 40, bottom: 40 };

function fitView(points: Point[]) {
  if (points.length === 0 || typeof window === 'undefined') return DEFAULT_VIEW_STATE;
  let [minLon, minLat, maxLon, maxLat] = [180, 90, -180, -90];
  for (const p of points) {
    minLon = Math.min(minLon, p.lon);
    maxLon = Math.max(maxLon, p.lon);
    minLat = Math.min(minLat, p.lat);
    maxLat = Math.max(maxLat, p.lat);
  }
  // Pad a degenerate (single-point) box so fitBounds has something to fit.
  if (maxLon - minLon < 0.5) [minLon, maxLon] = [minLon - 1, maxLon + 1];
  if (maxLat - minLat < 0.5) [minLat, maxLat] = [minLat - 1, maxLat + 1];
  const vp = new WebMercatorViewport({ width: window.innerWidth, height: window.innerHeight - 45 });
  const { longitude, latitude, zoom } = vp.fitBounds(
    [
      [minLon, minLat],
      [maxLon, maxLat],
    ],
    { padding: FIT_PADDING },
  );
  return { ...DEFAULT_VIEW_STATE, longitude, latitude, zoom: Math.min(zoom, 9) };
}
const MAP_STYLE = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';

interface Props {
  points: Point[];
  /** Fixed nodes are only shown when the run will use them. */
  showFixed: boolean;
  /** A solution to draw, with the points it was solved on (its allocation indexes their demand). */
  solution?: { solution: Solution; points: Point[] };
}

const nodeColor = (j: number): RGB => NODE_PALETTE[j % NODE_PALETTE.length];

export default function MapView({ points: currentPoints, showFixed: showFixedParam, solution }: Props) {
  // While a solution is shown, draw the data it was solved on so allocation colors line up.
  const points = solution ? solution.points : currentPoints;
  const showFixed = solution ? false : showFixedParam;
  const demandAlloc = useMemo(() => {
    if (!solution) return null;
    const byPoint = new Map<Point, number>();
    solution.points
      .filter((p) => p.type === 'demand')
      .forEach((p, i) => byPoint.set(p, solution.solution.alloc[i]));
    return byPoint;
  }, [solution]);
  const maxThroughput = useMemo(
    () => (solution ? Math.max(1, ...solution.solution.nodes.map((n) => n.throughput)) : 1),
    [solution],
  );
  const maxVolume = useMemo(
    () => points.reduce((m, p) => (p.type === 'fixed' ? m : Math.max(m, p.volume)), 1),
    [points],
  );

  // Refit only when a different dataset arrives, not on every edit.
  const fitKey = points.length;
  const initialViewState = useMemo(() => fitView(points), [fitKey]); // eslint-disable-line react-hooks/exhaustive-deps

  const layers = [
    new ScatterplotLayer<Point>({
      id: 'demand',
      data: points.filter((p) => p.type === 'demand'),
      getPosition: (p) => [p.lon, p.lat],
      getRadius: (p) => 2 + 10 * Math.sqrt(p.volume / maxVolume),
      radiusUnits: 'pixels',
      getFillColor: (p) => {
        const j = demandAlloc?.get(p);
        return [...(j === undefined ? POINT_COLORS.demand : nodeColor(j)), 150];
      },
      updateTriggers: { getFillColor: demandAlloc },
      pickable: true,
    }),
    new ScatterplotLayer<Point>({
      id: 'sources',
      data: points.filter((p) => p.type === 'source'),
      getPosition: (p) => [p.lon, p.lat],
      getRadius: 9,
      radiusUnits: 'pixels',
      getFillColor: [...POINT_COLORS.source, 230],
      stroked: true,
      getLineColor: [255, 255, 255],
      lineWidthUnits: 'pixels',
      getLineWidth: 1.5,
      pickable: true,
    }),
    new ScatterplotLayer<Point>({
      id: 'fixed',
      data: showFixed ? points.filter((p) => p.type === 'fixed') : [],
      getPosition: (p) => [p.lon, p.lat],
      getRadius: 10,
      radiusUnits: 'pixels',
      filled: false,
      stroked: true,
      getLineColor: [...POINT_COLORS.fixed, 255],
      lineWidthUnits: 'pixels',
      getLineWidth: 3,
      pickable: true,
    }),
    new ScatterplotLayer<NodeStats & { j: number }>({
      id: 'nodes',
      data: solution ? solution.solution.nodes.map((n, j) => ({ ...n, j })) : [],
      getPosition: (n) => [n.lon, n.lat],
      getRadius: (n) => 7 + 13 * Math.sqrt(n.throughput / maxThroughput),
      radiusUnits: 'pixels',
      getFillColor: (n) => [...nodeColor(n.j), 255],
      stroked: true,
      getLineColor: (n) => (n.fixed ? [...POINT_COLORS.fixed, 255] : [255, 255, 255, 255]),
      lineWidthUnits: 'pixels',
      getLineWidth: (n) => (n.fixed ? 4 : 2),
      pickable: true,
    }),
  ];

  return (
    <div className="map-container">
      <DeckGL
        initialViewState={initialViewState}
        controller
        layers={layers}
        getTooltip={({ object }) => {
          if (!object) return null;
          if ('throughput' in object) {
            const n = object as NodeStats;
            return `${n.fixed ? 'Fixed node' : 'Node'}\nThroughput: ${Math.round(n.throughput).toLocaleString()}\n${n.lat.toFixed(3)}, ${n.lon.toFixed(3)}`;
          }
          const p = object as Point;
          const vol = p.type === 'fixed' ? '' : `\nVolume: ${p.volume.toLocaleString()}`;
          const rows = p.rows.length > 1 ? `\n${p.rows.length} rows aggregated` : '';
          return `${p.type[0].toUpperCase() + p.type.slice(1)}${vol}\n${p.lat.toFixed(3)}, ${p.lon.toFixed(3)}${rows}`;
        }}
      >
        <BaseMap mapStyle={MAP_STYLE} />
      </DeckGL>
      <div className="legend">
        {(['demand', 'source', 'fixed'] as const)
          .filter((t) => t !== 'fixed' || showFixed || solution?.solution.nodes.some((n) => n.fixed))
          .map((t) => (
          <div key={t} className="legend-row">
            <span
              className={`swatch ${t}`}
              style={t === 'fixed' ? { borderColor: cssColor(POINT_COLORS[t]) } : { background: cssColor(POINT_COLORS[t]) }}
            />
            {t === 'fixed' ? 'Fixed node' : t === 'demand' && solution ? 'Demand (colored by node)' : t[0].toUpperCase() + t.slice(1)}
          </div>
        ))}
        {solution && (
          <div className="legend-row">
            <span className="swatch node" />
            Node (size = throughput)
          </div>
        )}
      </div>
    </div>
  );
}
