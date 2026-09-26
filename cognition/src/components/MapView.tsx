import { useMemo } from 'react';
import DeckGL from '@deck.gl/react';
import { WebMercatorViewport } from '@deck.gl/core';
import { LineLayer, PathLayer, ScatterplotLayer } from '@deck.gl/layers';
import { Map as BaseMap } from 'react-map-gl/maplibre';
import type { Point } from '../data/types';
import type { FrameView, NodeView } from '../playback/frameView';
import { greatCirclePath } from '../utils/greatCircle';
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

export interface DisplayOptions {
  /** Each node's path from its starting guess to its current position. */
  paths: boolean;
  /** Line from each demand point to its node. */
  demandLines: boolean;
  /** Great-circle arc from each source to each node, width ∝ inbound volume. */
  sourceLines: boolean;
}

interface Props {
  points: Point[];
  /** Fixed nodes are only shown when the run will use them. */
  showFixed: boolean;
  /** A moment of a search to draw. Its own points are drawn so allocation lines up. */
  view: FrameView | null;
  display: DisplayOptions;
}

const nodeColor = (j: number): RGB => NODE_PALETTE[j % NODE_PALETTE.length];

interface SourceArc {
  path: [number, number][];
  volume: number;
}

export default function MapView({ points: currentPoints, showFixed: showFixedParam, view, display }: Props) {
  const points = view ? view.points : currentPoints;
  const showFixed = view ? false : showFixedParam;
  const demand = useMemo(() => (view ? view.demand : points.filter((p) => p.type === 'demand')), [view?.demand, points]); // eslint-disable-line react-hooks/exhaustive-deps

  const maxVolume = useMemo(
    () => points.reduce((m, p) => (p.type === 'fixed' ? m : Math.max(m, p.volume)), 1),
    [points],
  );

  // Refit only when a different dataset arrives, not on every edit.
  const fitKey = points.length;
  const initialViewState = useMemo(() => fitView(points), [fitKey]); // eslint-disable-line react-hooks/exhaustive-deps

  const nodes = view?.nodes ?? [];
  const maxThroughput = Math.max(1, ...nodes.map((n) => n.throughput));
  const alloc = view?.alloc;

  const sourceArcs: SourceArc[] = [];
  if (view && display.sourceLines) {
    for (const { point, share } of view.sources) {
      for (const n of nodes) {
        if (n.throughput > 0) {
          sourceArcs.push({ path: greatCirclePath(point.lat, point.lon, n.lat, n.lon), volume: share * n.throughput });
        }
      }
    }
  }
  const maxArc = Math.max(1, ...sourceArcs.map((a) => a.volume));

  const layers = [
    new LineLayer<Point>({
      id: 'demand-lines',
      data: view && display.demandLines ? demand : [],
      getSourcePosition: (p) => [p.lon, p.lat],
      getTargetPosition: (_p, { index }) => {
        const n = nodes[alloc![index]];
        return [n.lon, n.lat];
      },
      getColor: (_p, { index }) => [...nodeColor(alloc![index]), 70],
      getWidth: 1,
      widthUnits: 'pixels',
      updateTriggers: { getTargetPosition: nodes, getColor: alloc },
    }),
    new PathLayer<SourceArc>({
      id: 'source-arcs',
      data: sourceArcs,
      getPath: (a) => a.path,
      getColor: [...POINT_COLORS.source, 80],
      getWidth: (a) => 1 + 5 * Math.sqrt(a.volume / maxArc),
      widthUnits: 'pixels',
      capRounded: true,
      jointRounded: true,
    }),
    new ScatterplotLayer<Point>({
      id: 'demand',
      data: demand,
      getPosition: (p) => [p.lon, p.lat],
      getRadius: (p) => 2 + 10 * Math.sqrt(p.volume / maxVolume),
      radiusUnits: 'pixels',
      getFillColor: (_p, { index }) => [...(alloc ? nodeColor(alloc[index]) : POINT_COLORS.demand), 150],
      updateTriggers: { getFillColor: alloc },
      pickable: true,
    }),
    new PathLayer<{ path: [number, number][]; j: number }>({
      id: 'trails',
      data: view && display.paths ? view.trails.map((path, j) => ({ path, j })).filter((d) => !nodes[d.j].fixed) : [],
      getPath: (d) => d.path,
      getColor: (d) => [...nodeColor(d.j), 220],
      getWidth: 2.5,
      widthUnits: 'pixels',
      capRounded: true,
      jointRounded: true,
    }),
    new ScatterplotLayer<{ pos: [number, number]; j: number }>({
      id: 'starts',
      data: view && display.paths ? view.trails.map((path, j) => ({ pos: path[0], j })).filter((d) => !nodes[d.j].fixed) : [],
      getPosition: (d) => d.pos,
      getRadius: 5,
      radiusUnits: 'pixels',
      filled: false,
      stroked: true,
      getLineColor: (d) => [...nodeColor(d.j), 255],
      lineWidthUnits: 'pixels',
      getLineWidth: 2,
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
    new ScatterplotLayer<NodeView>({
      id: 'nodes',
      data: nodes,
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
            const n = object as NodeView;
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
        <div className="legend-row">
          <span className="swatch" style={{ background: cssColor(POINT_COLORS.demand) }} />
          {view ? 'Demand (colored by node)' : 'Demand'}
        </div>
        <div className="legend-row">
          <span className="swatch" style={{ background: cssColor(POINT_COLORS.source) }} />
          Source
        </div>
        {(showFixed || nodes.some((n) => n.fixed)) && (
          <div className="legend-row">
            <span className="swatch fixed" style={{ borderColor: cssColor(POINT_COLORS.fixed) }} />
            Fixed node
          </div>
        )}
        {view && (
          <div className="legend-row">
            <span className="swatch node" />
            Node (size = throughput)
          </div>
        )}
        {view && display.paths && (
          <div className="legend-row">
            <span className="swatch start" />
            Starting guess
          </div>
        )}
      </div>
    </div>
  );
}
