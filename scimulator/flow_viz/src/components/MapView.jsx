import React, { useMemo } from 'react';
import { Map } from 'react-map-gl/maplibre';
import DeckGL from '@deck.gl/react';
import { TripsLayer } from '@deck.gl/geo-layers';
import { ScatterplotLayer, TextLayer, PathLayer } from '@deck.gl/layers';
import { CONFIG } from '../config';
import { buildColorAccessor, buildSizeAccessor } from '../utils/colorScales';
import { interpolateGreatCircle } from '../utils/greatCircle';
import 'maplibre-gl/dist/maplibre-gl.css';

// Binary search: count how many values in sorted array are <= target
function countUpTo(sortedArr, target) {
  let lo = 0;
  let hi = sortedArr.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (sortedArr[mid] <= target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

export function MapView({
  trips,
  origins,
  odRoutes,
  stats,
  currentTime,
  params,
  liveParticleCount,
}) {
  const colorAccessor = useMemo(
    () => buildColorAccessor(params.colorAttribute, stats),
    [params.colorAttribute, stats]
  );

  const sizeAccessor = useMemo(
    () => buildSizeAccessor(
      params.sizeAttribute,
      stats,
      params.sizeScale,
      CONFIG.particleSizeRange
    ),
    [params.sizeAttribute, params.sizeScale, stats]
  );

  const trailLengthMs = params.trailLength * 24 * 60 * 60 * 1000;

  // --- Data for "Recent Trail" mode (TripsLayer) ---
  // deck.gl v9: separate getPath (coordinates) and getTimestamps
  const recentTrailData = useMemo(() => {
    if (params.pathMode !== 'recent') return [];
    return trips.map((trip) => ({
      ...trip,
      path: trip.waypoints.map((wp) => wp.coordinates),
      timestamps: trip.waypoints.map((wp) => wp.timestamp),
    }));
  }, [trips, params.pathMode]);

  // --- Data for "Full Route" mode (PathLayer) ---
  // Shows full origin->destination arc for active trips + recently delivered (fading)
  const fullRouteData = useMemo(() => {
    if (params.pathMode !== 'full_route') return [];
    const fadeMs = trailLengthMs || (0.5 * 24 * 60 * 60 * 1000); // default 0.5 day fade
    const result = [];
    for (const trip of trips) {
      // Active: currently in transit
      if (currentTime >= trip.startTime && currentTime <= trip.endTime) {
        result.push({ trip, opacity: 0.7 });
      }
      // Recently delivered: fade out over trailLength days
      else if (currentTime > trip.endTime && currentTime < trip.endTime + fadeMs) {
        const fade = 1 - (currentTime - trip.endTime) / fadeMs;
        result.push({ trip, opacity: fade * 0.7 });
      }
    }
    return result;
  }, [trips, currentTime, params.pathMode, trailLengthMs]);

  // --- Data for "Cumulative" mode (PathLayer) ---
  // Routes get brighter/wider as more trips complete on them
  const { cumulativeData, maxCount } = useMemo(() => {
    if (params.pathMode !== 'cumulative') return { cumulativeData: [], maxCount: 1 };
    let max = 1;
    const data = [];
    for (const route of odRoutes) {
      const count = countUpTo(route.deliveryTimes, currentTime);
      if (count > 0) {
        data.push({ ...route, count });
        if (count > max) max = count;
      }
    }
    return { cumulativeData: data, maxCount: max };
  }, [odRoutes, currentTime, params.pathMode]);

  // --- Live particle positions (always shown) ---
  const liveParticles = useMemo(() => {
    const particles = [];
    for (const trip of trips) {
      if (currentTime >= trip.startTime && currentTime <= trip.endTime) {
        const t = (currentTime - trip.startTime) / (trip.endTime - trip.startTime);
        const [lng, lat] = interpolateGreatCircle(
          trip.originLat, trip.originLng,
          trip.destLat, trip.destLng,
          t
        );
        particles.push({ position: [lng, lat], trip });
      }
    }
    return particles;
  }, [trips, currentTime]);

  // --- Build layers ---
  const layers = [];

  // Path mode layers
  if (params.pathMode === 'recent' && trailLengthMs > 0) {
    layers.push(new TripsLayer({
      id: 'recent-trail',
      data: recentTrailData,
      getPath: (d) => d.path,
      getTimestamps: (d) => d.timestamps,
      getColor: (d) => colorAccessor(d),
      widthMinPixels: 2,
      widthMaxPixels: 4,
      opacity: 0.6,
      currentTime,
      trailLength: trailLengthMs,
      capRounded: true,
      jointRounded: true,
    }));
  }

  if (params.pathMode === 'full_route') {
    layers.push(new PathLayer({
      id: 'full-route',
      data: fullRouteData,
      getPath: (d) => d.trip.waypoints.map((wp) => wp.coordinates),
      getColor: (d) => {
        const base = colorAccessor(d.trip);
        return [base[0], base[1], base[2], Math.round(d.opacity * 255)];
      },
      widthMinPixels: 1,
      widthMaxPixels: 3,
      widthScale: 1,
      capRounded: true,
      jointRounded: true,
    }));
  }

  if (params.pathMode === 'cumulative') {
    layers.push(new PathLayer({
      id: 'cumulative-routes',
      data: cumulativeData,
      getPath: (d) => d.path,
      getColor: (d) => {
        const intensity = Math.min(1, d.count / maxCount);
        // Bright cyan that intensifies with volume
        const alpha = Math.round(40 + intensity * 200);
        return [0, 180 + intensity * 75, 220 + intensity * 35, alpha];
      },
      getWidth: (d) => {
        const intensity = Math.min(1, d.count / maxCount);
        return 1 + intensity * 6;
      },
      widthUnits: 'pixels',
      widthMinPixels: 1,
      capRounded: true,
      jointRounded: true,
    }));
  }

  // Live particle dots (always on)
  layers.push(new ScatterplotLayer({
    id: 'particles-layer',
    data: liveParticles,
    getPosition: (d) => d.position,
    getFillColor: (d) => colorAccessor(d.trip),
    getRadius: (d) => sizeAccessor(d.trip),
    radiusUnits: 'pixels',
    radiusMinPixels: 1,
    opacity: 0.9,
    pickable: liveParticleCount < CONFIG.tooltipThreshold,
  }));

  // Origin site dots
  layers.push(new ScatterplotLayer({
    id: 'origins-layer',
    data: origins,
    getPosition: (d) => [d.longitude, d.latitude],
    getFillColor: [255, 255, 255, 180],
    getLineColor: [0, 200, 255, 200],
    getRadius: 6,
    radiusUnits: 'pixels',
    stroked: true,
    lineWidthMinPixels: 2,
    pickable: true,
  }));

  // Origin labels
  layers.push(new TextLayer({
    id: 'origin-labels',
    data: origins,
    getPosition: (d) => [d.longitude, d.latitude],
    getText: (d) => d.name,
    getColor: [255, 255, 255, 200],
    getSize: 12,
    getTextAnchor: 'start',
    getAlignmentBaseline: 'center',
    getPixelOffset: [10, 0],
    fontFamily: 'monospace',
  }));

  const getTooltip = ({ object }) => {
    if (!object) return null;
    if (object.trip) {
      const t = object.trip;
      return {
        text: `Value: $${t.value.toLocaleString()}\nWeight: ${t.weight} lbs\nDelivery: ${t.deliveryDays.toFixed(1)} days\nBrand: ${t.brand}`,
      };
    }
    if (object.name) {
      return { text: object.name };
    }
    return null;
  };

  return (
    <DeckGL
      initialViewState={CONFIG.initialViewState}
      controller={true}
      layers={layers}
      getTooltip={getTooltip}
    >
      <Map mapStyle={CONFIG.mapStyle} />
    </DeckGL>
  );
}
