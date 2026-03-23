import React, { useState, useEffect, useRef, useCallback } from 'react';
import { MapView } from './components/MapView';
import { TimeControls } from './components/TimeControls';
import { Sidebar } from './components/Sidebar';
import { DateDisplay } from './components/DateDisplay';
import { loadFlowData } from './utils/dataLoader';
import { CONFIG } from './config';
import './styles/app.css';

export default function App() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const [currentTime, setCurrentTime] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);
  const [playbackSpeed, setPlaybackSpeed] = useState(CONFIG.defaults.playbackSpeed);

  const [params, setParams] = useState({
    aggregation: CONFIG.defaults.aggregation,
    sizeAttribute: CONFIG.defaults.sizeAttribute,
    sizeScale: CONFIG.defaults.sizeScale,
    colorAttribute: CONFIG.defaults.colorAttribute,
    pathMode: CONFIG.defaults.pathMode,
    trailLength: CONFIG.defaults.trailLength,
  });
  const [appliedParams, setAppliedParams] = useState({ ...params });
  const [dirty, setDirty] = useState(false);

  const animFrameRef = useRef(null);
  const lastFrameTimeRef = useRef(null);

  // Load data on mount
  useEffect(() => {
    loadFlowData(CONFIG.dataFile)
      .then((result) => {
        setData(result);
        setCurrentTime(result.timeRange[0]);
        setLoading(false);
      })
      .catch((err) => {
        console.error('Failed to load data:', err);
        setError(err.message);
        setLoading(false);
      });
  }, []);

  // Animation loop
  useEffect(() => {
    if (!isPlaying || !data) return;

    const secsPerDay = CONFIG.speeds[playbackSpeed];
    const msPerSimDay = secsPerDay * 1000;
    const simMsPerRealMs = (24 * 60 * 60 * 1000) / msPerSimDay;

    const animate = (timestamp) => {
      if (lastFrameTimeRef.current !== null) {
        const realDelta = timestamp - lastFrameTimeRef.current;
        const simDelta = realDelta * simMsPerRealMs;

        setCurrentTime((prev) => {
          const next = prev + simDelta;
          if (next >= data.timeRange[1]) {
            setIsPlaying(false);
            return data.timeRange[1];
          }
          return next;
        });
      }
      lastFrameTimeRef.current = timestamp;
      animFrameRef.current = requestAnimationFrame(animate);
    };

    lastFrameTimeRef.current = null;
    animFrameRef.current = requestAnimationFrame(animate);

    return () => {
      if (animFrameRef.current) {
        cancelAnimationFrame(animFrameRef.current);
      }
    };
  }, [isPlaying, playbackSpeed, data]);

  // Count live particles for tooltip threshold
  const liveParticleCount = data
    ? data.trips.filter((t) => currentTime >= t.startTime && currentTime <= t.endTime).length
    : 0;

  const handleParamsChange = useCallback((newParams) => {
    setParams(newParams);
    setDirty(true);
  }, []);

  const handleRecalculate = useCallback(() => {
    setAppliedParams({ ...params });
    setDirty(false);
  }, [params]);

  const handlePlayPause = useCallback(() => {
    setIsPlaying((prev) => !prev);
  }, []);

  const handleTimeChange = useCallback((t) => {
    setCurrentTime(t);
    setIsPlaying(false);
  }, []);

  if (loading) {
    return (
      <div className="loading-screen">
        <div className="loading-spinner" />
        <p>Loading flow data...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="loading-screen">
        <p className="error-msg">Error: {error}</p>
        <p>Make sure sample_data.csv is in the public/ directory.</p>
      </div>
    );
  }

  return (
    <div className="app">
      <div className="hamburger-menu" title="Menu (coming soon)">
        <span /><span /><span />
      </div>

      <DateDisplay currentTime={currentTime} />

      <div className="particle-count">
        {liveParticleCount.toLocaleString()} particles
      </div>

      <Sidebar
        params={params}
        onChange={handleParamsChange}
        onRecalculate={handleRecalculate}
        dirty={dirty}
      />

      <MapView
        trips={data.trips}
        origins={data.origins}
        odRoutes={data.odRoutes}
        stats={data.stats}
        currentTime={currentTime}
        params={appliedParams}
        liveParticleCount={liveParticleCount}
      />

      <TimeControls
        currentTime={currentTime}
        timeRange={data.timeRange}
        isPlaying={isPlaying}
        playbackSpeed={playbackSpeed}
        onTimeChange={handleTimeChange}
        onPlayPause={handlePlayPause}
        onSpeedChange={setPlaybackSpeed}
      />
    </div>
  );
}
