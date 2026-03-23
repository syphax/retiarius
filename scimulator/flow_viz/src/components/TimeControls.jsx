import React from 'react';
import { CONFIG } from '../config';

export function TimeControls({
  currentTime,
  timeRange,
  isPlaying,
  playbackSpeed,
  onTimeChange,
  onPlayPause,
  onSpeedChange,
}) {
  const [minTime, maxTime] = timeRange;

  const handleSliderChange = (e) => {
    onTimeChange(Number(e.target.value));
  };

  const formatDate = (ts) => {
    const d = new Date(ts);
    return d.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    });
  };

  return (
    <div className="time-controls">
      <button
        className="play-btn"
        onClick={onPlayPause}
        title={isPlaying ? 'Pause' : 'Play'}
      >
        {isPlaying ? '⏸' : '▶'}
      </button>

      <span className="time-label">{formatDate(minTime)}</span>

      <input
        type="range"
        className="time-slider"
        min={minTime}
        max={maxTime}
        value={currentTime}
        onChange={handleSliderChange}
        step={(maxTime - minTime) / 1000}
      />

      <span className="time-label">{formatDate(maxTime)}</span>

      <select
        className="speed-select"
        value={playbackSpeed}
        onChange={(e) => onSpeedChange(Number(e.target.value))}
        title="Playback speed"
      >
        {Object.keys(CONFIG.speeds).map((level) => (
          <option key={level} value={level}>
            {level}x
          </option>
        ))}
      </select>
    </div>
  );
}
