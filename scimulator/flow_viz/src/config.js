// Parse URL params to allow dynamic data source
const urlParams = new URLSearchParams(window.location.search);

export const CONFIG = {
  // Data source: URL param ?data=/api/results/xxx/flow-data?db=yyy
  // Falls back to local sample CSV
  dataFile: urlParams.get('data') || '/sample_data.csv',

  // Playback speed settings: speed level -> seconds per simulated day
  speeds: {
    1: 6.0,
    2: 4.0,
    3: 2.0,
    4: 1.0,
    5: 0.5,
  },

  // Default sidebar parameter values
  defaults: {
    aggregation: 'none',       // 'none' | 'day_zip3'
    sizeAttribute: 'none',     // 'none' | 'value' | 'weight' | 'cube'
    sizeScale: 3,              // 1-10
    colorAttribute: 'none',    // 'none' | 'value' | 'weight' | 'cube' | 'delivery_days' | 'brand'
    pathMode: 'recent',        // 'none' | 'recent' | 'full_route' | 'cumulative'
    trailLength: 0.5,          // days (0 = none, max 2.0) — used by 'recent' and 'full_route' fade
    playbackSpeed: 3,          // speed level 1-5
  },

  // Tooltip threshold: only show tooltips when particle count is below this
  tooltipThreshold: 1000,

  // Max simultaneous particles before aggregation is recommended
  maxParticles: 100000,

  // Trail settings
  trailWidthRange: [1, 4],

  // Particle size range (pixels)
  particleSizeRange: [2, 20],

  // Map initial view
  initialViewState: {
    longitude: -98.5,
    latitude: 39.8,
    zoom: 4,
    pitch: 0,
    bearing: 0,
  },

  // Dark basemap style (free, no token required)
  mapStyle: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
};
