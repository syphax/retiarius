// Viridis palette (sampled at 10 stops) for continuous attributes
const VIRIDIS = [
  [68, 1, 84],
  [72, 36, 117],
  [65, 68, 135],
  [53, 95, 141],
  [42, 120, 142],
  [33, 145, 140],
  [34, 168, 132],
  [68, 191, 112],
  [122, 209, 81],
  [189, 223, 38],
];

// Blue-to-Red diverging scale for delivery days (fast=blue, slow=red)
const BLUE_RED = [
  [8, 48, 107],
  [33, 102, 172],
  [67, 147, 195],
  [146, 197, 222],
  [209, 229, 240],
  [253, 219, 199],
  [244, 165, 130],
  [214, 96, 77],
  [178, 24, 43],
  [127, 0, 0],
];

// D3 Category10 palette for categorical attributes
const CATEGORY10 = [
  [31, 119, 180],
  [255, 127, 14],
  [44, 160, 44],
  [214, 39, 40],
  [148, 103, 189],
  [140, 86, 75],
  [227, 119, 194],
  [127, 127, 127],
  [188, 189, 34],
  [23, 190, 207],
];

const OTHER_COLOR = [100, 100, 100];

function interpolatePalette(palette, t) {
  const clamped = Math.max(0, Math.min(1, t));
  const idx = clamped * (palette.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.min(lo + 1, palette.length - 1);
  const f = idx - lo;
  return [
    Math.round(palette[lo][0] + f * (palette[hi][0] - palette[lo][0])),
    Math.round(palette[lo][1] + f * (palette[hi][1] - palette[lo][1])),
    Math.round(palette[lo][2] + f * (palette[hi][2] - palette[lo][2])),
  ];
}

function normalize(value, min, max) {
  if (max === min) return 0.5;
  return (value - min) / (max - min);
}

/**
 * Build a color accessor function based on the selected attribute.
 * @param {string} attribute - 'none' | 'value' | 'weight' | 'cube' | 'delivery_days' | 'brand'
 * @param {Object} stats - Data statistics from dataLoader
 * @returns {function(Object): [number, number, number, number]} RGBA color accessor
 */
export function buildColorAccessor(attribute, stats) {
  if (attribute === 'none') {
    return () => [0, 200, 255, 220];
  }

  if (attribute === 'delivery_days') {
    return (trip) => {
      const t = normalize(trip.deliveryDays, stats.deliveryDays.min, stats.deliveryDays.max);
      const rgb = interpolatePalette(BLUE_RED, t);
      return [...rgb, 220];
    };
  }

  if (attribute === 'brand') {
    const brandIndex = {};
    stats.brands.forEach((b, i) => {
      brandIndex[b] = i < 10 ? i : -1;
    });
    return (trip) => {
      const idx = brandIndex[trip.brand];
      const rgb = idx >= 0 ? CATEGORY10[idx] : OTHER_COLOR;
      return [...rgb, 220];
    };
  }

  // Continuous attributes: value, weight, cube
  const range = stats[attribute];
  if (!range) return () => [0, 200, 255, 220];

  return (trip) => {
    const t = normalize(trip[attribute], range.min, range.max);
    const rgb = interpolatePalette(VIRIDIS, t);
    return [...rgb, 220];
  };
}

/**
 * Build a size accessor function based on the selected attribute.
 * @param {string} attribute - 'none' | 'value' | 'weight' | 'cube'
 * @param {Object} stats - Data statistics
 * @param {number} scale - Size scale factor (1-10)
 * @param {[number, number]} sizeRange - [min, max] pixel sizes
 * @returns {function(Object): number} Size accessor
 */
export function buildSizeAccessor(attribute, stats, scale, sizeRange) {
  const [minSize, maxSize] = sizeRange;
  const baseSize = minSize + (scale - 1) / 9 * (maxSize - minSize);

  if (attribute === 'none') {
    return () => baseSize;
  }

  const range = stats[attribute];
  if (!range) return () => baseSize;

  return (trip) => {
    const t = normalize(trip[attribute], range.min, range.max);
    return minSize + t * (baseSize - minSize + maxSize * 0.5);
  };
}
