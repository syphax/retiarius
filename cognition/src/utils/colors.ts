// Shared color vocabulary (matches scimulator/flow_viz).
export type RGB = [number, number, number];

export const ACCENT: RGB = [0, 200, 255];

export const POINT_COLORS: Record<'demand' | 'source' | 'fixed', RGB> = {
  demand: [0, 200, 255],
  source: [255, 127, 14],
  fixed: [227, 119, 194],
};

/**
 * Node colors (each node's allocated demand shares its color). D3 Category10 without its orange
 * and pink, which mean "source" and "fixed node" here, plus gold and teal.
 */
export const NODE_PALETTE: RGB[] = [
  [31, 119, 180],
  [44, 160, 44],
  [214, 39, 40],
  [148, 103, 189],
  [188, 189, 34],
  [23, 190, 207],
  [140, 86, 75],
  [255, 215, 0],
  [127, 127, 127],
  [0, 128, 128],
];

export const cssColor = ([r, g, b]: RGB) => `rgb(${r}, ${g}, ${b})`;
