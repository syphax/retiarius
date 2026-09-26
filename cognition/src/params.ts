export type DistanceUnits = 'mi' | 'km';

export interface Params {
  minNodes: number;
  maxNodes: number;
  /** Inbound cost per unit-distance, relative to outbound (= 1). */
  inboundRatio: number;
  /** Service band for "% within X". */
  serviceDistance: number;
  units: DistanceUnits;
  /** Road distance / great-circle distance. */
  circuity: number;
  useFixedNodes: boolean;
}

export const DEFAULT_PARAMS: Params = {
  minNodes: 1,
  maxNodes: 8,
  inboundRatio: 0.4,
  serviceDistance: 300,
  units: 'mi',
  circuity: 1.2,
  useFixedNodes: false,
};
