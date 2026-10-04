export type DistanceUnits = 'mi' | 'km';

export interface Params {
  minNodes: number;
  maxNodes: number;
  /** Inbound cost per unit-distance, relative to outbound (= 1). */
  inboundRatio: number;
  /**
   * Inbound sourcing, 0–1: the share of each node's inbound drawn from all sources in proportion
   * to supply (dedicated sources). The rest comes from the node's nearest source
   * (interchangeable sources). 1 = fully proportional, 0 = nearest source only.
   */
  proportionalSourcing: number;
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
  inboundRatio: 0.2,
  proportionalSourcing: 0,
  serviceDistance: 300,
  units: 'mi',
  circuity: 1.2,
  useFixedNodes: false,
};
