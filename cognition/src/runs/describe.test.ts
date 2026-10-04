import { describe, expect, it } from 'vitest';
import { DEFAULT_PARAMS } from '../params';
import { describeScenario, diffParams } from './describe';

describe('describeScenario', () => {
  it('names greenfield vs brownfield', () => {
    expect(describeScenario(DEFAULT_PARAMS, 2)).toBe('greenfield · in:out 0.2');
    expect(describeScenario({ ...DEFAULT_PARAMS, useFixedNodes: true }, 2)).toBe('brownfield (2 fixed) · in:out 0.2');
    expect(describeScenario({ ...DEFAULT_PARAMS, useFixedNodes: true }, 0)).toBe('greenfield · in:out 0.2');
    expect(describeScenario({ ...DEFAULT_PARAMS, proportionalSourcing: 0.6 }, 0)).toBe('greenfield · in:out 0.2 · sourcing 60%');
  });
});

describe('diffParams', () => {
  it('lists changed parameters and ignores N bounds', () => {
    const next = { ...DEFAULT_PARAMS, inboundRatio: 1, useFixedNodes: true, maxNodes: 20 };
    expect(diffParams(DEFAULT_PARAMS, next)).toEqual(['in:out 0.2 → 1', 'fixed nodes off → on']);
    expect(diffParams(DEFAULT_PARAMS, DEFAULT_PARAMS)).toEqual([]);
    expect(diffParams(DEFAULT_PARAMS, { ...DEFAULT_PARAMS, proportionalSourcing: 1 })).toEqual(['sourcing 0% → 100%']);
  });
});
