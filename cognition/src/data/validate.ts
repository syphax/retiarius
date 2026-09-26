import type { Gazetteer } from './geo';
import { normalizePostal } from './geo';
import {
  POSTAL_TYPES,
  ROW_TYPES,
  type Point,
  type PostalType,
  type RawRow,
  type RowType,
  type ValidRow,
  type ValidationResult,
} from './types';

function parseNumber(s: string): number | null {
  if (s.trim() === '') return null;
  const n = Number(s.replace(/[,$\s]/g, ''));
  return Number.isFinite(n) ? n : null;
}

function validateRow(row: RawRow, index: number, gaz: Gazetteer): ValidRow | string[] {
  const problems: string[] = [];

  const type = row.type.trim().toLowerCase() as RowType;
  if (!ROW_TYPES.includes(type)) problems.push(`type must be one of ${ROW_TYPES.join(', ')}`);

  let volume = 0;
  if (type !== 'fixed') {
    const v = parseNumber(row.volume);
    if (v === null || v <= 0) problems.push('volume must be a positive number');
    else volume = v;
  }

  // Geo precedence: lat/lon → postal code.
  let geo: Pick<ValidRow, 'lat' | 'lon' | 'geoSource'> | null = null;
  const lat = parseNumber(row.lat);
  const lon = parseNumber(row.lon);
  if (lat !== null || lon !== null) {
    if (lat === null || lon === null) problems.push('lat and lon must both be set');
    else if (Math.abs(lat) > 90 || Math.abs(lon) > 180) problems.push('lat/lon out of range');
    else geo = { lat, lon, geoSource: 'latlon' };
  } else if (row.postal_code.trim() !== '') {
    const ptype = row.postal_code_type.trim().toLowerCase() as PostalType;
    if (!POSTAL_TYPES.includes(ptype)) {
      problems.push(`postal_code_type must be one of ${POSTAL_TYPES.join(', ')}`);
    } else {
      const code = normalizePostal(row.postal_code, ptype);
      const hit = code && gaz[ptype].get(code);
      if (!hit) problems.push(`unknown ${ptype} "${row.postal_code}"`);
      else geo = { lat: hit[0], lon: hit[1], geoSource: ptype };
    }
  } else {
    problems.push('no location: set lat + lon, or postal_code + postal_code_type');
  }

  if (problems.length || !geo) return problems;
  return { index, type, volume, ...geo };
}

/** Merge valid rows of the same type at the same location into one point. */
export function aggregate(rows: ValidRow[]): Point[] {
  const byKey = new Map<string, Point>();
  for (const r of rows) {
    const key = `${r.type}|${r.lat.toFixed(5)}|${r.lon.toFixed(5)}`;
    const p = byKey.get(key);
    if (p) {
      p.volume += r.volume;
      p.rows.push(r.index);
    } else {
      byKey.set(key, { type: r.type, lat: r.lat, lon: r.lon, volume: r.volume, rows: [r.index] });
    }
  }
  return [...byKey.values()];
}

export function validate(rows: RawRow[], gaz: Gazetteer): ValidationResult {
  const valid: ValidRow[] = [];
  const issues = new Map<number, string[]>();
  rows.forEach((row, i) => {
    if (Object.values(row).every((v) => v.trim() === '')) return; // blank grid rows are ignored
    const r = validateRow(row, i, gaz);
    if (Array.isArray(r)) issues.set(i, r);
    else valid.push(r);
  });
  return { valid, issues, points: aggregate(valid) };
}
