import Papa from 'papaparse';
import type { PostalType } from './types';

export type Gazetteer = Record<PostalType, Map<string, [number, number]>>;

async function loadTable(url: string): Promise<Map<string, [number, number]>> {
  const text = await (await fetch(url)).text();
  const rows = Papa.parse<{ code: string; lat: string; lon: string }>(text, {
    header: true,
    skipEmptyLines: true,
  }).data;
  return new Map(rows.map((r) => [r.code, [Number(r.lat), Number(r.lon)]]));
}

let cache: Promise<Gazetteer> | null = null;

/** Load bundled postal-code centroids (once). */
export function loadGazetteer(): Promise<Gazetteer> {
  cache ??= Promise.all([loadTable('/geo/zip5.csv'), loadTable('/geo/zip3.csv')]).then(
    ([zip5, zip3]) => ({ zip5, zip3 }),
  );
  return cache;
}

/**
 * Canonicalize a postal code for lookup. Excel drops leading zeros, so US codes are left-padded.
 * Returns null if the code can't be a valid code of that type.
 */
export function normalizePostal(code: string, type: PostalType): string | null {
  const c = code.trim();
  if (type === 'zip5') {
    const m = c.match(/^(\d{1,5})(?:-\d{4})?$/); // ZIP+4 is truncated
    return m ? m[1].padStart(5, '0') : null;
  }
  const m = c.match(/^\d{1,3}$/);
  return m ? c.padStart(3, '0') : null;
}
