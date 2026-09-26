import { describe, expect, it } from 'vitest';
import * as XLSX from 'xlsx';
import type { Gazetteer } from './geo';
import { normalizePostal } from './geo';
import { ImportError, parseText, parseWorkbook, sheetsCsvUrl } from './parse';
import { validate } from './validate';

const gaz: Gazetteer = {
  zip5: new Map([
    ['00601', [18.18, -66.75]],
    ['75201', [32.79, -96.8]],
  ]),
  zip3: new Map([
    ['006', [18.3, -66.83]],
    ['100', [40.78, -73.97]],
  ]),
};

describe('parse', () => {
  it('parses tab-separated paste with messy headers and ignores extra columns', () => {
    const rows = parseText('Type\tVolume\tPostal Code\tpostal-code-type\tnotes\ndemand\t10\t601\tzip5\thello');
    expect(rows).toEqual([
      { type: 'demand', volume: '10', lat: '', lon: '', postal_code: '601', postal_code_type: 'zip5' },
    ]);
  });

  it('parses CSV', () => {
    expect(parseText('type,volume,lat,lon\nsource,5,33.7,-118.2')).toHaveLength(1);
  });

  it('rejects missing required columns', () => {
    expect(() => parseText('type,lat,lon\ndemand,1,2')).toThrow(ImportError);
  });

  it('rejects data with no geo columns', () => {
    expect(() => parseText('type,volume\ndemand,1')).toThrow(/lat \+ lon/);
  });

  it('parses the first sheet of a workbook', () => {
    const ws = XLSX.utils.aoa_to_sheet([
      ['type', 'volume', 'postal_code', 'postal_code_type'],
      ['demand', 3, '00601', 'zip5'],
    ]);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');
    const buf = XLSX.write(wb, { type: 'array', bookType: 'xlsx' }) as ArrayBuffer;
    expect(parseWorkbook(buf)[0]).toMatchObject({ volume: '3', postal_code: '00601' });
  });

  it('builds a Sheets CSV URL that keeps the tab', () => {
    expect(sheetsCsvUrl('https://docs.google.com/spreadsheets/d/abc_123/edit#gid=42')).toBe(
      'https://docs.google.com/spreadsheets/d/abc_123/export?format=csv&gid=42',
    );
    expect(() => sheetsCsvUrl('https://example.com')).toThrow(ImportError);
  });
});

describe('normalizePostal', () => {
  it('pads codes that lost leading zeros and truncates ZIP+4', () => {
    expect(normalizePostal('601', 'zip5')).toBe('00601');
    expect(normalizePostal('75201-1234', 'zip5')).toBe('75201');
    expect(normalizePostal('6', 'zip3')).toBe('006');
    expect(normalizePostal('ABC', 'zip5')).toBeNull();
    expect(normalizePostal('1234', 'zip3')).toBeNull();
  });
});

describe('validate', () => {
  const row = (o: Partial<Record<string, string>>) => ({
    type: '', volume: '', lat: '', lon: '', postal_code: '', postal_code_type: '', ...o,
  });

  it('geocodes, prefers lat/lon, and flags bad rows', () => {
    const r = validate(
      [
        row({ type: 'demand', volume: '1,000', postal_code: '601', postal_code_type: 'zip5' }),
        row({ type: 'Demand', volume: '5', lat: '40', lon: '-100', postal_code: '601', postal_code_type: 'zip5' }),
        row({ type: 'fixed', postal_code: '75201', postal_code_type: 'zip5' }),
        row({ type: 'demand', volume: '1', postal_code: '99999', postal_code_type: 'zip5' }),
        row({ type: 'demand', volume: '-1', lat: '40', lon: '-100' }),
        row({ type: 'warehouse', volume: '1', lat: '40' }),
        row({ type: 'demand', volume: '1', postal_code: '100', postal_code_type: 'fsa' }),
        row({}),
      ],
      gaz,
    );
    expect(r.valid.map((v) => [v.index, v.geoSource])).toEqual([
      [0, 'zip5'],
      [1, 'latlon'],
      [2, 'zip5'],
    ]);
    expect(r.valid[0].volume).toBe(1000);
    expect(r.valid[2].volume).toBe(0);
    expect([...r.issues.keys()]).toEqual([3, 4, 5, 6]); // blank row 7 is ignored
    expect(r.issues.get(5)).toHaveLength(2);
  });

  it('aggregates same-type rows at the same location', () => {
    const r = validate(
      [
        row({ type: 'demand', volume: '2', postal_code: '100', postal_code_type: 'zip3' }),
        row({ type: 'demand', volume: '3', postal_code: '100', postal_code_type: 'zip3' }),
        row({ type: 'source', volume: '3', postal_code: '100', postal_code_type: 'zip3' }),
      ],
      gaz,
    );
    expect(r.points).toHaveLength(2);
    expect(r.points[0]).toMatchObject({ type: 'demand', volume: 5, rows: [0, 1] });
  });
});
