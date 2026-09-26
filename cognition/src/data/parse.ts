import Papa from 'papaparse';
import * as XLSX from 'xlsx';
import { COLUMNS, REQUIRED_COLUMNS, emptyRow, type Column, type RawRow } from './types';

export class ImportError extends Error {}

/** "Postal Code" / "postal-code" / " POSTAL_CODE " → "postal_code". */
export function normalizeHeader(h: string): string {
  return h.trim().toLowerCase().replace(/[\s-]+/g, '_');
}

/** Turn a header row + data rows (arrays of cells) into RawRows. Unknown columns are ignored. */
export function rowsFromTable(table: unknown[][]): RawRow[] {
  const nonEmpty = table.filter((r) => r.some((c) => String(c ?? '').trim() !== ''));
  if (nonEmpty.length === 0) throw new ImportError('No data found.');
  const header = nonEmpty[0].map((h) => normalizeHeader(String(h ?? '')));
  const missing = REQUIRED_COLUMNS.filter((c) => !header.includes(c));
  if (missing.length) {
    throw new ImportError(
      `Missing required column(s): ${missing.join(', ')}. Expected columns: ${COLUMNS.join(', ')}.`,
    );
  }
  const hasGeo = header.includes('lat') && header.includes('lon');
  const hasPostal = header.includes('postal_code') && header.includes('postal_code_type');
  if (!hasGeo && !hasPostal) {
    throw new ImportError('Need either lat + lon columns, or postal_code + postal_code_type columns.');
  }
  const colIndex = new Map<Column, number>();
  for (const c of COLUMNS) {
    const i = header.indexOf(c);
    if (i >= 0) colIndex.set(c, i);
  }
  return nonEmpty.slice(1).map((cells) => {
    const row = emptyRow();
    for (const [c, i] of colIndex) row[c] = String(cells[i] ?? '').trim();
    return row;
  });
}

/** Parse pasted text or a CSV file. Delimiter is auto-detected (tab from Sheets/Excel paste, comma from CSV). */
export function parseText(text: string): RawRow[] {
  const result = Papa.parse<string[]>(text.trim(), { skipEmptyLines: true });
  return rowsFromTable(result.data);
}

/** Parse the first sheet of an Excel workbook. Uses formatted text so ZIPs like 00601 keep their zeros where the sheet formats them. */
export function parseWorkbook(data: ArrayBuffer): RawRow[] {
  const wb = XLSX.read(data, { type: 'array' });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const table = XLSX.utils.sheet_to_json<unknown[]>(sheet, { header: 1, raw: false, defval: '' });
  return rowsFromTable(table);
}

export async function parseFile(file: File): Promise<RawRow[]> {
  if (/\.(xlsx|xlsm|xls)$/i.test(file.name)) return parseWorkbook(await file.arrayBuffer());
  return parseText(await file.text());
}

/** Build the CSV export URL for a Google Sheets link. Keeps the tab (gid) if the link has one. */
export function sheetsCsvUrl(link: string): string {
  const id = link.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/)?.[1];
  if (!id) throw new ImportError('That does not look like a Google Sheets link.');
  const gid = link.match(/[#&?]gid=(\d+)/)?.[1] ?? '0';
  return `https://docs.google.com/spreadsheets/d/${id}/export?format=csv&gid=${gid}`;
}

export async function fetchSheet(link: string): Promise<RawRow[]> {
  let res: Response;
  try {
    res = await fetch(sheetsCsvUrl(link));
  } catch {
    throw new ImportError('Could not fetch the sheet. Is it shared as "Anyone with the link can view"?');
  }
  if (!res.ok) throw new ImportError(`Google Sheets returned ${res.status}. Is the sheet shared publicly?`);
  const text = await res.text();
  if (text.trimStart().startsWith('<')) {
    throw new ImportError('Got a sign-in page instead of data. Share the sheet as "Anyone with the link can view".');
  }
  return parseText(text);
}
