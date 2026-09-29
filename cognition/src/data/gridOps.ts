import { COLUMNS, ROW_TYPES, type RawRow } from './types';
import { parseActive } from './validate';

export type TypeFilter = (typeof ROW_TYPES)[number] | 'other';
export type ActiveFilter = 'all' | 'active' | 'inactive';

export interface RowFilter {
  /** Types to show; empty = all. */
  types: Set<TypeFilter>;
  active: ActiveFilter;
}

export const NO_FILTER: RowFilter = { types: new Set(), active: 'all' };

export function typeBucket(row: RawRow): TypeFilter {
  const t = row.type.trim().toLowerCase();
  return (ROW_TYPES as readonly string[]).includes(t) ? (t as TypeFilter) : 'other';
}

/** Raw indices of the rows that pass the filter, in data order. */
export function visibleRows(rows: RawRow[], f: RowFilter): number[] {
  const out: number[] = [];
  rows.forEach((r, i) => {
    if (f.types.size && !f.types.has(typeBucket(r))) return;
    if (f.active !== 'all') {
      const on = parseActive(r.active) !== false;
      if ((f.active === 'active') !== on) return;
    }
    out.push(i);
  });
  return out;
}

/**
 * Copy the first selected row's value down through the selection, one column at a time.
 * `rowsInView` are raw indices of the visible rows, so a filtered fill only touches what's shown.
 * `range` is in view coordinates over the data columns (0 = first data column).
 */
export function fillDown(
  rows: RawRow[],
  rowsInView: number[],
  range: { x: number; y: number; width: number; height: number },
): RawRow[] | null {
  if (range.height < 2) return null;
  const next = rows.slice();
  for (let c = range.x; c < range.x + range.width; c++) {
    const col = COLUMNS[c];
    if (!col) continue;
    const value = rows[rowsInView[range.y]][col];
    for (let y = range.y + 1; y < range.y + range.height; y++) {
      const i = rowsInView[y];
      next[i] = { ...next[i], [col]: value };
    }
  }
  return next;
}

/** Set Active on every listed row. */
export function setActive(rows: RawRow[], indices: number[], active: boolean): RawRow[] {
  const next = rows.slice();
  for (const i of indices) next[i] = { ...next[i], active: active ? 'TRUE' : 'FALSE' };
  return next;
}
