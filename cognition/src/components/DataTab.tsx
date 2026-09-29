import { useCallback, useMemo, useState } from 'react';
import {
  CompactSelection,
  DataEditor,
  GridCellKind,
  type EditListItem,
  type GridCell,
  type GridColumn,
  type GridSelection,
  type Item,
  type Theme,
} from '@glideapps/glide-data-grid';
import { COLUMNS, COLUMN_TITLES, type RawRow, type ValidRow, type ValidationResult } from '../data/types';
import { parseActive } from '../data/validate';
import { NO_FILTER, fillDown, setActive, visibleRows, type ActiveFilter, type RowFilter, type TypeFilter } from '../data/gridOps';

interface Props {
  rows: RawRow[];
  onRowsChange: (rows: RawRow[]) => void;
  validation: ValidationResult;
  geoReady: boolean;
}

// Grid columns: Row #, the data columns, Issues.
const ROW_COL = 0;
const FIRST_DATA_COL = 1;
const ISSUE_COL = FIRST_DATA_COL + COLUMNS.length;

const WIDTHS: Partial<Record<(typeof COLUMNS)[number], number>> = { postal_code_type: 150, active: 70, postal_code: 120 };

const GRID_COLUMNS: GridColumn[] = [
  { id: 'row', title: 'Row', width: 60 },
  ...COLUMNS.map((c) => ({ id: c, title: COLUMN_TITLES[c], width: WIDTHS[c] ?? 110 })),
  { id: 'issues', title: 'Issues', width: 420 },
];

const DARK_THEME: Partial<Theme> = {
  accentColor: '#00c8ff',
  accentLight: 'rgba(0, 200, 255, 0.15)',
  textDark: '#e0e0e0',
  textMedium: '#a0a0a8',
  textLight: '#707078',
  textHeader: '#c0c0c8',
  bgCell: '#14141c',
  bgCellMedium: '#1a1a24',
  bgHeader: '#1c1c28',
  bgHeaderHasFocus: '#24243a',
  bgHeaderHovered: '#24243a',
  bgBubble: '#24243a',
  borderColor: 'rgba(255, 255, 255, 0.08)',
  horizontalBorderColor: 'rgba(255, 255, 255, 0.05)',
  linkColor: '#00c8ff',
  fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
};

const FLAGGED_ROW: Partial<Theme> = { bgCell: '#3a1a1f', bgCellMedium: '#44202a' };
const INACTIVE_ROW: Partial<Theme> = { textDark: '#5c5c66', textLight: '#4a4a52' };
const ISSUE_TEXT: Partial<Theme> = { textDark: '#ff8a8a' };
const MUTED_TEXT: Partial<Theme> = { textDark: '#707078' };
/** Glide fills checked boxes with textMedium; use the accent so "active" reads at a glance. */
const CHECKED: Partial<Theme> = { textMedium: '#00c8ff' };
/** Coordinates filled in from a postal code rather than typed. */
const GEOCODED: Partial<Theme> = { textDark: '#8a8a94', baseFontStyle: 'italic 13px' };

const TYPE_CHIPS: { key: TypeFilter; label: string }[] = [
  { key: 'demand', label: 'Demand' },
  { key: 'source', label: 'Source' },
  { key: 'fixed', label: 'Fixed' },
  { key: 'other', label: 'Other' },
];

const EMPTY_SELECTION: GridSelection = { columns: CompactSelection.empty(), rows: CompactSelection.empty() };

export default function DataTab({ rows, onRowsChange, validation, geoReady }: Props) {
  const [selection, setSelection] = useState<GridSelection>(EMPTY_SELECTION);
  const [filter, setFilter] = useState<RowFilter>(NO_FILTER);

  const view = useMemo(() => visibleRows(rows, filter), [rows, filter]);
  const validByIndex = useMemo(() => new Map<number, ValidRow>(validation.valid.map((v) => [v.index, v])), [validation]);

  const updateFilter = (f: RowFilter) => {
    setFilter(f);
    setSelection(EMPTY_SELECTION); // view rows shift, so an old selection would point at other rows
  };
  const toggleType = (t: TypeFilter) => {
    const types = new Set(filter.types);
    if (types.has(t)) types.delete(t);
    else types.add(t);
    updateFilter({ ...filter, types });
  };

  const getCellContent = useCallback(
    ([col, viewRow]: Item): GridCell => {
      const i = view[viewRow];
      const row = rows[i];
      const inactive = validation.inactive.has(i);
      if (col === ROW_COL) {
        const text = String(i + 1);
        return { kind: GridCellKind.Text, data: text, displayData: text, allowOverlay: false, readonly: true, themeOverride: MUTED_TEXT };
      }
      if (col === ISSUE_COL) {
        const text = validation.issues.get(i)?.join('; ') ?? '';
        return {
          kind: GridCellKind.Text,
          data: text,
          displayData: text,
          allowOverlay: false,
          readonly: true,
          themeOverride: inactive ? undefined : ISSUE_TEXT,
        };
      }
      const key = COLUMNS[col - FIRST_DATA_COL];
      if (key === 'active') {
        const on = parseActive(row.active);
        return {
          kind: GridCellKind.Boolean,
          data: on,
          allowOverlay: false,
          readonly: false,
          themeOverride: on ? CHECKED : undefined,
        };
      }
      const value = row[key];
      // Show geocoded coordinates where the user left lat/lon blank. Editing starts from the raw (blank) value.
      if ((key === 'lat' || key === 'lon') && value.trim() === '') {
        const v = validByIndex.get(i);
        if (v && v.geoSource !== 'latlon') {
          const shown = v[key].toFixed(5);
          return { kind: GridCellKind.Text, data: '', displayData: shown, allowOverlay: true, themeOverride: GEOCODED };
        }
      }
      return { kind: GridCellKind.Text, data: value, displayData: value, allowOverlay: true };
    },
    [rows, view, validation, validByIndex],
  );

  const onCellsEdited = useCallback(
    (edits: readonly EditListItem[]) => {
      const next = rows.slice();
      for (const { location: [col, viewRow], value } of edits) {
        const key = COLUMNS[col - FIRST_DATA_COL];
        const i = view[viewRow];
        if (!key || i === undefined) continue;
        if (value.kind === GridCellKind.Boolean) next[i] = { ...next[i], [key]: value.data ? 'TRUE' : 'FALSE' };
        else if (value.kind === GridCellKind.Text) next[i] = { ...next[i], [key]: value.data };
      }
      onRowsChange(next);
      return true;
    },
    [rows, view, onRowsChange],
  );

  const getRowThemeOverride = useCallback(
    (viewRow: number) => {
      const i = view[viewRow];
      if (validation.inactive.has(i)) return INACTIVE_ROW;
      return validation.issues.has(i) ? FLAGGED_ROW : undefined;
    },
    [view, validation],
  );

  const doFillDown = () => {
    const r = selection.current?.range;
    if (!r) return;
    // Clip the selection to data columns and shift to data-column coordinates.
    const x0 = Math.max(r.x, FIRST_DATA_COL);
    const x1 = Math.min(r.x + r.width, ISSUE_COL);
    if (x1 <= x0) return;
    const next = fillDown(rows, view, { x: x0 - FIRST_DATA_COL, y: r.y, width: x1 - x0, height: r.height });
    if (next) onRowsChange(next);
  };

  const stats = useMemo(() => {
    const nonBlank = rows.filter((r) => Object.values(r).some((v) => v.trim() !== '')).length;
    return { nonBlank, points: validation.points.length };
  }, [rows, validation]);

  const canFill = (selection.current?.range.height ?? 0) > 1;
  const filtered = filter.types.size > 0 || filter.active !== 'all';

  if (rows.length === 0) {
    return <div className="placeholder">No data loaded. Use Import in the top bar.</div>;
  }

  return (
    <div className="data-tab">
      <div className="data-toolbar">
        <button
          disabled={!canFill}
          onClick={doFillDown}
          title="Copy the top selected cell down through the selection (Ctrl/⌘+D). With a filter on, only shown rows change."
        >
          Fill down
        </button>
        <span className="toolbar-sep" />
        <span className="label">Type</span>
        <div className="seg">
          {TYPE_CHIPS.map(({ key, label }) => (
            <button key={key} className={filter.types.has(key) ? 'chip active' : 'chip'} onClick={() => toggleType(key)}>
              {label}
            </button>
          ))}
        </div>
        <span className="label">Active</span>
        <div className="seg">
          {(['all', 'active', 'inactive'] as ActiveFilter[]).map((a) => (
            <button
              key={a}
              className={filter.active === a ? 'chip active' : 'chip'}
              onClick={() => updateFilter({ ...filter, active: a })}
            >
              {a[0].toUpperCase() + a.slice(1)}
            </button>
          ))}
        </div>
        {filtered && (
          <button className="link" onClick={() => updateFilter(NO_FILTER)}>
            Clear filters
          </button>
        )}
        <span className="toolbar-sep" />
        <button onClick={() => onRowsChange(setActive(rows, view, true))} title="Check Active on every row shown">
          Activate {filtered ? 'shown' : 'all'}
        </button>
        <button onClick={() => onRowsChange(setActive(rows, view, false))} title="Uncheck Active on every row shown">
          Deactivate {filtered ? 'shown' : 'all'}
        </button>
      </div>
      <div className="data-stats-bar">
        {filtered && (
          <>
            Showing {view.length.toLocaleString()} of {stats.nonBlank.toLocaleString()} rows ·{' '}
          </>
        )}
        {!filtered && <>{stats.nonBlank.toLocaleString()} rows · </>}
        {validation.inactive.size > 0 && <>{validation.inactive.size.toLocaleString()} inactive · </>}
        {validation.flagged > 0 && <span className="warn">{validation.flagged.toLocaleString()} flagged (excluded) · </span>}
        {stats.points.toLocaleString()} active locations after aggregation
        {!geoReady && ' · loading postal codes…'}
        <span className="muted"> · gray italic lat/lon = geocoded from postal code</span>
      </div>
      <div className="grid-container">
        <DataEditor
          columns={GRID_COLUMNS}
          rows={view.length}
          getCellContent={getCellContent}
          getCellsForSelection
          onCellsEdited={onCellsEdited}
          onPaste
          fillHandle
          keybindings={{ downFill: true }}
          gridSelection={selection}
          onGridSelectionChange={setSelection}
          getRowThemeOverride={getRowThemeOverride}
          rowMarkers="none"
          freezeColumns={1}
          smoothScrollX
          smoothScrollY
          theme={DARK_THEME}
          width="100%"
          height="100%"
        />
      </div>
    </div>
  );
}
