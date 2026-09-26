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
import { COLUMNS, type RawRow, type ValidationResult } from '../data/types';

interface Props {
  rows: RawRow[];
  onRowsChange: (rows: RawRow[]) => void;
  validation: ValidationResult;
  geoReady: boolean;
}

const ISSUE_COL = COLUMNS.length;

const GRID_COLUMNS: GridColumn[] = [
  ...COLUMNS.map((c) => ({ id: c, title: c, width: c === 'postal_code_type' ? 150 : 120 })),
  { id: 'issues', title: 'issues', width: 420 },
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
const ISSUE_TEXT: Partial<Theme> = { textDark: '#ff8a8a' };

/** Copy the top cell of each selected column down through the selected rows. */
function fillDown(rows: RawRow[], sel: GridSelection): RawRow[] | null {
  const r = sel.current?.range;
  if (!r || r.height < 2) return null;
  const next = rows.slice();
  for (let c = r.x; c < r.x + r.width; c++) {
    if (c >= COLUMNS.length) continue;
    const col = COLUMNS[c];
    const value = rows[r.y][col];
    for (let y = r.y + 1; y < r.y + r.height; y++) next[y] = { ...next[y], [col]: value };
  }
  return next;
}

export default function DataTab({ rows, onRowsChange, validation, geoReady }: Props) {
  const [selection, setSelection] = useState<GridSelection>({
    columns: CompactSelection.empty(),
    rows: CompactSelection.empty(),
  });

  const getCellContent = useCallback(
    ([col, row]: Item): GridCell => {
      if (col === ISSUE_COL) {
        const text = validation.issues.get(row)?.join('; ') ?? '';
        return {
          kind: GridCellKind.Text,
          data: text,
          displayData: text,
          allowOverlay: false,
          readonly: true,
          themeOverride: ISSUE_TEXT,
        };
      }
      const value = rows[row]?.[COLUMNS[col]] ?? '';
      return { kind: GridCellKind.Text, data: value, displayData: value, allowOverlay: true };
    },
    [rows, validation],
  );

  const onCellsEdited = useCallback(
    (edits: readonly EditListItem[]) => {
      const next = rows.slice();
      for (const { location: [col, row], value } of edits) {
        if (col >= COLUMNS.length || value.kind !== GridCellKind.Text || !next[row]) continue;
        next[row] = { ...next[row], [COLUMNS[col]]: value.data };
      }
      onRowsChange(next);
      return true;
    },
    [rows, onRowsChange],
  );

  const getRowThemeOverride = useCallback(
    (row: number) => (validation.issues.has(row) ? FLAGGED_ROW : undefined),
    [validation],
  );

  const stats = useMemo(() => {
    const nonBlank = rows.filter((r) => Object.values(r).some((v) => v.trim() !== '')).length;
    return { nonBlank, valid: validation.valid.length, flagged: validation.issues.size, points: validation.points.length };
  }, [rows, validation]);

  const canFill = (selection.current?.range.height ?? 0) > 1;

  if (rows.length === 0) {
    return <div className="placeholder">No data loaded. Use Import in the top bar.</div>;
  }

  return (
    <div className="data-tab">
      <div className="data-toolbar">
        <button
          disabled={!canFill}
          onClick={() => {
            const next = fillDown(rows, selection);
            if (next) onRowsChange(next);
          }}
          title="Copy the top selected cell down through the selection (Ctrl/⌘+D)"
        >
          Fill down
        </button>
        <span className="data-stats">
          {stats.nonBlank.toLocaleString()} rows · {stats.valid.toLocaleString()} valid
          {stats.flagged > 0 && <span className="warn"> · {stats.flagged.toLocaleString()} flagged (excluded)</span>} ·{' '}
          {stats.points.toLocaleString()} locations after aggregation
          {!geoReady && ' · loading postal codes…'}
        </span>
      </div>
      <div className="grid-container">
        <DataEditor
          columns={GRID_COLUMNS}
          rows={rows.length}
          getCellContent={getCellContent}
          getCellsForSelection
          onCellsEdited={onCellsEdited}
          onPaste
          fillHandle
          keybindings={{ downFill: true }}
          gridSelection={selection}
          onGridSelectionChange={setSelection}
          getRowThemeOverride={getRowThemeOverride}
          rowMarkers="number"
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
