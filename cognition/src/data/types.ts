// Input data contract. The grid holds raw strings; validation turns them into typed rows.

export const COLUMNS = ['type', 'volume', 'lat', 'lon', 'postal_code', 'postal_code_type', 'active'] as const;
export type Column = (typeof COLUMNS)[number];

/** Display names. Imports match headers case-insensitively, with spaces or dashes as underscores. */
export const COLUMN_TITLES: Record<Column, string> = {
  type: 'Type',
  volume: 'Volume',
  lat: 'Lat',
  lon: 'Lon',
  postal_code: 'Postal Code',
  postal_code_type: 'Postal Code Type',
  active: 'Active',
};

/** Columns that must be present in any upload. Geo columns are checked per row. */
export const REQUIRED_COLUMNS: Column[] = ['type', 'volume'];

export type RawRow = Record<Column, string>;

export const ROW_TYPES = ['demand', 'source', 'fixed'] as const;
export type RowType = (typeof ROW_TYPES)[number];

export const POSTAL_TYPES = ['zip5', 'zip3'] as const;
export type PostalType = (typeof POSTAL_TYPES)[number];

export type GeoSource = 'latlon' | PostalType;

export interface ValidRow {
  /** Index into the raw row array. */
  index: number;
  type: RowType;
  /** 0 for fixed nodes. */
  volume: number;
  /** Inactive rows are validated (and geocoded) but excluded from runs. */
  active: boolean;
  lat: number;
  lon: number;
  geoSource: GeoSource;
}

/** A demand/source/fixed point after aggregating rows that share a location. */
export interface Point {
  type: RowType;
  lat: number;
  lon: number;
  volume: number;
  /** Raw row indices that were aggregated into this point. */
  rows: number[];
}

export interface ValidationResult {
  valid: ValidRow[];
  /** Raw row index → problems. Rows with issues are excluded from runs. */
  issues: Map<number, string[]>;
  /** Active rows with issues: the ones that matter for a run. */
  flagged: number;
  /** Raw row indices of rows switched off by the Active column. */
  inactive: Set<number>;
  /** Active valid rows, aggregated. What a run uses. */
  points: Point[];
}

export const EMPTY_VALIDATION: ValidationResult = {
  valid: [],
  issues: new Map(),
  flagged: 0,
  inactive: new Set(),
  points: [],
};

export const emptyRow = (): RawRow =>
  Object.fromEntries(COLUMNS.map((c) => [c, ''])) as RawRow;
