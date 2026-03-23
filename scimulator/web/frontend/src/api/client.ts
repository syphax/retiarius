/**
 * API client for the SCimulator backend.
 */

const BASE = '/api';

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status}: ${body}`);
  }
  return res.json();
}

// Database management
export interface DatabaseInfo {
  name: string;
  size_mb: number;
  path: string;
}

export interface DatabaseInspection {
  database: string;
  tables: Record<string, number>;
  scenarios: { scenario_id: string; name: string; start_date: string; end_date: string }[];
}

export function listDatabases(): Promise<DatabaseInfo[]> {
  return fetchJson(`${BASE}/databases`);
}

export function inspectDatabase(dbName: string): Promise<DatabaseInspection> {
  return fetchJson(`${BASE}/databases/${encodeURIComponent(dbName)}/inspect`);
}

// Scenarios
export interface ScenarioSummary {
  scenario_id: string;
  name: string;
  description: string | null;
  start_date: string;
  end_date: string;
  currency_code: string;
  time_resolution: string;
  backorder_probability: number | null;
  status: string | null;
  total_steps: number | null;
  wall_clock_seconds: number | null;
  run_started_at: string | null;
  run_completed_at: string | null;
}

export function listScenarios(dbName: string): Promise<ScenarioSummary[]> {
  return fetchJson(`${BASE}/scenarios?db=${encodeURIComponent(dbName)}`);
}

export function getScenario(dbName: string, scenarioId: string): Promise<Record<string, unknown>> {
  return fetchJson(`${BASE}/scenarios/${encodeURIComponent(scenarioId)}?db=${encodeURIComponent(dbName)}`);
}

// Run simulation
export async function runScenario(
  scenarioFile: File,
  demandFile?: File,
  dbName?: string,
  replace?: boolean,
  forkId?: string,
): Promise<{ scenario_id: string; database: string; status: string }> {
  const form = new FormData();
  form.append('scenario_file', scenarioFile);
  if (demandFile) form.append('demand_file', demandFile);
  if (dbName) form.append('db_name', dbName);
  if (replace) form.append('replace', 'true');
  if (forkId) form.append('fork_id', forkId);

  return fetchJson(`${BASE}/scenarios/run`, { method: 'POST', body: form });
}

// Results
export interface EventSummaryRow {
  event_type: string;
  count: number;
  total_qty: number | null;
  total_cost: number | null;
}

export interface FulfillmentStats {
  demand_events: number;
  demand_units: number;
  fulfilled_events: number;
  fulfilled_units: number;
  fill_rate_pct: number;
  lost_sale_events: number;
  lost_sale_units: number;
  backorder_events: number;
  backorder_units: number;
}

export interface CostSummary {
  total_cost: number;
  by_event_type: { event_type: string; cost: number }[];
}

export interface InventorySummary {
  snapshot_date: string;
  states: { state: string; quantity: number; nodes: number; products: number }[];
}

export interface ResultsSummary {
  metadata: Record<string, unknown>;
  events: EventSummaryRow[];
  fulfillment: FulfillmentStats;
  costs: CostSummary;
  inventory: InventorySummary | null;
}

export function getResultsSummary(dbName: string, scenarioId: string): Promise<ResultsSummary> {
  return fetchJson(`${BASE}/results/${encodeURIComponent(scenarioId)}/summary?db=${encodeURIComponent(dbName)}`);
}

export interface InventoryTimeseries {
  dates: string[];
  series: Record<string, number[]>;
  group_by: string;
}

export function getInventoryTimeseries(
  dbName: string,
  scenarioId: string,
  groupBy: string = 'node',
  nodeId?: string,
  productId?: string,
): Promise<InventoryTimeseries> {
  const params = new URLSearchParams({ db: dbName, group_by: groupBy });
  if (nodeId) params.set('node_id', nodeId);
  if (productId) params.set('product_id', productId);
  return fetchJson(`${BASE}/results/${encodeURIComponent(scenarioId)}/inventory?${params}`);
}

// Events (paginated)
export interface EventPage {
  total: number;
  limit: number;
  offset: number;
  events: Record<string, unknown>[];
}

export function getEvents(
  dbName: string,
  scenarioId: string,
  params: Record<string, string | number> = {},
): Promise<EventPage> {
  const search = new URLSearchParams({ db: dbName });
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== '') search.set(k, String(v));
  }
  return fetchJson(`${BASE}/results/${encodeURIComponent(scenarioId)}/events?${search}`);
}

// Network
export interface NetworkNode {
  id: string;
  name: string;
  latitude: number | null;
  longitude: number | null;
  node_type: string;
  supplier_id: string | null;
}

export function getNetworkNodes(dbName: string): Promise<NetworkNode[]> {
  return fetchJson(`${BASE}/network/${encodeURIComponent(dbName)}/nodes`);
}

export interface NetworkEdge {
  edge_id: string;
  origin_node_id: string;
  origin_node_type: string;
  dest_node_id: string;
  dest_node_type: string;
  transport_type: string;
  mean_transit_time: number | null;
  cost_fixed: number | null;
  cost_variable: number | null;
  cost_variable_basis: string | null;
}

export function getNetworkEdges(dbName: string): Promise<NetworkEdge[]> {
  return fetchJson(`${BASE}/network/${encodeURIComponent(dbName)}/edges`);
}

// Export URLs (for download links)
export function eventsExportUrl(dbName: string, scenarioId: string): string {
  return `${BASE}/export/${encodeURIComponent(scenarioId)}/events.csv?db=${encodeURIComponent(dbName)}`;
}

export function snapshotsExportUrl(dbName: string, scenarioId: string): string {
  return `${BASE}/export/${encodeURIComponent(scenarioId)}/snapshots.csv?db=${encodeURIComponent(dbName)}`;
}

export function databaseExportUrl(dbName: string): string {
  return `${BASE}/export/database/${encodeURIComponent(dbName)}`;
}
