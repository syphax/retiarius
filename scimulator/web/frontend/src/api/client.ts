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

// Registry scenarios (with last_run_at and updated_at)
export interface RegistryScenarioSummary {
  scenario_id: string;
  project_id: string;
  name: string;
  description: string;
  start_date: string | null;
  end_date: string | null;
  currency_code: string;
  time_resolution: string;
  backorder_probability: number | null;
  status: string | null;
  last_run_at: string | null;
  run_wall_clock_seconds: number | null;
  created_at: string;
  updated_at: string;
}

export function listRegistryScenarios(projectId: string): Promise<RegistryScenarioSummary[]> {
  return fetchJson(`${BASE}/registry/projects/${encodeURIComponent(projectId)}/scenarios`);
}

export function getRegistryScenario(projectId: string, scenarioId: string): Promise<RegistryScenarioSummary> {
  return fetchJson(`${BASE}/registry/projects/${encodeURIComponent(projectId)}/scenarios/${encodeURIComponent(scenarioId)}`);
}

// Projects
export interface ProjectSummary {
  project_id: string;
  name: string;
  description: string;
  database: string;
  status: string;
  scenario_count: number;
  created_at: string;
  updated_at: string;
}

export function listProjects(): Promise<ProjectSummary[]> {
  return fetchJson(`${BASE}/registry/projects`);
}

export function updateProject(
  projectId: string,
  fields: { name?: string; description?: string },
): Promise<ProjectSummary> {
  return fetchJson(`${BASE}/registry/projects/${encodeURIComponent(projectId)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(fields),
  });
}

export function cloneProject(
  projectId: string,
  newName: string,
): Promise<ProjectSummary> {
  return fetchJson(`${BASE}/registry/projects/${encodeURIComponent(projectId)}/clone`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ new_name: newName }),
  });
}

export function archiveProject(
  projectId: string,
): Promise<{ archived: string }> {
  return fetchJson(`${BASE}/registry/projects/${encodeURIComponent(projectId)}/archive`, {
    method: 'POST',
  });
}

// Scenario actions
export function rerunScenario(dbName: string, scenarioId: string): Promise<{ scenario_id: string; status: string }> {
  return fetchJson(`${BASE}/scenarios/${encodeURIComponent(scenarioId)}/rerun?db=${encodeURIComponent(dbName)}`, {
    method: 'POST',
  });
}

export function duplicateScenario(
  projectId: string,
  scenarioId: string,
): Promise<RegistryScenarioSummary> {
  return fetchJson(
    `${BASE}/registry/projects/${encodeURIComponent(projectId)}/scenarios/${encodeURIComponent(scenarioId)}/clone`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    },
  );
}

export function updateRegistryScenario(
  projectId: string,
  scenarioId: string,
  fields: { name?: string; description?: string },
): Promise<RegistryScenarioSummary> {
  return fetchJson(
    `${BASE}/registry/projects/${encodeURIComponent(projectId)}/scenarios/${encodeURIComponent(scenarioId)}`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(fields),
    },
  );
}

export function archiveScenario(
  projectId: string,
  scenarioId: string,
): Promise<{ archived: string }> {
  return fetchJson(
    `${BASE}/registry/projects/${encodeURIComponent(projectId)}/scenarios/${encodeURIComponent(scenarioId)}/archive`,
    { method: 'POST' },
  );
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
  value_shipped?: number;
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
  metric: string;
  y_label: string;
}

export function getInventoryTimeseries(
  dbName: string,
  scenarioId: string,
  groupBy: string = 'node',
  metric: string = 'units',
  nodeId?: string,
  productId?: string,
): Promise<InventoryTimeseries> {
  const params = new URLSearchParams({ db: dbName, group_by: groupBy, metric });
  if (nodeId) params.set('node_id', nodeId);
  if (productId) params.set('product_id', productId);
  return fetchJson(`${BASE}/results/${encodeURIComponent(scenarioId)}/inventory?${params}`);
}

// Fulfillment detail
export interface FulfillmentByNode {
  dist_node_id: string;
  fulfilled_events: number;
  fulfilled_units: number;
  fulfillment_cost: number;
  value_shipped: number;
}

export interface FulfillmentByProduct {
  product_id: string;
  demand_units: number;
  fulfilled_units: number;
  lost_units: number;
  backorder_units: number;
  value_shipped: number;
  fill_rate_pct: number;
}

export interface FulfillmentDayBucket {
  day: number;
  label: string;
  qty: number;
  value: number;
}

export interface FulfillmentByDays {
  buckets: FulfillmentDayBucket[];
  avg_days: number;
  median_days: number;
}

export interface FulfillmentDetail {
  stats: FulfillmentStats;
  by_days: FulfillmentByDays;
  by_node: FulfillmentByNode[];
  by_product: FulfillmentByProduct[];
}

export function getFulfillmentDetail(dbName: string, scenarioId: string): Promise<FulfillmentDetail> {
  return fetchJson(`${BASE}/results/${encodeURIComponent(scenarioId)}/fulfillment?db=${encodeURIComponent(dbName)}`);
}

export function fulfillmentCsvUrl(dbName: string, scenarioId: string, view: 'by_node' | 'by_product'): string {
  return `${BASE}/results/${encodeURIComponent(scenarioId)}/fulfillment/csv?db=${encodeURIComponent(dbName)}&view=${view}`;
}

// Inventory KPIs
export interface InventoryKpis {
  avg_inventory_units: number;
  avg_inventory_value: number;
  total_fulfilled_units: number;
  months_of_supply: number;
  inventory_turns: number;
  num_months: number;
}

export interface AvgInventoryByNode {
  dist_node_id: string;
  avg_parts_in_stock: number;
  avg_units_in_stock: number;
  avg_value_in_stock: number;
}

export interface AvgInventoryByProduct {
  product_id: string;
  avg_units_oh: number;
  avg_value: number;
  pct_of_total: number;
  avg_fcs_with_oh: number;
}

export interface InventoryKpiData {
  kpis: InventoryKpis | null;
  by_node: AvgInventoryByNode[];
  by_product: AvgInventoryByProduct[];
}

export function getInventoryKpis(dbName: string, scenarioId: string): Promise<InventoryKpiData> {
  return fetchJson(`${BASE}/results/${encodeURIComponent(scenarioId)}/inventory/kpis?db=${encodeURIComponent(dbName)}`);
}

// Node summary
export interface DistributionNodeSummary {
  node_id: string;
  name: string;
  latitude: number | null;
  longitude: number | null;
  storage_capacity: number | null;
  storage_capacity_uom: string | null;
  fixed_cost_rate: number | null;
  fixed_cost_basis: string | null;
  variable_cost_rate: number | null;
  variable_cost_basis: string | null;
  fulfilled_units: number;
  fulfillment_cost: number;
  fixed_cost_total: number;
  overage_cost: number;
  final_inventory: number;
}

export interface SupplyNodeSummary {
  node_id: string;
  name: string;
  latitude: number | null;
  longitude: number | null;
  supplier_id: string;
  supplier_name: string;
  lead_time_days: number | null;
}

export interface DemandNodeSummary {
  node_id: string;
  name: string;
  latitude: number | null;
  longitude: number | null;
  demand_units: number;
}

export interface NodeSummary {
  distribution: DistributionNodeSummary[];
  supply: SupplyNodeSummary[];
  demand: DemandNodeSummary[];
}

export function getNodeSummary(dbName: string, scenarioId: string): Promise<NodeSummary> {
  return fetchJson(`${BASE}/results/${encodeURIComponent(scenarioId)}/nodes?db=${encodeURIComponent(dbName)}`);
}

// Transportation summary
export interface TransportationEdge {
  edge_id: string;
  origin_node_id: string;
  origin_node_type: string;
  dest_node_id: string;
  dest_node_type: string;
  transport_type: string;
  mean_transit_time: number | null;
  distance: number | null;
  distance_uom: string | null;
  shipments: number;
  total_qty: number;
  total_cost: number;
}

export function getTransportationSummary(dbName: string, scenarioId: string): Promise<TransportationEdge[]> {
  return fetchJson(`${BASE}/results/${encodeURIComponent(scenarioId)}/transportation?db=${encodeURIComponent(dbName)}`);
}

// Cost detail
export interface CostDetail {
  total_cost: number;
  by_event_type: { event_type: string; cost: number }[];
  by_node: { node_id: string; total_cost: number; fixed_cost: number; fulfillment_cost: number; overage_cost: number }[];
  by_product: { product_id: string; cost: number }[];
}

export function getCostDetail(dbName: string, scenarioId: string): Promise<CostDetail> {
  return fetchJson(`${BASE}/results/${encodeURIComponent(scenarioId)}/costs?db=${encodeURIComponent(dbName)}`);
}

// Events (paginated)
export interface EventPage {
  total: number;
  limit: number;
  offset: number;
  events: Record<string, unknown>[];
}

export interface EventFilterOptions {
  event_types: string[];
  products: string[];
  origin_nodes: string[];
  dest_nodes: string[];
}

export function getEventFilterOptions(
  dbName: string,
  scenarioId: string,
): Promise<EventFilterOptions> {
  return fetchJson(`${BASE}/results/${encodeURIComponent(scenarioId)}/events/filters?db=${encodeURIComponent(dbName)}`);
}

export function getEvents(
  dbName: string,
  scenarioId: string,
  params: Record<string, string | number | string[]> = {},
): Promise<EventPage> {
  const search = new URLSearchParams({ db: dbName });
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === '') continue;
    if (Array.isArray(v)) {
      for (const item of v) search.append(k, item);
    } else {
      search.set(k, String(v));
    }
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

// Org config
export interface OrgTerminology {
  edge: string;
  edges: string;
  node: string;
  nodes: string;
}

export interface OrgConfig {
  terminology: OrgTerminology;
}

const DEFAULT_ORG_CONFIG: OrgConfig = {
  terminology: { edge: 'Edge', edges: 'Edges', node: 'Node', nodes: 'Nodes' },
};

let _orgConfigCache: OrgConfig | null = null;

export async function getOrgConfig(): Promise<OrgConfig> {
  if (_orgConfigCache) return _orgConfigCache;
  try {
    const raw = await fetchJson<Record<string, unknown>>(`${BASE}/org-config`);
    _orgConfigCache = {
      terminology: {
        edge: String((raw.terminology as Record<string, string>)?.edge || 'Edge'),
        edges: String((raw.terminology as Record<string, string>)?.edges || 'Edges'),
        node: String((raw.terminology as Record<string, string>)?.node || 'Node'),
        nodes: String((raw.terminology as Record<string, string>)?.nodes || 'Nodes'),
      },
    };
  } catch {
    _orgConfigCache = DEFAULT_ORG_CONFIG;
  }
  return _orgConfigCache;
}
