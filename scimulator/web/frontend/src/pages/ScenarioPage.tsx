import { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react'
import { useParams, useSearchParams, Link } from 'react-router-dom'
import {
  getResultsSummary,
  getEvents as getEventsApi,
  getEventFilterOptions,
  getFulfillmentDetail,
  getTransportationSummary,
  getCostDetail,
  getInventoryKpis,
  getRegistryScenario,
  updateRegistryScenario,
  getOrgConfig,
  fulfillmentCsvUrl,
  eventsExportUrl,
  snapshotsExportUrl,
  databaseExportUrl,
} from '../api/client'
import type {
  ResultsSummary,
  EventFilterOptions,
  FulfillmentDetail,
  TransportationEdge,
  CostDetail,
  InventoryKpiData,
  OrgConfig,
} from '../api/client'
import InventoryChart from '../components/InventoryChart'
import ScenarioConfigForm from '../components/ScenarioConfigForm'

// ── Collapsible section context ──────────────────────────────────────

const SectionContext = createContext<{
  version: number
  setAllExpanded: (expanded: boolean) => void
  registerKey: (key: string) => void
}>({ version: 0, setAllExpanded: () => {}, registerKey: () => {} })

function useSectionProvider() {
  const [version, setVersion] = useState(0)
  const keysRef = useRef(new Set<string>())

  const registerKey = useCallback((key: string) => { keysRef.current.add(key) }, [])

  const setAllExpanded = useCallback((expanded: boolean) => {
    for (const key of keysRef.current) {
      localStorage.setItem(key, expanded ? '1' : '0')
    }
    setVersion(v => v + 1)
  }, [])

  return { version, setAllExpanded, registerKey }
}

/** Collapsible section with localStorage-persisted state. */
function Section({ sectionKey, title, children }: {
  sectionKey: string
  title: string
  children: React.ReactNode
}) {
  const storageKey = `scim_section_${sectionKey}`
  const { version, registerKey } = useContext(SectionContext)
  const [expanded, setExpanded] = useState(() => localStorage.getItem(storageKey) !== '0')

  useEffect(() => { registerKey(storageKey) }, [storageKey, registerKey])

  // Re-read from localStorage when version changes (show/collapse all)
  useEffect(() => {
    setExpanded(localStorage.getItem(storageKey) !== '0')
  }, [version, storageKey])

  const toggle = () => {
    const next = !expanded
    setExpanded(next)
    localStorage.setItem(storageKey, next ? '1' : '0')
  }

  return (
    <section className="kpi-section collapsible-section">
      <h2 className="section-header" onClick={toggle} style={{ cursor: 'pointer', userSelect: 'none' }}>
        <span className="section-chevron">{expanded ? '\u25BC' : '\u25B6'}</span>
        {' '}{title}
      </h2>
      {expanded && <div className="section-body">{children}</div>}
    </section>
  )
}

function SectionControls() {
  const { setAllExpanded } = useContext(SectionContext)
  return (
    <div className="section-controls">
      <button type="button" className="link-btn" onClick={() => setAllExpanded(true)}>Show All Sections</button>
      <span className="section-controls-sep">|</span>
      <button type="button" className="link-btn" onClick={() => setAllExpanded(false)}>Collapse All Sections</button>
    </div>
  )
}

// ── Formatting helpers ───────────────────────────────────────────────

function fmtCost(v: number): string {
  return `$${v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

function fmtQty(v: number): string {
  return v.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

// ── Main page ────────────────────────────────────────────────────────

type TabKey = 'overview' | 'fulfillment' | 'inventory' | 'nodes' | 'transportation' | 'costs' | 'events' | 'flows' | 'configure'
const TABS: TabKey[] = ['overview', 'fulfillment', 'inventory', 'nodes', 'transportation', 'costs', 'events', 'flows', 'configure']

export default function ScenarioPage() {
  const { dbName, scenarioId } = useParams<{ dbName: string; scenarioId: string }>()
  const [data, setData] = useState<ResultsSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [searchParams, setSearchParams] = useSearchParams()
  const activeTab = (TABS.includes(searchParams.get('tab') as TabKey) ? searchParams.get('tab') : 'overview') as TabKey
  const setActiveTab = (tab: TabKey) => setSearchParams({ tab }, { replace: true })
  const flowDataUrl = dbName && scenarioId
    ? `/api/results/${encodeURIComponent(scenarioId)}/flow-data?db=${encodeURIComponent(dbName)}`
    : null

  // Inline editing state
  const [scenarioName, setScenarioName] = useState<string>('')
  const [scenarioDesc, setScenarioDesc] = useState<string>('')
  const [editing, setEditing] = useState(false)
  const [editName, setEditName] = useState('')
  const [editDesc, setEditDesc] = useState('')

  const projectId = dbName?.replace(/\.duckdb$/, '') || ''
  const [overviewKpis, setOverviewKpis] = useState<{ avg_inventory_value: number; months_of_supply: number } | null>(null)
  const [registryStatus, setRegistryStatus] = useState<string | null>(null)

  const sectionCtx = useSectionProvider()

  const refreshData = useCallback(() => {
    if (!dbName || !scenarioId) return
    // Fetch results summary and scenario detail (for name/description)
    // Results may not exist (draft/modified scenarios), so handle gracefully
    Promise.all([
      getResultsSummary(dbName, scenarioId).catch(() => null),
      getRegistryScenario(dbName.replace(/\.duckdb$/, ''), scenarioId).catch(() => null),
      getInventoryKpis(dbName, scenarioId).catch(() => null),
    ])
      .then(([results, regScenario, invKpiData]) => {
        setData(results)
        if (regScenario) {
          setScenarioName(String(regScenario.name || scenarioId))
          setScenarioDesc(String(regScenario.description || ''))
          setRegistryStatus(regScenario.status)
        }
        if (invKpiData?.kpis) {
          setOverviewKpis({
            avg_inventory_value: invKpiData.kpis.avg_inventory_value,
            months_of_supply: invKpiData.kpis.months_of_supply,
          })
        }
        setLoading(false)
      })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [dbName, scenarioId])

  useEffect(() => { refreshData() }, [refreshData])

  function startEditing() {
    setEditName(scenarioName)
    setEditDesc(scenarioDesc)
    setEditing(true)
  }

  async function saveEdits() {
    const newName = editName.trim() || scenarioName
    const newDesc = editDesc.trim()
    const fields: { name?: string; description?: string } = {}
    if (newName !== scenarioName) fields.name = newName
    if (newDesc !== scenarioDesc) fields.description = newDesc
    if (Object.keys(fields).length > 0) {
      try {
        await updateRegistryScenario(projectId, scenarioId!, fields)
        if (fields.name) setScenarioName(fields.name)
        if (fields.description !== undefined) setScenarioDesc(fields.description)
      } catch { /* ignore — registry may not have this scenario */ }
    }
    setEditing(false)
  }

  function cancelEditing() {
    setEditing(false)
  }

  if (loading) return <p>Loading results...</p>
  if (error && !data && activeTab !== 'configure') return <div className="error">Error: {error}</div>
  if (!dbName || !scenarioId) return <div className="error">No data</div>

  const isModified = registryStatus === 'modified'
  const hasResults = !!data
  const metadata = data?.metadata
  const events = data?.events
  const fulfillment = data?.fulfillment
  const costs = data?.costs

  return (
    <SectionContext.Provider value={sectionCtx}>
      <div className="scenario-page">
        <div className="scenario-header">
          <Link to={`/project/${encodeURIComponent(dbName)}/${encodeURIComponent(projectId)}`} className="back-link">&larr; Back</Link>
          {editing ? (
            <>
              <div className="inline-edit">
                <input
                  className="inline-edit-input inline-edit-name"
                  value={editName}
                  onChange={e => setEditName(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter') saveEdits(); if (e.key === 'Escape') cancelEditing() }}
                  autoFocus
                />
              </div>
              <div className="inline-edit">
                <textarea
                  className="inline-edit-input inline-edit-desc"
                  value={editDesc}
                  onChange={e => setEditDesc(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Escape') cancelEditing() }}
                  placeholder="Add a description..."
                  rows={3}
                />
              </div>
              <div className="inline-edit-actions">
                <button className="inline-edit-btn" onClick={saveEdits}>Save</button>
                <button className="inline-edit-btn" onClick={cancelEditing}>Cancel</button>
              </div>
            </>
          ) : (
            <>
              <div className="scenario-id-label">Scenario ID: {String(scenarioId)}</div>
              <div className="scenario-title-row">
                <h1>{scenarioName}</h1>
                <button className="icon-btn" title="Edit name and description" onClick={startEditing}>{'\u270E'}</button>
              </div>
              <p className="scenario-description">
                {scenarioDesc || <span className="placeholder-text">No description</span>}
              </p>
            </>
          )}
          <div className="scenario-meta">
            {metadata ? (
              <>
                <span className={`status-badge status-${metadata.status}`}>{String(metadata.status)}</span>
                <span>{String(metadata.total_steps)} steps</span>
                <span>{String(metadata.wall_clock_seconds)}s runtime</span>
              </>
            ) : (
              <span className={`status-badge status-${registryStatus || 'none'}`}>{registryStatus || 'not run'}</span>
            )}
          </div>
        </div>

        <div className="tab-bar">
          {TABS.map(tab => (
            <button
              key={tab}
              className={`tab ${activeTab === tab ? 'active' : ''}`}
              onClick={() => setActiveTab(tab)}
            >
              {tab.charAt(0).toUpperCase() + tab.slice(1)}
            </button>
          ))}
        </div>
        {activeTab !== 'configure' && <SectionControls />}

        {isModified && activeTab !== 'configure' && (
          <div className="config-banner">
            These results are from a previous configuration. Re-run to update.
          </div>
        )}

        {activeTab === 'configure' && (
          <div className="tab-content">
            <ScenarioConfigForm
              dbName={dbName}
              scenarioId={scenarioId}
              projectId={projectId}
              onStatusChange={refreshData}
            />
          </div>
        )}

        {activeTab !== 'configure' && !hasResults && (
          <div className="tab-content">
            <p className="empty-state">No results yet. Run this scenario or switch to the Configure tab.</p>
          </div>
        )}

        {activeTab === 'overview' && hasResults && fulfillment && costs && events && (
          <div className="tab-content">
            <Section sectionKey="overview_fulfillment" title="Fulfillment">
              <div className="kpi-grid">
                <KpiCard label="Fill Rate" value={`${fulfillment.fill_rate_pct}%`} />
                <KpiCard label="Demand Units" value={fulfillment.demand_units.toLocaleString()} />
                <KpiCard label="Fulfilled" value={fulfillment.fulfilled_units.toLocaleString()} />
                <KpiCard label="Lost Sales" value={fulfillment.lost_sale_units.toLocaleString()} />
                <KpiCard label="Backorders" value={fulfillment.backorder_units.toLocaleString()} />
              </div>
            </Section>

            <Section sectionKey="overview_costs" title="Costs">
              <div className="kpi-grid">
                <KpiCard label="Total Cost" value={`$${costs.total_cost.toLocaleString(undefined, { minimumFractionDigits: 2 })}`} />
              </div>
              <table className="data-table compact">
                <thead>
                  <tr><th>Cost Type</th><th>Amount</th></tr>
                </thead>
                <tbody>
                  {costs.by_event_type.map(c => (
                    <tr key={c.event_type}>
                      <td>{c.event_type}</td>
                      <td>${c.cost.toLocaleString(undefined, { minimumFractionDigits: 2 })}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Section>

            <Section sectionKey="overview_events" title="Event Summary">
              <table className="data-table compact">
                <thead>
                  <tr><th>Event Type</th><th>Count</th><th>Qty</th><th>Cost</th></tr>
                </thead>
                <tbody>
                  {events.map(e => (
                    <tr key={e.event_type}>
                      <td>{e.event_type}</td>
                      <td>{e.count.toLocaleString()}</td>
                      <td>{e.total_qty != null ? e.total_qty.toLocaleString() : '-'}</td>
                      <td>{e.total_cost != null ? `$${e.total_cost.toLocaleString(undefined, { minimumFractionDigits: 2 })}` : '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Section>

            <Section sectionKey="overview_inventory" title="Inventory">
              <div className="kpi-grid">
                <KpiCard label="Avg. Inventory Value (at Cost)" value={overviewKpis ? fmtCost(overviewKpis.avg_inventory_value) : '-'} />
                <KpiCard label="Avg. MOS" value={overviewKpis ? String(overviewKpis.months_of_supply) : '-'} />
              </div>
            </Section>

            <Section sectionKey="overview_export" title="Export">
              <div className="export-links">
                <a href={eventsExportUrl(dbName, scenarioId)} download>Download Events CSV</a>
                <a href={snapshotsExportUrl(dbName, scenarioId)} download>Download Snapshots CSV</a>
                <a href={databaseExportUrl(dbName)} download>Download Database</a>
              </div>
            </Section>
          </div>
        )}

        {activeTab === 'fulfillment' && (
          <div className="tab-content">
            <FulfillmentTab dbName={dbName} scenarioId={scenarioId} />
          </div>
        )}

        {activeTab === 'inventory' && (
          <div className="tab-content">
            <InventoryTab dbName={dbName} scenarioId={scenarioId} />
          </div>
        )}

        {activeTab === 'nodes' && (
          <div className="tab-content">
            <NodesTab dbName={dbName} scenarioId={scenarioId} />
          </div>
        )}

        {activeTab === 'transportation' && (
          <div className="tab-content">
            <TransportationTab dbName={dbName} scenarioId={scenarioId} />
          </div>
        )}

        {activeTab === 'costs' && (
          <div className="tab-content">
            <CostsTab dbName={dbName} scenarioId={scenarioId} />
          </div>
        )}

        {activeTab === 'events' && (
          <div className="tab-content">
            <EventLog dbName={dbName} scenarioId={scenarioId} />
          </div>
        )}

        {activeTab === 'flows' && (
          <div className="tab-content">
            <Section sectionKey="flows_viz" title="Network Flow Visualization">
              <p style={{ marginBottom: 12, color: 'var(--text-muted)', fontSize: 13 }}>
                Animated map of fulfillment flows. Opens the flow visualizer with this scenario&apos;s data.
              </p>
              {flowDataUrl && (
                <a
                  href={`http://localhost:5174?data=${encodeURIComponent(flowDataUrl)}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="btn-primary"
                  style={{ display: 'inline-block', textDecoration: 'none' }}
                >
                  Open Flow Visualizer
                </a>
              )}
            </Section>
          </div>
        )}
      </div>
    </SectionContext.Provider>
  )
}

// ── Shared components ────────────────────────────────────────────────

function KpiCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="kpi-card">
      <div className="kpi-value">{value}</div>
      <div className="kpi-label">{label}</div>
    </div>
  )
}

function MultiSelect({ label, options, selected, onChange }: {
  label: string
  options: string[]
  selected: string[]
  onChange: (vals: string[]) => void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const toggle = (val: string) => {
    onChange(selected.includes(val) ? selected.filter(v => v !== val) : [...selected, val])
  }

  return (
    <div ref={ref} style={{ position: 'relative', display: 'inline-block' }}>
      <label>{label}: </label>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        style={{
          minWidth: 100, textAlign: 'left', padding: '2px 6px',
          background: 'var(--bg-secondary, #fff)', border: '1px solid var(--border, #ccc)',
          borderRadius: 4, cursor: 'pointer', fontSize: 13,
        }}
      >
        {selected.length === 0 ? 'All' : `${selected.length} selected`}
        {' \u25BE'}
      </button>
      {open && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, zIndex: 100,
          background: 'var(--bg-primary, #fff)', border: '1px solid var(--border, #ccc)',
          borderRadius: 4, maxHeight: 240, overflowY: 'auto', minWidth: 180,
          boxShadow: '0 4px 12px rgba(0,0,0,.15)',
        }}>
          {selected.length > 0 && (
            <div
              style={{ padding: '4px 8px', cursor: 'pointer', fontSize: 12, color: 'var(--text-muted)', borderBottom: '1px solid var(--border, #eee)' }}
              onClick={() => onChange([])}
            >
              Clear all
            </div>
          )}
          {options.map(opt => (
            <label key={opt} style={{ display: 'flex', alignItems: 'center', padding: '3px 8px', cursor: 'pointer', fontSize: 13, gap: 6 }}>
              <input type="checkbox" checked={selected.includes(opt)} onChange={() => toggle(opt)} />
              {opt}
            </label>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Tab components ───────────────────────────────────────────────────

function FulfillmentTab({ dbName, scenarioId }: { dbName: string; scenarioId: string }) {
  const [data, setData] = useState<FulfillmentDetail | null>(null)
  const [loading, setLoading] = useState(true)
  const [productLimit, setProductLimit] = useState(50)

  useEffect(() => {
    getFulfillmentDetail(dbName, scenarioId)
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [dbName, scenarioId])

  if (loading) return <p>Loading...</p>
  if (!data) return <div className="error">Failed to load fulfillment data</div>

  const { stats, by_days, by_node, by_product } = data
  const visibleProducts = by_product.slice(0, productLimit)

  return (
    <div>
      <Section sectionKey="fulfillment_summary" title="Fulfillment Summary">
        <div className="kpi-grid">
          <KpiCard label="Fill Rate" value={`${stats.fill_rate_pct}%`} />
          <KpiCard label="Demand Units" value={fmtQty(stats.demand_units)} />
          <KpiCard label="Fulfilled" value={fmtQty(stats.fulfilled_units)} />
          <KpiCard label="Value Shipped" value={fmtCost(stats.value_shipped ?? 0)} />
          <KpiCard label="Lost Sales" value={fmtQty(stats.lost_sale_units)} />
          <KpiCard label="Backorders" value={fmtQty(stats.backorder_units)} />
        </div>
      </Section>

      <Section sectionKey="fulfillment_by_days" title="Delivery Speed">
        <div className="kpi-grid" style={{ marginBottom: 12 }}>
          <KpiCard label="Median Days" value={String(by_days.median_days)} />
          <KpiCard label="Average Days" value={by_days.avg_days.toFixed(1)} />
        </div>
        {(() => {
          const totalQty = by_days.buckets.reduce((sum, b) => sum + b.qty, 0)
          const maxPct = totalQty > 0 ? Math.max(...by_days.buckets.map(b => b.qty / totalQty * 100)) : 0
          return (
            <table className="data-table compact">
              <thead>
                <tr><th>Delivery Days</th><th>Qty</th><th>Pct (Qty)</th><th>Value</th><th style={{ width: 120 }}></th></tr>
              </thead>
              <tbody>
                {by_days.buckets.map(b => {
                  const pct = totalQty > 0 ? b.qty / totalQty * 100 : 0
                  const barWidth = maxPct > 0 ? pct / maxPct * 100 : 0
                  return (
                    <tr key={b.day}>
                      <td>{b.label}{b.day >= 5 ? '' : ' day'}{b.day === 1 ? '' : b.day < 5 ? 's' : ''}</td>
                      <td>{fmtQty(b.qty)}</td>
                      <td>{pct.toFixed(1)}%</td>
                      <td>{fmtCost(b.value)}</td>
                      <td><div className="histogram-bar" style={{ width: `${barWidth}%` }} /></td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )
        })()}
      </Section>

      <Section sectionKey="fulfillment_by_node" title="Fulfillment by Distribution Node">
        <div className="export-links" style={{ marginBottom: 12 }}>
          <a href={fulfillmentCsvUrl(dbName, scenarioId, 'by_node')} download>Download CSV</a>
        </div>
        <table className="data-table compact">
          <thead>
            <tr>
              <th>Node</th>
              <th>Shipments</th>
              <th>Units Fulfilled</th>
              <th>Value Shipped</th>
              <th>Fulfillment Cost</th>
            </tr>
          </thead>
          <tbody>
            {by_node.map(n => (
              <tr key={n.dist_node_id}>
                <td>{n.dist_node_id}</td>
                <td>{fmtQty(n.fulfilled_events)}</td>
                <td>{fmtQty(n.fulfilled_units)}</td>
                <td>{fmtCost(n.value_shipped)}</td>
                <td>{fmtCost(n.fulfillment_cost)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>

      <Section sectionKey="fulfillment_by_product" title="Fulfillment by Product">
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12 }}>
          <label style={{ fontSize: 13 }}>
            Show:{' '}
            <select value={productLimit} onChange={e => setProductLimit(Number(e.target.value))}>
              {[10, 50, 100, 200, 1000].map(n => (
                <option key={n} value={n}>Top {n}</option>
              ))}
            </select>
          </label>
          <a href={fulfillmentCsvUrl(dbName, scenarioId, 'by_product')} download className="export-links">
            Download All Products CSV
          </a>
        </div>
        <table className="data-table compact">
          <thead>
            <tr>
              <th>Product</th>
              <th>Demand</th>
              <th>Fulfilled</th>
              <th>Value Shipped</th>
              <th>Lost</th>
              <th>Backorder</th>
              <th>Fill Rate</th>
            </tr>
          </thead>
          <tbody>
            {visibleProducts.map(p => (
              <tr key={p.product_id}>
                <td>{p.product_id}</td>
                <td>{fmtQty(p.demand_units)}</td>
                <td>{fmtQty(p.fulfilled_units)}</td>
                <td>{fmtCost(p.value_shipped)}</td>
                <td>{fmtQty(p.lost_units)}</td>
                <td>{fmtQty(p.backorder_units)}</td>
                <td>{p.fill_rate_pct}%</td>
              </tr>
            ))}
          </tbody>
        </table>
        {by_product.length > productLimit && (
          <p style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 8 }}>
            Showing {productLimit} of {by_product.length} products
          </p>
        )}
      </Section>
    </div>
  )
}

function InventoryTab({ dbName, scenarioId }: { dbName: string; scenarioId: string }) {
  const [kpiData, setKpiData] = useState<InventoryKpiData | null>(null)
  const [kpiLoading, setKpiLoading] = useState(true)
  const [productLimit, setProductLimit] = useState(50)

  useEffect(() => {
    getInventoryKpis(dbName, scenarioId)
      .then(d => { setKpiData(d); setKpiLoading(false) })
      .catch(() => setKpiLoading(false))
  }, [dbName, scenarioId])

  const visibleProducts = kpiData?.by_product?.slice(0, productLimit) ?? []

  return (
    <div>
      {!kpiLoading && kpiData?.kpis && (
        <Section sectionKey="inventory_kpis" title="Inventory KPIs">
          <div className="kpi-grid">
            <KpiCard label="Avg. Inventory Value (at Cost)" value={fmtCost(kpiData.kpis.avg_inventory_value)} />
            <KpiCard label="Avg Inventory (units)" value={fmtQty(kpiData.kpis.avg_inventory_units)} />
            <KpiCard label="Months of Supply" value={String(kpiData.kpis.months_of_supply)} />
            <KpiCard label="Inventory Turns" value={String(kpiData.kpis.inventory_turns)} />
          </div>
        </Section>
      )}

      <Section sectionKey="inventory_chart" title="Inventory Over Time">
        <InventoryChart dbName={dbName} scenarioId={scenarioId} />
      </Section>

      {!kpiLoading && kpiData && kpiData.by_node.length > 0 && (
        <Section sectionKey="inventory_by_node" title="Average Inventory by Node">
          <table className="data-table compact">
            <thead>
              <tr>
                <th>Node</th>
                <th>Avg Parts in Stock</th>
                <th>Avg Units in Stock</th>
                <th>Avg $ in Stock</th>
              </tr>
            </thead>
            <tbody>
              {kpiData.by_node.map(n => (
                <tr key={n.dist_node_id}>
                  <td>{n.dist_node_id}</td>
                  <td>{fmtQty(n.avg_parts_in_stock)}</td>
                  <td>{fmtQty(n.avg_units_in_stock)}</td>
                  <td>{fmtCost(n.avg_value_in_stock)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Section>
      )}

      {!kpiLoading && kpiData && visibleProducts.length > 0 && (
        <Section sectionKey="inventory_by_product" title="Average Inventory by Product">
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12 }}>
            <label style={{ fontSize: 13 }}>
              Show:{' '}
              <select value={productLimit} onChange={e => setProductLimit(Number(e.target.value))}>
                {[10, 20, 50, 100, 200, 500].map(n => (
                  <option key={n} value={n}>Top {n}</option>
                ))}
              </select>
            </label>
          </div>
          <table className="data-table compact">
            <thead>
              <tr>
                <th>Product</th>
                <th>Avg Units OH</th>
                <th>Avg Value (at Cost)</th>
                <th>% of Total Inventory</th>
                <th>Avg FCs with OH &gt; 0</th>
              </tr>
            </thead>
            <tbody>
              {visibleProducts.map(p => (
                <tr key={p.product_id}>
                  <td>{p.product_id}</td>
                  <td>{fmtQty(p.avg_units_oh)}</td>
                  <td>{fmtCost(p.avg_value)}</td>
                  <td>{p.pct_of_total.toFixed(1)}%</td>
                  <td>{p.avg_fcs_with_oh.toFixed(1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {(kpiData.by_product?.length ?? 0) > productLimit && (
            <p style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 8 }}>
              Showing {productLimit} of {kpiData.by_product.length} products
            </p>
          )}
        </Section>
      )}
    </div>
  )
}

function NodesTab({ dbName, scenarioId }: { dbName: string; scenarioId: string }) {
  const [fulfillment, setFulfillment] = useState<FulfillmentDetail | null>(null)
  const [invKpis, setInvKpis] = useState<InventoryKpiData | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      getFulfillmentDetail(dbName, scenarioId),
      getInventoryKpis(dbName, scenarioId),
    ])
      .then(([f, i]) => { setFulfillment(f); setInvKpis(i); setLoading(false) })
      .catch(() => setLoading(false))
  }, [dbName, scenarioId])

  if (loading) return <p>Loading...</p>
  if (!fulfillment || !invKpis) return <div className="error">Failed to load node data</div>

  // Merge fulfillment by_node + inventory by_node into a single table
  const invMap = new Map(invKpis.by_node.map(n => [n.dist_node_id, n]))
  const nodeIds = new Set([
    ...fulfillment.by_node.map(n => n.dist_node_id),
    ...invKpis.by_node.map(n => n.dist_node_id),
  ])
  const fulfMap = new Map(fulfillment.by_node.map(n => [n.dist_node_id, n]))

  const rows = Array.from(nodeIds).map(id => ({
    node_id: id,
    shipments: fulfMap.get(id)?.fulfilled_events ?? 0,
    fulfilled_units: fulfMap.get(id)?.fulfilled_units ?? 0,
    value_shipped: fulfMap.get(id)?.value_shipped ?? 0,
    fulfillment_cost: fulfMap.get(id)?.fulfillment_cost ?? 0,
    avg_parts: invMap.get(id)?.avg_parts_in_stock ?? 0,
    avg_units: invMap.get(id)?.avg_units_in_stock ?? 0,
    avg_value: invMap.get(id)?.avg_value_in_stock ?? 0,
  })).sort((a, b) => b.fulfilled_units - a.fulfilled_units)

  return (
    <div>
      <Section sectionKey="nodes_combined" title={`Distribution Nodes (${rows.length})`}>
        <table className="data-table compact">
          <thead>
            <tr>
              <th>Node</th>
              <th>Shipments</th>
              <th>Fulfilled Units</th>
              <th>Value Shipped</th>
              <th>Fulfillment Cost</th>
              <th>Avg Parts in Stock</th>
              <th>Avg Units in Stock</th>
              <th>Avg Value in Stock</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(n => (
              <tr key={n.node_id}>
                <td>{n.node_id}</td>
                <td>{fmtQty(n.shipments)}</td>
                <td>{fmtQty(n.fulfilled_units)}</td>
                <td>{fmtCost(n.value_shipped)}</td>
                <td>{fmtCost(n.fulfillment_cost)}</td>
                <td>{fmtQty(n.avg_parts)}</td>
                <td>{fmtQty(n.avg_units)}</td>
                <td>{fmtCost(n.avg_value)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>
    </div>
  )
}

function TransportationTab({ dbName, scenarioId }: { dbName: string; scenarioId: string }) {
  const [data, setData] = useState<TransportationEdge[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [terms, setTerms] = useState<OrgConfig['terminology']>({ edge: 'Lane', edges: 'Lanes', node: 'Node', nodes: 'Nodes' })
  const [laneLimit, setLaneLimit] = useState(50)
  const [hideZeroVolume, setHideZeroVolume] = useState(true)

  useEffect(() => {
    getTransportationSummary(dbName, scenarioId)
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
    getOrgConfig().then(cfg => setTerms(cfg.terminology))
  }, [dbName, scenarioId])

  if (loading) return <p>Loading...</p>
  if (!data) return <div className="error">Failed to load transportation data</div>

  const filtered = hideZeroVolume ? data.filter(e => e.shipments > 0) : data
  const byType = new Map<string, { shipments: number; qty: number; cost: number; lanes: number }>()
  for (const e of filtered) {
    const key = e.transport_type || 'unknown'
    const cur = byType.get(key) || { shipments: 0, qty: 0, cost: 0, lanes: 0 }
    cur.shipments += e.shipments
    cur.qty += e.total_qty
    cur.cost += e.total_cost
    cur.lanes += 1
    byType.set(key, cur)
  }
  const visibleLanes = filtered.slice(0, laneLimit)
  const zeroCount = data.filter(e => e.shipments === 0).length

  // Node-level summary: aggregate outbound lanes per origin node
  const byNode = new Map<string, { activeDests: number; shipments: number; weightedTime: number; weightedDist: number; totalShipments: number }>()
  for (const e of filtered) {
    if (e.origin_node_type !== 'distribution') continue
    const cur = byNode.get(e.origin_node_id) || { activeDests: 0, shipments: 0, weightedTime: 0, weightedDist: 0, totalShipments: 0 }
    if (e.shipments > 0) cur.activeDests += 1
    cur.shipments += e.shipments
    cur.weightedTime += (e.mean_transit_time ?? 0) * e.shipments
    cur.weightedDist += (e.distance ?? 0) * e.shipments
    cur.totalShipments += e.shipments
    byNode.set(e.origin_node_id, cur)
  }
  const nodeSummary = Array.from(byNode.entries())
    .map(([id, n]) => ({
      node_id: id,
      active_dests: n.activeDests,
      shipments: n.shipments,
      avg_s2d: n.totalShipments > 0 ? n.weightedTime / n.totalShipments : 0,
      avg_dist: n.totalShipments > 0 ? n.weightedDist / n.totalShipments : 0,
    }))
    .sort((a, b) => b.shipments - a.shipments)

  return (
    <div>
      <Section sectionKey="transport_by_type" title="Summary by Transport Type">
        <table className="data-table compact">
          <thead>
            <tr>
              <th>Type</th>
              <th>{terms.edges}</th>
              <th>Shipments</th>
              <th>Total Qty</th>
              <th>Total Cost</th>
            </tr>
          </thead>
          <tbody>
            {Array.from(byType.entries()).map(([type, s]) => (
              <tr key={type}>
                <td>{type}</td>
                <td>{fmtQty(s.lanes)}</td>
                <td>{fmtQty(s.shipments)}</td>
                <td>{fmtQty(s.qty)}</td>
                <td>{fmtCost(s.cost)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>

      {nodeSummary.length > 0 && (
        <Section sectionKey="transport_by_node" title={`By ${terms.node} (${nodeSummary.length})`}>
          <table className="data-table compact">
            <thead>
              <tr>
                <th>{terms.node}</th>
                <th>Active Outbound {terms.nodes}</th>
                <th>Total Shipments</th>
                <th>Avg. S2D Time</th>
                <th>Avg. Haversine Distance</th>
              </tr>
            </thead>
            <tbody>
              {nodeSummary.map(n => (
                <tr key={n.node_id}>
                  <td>{n.node_id}</td>
                  <td>{fmtQty(n.active_dests)}</td>
                  <td>{fmtQty(n.shipments)}</td>
                  <td>{n.avg_s2d.toFixed(1)}d</td>
                  <td>{Math.round(n.avg_dist).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Section>
      )}

      <Section sectionKey="transport_edges" title={`${terms.edge} Detail (${filtered.length} ${terms.edges.toLowerCase()})`}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12, flexWrap: 'wrap' }}>
          <label style={{ fontSize: 13 }}>
            Show:{' '}
            <select value={laneLimit} onChange={e => setLaneLimit(Number(e.target.value))}>
              {[10, 20, 50, 100, 200, 500, 1000].map(n => (
                <option key={n} value={n}>Top {n}</option>
              ))}
            </select>
          </label>
          <label style={{ fontSize: 13, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4 }}>
            <input type="checkbox" checked={hideZeroVolume} onChange={e => setHideZeroVolume(e.target.checked)} />
            Hide zero-volume {terms.edges.toLowerCase()} {zeroCount > 0 && `(${zeroCount})`}
          </label>
        </div>
        <table className="data-table compact">
          <thead>
            <tr>
              <th>Origin</th>
              <th>Destination</th>
              <th>Type</th>
              <th>Shipments</th>
              <th>Qty</th>
              <th>Avg. S2D Time</th>
              <th>Haversine Distance</th>
              <th>Cost</th>
            </tr>
          </thead>
          <tbody>
            {visibleLanes.map(e => (
              <tr key={e.edge_id}>
                <td>{e.origin_node_id}</td>
                <td>{e.dest_node_id}</td>
                <td>{e.transport_type}</td>
                <td>{fmtQty(e.shipments)}</td>
                <td>{fmtQty(e.total_qty)}</td>
                <td>{e.mean_transit_time != null ? `${e.mean_transit_time}d` : '-'}</td>
                <td>{e.distance != null ? `${Math.round(e.distance).toLocaleString()}` : '-'}</td>
                <td>{e.total_cost > 0 ? fmtCost(e.total_cost) : '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {filtered.length > laneLimit && (
          <p style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 8 }}>
            Showing {laneLimit} of {filtered.length} {terms.edges.toLowerCase()}
          </p>
        )}
      </Section>
    </div>
  )
}

function CostsTab({ dbName, scenarioId }: { dbName: string; scenarioId: string }) {
  const [data, setData] = useState<CostDetail | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getCostDetail(dbName, scenarioId)
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [dbName, scenarioId])

  if (loading) return <p>Loading...</p>
  if (!data) return <div className="error">Failed to load cost data</div>

  return (
    <div>
      <Section sectionKey="costs_summary" title="Cost Summary">
        <div className="kpi-grid">
          <KpiCard label="Total Cost" value={fmtCost(data.total_cost)} />
        </div>
      </Section>

      <Section sectionKey="costs_by_type" title="Cost by Type">
        <table className="data-table compact">
          <thead>
            <tr>
              <th>Cost Type</th>
              <th>Amount</th>
              <th>% of Total</th>
            </tr>
          </thead>
          <tbody>
            {data.by_event_type.map(c => (
              <tr key={c.event_type}>
                <td>{c.event_type}</td>
                <td>{fmtCost(c.cost)}</td>
                <td>{data.total_cost > 0 ? `${(c.cost / data.total_cost * 100).toFixed(1)}%` : '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>

      <Section sectionKey="costs_by_node" title="Cost by Node">
        <table className="data-table compact">
          <thead>
            <tr>
              <th>Node</th>
              <th>Total</th>
              <th>Fixed</th>
              <th>Fulfillment</th>
              <th>Overage</th>
            </tr>
          </thead>
          <tbody>
            {data.by_node.map(n => (
              <tr key={n.node_id}>
                <td>{n.node_id}</td>
                <td>{fmtCost(n.total_cost)}</td>
                <td>{fmtCost(n.fixed_cost)}</td>
                <td>{fmtCost(n.fulfillment_cost)}</td>
                <td>{n.overage_cost > 0 ? fmtCost(n.overage_cost) : '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>

      <Section sectionKey="costs_by_product" title="Cost by Product">
        <table className="data-table compact">
          <thead>
            <tr>
              <th>Product</th>
              <th>Total Cost</th>
              <th>% of Total</th>
            </tr>
          </thead>
          <tbody>
            {data.by_product.map(p => (
              <tr key={p.product_id}>
                <td>{p.product_id}</td>
                <td>{fmtCost(p.cost)}</td>
                <td>{data.total_cost > 0 ? `${(p.cost / data.total_cost * 100).toFixed(1)}%` : '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>
    </div>
  )
}

function EventLog({ dbName, scenarioId }: { dbName: string; scenarioId: string }) {
  const [events, setEvents] = useState<Record<string, unknown>[]>([])
  const [total, setTotal] = useState(0)
  const [offset, setOffset] = useState(0)
  const [filterOpts, setFilterOpts] = useState<EventFilterOptions | null>(null)
  const [eventTypes, setEventTypes] = useState<string[]>([])
  const [productIds, setProductIds] = useState<string[]>([])
  const [originNodeIds, setOriginNodeIds] = useState<string[]>([])
  const [destNodeIds, setDestNodeIds] = useState<string[]>([])
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [sortBy, setSortBy] = useState('')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')
  const [loading, setLoading] = useState(true)
  const limit = 50

  useEffect(() => {
    getEventFilterOptions(dbName, scenarioId).then(setFilterOpts)
  }, [dbName, scenarioId])

  useEffect(() => {
    setLoading(true)
    const params: Record<string, string | number | string[]> = { limit, offset }
    if (eventTypes.length) params.event_type = eventTypes
    if (productIds.length) params.product_id = productIds
    if (originNodeIds.length) params.origin_node_id = originNodeIds
    if (destNodeIds.length) params.dest_node_id = destNodeIds
    if (dateFrom) params.date_from = dateFrom
    if (dateTo) params.date_to = dateTo
    if (sortBy) { params.sort_by = sortBy; params.sort_dir = sortDir }

    getEventsApi(dbName, scenarioId, params)
      .then(page => {
        setEvents(page.events)
        setTotal(page.total)
        setLoading(false)
      })
  }, [dbName, scenarioId, offset, eventTypes, productIds, originNodeIds, destNodeIds, dateFrom, dateTo, sortBy, sortDir])

  const handleSort = (col: string) => {
    if (sortBy === col) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(col)
      setSortDir('asc')
    }
    setOffset(0)
  }

  const sortIcon = (col: string) => sortBy === col ? (sortDir === 'asc' ? ' \u25B2' : ' \u25BC') : ''

  const setFilterAndReset = <T,>(setter: (v: T) => void) => (v: T) => { setter(v); setOffset(0) }

  return (
    <div>
      <div className="event-filters" style={{ display: 'flex', gap: '12px', flexWrap: 'wrap', alignItems: 'center', marginBottom: '8px' }}>
        {filterOpts && (
          <>
            <MultiSelect label="Type" options={filterOpts.event_types} selected={eventTypes} onChange={setFilterAndReset(setEventTypes)} />
            <MultiSelect label="From" options={filterOpts.origin_nodes} selected={originNodeIds} onChange={setFilterAndReset(setOriginNodeIds)} />
            <MultiSelect label="To" options={filterOpts.dest_nodes} selected={destNodeIds} onChange={setFilterAndReset(setDestNodeIds)} />
            <MultiSelect label="Product" options={filterOpts.products} selected={productIds} onChange={setFilterAndReset(setProductIds)} />
          </>
        )}
        <label>
          From: <input type="date" value={dateFrom}
            onChange={e => { setDateFrom(e.target.value); setOffset(0) }} />
        </label>
        <label>
          To: <input type="date" value={dateTo}
            onChange={e => { setDateTo(e.target.value); setOffset(0) }} />
        </label>
        <span className="event-count">{total.toLocaleString()} events</span>
      </div>

      {loading ? <p>Loading...</p> : (
        <>
          <table className="data-table compact">
            <thead>
              <tr>
                <th style={{ cursor: 'pointer' }} onClick={() => handleSort('sim_step')}>Step{sortIcon('sim_step')}</th>
                <th style={{ cursor: 'pointer' }} onClick={() => handleSort('sim_date')}>Date{sortIcon('sim_date')}</th>
                <th style={{ cursor: 'pointer' }} onClick={() => handleSort('event_type')}>Type{sortIcon('event_type')}</th>
                <th style={{ cursor: 'pointer' }} onClick={() => handleSort('origin_node_id')}>From{sortIcon('origin_node_id')}</th>
                <th style={{ cursor: 'pointer' }} onClick={() => handleSort('dest_node_id')}>To{sortIcon('dest_node_id')}</th>
                <th style={{ cursor: 'pointer' }} onClick={() => handleSort('product_id')}>Product{sortIcon('product_id')}</th>
                <th style={{ cursor: 'pointer' }} onClick={() => handleSort('quantity')}>Qty{sortIcon('quantity')}</th>
                <th style={{ cursor: 'pointer' }} onClick={() => handleSort('cost')}>Cost{sortIcon('cost')}</th>
              </tr>
            </thead>
            <tbody>
              {events.map((e, i) => (
                <tr key={i}>
                  <td>{String(e.sim_step)}</td>
                  <td>{String(e.sim_date)}</td>
                  <td>{String(e.event_type)}</td>
                  <td>{String(e.origin_node_id || e.node_id || '-')}</td>
                  <td>{String(e.dest_node_id || '-')}</td>
                  <td>{String(e.product_id || '-')}</td>
                  <td>{e.quantity != null ? Number(e.quantity).toLocaleString() : '-'}</td>
                  <td>{e.cost != null ? `$${Number(e.cost).toLocaleString(undefined, { minimumFractionDigits: 2 })}` : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="pagination">
            <button disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - limit))}>Prev</button>
            <span>{offset + 1}-{Math.min(offset + limit, total)} of {total}</span>
            <button disabled={offset + limit >= total} onClick={() => setOffset(offset + limit)}>Next</button>
          </div>
        </>
      )}
    </div>
  )
}
