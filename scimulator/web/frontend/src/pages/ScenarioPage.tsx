import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import {
  getResultsSummary,
  getEvents as getEventsApi,
  eventsExportUrl,
  snapshotsExportUrl,
  databaseExportUrl,
} from '../api/client'
import type { ResultsSummary } from '../api/client'
import InventoryChart from '../components/InventoryChart'

export default function ScenarioPage() {
  const { dbName, scenarioId } = useParams<{ dbName: string; scenarioId: string }>()
  const [data, setData] = useState<ResultsSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'overview' | 'events' | 'inventory' | 'network'>('overview')
  const flowDataUrl = dbName && scenarioId
    ? `/api/results/${encodeURIComponent(scenarioId)}/flow-data?db=${encodeURIComponent(dbName)}`
    : null

  useEffect(() => {
    if (!dbName || !scenarioId) return
    getResultsSummary(dbName, scenarioId)
      .then(d => {
        setData(d)
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [dbName, scenarioId])

  if (loading) return <p>Loading results...</p>
  if (error) return <div className="error">Error: {error}</div>
  if (!data || !dbName || !scenarioId) return <div className="error">No data</div>

  const { metadata, events, fulfillment, costs, inventory } = data

  return (
    <div className="scenario-page">
      <div className="scenario-header">
        <Link to="/" className="back-link">&larr; Back</Link>
        <h1>{String(metadata.scenario_id)}</h1>
        <div className="scenario-meta">
          <span className={`status-badge status-${metadata.status}`}>{String(metadata.status)}</span>
          <span>{String(metadata.total_steps)} steps</span>
          <span>{String(metadata.wall_clock_seconds)}s runtime</span>
        </div>
      </div>

      <div className="tab-bar">
        {(['overview', 'inventory', 'events', 'network'] as const).map(tab => (
          <button
            key={tab}
            className={`tab ${activeTab === tab ? 'active' : ''}`}
            onClick={() => setActiveTab(tab)}
          >
            {tab.charAt(0).toUpperCase() + tab.slice(1)}
          </button>
        ))}
      </div>

      {activeTab === 'overview' && (
        <div className="tab-content">
          {/* Fulfillment KPIs */}
          <section className="kpi-section">
            <h2>Fulfillment</h2>
            <div className="kpi-grid">
              <KpiCard label="Fill Rate" value={`${fulfillment.fill_rate_pct}%`} />
              <KpiCard label="Demand Units" value={fulfillment.demand_units.toLocaleString()} />
              <KpiCard label="Fulfilled" value={fulfillment.fulfilled_units.toLocaleString()} />
              <KpiCard label="Lost Sales" value={fulfillment.lost_sale_units.toLocaleString()} />
              <KpiCard label="Backorders" value={fulfillment.backorder_units.toLocaleString()} />
            </div>
          </section>

          {/* Cost Summary */}
          <section className="kpi-section">
            <h2>Costs</h2>
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
          </section>

          {/* Event Summary */}
          <section className="kpi-section">
            <h2>Event Summary</h2>
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
          </section>

          {/* Final Inventory */}
          {inventory && (
            <section className="kpi-section">
              <h2>Final Inventory (as of {inventory.snapshot_date})</h2>
              <table className="data-table compact">
                <thead>
                  <tr><th>State</th><th>Qty</th><th>Nodes</th><th>Products</th></tr>
                </thead>
                <tbody>
                  {inventory.states.map(s => (
                    <tr key={s.state}>
                      <td>{s.state}</td>
                      <td>{s.quantity.toLocaleString()}</td>
                      <td>{s.nodes}</td>
                      <td>{s.products}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          )}

          {/* Export Links */}
          <section className="kpi-section">
            <h2>Export</h2>
            <div className="export-links">
              <a href={eventsExportUrl(dbName, scenarioId)} download>Download Events CSV</a>
              <a href={snapshotsExportUrl(dbName, scenarioId)} download>Download Snapshots CSV</a>
              <a href={databaseExportUrl(dbName)} download>Download Database</a>
            </div>
          </section>
        </div>
      )}

      {activeTab === 'inventory' && (
        <div className="tab-content">
          <InventoryChart dbName={dbName} scenarioId={scenarioId} />
        </div>
      )}

      {activeTab === 'events' && (
        <div className="tab-content">
          <EventLog dbName={dbName} scenarioId={scenarioId} />
        </div>
      )}

      {activeTab === 'network' && (
        <div className="tab-content">
          <section className="kpi-section">
            <h2>Network Flow Visualization</h2>
            <p style={{ marginBottom: 12, color: 'var(--text-muted)', fontSize: 13 }}>
              Animated map of fulfillment flows. Opens the flow visualizer with this scenario's data.
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
          </section>
        </div>
      )}
    </div>
  )
}

function KpiCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="kpi-card">
      <div className="kpi-value">{value}</div>
      <div className="kpi-label">{label}</div>
    </div>
  )
}

function EventLog({ dbName, scenarioId }: { dbName: string; scenarioId: string }) {
  const [events, setEvents] = useState<Record<string, unknown>[]>([])
  const [total, setTotal] = useState(0)
  const [offset, setOffset] = useState(0)
  const [eventType, setEventType] = useState('')
  const [loading, setLoading] = useState(true)
  const limit = 50

  useEffect(() => {
    setLoading(true)
    const params: Record<string, string | number> = { limit, offset }
    if (eventType) params.event_type = eventType

    getEventsApi(dbName, scenarioId, params)
      .then(page => {
        setEvents(page.events)
        setTotal(page.total)
        setLoading(false)
      })
  }, [dbName, scenarioId, offset, eventType])

  return (
    <div>
      <div className="event-filters">
        <label>Event type: </label>
        <select value={eventType} onChange={e => { setEventType(e.target.value); setOffset(0) }}>
          <option value="">All</option>
          {['demand_received', 'demand_fulfilled', 'backorder_created', 'backorder_fulfilled',
            'lost_sale', 'shipment_arrived', 'inventory_received', 'fixed_cost'].map(t => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>
        <span className="event-count">{total.toLocaleString()} events</span>
      </div>

      {loading ? <p>Loading...</p> : (
        <>
          <table className="data-table compact">
            <thead>
              <tr>
                <th>Step</th><th>Date</th><th>Type</th><th>Node</th>
                <th>Product</th><th>Qty</th><th>Cost</th>
              </tr>
            </thead>
            <tbody>
              {events.map((e, i) => (
                <tr key={i}>
                  <td>{String(e.sim_step)}</td>
                  <td>{String(e.sim_date)}</td>
                  <td>{String(e.event_type)}</td>
                  <td>{String(e.node_id || '-')}</td>
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
