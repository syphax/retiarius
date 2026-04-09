import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { listDatasets, deleteEntitySet } from '../api/client'
import type { DatasetInfo, TopologyInfo, EntitySetItem } from '../api/client'

const DATA_TABLES = [
  { key: 'demand', label: 'Demand' },
  { key: 'inbound_schedule', label: 'Inbound Schedule' },
  { key: 'initial_inventory', label: 'Initial Inventory' },
] as const

export default function DatasetsPage() {
  const { dbName } = useParams<{ dbName: string }>()
  const [datasets, setDatasets] = useState<DatasetInfo[]>([])
  const [topology, setTopology] = useState<TopologyInfo[]>([])
  const [entitySets, setEntitySets] = useState<EntitySetItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  function refresh() {
    if (!dbName) return
    setLoading(true)
    listDatasets(dbName)
      .then(data => {
        setDatasets(data.dataset_versions)
        setTopology(data.topology)
        setEntitySets(data.entity_sets)
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }

  useEffect(() => { refresh() }, [dbName])

  async function handleDeleteSet(s: EntitySetItem) {
    if (!dbName) return
    if (!confirm(`Delete entity set "${s.name}"?`)) return
    setError(null)
    try {
      await deleteEntitySet(dbName, s.set_table, s.set_id)
      refresh()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  if (!dbName) return <div className="error">No database specified.</div>

  // Group entity sets by type
  const setsByType: Record<string, EntitySetItem[]> = {}
  for (const s of entitySets) {
    ;(setsByType[s.set_type] ??= []).push(s)
  }

  return (
    <div className="datasets-page">
      <Link to="/" className="back-link">&larr; Projects</Link>
      <h1>Manage Datasets</h1>
      <p className="datasets-subtitle">
        Database: <code>{dbName}</code>
      </p>

      {error && <div className="error">Error: {error}</div>}

      {loading ? (
        <p>Loading datasets...</p>
      ) : (
        <>
          {/* ── Topology Tables ────────────────────────────────── */}
          <section className="datasets-section">
            <h2>Network Topology</h2>
            <table className="data-table">
              <thead>
                <tr>
                  <th>Table</th>
                  <th>Rows</th>
                </tr>
              </thead>
              <tbody>
                {topology.map(t => (
                  <tr key={t.table}>
                    <td>{t.label}</td>
                    <td>{t.row_count.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          {/* ── Entity Sets ───────────────────────────────────── */}
          <section className="datasets-section">
            <h2>Entity Sets</h2>
            <p>
              <Link to={`/datasets/${dbName}/entity-sets/create`}>+ Create new entity set</Link>
            </p>
            {Object.keys(setsByType).length === 0 ? (
              <p className="empty-state">No entity sets defined. All scenarios use the full topology.</p>
            ) : (
              Object.entries(setsByType).map(([type, sets]) => (
                <div key={type} className="datasets-subsection">
                  <h3>{type}</h3>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Set ID</th>
                        <th>Name</th>
                        <th>Members</th>
                        <th>Used By</th>
                        <th></th>
                      </tr>
                    </thead>
                    <tbody>
                      {sets.map(s => (
                        <tr key={s.set_id}>
                          <td className="scenario-id-col">{s.set_id}</td>
                          <td>{s.name}</td>
                          <td>{s.member_count.toLocaleString()}</td>
                          <td>
                            {s.scenarios.length === 0
                              ? <span className="text-muted">—</span>
                              : s.scenarios.map((sc, i) => (
                                <span key={sc.scenario_id}>
                                  {i > 0 && ', '}
                                  {sc.name}
                                </span>
                              ))
                            }
                          </td>
                          <td className="row-actions">
                            <div className="row-actions-inner">
                              <button
                                className="icon-btn icon-btn-danger"
                                title={s.scenarios.length > 0 ? 'Cannot delete: used by scenario(s)' : 'Delete entity set'}
                                disabled={s.scenarios.length > 0}
                                onClick={() => handleDeleteSet(s)}
                              >
                                {'\u2715'}
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ))
            )}
          </section>

          {/* ── Dataset Versions by Table ─────────────────────── */}
          {DATA_TABLES.map(table => {
            const relevant = datasets.filter(d => (d.row_counts[table.key] ?? 0) > 0)
            return (
              <section key={table.key} className="datasets-section">
                <h2>{table.label} Data</h2>
                {relevant.length === 0 ? (
                  <p className="empty-state">No datasets with {table.label.toLowerCase()} data.</p>
                ) : (
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Dataset Version</th>
                        <th>Name</th>
                        <th>Rows</th>
                        <th>Used By</th>
                        <th>Created</th>
                      </tr>
                    </thead>
                    <tbody>
                      {relevant.map(d => (
                        <tr key={d.dataset_version_id}>
                          <td className="scenario-id-col">{d.dataset_version_id}</td>
                          <td>{d.name}</td>
                          <td>{(d.row_counts[table.key] ?? 0).toLocaleString()}</td>
                          <td>
                            {d.scenarios.length === 0
                              ? <span className="text-muted">—</span>
                              : d.scenarios.map((s, i) => (
                                <span key={s.scenario_id}>
                                  {i > 0 && ', '}
                                  {s.name}
                                </span>
                              ))
                            }
                          </td>
                          <td className="text-muted">
                            {d.created_at ? new Date(d.created_at).toLocaleDateString() : '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </section>
            )
          })}

          {/* ── All Dataset Versions Summary ──────────────────── */}
          <section className="datasets-section">
            <h2>All Dataset Versions</h2>
            <table className="data-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Name</th>
                  <th>Description</th>
                  <th>Demand</th>
                  <th>Inbound</th>
                  <th>Inventory</th>
                  <th>Scenarios</th>
                </tr>
              </thead>
              <tbody>
                {datasets.map(d => (
                  <tr key={d.dataset_version_id}>
                    <td className="scenario-id-col">{d.dataset_version_id}</td>
                    <td>{d.name}</td>
                    <td className="text-muted">{d.description || '—'}</td>
                    <td>{(d.row_counts['demand'] ?? 0).toLocaleString()}</td>
                    <td>{(d.row_counts['inbound_schedule'] ?? 0).toLocaleString()}</td>
                    <td>{(d.row_counts['initial_inventory'] ?? 0).toLocaleString()}</td>
                    <td>{d.scenarios.length}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        </>
      )}
    </div>
  )
}
