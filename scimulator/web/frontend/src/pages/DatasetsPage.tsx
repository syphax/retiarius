import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { listDatasets } from '../api/client'
import type { DatasetInfo } from '../api/client'

const DATA_TABLES = [
  { key: 'demand', label: 'Demand' },
  { key: 'inbound_schedule', label: 'Inbound Schedule' },
  { key: 'initial_inventory', label: 'Initial Inventory' },
] as const

export default function DatasetsPage() {
  const { dbName } = useParams<{ dbName: string }>()
  const [datasets, setDatasets] = useState<DatasetInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!dbName) return
    setLoading(true)
    listDatasets(dbName)
      .then(data => {
        setDatasets(data)
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [dbName])

  if (!dbName) return <div className="error">No database specified.</div>

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
      ) : datasets.length === 0 ? (
        <p className="empty-state">No dataset versions found in this database.</p>
      ) : (
        <>
          {DATA_TABLES.map(table => {
            const relevant = datasets.filter(d => (d.row_counts[table.key] ?? 0) > 0)
            return (
              <section key={table.key} className="datasets-section">
                <h2>{table.label}</h2>
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

          {/* Summary: all datasets */}
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
