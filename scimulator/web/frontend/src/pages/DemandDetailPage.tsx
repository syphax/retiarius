import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { getDemandSummary } from '../api/client'
import type { DemandVersionSummary } from '../api/client'

export default function DemandDetailPage() {
  const { dbName, projectId } = useParams<{ dbName: string; projectId: string }>()
  const [versions, setVersions] = useState<DemandVersionSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!dbName) return
    setLoading(true)
    getDemandSummary(dbName)
      .then(d => { setVersions(d.dataset_versions); setLoading(false) })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [dbName])

  const datasetsTab = `/project/${encodeURIComponent(dbName ?? '')}/${encodeURIComponent(projectId ?? '')}/datasets`
  const present = versions.filter(v => v.row_count > 0)

  return (
    <div className="demand-detail-page">
      <Link to={datasetsTab} className="back-link">&larr; Datasets</Link>
      <h1>Demand</h1>
      <p className="datasets-subtitle">
        Database: <code>{dbName}</code>
      </p>

      {error && <div className="error">Error: {error}</div>}

      {loading ? (
        <p>Loading...</p>
      ) : (
        <section className="datasets-section">
          <h2>Dataset Versions</h2>
          {present.length === 0 ? (
            <p className="empty-state">No datasets with demand data.</p>
          ) : (
            <table className="data-table">
              <thead>
                {/* TODO(value): add Total Value and Total Qty columns once product-version pricing is wired up */}
                <tr>
                  <th>Dataset Version</th>
                  <th>Name</th>
                  <th>Rows</th>
                  <th>Start Date</th>
                  <th>End Date</th>
                  <th># Demand Nodes</th>
                  <th>Used By</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {present.map(v => (
                  <tr key={v.dataset_version_id}>
                    <td className="scenario-id-col">{v.dataset_version_id}</td>
                    <td>{v.name}</td>
                    <td>{v.row_count.toLocaleString()}</td>
                    <td>{v.start_date ?? '—'}</td>
                    <td>{v.end_date ?? '—'}</td>
                    <td>{v.demand_node_count.toLocaleString()}</td>
                    <td>
                      {v.scenarios.length === 0
                        ? <span className="text-muted">{'—'}</span>
                        : v.scenarios.map((s, i) => (
                          <span key={s.scenario_id}>{i > 0 && ', '}{s.name}</span>
                        ))
                      }
                    </td>
                    <td className="text-muted">
                      {v.created_at ? new Date(v.created_at).toLocaleDateString() : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      )}
    </div>
  )
}
