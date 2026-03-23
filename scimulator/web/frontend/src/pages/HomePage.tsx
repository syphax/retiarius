import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { listDatabases, listScenarios } from '../api/client'
import type { DatabaseInfo, ScenarioSummary } from '../api/client'

export default function HomePage() {
  const [databases, setDatabases] = useState<DatabaseInfo[]>([])
  const [selectedDb, setSelectedDb] = useState<string | null>(null)
  const [scenarios, setScenarios] = useState<ScenarioSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    listDatabases()
      .then(dbs => {
        setDatabases(dbs)
        if (dbs.length > 0) {
          setSelectedDb(dbs[0].name)
        }
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [])

  useEffect(() => {
    if (!selectedDb) return
    setLoading(true)
    listScenarios(selectedDb)
      .then(s => {
        setScenarios(s)
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [selectedDb])

  if (error) return <div className="error">Error: {error}</div>

  return (
    <div className="home-page">
      <h1>Scenarios</h1>

      {databases.length > 1 && (
        <div className="db-selector">
          <label>Database: </label>
          <select
            value={selectedDb || ''}
            onChange={e => setSelectedDb(e.target.value)}
          >
            {databases.map(db => (
              <option key={db.name} value={db.name}>
                {db.name} ({db.size_mb} MB)
              </option>
            ))}
          </select>
        </div>
      )}

      {databases.length === 0 && !loading && (
        <p className="empty-state">
          No databases found. <Link to="/run">Run a simulation</Link> to get started.
        </p>
      )}

      {loading ? (
        <p>Loading...</p>
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>Scenario</th>
              <th>Period</th>
              <th>Status</th>
              <th>Steps</th>
              <th>Runtime</th>
            </tr>
          </thead>
          <tbody>
            {scenarios.map(s => (
              <tr key={s.scenario_id}>
                <td>
                  <Link to={`/scenario/${selectedDb}/${s.scenario_id}`}>
                    <strong>{s.name}</strong>
                  </Link>
                  <br />
                  <small>{s.scenario_id}</small>
                </td>
                <td>{s.start_date} to {s.end_date}</td>
                <td>
                  <span className={`status-badge status-${s.status || 'none'}`}>
                    {s.status || 'not run'}
                  </span>
                </td>
                <td>{s.total_steps ?? '-'}</td>
                <td>{s.wall_clock_seconds != null ? `${s.wall_clock_seconds}s` : '-'}</td>
              </tr>
            ))}
            {scenarios.length === 0 && (
              <tr><td colSpan={5} className="empty-state">No scenarios in this database.</td></tr>
            )}
          </tbody>
        </table>
      )}
    </div>
  )
}
