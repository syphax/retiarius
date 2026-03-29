import { useEffect, useState, useCallback } from 'react'
import { Link } from 'react-router-dom'
import {
  listDatabases, listScenarios, listProjects, listRegistryScenarios,
  rerunScenario, duplicateScenario, archiveScenario,
} from '../api/client'
import type { DatabaseInfo, ScenarioSummary, ProjectSummary, RegistryScenarioSummary } from '../api/client'

function formatTimestamp(ts: string | null): string {
  if (!ts) return '-'
  const d = new Date(ts)
  if (isNaN(d.getTime())) return ts
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

export default function HomePage() {
  const [databases, setDatabases] = useState<DatabaseInfo[]>([])
  const [projects, setProjects] = useState<ProjectSummary[]>([])
  const [selectedDb, setSelectedDb] = useState<string | null>(null)
  const [selectedProject, setSelectedProject] = useState<string | null>(null)
  const [scenarios, setScenarios] = useState<ScenarioSummary[]>([])
  const [registryScenarios, setRegistryScenarios] = useState<RegistryScenarioSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [actionInProgress, setActionInProgress] = useState<string | null>(null)

  // Load databases and projects on mount
  useEffect(() => {
    Promise.all([listDatabases(), listProjects()])
      .then(([dbs, projs]) => {
        setDatabases(dbs)
        setProjects(projs)
        if (dbs.length > 0) {
          setSelectedDb(dbs[0].name)
          const match = projs.find(p => p.database === dbs[0].name)
          setSelectedProject(match?.project_id || null)
        }
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [])

  const refreshScenarios = useCallback(() => {
    if (!selectedDb) return
    setLoading(true)

    const promises: Promise<void>[] = [
      listScenarios(selectedDb).then(s => setScenarios(s))
    ]

    if (selectedProject) {
      promises.push(
        listRegistryScenarios(selectedProject)
          .then(s => setRegistryScenarios(s))
          .catch(() => setRegistryScenarios([]))
      )
    } else {
      setRegistryScenarios([])
    }

    Promise.all(promises)
      .then(() => setLoading(false))
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [selectedDb, selectedProject])

  // Load scenarios when selection changes
  useEffect(() => { refreshScenarios() }, [refreshScenarios])

  function handleDbChange(dbName: string) {
    setSelectedDb(dbName)
    const match = projects.find(p => p.database === dbName)
    setSelectedProject(match?.project_id || null)
  }

  async function handleRun(scenarioId: string) {
    if (!selectedDb) return
    setActionInProgress(scenarioId)
    setError(null)
    try {
      await rerunScenario(selectedDb, scenarioId)
      refreshScenarios()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setActionInProgress(null)
    }
  }

  async function handleDuplicate(scenarioId: string, name: string) {
    if (!selectedProject) return
    const newId = prompt('New scenario ID:', `${scenarioId}-copy`)
    if (!newId) return
    setError(null)
    try {
      await duplicateScenario(selectedProject, scenarioId, newId)
      refreshScenarios()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  async function handleArchive(scenarioId: string, name: string) {
    if (!selectedProject) return
    if (!confirm(`Archive "${name}"? It will be hidden from this list.`)) return
    setError(null)
    try {
      await archiveScenario(selectedProject, scenarioId)
      refreshScenarios()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  // Merge: use result DB scenarios as base, enrich with registry timestamps
  const mergedScenarios = scenarios.map(s => {
    const reg = registryScenarios.find(r => r.scenario_id === s.scenario_id)
    return {
      ...s,
      last_run_at: reg?.last_run_at || s.run_completed_at,
      updated_at: reg?.updated_at || null,
    }
  })

  if (error) return <div className="error">Error: {error}</div>

  return (
    <div className="home-page">
      <h1>Scenarios</h1>

      {databases.length > 1 && (
        <div className="db-selector">
          <label>Project: </label>
          <select
            value={selectedDb || ''}
            onChange={e => handleDbChange(e.target.value)}
          >
            {databases.map(db => (
              <option key={db.name} value={db.name}>
                {db.name.replace(/\.duckdb$/, '')}
              </option>
            ))}
          </select>
        </div>
      )}

      {databases.length === 0 && !loading && (
        <p className="empty-state">
          No projects found. <Link to="/run">Run a simulation</Link> to get started.
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
              <th>Last Run</th>
              <th>Last Modified</th>
              <th>Runtime</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {mergedScenarios.map(s => (
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
                <td>{formatTimestamp(s.last_run_at)}</td>
                <td>{formatTimestamp(s.updated_at)}</td>
                <td>{s.wall_clock_seconds != null ? `${s.wall_clock_seconds}s` : '-'}</td>
                <td className="row-actions">
                  <button
                    className="icon-btn"
                    title="Run scenario"
                    disabled={actionInProgress === s.scenario_id}
                    onClick={() => handleRun(s.scenario_id)}
                  >
                    {actionInProgress === s.scenario_id ? '...' : '\u25B6'}
                  </button>
                  <button
                    className="icon-btn"
                    title="Duplicate scenario"
                    onClick={() => handleDuplicate(s.scenario_id, s.name)}
                  >
                    {'\u2398'}
                  </button>
                  <button
                    className="icon-btn icon-btn-danger"
                    title="Archive scenario"
                    onClick={() => handleArchive(s.scenario_id, s.name)}
                  >
                    {'\u2715'}
                  </button>
                </td>
              </tr>
            ))}
            {mergedScenarios.length === 0 && (
              <tr><td colSpan={7} className="empty-state">No scenarios in this project.</td></tr>
            )}
          </tbody>
        </table>
      )}
    </div>
  )
}
