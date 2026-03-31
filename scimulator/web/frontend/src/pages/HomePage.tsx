import { useEffect, useState, useCallback } from 'react'
import { Link, useParams } from 'react-router-dom'
import {
  listScenarios, listRegistryScenarios,
  rerunScenario, duplicateScenario, archiveScenario,
} from '../api/client'
import type { ScenarioSummary, RegistryScenarioSummary } from '../api/client'

function formatTimestamp(ts: string | null): string {
  if (!ts) return '-'
  const d = new Date(ts)
  if (isNaN(d.getTime())) return ts
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

export default function HomePage() {
  const { dbName, projectId } = useParams<{ dbName: string; projectId: string }>()
  const [scenarios, setScenarios] = useState<ScenarioSummary[]>([])
  const [registryScenarios, setRegistryScenarios] = useState<RegistryScenarioSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [actionInProgress, setActionInProgress] = useState<string | null>(null)

  const refreshScenarios = useCallback(() => {
    if (!dbName || !projectId) return
    setLoading(true)

    Promise.all([
      listScenarios(dbName).then(s => setScenarios(s)),
      listRegistryScenarios(projectId)
        .then(s => setRegistryScenarios(s))
        .catch(() => setRegistryScenarios([])),
    ])
      .then(() => setLoading(false))
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [dbName, projectId])

  useEffect(() => { refreshScenarios() }, [refreshScenarios])

  async function handleRun(scenarioId: string) {
    if (!dbName) return
    setActionInProgress(scenarioId)
    setError(null)
    try {
      await rerunScenario(dbName, scenarioId)
      refreshScenarios()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setActionInProgress(null)
    }
  }

  async function handleDuplicate(scenarioId: string) {
    if (!projectId) return
    setActionInProgress(`dup-${scenarioId}`)
    setError(null)
    try {
      await duplicateScenario(projectId, scenarioId)
      refreshScenarios()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setActionInProgress(null)
    }
  }

  async function handleArchive(scenarioId: string, name: string) {
    if (!projectId) return
    if (!confirm(`Archive "${name}"? It will be hidden from this list.`)) return
    setError(null)
    try {
      await archiveScenario(projectId, scenarioId)
      refreshScenarios()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  // Merge: union of result DB and registry scenarios, excluding archived
  const registryIds = new Set(registryScenarios.map(r => r.scenario_id))
  const resultDbIds = new Set(scenarios.map(s => s.scenario_id))
  const mergedScenarios = [
    ...scenarios
      .filter(s => registryIds.size === 0 || registryIds.has(s.scenario_id))
      .map(s => {
        const reg = registryScenarios.find(r => r.scenario_id === s.scenario_id)
        return {
          ...s,
          last_run_at: reg?.last_run_at || s.run_completed_at,
          updated_at: reg?.updated_at || null,
        }
      }),
    ...registryScenarios
      .filter(r => !resultDbIds.has(r.scenario_id))
      .map(r => ({
        scenario_id: r.scenario_id,
        name: r.name,
        description: r.description,
        start_date: r.start_date || '',
        end_date: r.end_date || '',
        currency_code: r.currency_code,
        time_resolution: r.time_resolution,
        backorder_probability: r.backorder_probability,
        status: r.status,
        total_steps: null as number | null,
        wall_clock_seconds: r.run_wall_clock_seconds,
        run_started_at: null as string | null,
        run_completed_at: null as string | null,
        last_run_at: r.last_run_at,
        updated_at: r.updated_at,
      })),
  ]

  return (
    <div className="home-page">
      <Link to="/" className="back-link">&larr; Projects</Link>
      <h1>Scenarios</h1>

      {error && <div className="error">Error: {error}</div>}

      {loading ? (
        <p>Loading...</p>
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>ID</th>
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
                <td className="scenario-id-col">
                  <Link to={`/scenario/${dbName}/${s.scenario_id}`}>
                    {s.scenario_id.toUpperCase()}
                  </Link>
                </td>
                <td>
                  <Link to={`/scenario/${dbName}/${s.scenario_id}`}>
                    {s.name}
                  </Link>
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
                    disabled={actionInProgress === `dup-${s.scenario_id}`}
                    onClick={() => handleDuplicate(s.scenario_id)}
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
              <tr><td colSpan={8} className="empty-state">No scenarios in this project.</td></tr>
            )}
          </tbody>
        </table>
      )}
    </div>
  )
}
