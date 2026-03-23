import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { runScenario } from '../api/client'

export default function RunPage() {
  const navigate = useNavigate()
  const [scenarioFile, setScenarioFile] = useState<File | null>(null)
  const [demandFile, setDemandFile] = useState<File | null>(null)
  const [dbName, setDbName] = useState('')
  const [replace, setReplace] = useState(false)
  const [running, setRunning] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!scenarioFile) return

    setRunning(true)
    setError(null)

    try {
      const result = await runScenario(
        scenarioFile,
        demandFile || undefined,
        dbName || undefined,
        replace,
      )
      navigate(`/scenario/${result.database}/${result.scenario_id}`)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      setRunning(false)
    }
  }

  return (
    <div className="run-page">
      <h1>Run Simulation</h1>

      <form onSubmit={handleSubmit} className="run-form">
        <div className="form-group">
          <label htmlFor="scenario">Scenario YAML *</label>
          <input
            id="scenario"
            type="file"
            accept=".yaml,.yml"
            onChange={e => setScenarioFile(e.target.files?.[0] ?? null)}
            required
          />
        </div>

        <div className="form-group">
          <label htmlFor="demand">Demand CSV (optional — uses path in YAML if not provided)</label>
          <input
            id="demand"
            type="file"
            accept=".csv"
            onChange={e => setDemandFile(e.target.files?.[0] ?? null)}
          />
        </div>

        <div className="form-group">
          <label htmlFor="dbname">Database name (optional)</label>
          <input
            id="dbname"
            type="text"
            placeholder="scenario_id.duckdb"
            value={dbName}
            onChange={e => setDbName(e.target.value)}
          />
        </div>

        <div className="form-group checkbox">
          <label>
            <input
              type="checkbox"
              checked={replace}
              onChange={e => setReplace(e.target.checked)}
            />
            Replace existing results
          </label>
        </div>

        {error && <div className="error">{error}</div>}

        <button type="submit" disabled={!scenarioFile || running} className="btn-primary">
          {running ? 'Running...' : 'Run Simulation'}
        </button>
      </form>
    </div>
  )
}
