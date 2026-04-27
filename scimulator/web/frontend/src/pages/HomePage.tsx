import { useEffect, useState, useCallback, useRef, useMemo } from 'react'
import { Link, useParams, useNavigate } from 'react-router-dom'
import {
  listScenarios, listRegistryScenarios,
  rerunScenario, duplicateScenario, archiveScenario,
  updateRegistryScenario,
} from '../api/client'
import type { ScenarioSummary, RegistryScenarioSummary } from '../api/client'

function formatTimestamp(ts: string | null): string {
  if (!ts) return '-'
  const d = new Date(ts)
  if (isNaN(d.getTime())) return ts
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

function parseTags(s: string | null | undefined): string[] {
  if (!s) return []
  return s.split(',').map(t => t.trim()).filter(Boolean)
}

type MergedScenario = {
  scenario_id: string
  name: string
  description: string
  start_date: string
  end_date: string
  currency_code: string
  time_resolution: string
  backorder_probability: number | null
  status: string | null
  total_steps: number | null
  wall_clock_seconds: number | null
  run_started_at: string | null
  run_completed_at: string | null
  last_run_at: string | null
  updated_at: string | null
  tags: string
}

type SortKey = 'scenario_id' | 'name' | 'period' | 'status' | 'tags' | 'last_run_at' | 'updated_at' | 'wall_clock_seconds'

export default function HomePage() {
  const navigate = useNavigate()
  const { dbName, projectId } = useParams<{ dbName: string; projectId: string }>()
  const [scenarios, setScenarios] = useState<ScenarioSummary[]>([])
  const [registryScenarios, setRegistryScenarios] = useState<RegistryScenarioSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [actionInProgress, setActionInProgress] = useState<string | null>(null)

  // Sorting
  const [sortKey, setSortKey] = useState<SortKey>('name')
  const [sortAsc, setSortAsc] = useState(true)

  // Tag filtering
  const [tagFilter, setTagFilter] = useState<string | null>(null)

  // Inline tag editing
  const [editingTagsId, setEditingTagsId] = useState<string | null>(null)
  const [tagInput, setTagInput] = useState('')
  const [editTags, setEditTags] = useState<string[]>([])
  const tagInputRef = useRef<HTMLInputElement>(null)

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

  // Tag editing
  function startEditingTags(s: MergedScenario) {
    setEditingTagsId(s.scenario_id)
    setEditTags(parseTags(s.tags))
    setTagInput('')
    setTimeout(() => tagInputRef.current?.focus(), 0)
  }

  function commitTagInput() {
    const tag = tagInput.trim()
    if (tag && !editTags.includes(tag)) {
      setEditTags(prev => [...prev, tag])
    }
    setTagInput('')
  }

  function removeEditTag(tag: string) {
    setEditTags(prev => prev.filter(t => t !== tag))
  }

  async function saveTags(scenarioId: string) {
    if (!projectId) return
    // Commit any pending input
    const finalTags = [...editTags]
    const pending = tagInput.trim()
    if (pending && !finalTags.includes(pending)) {
      finalTags.push(pending)
    }
    const tagsStr = finalTags.join(', ')
    setEditingTagsId(null)
    try {
      await updateRegistryScenario(projectId, scenarioId, { tags: tagsStr })
      refreshScenarios()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  // Merge: union of result DB and registry scenarios, excluding archived
  const registryIds = new Set(registryScenarios.map(r => r.scenario_id))
  const resultDbIds = new Set(scenarios.map(s => s.scenario_id))
  const mergedScenarios: MergedScenario[] = [
    ...scenarios
      .filter(s => registryIds.size === 0 || registryIds.has(s.scenario_id))
      .map(s => {
        const reg = registryScenarios.find(r => r.scenario_id === s.scenario_id)
        return {
          ...s,
          last_run_at: reg?.last_run_at || s.run_completed_at,
          updated_at: reg?.updated_at || null,
          tags: reg?.tags || '',
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
        tags: r.tags || '',
      })),
  ]

  // Collect all unique tags for the filter
  const allTags = useMemo(() => {
    const set = new Set<string>()
    for (const s of mergedScenarios) {
      for (const t of parseTags(s.tags)) set.add(t)
    }
    return Array.from(set).sort()
  }, [mergedScenarios])

  // Filter
  const filteredScenarios = tagFilter
    ? mergedScenarios.filter(s => parseTags(s.tags).includes(tagFilter))
    : mergedScenarios

  // Sort
  const sortedScenarios = useMemo(() => {
    const sorted = [...filteredScenarios]
    const dir = sortAsc ? 1 : -1
    sorted.sort((a, b) => {
      let va: string | number | null, vb: string | number | null
      switch (sortKey) {
        case 'scenario_id': va = a.scenario_id; vb = b.scenario_id; break
        case 'name': va = a.name; vb = b.name; break
        case 'period': va = a.start_date; vb = b.start_date; break
        case 'status': va = a.status || ''; vb = b.status || ''; break
        case 'tags': va = a.tags; vb = b.tags; break
        case 'last_run_at': va = a.last_run_at || ''; vb = b.last_run_at || ''; break
        case 'updated_at': va = a.updated_at || ''; vb = b.updated_at || ''; break
        case 'wall_clock_seconds': va = a.wall_clock_seconds; vb = b.wall_clock_seconds; break
        default: return 0
      }
      if (va == null && vb == null) return 0
      if (va == null) return 1
      if (vb == null) return -1
      if (va < vb) return -1 * dir
      if (va > vb) return 1 * dir
      return 0
    })
    return sorted
  }, [filteredScenarios, sortKey, sortAsc])

  function handleSort(key: SortKey) {
    if (sortKey === key) {
      setSortAsc(!sortAsc)
    } else {
      setSortKey(key)
      setSortAsc(true)
    }
  }

  function sortIndicator(key: SortKey) {
    if (sortKey !== key) return null
    return sortAsc ? ' \u25B2' : ' \u25BC'
  }

  return (
    <div className="home-page">
      <Link to="/" className="back-link">&larr; Projects</Link>
      <h1>Scenarios</h1>

      {error && <div className="error">Error: {error}</div>}

      {/* Tag filter */}
      {allTags.length > 0 && (
        <div className="tag-filter-bar">
          <span className="tag-filter-label">Filter by tag:</span>
          <button
            className={`tag-filter-btn${tagFilter === null ? ' active' : ''}`}
            onClick={() => setTagFilter(null)}
          >
            All
          </button>
          {allTags.map(t => (
            <button
              key={t}
              className={`tag-filter-btn${tagFilter === t ? ' active' : ''}`}
              onClick={() => setTagFilter(tagFilter === t ? null : t)}
            >
              {t}
            </button>
          ))}
        </div>
      )}

      {loading ? (
        <p>Loading...</p>
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th className="sortable-th" onClick={() => handleSort('scenario_id')}>ID{sortIndicator('scenario_id')}</th>
              <th className="sortable-th" onClick={() => handleSort('name')}>Scenario{sortIndicator('name')}</th>
              <th className="sortable-th" onClick={() => handleSort('period')}>Period{sortIndicator('period')}</th>
              <th className="sortable-th" onClick={() => handleSort('status')}>Status{sortIndicator('status')}</th>
              <th className="sortable-th" onClick={() => handleSort('tags')}>Tags{sortIndicator('tags')}</th>
              <th className="sortable-th" onClick={() => handleSort('last_run_at')}>Last Run{sortIndicator('last_run_at')}</th>
              <th className="sortable-th" onClick={() => handleSort('updated_at')}>Last Modified{sortIndicator('updated_at')}</th>
              <th className="sortable-th" onClick={() => handleSort('wall_clock_seconds')}>Runtime{sortIndicator('wall_clock_seconds')}</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {sortedScenarios.map(s => (
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
                <td style={{ whiteSpace: 'nowrap' }}>{s.start_date} to {s.end_date}</td>
                <td>
                  <span className={`status-badge status-${s.status || 'none'}`}>
                    {s.status || 'not run'}
                  </span>
                </td>
                <td className="tags-cell" onClick={() => { if (editingTagsId !== s.scenario_id) startEditingTags(s) }}>
                  {editingTagsId === s.scenario_id ? (
                    <div className="tags-editor">
                      <div className="tags-pills">
                        {editTags.map(t => (
                          <span key={t} className="tag-pill">
                            {t}
                            <button className="tag-pill-remove" onClick={e => { e.stopPropagation(); removeEditTag(t) }}>&times;</button>
                          </span>
                        ))}
                        <input
                          ref={tagInputRef}
                          className="tag-inline-input"
                          value={tagInput}
                          onChange={e => setTagInput(e.target.value)}
                          onKeyDown={e => {
                            if (e.key === ',' || e.key === 'Enter') {
                              e.preventDefault()
                              commitTagInput()
                            } else if (e.key === 'Backspace' && tagInput === '' && editTags.length > 0) {
                              setEditTags(prev => prev.slice(0, -1))
                            } else if (e.key === 'Escape') {
                              setEditingTagsId(null)
                            }
                          }}
                          onBlur={() => saveTags(s.scenario_id)}
                          placeholder={editTags.length === 0 ? 'Add tags...' : ''}
                          onClick={e => e.stopPropagation()}
                        />
                      </div>
                    </div>
                  ) : (
                    <div className="tags-pills">
                      {parseTags(s.tags).map(t => (
                        <span key={t} className="tag-pill">{t}</span>
                      ))}
                    </div>
                  )}
                </td>
                <td style={{ whiteSpace: 'nowrap' }}>{formatTimestamp(s.last_run_at)}</td>
                <td>{formatTimestamp(s.updated_at)}</td>
                <td>{s.wall_clock_seconds != null ? `${s.wall_clock_seconds}s` : '-'}</td>
                <td className="row-actions">
                  <div className="row-actions-inner">
                    <button
                      className="icon-btn"
                      title="Configure scenario"
                      onClick={() => navigate(`/scenario/${dbName}/${encodeURIComponent(s.scenario_id)}?tab=configure`)}
                    >
                      {'\u2699'}
                    </button>
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
                  </div>
                </td>
              </tr>
            ))}
            {sortedScenarios.length === 0 && (
              <tr><td colSpan={9} className="empty-state">
                {tagFilter ? `No scenarios with tag "${tagFilter}".` : 'No scenarios in this project.'}
              </td></tr>
            )}
          </tbody>
        </table>
      )}
    </div>
  )
}
