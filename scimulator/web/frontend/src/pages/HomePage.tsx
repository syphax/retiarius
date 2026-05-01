import { useEffect, useState, useCallback, useRef, useMemo } from 'react'
import { Link, useParams, useNavigate } from 'react-router-dom'
import {
  listScenarios, listRegistryScenarios,
  rerunScenario, duplicateScenario, archiveScenario,
  updateRegistryScenario,
  listDatasets, getInventorySummary,
} from '../api/client'
import type { ScenarioSummary, RegistryScenarioSummary, DatasetInfo, TopologyInfo, EntitySetItem, InventoryVersionSummary } from '../api/client'

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
  created_at: string | null
  updated_at: string | null
  tags: string
}

type SortKey = 'scenario_id' | 'name' | 'period' | 'status' | 'tags' | 'last_run_at' | 'created_at' | 'updated_at' | 'wall_clock_seconds'

export default function HomePage() {
  const navigate = useNavigate()
  const { dbName, projectId, tab } = useParams<{ dbName: string; projectId: string; tab?: string }>()
  const [scenarios, setScenarios] = useState<ScenarioSummary[]>([])
  const [registryScenarios, setRegistryScenarios] = useState<RegistryScenarioSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [actionInProgress, setActionInProgress] = useState<string | null>(null)

  // Project-level tab — driven by URL
  const activeProjectTab: 'scenarios' | 'datasets' = tab === 'datasets' ? 'datasets' : 'scenarios'
  const setActiveProjectTab = useCallback((next: 'scenarios' | 'datasets') => {
    if (!dbName || !projectId) return
    navigate(`/project/${encodeURIComponent(dbName)}/${encodeURIComponent(projectId)}/${next}`)
  }, [dbName, projectId, navigate])

  // Datasets state
  const [datasets, setDatasets] = useState<DatasetInfo[]>([])
  const [topology, setTopology] = useState<TopologyInfo[]>([])
  const [entitySets, setEntitySets] = useState<EntitySetItem[]>([])
  const [inventoryVersions, setInventoryVersions] = useState<InventoryVersionSummary[]>([])
  const [datasetsLoading, setDatasetsLoading] = useState(false)

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

  const refreshDatasets = useCallback(() => {
    if (!dbName) return
    setDatasetsLoading(true)
    Promise.all([
      listDatasets(dbName).then(data => {
        setDatasets(data.dataset_versions)
        setTopology(data.topology)
        setEntitySets(data.entity_sets)
      }),
      getInventorySummary(dbName).then(d => setInventoryVersions(d.dataset_versions)),
    ])
      .then(() => setDatasetsLoading(false))
      .catch(err => {
        setError(err.message)
        setDatasetsLoading(false)
      })
  }, [dbName])

  useEffect(() => {
    if (activeProjectTab === 'datasets') refreshDatasets()
  }, [activeProjectTab, refreshDatasets])

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
          status: reg?.status || s.status,
          last_run_at: reg?.last_run_at || s.run_completed_at,
          created_at: reg?.created_at || null,
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
        created_at: r.created_at,
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
        case 'created_at': va = a.created_at || ''; vb = b.created_at || ''; break
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

  // Group entity sets by type
  const setsByType: Record<string, EntitySetItem[]> = {}
  for (const s of entitySets) {
    ;(setsByType[s.set_type] ??= []).push(s)
  }

  const demandVersions = datasets.filter(d => (d.row_counts['demand'] ?? 0) > 0)
  const inventoryPresent = inventoryVersions.filter(v => v.row_count > 0)
  const inboundPresent = datasets.filter(d => (d.row_counts['inbound_schedule'] ?? 0) > 0)
  const projectBase = `/project/${encodeURIComponent(dbName ?? '')}/${encodeURIComponent(projectId ?? '')}`
  const demandDetailUrl = `${projectBase}/data/demand`
  const inventoryDetailUrl = `${projectBase}/data/initial_inventory`
  const inboundDetailUrl = `${projectBase}/data/inbound_schedule`

  return (
    <div className="home-page">
      <Link to="/" className="back-link">&larr; Projects</Link>

      <div className="tab-bar" style={{ marginBottom: 16 }}>
        <button
          className={`tab ${activeProjectTab === 'scenarios' ? 'active' : ''}`}
          onClick={() => setActiveProjectTab('scenarios')}
        >
          Scenarios
        </button>
        <button
          className={`tab ${activeProjectTab === 'datasets' ? 'active' : ''}`}
          onClick={() => setActiveProjectTab('datasets')}
        >
          Datasets
        </button>
      </div>

      {error && <div className="error">Error: {error}</div>}

      {activeProjectTab === 'scenarios' && (
        <>
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
              <th className="sortable-th" onClick={() => handleSort('created_at')}>Created{sortIndicator('created_at')}</th>
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
                <td>{formatTimestamp(s.created_at)}</td>
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
        </>
      )}

      {activeProjectTab === 'datasets' && (
        datasetsLoading ? (
          <p>Loading datasets...</p>
        ) : (
          <>
            {/* Network Topology */}
            <section className="datasets-section">
              <h2>Network Topology</h2>
              <p className="datasets-subtitle">
                These tables define the ingredients of the distribution network: who are the customers, what are the products, what are the network nodes, and how are they connected?
              </p>
              <table className="data-table">
                <thead>
                  <tr><th>Table</th><th>Rows</th><th>Entities</th></tr>
                </thead>
                <tbody>
                  {topology.map(t => {
                    const userSets = setsByType[t.label]?.length ?? 0
                    const entityCount = userSets + 1 // +1 for virtual "All"
                    return (
                      <tr key={t.table}>
                        <td>
                          <Link to={`/project/${encodeURIComponent(dbName!)}/${encodeURIComponent(projectId!)}/topology/${t.table}`}>
                            {t.label}
                          </Link>
                        </td>
                        <td>{t.row_count.toLocaleString()}</td>
                        <td>{entityCount.toLocaleString()}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </section>

            {/* Demand */}
            <section className="datasets-section">
              <h2>
                <Link to={demandDetailUrl}>Demand Data</Link>
              </h2>
              {demandVersions.length === 0 ? (
                <p className="empty-state">No datasets with demand data.</p>
              ) : (
                <table className="data-table">
                  <thead>
                    {/* TODO(value): add Total Value, Total Qty columns once product-version pricing is wired up */}
                    <tr><th>Dataset Version</th><th>Name</th><th>Rows</th><th>Used By</th><th>Created</th></tr>
                  </thead>
                  <tbody>
                    {demandVersions.map(d => (
                      <tr key={d.dataset_version_id}>
                        <td className="scenario-id-col">{d.dataset_version_id}</td>
                        <td>{d.name}</td>
                        <td>{(d.row_counts['demand'] ?? 0).toLocaleString()}</td>
                        <td>
                          {d.scenarios.length === 0
                            ? <span className="text-muted">{'—'}</span>
                            : d.scenarios.map((s, i) => (
                              <span key={s.scenario_id}>{i > 0 && ', '}{s.name}</span>
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

            {/* Initial Conditions */}
            <section className="datasets-section">
              <h2>Initial Conditions</h2>

              <div className="datasets-subsection">
                <h3>
                  <Link to={inventoryDetailUrl}>Initial Inventory Data</Link>
                </h3>
                {inventoryPresent.length === 0 ? (
                  <p className="empty-state">No datasets with initial inventory data.</p>
                ) : (
                  <table className="data-table">
                    <thead>
                      {/* TODO(value): add Value (at cost) column once product-version pricing is wired up */}
                      <tr>
                        <th>Dataset Version</th>
                        <th>Name</th>
                        <th>Rows</th>
                        <th># Nodes</th>
                        <th># Products</th>
                        <th>Used By</th>
                        <th>Created</th>
                      </tr>
                    </thead>
                    <tbody>
                      {inventoryPresent.map(v => (
                        <tr key={v.dataset_version_id}>
                          <td className="scenario-id-col">{v.dataset_version_id}</td>
                          <td>{v.name}</td>
                          <td>{v.row_count.toLocaleString()}</td>
                          <td>{v.node_count.toLocaleString()}</td>
                          <td>{v.product_count.toLocaleString()}</td>
                          <td>
                            {v.scenarios.length === 0
                              ? <span className="text-muted">{'\u2014'}</span>
                              : v.scenarios.map((s, i) => (
                                <span key={s.scenario_id}>{i > 0 && ', '}{s.name}</span>
                              ))
                            }
                          </td>
                          <td className="text-muted">
                            {v.created_at ? new Date(v.created_at).toLocaleDateString() : '\u2014'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>

              <div className="datasets-subsection">
                <h3>
                  <Link to={inboundDetailUrl}>Inbound Schedule Data</Link>
                </h3>
                {inboundPresent.length === 0 ? (
                  <p className="empty-state">No datasets with inbound schedule data.</p>
                ) : (
                  <table className="data-table">
                    <thead>
                      <tr><th>Dataset Version</th><th>Name</th><th>Rows</th><th>Used By</th><th>Created</th></tr>
                    </thead>
                    <tbody>
                      {inboundPresent.map(d => (
                        <tr key={d.dataset_version_id}>
                          <td className="scenario-id-col">{d.dataset_version_id}</td>
                          <td>{d.name}</td>
                          <td>{(d.row_counts['inbound_schedule'] ?? 0).toLocaleString()}</td>
                          <td>
                            {d.scenarios.length === 0
                              ? <span className="text-muted">{'\u2014'}</span>
                              : d.scenarios.map((s, i) => (
                                <span key={s.scenario_id}>{i > 0 && ', '}{s.name}</span>
                              ))
                            }
                          </td>
                          <td className="text-muted">
                            {d.created_at ? new Date(d.created_at).toLocaleDateString() : '\u2014'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </section>
          </>
        )
      )}
    </div>
  )
}
