import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  listProjects, updateProject, cloneProject, archiveProject,
} from '../api/client'
import type { ProjectSummary } from '../api/client'

function formatTimestamp(ts: string | null): string {
  if (!ts) return '-'
  const d = new Date(ts)
  if (isNaN(d.getTime())) return ts
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

export default function ProjectsPage() {
  const [projects, setProjects] = useState<ProjectSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [actionInProgress, setActionInProgress] = useState<string | null>(null)

  // Inline editing
  const [editingId, setEditingId] = useState<string | null>(null)
  const [editName, setEditName] = useState('')
  const [editDesc, setEditDesc] = useState('')

  function refresh() {
    setLoading(true)
    listProjects()
      .then(p => { setProjects(p); setLoading(false) })
      .catch(err => { setError(err.message); setLoading(false) })
  }

  useEffect(() => { refresh() }, [])

  function startEditing(p: ProjectSummary) {
    setEditingId(p.project_id)
    setEditName(p.name)
    setEditDesc(p.description || '')
  }

  async function saveEdits(projectId: string) {
    const orig = projects.find(p => p.project_id === projectId)
    const fields: { name?: string; description?: string } = {}
    if (editName.trim() && editName.trim() !== orig?.name) fields.name = editName.trim()
    if (editDesc.trim() !== (orig?.description || '')) fields.description = editDesc.trim()
    if (Object.keys(fields).length > 0) {
      try {
        await updateProject(projectId, fields)
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err))
      }
    }
    setEditingId(null)
    refresh()
  }

  async function handleClone(projectId: string, name: string) {
    const newName = prompt('Name for the new project:', `${name} (copy)`)
    if (!newName) return
    setActionInProgress(`clone-${projectId}`)
    setError(null)
    try {
      await cloneProject(projectId, newName)
      refresh()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setActionInProgress(null)
    }
  }

  async function handleArchive(projectId: string, name: string) {
    if (!confirm(`Archive "${name}"? It will be hidden from this list.`)) return
    setError(null)
    try {
      await archiveProject(projectId)
      refresh()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div className="projects-page">
      <h1>Projects</h1>

      {error && <div className="error">Error: {error}</div>}

      {loading ? (
        <p>Loading...</p>
      ) : projects.length === 0 ? (
        <p className="empty-state">
          No projects found. <Link to="/run">Run a simulation</Link> to get started.
        </p>
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>Project</th>
              <th>Scenarios</th>
              <th>Last Modified</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {projects.map(p => (
              <tr key={p.project_id}>
                <td>
                  {editingId === p.project_id ? (
                    <>
                      <div className="inline-edit">
                        <input
                          className="inline-edit-input"
                          value={editName}
                          onChange={e => setEditName(e.target.value)}
                          onKeyDown={e => { if (e.key === 'Enter') saveEdits(p.project_id); if (e.key === 'Escape') setEditingId(null) }}
                          autoFocus
                          style={{ fontWeight: 700 }}
                        />
                      </div>
                      <div className="inline-edit" style={{ marginTop: 4 }}>
                        <textarea
                          className="inline-edit-input inline-edit-desc"
                          value={editDesc}
                          onChange={e => setEditDesc(e.target.value)}
                          onKeyDown={e => { if (e.key === 'Escape') setEditingId(null) }}
                          placeholder="Add a description..."
                          rows={2}
                        />
                      </div>
                      <div className="inline-edit-actions" style={{ marginTop: 4 }}>
                        <button className="inline-edit-btn" onClick={() => saveEdits(p.project_id)}>Save</button>
                        <button className="inline-edit-btn" onClick={() => setEditingId(null)}>Cancel</button>
                      </div>
                    </>
                  ) : (
                    <>
                      <Link to={`/project/${p.database}/${p.project_id}`}>
                        <strong>{p.name}</strong>
                      </Link>
                      {p.description && (
                        <div className="scenario-description" style={{ marginTop: 2 }}>{p.description}</div>
                      )}
                    </>
                  )}
                </td>
                <td>{p.scenario_count}</td>
                <td>{formatTimestamp(p.updated_at)}</td>
                <td className="row-actions">
                  <button
                    className="icon-btn"
                    title="Edit project"
                    onClick={() => startEditing(p)}
                  >
                    {'\u270E'}
                  </button>
                  <button
                    className="icon-btn"
                    title="Duplicate project"
                    disabled={actionInProgress === `clone-${p.project_id}`}
                    onClick={() => handleClone(p.project_id, p.name)}
                  >
                    {'\u2398'}
                  </button>
                  <button
                    className="icon-btn icon-btn-danger"
                    title="Archive project"
                    onClick={() => handleArchive(p.project_id, p.name)}
                  >
                    {'\u2715'}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}
