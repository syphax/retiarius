import { useEffect, useState, useMemo } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import {
  listEntitySetTables, listEntityItems, createEntitySet,
} from '../api/client'
import type { EntitySetTableInfo } from '../api/client'

const PAGE_SIZES = [10, 20, 50, 100, 'All'] as const
type PageSize = number | 'All'

export default function EntitySetCreatePage() {
  const { dbName } = useParams<{ dbName: string }>()
  const navigate = useNavigate()

  const [tables, setTables] = useState<EntitySetTableInfo[]>([])
  const [selectedTable, setSelectedTable] = useState<string>('')
  const [columns, setColumns] = useState<string[]>([])
  const [rows, setRows] = useState<Record<string, string | null>[]>([])
  const [checked, setChecked] = useState<Set<string>>(new Set())
  const [loading, setLoading] = useState(true)
  const [loadingItems, setLoadingItems] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  const [name, setName] = useState('')
  const [description, setDescription] = useState('')

  const [pageSize, setPageSize] = useState<PageSize>(20)
  const [currentPage, setCurrentPage] = useState(1)

  // Load available tables
  useEffect(() => {
    if (!dbName) return
    setLoading(true)
    listEntitySetTables(dbName)
      .then(data => {
        setTables(data.tables)
        setLoading(false)
      })
      .catch(err => { setError(err.message); setLoading(false) })
  }, [dbName])

  // Load items when table is selected
  useEffect(() => {
    if (!dbName || !selectedTable) return
    setLoadingItems(true)
    setChecked(new Set())
    setCurrentPage(1)
    listEntityItems(dbName, selectedTable)
      .then(data => {
        setColumns(data.columns)
        setRows(data.rows)
        setLoadingItems(false)
      })
      .catch(err => { setError(err.message); setLoadingItems(false) })
  }, [dbName, selectedTable])

  const tableInfo = tables.find(t => t.set_table === selectedTable)
  const idColumn = tableInfo ? columns[0] : ''

  // Pagination
  const totalRows = rows.length
  const effectivePageSize = pageSize === 'All' ? totalRows : pageSize
  const totalPages = effectivePageSize > 0 ? Math.ceil(totalRows / effectivePageSize) : 1
  const paginatedRows = useMemo(() => {
    if (pageSize === 'All') return rows
    const start = (currentPage - 1) * (pageSize as number)
    return rows.slice(start, start + (pageSize as number))
  }, [rows, currentPage, pageSize])

  function toggleItem(id: string) {
    setChecked(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function toggleAll() {
    if (checked.size === rows.length) {
      setChecked(new Set())
    } else {
      setChecked(new Set(rows.map(r => r[idColumn]!)))
    }
  }

  function togglePage() {
    const pageIds = paginatedRows.map(r => r[idColumn]!)
    const allPageChecked = pageIds.every(id => checked.has(id))
    setChecked(prev => {
      const next = new Set(prev)
      for (const id of pageIds) {
        if (allPageChecked) next.delete(id)
        else next.add(id)
      }
      return next
    })
  }

  function exportCsv() {
    if (!columns.length || !rows.length) return
    const header = ['selected', ...columns].join(',')
    const csvRows = rows.map(r => {
      const sel = checked.has(r[idColumn]!) ? '1' : '0'
      const vals = columns.map(c => {
        const v = r[c] ?? ''
        return v.includes(',') || v.includes('"') || v.includes('\n')
          ? `"${v.replace(/"/g, '""')}"`
          : v
      })
      return [sel, ...vals].join(',')
    })
    const csv = [header, ...csvRows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${tableInfo?.source_table ?? 'entity'}_items.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  function importCsv(file: File) {
    const reader = new FileReader()
    reader.onload = () => {
      const text = reader.result as string
      const lines = text.split('\n').filter(l => l.trim())
      if (lines.length < 2) return
      // Parse header to find the 'selected' column (should be first) and the id column
      const headerCols = lines[0].split(',')
      const selIdx = headerCols.indexOf('selected')
      const idIdx = headerCols.indexOf(idColumn)
      if (selIdx < 0 || idIdx < 0) {
        setError(`CSV must have 'selected' and '${idColumn}' columns`)
        return
      }
      const newChecked = new Set<string>()
      for (let i = 1; i < lines.length; i++) {
        const vals = lines[i].split(',')
        if (vals[selIdx]?.trim() === '1') {
          const id = vals[idIdx]?.trim().replace(/^"|"$/g, '')
          if (id) newChecked.add(id)
        }
      }
      setChecked(newChecked)
    }
    reader.readAsText(file)
  }

  async function handleCreate() {
    if (!dbName || !selectedTable || !name.trim()) return
    setSaving(true)
    setError(null)
    try {
      await createEntitySet(dbName, selectedTable, name.trim(), description.trim(), Array.from(checked))
      navigate(`/datasets/${dbName}`)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      setSaving(false)
    }
  }

  if (!dbName) return <div className="error">No database specified.</div>

  return (
    <div className="entity-set-create-page">
      <Link to={`/datasets/${dbName}`} className="back-link">&larr; Datasets</Link>
      <h1>Create Entity Set</h1>

      {error && <div className="error">Error: {error}</div>}

      {loading ? (
        <p>Loading...</p>
      ) : (
        <>
          {/* Table selector */}
          <div className="form-group">
            <label>Entity type</label>
            <select
              value={selectedTable}
              onChange={e => setSelectedTable(e.target.value)}
              className="form-select"
            >
              <option value="">Select a table...</option>
              {tables.map(t => (
                <option key={t.set_table} value={t.set_table}>
                  {t.label} ({t.row_count.toLocaleString()} items)
                </option>
              ))}
            </select>
          </div>

          {selectedTable && (
            <>
              {/* Name & description */}
              <div className="form-row">
                <div className="form-group" style={{ flex: 1 }}>
                  <label>Set name</label>
                  <input
                    className="form-input"
                    value={name}
                    onChange={e => setName(e.target.value)}
                    placeholder="e.g. West Coast DCs"
                  />
                </div>
                <div className="form-group" style={{ flex: 2 }}>
                  <label>Description</label>
                  <input
                    className="form-input"
                    value={description}
                    onChange={e => setDescription(e.target.value)}
                    placeholder="Optional description"
                  />
                </div>
              </div>

              {loadingItems ? (
                <p>Loading items...</p>
              ) : (
                <>
                  {/* Toolbar */}
                  <div className="entity-set-toolbar">
                    <span className="entity-set-count">
                      {checked.size} of {totalRows} selected
                    </span>

                    <span className="entity-set-toolbar-sep" />

                    <label className="entity-set-page-size-label">Show</label>
                    <select
                      className="form-select form-select-sm"
                      value={String(pageSize)}
                      onChange={e => {
                        const v = e.target.value
                        setPageSize(v === 'All' ? 'All' : Number(v))
                        setCurrentPage(1)
                      }}
                    >
                      {PAGE_SIZES.map(s => (
                        <option key={s} value={String(s)}>{s}</option>
                      ))}
                    </select>

                    <span className="entity-set-toolbar-sep" />

                    <button className="btn btn-sm" onClick={exportCsv}>Export CSV</button>
                    <label className="btn btn-sm">
                      Import CSV
                      <input
                        type="file"
                        accept=".csv"
                        style={{ display: 'none' }}
                        onChange={e => { if (e.target.files?.[0]) importCsv(e.target.files[0]); e.target.value = '' }}
                      />
                    </label>
                  </div>

                  {/* Items table */}
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th style={{ width: 36 }}>
                          <input
                            type="checkbox"
                            checked={rows.length > 0 && checked.size === rows.length}
                            ref={el => { if (el) el.indeterminate = checked.size > 0 && checked.size < rows.length }}
                            onChange={toggleAll}
                            title="Select / deselect all"
                          />
                        </th>
                        {columns.map(c => (
                          <th key={c}>{c}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {paginatedRows.map(r => {
                        const id = r[idColumn]!
                        return (
                          <tr key={id} onClick={() => toggleItem(id)} style={{ cursor: 'pointer' }}>
                            <td>
                              <input
                                type="checkbox"
                                checked={checked.has(id)}
                                onChange={() => toggleItem(id)}
                                onClick={e => e.stopPropagation()}
                              />
                            </td>
                            {columns.map(c => (
                              <td key={c}>{r[c] ?? ''}</td>
                            ))}
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>

                  {/* Pagination */}
                  {pageSize !== 'All' && totalPages > 1 && (
                    <div className="entity-set-pagination">
                      <button
                        className="btn btn-sm"
                        disabled={currentPage <= 1}
                        onClick={() => setCurrentPage(p => p - 1)}
                      >
                        &larr; Prev
                      </button>
                      <span>
                        Page {currentPage} of {totalPages}
                      </span>
                      <button className="btn btn-sm" onClick={togglePage}>
                        {paginatedRows.every(r => checked.has(r[idColumn]!))
                          ? 'Deselect page'
                          : 'Select page'}
                      </button>
                      <button
                        className="btn btn-sm"
                        disabled={currentPage >= totalPages}
                        onClick={() => setCurrentPage(p => p + 1)}
                      >
                        Next &rarr;
                      </button>
                    </div>
                  )}

                  {/* Create button */}
                  <div className="entity-set-actions">
                    <button
                      className="btn btn-primary"
                      disabled={saving || !name.trim() || checked.size === 0}
                      onClick={handleCreate}
                    >
                      {saving ? 'Creating...' : `Create set (${checked.size} members)`}
                    </button>
                    <Link to={`/datasets/${dbName}`} className="btn">Cancel</Link>
                  </div>
                </>
              )}
            </>
          )}
        </>
      )}
    </div>
  )
}
