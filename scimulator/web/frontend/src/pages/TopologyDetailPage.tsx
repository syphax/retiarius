import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { listDatasets, deleteEntitySet, getTopologySchema } from '../api/client'
import type { TopologyInfo, EntitySetItem, TopologyColumn } from '../api/client'

const TABLE_LABELS: Record<string, string> = {
  product: 'Products',
  supply_node: 'Supply Nodes',
  distribution_node: 'Distribution Nodes',
  demand_node: 'Demand Nodes',
  customer: 'Customers',
  edge: 'Edges',
}

// Map topology table → entity set table. Customer has no entity set support.
const SET_TABLE_FOR: Record<string, string> = {
  product: 'product_set',
  supply_node: 'supply_node_set',
  distribution_node: 'distribution_node_set',
  demand_node: 'demand_node_set',
  edge: 'edge_set',
}

export default function TopologyDetailPage() {
  const { dbName, projectId, table } = useParams<{ dbName: string; projectId: string; table: string }>()
  const label = (table && TABLE_LABELS[table]) || table || 'Unknown'
  const setTable = table ? SET_TABLE_FOR[table] : undefined

  const [topology, setTopology] = useState<TopologyInfo[]>([])
  const [entitySets, setEntitySets] = useState<EntitySetItem[]>([])
  const [columns, setColumns] = useState<TopologyColumn[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  function refresh() {
    if (!dbName || !table) return
    setLoading(true)
    Promise.all([
      listDatasets(dbName).then(d => {
        setTopology(d.topology)
        setEntitySets(d.entity_sets)
      }),
      getTopologySchema(dbName, table)
        .then(s => setColumns(s.columns))
        .catch(() => setColumns([])),
    ])
      .then(() => setLoading(false))
      .catch(err => { setError(err.message); setLoading(false) })
  }

  useEffect(refresh, [dbName, table])

  async function handleDelete(s: EntitySetItem) {
    if (!dbName) return
    if (!confirm(`Delete entity set "${s.name}"?`)) return
    setError(null)
    try {
      await deleteEntitySet(dbName, s.set_table, s.set_id)
      refresh()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const datasetsTab = `/project/${encodeURIComponent(dbName ?? '')}/${encodeURIComponent(projectId ?? '')}/datasets`
  const topologyRow = topology.find(t => t.table === table)
  const userSets = entitySets.filter(s => s.set_type === label)
  const totalCount = topologyRow?.row_count ?? 0

  return (
    <div className="topology-detail-page">
      <Link to={datasetsTab} className="back-link">&larr; Datasets</Link>
      <h1>{label}</h1>
      <p className="datasets-subtitle">
        Database: <code>{dbName}</code>
      </p>

      {error && <div className="error">Error: {error}</div>}

      {loading ? (
        <p>Loading...</p>
      ) : (
        <>
          <section className="datasets-section">
            <h2>Entity Sets</h2>
            {setTable ? (
              <p>
                <Link to={`/datasets/${encodeURIComponent(dbName ?? '')}/entity-sets/create?type=${setTable}`}>
                  + Create new entity set
                </Link>
              </p>
            ) : (
              <p className="empty-state">Entity sets are not supported for this table.</p>
            )}
            <table className="data-table">
              <thead>
                <tr>
                  <th>Set ID</th>
                  <th>Name</th>
                  <th>Members</th>
                  <th>Used By</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td className="scenario-id-col">{'—'}</td>
                  <td><strong>All</strong> <span className="text-muted">(default)</span></td>
                  <td>{totalCount.toLocaleString()}</td>
                  <td className="text-muted">implicit default for scenarios with no explicit set</td>
                  <td className="row-actions"></td>
                </tr>
                {userSets.map(s => (
                  <tr key={s.set_id}>
                    <td className="scenario-id-col">{s.set_id}</td>
                    <td>{s.name}</td>
                    <td>{s.member_count.toLocaleString()}</td>
                    <td>
                      {s.scenarios.length === 0
                        ? <span className="text-muted">{'—'}</span>
                        : s.scenarios.map((sc, i) => (
                          <span key={sc.scenario_id}>{i > 0 && ', '}{sc.name}</span>
                        ))
                      }
                    </td>
                    <td className="row-actions">
                      <div className="row-actions-inner">
                        <button
                          className="icon-btn icon-btn-danger"
                          title={s.scenarios.length > 0 ? 'Cannot delete: used by scenario(s)' : 'Delete entity set'}
                          disabled={s.scenarios.length > 0}
                          onClick={() => handleDelete(s)}
                        >
                          {'✕'}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          <section className="datasets-section">
            <h2>Schema</h2>
            {columns.length === 0 ? (
              <p className="empty-state">No columns found.</p>
            ) : (
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Nullable</th>
                    <th>Key</th>
                  </tr>
                </thead>
                <tbody>
                  {columns.map(c => (
                    <tr key={c.name}>
                      <td><code>{c.name}</code></td>
                      <td className="text-muted">{c.type}</td>
                      <td className="text-muted">{c.nullable ? 'yes' : 'no'}</td>
                      <td className="text-muted">{c.key || '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>

          <section className="datasets-section">
            <h2>Summary stats &amp; visuals (placeholder)</h2>
            <p className="empty-state">
              Table-specific stats / visuals (e.g. a map for distribution nodes) will live here.
            </p>
          </section>
        </>
      )}
    </div>
  )
}
