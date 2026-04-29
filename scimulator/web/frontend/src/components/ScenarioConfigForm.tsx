import { useCallback, useEffect, useRef, useState } from 'react'
import {
  getScenarioConfig,
  updateScenarioConfig,
  saveScenarioAs,
  exportScenarioYamlUrl,
  duplicateScenario,
} from '../api/client'
import type { DatasetVersionInfo, EntitySetInfo } from '../api/client'
import { useNavigate, useParams } from 'react-router-dom'
import HELP_TEXT from '../data/helpText'

// ── Field definitions ────────────────────────────────────────────────

interface FieldDef {
  key: string
  label: string
  type: 'text' | 'number' | 'date' | 'select' | 'checkbox' | 'textarea' | 'percent' | 'slider'
  options?: { value: string; label: string }[]
  readOnly?: boolean
  placeholder?: string
  step?: string
  min?: number
  max?: number
  fullWidth?: boolean  // spans entire grid row
}

// General: scenario_id, name, description, start_date/end_date (own row), time_resolution, then rest
const GENERAL_FIELDS: FieldDef[] = [
  { key: 'scenario_id', label: 'Scenario ID', type: 'text', readOnly: true },
  { key: 'name', label: 'Name', type: 'text' },
  { key: 'description', label: 'Description', type: 'textarea', fullWidth: true },
  { key: 'start_date', label: 'Start Date', type: 'date' },
  { key: 'end_date', label: 'End Date', type: 'date' },
  { key: 'time_resolution', label: 'Time Resolution', type: 'select', options: [
    { value: 'daily', label: 'Daily' },
    { value: 'weekly', label: 'Weekly' },
  ]},
  { key: 'currency_code', label: 'Currency Code', type: 'text', placeholder: 'USD' },
  { key: 'warm_up_days', label: 'Warm-up Days', type: 'number', min: 0 },
  { key: 'write_event_log', label: 'Write Event Log', type: 'checkbox' },
  { key: 'write_snapshots', label: 'Write Snapshots', type: 'checkbox' },
  { key: 'snapshot_interval_days', label: 'Snapshot Interval (days)', type: 'number', min: 1 },
]

const DATASET_KEYS = [
  { key: 'dataset_version_id', label: 'Default Dataset Version', table: null },
  { key: 'demand_version_id', label: 'Demand', table: 'demand' },
  { key: 'inbound_version_id', label: 'Inbound Schedule', table: 'inbound_schedule' },
  { key: 'inventory_version_id', label: 'Initial Inventory', table: 'initial_inventory' },
]

const ENTITY_SET_FIELDS = [
  { key: 'product_set_id', label: 'Product Set' },
  { key: 'supply_node_set_id', label: 'Supply Node Set' },
  { key: 'distribution_node_set_id', label: 'Distribution Node Set' },
  { key: 'demand_node_set_id', label: 'Demand Node Set' },
  { key: 'edge_set_id', label: 'Edge Set' },
]

// Fulfillment: logic selector on own row, then backorder probability slider
const FULFILLMENT_METHOD: FieldDef = {
  key: 'fulfillment_logic', label: 'Fulfillment Logic', type: 'select', fullWidth: true, options: [
    { value: 'closest_node_wins', label: 'Closest Node Wins' },
    { value: 'closest_node_only', label: 'Closest Node Only' },
  ],
}

const FULFILLMENT_PARAMS: FieldDef[] = [
  { key: 'backorder_probability', label: 'Backorder Probability', type: 'slider', min: 0, max: 100, step: '1' },
]

// Ordering: trigger selector on own row, then params (shown only when trigger is set)
const ORDERING_METHOD: FieldDef = {
  key: 'reorder_logic', label: 'Reorder Trigger', type: 'select', fullWidth: true, options: [
    { value: '', label: 'None (drawdown only)' },
    { value: 'periodic', label: 'Periodic' },
  ],
}

const ORDERING_PARAMS: FieldDef[] = [
  { key: 'reorder_resolution', label: 'Reorder Resolution', type: 'select', options: [
    { value: '', label: 'None' },
    { value: 'national', label: 'National' },
    { value: 'node', label: 'Node' },
  ]},
  { key: 'reorder_allocation', label: 'Reorder Allocation', type: 'select', options: [
    { value: 'fair_share', label: 'Fair Share' },
  ]},
  { key: 'order_frequency_days', label: 'Order Frequency (days)', type: 'number', min: 1 },
  { key: 'safety_stock_days', label: 'Safety Stock (days)', type: 'number', min: 0 },
  { key: 'mrq_days', label: 'MRQ (days)', type: 'number', min: 1 },
  { key: 'consolidation_mode', label: 'Consolidation Mode', type: 'select', options: [
    { value: 'free', label: 'Free' },
  ]},
]

// Forecasting: method selector on own row, then params (shown only when method is set)
const FORECAST_METHOD: FieldDef = {
  key: 'forecast_method', label: 'Forecast Method', type: 'select', fullWidth: true, options: [
    { value: '', label: 'None' },
    { value: 'noisy_actuals', label: 'Noisy Actuals' },
  ],
}

const FORECAST_PARAMS: FieldDef[] = [
  { key: 'forecast_bias', label: 'Forecast Bias', type: 'percent', step: '1', min: -100, max: 100 },
  { key: 'forecast_error', label: 'Forecast Error', type: 'percent', step: '1', min: 0, max: 100 },
  { key: 'forecast_distribution', label: 'Forecast Distribution', type: 'select', options: [
    { value: 'normal', label: 'Normal' },
    { value: 'lognormal', label: 'Log-Normal' },
    { value: 'poisson', label: 'Poisson' },
  ]},
]

// ── Validation ───────────────────────────────────────────────────────

interface ValidationError {
  field: string
  message: string
}

function validate(values: Record<string, unknown>): ValidationError[] {
  const errors: ValidationError[] = []
  const s = (k: string) => String(values[k] ?? '')
  const n = (k: string) => Number(values[k])

  if (!s('name').trim()) errors.push({ field: 'name', message: 'Name is required' })
  if (s('start_date') && s('end_date') && s('start_date') >= s('end_date'))
    errors.push({ field: 'end_date', message: 'End date must be after start date' })
  if (n('warm_up_days') < 0) errors.push({ field: 'warm_up_days', message: 'Must be >= 0' })
  const bp = n('backorder_probability')
  if (bp < 0 || bp > 1) errors.push({ field: 'backorder_probability', message: 'Must be between 0% and 100%' })
  if (values['write_snapshots'] && n('snapshot_interval_days') < 1)
    errors.push({ field: 'snapshot_interval_days', message: 'Must be >= 1' })

  const reorder = s('reorder_logic')
  if (reorder && reorder !== '') {
    if (n('order_frequency_days') < 1) errors.push({ field: 'order_frequency_days', message: 'Must be >= 1' })
    if (n('safety_stock_days') < 0) errors.push({ field: 'safety_stock_days', message: 'Must be >= 0' })
  }

  const fb = n('forecast_bias')
  if (fb < -1 || fb > 1) errors.push({ field: 'forecast_bias', message: 'Must be between -100% and +100%' })
  const fe = n('forecast_error')
  if (fe < 0 || fe > 1) errors.push({ field: 'forecast_error', message: 'Must be between 0% and 100%' })

  if (!s('dataset_version_id')) errors.push({ field: 'dataset_version_id', message: 'Required' })

  return errors
}

// ── Helpers: percent ↔ decimal ──────────────────────────────────────

function decimalToPercent(val: unknown): string {
  if (val === '' || val === null || val === undefined) return ''
  const n = Number(val)
  if (isNaN(n)) return ''
  return String(Math.round(n * 100))
}

function percentToDecimal(pctStr: string): number | '' {
  if (pctStr === '') return ''
  return Number(pctStr) / 100
}

// ── Component ────────────────────────────────────────────────────────

interface Props {
  dbName: string
  scenarioId: string
  projectId: string
  onStatusChange?: () => void
}

export default function ScenarioConfigForm({ dbName, scenarioId, projectId, onStatusChange }: Props) {
  const navigate = useNavigate()
  const { dbName: routeDbName } = useParams()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  const [values, setValues] = useState<Record<string, unknown>>({})
  const [savedValues, setSavedValues] = useState<Record<string, unknown>>({})
  const undoSnapshot = useRef<Record<string, unknown> | null>(null)

  const [datasetVersions, setDatasetVersions] = useState<DatasetVersionInfo[]>([])
  const [datasetVersionsByTable, setDatasetVersionsByTable] = useState<Record<string, DatasetVersionInfo[]>>({})
  const [entitySets, setEntitySets] = useState<Record<string, EntitySetInfo[]>>({})
  const [validationErrors, setValidationErrors] = useState<ValidationError[]>([])

  // Toast
  const [toast, setToast] = useState<{ message: string; showUndo: boolean } | null>(null)
  const toastTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  const showToast = useCallback((message: string, showUndo: boolean = false) => {
    if (toastTimer.current) clearTimeout(toastTimer.current)
    setToast({ message, showUndo })
    toastTimer.current = setTimeout(() => setToast(null), 10000)
  }, [])

  const dismissToast = useCallback(() => {
    if (toastTimer.current) clearTimeout(toastTimer.current)
    setToast(null)
  }, [])

  // Load scenario config
  useEffect(() => {
    setLoading(true)
    getScenarioConfig(dbName, scenarioId)
      .then(data => {
        setValues(data.scenario)
        setSavedValues(data.scenario)
        setDatasetVersions(data.dataset_versions)
        setDatasetVersionsByTable(data.dataset_versions_by_table || {})
        setEntitySets(data.entity_sets || {})
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [dbName, scenarioId])

  const isModified = (key: string) => {
    const current = values[key] ?? ''
    const saved = savedValues[key] ?? ''
    return String(current) !== String(saved)
  }

  const hasAnyChanges = Object.keys(values).some(k => isModified(k))

  const fieldError = (key: string) => validationErrors.find(e => e.field === key)?.message

  const updateField = (key: string, value: unknown) => {
    setValues(prev => ({ ...prev, [key]: value }))
    setValidationErrors(prev => prev.filter(e => e.field !== key))
  }

  const hadResults = !!savedValues['scenario_id']

  // ── Actions ──────────────────────────────────────────────────────

  async function handleSave() {
    const errors = validate(values)
    if (errors.length > 0) {
      setValidationErrors(errors)
      return
    }

    const changed: Record<string, unknown> = {}
    for (const key of Object.keys(values)) {
      if (isModified(key) && key !== 'scenario_id' && key !== 'created_at') {
        changed[key] = values[key]
      }
    }
    if (Object.keys(changed).length === 0) {
      showToast('No changes to save.')
      return
    }

    setSaving(true)
    setError(null)
    try {
      undoSnapshot.current = { ...savedValues }
      await updateScenarioConfig(dbName, scenarioId, changed)
      setSavedValues({ ...values })
      setValidationErrors([])

      if (hadResults) {
        showToast('Saved. Results are no longer relevant.', true)
      } else {
        showToast('Saved.', true)
      }
      onStatusChange?.()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setSaving(false)
    }
  }

  async function handleUndo() {
    if (!undoSnapshot.current) return
    setSaving(true)
    setError(null)
    try {
      const revert: Record<string, unknown> = {}
      for (const key of Object.keys(undoSnapshot.current)) {
        if (key === 'scenario_id' || key === 'created_at') continue
        if (String(savedValues[key] ?? '') !== String(undoSnapshot.current[key] ?? '')) {
          revert[key] = undoSnapshot.current[key]
        }
      }
      if (Object.keys(revert).length > 0) {
        await updateScenarioConfig(dbName, scenarioId, revert)
      }
      const restored = { ...undoSnapshot.current }
      setValues(restored)
      setSavedValues(restored)
      undoSnapshot.current = null
      dismissToast()
      showToast('Undone.')
      onStatusChange?.()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setSaving(false)
    }
  }

  function handleDiscard() {
    setValues({ ...savedValues })
    setValidationErrors([])
  }

  async function handleSaveAs() {
    setSaving(true)
    setError(null)
    try {
      const result = await saveScenarioAs(dbName, scenarioId)
      showToast(`Created "${result.name}" (${result.scenario_id.toUpperCase()})`)
      navigate(`/scenario/${encodeURIComponent(dbName)}/${encodeURIComponent(result.scenario_id)}?tab=configure`)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setSaving(false)
    }
  }

  async function handleDuplicate() {
    setSaving(true)
    setError(null)
    try {
      const result = await duplicateScenario(projectId, scenarioId)
      showToast(`Duplicated as "${result.name}"`)
      navigate(`/scenario/${encodeURIComponent(dbName)}/${encodeURIComponent(result.scenario_id)}?tab=configure`)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setSaving(false)
    }
  }

  // ── Rendering helpers ────────────────────────────────────────────

  function helpIcon(key: string) {
    const text = HELP_TEXT[key]
    if (!text) return null
    return (
      <span className="help-icon" data-tooltip={text}>?</span>
    )
  }

  function renderField(field: FieldDef, disabled: boolean = false) {
    const val = values[field.key]
    const modified = isModified(field.key)
    const errMsg = fieldError(field.key)
    const isDisabled = field.readOnly || disabled || saving

    const wrapperClass = [
      'config-field',
      modified ? 'config-field-modified' : '',
      errMsg ? 'config-field-error' : '',
      field.fullWidth ? 'config-field-full' : '',
    ].filter(Boolean).join(' ')

    if (field.type === 'checkbox') {
      return (
        <div key={field.key} className={wrapperClass}>
          <label className="config-checkbox-label">
            <input
              type="checkbox"
              checked={!!val}
              onChange={e => updateField(field.key, e.target.checked)}
              disabled={isDisabled}
            />
            {field.label}
            {helpIcon(field.key)}
          </label>
          {errMsg && <div className="config-field-msg">{errMsg}</div>}
        </div>
      )
    }

    if (field.type === 'textarea') {
      return (
        <div key={field.key} className={wrapperClass}>
          <label>{field.label} {helpIcon(field.key)}</label>
          <textarea
            value={String(val ?? '')}
            onChange={e => updateField(field.key, e.target.value)}
            disabled={isDisabled}
            rows={3}
            placeholder={field.placeholder}
          />
          {errMsg && <div className="config-field-msg">{errMsg}</div>}
        </div>
      )
    }

    if (field.type === 'select') {
      return (
        <div key={field.key} className={wrapperClass}>
          <label>{field.label} {helpIcon(field.key)}</label>
          <select
            value={String(val ?? '')}
            onChange={e => updateField(field.key, e.target.value)}
            disabled={isDisabled}
          >
            {field.options?.map(o => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
          {errMsg && <div className="config-field-msg">{errMsg}</div>}
        </div>
      )
    }

    // Slider: 0-100% slider with text box (stored as decimal 0-1)
    if (field.type === 'slider') {
      const pctVal = decimalToPercent(val)
      return (
        <div key={field.key} className={wrapperClass}>
          <label>{field.label} {helpIcon(field.key)}</label>
          <div className="slider-group">
            <input
              type="range"
              min={field.min ?? 0}
              max={field.max ?? 100}
              step={field.step ?? '1'}
              value={pctVal || '0'}
              onChange={e => updateField(field.key, percentToDecimal(e.target.value))}
              disabled={isDisabled}
              className="slider-input"
            />
            <input
              type="number"
              min={field.min ?? 0}
              max={field.max ?? 100}
              step={field.step ?? '1'}
              value={pctVal}
              onChange={e => updateField(field.key, percentToDecimal(e.target.value))}
              disabled={isDisabled}
              className="slider-text"
            />
            <span className="slider-unit">%</span>
          </div>
          {errMsg && <div className="config-field-msg">{errMsg}</div>}
        </div>
      )
    }

    // Percent: stored as decimal, displayed as % integer
    if (field.type === 'percent') {
      const pctVal = decimalToPercent(val)
      return (
        <div key={field.key} className={wrapperClass}>
          <label>{field.label} {helpIcon(field.key)}</label>
          <div className="percent-group">
            <input
              type="number"
              min={field.min ?? -100}
              max={field.max ?? 100}
              step={field.step ?? '1'}
              value={pctVal}
              onChange={e => updateField(field.key, percentToDecimal(e.target.value))}
              disabled={isDisabled}
            />
            <span className="slider-unit">%</span>
          </div>
          {errMsg && <div className="config-field-msg">{errMsg}</div>}
        </div>
      )
    }

    return (
      <div key={field.key} className={wrapperClass}>
        <label>{field.label} {helpIcon(field.key)}</label>
        <input
          type={field.type}
          value={String(val ?? '')}
          onChange={e => {
            const v = field.type === 'number' ? (e.target.value === '' ? '' : Number(e.target.value)) : e.target.value
            updateField(field.key, v)
          }}
          disabled={isDisabled}
          placeholder={field.placeholder}
          step={field.step}
          min={field.min}
          max={field.max}
        />
        {errMsg && <div className="config-field-msg">{errMsg}</div>}
      </div>
    )
  }

  function renderDatasetField(field: { key: string; label: string; table: string | null }) {
    const val = values[field.key]
    const modified = isModified(field.key)
    const errMsg = fieldError(field.key)
    const wrapperClass = `config-field${modified ? ' config-field-modified' : ''}${errMsg ? ' config-field-error' : ''}`

    // Use scoped versions for table-specific fields, all versions for default
    const versions = field.table
      ? (datasetVersionsByTable[field.table] || datasetVersions)
      : datasetVersions

    return (
      <div key={field.key} className={wrapperClass}>
        <label>{field.label} {helpIcon(field.key)}</label>
        <select
          value={String(val ?? '')}
          onChange={e => updateField(field.key, e.target.value || null)}
          disabled={saving}
        >
          <option value="">— default —</option>
          {versions.map(dv => (
            <option key={dv.dataset_version_id} value={dv.dataset_version_id}>
              {dv.name || dv.dataset_version_id}
            </option>
          ))}
        </select>
        {errMsg && <div className="config-field-msg">{errMsg}</div>}
      </div>
    )
  }

  function renderEntitySetField(field: { key: string; label: string }) {
    const val = values[field.key]
    const modified = isModified(field.key)
    const errMsg = fieldError(field.key)
    const wrapperClass = `config-field${modified ? ' config-field-modified' : ''}${errMsg ? ' config-field-error' : ''}`
    const options = entitySets[field.key] || []

    return (
      <div key={field.key} className={wrapperClass}>
        <label>{field.label} {helpIcon(field.key)}</label>
        <select
          value={String(val ?? '')}
          onChange={e => updateField(field.key, e.target.value || null)}
          disabled={saving}
        >
          <option value="">(all)</option>
          {options.map(s => (
            <option key={s.id} value={s.id}>
              {s.name || s.id}
            </option>
          ))}
        </select>
        {errMsg && <div className="config-field-msg">{errMsg}</div>}
      </div>
    )
  }

  // ── Render ────────────────────────────────────────────────────────

  if (loading) return <p>Loading configuration...</p>
  if (error && !values['scenario_id']) return <div className="error">Error: {error}</div>

  const reorderActive = !!values['reorder_logic'] && values['reorder_logic'] !== ''
  const forecastActive = !!values['forecast_method'] && values['forecast_method'] !== ''

  return (
    <div className="config-form">
      {/* Action bar */}
      <div className="config-actions">
        <button className="btn-primary" onClick={handleSave} disabled={saving || !hasAnyChanges}>
          {saving ? 'Saving...' : 'Save'}
        </button>
        <button className="config-btn" onClick={handleSaveAs} disabled={saving} title="Save current edits as a new scenario">Save As...</button>
        <button className="config-btn" onClick={handleDiscard} disabled={saving || !hasAnyChanges}>
          Discard Changes
        </button>
        <button className="config-btn" onClick={handleDuplicate} disabled={saving} title="Clone the saved scenario (including results) as a new scenario">Duplicate</button>
        <a className="config-btn" href={exportScenarioYamlUrl(dbName, scenarioId)} download>
          Export YAML
        </a>
      </div>

      {error && <div className="error" style={{ marginTop: 12 }}>Error: {error}</div>}

      {/* General */}
      <ConfigSection title="General" defaultOpen>
        <div className="config-grid">
          {GENERAL_FIELDS.map(f => renderField(f))}
        </div>
      </ConfigSection>

      {/* Input Datasets */}
      <ConfigSection title="Input Datasets" defaultOpen>
        <div className="config-section-links">
          <a href={`/datasets/${encodeURIComponent(routeDbName || dbName)}`}>Manage Datasets</a>
        </div>
        <div className="config-grid">
          {DATASET_KEYS.map(f => renderDatasetField(f))}
        </div>
      </ConfigSection>

      {/* Entity Sets */}
      <ConfigSection title="Entity Sets" defaultOpen>
        <div className="config-grid">
          {ENTITY_SET_FIELDS.map(f => renderEntitySetField(f))}
        </div>
      </ConfigSection>

      {/* Fulfillment */}
      <ConfigSection title="Fulfillment" defaultOpen>
        <div className="config-grid">
          {renderField(FULFILLMENT_METHOD)}
          {FULFILLMENT_PARAMS.map(f => renderField(f))}
        </div>
      </ConfigSection>

      {/* Ordering */}
      <ConfigSection title="Ordering" defaultOpen>
        <div className="config-grid">
          {renderField(ORDERING_METHOD)}
          {reorderActive && ORDERING_PARAMS.map(f => renderField(f))}
        </div>
      </ConfigSection>

      {/* Forecasting */}
      <ConfigSection title="Forecasting" defaultOpen>
        <div className="config-grid">
          {renderField(FORECAST_METHOD)}
          {forecastActive && FORECAST_PARAMS.map(f => renderField(f))}
        </div>
      </ConfigSection>

      {/* Notes */}
      <ConfigSection title="Notes">
        <div className="config-grid">
          {renderField({ key: 'notes', label: 'Notes', type: 'textarea', fullWidth: true })}
        </div>
      </ConfigSection>

      {/* Toast */}
      {toast && (
        <div className="config-toast">
          <span>{toast.message}</span>
          {toast.showUndo && (
            <button className="config-toast-undo" onClick={handleUndo}>Undo</button>
          )}
          <button className="config-toast-dismiss" onClick={dismissToast}>{'\u2715'}</button>
        </div>
      )}
    </div>
  )
}

// ── Collapsible section for the config form ─────────────────────────

function ConfigSection({ title, defaultOpen = false, children, enabled = true, onToggle }: {
  title: string
  defaultOpen?: boolean
  children: React.ReactNode
  enabled?: boolean
  onToggle?: (enabled: boolean) => void
}) {
  const [expanded, setExpanded] = useState(defaultOpen)

  if (!enabled && onToggle) {
    return (
      <section className="config-section">
        <h2 className="config-section-header" onClick={() => onToggle(true)}>
          <span className="section-chevron">{'\u25B6'}</span>
          <label className="config-section-toggle">
            <input type="checkbox" checked={false} onChange={() => onToggle(true)} />
            {title}
          </label>
        </h2>
      </section>
    )
  }

  return (
    <section className="config-section">
      <h2
        className="config-section-header"
        onClick={() => setExpanded(!expanded)}
        style={{ cursor: 'pointer', userSelect: 'none' }}
      >
        <span className="section-chevron">{expanded ? '\u25BC' : '\u25B6'}</span>
        {onToggle ? (
          <label className="config-section-toggle" onClick={e => e.stopPropagation()}>
            <input type="checkbox" checked={true} onChange={() => onToggle(false)} />
            {title}
          </label>
        ) : (
          <>{' '}{title}</>
        )}
      </h2>
      {expanded && <div className="config-section-body">{children}</div>}
    </section>
  )
}
