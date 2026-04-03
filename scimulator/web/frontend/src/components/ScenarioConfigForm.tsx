import { useCallback, useEffect, useRef, useState } from 'react'
import {
  getScenarioConfig,
  updateScenarioConfig,
  saveScenarioAs,
  exportScenarioYamlUrl,
  duplicateScenario,
} from '../api/client'
import type { DatasetVersionInfo } from '../api/client'
import { useNavigate } from 'react-router-dom'

// ── Field definitions ────────────────────────────────────────────────

interface FieldDef {
  key: string
  label: string
  type: 'text' | 'number' | 'date' | 'select' | 'checkbox' | 'textarea'
  options?: { value: string; label: string }[]
  readOnly?: boolean
  placeholder?: string
  step?: string
  min?: number
  max?: number
}

const GENERAL_FIELDS: FieldDef[] = [
  { key: 'scenario_id', label: 'Scenario ID', type: 'text', readOnly: true },
  { key: 'name', label: 'Name', type: 'text' },
  { key: 'description', label: 'Description', type: 'textarea' },
  { key: 'currency_code', label: 'Currency Code', type: 'text', placeholder: 'USD' },
  { key: 'time_resolution', label: 'Time Resolution', type: 'select', options: [
    { value: 'daily', label: 'Daily' },
    { value: 'weekly', label: 'Weekly' },
  ]},
  { key: 'start_date', label: 'Start Date', type: 'date' },
  { key: 'end_date', label: 'End Date', type: 'date' },
  { key: 'warm_up_days', label: 'Warm-up Days', type: 'number', min: 0 },
  { key: 'backorder_probability', label: 'Backorder Probability', type: 'number', step: '0.01', min: 0, max: 1 },
  { key: 'write_event_log', label: 'Write Event Log', type: 'checkbox' },
  { key: 'write_snapshots', label: 'Write Snapshots', type: 'checkbox' },
  { key: 'snapshot_interval_days', label: 'Snapshot Interval (days)', type: 'number', min: 1 },
]

const DATASET_KEYS = [
  { key: 'dataset_version_id', label: 'Default Dataset Version' },
  { key: 'demand_version_id', label: 'Demand (override)' },
  { key: 'inbound_version_id', label: 'Inbound Schedule (override)' },
  { key: 'inventory_version_id', label: 'Initial Inventory (override)' },
]

const FULFILLMENT_FIELDS: FieldDef[] = [
  { key: 'fulfillment_logic', label: 'Fulfillment Logic', type: 'select', options: [
    { value: 'closest_node_wins', label: 'Closest Node Wins' },
    { value: 'closest_node_only', label: 'Closest Node Only' },
  ]},
]

const ORDERING_FIELDS: FieldDef[] = [
  { key: 'reorder_logic', label: 'Reorder Logic', type: 'select', options: [
    { value: '', label: 'None' },
    { value: 'periodic', label: 'Periodic' },
  ]},
  { key: 'order_frequency_days', label: 'Order Frequency (days)', type: 'number', min: 1 },
  { key: 'safety_stock_days', label: 'Safety Stock (days)', type: 'number', min: 0 },
  { key: 'mrq_days', label: 'MRQ (days)', type: 'number', min: 1 },
  { key: 'consolidation_mode', label: 'Consolidation Mode', type: 'select', options: [
    { value: 'free', label: 'Free' },
  ]},
]

const FORECAST_FIELDS: FieldDef[] = [
  { key: 'forecast_method', label: 'Forecast Method', type: 'select', options: [
    { value: '', label: 'None' },
    { value: 'noisy_actuals', label: 'Noisy Actuals' },
  ]},
  { key: 'forecast_bias', label: 'Forecast Bias', type: 'number', step: '0.01' },
  { key: 'forecast_error', label: 'Forecast Error', type: 'number', step: '0.01', min: 0 },
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
  if (bp < 0 || bp > 1) errors.push({ field: 'backorder_probability', message: 'Must be between 0 and 1' })
  if (values['write_snapshots'] && n('snapshot_interval_days') < 1)
    errors.push({ field: 'snapshot_interval_days', message: 'Must be >= 1' })

  const reorder = s('reorder_logic')
  if (reorder && reorder !== '') {
    if (n('order_frequency_days') < 1) errors.push({ field: 'order_frequency_days', message: 'Must be >= 1' })
    if (n('safety_stock_days') < 0) errors.push({ field: 'safety_stock_days', message: 'Must be >= 0' })
  }
  if (n('forecast_error') < 0) errors.push({ field: 'forecast_error', message: 'Must be >= 0' })

  if (!s('dataset_version_id')) errors.push({ field: 'dataset_version_id', message: 'Required' })

  return errors
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
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  // Form values (current edits)
  const [values, setValues] = useState<Record<string, unknown>>({})
  // Saved baseline (for change highlighting and discard)
  const [savedValues, setSavedValues] = useState<Record<string, unknown>>({})
  // Undo snapshot (previous saved state, for undo after save)
  const undoSnapshot = useRef<Record<string, unknown> | null>(null)

  const [datasetVersions, setDatasetVersions] = useState<DatasetVersionInfo[]>([])
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
        setLoading(false)
      })
      .catch(err => {
        setError(err.message)
        setLoading(false)
      })
  }, [dbName, scenarioId])

  // Check if a field has been modified from saved state
  const isModified = (key: string) => {
    const current = values[key] ?? ''
    const saved = savedValues[key] ?? ''
    return String(current) !== String(saved)
  }

  const hasAnyChanges = Object.keys(values).some(k => isModified(k))

  // Get validation error for a field (for inline display)
  const fieldError = (key: string) => validationErrors.find(e => e.field === key)?.message

  // Update a single field
  const updateField = (key: string, value: unknown) => {
    setValues(prev => ({ ...prev, [key]: value }))
    // Clear validation error for this field
    setValidationErrors(prev => prev.filter(e => e.field !== key))
  }

  // Determine if field had prior results (for results-invalidation toast)
  const hadResults = savedValues['scenario_id'] && (
    // Check if there's run metadata — the presence of scenario in result DB implies it may have been run
    true // We'll check registry status instead
  )

  // ── Actions ──────────────────────────────────────────────────────

  async function handleSave() {
    const errors = validate(values)
    if (errors.length > 0) {
      setValidationErrors(errors)
      return
    }

    // Collect only changed fields
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
      // Stash current saved state for undo
      undoSnapshot.current = { ...savedValues }

      await updateScenarioConfig(dbName, scenarioId, changed)
      setSavedValues({ ...values })
      setValidationErrors([])

      // Show combined toast
      if (hadResults) {
        showToast('Saved. Results are no longer relevant. [Undo] to preserve them and dupe the scenario.', true)
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
      // Compute diff between current saved and snapshot
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
      // Navigate to the new scenario's config
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

  function renderField(field: FieldDef, disabled: boolean = false) {
    const val = values[field.key]
    const modified = isModified(field.key)
    const errMsg = fieldError(field.key)
    const isDisabled = field.readOnly || disabled || saving

    const wrapperClass = `config-field${modified ? ' config-field-modified' : ''}${errMsg ? ' config-field-error' : ''}`

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
          </label>
          {errMsg && <div className="config-field-msg">{errMsg}</div>}
        </div>
      )
    }

    if (field.type === 'textarea') {
      return (
        <div key={field.key} className={wrapperClass}>
          <label>{field.label}</label>
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
          <label>{field.label}</label>
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

    return (
      <div key={field.key} className={wrapperClass}>
        <label>{field.label}</label>
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

  function renderDatasetField(field: { key: string; label: string }) {
    const val = values[field.key]
    const modified = isModified(field.key)
    const errMsg = fieldError(field.key)
    const wrapperClass = `config-field${modified ? ' config-field-modified' : ''}${errMsg ? ' config-field-error' : ''}`

    return (
      <div key={field.key} className={wrapperClass}>
        <label>{field.label}</label>
        <select
          value={String(val ?? '')}
          onChange={e => updateField(field.key, e.target.value || null)}
          disabled={saving}
        >
          <option value="">— default —</option>
          {datasetVersions.map(dv => (
            <option key={dv.dataset_version_id} value={dv.dataset_version_id}>
              {dv.name || dv.dataset_version_id}
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
        <button className="config-btn" onClick={handleSaveAs} disabled={saving}>Save As...</button>
        <button className="config-btn" onClick={handleDiscard} disabled={saving || !hasAnyChanges}>
          Discard Changes
        </button>
        <a className="config-btn" href={exportScenarioYamlUrl(dbName, scenarioId)} download>
          Export YAML
        </a>
        <span className="config-actions-sep" />
        <button className="config-btn" onClick={handleDuplicate} disabled={saving}>Duplicate</button>
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
        <div className="config-grid">
          {DATASET_KEYS.map(f => renderDatasetField(f))}
        </div>
      </ConfigSection>

      {/* Fulfillment */}
      <ConfigSection title="Fulfillment" defaultOpen>
        <div className="config-grid">
          {FULFILLMENT_FIELDS.map(f => renderField(f))}
        </div>
      </ConfigSection>

      {/* Ordering */}
      <ConfigSection title="Ordering" defaultOpen>
        <div className="config-grid">
          {ORDERING_FIELDS.map((f, i) => renderField(f, i > 0 && !reorderActive))}
        </div>
      </ConfigSection>

      {/* Forecasting */}
      <ConfigSection title="Forecasting" defaultOpen>
        <div className="config-grid">
          {FORECAST_FIELDS.map((f, i) => renderField(f, i > 0 && !forecastActive))}
        </div>
      </ConfigSection>

      {/* Entity Sets */}
      <ConfigSection title="Entity Sets">
        <div className="config-grid">
          {[
            { key: 'product_set_id', label: 'Product Set' },
            { key: 'supply_node_set_id', label: 'Supply Node Set' },
            { key: 'distribution_node_set_id', label: 'Distribution Node Set' },
            { key: 'demand_node_set_id', label: 'Demand Node Set' },
            { key: 'edge_set_id', label: 'Edge Set' },
          ].map(f => renderField({ ...f, type: 'text', placeholder: '(all)' } as FieldDef))}
        </div>
      </ConfigSection>

      {/* Notes */}
      <ConfigSection title="Notes">
        <div className="config-grid">
          {renderField({ key: 'notes', label: 'Notes', type: 'textarea' })}
        </div>
      </ConfigSection>

      {/* Toast */}
      {toast && (
        <div className="config-toast">
          <span>{toast.message.replace(' [Undo] ', ' ')}</span>
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
