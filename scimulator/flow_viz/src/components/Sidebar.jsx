import React, { useState } from 'react';

export function Sidebar({ params, onChange, onRecalculate, dirty }) {
  const [collapsed, setCollapsed] = useState(false);

  const handleChange = (key, value) => {
    onChange({ ...params, [key]: value });
  };

  return (
    <div className={`sidebar ${collapsed ? 'collapsed' : ''}`}>
      <button
        className="sidebar-toggle"
        onClick={() => setCollapsed(!collapsed)}
        title={collapsed ? 'Expand' : 'Collapse'}
      >
        {collapsed ? '▶' : '◀'}
      </button>

      {!collapsed && (
        <div className="sidebar-content">
          <h3>Settings</h3>

          <div className="control-group">
            <label>Aggregation</label>
            <select
              value={params.aggregation}
              onChange={(e) => handleChange('aggregation', e.target.value)}
            >
              <option value="none">None</option>
              <option value="day_zip3">Day / ZIP3</option>
            </select>
          </div>

          <div className="control-group">
            <label>Size Attribute</label>
            <select
              value={params.sizeAttribute}
              onChange={(e) => handleChange('sizeAttribute', e.target.value)}
            >
              <option value="none">None</option>
              <option value="value">Value</option>
              <option value="weight">Weight</option>
              <option value="cube">Cube</option>
            </select>
          </div>

          <div className="control-group">
            <label>Size Scale: {params.sizeScale}</label>
            <input
              type="range"
              min={1}
              max={10}
              step={1}
              value={params.sizeScale}
              onChange={(e) => handleChange('sizeScale', Number(e.target.value))}
            />
          </div>

          <div className="control-group">
            <label>Color Attribute</label>
            <select
              value={params.colorAttribute}
              onChange={(e) => handleChange('colorAttribute', e.target.value)}
            >
              <option value="none">None (Cyan)</option>
              <option value="value">Value</option>
              <option value="weight">Weight</option>
              <option value="cube">Cube</option>
              <option value="delivery_days">Delivery Days</option>
              <option value="brand">Brand</option>
            </select>
          </div>

          <div className="control-group">
            <label>Path Mode</label>
            <select
              value={params.pathMode}
              onChange={(e) => handleChange('pathMode', e.target.value)}
            >
              <option value="none">None</option>
              <option value="recent">Recent Trail</option>
              <option value="full_route">Full Route</option>
              <option value="cumulative">Cumulative</option>
            </select>
          </div>

          <div className="control-group">
            <label>Trail Length: {params.trailLength === 0 ? 'None' : `${params.trailLength}d`}</label>
            <input
              type="range"
              min={0}
              max={2}
              step={0.5}
              value={params.trailLength}
              onChange={(e) => handleChange('trailLength', Number(e.target.value))}
            />
          </div>

          <button
            className={`recalc-btn ${dirty ? 'active' : ''}`}
            onClick={onRecalculate}
            disabled={!dirty}
          >
            Recalculate
          </button>
        </div>
      )}
    </div>
  );
}
