import { useRef, useState } from 'react';
import { fetchSheet, ImportError, parseFile, parseText } from '../data/parse';
import type { RawRow } from '../data/types';

export type Tab = 'map' | 'data' | 'results';

interface Props {
  tab: Tab;
  onTab: (t: Tab) => void;
  datasetName: string;
  flaggedCount: number;
  onImport: (rows: RawRow[], name: string) => void;
  onLoadSample: () => void;
}

type DialogMode = 'paste' | 'sheet' | null;

export default function TopBar({ tab, onTab, datasetName, flaggedCount, onImport, onLoadSample }: Props) {
  const fileInput = useRef<HTMLInputElement>(null);
  const [dialog, setDialog] = useState<DialogMode>(null);
  const [text, setText] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const run = async (load: () => Promise<RawRow[]> | RawRow[], name: string) => {
    setError('');
    setBusy(true);
    try {
      onImport(await load(), name);
      setDialog(null);
      setText('');
    } catch (e) {
      setError(e instanceof ImportError ? e.message : `Import failed: ${String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  const onFile = (file: File | undefined) => {
    if (fileInput.current) fileInput.current.value = '';
    if (file) run(() => parseFile(file), file.name);
  };

  const openDialog = (mode: DialogMode) => {
    setError('');
    setText('');
    setDialog(mode);
  };

  return (
    <header className="topbar">
      <div className="brand">
        Cognition <span className="brand-sub">center of gravity</span>
      </div>
      <nav className="tabs">
        {(['map', 'data', 'results'] as Tab[]).map((t) => (
          <button key={t} className={t === tab ? 'tab active' : 'tab'} onClick={() => onTab(t)}>
            {t[0].toUpperCase() + t.slice(1)}
            {t === 'data' && flaggedCount > 0 && <span className="badge">{flaggedCount}</span>}
          </button>
        ))}
      </nav>
      <div className="spacer" />
      {datasetName && <div className="dataset-name" title={datasetName}>{datasetName}</div>}
      <div className="import-buttons">
        <span className="label">Import:</span>
        <button onClick={() => openDialog('paste')}>Paste</button>
        <button onClick={() => fileInput.current?.click()}>File</button>
        <button onClick={() => openDialog('sheet')}>Google Sheet</button>
        <button onClick={onLoadSample}>Sample</button>
        <input
          ref={fileInput}
          type="file"
          accept=".csv,.tsv,.txt,.xlsx,.xlsm,.xls"
          hidden
          onChange={(e) => onFile(e.target.files?.[0])}
        />
      </div>
      {error && !dialog && (
        <div className="import-error" onClick={() => setError('')}>
          {error}
        </div>
      )}

      {dialog && (
        <div className="modal-backdrop" onClick={() => setDialog(null)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h3>{dialog === 'paste' ? 'Paste from Sheets or Excel' : 'Import a Google Sheet'}</h3>
            {dialog === 'paste' ? (
              <textarea
                autoFocus
                rows={12}
                placeholder={'Include the header row, e.g.\nType\tVolume\tLat\tLon\tPostal Code\tPostal Code Type\tActive'}
                value={text}
                onChange={(e) => setText(e.target.value)}
              />
            ) : (
              <>
                <input
                  autoFocus
                  type="url"
                  placeholder="https://docs.google.com/spreadsheets/d/…/edit#gid=0"
                  value={text}
                  onChange={(e) => setText(e.target.value)}
                />
                <p className="hint">
                  The sheet must be shared as "Anyone with the link can view". The tab in the link (gid) is imported.
                </p>
              </>
            )}
            <p className="hint">
              Required columns: <code>Type</code> (demand / source / fixed), <code>Volume</code>, and either{' '}
              <code>Lat</code> + <code>Lon</code> or <code>Postal Code</code> + <code>Postal Code Type</code> (zip5 /
              zip3). Optional: <code>Active</code> (TRUE / FALSE; blank = active). Header case doesn't matter; other
              columns are ignored. Importing replaces the current data.
            </p>
            {error && <div className="error">{error}</div>}
            <div className="modal-actions">
              <button onClick={() => setDialog(null)}>Cancel</button>
              <button
                className="primary"
                disabled={busy || !text.trim()}
                onClick={() =>
                  dialog === 'paste'
                    ? run(() => parseText(text), 'Pasted data')
                    : run(() => fetchSheet(text.trim()), 'Google Sheet')
                }
              >
                {busy ? 'Importing…' : 'Import'}
              </button>
            </div>
          </div>
        </div>
      )}
    </header>
  );
}
