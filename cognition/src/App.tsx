import { useEffect, useMemo, useState } from 'react';
import { loadGazetteer, type Gazetteer } from './data/geo';
import { parseText } from './data/parse';
import type { RawRow, ValidationResult } from './data/types';
import { validate } from './data/validate';
import { DEFAULT_PARAMS, type Params } from './params';
import TopBar, { type Tab } from './components/TopBar';
import ParamsPane from './components/ParamsPane';
import MapView, { type DisplayOptions } from './components/MapView';
import DataTab from './components/DataTab';
import ResultsTab from './components/ResultsTab';
import SolutionBar from './components/SolutionBar';
import { useRuns } from './runs/useRuns';
import { buildModel } from './solver/model';
import { usePlayback } from './playback/usePlayback';
import { useFrameView } from './playback/frameView';

const EMPTY_VALIDATION: ValidationResult = { valid: [], issues: new Map(), points: [] };

export default function App() {
  const [tab, setTab] = useState<Tab>('map');
  const [rows, setRows] = useState<RawRow[]>([]);
  const [datasetName, setDatasetName] = useState('');
  const [gaz, setGaz] = useState<Gazetteer | null>(null);
  const [params, setParams] = useState<Params>(DEFAULT_PARAMS);

  useEffect(() => {
    loadGazetteer().then(setGaz);
  }, []);

  const validation = useMemo(() => (gaz ? validate(rows, gaz) : EMPTY_VALIDATION), [rows, gaz]);

  const { runs, active, setActive, runSweep, runAdhoc, cancel, remove, toggleChart } = useRuns();
  const player = usePlayback(active?.solutions ?? [], active?.status === 'running');
  const selected = active?.solutions.find((s) => s.n === player.playback.n);
  const activeModel = useMemo(() => (active ? buildModel(active.points, active.params) : null), [active?.points, active?.params]); // eslint-disable-line react-hooks/exhaustive-deps
  const view = useFrameView(activeModel, active?.points ?? [], selected, player.playback.t);
  const [display, setDisplay] = useState<DisplayOptions>({ paths: true, demandLines: true, sourceLines: true });
  const running = runs.some((r) => r.status === 'running');
  const stale =
    !!active &&
    (active.points !== validation.points ||
      JSON.stringify({ ...active.params, minNodes: 0, maxNodes: 0 }) !==
        JSON.stringify({ ...params, minNodes: 0, maxNodes: 0 }));

  const runAll = () => {
    runSweep(validation.points, params);
    player.startSweep();
  };

  const rerun = () => {
    if (!active || !selected) return;
    runAdhoc(active, selected, validation.points, params);
    player.startSweep();
  };

  const showOnMap = (runId: number, n: number) => {
    setActive(runId);
    player.select(n, runs.find((r) => r.id === runId)?.solutions);
    setTab('map');
  };

  const onImport = (newRows: RawRow[], name: string) => {
    setRows(newRows);
    setDatasetName(name);
  };

  const loadSample = async () => {
    const text = await (await fetch('/sample/sample.csv')).text();
    onImport(parseText(text), 'Sample (US ZIP3 population)');
  };

  return (
    <div className="app">
      <TopBar
        tab={tab}
        onTab={setTab}
        datasetName={datasetName}
        flaggedCount={validation.issues.size}
        onImport={onImport}
        onLoadSample={loadSample}
      />
      <main className="main">
        {tab === 'map' && (
          <div className="map-screen">
            <MapView
              points={validation.points}
              showFixed={params.useFixedNodes}
              view={view}
              display={display}
            />
            <ParamsPane
              params={params}
              onChange={setParams}
              validation={validation}
              running={running}
              stale={stale}
              onRunAll={runAll}
              rerunFrom={active && selected && active.status !== 'running' ? { label: active.label, n: selected.n } : null}
              onRerun={rerun}
            />
            <SolutionBar
              run={active}
              view={view}
              player={player}
              display={display}
              onDisplay={setDisplay}
              onCancel={cancel}
            />
            {rows.length === 0 && (
              <div className="empty-hint">
                <p>No data loaded.</p>
                <p>
                  Import data from the top bar, or <button className="link" onClick={loadSample}>load the sample dataset</button>.
                </p>
              </div>
            )}
          </div>
        )}
        {tab === 'data' && (
          <DataTab rows={rows} onRowsChange={setRows} validation={validation} geoReady={gaz !== null} />
        )}
        {tab === 'results' && (
          <ResultsTab
            runs={runs}
            activeId={active?.id}
            onShowOnMap={showOnMap}
            onToggleChart={toggleChart}
            onRemove={remove}
          />
        )}
      </main>
    </div>
  );
}
