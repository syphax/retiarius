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
import { useSweep } from './solver/useSweep';
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

  const { sweep, runSweep, cancelSweep } = useSweep();
  const player = usePlayback(sweep.solutions, sweep.status === 'running');
  const selected = sweep.solutions.find((s) => s.n === player.playback.n);
  const sweepModel = useMemo(
    () => (sweep.params ? buildModel(sweep.points, sweep.params) : null),
    [sweep.points, sweep.params],
  );
  const view = useFrameView(sweepModel, sweep.points, selected, player.playback.t);
  const [display, setDisplay] = useState<DisplayOptions>({ paths: true, demandLines: true, sourceLines: true });
  const stale =
    sweep.status !== 'idle' &&
    (sweep.points !== validation.points || JSON.stringify(sweep.params) !== JSON.stringify(params));

  const runAll = () => {
    runSweep(validation.points, params);
    player.startSweep();
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
              running={sweep.status === 'running'}
              stale={stale}
              onRunAll={runAll}
            />
            <SolutionBar
              sweep={sweep}
              view={view}
              player={player}
              display={display}
              onDisplay={setDisplay}
              onCancel={cancelSweep}
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
            sweep={sweep}
            onShowOnMap={(n) => {
              player.select(n);
              setTab('map');
            }}
          />
        )}
      </main>
    </div>
  );
}
