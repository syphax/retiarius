import { Routes, Route, Navigate, useParams } from 'react-router-dom'
import Layout from './components/Layout'
import ProjectsPage from './pages/ProjectsPage'
import HomePage from './pages/HomePage'
import ScenarioPage from './pages/ScenarioPage'
import RunPage from './pages/RunPage'
import HelpPage from './pages/HelpPage'
import DatasetsPage from './pages/DatasetsPage'
import EntitySetCreatePage from './pages/EntitySetCreatePage'
import TopologyDetailPage from './pages/TopologyDetailPage'
import DemandDetailPage from './pages/DemandDetailPage'
import InventoryDetailPage from './pages/InventoryDetailPage'
import InboundDetailPage from './pages/InboundDetailPage'

function ProjectDefaultTabRedirect() {
  const { dbName, projectId } = useParams<{ dbName: string; projectId: string }>()
  if (!dbName || !projectId) return <Navigate to="/" replace />
  return <Navigate to={`/project/${encodeURIComponent(dbName)}/${encodeURIComponent(projectId)}/scenarios`} replace />
}

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<ProjectsPage />} />
        <Route path="/project/:dbName/:projectId" element={<ProjectDefaultTabRedirect />} />
        <Route path="/project/:dbName/:projectId/:tab" element={<HomePage />} />
        <Route path="/project/:dbName/:projectId/topology/:table" element={<TopologyDetailPage />} />
        <Route path="/project/:dbName/:projectId/data/demand" element={<DemandDetailPage />} />
        <Route path="/project/:dbName/:projectId/data/initial_inventory" element={<InventoryDetailPage />} />
        <Route path="/project/:dbName/:projectId/data/inbound_schedule" element={<InboundDetailPage />} />
        <Route path="/run" element={<RunPage />} />
        <Route path="/help" element={<HelpPage />} />
        <Route path="/scenario/:dbName/:scenarioId" element={<ScenarioPage />} />
        <Route path="/datasets/:dbName" element={<DatasetsPage />} />
        <Route path="/datasets/:dbName/entity-sets/create" element={<EntitySetCreatePage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  )
}
