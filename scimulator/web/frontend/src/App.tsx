import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import ProjectsPage from './pages/ProjectsPage'
import HomePage from './pages/HomePage'
import ScenarioPage from './pages/ScenarioPage'
import RunPage from './pages/RunPage'
import HelpPage from './pages/HelpPage'
import DatasetsPage from './pages/DatasetsPage'
import EntitySetCreatePage from './pages/EntitySetCreatePage'

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<ProjectsPage />} />
        <Route path="/project/:dbName/:projectId" element={<HomePage />} />
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
