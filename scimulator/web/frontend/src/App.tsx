import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import HomePage from './pages/HomePage'
import ScenarioPage from './pages/ScenarioPage'
import RunPage from './pages/RunPage'

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<HomePage />} />
        <Route path="/run" element={<RunPage />} />
        <Route path="/scenario/:dbName/:scenarioId" element={<ScenarioPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  )
}
