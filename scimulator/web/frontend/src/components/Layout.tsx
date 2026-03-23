import { Link, Outlet } from 'react-router-dom'

export default function Layout() {
  return (
    <div className="app-layout">
      <header className="app-header">
        <Link to="/" className="app-logo">SCimulator</Link>
        <nav>
          <Link to="/">Scenarios</Link>
          <Link to="/run">Run Simulation</Link>
        </nav>
      </header>
      <main className="app-main">
        <Outlet />
      </main>
    </div>
  )
}
