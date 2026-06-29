import { NavLink, Outlet } from "react-router-dom";

export default function App() {
  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark">⛭</span> Workflow Orchestrator
        </div>
        <nav>
          <NavLink to="/" end>
            Runs
          </NavLink>
          <NavLink to="/definitions">Definitions</NavLink>
          <NavLink to="/workers">Workers</NavLink>
          <NavLink to="/cluster">Cluster</NavLink>
        </nav>
      </header>
      <main className="content">
        <Outlet />
      </main>
    </div>
  );
}
