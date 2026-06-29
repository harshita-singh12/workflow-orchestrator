import { api } from "../api";
import { usePolling } from "../hooks";

export default function WorkersPage() {
  const { data: workers, error } = usePolling(() => api.listWorkers(), 3000, []);

  return (
    <div>
      <div className="page-header">
        <h1>Worker Fleet</h1>
      </div>
      <p className="muted">
        Workers heartbeat every few seconds; entries disappear automatically ~15s after a worker stops (see
        internal/workers.Registry). This is purely for visibility — a worker that never registers can still
        claim and execute tasks fine.
      </p>
      {error && <div className="banner banner-error">{error}</div>}
      {workers && workers.length === 0 && <div className="empty-state">No workers currently registered.</div>}
      {workers && workers.length > 0 && (
        <table className="table">
          <thead>
            <tr>
              <th>Worker ID</th>
              <th>Queues</th>
              <th>Capacity</th>
              <th>Last seen</th>
            </tr>
          </thead>
          <tbody>
            {workers.map((w) => (
              <tr key={w.worker_id}>
                <td className="mono">{w.worker_id}</td>
                <td className="muted">{w.queues?.join(", ") || "—"}</td>
                <td className="muted">{w.capacity}</td>
                <td className="muted">{new Date(w.last_seen).toLocaleTimeString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
