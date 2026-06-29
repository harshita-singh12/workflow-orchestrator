import { useState } from "react";
import { Link } from "react-router-dom";
import { api, RunStatus } from "../api";
import { usePolling } from "../hooks";
import StatusBadge from "../components/StatusBadge";
import NewRunModal from "../components/NewRunModal";

const STATUS_FILTERS: (RunStatus | "ALL")[] = ["ALL", "PENDING", "RUNNING", "COMPLETED", "FAILED", "CANCELLED"];

function duration(createdAt: string, completedAt: string | null): string {
  const start = new Date(createdAt).getTime();
  const end = completedAt ? new Date(completedAt).getTime() : Date.now();
  const ms = Math.max(0, end - start);
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}

export default function RunListPage() {
  const [status, setStatus] = useState<RunStatus | "ALL">("ALL");
  const [showNewRun, setShowNewRun] = useState(false);
  const { data: runs, error, loading } = usePolling(
    () => api.listRuns({ status: status === "ALL" ? undefined : status, limit: 100 }),
    2000,
    [status],
  );

  return (
    <div>
      <div className="page-header">
        <h1>Workflow Runs</h1>
        <button className="btn btn-primary" onClick={() => setShowNewRun(true)}>
          + New Run
        </button>
      </div>

      <div className="filter-bar">
        {STATUS_FILTERS.map((s) => (
          <button
            key={s}
            className={`chip ${status === s ? "chip-active" : ""}`}
            onClick={() => setStatus(s)}
          >
            {s}
          </button>
        ))}
      </div>

      {error && <div className="banner banner-error">Failed to load runs: {error}</div>}
      {loading && !runs && <div className="empty-state">Loading…</div>}
      {runs && runs.length === 0 && <div className="empty-state">No runs yet. Start one with “New Run”.</div>}

      {runs && runs.length > 0 && (
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Status</th>
              <th>Created</th>
              <th>Duration</th>
              <th>Run ID</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.ID}>
                <td>
                  <Link to={`/runs/${r.ID}`}>{r.Name}</Link>
                  <span className="muted"> v{r.Version}</span>
                </td>
                <td>
                  <StatusBadge status={r.Status} />
                </td>
                <td className="muted">{new Date(r.CreatedAt).toLocaleString()}</td>
                <td className="muted">{duration(r.CreatedAt, r.CompletedAt)}</td>
                <td className="mono muted">{r.ID.slice(0, 8)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {showNewRun && <NewRunModal onClose={() => setShowNewRun(false)} />}
    </div>
  );
}
