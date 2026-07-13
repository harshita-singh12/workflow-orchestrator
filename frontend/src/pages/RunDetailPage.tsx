import { Fragment, useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api";
import { usePolling } from "../hooks";
import StatusBadge from "../components/StatusBadge";
import JsonView from "../components/JsonView";

export default function RunDetailPage() {
  const { id = "" } = useParams();
  const [signalName, setSignalName] = useState("");
  const [signalPayload, setSignalPayload] = useState("{}");
  const [actionError, setActionError] = useState<string | null>(null);
  const [expandedStep, setExpandedStep] = useState<string | null>(null);

  const isTerminal = (s?: string) => s === "COMPLETED" || s === "FAILED" || s === "CANCELLED";

  // Tracks the last-seen run status so the run poll itself can stop once the run reaches a
  // terminal state (mirrors the steps/history polls below) without the circularity of a poll
  // referencing its own not-yet-assigned result.
  const [runStatus, setRunStatus] = useState<string | undefined>(undefined);

  const { data: run, error: runError, refresh: refreshRun } = usePolling(
    () => api.getRun(id),
    1500,
    [id],
    !isTerminal(runStatus),
  );

  useEffect(() => {
    setRunStatus(run?.Status);
  }, [run?.Status]);
  const { data: steps, refresh: refreshSteps } = usePolling(
    () => api.getSteps(id),
    1500,
    [id],
    !isTerminal(run?.Status),
  );
  const { data: history, refresh: refreshHistory } = usePolling(
    () => api.getHistory(id),
    1500,
    [id],
    !isTerminal(run?.Status),
  );

  async function sendSignal() {
    setActionError(null);
    let payload: unknown;
    try {
      payload = JSON.parse(signalPayload || "{}");
    } catch {
      setActionError("Signal payload must be valid JSON");
      return;
    }
    try {
      await api.sendSignal(id, signalName, payload);
      setSignalName("");
      setSignalPayload("{}");
      refreshRun();
      refreshSteps();
      refreshHistory();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    }
  }

  async function cancel() {
    setActionError(null);
    try {
      await api.cancelRun(id);
      refreshRun();
      refreshSteps();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    }
  }

  if (runError) return <div className="banner banner-error">Failed to load run: {runError}</div>;
  if (!run) return <div className="empty-state">Loading…</div>;

  const canCancel = run.Status === "PENDING" || run.Status === "RUNNING";

  return (
    <div>
      <div className="page-header">
        <div>
          <Link to="/" className="back-link">
            ← All runs
          </Link>
          <h1>
            {run.Name} <span className="muted">v{run.Version}</span>
          </h1>
          <div className="mono muted">{run.ID}</div>
        </div>
        <div className="header-actions">
          <StatusBadge status={run.Status} />
          {canCancel && (
            <button className="btn btn-danger" onClick={cancel}>
              Cancel run
            </button>
          )}
        </div>
      </div>

      {actionError && <div className="banner banner-error">{actionError}</div>}
      {run.Error && <div className="banner banner-error">Run error: {run.Error}</div>}

      <div className="grid-2">
        <section className="panel">
          <h3>Input</h3>
          <JsonView value={run.Input} />
        </section>
        <section className="panel">
          <h3>Output</h3>
          <JsonView value={run.Output} />
        </section>
      </div>

      <section className="panel">
        <h3>Steps</h3>
        <table className="table">
          <thead>
            <tr>
              <th>Step</th>
              <th>Type</th>
              <th>Status</th>
              <th>Depends on</th>
              <th>Attempts</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {(steps ?? []).map((s) => (
              <Fragment key={s.ID}>
                <tr className="clickable" onClick={() => setExpandedStep(expandedStep === s.ID ? null : s.ID)}>
                  <td>{s.StepName}</td>
                  <td className="mono muted">{s.TaskType}</td>
                  <td>
                    <StatusBadge status={s.Status} />
                  </td>
                  <td className="muted">{s.DependsOn.length ? s.DependsOn.join(", ") : "—"}</td>
                  <td className="muted">
                    {s.AttemptCount} / {s.MaxAttempts}
                  </td>
                  <td className="muted">{expandedStep === s.ID ? "▲" : "▼"}</td>
                </tr>
                {expandedStep === s.ID && (
                  <tr key={`${s.ID}-detail`}>
                    <td colSpan={6}>
                      <div className="step-detail">
                        <div className="grid-2">
                          <div>
                            <h4>Input</h4>
                            <JsonView value={s.Input} />
                          </div>
                          <div>
                            <h4>Output</h4>
                            <JsonView value={s.Output} />
                          </div>
                        </div>
                        {s.Error && <div className="banner banner-error">{s.Error}</div>}
                        <h4>Attempts</h4>
                        <table className="table table-nested">
                          <thead>
                            <tr>
                              <th>#</th>
                              <th>Status</th>
                              <th>Worker</th>
                              <th>Queued</th>
                              <th>Completed</th>
                              <th>Error</th>
                            </tr>
                          </thead>
                          <tbody>
                            {s.attempts.map((a) => (
                              <tr key={a.ID}>
                                <td>{a.AttemptNumber}</td>
                                <td>
                                  <StatusBadge status={a.Status} />
                                </td>
                                <td className="mono muted">{a.LeaseOwner ?? "—"}</td>
                                <td className="muted">{new Date(a.QueuedAt).toLocaleTimeString()}</td>
                                <td className="muted">{a.CompletedAt ? new Date(a.CompletedAt).toLocaleTimeString() : "—"}</td>
                                <td className="muted">{a.Error ?? "—"}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
          </tbody>
        </table>
      </section>

      <div className="grid-2">
        <section className="panel">
          <h3>History</h3>
          <ol className="timeline">
            {(history ?? []).map((h) => (
              <li key={h.ID}>
                <span className="mono muted">{new Date(h.CreatedAt).toLocaleTimeString()}</span>
                <span className="timeline-event">{h.EventType}</span>
                <JsonView value={h.Payload} />
              </li>
            ))}
          </ol>
        </section>

        <section className="panel">
          <h3>Send signal</h3>
          <p className="muted">
            Deliver an external event to this run — e.g. resolve a <code>signal_wait</code> step, or send{" "}
            <code>__cancel__</code> to cancel.
          </p>
          <label>
            Signal name
            <input value={signalName} onChange={(e) => setSignalName(e.target.value)} placeholder="await_approval" />
          </label>
          <label>
            Payload (JSON)
            <textarea value={signalPayload} onChange={(e) => setSignalPayload(e.target.value)} rows={4} spellCheck={false} />
          </label>
          <button className="btn btn-primary" onClick={sendSignal} disabled={!signalName}>
            Send signal
          </button>
        </section>
      </div>
    </div>
  );
}
