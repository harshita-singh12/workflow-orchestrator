import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api";
import { usePolling } from "../hooks";

export default function NewRunModal({ onClose }: { onClose: () => void }) {
  const navigate = useNavigate();
  const { data: defs } = usePolling(() => api.listDefinitions(), 5000, []);
  const [name, setName] = useState("");
  const [input, setInput] = useState("{}");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const uniqueNames = Array.from(new Set((defs ?? []).map((d) => d.Name)));

  async function submit() {
    setError(null);
    let parsedInput: unknown;
    try {
      parsedInput = JSON.parse(input || "{}");
    } catch {
      setError("Input must be valid JSON");
      return;
    }
    if (!name) {
      setError("Choose a workflow definition");
      return;
    }
    setSubmitting(true);
    try {
      const run = await api.createRun(name, parsedInput);
      onClose();
      navigate(`/runs/${run.ID}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>Start a new run</h2>
        {uniqueNames.length === 0 ? (
          <p className="muted">
            No workflow definitions registered yet. Register one via <code>POST /api/definitions</code> (see
            the <code>examples/</code> directory) then come back here.
          </p>
        ) : (
          <>
            <label>
              Workflow definition
              <select value={name} onChange={(e) => setName(e.target.value)}>
                <option value="">Select…</option>
                {uniqueNames.map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Input (JSON)
              <textarea value={input} onChange={(e) => setInput(e.target.value)} rows={6} spellCheck={false} />
            </label>
          </>
        )}
        {error && <div className="banner banner-error">{error}</div>}
        <div className="modal-actions">
          <button className="btn" onClick={onClose}>
            Cancel
          </button>
          <button className="btn btn-primary" onClick={submit} disabled={submitting || uniqueNames.length === 0}>
            {submitting ? "Starting…" : "Start run"}
          </button>
        </div>
      </div>
    </div>
  );
}
