import { useState } from "react";
import { api } from "../api";
import { usePolling } from "../hooks";

const PLACEHOLDER = `name: my-workflow
version: 1
steps:
  - name: step_one
    type: noop
  - name: step_two
    type: noop
    depends_on: [step_one]
`;

export default function DefinitionsPage() {
  const { data: defs, error, refresh } = usePolling(() => api.listDefinitions(), 5000, []);
  const [yaml, setYaml] = useState(PLACEHOLDER);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function submit() {
    setSubmitError(null);
    setSubmitting(true);
    try {
      await api.createDefinition(yaml);
      refresh();
    } catch (e) {
      setSubmitError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div>
      <div className="page-header">
        <h1>Workflow Definitions</h1>
      </div>

      {error && <div className="banner banner-error">{error}</div>}
      {defs && defs.length > 0 && (
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Version</th>
              <th>Steps</th>
              <th>Registered</th>
            </tr>
          </thead>
          <tbody>
            {defs.map((d) => {
              const stepCount = Array.isArray((d.DAG as { steps?: unknown[] })?.steps)
                ? (d.DAG as { steps: unknown[] }).steps.length
                : "?";
              return (
                <tr key={d.ID}>
                  <td>{d.Name}</td>
                  <td className="muted">{d.Version}</td>
                  <td className="muted">{stepCount}</td>
                  <td className="muted">{new Date(d.CreatedAt).toLocaleString()}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
      {defs && defs.length === 0 && <div className="empty-state">No definitions registered yet.</div>}

      <section className="panel">
        <h3>Register a new definition</h3>
        <p className="muted">
          Paste a YAML or JSON DAG (see the <code>examples/</code> directory for ready-made ones covering
          sequential steps, parallel fan-out/fan-in, retries, and signals).
        </p>
        <textarea
          className="yaml-editor"
          value={yaml}
          onChange={(e) => setYaml(e.target.value)}
          rows={14}
          spellCheck={false}
        />
        {submitError && <div className="banner banner-error">{submitError}</div>}
        <button className="btn btn-primary" onClick={submit} disabled={submitting}>
          {submitting ? "Registering…" : "Register definition"}
        </button>
      </section>
    </div>
  );
}
