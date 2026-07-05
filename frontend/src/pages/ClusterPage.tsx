import { api } from "../api";
import { usePolling } from "../hooks";

export default function ClusterPage() {
  const { data: status, error } = usePolling(() => api.clusterStatus(), 3000, []);

  return (
    <div>
      <div className="page-header">
        <h1>Cluster / Sharding</h1>
      </div>
      <p className="muted">
        Shard ownership is a throughput optimization, not a correctness
        mechanism — every scheduling decision is independently guarded by a compare-and-swap in
        Postgres, so this node only reflects how work is currently partitioned, not who is "allowed"
        to make a given transition.
      </p>
      {error && <div className="banner banner-error">{error}</div>}
      {status && (
        <div className="panel stat-panel">
          {!status.sharding_enabled ? (
            <p>
              Sharding is <strong>disabled</strong> (single-node mode, <code>ENABLE_SHARDING=false</code>).
              This node owns all shards implicitly.
            </p>
          ) : (
            <dl className="stat-grid">
              <dt>Node ID</dt>
              <dd className="mono">{status.node_id}</dd>
              <dt>Leader</dt>
              <dd>{status.is_leader ? "yes" : "no"}</dd>
              <dt>Owned shards</dt>
              <dd>
                {status.owned_shards} / {status.total_shards}
              </dd>
            </dl>
          )}
        </div>
      )}
    </div>
  );
}
