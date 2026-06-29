// Thin typed client for the server's REST API (internal/httpapi). The Go store types carry
// no `json:` tags, so field names on the wire are the exact exported Go field names
// (PascalCase) — that's mirrored here rather than fighting it with a translation layer.

export type RunStatus = "PENDING" | "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED";
export type StepStatus =
  | "PENDING"
  | "READY"
  | "QUEUED"
  | "RUNNING"
  | "RETRY_BACKOFF"
  | "COMPLETED"
  | "FAILED"
  | "SKIPPED"
  | "CANCELLED"
  | "WAITING";
export type AttemptStatus = "QUEUED" | "LEASED" | "SUCCEEDED" | "FAILED" | "EXPIRED" | "ABANDONED";

export interface WorkflowDefinition {
  ID: string;
  Name: string;
  Version: number;
  DAG: unknown;
  CreatedAt: string;
}

export interface WorkflowRun {
  ID: string;
  DefinitionID: string;
  Name: string;
  Version: number;
  Status: RunStatus;
  ShardID: number;
  Input: unknown;
  Output: unknown;
  Context: unknown;
  Error: string | null;
  HistorySeq: number;
  CreatedAt: string;
  StartedAt: string | null;
  CompletedAt: string | null;
}

export interface TaskAttempt {
  ID: string;
  StepID: string;
  WorkflowRunID: string;
  AttemptNumber: number;
  IdempotencyKey: string;
  Status: AttemptStatus;
  QueueName: string;
  LeaseOwner: string | null;
  LeaseExpiresAt: string | null;
  Result: unknown;
  Error: string | null;
  QueuedAt: string;
  StartedAt: string | null;
  CompletedAt: string | null;
}

export interface Step {
  ID: string;
  WorkflowRunID: string;
  StepName: string;
  TaskType: string;
  DependsOn: string[];
  Status: StepStatus;
  AttemptCount: number;
  MaxAttempts: number;
  Input: unknown;
  Output: unknown;
  Error: string | null;
  InitialBackoffMS: number;
  BackoffMultiplier: number;
  MaxBackoffMS: number;
  TimeoutSeconds: number;
  CreatedAt: string;
  StartedAt: string | null;
  CompletedAt: string | null;
  attempts: TaskAttempt[];
}

export interface HistoryEvent {
  ID: number;
  WorkflowRunID: string;
  Seq: number;
  EventType: string;
  Payload: unknown;
  CreatedAt: string;
}

export interface WorkerInfo {
  worker_id: string;
  queues: string[];
  capacity: number;
  last_seen: string;
}

export interface ClusterStatus {
  sharding_enabled: boolean;
  node_id?: string;
  is_leader?: boolean;
  owned_shards?: number;
  total_shards?: number;
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(path, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  if (!resp.ok) {
    let msg = `${resp.status} ${resp.statusText}`;
    try {
      const body = await resp.json();
      if (body?.error) msg = body.error;
    } catch {
      // ignore
    }
    throw new Error(msg);
  }
  if (resp.status === 204) return undefined as T;
  return resp.json() as Promise<T>;
}

export const api = {
  listDefinitions: () => req<WorkflowDefinition[]>("/api/definitions"),
  createDefinition: (yamlOrJson: string) =>
    req<WorkflowDefinition>("/api/definitions", {
      method: "POST",
      headers: { "Content-Type": "application/yaml" },
      body: yamlOrJson,
    }),

  listRuns: (params?: { status?: string; name?: string; limit?: number }) => {
    const qs = new URLSearchParams();
    if (params?.status) qs.set("status", params.status);
    if (params?.name) qs.set("name", params.name);
    if (params?.limit) qs.set("limit", String(params.limit));
    const suffix = qs.toString() ? `?${qs.toString()}` : "";
    return req<WorkflowRun[]>(`/api/runs${suffix}`);
  },
  createRun: (name: string, input: unknown, version?: number) =>
    req<WorkflowRun>("/api/runs", {
      method: "POST",
      body: JSON.stringify({ name, version: version ?? 0, input: input ?? {} }),
    }),
  getRun: (id: string) => req<WorkflowRun>(`/api/runs/${id}`),
  getSteps: (id: string) => req<Step[]>(`/api/runs/${id}/steps`),
  getHistory: (id: string) => req<HistoryEvent[]>(`/api/runs/${id}/history`),
  sendSignal: (id: string, name: string, payload: unknown) =>
    req<{ status: string }>(`/api/runs/${id}/signal`, {
      method: "POST",
      body: JSON.stringify({ name, payload: payload ?? {} }),
    }),
  cancelRun: (id: string) => req<{ status: string }>(`/api/runs/${id}/cancel`, { method: "POST" }),

  listWorkers: () => req<WorkerInfo[]>("/api/workers"),
  clusterStatus: () => req<ClusterStatus>("/api/cluster"),
};
