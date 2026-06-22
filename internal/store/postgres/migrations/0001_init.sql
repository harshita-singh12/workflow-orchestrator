-- Initial schema for the workflow orchestrator.
-- See DESIGN.md section 2 for the full rationale behind each table and index.

CREATE EXTENSION IF NOT EXISTS pgcrypto; -- gen_random_uuid()

CREATE TABLE workflow_definitions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    version     INT NOT NULL,
    dag         JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

CREATE TABLE workflow_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    definition_id   UUID NOT NULL REFERENCES workflow_definitions(id),
    name            TEXT NOT NULL,
    version         INT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED','CANCELLED')),
    shard_id        INT NOT NULL,
    input           JSONB NOT NULL DEFAULT '{}',
    output          JSONB,
    context         JSONB NOT NULL DEFAULT '{}',
    error           TEXT,
    history_seq     BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ
);
CREATE INDEX idx_workflow_runs_shard_status ON workflow_runs (shard_id, status) WHERE status = 'RUNNING';
CREATE INDEX idx_workflow_runs_status ON workflow_runs (status);
CREATE INDEX idx_workflow_runs_created_at ON workflow_runs (created_at DESC);

CREATE TABLE steps (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id     UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    step_name           TEXT NOT NULL,
    task_type           TEXT NOT NULL,
    depends_on          JSONB NOT NULL DEFAULT '[]',
    status              TEXT NOT NULL DEFAULT 'PENDING'
                            CHECK (status IN ('PENDING','READY','QUEUED','RUNNING','RETRY_BACKOFF','COMPLETED','FAILED','SKIPPED','CANCELLED','WAITING')),
    attempt_count       INT NOT NULL DEFAULT 0,
    max_attempts        INT NOT NULL DEFAULT 1,
    input               JSONB NOT NULL DEFAULT '{}',
    output              JSONB,
    error               TEXT,
    initial_backoff_ms  INT NOT NULL DEFAULT 1000,
    backoff_multiplier  DOUBLE PRECISION NOT NULL DEFAULT 2.0,
    max_backoff_ms      INT NOT NULL DEFAULT 60000,
    timeout_seconds     INT NOT NULL DEFAULT 30,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    UNIQUE (workflow_run_id, step_name)
);
CREATE INDEX idx_steps_run_status ON steps (workflow_run_id, status);

CREATE TABLE task_attempts (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    step_id             UUID NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
    workflow_run_id     UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    attempt_number      INT NOT NULL,
    idempotency_key     UUID NOT NULL DEFAULT gen_random_uuid(),
    status              TEXT NOT NULL DEFAULT 'QUEUED'
                            CHECK (status IN ('QUEUED','LEASED','SUCCEEDED','FAILED','EXPIRED','ABANDONED')),
    queue_name          TEXT NOT NULL,
    lease_owner         TEXT,
    lease_expires_at    TIMESTAMPTZ,
    result              JSONB,
    error                TEXT,
    queued_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    UNIQUE (step_id, attempt_number)
);
CREATE INDEX idx_task_attempts_leased_expiry ON task_attempts (status, lease_expires_at) WHERE status = 'LEASED';
CREATE INDEX idx_task_attempts_run ON task_attempts (workflow_run_id);

CREATE TABLE outbox (
    id                  BIGSERIAL PRIMARY KEY,
    aggregate_type      TEXT NOT NULL,
    aggregate_id        UUID NOT NULL,
    shard_id            INT NOT NULL,
    stream_name         TEXT NOT NULL,
    payload             JSONB NOT NULL,
    status              TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','PUBLISHED','FAILED')),
    publish_attempts    INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at        TIMESTAMPTZ
);
CREATE INDEX idx_outbox_pending ON outbox (id) WHERE status = 'PENDING';
CREATE INDEX idx_outbox_shard_pending ON outbox (shard_id, id) WHERE status = 'PENDING';

CREATE TABLE timers (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id     UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    step_id             UUID REFERENCES steps(id) ON DELETE CASCADE,
    shard_id            INT NOT NULL,
    kind                TEXT NOT NULL CHECK (kind IN ('retry_backoff','step_timeout','user_timer')),
    status              TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','FIRED','CANCELLED')),
    payload             JSONB NOT NULL DEFAULT '{}',
    fire_at             TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    fired_at            TIMESTAMPTZ
);
CREATE INDEX idx_timers_pending_fireat ON timers (fire_at) WHERE status = 'PENDING';
CREATE INDEX idx_timers_shard_pending ON timers (shard_id, fire_at) WHERE status = 'PENDING';

CREATE TABLE signals (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id     UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    signal_name         TEXT NOT NULL,
    payload             JSONB NOT NULL DEFAULT '{}',
    processed           BOOLEAN NOT NULL DEFAULT false,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_signals_run_unprocessed ON signals (workflow_run_id) WHERE processed = false;

CREATE TABLE workflow_run_history (
    id                  BIGSERIAL PRIMARY KEY,
    workflow_run_id     UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    seq                 BIGINT NOT NULL,
    event_type          TEXT NOT NULL,
    payload             JSONB NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workflow_run_id, seq)
);
CREATE INDEX idx_history_run ON workflow_run_history (workflow_run_id, seq);
