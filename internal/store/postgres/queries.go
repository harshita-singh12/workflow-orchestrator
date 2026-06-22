package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/aryanraj/workflow-orchestrator/internal/store"
)

// queries implements store.Queries against any dbtx (pool or transaction).
type queries struct {
	db dbtx
}

func wrapErr(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound
	}
	return err
}

// ---------------------------------------------------------------------------
// workflow definitions
// ---------------------------------------------------------------------------

func (q *queries) CreateWorkflowDefinition(ctx context.Context, name string, version int, dag []byte) (*store.WorkflowDefinition, error) {
	row := q.db.QueryRow(ctx, `INSERT INTO workflow_definitions (name, version, dag) VALUES ($1,$2,$3)
		RETURNING id, name, version, dag, created_at`, name, version, dag)
	return scanWorkflowDefinition(row)
}

func (q *queries) GetWorkflowDefinition(ctx context.Context, name string, version int) (*store.WorkflowDefinition, error) {
	row := q.db.QueryRow(ctx, `SELECT id, name, version, dag, created_at FROM workflow_definitions WHERE name=$1 AND version=$2`, name, version)
	d, err := scanWorkflowDefinition(row)
	if err != nil {
		return nil, wrapErr(err)
	}
	return d, nil
}

func (q *queries) GetLatestWorkflowDefinition(ctx context.Context, name string) (*store.WorkflowDefinition, error) {
	row := q.db.QueryRow(ctx, `SELECT id, name, version, dag, created_at FROM workflow_definitions WHERE name=$1 ORDER BY version DESC LIMIT 1`, name)
	d, err := scanWorkflowDefinition(row)
	if err != nil {
		return nil, wrapErr(err)
	}
	return d, nil
}

func (q *queries) ListWorkflowDefinitions(ctx context.Context) ([]store.WorkflowDefinition, error) {
	rows, err := q.db.Query(ctx, `SELECT id, name, version, dag, created_at FROM workflow_definitions ORDER BY name, version DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.WorkflowDefinition
	for rows.Next() {
		d, err := scanWorkflowDefinition(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *d)
	}
	return out, rows.Err()
}

func scanWorkflowDefinition(row pgx.Row) (*store.WorkflowDefinition, error) {
	var d store.WorkflowDefinition
	var dag []byte
	if err := row.Scan(&d.ID, &d.Name, &d.Version, &dag, &d.CreatedAt); err != nil {
		return nil, err
	}
	d.DAG = dag
	return &d, nil
}

// ---------------------------------------------------------------------------
// workflow runs
// ---------------------------------------------------------------------------

func (q *queries) CreateWorkflowRun(ctx context.Context, id, definitionID uuid.UUID, name string, version int, shardID int, input []byte) (*store.WorkflowRun, error) {
	row := q.db.QueryRow(ctx, `INSERT INTO workflow_runs (id, definition_id, name, version, status, shard_id, input, context)
		VALUES ($1,$2,$3,$4,'PENDING',$5,$6,'{}')
		RETURNING id, definition_id, name, version, status, shard_id, input, output, context, error, history_seq, created_at, started_at, completed_at`,
		id, definitionID, name, version, shardID, input)
	return scanWorkflowRun(row)
}

func (q *queries) GetWorkflowRun(ctx context.Context, id uuid.UUID) (*store.WorkflowRun, error) {
	row := q.db.QueryRow(ctx, `SELECT id, definition_id, name, version, status, shard_id, input, output, context, error, history_seq, created_at, started_at, completed_at
		FROM workflow_runs WHERE id=$1`, id)
	r, err := scanWorkflowRun(row)
	if err != nil {
		return nil, wrapErr(err)
	}
	return r, nil
}

func (q *queries) ListWorkflowRuns(ctx context.Context, f store.RunFilter) ([]store.WorkflowRun, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	sql := `SELECT id, definition_id, name, version, status, shard_id, input, output, context, error, history_seq, created_at, started_at, completed_at
		FROM workflow_runs WHERE ($1::text IS NULL OR status=$1) AND ($2::text IS NULL OR name=$2)
		ORDER BY created_at DESC LIMIT $3 OFFSET $4`
	var statusArg, nameArg *string
	if f.Status != nil {
		s := string(*f.Status)
		statusArg = &s
	}
	nameArg = f.Name
	rows, err := q.db.Query(ctx, sql, statusArg, nameArg, limit, f.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.WorkflowRun
	for rows.Next() {
		r, err := scanWorkflowRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func (q *queries) ListRunningRunsForShards(ctx context.Context, shardIDs []int) ([]store.WorkflowRun, error) {
	rows, err := q.db.Query(ctx, `SELECT id, definition_id, name, version, status, shard_id, input, output, context, error, history_seq, created_at, started_at, completed_at
		FROM workflow_runs WHERE status IN ('PENDING','RUNNING') AND shard_id = ANY($1)`, shardIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.WorkflowRun
	for rows.Next() {
		r, err := scanWorkflowRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func (q *queries) UpdateWorkflowRunStatus(ctx context.Context, id uuid.UUID, expected []store.RunStatus, next store.RunStatus, output []byte, errMsg *string) (bool, error) {
	expStrs := statusesToStrings(expected)
	tag, err := q.db.Exec(ctx, `UPDATE workflow_runs SET
			status=$1,
			output=COALESCE($2, output),
			error=COALESCE($3, error),
			started_at=CASE WHEN $1='RUNNING' AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at=CASE WHEN $1 IN ('COMPLETED','FAILED','CANCELLED') THEN now() ELSE completed_at END
		WHERE id=$4 AND status = ANY($5)`,
		string(next), output, errMsg, id, expStrs)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func scanWorkflowRun(row pgx.Row) (*store.WorkflowRun, error) {
	var r store.WorkflowRun
	var input, output, ctxb []byte
	var status string
	if err := row.Scan(&r.ID, &r.DefinitionID, &r.Name, &r.Version, &status, &r.ShardID, &input, &output, &ctxb, &r.Error, &r.HistorySeq, &r.CreatedAt, &r.StartedAt, &r.CompletedAt); err != nil {
		return nil, err
	}
	r.Status = store.RunStatus(status)
	r.Input = input
	r.Output = output
	r.Context = ctxb
	return &r, nil
}

// ---------------------------------------------------------------------------
// steps
// ---------------------------------------------------------------------------

func (q *queries) CreateSteps(ctx context.Context, runID uuid.UUID, steps []store.NewStep) ([]store.Step, error) {
	out := make([]store.Step, 0, len(steps))
	for _, ns := range steps {
		dependsOnJSON, err := json.Marshal(ns.DependsOn)
		if err != nil {
			return nil, err
		}
		input := ns.Input
		if input == nil {
			input = json.RawMessage(`{}`)
		}
		row := q.db.QueryRow(ctx, `INSERT INTO steps
			(workflow_run_id, step_name, task_type, depends_on, status, max_attempts, input,
			 initial_backoff_ms, backoff_multiplier, max_backoff_ms, timeout_seconds)
			VALUES ($1,$2,$3,$4,'PENDING',$5,$6,$7,$8,$9,$10)
			RETURNING id, workflow_run_id, step_name, task_type, depends_on, status, attempt_count, max_attempts,
			          input, output, error, initial_backoff_ms, backoff_multiplier, max_backoff_ms, timeout_seconds,
			          created_at, started_at, completed_at`,
			runID, ns.StepName, ns.TaskType, dependsOnJSON, ns.MaxAttempts, input,
			ns.InitialBackoffMS, ns.BackoffMultiplier, ns.MaxBackoffMS, ns.TimeoutSeconds)
		s, err := scanStep(row)
		if err != nil {
			return nil, err
		}
		out = append(out, *s)
	}
	return out, nil
}

func (q *queries) GetSteps(ctx context.Context, runID uuid.UUID) ([]store.Step, error) {
	rows, err := q.db.Query(ctx, `SELECT id, workflow_run_id, step_name, task_type, depends_on, status, attempt_count, max_attempts,
			input, output, error, initial_backoff_ms, backoff_multiplier, max_backoff_ms, timeout_seconds,
			created_at, started_at, completed_at
		FROM steps WHERE workflow_run_id=$1 ORDER BY created_at`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Step
	for rows.Next() {
		s, err := scanStep(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *s)
	}
	return out, rows.Err()
}

func (q *queries) GetStep(ctx context.Context, id uuid.UUID) (*store.Step, error) {
	row := q.db.QueryRow(ctx, `SELECT id, workflow_run_id, step_name, task_type, depends_on, status, attempt_count, max_attempts,
			input, output, error, initial_backoff_ms, backoff_multiplier, max_backoff_ms, timeout_seconds,
			created_at, started_at, completed_at
		FROM steps WHERE id=$1`, id)
	s, err := scanStep(row)
	if err != nil {
		return nil, wrapErr(err)
	}
	return s, nil
}

func (q *queries) UpdateStepStatus(ctx context.Context, id uuid.UUID, expected []store.StepStatus, next store.StepStatus, output []byte, errMsg *string, bumpAttempt bool) (bool, error) {
	expStrs := make([]string, len(expected))
	for i, e := range expected {
		expStrs[i] = string(e)
	}
	tag, err := q.db.Exec(ctx, `UPDATE steps SET
			status=$1,
			output=COALESCE($2, output),
			error=COALESCE($3, error),
			attempt_count = attempt_count + CASE WHEN $4 THEN 1 ELSE 0 END,
			started_at = CASE WHEN $1 IN ('QUEUED','RUNNING') AND started_at IS NULL THEN now() ELSE started_at END,
			completed_at = CASE WHEN $1 IN ('COMPLETED','FAILED','SKIPPED','CANCELLED') THEN now() ELSE completed_at END
		WHERE id=$5 AND status = ANY($6)`,
		string(next), output, errMsg, bumpAttempt, id, expStrs)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func scanStep(row pgx.Row) (*store.Step, error) {
	var s store.Step
	var input, output []byte
	var dependsOn []byte
	var status string
	if err := row.Scan(&s.ID, &s.WorkflowRunID, &s.StepName, &s.TaskType, &dependsOn, &status, &s.AttemptCount, &s.MaxAttempts,
		&input, &output, &s.Error, &s.InitialBackoffMS, &s.BackoffMultiplier, &s.MaxBackoffMS, &s.TimeoutSeconds,
		&s.CreatedAt, &s.StartedAt, &s.CompletedAt); err != nil {
		return nil, err
	}
	s.Status = store.StepStatus(status)
	s.Input = input
	s.Output = output
	if len(dependsOn) > 0 {
		if err := json.Unmarshal(dependsOn, &s.DependsOn); err != nil {
			return nil, fmt.Errorf("decode depends_on: %w", err)
		}
	}
	return &s, nil
}

// ---------------------------------------------------------------------------
// task attempts
// ---------------------------------------------------------------------------

func (q *queries) CreateTaskAttempt(ctx context.Context, stepID, runID uuid.UUID, attemptNumber int, queueName string) (*store.TaskAttempt, error) {
	row := q.db.QueryRow(ctx, `INSERT INTO task_attempts (step_id, workflow_run_id, attempt_number, status, queue_name)
		VALUES ($1,$2,$3,'QUEUED',$4)
		RETURNING id, step_id, workflow_run_id, attempt_number, idempotency_key, status, queue_name,
		          lease_owner, lease_expires_at, result, error, queued_at, started_at, completed_at`,
		stepID, runID, attemptNumber, queueName)
	return scanTaskAttempt(row)
}

func (q *queries) ClaimTaskAttempt(ctx context.Context, attemptID, idempotencyKey uuid.UUID, workerID string, leaseFor time.Duration) (*store.TaskAttempt, store.ClaimResult, error) {
	row := q.db.QueryRow(ctx, `UPDATE task_attempts SET
			status='LEASED', lease_owner=$1, lease_expires_at = now() + ($2 * interval '1 second'), started_at = now()
		WHERE id=$3 AND idempotency_key=$4 AND status='QUEUED'
		RETURNING id, step_id, workflow_run_id, attempt_number, idempotency_key, status, queue_name,
		          lease_owner, lease_expires_at, result, error, queued_at, started_at, completed_at`,
		workerID, leaseFor.Seconds(), attemptID, idempotencyKey)
	att, err := scanTaskAttempt(row)
	if err == nil {
		return att, store.ClaimOK, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, "", err
	}
	// Distinguish "doesn't exist" from "exists but already claimed/terminal".
	existing, gerr := q.GetTaskAttempt(ctx, attemptID)
	if gerr != nil {
		if errors.Is(gerr, store.ErrNotFound) {
			return nil, store.ClaimNotFound, nil
		}
		return nil, "", gerr
	}
	return existing, store.ClaimAlreadyClaimed, nil
}

func (q *queries) HeartbeatTaskAttempt(ctx context.Context, attemptID uuid.UUID, workerID string, leaseFor time.Duration) (*time.Time, bool, error) {
	row := q.db.QueryRow(ctx, `UPDATE task_attempts SET lease_expires_at = now() + ($1 * interval '1 second')
		WHERE id=$2 AND lease_owner=$3 AND status='LEASED'
		RETURNING lease_expires_at`, leaseFor.Seconds(), attemptID, workerID)
	var exp time.Time
	if err := row.Scan(&exp); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &exp, true, nil
}

func (q *queries) CompleteTaskAttempt(ctx context.Context, attemptID uuid.UUID, workerID string, success bool, result []byte, errMsg *string) (*store.TaskAttempt, bool, error) {
	status := "FAILED"
	if success {
		status = "SUCCEEDED"
	}
	row := q.db.QueryRow(ctx, `UPDATE task_attempts SET status=$1, result=$2, error=$3, completed_at=now()
		WHERE id=$4 AND lease_owner=$5 AND status='LEASED'
		RETURNING id, step_id, workflow_run_id, attempt_number, idempotency_key, status, queue_name,
		          lease_owner, lease_expires_at, result, error, queued_at, started_at, completed_at`,
		status, result, errMsg, attemptID, workerID)
	att, err := scanTaskAttempt(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Stale/duplicate report: return current state (if any) without applying it.
			existing, gerr := q.GetTaskAttempt(ctx, attemptID)
			if gerr != nil {
				return nil, false, gerr
			}
			return existing, false, nil
		}
		return nil, false, err
	}
	return att, true, nil
}

func (q *queries) ReapExpiredAttempts(ctx context.Context, limit int) ([]store.TaskAttempt, error) {
	rows, err := q.db.Query(ctx, `UPDATE task_attempts SET status='EXPIRED', completed_at=now(), error='lease expired'
		WHERE id IN (
			SELECT id FROM task_attempts WHERE status='LEASED' AND lease_expires_at < now()
			ORDER BY lease_expires_at LIMIT $1 FOR UPDATE SKIP LOCKED
		)
		RETURNING id, step_id, workflow_run_id, attempt_number, idempotency_key, status, queue_name,
		          lease_owner, lease_expires_at, result, error, queued_at, started_at, completed_at`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.TaskAttempt
	for rows.Next() {
		a, err := scanTaskAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

func (q *queries) GetTaskAttempt(ctx context.Context, id uuid.UUID) (*store.TaskAttempt, error) {
	row := q.db.QueryRow(ctx, `SELECT id, step_id, workflow_run_id, attempt_number, idempotency_key, status, queue_name,
		          lease_owner, lease_expires_at, result, error, queued_at, started_at, completed_at
		FROM task_attempts WHERE id=$1`, id)
	a, err := scanTaskAttempt(row)
	if err != nil {
		return nil, wrapErr(err)
	}
	return a, nil
}

func (q *queries) ListTaskAttemptsForStep(ctx context.Context, stepID uuid.UUID) ([]store.TaskAttempt, error) {
	rows, err := q.db.Query(ctx, `SELECT id, step_id, workflow_run_id, attempt_number, idempotency_key, status, queue_name,
		          lease_owner, lease_expires_at, result, error, queued_at, started_at, completed_at
		FROM task_attempts WHERE step_id=$1 ORDER BY attempt_number`, stepID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.TaskAttempt
	for rows.Next() {
		a, err := scanTaskAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

func scanTaskAttempt(row pgx.Row) (*store.TaskAttempt, error) {
	var a store.TaskAttempt
	var status string
	var result []byte
	if err := row.Scan(&a.ID, &a.StepID, &a.WorkflowRunID, &a.AttemptNumber, &a.IdempotencyKey, &status, &a.QueueName,
		&a.LeaseOwner, &a.LeaseExpiresAt, &result, &a.Error, &a.QueuedAt, &a.StartedAt, &a.CompletedAt); err != nil {
		return nil, err
	}
	a.Status = store.AttemptStatus(status)
	a.Result = result
	return &a, nil
}

// ---------------------------------------------------------------------------
// outbox
// ---------------------------------------------------------------------------

func (q *queries) InsertOutboxMessage(ctx context.Context, msg store.NewOutboxMessage) (int64, error) {
	var id int64
	err := q.db.QueryRow(ctx, `INSERT INTO outbox (aggregate_type, aggregate_id, shard_id, stream_name, payload, status)
		VALUES ($1,$2,$3,$4,$5,'PENDING') RETURNING id`,
		msg.AggregateType, msg.AggregateID, msg.ShardID, msg.StreamName, msg.Payload).Scan(&id)
	return id, err
}

func (q *queries) FetchAndLockPendingOutbox(ctx context.Context, shardIDs []int, limit int) ([]store.OutboxMessage, error) {
	rows, err := q.db.Query(ctx, `SELECT id, aggregate_type, aggregate_id, shard_id, stream_name, payload, status, publish_attempts, created_at, published_at
		FROM outbox WHERE status='PENDING' AND ($1::int[] IS NULL OR shard_id = ANY($1))
		ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED`, intSliceOrNil(shardIDs), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.OutboxMessage
	for rows.Next() {
		var m store.OutboxMessage
		var payload []byte
		if err := rows.Scan(&m.ID, &m.AggregateType, &m.AggregateID, &m.ShardID, &m.StreamName, &payload, &m.Status, &m.PublishAttempts, &m.CreatedAt, &m.PublishedAt); err != nil {
			return nil, err
		}
		m.Payload = payload
		out = append(out, m)
	}
	return out, rows.Err()
}

func (q *queries) MarkOutboxPublished(ctx context.Context, id int64) error {
	_, err := q.db.Exec(ctx, `UPDATE outbox SET status='PUBLISHED', published_at=now() WHERE id=$1`, id)
	return err
}

// ---------------------------------------------------------------------------
// timers
// ---------------------------------------------------------------------------

func (q *queries) InsertTimer(ctx context.Context, t store.NewTimer) (*store.Timer, error) {
	payload := t.Payload
	if payload == nil {
		payload = json.RawMessage(`{}`)
	}
	row := q.db.QueryRow(ctx, `INSERT INTO timers (workflow_run_id, step_id, shard_id, kind, status, payload, fire_at)
		VALUES ($1,$2,$3,$4,'PENDING',$5,$6)
		RETURNING id, workflow_run_id, step_id, shard_id, kind, status, payload, fire_at, created_at, fired_at`,
		t.WorkflowRunID, t.StepID, t.ShardID, string(t.Kind), payload, t.FireAt)
	return scanTimer(row)
}

func (q *queries) ListDueTimers(ctx context.Context, shardIDs []int, limit int) ([]store.Timer, error) {
	rows, err := q.db.Query(ctx, `SELECT id, workflow_run_id, step_id, shard_id, kind, status, payload, fire_at, created_at, fired_at
		FROM timers WHERE status='PENDING' AND fire_at <= now() AND ($1::int[] IS NULL OR shard_id = ANY($1))
		ORDER BY fire_at LIMIT $2`, intSliceOrNil(shardIDs), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Timer
	for rows.Next() {
		t, err := scanTimer(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

func (q *queries) FireTimerCAS(ctx context.Context, timerID uuid.UUID) (*store.Timer, bool, error) {
	row := q.db.QueryRow(ctx, `UPDATE timers SET status='FIRED', fired_at=now()
		WHERE id=$1 AND status='PENDING'
		RETURNING id, workflow_run_id, step_id, shard_id, kind, status, payload, fire_at, created_at, fired_at`, timerID)
	t, err := scanTimer(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return t, true, nil
}

func (q *queries) CancelTimersForRun(ctx context.Context, runID uuid.UUID) error {
	_, err := q.db.Exec(ctx, `UPDATE timers SET status='CANCELLED' WHERE workflow_run_id=$1 AND status='PENDING'`, runID)
	return err
}

func scanTimer(row pgx.Row) (*store.Timer, error) {
	var t store.Timer
	var kind, status string
	var payload []byte
	if err := row.Scan(&t.ID, &t.WorkflowRunID, &t.StepID, &t.ShardID, &kind, &status, &payload, &t.FireAt, &t.CreatedAt, &t.FiredAt); err != nil {
		return nil, err
	}
	t.Kind = store.TimerKind(kind)
	t.Status = store.TimerStatus(status)
	t.Payload = payload
	return &t, nil
}

// ---------------------------------------------------------------------------
// signals
// ---------------------------------------------------------------------------

func (q *queries) InsertSignal(ctx context.Context, runID uuid.UUID, name string, payload []byte) (*store.Signal, error) {
	if payload == nil {
		payload = json.RawMessage(`{}`)
	}
	row := q.db.QueryRow(ctx, `INSERT INTO signals (workflow_run_id, signal_name, payload) VALUES ($1,$2,$3)
		RETURNING id, workflow_run_id, signal_name, payload, processed, received_at`, runID, name, payload)
	return scanSignal(row)
}

func (q *queries) ListUnprocessedSignals(ctx context.Context, runID uuid.UUID) ([]store.Signal, error) {
	rows, err := q.db.Query(ctx, `SELECT id, workflow_run_id, signal_name, payload, processed, received_at
		FROM signals WHERE workflow_run_id=$1 AND processed=false ORDER BY received_at`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Signal
	for rows.Next() {
		s, err := scanSignal(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *s)
	}
	return out, rows.Err()
}

func (q *queries) MarkSignalProcessed(ctx context.Context, id uuid.UUID) (bool, error) {
	tag, err := q.db.Exec(ctx, `UPDATE signals SET processed=true WHERE id=$1 AND processed=false`, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func scanSignal(row pgx.Row) (*store.Signal, error) {
	var s store.Signal
	var payload []byte
	if err := row.Scan(&s.ID, &s.WorkflowRunID, &s.SignalName, &payload, &s.Processed, &s.ReceivedAt); err != nil {
		return nil, err
	}
	s.Payload = payload
	return &s, nil
}

// ---------------------------------------------------------------------------
// history
// ---------------------------------------------------------------------------

func (q *queries) AppendHistory(ctx context.Context, runID uuid.UUID, eventType string, payload []byte) (*store.HistoryEvent, error) {
	if payload == nil {
		payload = json.RawMessage(`{}`)
	}
	row := q.db.QueryRow(ctx, `WITH next_seq AS (
			UPDATE workflow_runs SET history_seq = history_seq + 1 WHERE id=$1 RETURNING history_seq
		)
		INSERT INTO workflow_run_history (workflow_run_id, seq, event_type, payload)
		SELECT $1, next_seq.history_seq, $2, $3 FROM next_seq
		RETURNING id, workflow_run_id, seq, event_type, payload, created_at`, runID, eventType, payload)
	var h store.HistoryEvent
	var p []byte
	if err := row.Scan(&h.ID, &h.WorkflowRunID, &h.Seq, &h.EventType, &p, &h.CreatedAt); err != nil {
		return nil, err
	}
	h.Payload = p
	return &h, nil
}

func (q *queries) ListHistory(ctx context.Context, runID uuid.UUID) ([]store.HistoryEvent, error) {
	rows, err := q.db.Query(ctx, `SELECT id, workflow_run_id, seq, event_type, payload, created_at
		FROM workflow_run_history WHERE workflow_run_id=$1 ORDER BY seq`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.HistoryEvent
	for rows.Next() {
		var h store.HistoryEvent
		var p []byte
		if err := rows.Scan(&h.ID, &h.WorkflowRunID, &h.Seq, &h.EventType, &p, &h.CreatedAt); err != nil {
			return nil, err
		}
		h.Payload = p
		out = append(out, h)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func statusesToStrings(ss []store.RunStatus) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}

// intSliceOrNil lets us pass a Go nil slice through to Postgres as a real SQL NULL (so the
// `$1::int[] IS NULL OR ...` clause means "no shard filter"), since pgx encodes a nil
// []int as an empty (non-NULL) array rather than NULL.
func intSliceOrNil(xs []int) any {
	if xs == nil {
		return nil
	}
	return xs
}
