package store

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ClaimResult tells the caller what happened when it tried to claim a task attempt.
type ClaimResult string

const (
	ClaimOK             ClaimResult = "CLAIMED"
	ClaimAlreadyClaimed ClaimResult = "ALREADY_CLAIMED"
	ClaimNotFound       ClaimResult = "NOT_FOUND"
)

// Queries is every read/write operation the engine and its background services need.
// It is satisfied both by a pool-level Store (autocommit, one statement = one transaction)
// and by the tx-scoped value passed into WithTx's callback, which is what lets the engine
// compose several of these into one ACID transaction (e.g. the outbox write path).
type Queries interface {
	// --- workflow definitions ---
	CreateWorkflowDefinition(ctx context.Context, name string, version int, dag []byte) (*WorkflowDefinition, error)
	GetWorkflowDefinition(ctx context.Context, name string, version int) (*WorkflowDefinition, error)
	GetLatestWorkflowDefinition(ctx context.Context, name string) (*WorkflowDefinition, error)
	ListWorkflowDefinitions(ctx context.Context) ([]WorkflowDefinition, error)

	// --- workflow runs ---
	// CreateWorkflowRun takes a caller-generated ID (rather than a DB default) because the
	// engine needs to compute shard_id = hash(id) deterministically before the insert.
	CreateWorkflowRun(ctx context.Context, id, definitionID uuid.UUID, name string, version int, shardID int, input []byte) (*WorkflowRun, error)
	GetWorkflowRun(ctx context.Context, id uuid.UUID) (*WorkflowRun, error)
	ListWorkflowRuns(ctx context.Context, f RunFilter) ([]WorkflowRun, error)
	ListRunningRunsForShards(ctx context.Context, shardIDs []int) ([]WorkflowRun, error)
	// UpdateWorkflowRunStatus is a CAS: it only applies if the run's current status is in
	// `expected`. Returns (applied, error). Automatically stamps started_at/completed_at.
	UpdateWorkflowRunStatus(ctx context.Context, id uuid.UUID, expected []RunStatus, next RunStatus, output []byte, errMsg *string) (bool, error)

	// --- steps ---
	CreateSteps(ctx context.Context, runID uuid.UUID, steps []NewStep) ([]Step, error)
	GetSteps(ctx context.Context, runID uuid.UUID) ([]Step, error)
	GetStep(ctx context.Context, id uuid.UUID) (*Step, error)
	// UpdateStepStatus is a CAS on current status; bumpAttempt increments attempt_count.
	UpdateStepStatus(ctx context.Context, id uuid.UUID, expected []StepStatus, next StepStatus, output []byte, errMsg *string, bumpAttempt bool) (bool, error)

	// --- task attempts ---
	CreateTaskAttempt(ctx context.Context, stepID, runID uuid.UUID, attemptNumber int, queueName string) (*TaskAttempt, error)
	ClaimTaskAttempt(ctx context.Context, attemptID, idempotencyKey uuid.UUID, workerID string, leaseFor time.Duration) (*TaskAttempt, ClaimResult, error)
	HeartbeatTaskAttempt(ctx context.Context, attemptID uuid.UUID, workerID string, leaseFor time.Duration) (*time.Time, bool, error)
	CompleteTaskAttempt(ctx context.Context, attemptID uuid.UUID, workerID string, success bool, result []byte, errMsg *string) (*TaskAttempt, bool, error)
	ReapExpiredAttempts(ctx context.Context, limit int) ([]TaskAttempt, error)
	GetTaskAttempt(ctx context.Context, id uuid.UUID) (*TaskAttempt, error)
	ListTaskAttemptsForStep(ctx context.Context, stepID uuid.UUID) ([]TaskAttempt, error)

	// --- outbox ---
	InsertOutboxMessage(ctx context.Context, msg NewOutboxMessage) (int64, error)
	// FetchAndLockPendingOutbox selects up to `limit` PENDING rows with FOR UPDATE SKIP LOCKED.
	// Must be called inside WithTx; the caller publishes to the queue and then calls
	// MarkOutboxPublished for each row before the transaction commits.
	FetchAndLockPendingOutbox(ctx context.Context, shardIDs []int, limit int) ([]OutboxMessage, error)
	MarkOutboxPublished(ctx context.Context, id int64) error

	// --- timers ---
	InsertTimer(ctx context.Context, t NewTimer) (*Timer, error)
	// ListDueTimers is a plain, non-mutating read of PENDING timers whose fire_at has
	// passed — a candidate list. It is intentionally *not* how a timer is actually claimed;
	// see FireTimerCAS. Multiple nodes may see the same candidate concurrently; that's fine.
	ListDueTimers(ctx context.Context, shardIDs []int, limit int) ([]Timer, error)
	// FireTimerCAS atomically transitions one timer PENDING->FIRED
	// (`WHERE id=$1 AND status='PENDING'`). The caller (internal/engine) performs this in the
	// same database transaction as the retry-dispatch it triggers, so "timer fired" and "next
	// attempt scheduled" are atomic together. Returns
	// (timer, applied). If another node's poller already claimed it, applied is false.
	FireTimerCAS(ctx context.Context, timerID uuid.UUID) (*Timer, bool, error)
	CancelTimersForRun(ctx context.Context, runID uuid.UUID) error

	// --- signals ---
	InsertSignal(ctx context.Context, runID uuid.UUID, name string, payload []byte) (*Signal, error)
	ListUnprocessedSignals(ctx context.Context, runID uuid.UUID) ([]Signal, error)
	MarkSignalProcessed(ctx context.Context, id uuid.UUID) (bool, error)

	// --- history ---
	AppendHistory(ctx context.Context, runID uuid.UUID, eventType string, payload []byte) (*HistoryEvent, error)
	ListHistory(ctx context.Context, runID uuid.UUID) ([]HistoryEvent, error)
}

// Store adds transaction management on top of Queries. Store itself implements Queries
// directly (each call is its own implicit transaction / autocommit statement).
type Store interface {
	Queries
	// WithTx runs fn inside a single database transaction. If fn returns an error (or
	// panics), the transaction is rolled back; otherwise it is committed. All Queries calls
	// made through the passed-in Queries value participate in that one transaction.
	WithTx(ctx context.Context, fn func(ctx context.Context, q Queries) error) error
	Close()
}
