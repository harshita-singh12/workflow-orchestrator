// Package store defines the persistence interface used by the engine and its background
// services (outbox relay, timer poller, lease reaper). It is intentionally storage-agnostic:
// internal/store/postgres implements it against Postgres/pgx, internal/store/memory
// implements it in-process for unit tests. See README's "Pluggable backends" section for the rationale.
package store

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type RunStatus string

const (
	RunPending   RunStatus = "PENDING"
	RunRunning   RunStatus = "RUNNING"
	RunCompleted RunStatus = "COMPLETED"
	RunFailed    RunStatus = "FAILED"
	RunCancelled RunStatus = "CANCELLED"
)

type StepStatus string

const (
	StepPending      StepStatus = "PENDING"
	StepReady        StepStatus = "READY"
	StepQueued       StepStatus = "QUEUED"
	StepRunning      StepStatus = "RUNNING"
	StepRetryBackoff StepStatus = "RETRY_BACKOFF"
	StepCompleted    StepStatus = "COMPLETED"
	StepFailed       StepStatus = "FAILED"
	StepSkipped      StepStatus = "SKIPPED"
	StepCancelled    StepStatus = "CANCELLED"
	StepWaiting      StepStatus = "WAITING" // signal_wait steps blocked on an external signal
)

// StepTerminal reports whether a step will never transition again.
func StepTerminal(s StepStatus) bool {
	switch s {
	case StepCompleted, StepFailed, StepSkipped, StepCancelled:
		return true
	default:
		return false
	}
}

// RunTerminal reports whether a run will never transition again.
func RunTerminal(s RunStatus) bool {
	switch s {
	case RunCompleted, RunFailed, RunCancelled:
		return true
	default:
		return false
	}
}

type AttemptStatus string

const (
	AttemptQueued    AttemptStatus = "QUEUED"
	AttemptLeased    AttemptStatus = "LEASED"
	AttemptSucceeded AttemptStatus = "SUCCEEDED"
	AttemptFailed    AttemptStatus = "FAILED"
	AttemptExpired   AttemptStatus = "EXPIRED"
	AttemptAbandoned AttemptStatus = "ABANDONED"
)

type TimerKind string

const (
	TimerRetryBackoff TimerKind = "retry_backoff"
	TimerStepTimeout  TimerKind = "step_timeout"
	TimerUser         TimerKind = "user_timer"
)

type TimerStatus string

const (
	TimerPending   TimerStatus = "PENDING"
	TimerFired     TimerStatus = "FIRED"
	TimerCancelled TimerStatus = "CANCELLED"
)

type WorkflowDefinition struct {
	ID        uuid.UUID
	Name      string
	Version   int
	DAG       json.RawMessage
	CreatedAt time.Time
}

type WorkflowRun struct {
	ID           uuid.UUID
	DefinitionID uuid.UUID
	Name         string
	Version      int
	Status       RunStatus
	ShardID      int
	Input        json.RawMessage
	Output       json.RawMessage
	Context      json.RawMessage
	Error        *string
	HistorySeq   int64
	CreatedAt    time.Time
	StartedAt    *time.Time
	CompletedAt  *time.Time
}

type Step struct {
	ID                uuid.UUID
	WorkflowRunID     uuid.UUID
	StepName          string
	TaskType          string
	DependsOn         []string
	Status            StepStatus
	AttemptCount      int
	MaxAttempts       int
	Input             json.RawMessage
	Output            json.RawMessage
	Error             *string
	InitialBackoffMS  int
	BackoffMultiplier float64
	MaxBackoffMS      int
	TimeoutSeconds    int
	CreatedAt         time.Time
	StartedAt         *time.Time
	CompletedAt       *time.Time
}

type NewStep struct {
	StepName          string
	TaskType          string
	DependsOn         []string
	Input             json.RawMessage
	MaxAttempts       int
	InitialBackoffMS  int
	BackoffMultiplier float64
	MaxBackoffMS      int
	TimeoutSeconds    int
}

type TaskAttempt struct {
	ID             uuid.UUID
	StepID         uuid.UUID
	WorkflowRunID  uuid.UUID
	AttemptNumber  int
	IdempotencyKey uuid.UUID
	Status         AttemptStatus
	QueueName      string
	LeaseOwner     *string
	LeaseExpiresAt *time.Time
	Result         json.RawMessage
	Error          *string
	QueuedAt       time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
}

type OutboxMessage struct {
	ID              int64
	AggregateType   string
	AggregateID     uuid.UUID
	ShardID         int
	StreamName      string
	Payload         json.RawMessage
	Status          string
	PublishAttempts int
	CreatedAt       time.Time
	PublishedAt     *time.Time
}

type NewOutboxMessage struct {
	AggregateType string
	AggregateID   uuid.UUID
	ShardID       int
	StreamName    string
	Payload       json.RawMessage
}

type Timer struct {
	ID            uuid.UUID
	WorkflowRunID uuid.UUID
	StepID        *uuid.UUID
	ShardID       int
	Kind          TimerKind
	Status        TimerStatus
	Payload       json.RawMessage
	FireAt        time.Time
	CreatedAt     time.Time
	FiredAt       *time.Time
}

type NewTimer struct {
	WorkflowRunID uuid.UUID
	StepID        *uuid.UUID
	ShardID       int
	Kind          TimerKind
	Payload       json.RawMessage
	FireAt        time.Time
}

type Signal struct {
	ID            uuid.UUID
	WorkflowRunID uuid.UUID
	SignalName    string
	Payload       json.RawMessage
	Processed     bool
	ReceivedAt    time.Time
}

type HistoryEvent struct {
	ID            int64
	WorkflowRunID uuid.UUID
	Seq           int64
	EventType     string
	Payload       json.RawMessage
	CreatedAt     time.Time
}

// TaskPayload is what actually gets marshalled into the outbox row / Redis Stream message
// for a task_attempt dispatch. Workers unmarshal this after XREADGROUP.
type TaskPayload struct {
	TaskAttemptID  uuid.UUID       `json:"task_attempt_id"`
	StepID         uuid.UUID       `json:"step_id"`
	WorkflowRunID  uuid.UUID       `json:"workflow_run_id"`
	StepName       string          `json:"step_name"`
	TaskType       string          `json:"task_type"`
	Input          json.RawMessage `json:"input"`
	AttemptNumber  int             `json:"attempt_number"`
	IdempotencyKey uuid.UUID       `json:"idempotency_key"`
	TimeoutSeconds int             `json:"timeout_seconds"`
}

type RunFilter struct {
	Status *RunStatus
	Name   *string
	Limit  int
	Offset int
}

// ErrNotFound is returned by single-row getters when nothing matches.
var ErrNotFound = errNotFound{}

type errNotFound struct{}

func (errNotFound) Error() string { return "store: not found" }

// ErrConflict is returned when a write would violate a uniqueness constraint (e.g.
// registering a workflow definition whose name+version already exists).
var ErrConflict = errConflict{}

type errConflict struct{}

func (errConflict) Error() string { return "store: conflict" }
