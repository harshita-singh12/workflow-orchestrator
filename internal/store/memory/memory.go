// Package memory is a full in-process implementation of store.Store, used by unit tests so
// the engine's scheduling logic can be tested without Docker/Postgres. It intentionally
// implements the exact same CAS/atomicity semantics as the Postgres implementation (guarded
// by a single mutex, which is trivially correct for an in-process store) so that engine
// tests exercise real compare-and-swap races, not a simplified stand-in.
package memory

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/aryanraj/workflow-orchestrator/internal/store"
)

type Store struct {
	mu sync.Mutex

	definitions  map[uuid.UUID]*store.WorkflowDefinition
	runs         map[uuid.UUID]*store.WorkflowRun
	steps        map[uuid.UUID]*store.Step
	attempts     map[uuid.UUID]*store.TaskAttempt
	outbox       map[int64]*store.OutboxMessage
	timers       map[uuid.UUID]*store.Timer
	signals      map[uuid.UUID]*store.Signal
	history      map[uuid.UUID][]*store.HistoryEvent
	nextOutboxID int64
}

func New() *Store {
	return &Store{
		definitions: map[uuid.UUID]*store.WorkflowDefinition{},
		runs:        map[uuid.UUID]*store.WorkflowRun{},
		steps:       map[uuid.UUID]*store.Step{},
		attempts:    map[uuid.UUID]*store.TaskAttempt{},
		outbox:      map[int64]*store.OutboxMessage{},
		timers:      map[uuid.UUID]*store.Timer{},
		signals:     map[uuid.UUID]*store.Signal{},
		history:     map[uuid.UUID][]*store.HistoryEvent{},
	}
}

func (s *Store) Close() {}

// WithTx: an in-process store can just hold the single mutex for the whole callback, which
// gives real all-or-nothing atomicity (no partial writes are ever observable by another
// goroutine) without needing to implement a rollback log — good enough, and correct, for a
// test double.
func (s *Store) WithTx(ctx context.Context, fn func(ctx context.Context, q store.Queries) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(ctx, (*txQueries)(s))
}

// The pool-level Store methods below take the lock themselves; txQueries (used inside
// WithTx) is the same receiver type without re-locking, since the lock is already held.
// We implement every method once on `Store` un-locked (`raw*`) and expose two thin wrappers:
// Store (locks) and txQueries (doesn't).

type txQueries Store

// ---- definitions ----

func (s *Store) CreateWorkflowDefinition(ctx context.Context, name string, version int, dag []byte) (*store.WorkflowDefinition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).CreateWorkflowDefinition(ctx, name, version, dag)
}
func (t *txQueries) CreateWorkflowDefinition(ctx context.Context, name string, version int, dag []byte) (*store.WorkflowDefinition, error) {
	d := &store.WorkflowDefinition{ID: uuid.New(), Name: name, Version: version, DAG: append(json.RawMessage{}, dag...), CreatedAt: time.Now()}
	t.definitions[d.ID] = d
	return d, nil
}

func (s *Store) GetWorkflowDefinition(ctx context.Context, name string, version int) (*store.WorkflowDefinition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).GetWorkflowDefinition(ctx, name, version)
}
func (t *txQueries) GetWorkflowDefinition(ctx context.Context, name string, version int) (*store.WorkflowDefinition, error) {
	for _, d := range t.definitions {
		if d.Name == name && d.Version == version {
			return d, nil
		}
	}
	return nil, store.ErrNotFound
}

func (s *Store) GetLatestWorkflowDefinition(ctx context.Context, name string) (*store.WorkflowDefinition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).GetLatestWorkflowDefinition(ctx, name)
}
func (t *txQueries) GetLatestWorkflowDefinition(ctx context.Context, name string) (*store.WorkflowDefinition, error) {
	var best *store.WorkflowDefinition
	for _, d := range t.definitions {
		if d.Name == name && (best == nil || d.Version > best.Version) {
			best = d
		}
	}
	if best == nil {
		return nil, store.ErrNotFound
	}
	return best, nil
}

func (s *Store) ListWorkflowDefinitions(ctx context.Context) ([]store.WorkflowDefinition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).ListWorkflowDefinitions(ctx)
}
func (t *txQueries) ListWorkflowDefinitions(ctx context.Context) ([]store.WorkflowDefinition, error) {
	out := make([]store.WorkflowDefinition, 0, len(t.definitions))
	for _, d := range t.definitions {
		out = append(out, *d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// ---- runs ----

func (s *Store) CreateWorkflowRun(ctx context.Context, id, definitionID uuid.UUID, name string, version int, shardID int, input []byte) (*store.WorkflowRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).CreateWorkflowRun(ctx, id, definitionID, name, version, shardID, input)
}
func (t *txQueries) CreateWorkflowRun(ctx context.Context, id, definitionID uuid.UUID, name string, version int, shardID int, input []byte) (*store.WorkflowRun, error) {
	r := &store.WorkflowRun{
		ID: id, DefinitionID: definitionID, Name: name, Version: version,
		Status: store.RunPending, ShardID: shardID, Input: append(json.RawMessage{}, input...),
		Context: json.RawMessage(`{}`), CreatedAt: time.Now(),
	}
	t.runs[r.ID] = r
	return cp(r), nil
}

func (s *Store) GetWorkflowRun(ctx context.Context, id uuid.UUID) (*store.WorkflowRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).GetWorkflowRun(ctx, id)
}
func (t *txQueries) GetWorkflowRun(ctx context.Context, id uuid.UUID) (*store.WorkflowRun, error) {
	r, ok := t.runs[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return cp(r), nil
}

func (s *Store) ListWorkflowRuns(ctx context.Context, f store.RunFilter) ([]store.WorkflowRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).ListWorkflowRuns(ctx, f)
}
func (t *txQueries) ListWorkflowRuns(ctx context.Context, f store.RunFilter) ([]store.WorkflowRun, error) {
	var out []store.WorkflowRun
	for _, r := range t.runs {
		if f.Status != nil && r.Status != *f.Status {
			continue
		}
		if f.Name != nil && r.Name != *f.Name {
			continue
		}
		out = append(out, *cp(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if f.Offset > 0 {
		if f.Offset >= len(out) {
			return []store.WorkflowRun{}, nil
		}
		out = out[f.Offset:]
	}
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

func (s *Store) ListRunningRunsForShards(ctx context.Context, shardIDs []int) ([]store.WorkflowRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).ListRunningRunsForShards(ctx, shardIDs)
}
func (t *txQueries) ListRunningRunsForShards(ctx context.Context, shardIDs []int) ([]store.WorkflowRun, error) {
	set := map[int]bool{}
	for _, id := range shardIDs {
		set[id] = true
	}
	var out []store.WorkflowRun
	for _, r := range t.runs {
		if r.Status != store.RunPending && r.Status != store.RunRunning {
			continue
		}
		if shardIDs != nil && !set[r.ShardID] {
			continue
		}
		out = append(out, *cp(r))
	}
	return out, nil
}

func (s *Store) UpdateWorkflowRunStatus(ctx context.Context, id uuid.UUID, expected []store.RunStatus, next store.RunStatus, output []byte, errMsg *string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).UpdateWorkflowRunStatus(ctx, id, expected, next, output, errMsg)
}
func (t *txQueries) UpdateWorkflowRunStatus(ctx context.Context, id uuid.UUID, expected []store.RunStatus, next store.RunStatus, output []byte, errMsg *string) (bool, error) {
	r, ok := t.runs[id]
	if !ok {
		return false, nil
	}
	if !statusIn(r.Status, expected) {
		return false, nil
	}
	r.Status = next
	if output != nil {
		r.Output = append(json.RawMessage{}, output...)
	}
	if errMsg != nil {
		r.Error = errMsg
	}
	now := time.Now()
	if next == store.RunRunning && r.StartedAt == nil {
		r.StartedAt = &now
	}
	if next == store.RunCompleted || next == store.RunFailed || next == store.RunCancelled {
		r.CompletedAt = &now
	}
	return true, nil
}

// ---- steps ----

func (s *Store) CreateSteps(ctx context.Context, runID uuid.UUID, steps []store.NewStep) ([]store.Step, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).CreateSteps(ctx, runID, steps)
}
func (t *txQueries) CreateSteps(ctx context.Context, runID uuid.UUID, steps []store.NewStep) ([]store.Step, error) {
	out := make([]store.Step, 0, len(steps))
	for _, ns := range steps {
		input := ns.Input
		if input == nil {
			input = json.RawMessage(`{}`)
		}
		st := &store.Step{
			ID: uuid.New(), WorkflowRunID: runID, StepName: ns.StepName, TaskType: ns.TaskType,
			DependsOn: append([]string{}, ns.DependsOn...), Status: store.StepPending, MaxAttempts: ns.MaxAttempts,
			Input: append(json.RawMessage{}, input...), InitialBackoffMS: ns.InitialBackoffMS,
			BackoffMultiplier: ns.BackoffMultiplier, MaxBackoffMS: ns.MaxBackoffMS, TimeoutSeconds: ns.TimeoutSeconds,
			CreatedAt: time.Now(),
		}
		t.steps[st.ID] = st
		out = append(out, *cp(st))
	}
	return out, nil
}

func (s *Store) GetSteps(ctx context.Context, runID uuid.UUID) ([]store.Step, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).GetSteps(ctx, runID)
}
func (t *txQueries) GetSteps(ctx context.Context, runID uuid.UUID) ([]store.Step, error) {
	var out []store.Step
	for _, st := range t.steps {
		if st.WorkflowRunID == runID {
			out = append(out, *cp(st))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) GetStep(ctx context.Context, id uuid.UUID) (*store.Step, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).GetStep(ctx, id)
}
func (t *txQueries) GetStep(ctx context.Context, id uuid.UUID) (*store.Step, error) {
	st, ok := t.steps[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return cp(st), nil
}

func (s *Store) UpdateStepStatus(ctx context.Context, id uuid.UUID, expected []store.StepStatus, next store.StepStatus, output []byte, errMsg *string, bumpAttempt bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).UpdateStepStatus(ctx, id, expected, next, output, errMsg, bumpAttempt)
}
func (t *txQueries) UpdateStepStatus(ctx context.Context, id uuid.UUID, expected []store.StepStatus, next store.StepStatus, output []byte, errMsg *string, bumpAttempt bool) (bool, error) {
	st, ok := t.steps[id]
	if !ok {
		return false, nil
	}
	if !stepStatusIn(st.Status, expected) {
		return false, nil
	}
	st.Status = next
	if output != nil {
		st.Output = append(json.RawMessage{}, output...)
	}
	if errMsg != nil {
		st.Error = errMsg
	}
	if bumpAttempt {
		st.AttemptCount++
	}
	now := time.Now()
	if (next == store.StepQueued || next == store.StepRunning) && st.StartedAt == nil {
		st.StartedAt = &now
	}
	if store.StepTerminal(next) {
		st.CompletedAt = &now
	}
	return true, nil
}

// ---- task attempts ----

func (s *Store) CreateTaskAttempt(ctx context.Context, stepID, runID uuid.UUID, attemptNumber int, queueName string) (*store.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).CreateTaskAttempt(ctx, stepID, runID, attemptNumber, queueName)
}
func (t *txQueries) CreateTaskAttempt(ctx context.Context, stepID, runID uuid.UUID, attemptNumber int, queueName string) (*store.TaskAttempt, error) {
	a := &store.TaskAttempt{
		ID: uuid.New(), StepID: stepID, WorkflowRunID: runID, AttemptNumber: attemptNumber,
		IdempotencyKey: uuid.New(), Status: store.AttemptQueued, QueueName: queueName, QueuedAt: time.Now(),
	}
	t.attempts[a.ID] = a
	return cp(a), nil
}

func (s *Store) ClaimTaskAttempt(ctx context.Context, attemptID, idempotencyKey uuid.UUID, workerID string, leaseFor time.Duration) (*store.TaskAttempt, store.ClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).ClaimTaskAttempt(ctx, attemptID, idempotencyKey, workerID, leaseFor)
}
func (t *txQueries) ClaimTaskAttempt(ctx context.Context, attemptID, idempotencyKey uuid.UUID, workerID string, leaseFor time.Duration) (*store.TaskAttempt, store.ClaimResult, error) {
	a, ok := t.attempts[attemptID]
	if !ok {
		return nil, store.ClaimNotFound, nil
	}
	if a.Status != store.AttemptQueued || a.IdempotencyKey != idempotencyKey {
		return cp(a), store.ClaimAlreadyClaimed, nil
	}
	a.Status = store.AttemptLeased
	owner := workerID
	a.LeaseOwner = &owner
	exp := time.Now().Add(leaseFor)
	a.LeaseExpiresAt = &exp
	now := time.Now()
	a.StartedAt = &now
	return cp(a), store.ClaimOK, nil
}

func (s *Store) HeartbeatTaskAttempt(ctx context.Context, attemptID uuid.UUID, workerID string, leaseFor time.Duration) (*time.Time, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).HeartbeatTaskAttempt(ctx, attemptID, workerID, leaseFor)
}
func (t *txQueries) HeartbeatTaskAttempt(ctx context.Context, attemptID uuid.UUID, workerID string, leaseFor time.Duration) (*time.Time, bool, error) {
	a, ok := t.attempts[attemptID]
	if !ok || a.Status != store.AttemptLeased || a.LeaseOwner == nil || *a.LeaseOwner != workerID {
		return nil, false, nil
	}
	exp := time.Now().Add(leaseFor)
	a.LeaseExpiresAt = &exp
	return &exp, true, nil
}

func (s *Store) CompleteTaskAttempt(ctx context.Context, attemptID uuid.UUID, workerID string, success bool, result []byte, errMsg *string) (*store.TaskAttempt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).CompleteTaskAttempt(ctx, attemptID, workerID, success, result, errMsg)
}
func (t *txQueries) CompleteTaskAttempt(ctx context.Context, attemptID uuid.UUID, workerID string, success bool, result []byte, errMsg *string) (*store.TaskAttempt, bool, error) {
	a, ok := t.attempts[attemptID]
	if !ok {
		return nil, false, nil
	}
	if a.Status != store.AttemptLeased || a.LeaseOwner == nil || *a.LeaseOwner != workerID {
		return cp(a), false, nil
	}
	if success {
		a.Status = store.AttemptSucceeded
	} else {
		a.Status = store.AttemptFailed
	}
	if result != nil {
		a.Result = append(json.RawMessage{}, result...)
	}
	a.Error = errMsg
	now := time.Now()
	a.CompletedAt = &now
	return cp(a), true, nil
}

func (s *Store) ReapExpiredAttempts(ctx context.Context, limit int) ([]store.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).ReapExpiredAttempts(ctx, limit)
}
func (t *txQueries) ReapExpiredAttempts(ctx context.Context, limit int) ([]store.TaskAttempt, error) {
	var out []store.TaskAttempt
	now := time.Now()
	for _, a := range t.attempts {
		if len(out) >= limit {
			break
		}
		if a.Status == store.AttemptLeased && a.LeaseExpiresAt != nil && a.LeaseExpiresAt.Before(now) {
			a.Status = store.AttemptExpired
			errMsg := "lease expired"
			a.Error = &errMsg
			a.CompletedAt = &now
			out = append(out, *cp(a))
		}
	}
	return out, nil
}

func (s *Store) GetTaskAttempt(ctx context.Context, id uuid.UUID) (*store.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).GetTaskAttempt(ctx, id)
}
func (t *txQueries) GetTaskAttempt(ctx context.Context, id uuid.UUID) (*store.TaskAttempt, error) {
	a, ok := t.attempts[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return cp(a), nil
}

func (s *Store) ListTaskAttemptsForStep(ctx context.Context, stepID uuid.UUID) ([]store.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).ListTaskAttemptsForStep(ctx, stepID)
}
func (t *txQueries) ListTaskAttemptsForStep(ctx context.Context, stepID uuid.UUID) ([]store.TaskAttempt, error) {
	var out []store.TaskAttempt
	for _, a := range t.attempts {
		if a.StepID == stepID {
			out = append(out, *cp(a))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AttemptNumber < out[j].AttemptNumber })
	return out, nil
}

// ---- outbox ----

func (s *Store) InsertOutboxMessage(ctx context.Context, msg store.NewOutboxMessage) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).InsertOutboxMessage(ctx, msg)
}
func (t *txQueries) InsertOutboxMessage(ctx context.Context, msg store.NewOutboxMessage) (int64, error) {
	t.nextOutboxID++
	id := t.nextOutboxID
	t.outbox[id] = &store.OutboxMessage{
		ID: id, AggregateType: msg.AggregateType, AggregateID: msg.AggregateID, ShardID: msg.ShardID,
		StreamName: msg.StreamName, Payload: append(json.RawMessage{}, msg.Payload...), Status: "PENDING", CreatedAt: time.Now(),
	}
	return id, nil
}

func (s *Store) FetchAndLockPendingOutbox(ctx context.Context, shardIDs []int, limit int) ([]store.OutboxMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).FetchAndLockPendingOutbox(ctx, shardIDs, limit)
}
func (t *txQueries) FetchAndLockPendingOutbox(ctx context.Context, shardIDs []int, limit int) ([]store.OutboxMessage, error) {
	set := map[int]bool{}
	for _, id := range shardIDs {
		set[id] = true
	}
	ids := make([]int64, 0, len(t.outbox))
	for id := range t.outbox {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var out []store.OutboxMessage
	for _, id := range ids {
		if len(out) >= limit {
			break
		}
		m := t.outbox[id]
		if m.Status != "PENDING" {
			continue
		}
		if shardIDs != nil && !set[m.ShardID] {
			continue
		}
		out = append(out, *m)
	}
	return out, nil
}
func (s *Store) MarkOutboxPublished(ctx context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).MarkOutboxPublished(ctx, id)
}
func (t *txQueries) MarkOutboxPublished(ctx context.Context, id int64) error {
	if m, ok := t.outbox[id]; ok {
		m.Status = "PUBLISHED"
		now := time.Now()
		m.PublishedAt = &now
	}
	return nil
}

// ---- timers ----

func (s *Store) InsertTimer(ctx context.Context, nt store.NewTimer) (*store.Timer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).InsertTimer(ctx, nt)
}
func (t *txQueries) InsertTimer(ctx context.Context, nt store.NewTimer) (*store.Timer, error) {
	payload := nt.Payload
	if payload == nil {
		payload = json.RawMessage(`{}`)
	}
	tm := &store.Timer{
		ID: uuid.New(), WorkflowRunID: nt.WorkflowRunID, StepID: nt.StepID, ShardID: nt.ShardID,
		Kind: nt.Kind, Status: store.TimerPending, Payload: append(json.RawMessage{}, payload...),
		FireAt: nt.FireAt, CreatedAt: time.Now(),
	}
	t.timers[tm.ID] = tm
	return cp(tm), nil
}

func (s *Store) ListDueTimers(ctx context.Context, shardIDs []int, limit int) ([]store.Timer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).ListDueTimers(ctx, shardIDs, limit)
}
func (t *txQueries) ListDueTimers(ctx context.Context, shardIDs []int, limit int) ([]store.Timer, error) {
	set := map[int]bool{}
	for _, id := range shardIDs {
		set[id] = true
	}
	now := time.Now()
	var out []store.Timer
	for _, tm := range t.timers {
		if len(out) >= limit {
			break
		}
		if tm.Status != store.TimerPending || tm.FireAt.After(now) {
			continue
		}
		if shardIDs != nil && !set[tm.ShardID] {
			continue
		}
		out = append(out, *cp(tm))
	}
	return out, nil
}

func (s *Store) FireTimerCAS(ctx context.Context, timerID uuid.UUID) (*store.Timer, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).FireTimerCAS(ctx, timerID)
}
func (t *txQueries) FireTimerCAS(ctx context.Context, timerID uuid.UUID) (*store.Timer, bool, error) {
	tm, ok := t.timers[timerID]
	if !ok || tm.Status != store.TimerPending {
		return nil, false, nil
	}
	now := time.Now()
	tm.Status = store.TimerFired
	tm.FiredAt = &now
	return cp(tm), true, nil
}

func (s *Store) CancelTimersForRun(ctx context.Context, runID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).CancelTimersForRun(ctx, runID)
}
func (t *txQueries) CancelTimersForRun(ctx context.Context, runID uuid.UUID) error {
	for _, tm := range t.timers {
		if tm.WorkflowRunID == runID && tm.Status == store.TimerPending {
			tm.Status = store.TimerCancelled
		}
	}
	return nil
}

// ---- signals ----

func (s *Store) InsertSignal(ctx context.Context, runID uuid.UUID, name string, payload []byte) (*store.Signal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).InsertSignal(ctx, runID, name, payload)
}
func (t *txQueries) InsertSignal(ctx context.Context, runID uuid.UUID, name string, payload []byte) (*store.Signal, error) {
	if payload == nil {
		payload = json.RawMessage(`{}`)
	}
	sig := &store.Signal{ID: uuid.New(), WorkflowRunID: runID, SignalName: name, Payload: append(json.RawMessage{}, payload...), ReceivedAt: time.Now()}
	t.signals[sig.ID] = sig
	return cp(sig), nil
}

func (s *Store) ListUnprocessedSignals(ctx context.Context, runID uuid.UUID) ([]store.Signal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).ListUnprocessedSignals(ctx, runID)
}
func (t *txQueries) ListUnprocessedSignals(ctx context.Context, runID uuid.UUID) ([]store.Signal, error) {
	var out []store.Signal
	for _, sig := range t.signals {
		if sig.WorkflowRunID == runID && !sig.Processed {
			out = append(out, *cp(sig))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReceivedAt.Before(out[j].ReceivedAt) })
	return out, nil
}

func (s *Store) MarkSignalProcessed(ctx context.Context, id uuid.UUID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).MarkSignalProcessed(ctx, id)
}
func (t *txQueries) MarkSignalProcessed(ctx context.Context, id uuid.UUID) (bool, error) {
	sig, ok := t.signals[id]
	if !ok || sig.Processed {
		return false, nil
	}
	sig.Processed = true
	return true, nil
}

// ---- history ----

func (s *Store) AppendHistory(ctx context.Context, runID uuid.UUID, eventType string, payload []byte) (*store.HistoryEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).AppendHistory(ctx, runID, eventType, payload)
}
func (t *txQueries) AppendHistory(ctx context.Context, runID uuid.UUID, eventType string, payload []byte) (*store.HistoryEvent, error) {
	if payload == nil {
		payload = json.RawMessage(`{}`)
	}
	r, ok := t.runs[runID]
	if !ok {
		return nil, store.ErrNotFound
	}
	r.HistorySeq++
	h := &store.HistoryEvent{ID: r.HistorySeq, WorkflowRunID: runID, Seq: r.HistorySeq, EventType: eventType, Payload: append(json.RawMessage{}, payload...), CreatedAt: time.Now()}
	t.history[runID] = append(t.history[runID], h)
	return cp(h), nil
}

func (s *Store) ListHistory(ctx context.Context, runID uuid.UUID) ([]store.HistoryEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (*txQueries)(s).ListHistory(ctx, runID)
}
func (t *txQueries) ListHistory(ctx context.Context, runID uuid.UUID) ([]store.HistoryEvent, error) {
	var out []store.HistoryEvent
	for _, h := range t.history[runID] {
		out = append(out, *cp(h))
	}
	return out, nil
}

// ---- helpers ----

func cp[T any](v *T) *T {
	c := *v
	return &c
}

func statusIn(s store.RunStatus, list []store.RunStatus) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func stepStatusIn(s store.StepStatus, list []store.StepStatus) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
