package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/google/uuid"

	"github.com/aryanraj/workflow-orchestrator/internal/store"
)

const signalWaitTaskType = "signal_wait"

// runState is the local, in-transaction view of a run's steps used while evaluating the
// DAG. It's populated once per reconcile pass and mutated as transitions are applied, so
// dependency checks within the same pass see the effect of earlier transitions (e.g. a step
// skipped earlier in this pass correctly propagates to its own dependents in the same pass)
// without needing extra round trips to the database.
type runState struct {
	byName map[string]*store.Step
	byID   map[uuid.UUID]*store.Step
}

func newRunState(steps []store.Step) *runState {
	rs := &runState{byName: map[string]*store.Step{}, byID: map[uuid.UUID]*store.Step{}}
	for i := range steps {
		s := &steps[i]
		rs.byName[s.StepName] = s
		rs.byID[s.ID] = s
	}
	return rs
}

func (rs *runState) allTerminal() bool {
	for _, s := range rs.byName {
		if !store.StepTerminal(s.Status) {
			return false
		}
	}
	return true
}

func (rs *runState) anyDispatchedOrRunning() bool {
	for _, s := range rs.byName {
		switch s.Status {
		case store.StepQueued, store.StepRunning, store.StepRetryBackoff, store.StepWaiting, store.StepCompleted:
			return true
		}
	}
	return false
}

// reconcileTx is the single DAG-evaluation function everything funnels through: initial run
// creation, a task result being reported, a retry timer firing, a signal arriving, and the
// periodic recovery sweep all end by calling this. It must run inside store.WithTx.
func (e *Engine) reconcileTx(ctx context.Context, q store.Queries, run *store.WorkflowRun, steps []store.Step) error {
	if run.Status == store.RunCompleted || run.Status == store.RunFailed || run.Status == store.RunCancelled {
		return nil // terminal run, nothing to reconcile
	}

	rs := newRunState(steps)

	if err := e.propagateSkips(ctx, q, run, rs); err != nil {
		return err
	}
	if err := e.dispatchReady(ctx, q, run, rs); err != nil {
		return err
	}
	return e.checkRunCompletion(ctx, q, run, rs)
}

// propagateSkips marks any non-terminal step SKIPPED if one of its dependencies has failed,
// been skipped, or been cancelled — fail-fast propagation. Iterates to a fixpoint because
// skipping a step can in turn make its own dependents skippable in the same pass.
func (e *Engine) propagateSkips(ctx context.Context, q store.Queries, run *store.WorkflowRun, rs *runState) error {
	for pass := 0; pass < len(rs.byName)+1; pass++ {
		changed := false
		for _, s := range rs.byName {
			if s.Status != store.StepPending && s.Status != store.StepReady {
				continue
			}
			blocked := false
			for _, dep := range s.DependsOn {
				d := rs.byName[dep]
				if d == nil {
					continue
				}
				if d.Status == store.StepFailed || d.Status == store.StepSkipped || d.Status == store.StepCancelled {
					blocked = true
					break
				}
			}
			if !blocked {
				continue
			}
			ok, err := q.UpdateStepStatus(ctx, s.ID, []store.StepStatus{s.Status}, store.StepSkipped, nil, nil, false)
			if err != nil {
				return err
			}
			if ok {
				s.Status = store.StepSkipped
				changed = true
				if _, err := q.AppendHistory(ctx, run.ID, "step_skipped", mustJSON(map[string]any{"step": s.StepName})); err != nil {
					return err
				}
			}
		}
		if !changed {
			break
		}
	}
	return nil
}

// dispatchReady dispatches every step whose dependencies are all COMPLETED and which is
// still PENDING (or READY). signal_wait steps are handled specially: they never go through
// the task queue at all, they resolve directly from the signals table.
func (e *Engine) dispatchReady(ctx context.Context, q store.Queries, run *store.WorkflowRun, rs *runState) error {
	// Steps already parked in WAITING had their dependencies satisfied the moment they
	// entered that state; every subsequent reconcile just needs to re-check whether a
	// matching signal has since arrived, not re-check dependencies.
	for _, s := range rs.byName {
		if s.Status != store.StepWaiting {
			continue
		}
		if _, err := e.tryResolveSignalWait(ctx, q, run, s); err != nil {
			return err
		}
	}

	for _, s := range rs.byName {
		if s.Status != store.StepPending && s.Status != store.StepReady {
			continue
		}
		ready := true
		for _, dep := range s.DependsOn {
			d := rs.byName[dep]
			if d == nil || d.Status != store.StepCompleted {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}

		if s.TaskType == signalWaitTaskType {
			resolved, err := e.tryResolveSignalWait(ctx, q, run, s)
			if err != nil {
				return err
			}
			if !resolved {
				if ok, err := q.UpdateStepStatus(ctx, s.ID, []store.StepStatus{s.Status}, store.StepWaiting, nil, nil, false); err != nil {
					return err
				} else if ok {
					s.Status = store.StepWaiting
				}
			}
			continue
		}

		if err := e.dispatchStep(ctx, q, run, s, []store.StepStatus{s.Status}); err != nil {
			return err
		}
	}
	return nil
}

// tryResolveSignalWait looks for an unprocessed signal matching this step and, if found,
// completes the step with the signal payload as its output. The signal name matched is the
// step's `signal_name` input field if present, otherwise the step name itself.
func (e *Engine) tryResolveSignalWait(ctx context.Context, q store.Queries, run *store.WorkflowRun, s *store.Step) (bool, error) {
	wantName := s.StepName
	var cfg struct {
		SignalName string `json:"signal_name"`
	}
	if len(s.Input) > 0 {
		_ = json.Unmarshal(s.Input, &cfg)
		if cfg.SignalName != "" {
			wantName = cfg.SignalName
		}
	}
	signals, err := q.ListUnprocessedSignals(ctx, run.ID)
	if err != nil {
		return false, err
	}
	for _, sig := range signals {
		if sig.SignalName != wantName {
			continue
		}
		applied, err := q.MarkSignalProcessed(ctx, sig.ID)
		if err != nil {
			return false, err
		}
		if !applied {
			continue // raced with another consumer of this signal; try the next one
		}
		ok, err := q.UpdateStepStatus(ctx, s.ID, []store.StepStatus{s.Status}, store.StepCompleted, sig.Payload, nil, false)
		if err != nil {
			return false, err
		}
		if ok {
			s.Status = store.StepCompleted
			s.Output = sig.Payload
			if _, err := q.AppendHistory(ctx, run.ID, "step_completed_by_signal", mustJSON(map[string]any{"step": s.StepName, "signal": sig.SignalName})); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	return false, nil
}

// dispatchStep is the transactional-outbox write path: create the
// next task_attempt, write the matching outbox row in the same transaction, and flip the
// step to QUEUED. `from` is the CAS guard (the step statuses this dispatch is allowed to
// originate from).
func (e *Engine) dispatchStep(ctx context.Context, q store.Queries, run *store.WorkflowRun, s *store.Step, from []store.StepStatus) error {
	attemptNumber := s.AttemptCount + 1
	queueName := "wf:tasks:" + s.TaskType

	attempt, err := q.CreateTaskAttempt(ctx, s.ID, run.ID, attemptNumber, queueName)
	if err != nil {
		return fmt.Errorf("engine: create task attempt: %w", err)
	}

	payload := store.TaskPayload{
		TaskAttemptID:  attempt.ID,
		StepID:         s.ID,
		WorkflowRunID:  run.ID,
		StepName:       s.StepName,
		TaskType:       s.TaskType,
		Input:          s.Input,
		AttemptNumber:  attemptNumber,
		IdempotencyKey: attempt.IdempotencyKey,
		TimeoutSeconds: s.TimeoutSeconds,
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := q.InsertOutboxMessage(ctx, store.NewOutboxMessage{
		AggregateType: "task_attempt",
		AggregateID:   attempt.ID,
		ShardID:       run.ShardID,
		StreamName:    queueName,
		Payload:       payloadJSON,
	}); err != nil {
		return fmt.Errorf("engine: insert outbox: %w", err)
	}

	ok, err := q.UpdateStepStatus(ctx, s.ID, from, store.StepQueued, nil, nil, true)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("engine: step %s changed status concurrently during dispatch", s.StepName)
	}
	s.Status = store.StepQueued
	s.AttemptCount = attemptNumber

	if _, err := q.AppendHistory(ctx, run.ID, "step_dispatched", mustJSON(map[string]any{
		"step": s.StepName, "attempt": attemptNumber, "task_type": s.TaskType,
	})); err != nil {
		return err
	}
	return nil
}

// checkRunCompletion transitions the run PENDING->RUNNING once something is in flight, and
// to a terminal status once every step has resolved.
func (e *Engine) checkRunCompletion(ctx context.Context, q store.Queries, run *store.WorkflowRun, rs *runState) error {
	if run.Status == store.RunPending && rs.anyDispatchedOrRunning() {
		if ok, err := q.UpdateWorkflowRunStatus(ctx, run.ID, []store.RunStatus{store.RunPending}, store.RunRunning, nil, nil); err != nil {
			return err
		} else if ok {
			run.Status = store.RunRunning
			if _, err := q.AppendHistory(ctx, run.ID, "run_started", nil); err != nil {
				return err
			}
		}
	}

	if !rs.allTerminal() {
		return nil
	}

	outputs := map[string]json.RawMessage{}
	var failedStep *store.Step
	var cancelledAny bool
	for _, s := range rs.byName {
		if s.Status == store.StepCompleted {
			outputs[s.StepName] = s.Output
		}
		if s.Status == store.StepFailed && failedStep == nil {
			failedStep = s
		}
		if s.Status == store.StepCancelled {
			cancelledAny = true
		}
	}

	outputJSON, _ := json.Marshal(outputs)

	switch {
	case failedStep != nil:
		errMsg := fmt.Sprintf("step %q failed", failedStep.StepName)
		if failedStep.Error != nil {
			errMsg = fmt.Sprintf("step %q failed: %s", failedStep.StepName, *failedStep.Error)
		}
		if ok, err := q.UpdateWorkflowRunStatus(ctx, run.ID, []store.RunStatus{store.RunPending, store.RunRunning}, store.RunFailed, outputJSON, &errMsg); err != nil {
			return err
		} else if ok {
			run.Status = store.RunFailed
			_, _ = q.AppendHistory(ctx, run.ID, "run_failed", mustJSON(map[string]any{"error": errMsg}))
		}
	case cancelledAny:
		if ok, err := q.UpdateWorkflowRunStatus(ctx, run.ID, []store.RunStatus{store.RunPending, store.RunRunning}, store.RunCancelled, outputJSON, nil); err != nil {
			return err
		} else if ok {
			run.Status = store.RunCancelled
			_, _ = q.AppendHistory(ctx, run.ID, "run_cancelled", nil)
		}
	default:
		if ok, err := q.UpdateWorkflowRunStatus(ctx, run.ID, []store.RunStatus{store.RunPending, store.RunRunning}, store.RunCompleted, outputJSON, nil); err != nil {
			return err
		} else if ok {
			run.Status = store.RunCompleted
			_, _ = q.AppendHistory(ctx, run.ID, "run_completed", nil)
		}
	}
	if err := q.CancelTimersForRun(ctx, run.ID); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

// Reconcile re-runs DAG evaluation for a single run. Safe (and expected) to call
// speculatively/redundantly — it's a no-op if there's nothing new to do. Used by the
// recovery sweep and by anything that just wants to "nudge" a run forward.
func (e *Engine) Reconcile(ctx context.Context, runID uuid.UUID) error {
	return e.Store.WithTx(ctx, func(ctx context.Context, q store.Queries) error {
		run, err := q.GetWorkflowRun(ctx, runID)
		if err != nil {
			return err
		}
		steps, err := q.GetSteps(ctx, runID)
		if err != nil {
			return err
		}
		return e.reconcileTx(ctx, q, run, steps)
	})
}

// HandleTaskResult applies a worker's reported outcome for a task attempt. Returns
// accepted=false if this was a stale/duplicate report.
func (e *Engine) HandleTaskResult(ctx context.Context, attemptID uuid.UUID, workerID string, success bool, output []byte, errMsg *string) (accepted bool, err error) {
	err = e.Store.WithTx(ctx, func(ctx context.Context, q store.Queries) error {
		attempt, applied, cerr := q.CompleteTaskAttempt(ctx, attemptID, workerID, success, output, errMsg)
		if cerr != nil {
			return cerr
		}
		if !applied {
			accepted = false
			return nil
		}
		accepted = true
		return e.handleAttemptOutcome(ctx, q, *attempt, success, output, errMsg)
	})
	return accepted, err
}

// handleAttemptOutcome contains the logic shared by a worker-reported failure and a
// reaper-detected lease expiry: on success, complete the step; on failure, either schedule a
// durable retry timer or fail the step terminally, then re-run DAG evaluation.
func (e *Engine) handleAttemptOutcome(ctx context.Context, q store.Queries, attempt store.TaskAttempt, success bool, output []byte, errMsg *string) error {
	step, err := q.GetStep(ctx, attempt.StepID)
	if err != nil {
		return err
	}
	run, err := q.GetWorkflowRun(ctx, attempt.WorkflowRunID)
	if err != nil {
		return err
	}

	if success {
		ok, err := q.UpdateStepStatus(ctx, step.ID, []store.StepStatus{store.StepQueued, store.StepRunning}, store.StepCompleted, output, nil, false)
		if err != nil {
			return err
		}
		if ok {
			step.Status = store.StepCompleted
			step.Output = output
			if _, err := q.AppendHistory(ctx, run.ID, "step_completed", mustJSON(map[string]any{"step": step.StepName, "attempt": attempt.AttemptNumber})); err != nil {
				return err
			}
		}
	} else {
		if step.AttemptCount < step.MaxAttempts {
			backoff := computeBackoff(step, step.AttemptCount)
			if _, err := q.InsertTimer(ctx, store.NewTimer{
				WorkflowRunID: run.ID,
				StepID:        &step.ID,
				ShardID:       run.ShardID,
				Kind:          store.TimerRetryBackoff,
				FireAt:        time.Now().Add(backoff),
				Payload:       mustJSON(map[string]any{"step_id": step.ID}),
			}); err != nil {
				return err
			}
			ok, err := q.UpdateStepStatus(ctx, step.ID, []store.StepStatus{store.StepQueued, store.StepRunning}, store.StepRetryBackoff, nil, errMsg, false)
			if err != nil {
				return err
			}
			if ok {
				step.Status = store.StepRetryBackoff
				if _, err := q.AppendHistory(ctx, run.ID, "step_retry_scheduled", mustJSON(map[string]any{
					"step": step.StepName, "attempt": attempt.AttemptNumber, "backoff_ms": backoff.Milliseconds(),
				})); err != nil {
					return err
				}
			}
		} else {
			ok, err := q.UpdateStepStatus(ctx, step.ID, []store.StepStatus{store.StepQueued, store.StepRunning}, store.StepFailed, nil, errMsg, false)
			if err != nil {
				return err
			}
			if ok {
				step.Status = store.StepFailed
				if _, err := q.AppendHistory(ctx, run.ID, "step_failed", mustJSON(map[string]any{"step": step.StepName, "attempt": attempt.AttemptNumber})); err != nil {
					return err
				}
			}
		}
	}

	steps, err := q.GetSteps(ctx, run.ID)
	if err != nil {
		return err
	}
	return e.reconcileTx(ctx, q, run, steps)
}

// computeBackoff applies exponential backoff with a cap and +/-20% jitter.
// attemptsMade is the number of attempts already made (i.e. the one that just failed).
func computeBackoff(step *store.Step, attemptsMade int) time.Duration {
	base := float64(step.InitialBackoffMS) * math.Pow(step.BackoffMultiplier, float64(attemptsMade-1))
	if base > float64(step.MaxBackoffMS) {
		base = float64(step.MaxBackoffMS)
	}
	jitter := base * (0.8 + 0.4*rand.Float64()) // +/-20%
	return time.Duration(jitter) * time.Millisecond
}

// HandleFiredTimer is called by the timer poller (internal/timers) once it discovers a
// candidate due timer. It atomically claims the timer (CAS PENDING->FIRED) and applies its
// effect in one transaction — see the FireTimerCAS doc comment.
func (e *Engine) HandleFiredTimer(ctx context.Context, timerID uuid.UUID) error {
	return e.Store.WithTx(ctx, func(ctx context.Context, q store.Queries) error {
		timer, applied, err := q.FireTimerCAS(ctx, timerID)
		if err != nil {
			return err
		}
		if !applied {
			return nil // already claimed by another poller/node
		}
		switch timer.Kind {
		case store.TimerRetryBackoff:
			return e.handleRetryTimerFired(ctx, q, timer)
		default:
			// step_timeout / user_timer: nudge a reconcile; specific handling can be extended
			// here as new timer kinds are introduced.
			run, err := q.GetWorkflowRun(ctx, timer.WorkflowRunID)
			if err != nil {
				return err
			}
			steps, err := q.GetSteps(ctx, run.ID)
			if err != nil {
				return err
			}
			return e.reconcileTx(ctx, q, run, steps)
		}
	})
}

func (e *Engine) handleRetryTimerFired(ctx context.Context, q store.Queries, timer *store.Timer) error {
	if timer.StepID == nil {
		return nil
	}
	step, err := q.GetStep(ctx, *timer.StepID)
	if err != nil {
		return err
	}
	run, err := q.GetWorkflowRun(ctx, timer.WorkflowRunID)
	if err != nil {
		return err
	}
	if step.Status != store.StepRetryBackoff {
		return nil // superseded (e.g. run cancelled) since the timer was scheduled
	}
	if err := e.dispatchStep(ctx, q, run, step, []store.StepStatus{store.StepRetryBackoff}); err != nil {
		return err
	}
	steps, err := q.GetSteps(ctx, run.ID)
	if err != nil {
		return err
	}
	return e.reconcileTx(ctx, q, run, steps)
}

// ReapExpiredLeases finds task attempts whose lease has expired without the worker
// reporting back, and drives them through the same retry/fail path as an explicit failure
// report. Returns the number reaped.
func (e *Engine) ReapExpiredLeases(ctx context.Context, limit int) (int, error) {
	expired, err := e.Store.ReapExpiredAttempts(ctx, limit)
	if err != nil {
		return 0, err
	}
	for _, attempt := range expired {
		errMsg := "task lease expired (worker unresponsive)"
		err := e.Store.WithTx(ctx, func(ctx context.Context, q store.Queries) error {
			return e.handleAttemptOutcome(ctx, q, attempt, false, nil, &errMsg)
		})
		if err != nil {
			e.Log.Error("engine: failed to handle expired attempt", "attempt_id", attempt.ID, "err", err)
		}
	}
	return len(expired), nil
}

// ApplySignal records an external signal and re-runs DAG evaluation so any signal_wait step
// blocked on it can resolve immediately. Two names are handled specially: "__cancel__" is
// intercepted here rather than stored as an ordinary signal.
func (e *Engine) ApplySignal(ctx context.Context, runID uuid.UUID, name string, payload []byte) error {
	if name == "__cancel__" {
		return e.CancelRun(ctx, runID)
	}
	return e.Store.WithTx(ctx, func(ctx context.Context, q store.Queries) error {
		if _, err := q.InsertSignal(ctx, runID, name, payload); err != nil {
			return err
		}
		run, err := q.GetWorkflowRun(ctx, runID)
		if err != nil {
			return err
		}
		if _, err := q.AppendHistory(ctx, runID, "signal_received", mustJSON(map[string]any{"name": name})); err != nil {
			return err
		}
		steps, err := q.GetSteps(ctx, runID)
		if err != nil {
			return err
		}
		return e.reconcileTx(ctx, q, run, steps)
	})
}

// CancelRun transitions a run (and every non-terminal step in it) to CANCELLED.
func (e *Engine) CancelRun(ctx context.Context, runID uuid.UUID) error {
	return e.Store.WithTx(ctx, func(ctx context.Context, q store.Queries) error {
		run, err := q.GetWorkflowRun(ctx, runID)
		if err != nil {
			return err
		}
		if run.Status == store.RunCompleted || run.Status == store.RunFailed || run.Status == store.RunCancelled {
			return nil
		}
		steps, err := q.GetSteps(ctx, runID)
		if err != nil {
			return err
		}
		for _, s := range steps {
			if store.StepTerminal(s.Status) {
				continue
			}
			if _, err := q.UpdateStepStatus(ctx, s.ID, []store.StepStatus{s.Status}, store.StepCancelled, nil, nil, false); err != nil {
				return err
			}
		}
		if err := q.CancelTimersForRun(ctx, runID); err != nil {
			return err
		}
		if _, err := q.UpdateWorkflowRunStatus(ctx, runID, []store.RunStatus{store.RunPending, store.RunRunning}, store.RunCancelled, nil, nil); err != nil {
			return err
		}
		_, err = q.AppendHistory(ctx, runID, "run_cancelled", nil)
		return err
	})
}

// RecoverShards is called at startup and periodically thereafter: it re-runs Reconcile for
// every non-terminal run owned by the given shard set. Because reconcileTx is a pure
// function of DB state, this is what makes crash recovery correct — a run that was mid-flight
// when the process died gets exactly the same treatment as one that's simply progressing
// normally under a periodic nudge.
func (e *Engine) RecoverShards(ctx context.Context, shardIDs []int) (int, error) {
	runs, err := e.Store.ListRunningRunsForShards(ctx, shardIDs)
	if err != nil {
		return 0, err
	}
	for _, r := range runs {
		if err := e.Reconcile(ctx, r.ID); err != nil {
			e.Log.Error("engine: recover reconcile failed", "run_id", r.ID, "err", err)
		}
	}
	return len(runs), nil
}
