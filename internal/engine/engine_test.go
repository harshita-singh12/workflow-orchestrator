package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/aryanraj/workflow-orchestrator/internal/engine"
	memq "github.com/aryanraj/workflow-orchestrator/internal/queue/memory"
	"github.com/aryanraj/workflow-orchestrator/internal/store"
	mems "github.com/aryanraj/workflow-orchestrator/internal/store/memory"
	"github.com/aryanraj/workflow-orchestrator/internal/workflowdsl"
)

func newTestEngine(t *testing.T) (*engine.Engine, store.Store) {
	t.Helper()
	s := mems.New()
	q := memq.New()
	return engine.New(s, q, nil), s
}

func mustDef(t *testing.T, yamlOrJSON string) *workflowdsl.Definition {
	t.Helper()
	def, err := workflowdsl.Parse([]byte(yamlOrJSON))
	require.NoError(t, err)
	require.NoError(t, def.Validate())
	return def
}

// simulateAttempt finds the (single, expected) QUEUED task attempt for a step and drives it
// through claim+complete as a real worker would over gRPC, without needing a real worker
// process — this exercises the exact same store CAS methods the gRPC layer calls.
func simulateAttempt(t *testing.T, ctx context.Context, s store.Store, stepID uuid.UUID, success bool, output string) {
	t.Helper()
	attempts, err := s.ListTaskAttemptsForStep(ctx, stepID)
	require.NoError(t, err)
	require.NotEmpty(t, attempts)
	latest := attempts[len(attempts)-1]
	require.Equal(t, store.AttemptQueued, latest.Status, "expected latest attempt to be QUEUED")

	claimed, result, err := s.ClaimTaskAttempt(ctx, latest.ID, latest.IdempotencyKey, "worker-1", 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, store.ClaimOK, result)
	require.NotNil(t, claimed)
}

func completeAttempt(t *testing.T, ctx context.Context, e *engine.Engine, stepID uuid.UUID, success bool, output string) {
	t.Helper()
	attempts, err := e.Store.ListTaskAttemptsForStep(ctx, stepID)
	require.NoError(t, err)
	latest := attempts[len(attempts)-1]
	var errMsg *string
	if !success {
		m := "boom"
		errMsg = &m
	}
	accepted, err := e.HandleTaskResult(ctx, latest.ID, "worker-1", success, json.RawMessage(output), errMsg)
	require.NoError(t, err)
	require.True(t, accepted)
}

func TestSequentialWorkflowCompletes(t *testing.T) {
	ctx := context.Background()
	e, s := newTestEngine(t)

	def := mustDef(t, `
name: seq-wf
version: 1
steps:
  - name: a
    type: noop
  - name: b
    type: noop
    depends_on: [a]
  - name: c
    type: noop
    depends_on: [b]
`)
	_, err := e.RegisterDefinition(ctx, def)
	require.NoError(t, err)

	run, err := e.CreateRun(ctx, "seq-wf", 0, nil)
	require.NoError(t, err)
	require.Equal(t, store.RunRunning, run.Status)

	steps, err := s.GetSteps(ctx, run.ID)
	require.NoError(t, err)
	byName := map[string]store.Step{}
	for _, st := range steps {
		byName[st.StepName] = st
	}
	require.Equal(t, store.StepQueued, byName["a"].Status)
	require.Equal(t, store.StepPending, byName["b"].Status)

	simulateAttempt(t, ctx, s, byName["a"].ID, true, `{}`)
	completeAttempt(t, ctx, e, byName["a"].ID, true, `{"ok":true}`)

	steps, _ = s.GetSteps(ctx, run.ID)
	for _, st := range steps {
		byName[st.StepName] = st
	}
	require.Equal(t, store.StepCompleted, byName["a"].Status)
	require.Equal(t, store.StepQueued, byName["b"].Status)

	simulateAttempt(t, ctx, s, byName["b"].ID, true, `{}`)
	completeAttempt(t, ctx, e, byName["b"].ID, true, `{}`)

	steps, _ = s.GetSteps(ctx, run.ID)
	for _, st := range steps {
		byName[st.StepName] = st
	}
	require.Equal(t, store.StepQueued, byName["c"].Status)
	simulateAttempt(t, ctx, s, byName["c"].ID, true, `{}`)
	completeAttempt(t, ctx, e, byName["c"].ID, true, `{}`)

	final, err := s.GetWorkflowRun(ctx, run.ID)
	require.NoError(t, err)
	require.Equal(t, store.RunCompleted, final.Status)
}

func TestParallelFanOutFanIn(t *testing.T) {
	ctx := context.Background()
	e, s := newTestEngine(t)

	def := mustDef(t, `
name: fanout-wf
version: 1
steps:
  - name: start
    type: noop
  - name: left
    type: noop
    depends_on: [start]
  - name: right
    type: noop
    depends_on: [start]
  - name: join
    type: noop
    depends_on: [left, right]
`)
	_, err := e.RegisterDefinition(ctx, def)
	require.NoError(t, err)
	run, err := e.CreateRun(ctx, "fanout-wf", 0, nil)
	require.NoError(t, err)

	getStep := func(name string) store.Step {
		steps, _ := s.GetSteps(ctx, run.ID)
		for _, st := range steps {
			if st.StepName == name {
				return st
			}
		}
		t.Fatalf("step %s not found", name)
		return store.Step{}
	}

	start := getStep("start")
	simulateAttempt(t, ctx, s, start.ID, true, `{}`)
	completeAttempt(t, ctx, e, start.ID, true, `{}`)

	left, right := getStep("left"), getStep("right")
	require.Equal(t, store.StepQueued, left.Status)
	require.Equal(t, store.StepQueued, right.Status)
	require.Equal(t, store.StepPending, getStep("join").Status)

	simulateAttempt(t, ctx, s, left.ID, true, `{}`)
	completeAttempt(t, ctx, e, left.ID, true, `{}`)
	require.Equal(t, store.StepPending, getStep("join").Status, "join must wait for both branches")

	simulateAttempt(t, ctx, s, right.ID, true, `{}`)
	completeAttempt(t, ctx, e, right.ID, true, `{}`)
	require.Equal(t, store.StepQueued, getStep("join").Status)

	join := getStep("join")
	simulateAttempt(t, ctx, s, join.ID, true, `{}`)
	completeAttempt(t, ctx, e, join.ID, true, `{}`)

	final, _ := s.GetWorkflowRun(ctx, run.ID)
	require.Equal(t, store.RunCompleted, final.Status)
}

func TestRetryThenSucceed(t *testing.T) {
	ctx := context.Background()
	e, s := newTestEngine(t)

	def := mustDef(t, `
name: retry-wf
version: 1
steps:
  - name: flaky
    type: noop
    max_attempts: 3
    initial_backoff_ms: 1
    max_backoff_ms: 5
`)
	_, err := e.RegisterDefinition(ctx, def)
	require.NoError(t, err)
	run, err := e.CreateRun(ctx, "retry-wf", 0, nil)
	require.NoError(t, err)

	steps, _ := s.GetSteps(ctx, run.ID)
	flaky := steps[0]

	simulateAttempt(t, ctx, s, flaky.ID, false, "")
	completeAttempt(t, ctx, e, flaky.ID, false, "")

	steps, _ = s.GetSteps(ctx, run.ID)
	flaky = steps[0]
	require.Equal(t, store.StepRetryBackoff, flaky.Status)
	require.Equal(t, 1, flaky.AttemptCount)

	// Find and fire the retry timer directly (simulating the timer service).
	timers, err := s.ListDueTimers(ctx, nil, 10)
	require.NoError(t, err)
	// backoff may not have elapsed yet in wall-clock terms for a 1ms backoff on a slow CI box;
	// poll briefly.
	deadline := time.Now().Add(2 * time.Second)
	for len(timers) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		timers, err = s.ListDueTimers(ctx, nil, 10)
		require.NoError(t, err)
	}
	require.Len(t, timers, 1)
	require.NoError(t, e.HandleFiredTimer(ctx, timers[0].ID))

	steps, _ = s.GetSteps(ctx, run.ID)
	flaky = steps[0]
	require.Equal(t, store.StepQueued, flaky.Status)
	require.Equal(t, 2, flaky.AttemptCount)

	simulateAttempt(t, ctx, s, flaky.ID, true, `{}`)
	completeAttempt(t, ctx, e, flaky.ID, true, `{"done":true}`)

	final, _ := s.GetWorkflowRun(ctx, run.ID)
	require.Equal(t, store.RunCompleted, final.Status)
}

func TestExhaustedRetriesFailsRunAndSkipsDownstream(t *testing.T) {
	ctx := context.Background()
	e, s := newTestEngine(t)

	def := mustDef(t, `
name: fail-wf
version: 1
steps:
  - name: doomed
    type: noop
    max_attempts: 1
  - name: after
    type: noop
    depends_on: [doomed]
`)
	_, err := e.RegisterDefinition(ctx, def)
	require.NoError(t, err)
	run, err := e.CreateRun(ctx, "fail-wf", 0, nil)
	require.NoError(t, err)

	steps, _ := s.GetSteps(ctx, run.ID)
	var doomed store.Step
	for _, st := range steps {
		if st.StepName == "doomed" {
			doomed = st
		}
	}

	simulateAttempt(t, ctx, s, doomed.ID, false, "")
	completeAttempt(t, ctx, e, doomed.ID, false, "")

	steps, _ = s.GetSteps(ctx, run.ID)
	byName := map[string]store.Step{}
	for _, st := range steps {
		byName[st.StepName] = st
	}
	require.Equal(t, store.StepFailed, byName["doomed"].Status)
	require.Equal(t, store.StepSkipped, byName["after"].Status)

	final, _ := s.GetWorkflowRun(ctx, run.ID)
	require.Equal(t, store.RunFailed, final.Status)
	require.NotNil(t, final.Error)
}

func TestSignalWaitStepResolvesFromSignal(t *testing.T) {
	ctx := context.Background()
	e, s := newTestEngine(t)

	def := mustDef(t, `
name: signal-wf
version: 1
steps:
  - name: wait_for_approval
    type: signal_wait
  - name: after
    type: noop
    depends_on: [wait_for_approval]
`)
	_, err := e.RegisterDefinition(ctx, def)
	require.NoError(t, err)
	run, err := e.CreateRun(ctx, "signal-wf", 0, nil)
	require.NoError(t, err)

	steps, _ := s.GetSteps(ctx, run.ID)
	byName := map[string]store.Step{}
	for _, st := range steps {
		byName[st.StepName] = st
	}
	require.Equal(t, store.StepWaiting, byName["wait_for_approval"].Status)

	require.NoError(t, e.ApplySignal(ctx, run.ID, "wait_for_approval", json.RawMessage(`{"approved":true}`)))

	steps, _ = s.GetSteps(ctx, run.ID)
	for _, st := range steps {
		byName[st.StepName] = st
	}
	require.Equal(t, store.StepCompleted, byName["wait_for_approval"].Status)
	require.Equal(t, store.StepQueued, byName["after"].Status)

	simulateAttempt(t, ctx, s, byName["after"].ID, true, `{}`)
	completeAttempt(t, ctx, e, byName["after"].ID, true, `{}`)

	final, _ := s.GetWorkflowRun(ctx, run.ID)
	require.Equal(t, store.RunCompleted, final.Status)
}

func TestCancelRun(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	def := mustDef(t, `
name: cancel-wf
version: 1
steps:
  - name: a
    type: noop
  - name: b
    type: noop
    depends_on: [a]
`)
	_, err := e.RegisterDefinition(ctx, def)
	require.NoError(t, err)
	run, err := e.CreateRun(ctx, "cancel-wf", 0, nil)
	require.NoError(t, err)

	require.NoError(t, e.CancelRun(ctx, run.ID))

	final, err := e.Store.GetWorkflowRun(ctx, run.ID)
	require.NoError(t, err)
	require.Equal(t, store.RunCancelled, final.Status)
}

func TestDuplicateReportIsIgnored(t *testing.T) {
	ctx := context.Background()
	e, s := newTestEngine(t)

	def := mustDef(t, `
name: dup-wf
version: 1
steps:
  - name: a
    type: noop
`)
	_, err := e.RegisterDefinition(ctx, def)
	require.NoError(t, err)
	run, err := e.CreateRun(ctx, "dup-wf", 0, nil)
	require.NoError(t, err)

	steps, _ := s.GetSteps(ctx, run.ID)
	a := steps[0]
	simulateAttempt(t, ctx, s, a.ID, true, `{}`)
	completeAttempt(t, ctx, e, a.ID, true, `{"x":1}`)

	attempts, _ := s.ListTaskAttemptsForStep(ctx, a.ID)
	latest := attempts[len(attempts)-1]

	// A second, duplicate ReportResult for the same attempt (e.g. a retried RPC) must be a
	// harmless no-op, not an error and not a double-apply.
	accepted, err := e.HandleTaskResult(ctx, latest.ID, "worker-1", true, json.RawMessage(`{"x":2}`), nil)
	require.NoError(t, err)
	require.False(t, accepted)

	final, _ := s.GetWorkflowRun(ctx, run.ID)
	require.Equal(t, store.RunCompleted, final.Status)
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(final.Output, &out))
	require.JSONEq(t, `{"x":1}`, string(out["a"])) // first report's value won, not the duplicate's
}

func TestClaimIsExactlyOnceUnderDoubleDelivery(t *testing.T) {
	ctx := context.Background()
	e, s := newTestEngine(t)

	def := mustDef(t, `
name: claim-wf
version: 1
steps:
  - name: a
    type: noop
`)
	_, err := e.RegisterDefinition(ctx, def)
	require.NoError(t, err)
	run, err := e.CreateRun(ctx, "claim-wf", 0, nil)
	require.NoError(t, err)

	steps, _ := s.GetSteps(ctx, run.ID)
	attempts, _ := s.ListTaskAttemptsForStep(ctx, steps[0].ID)
	latest := attempts[0]

	// Two "workers" race to claim the same attempt (simulating an at-least-once redelivery).
	_, res1, err := s.ClaimTaskAttempt(ctx, latest.ID, latest.IdempotencyKey, "worker-A", 30*time.Second)
	require.NoError(t, err)
	_, res2, err := s.ClaimTaskAttempt(ctx, latest.ID, latest.IdempotencyKey, "worker-B", 30*time.Second)
	require.NoError(t, err)

	require.Equal(t, store.ClaimOK, res1)
	require.Equal(t, store.ClaimAlreadyClaimed, res2)
}

func TestRegisterDuplicateDefinitionIsConflict(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	def := mustDef(t, `
name: dup-def-wf
version: 1
steps:
  - name: a
    type: noop
`)
	_, err := e.RegisterDefinition(ctx, def)
	require.NoError(t, err)

	// Re-parse: RegisterDefinition mutates/defaults its argument, so reuse a fresh copy rather
	// than the already-validated def above.
	again := mustDef(t, `
name: dup-def-wf
version: 1
steps:
  - name: a
    type: noop
`)
	_, err = e.RegisterDefinition(ctx, again)
	require.Error(t, err)
	require.True(t, errors.Is(err, store.ErrConflict), "expected store.ErrConflict, got %v", err)
}

func TestListWorkflowRunsRespectsOffset(t *testing.T) {
	ctx := context.Background()
	e, s := newTestEngine(t)

	def := mustDef(t, `
name: paged-wf
version: 1
steps:
  - name: a
    type: noop
`)
	_, err := e.RegisterDefinition(ctx, def)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err := e.CreateRun(ctx, "paged-wf", 0, nil)
		require.NoError(t, err)
	}

	all, err := s.ListWorkflowRuns(ctx, store.RunFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, all, 3)

	paged, err := s.ListWorkflowRuns(ctx, store.RunFilter{Limit: 10, Offset: 1})
	require.NoError(t, err)
	require.Len(t, paged, 2, "offset should skip the first (most recent) run")
	require.NotEqual(t, all[0].ID, paged[0].ID)
	require.Equal(t, all[1].ID, paged[0].ID)
}
