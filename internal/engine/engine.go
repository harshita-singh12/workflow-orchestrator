// Package engine contains the orchestration logic: turning DAG definitions into runs,
// evaluating which steps are ready to dispatch, handling task results, retry backoff,
// timers, signals and run-completion detection. Every exported entry point is designed to
// be a pure function of database state wrapped in a single transaction — that property is
// what makes durable execution work: a crash loses nothing but a few milliseconds of
// in-flight work, and recovery is just calling reconcile again.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/aryanraj/workflow-orchestrator/internal/queue"
	"github.com/aryanraj/workflow-orchestrator/internal/store"
	"github.com/aryanraj/workflow-orchestrator/internal/workflowdsl"
)

// NumShards is the fixed size of the consistent-hash ring workflow runs are partitioned
// over. Fixed at compile time for simplicity; changing it would
// require a migration that recomputes shard_id for existing rows.
const NumShards = 256

// DefaultLeaseDuration is how long a worker holds a claimed task attempt before the reaper
// considers it abandoned. Workers heartbeat well before this to keep long tasks alive.
const DefaultLeaseDuration = 30 * time.Second

type Engine struct {
	Store store.Store
	Queue queue.Queue
	Log   *slog.Logger
}

func New(s store.Store, q queue.Queue, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.Default()
	}
	return &Engine{Store: s, Queue: q, Log: log}
}

// ShardFor computes the deterministic shard a run ID belongs to.
func ShardFor(id uuid.UUID) int {
	h := fnv.New32a()
	_, _ = h.Write(id[:])
	return int(h.Sum32() % NumShards)
}

// RegisterDefinition validates and persists a workflow DAG definition.
func (e *Engine) RegisterDefinition(ctx context.Context, def *workflowdsl.Definition) (*store.WorkflowDefinition, error) {
	if err := def.Validate(); err != nil {
		return nil, err
	}
	dagJSON, err := def.ToJSON()
	if err != nil {
		return nil, err
	}
	return e.Store.CreateWorkflowDefinition(ctx, def.Name, def.Version, dagJSON)
}

// CreateRun instantiates a new run of the named workflow (latest version unless version>0),
// creates all its step rows, and performs the first reconcile pass (dispatching root steps)
// — all in one transaction, so a run never exists in Postgres without also having its
// initial steps queued for dispatch.
func (e *Engine) CreateRun(ctx context.Context, name string, version int, input json.RawMessage) (*store.WorkflowRun, error) {
	var def *store.WorkflowDefinition
	var err error
	if version > 0 {
		def, err = e.Store.GetWorkflowDefinition(ctx, name, version)
	} else {
		def, err = e.Store.GetLatestWorkflowDefinition(ctx, name)
	}
	if err != nil {
		return nil, fmt.Errorf("engine: lookup definition %s: %w", name, err)
	}

	var parsed workflowdsl.Definition
	if err := json.Unmarshal(def.DAG, &parsed); err != nil {
		return nil, fmt.Errorf("engine: decode stored dag: %w", err)
	}
	if input == nil {
		input = json.RawMessage(`{}`)
	}

	var run *store.WorkflowRun
	err = e.Store.WithTx(ctx, func(ctx context.Context, q store.Queries) error {
		// shard_id can't be known before the run ID is generated, so create the row first
		// with a placeholder-free approach: we generate the ID client-side isn't supported by
		// CreateWorkflowRun (DB assigns it), so instead we compute shard_id from a
		// pre-generated UUID and pass it through. This keeps ID generation in one place (DB
		// default) while still letting us shard on it.
		id := uuid.New()
		shardID := ShardFor(id)
		var err error
		run, err = q.CreateWorkflowRun(ctx, id, def.ID, name, def.Version, shardID, input)
		if err != nil {
			return err
		}
		steps, err := q.CreateSteps(ctx, run.ID, parsed.ToNewSteps())
		if err != nil {
			return err
		}
		if _, err := q.AppendHistory(ctx, run.ID, "run_created", mustJSON(map[string]any{"name": name, "version": def.Version, "steps": len(steps)})); err != nil {
			return err
		}
		return e.reconcileTx(ctx, q, run, steps)
	})
	if err != nil {
		return nil, err
	}
	// Re-fetch so the caller sees post-reconcile status (RUNNING, first steps QUEUED).
	return e.Store.GetWorkflowRun(ctx, run.ID)
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
