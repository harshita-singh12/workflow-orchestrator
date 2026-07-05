// Package outbox implements the relay half of the transactional outbox pattern: it polls
// Postgres for PENDING rows, publishes each to its Redis
// Stream, and marks it PUBLISHED — all with FOR UPDATE SKIP LOCKED so multiple relay
// instances (one per server node) can run concurrently without duplicating work, and with
// at-least-once semantics across a crash (a row that got XADD'd but not yet marked
// PUBLISHED before a crash is simply republished — harmless, because task_attempt claiming
// is itself idempotent).
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/aryanraj/workflow-orchestrator/internal/queue"
	"github.com/aryanraj/workflow-orchestrator/internal/store"
)

type Relay struct {
	Store     store.Store
	Queue     queue.Queue
	Log       *slog.Logger
	Interval  time.Duration
	BatchSize int
	// ShardIDs, if non-nil, restricts the relay to only publish outbox rows for those shards
	// (Phase 2 sharded scheduling). Nil means "no filter, own everything" — the correct
	// setting for a single-node deployment.
	ShardIDs func() []int
}

func New(s store.Store, q queue.Queue, log *slog.Logger) *Relay {
	if log == nil {
		log = slog.Default()
	}
	return &Relay{Store: s, Queue: q, Log: log, Interval: 200 * time.Millisecond, BatchSize: 100}
}

// Run blocks, polling until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := r.tick(ctx)
			if err != nil {
				r.Log.Error("outbox: relay tick failed", "err", err)
				continue
			}
			if n > 0 {
				r.Log.Debug("outbox: published", "count", n)
			}
		}
	}
}

func (r *Relay) tick(ctx context.Context) (int, error) {
	var shardIDs []int
	if r.ShardIDs != nil {
		shardIDs = r.ShardIDs()
	}
	published := 0
	err := r.Store.WithTx(ctx, func(ctx context.Context, q store.Queries) error {
		rows, err := q.FetchAndLockPendingOutbox(ctx, shardIDs, r.BatchSize)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if _, err := r.Queue.Publish(ctx, row.StreamName, row.Payload); err != nil {
				return err
			}
			if err := q.MarkOutboxPublished(ctx, row.ID); err != nil {
				return err
			}
			published++
		}
		return nil
	})
	return published, err
}
