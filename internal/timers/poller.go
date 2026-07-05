// Package timers implements the durable timer poller: it periodically asks Postgres "what's
// due", then hands each candidate to the engine, which
// atomically claims and fires it (CAS) in one transaction together with its effect (e.g.
// re-dispatching a retry). No wall clock other than the database's is ever consulted for the
// due-comparison, which is what avoids needing NTP-level clock sync between nodes.
package timers

import (
	"context"
	"log/slog"
	"time"

	"github.com/aryanraj/workflow-orchestrator/internal/engine"
	"github.com/aryanraj/workflow-orchestrator/internal/store"
)

type Poller struct {
	Store     store.Store
	Engine    *engine.Engine
	Log       *slog.Logger
	Interval  time.Duration
	BatchSize int
	ShardIDs  func() []int
}

func New(s store.Store, e *engine.Engine, log *slog.Logger) *Poller {
	if log == nil {
		log = slog.Default()
	}
	return &Poller{Store: s, Engine: e, Log: log, Interval: 200 * time.Millisecond, BatchSize: 100}
}

func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.tick(ctx); err != nil {
				p.Log.Error("timers: poll tick failed", "err", err)
			}
		}
	}
}

func (p *Poller) tick(ctx context.Context) error {
	var shardIDs []int
	if p.ShardIDs != nil {
		shardIDs = p.ShardIDs()
	}
	due, err := p.Store.ListDueTimers(ctx, shardIDs, p.BatchSize)
	if err != nil {
		return err
	}
	for _, t := range due {
		if err := p.Engine.HandleFiredTimer(ctx, t.ID); err != nil {
			p.Log.Error("timers: handle fired timer failed", "timer_id", t.ID, "err", err)
		}
	}
	return nil
}
