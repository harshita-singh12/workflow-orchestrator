// Package leases implements the lease-expiry reaper: it periodically finds task attempts
// whose worker went silent past its lease deadline and drives them through the same
// retry/fail path a reported failure would take, closing the "worker crashed after claiming
// but before reporting" gap.
package leases

import (
	"context"
	"log/slog"
	"time"

	"github.com/aryanraj/workflow-orchestrator/internal/engine"
)

type Reaper struct {
	Engine    *engine.Engine
	Log       *slog.Logger
	Interval  time.Duration
	BatchSize int
}

func New(e *engine.Engine, log *slog.Logger) *Reaper {
	if log == nil {
		log = slog.Default()
	}
	return &Reaper{Engine: e, Log: log, Interval: 1 * time.Second, BatchSize: 100}
}

func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := r.Engine.ReapExpiredLeases(ctx, r.BatchSize)
			if err != nil {
				r.Log.Error("leases: reap failed", "err", err)
				continue
			}
			if n > 0 {
				r.Log.Info("leases: reaped expired attempts", "count", n)
			}
		}
	}
}
