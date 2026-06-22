// Command worker is a generic worker process: it registers a handful of demo task handlers
// (noop, sleep, transform, flaky, http_fetch) and runs the worker runtime
// (internal/worker) against whichever server it's pointed at. Real deployments would swap in
// domain-specific handlers via Worker.Register; these exist so the whole system is runnable
// and demonstrable end-to-end (see README.md and the load test in loadtest/) without needing
// bespoke business logic.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	workerpb "github.com/aryanraj/workflow-orchestrator/gen/workerpb"
	"github.com/aryanraj/workflow-orchestrator/internal/config"
	"github.com/aryanraj/workflow-orchestrator/internal/queue/redisstream"
	"github.com/aryanraj/workflow-orchestrator/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.LoadWorkerConfig()
	log = log.With("worker_id", cfg.WorkerID)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("failed to connect to redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	conn, err := grpc.NewClient(cfg.ServerGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("failed to dial server", "addr", cfg.ServerGRPCAddr, "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := workerpb.NewWorkerServiceClient(conn)

	q := redisstream.New(rdb, log)
	w := worker.New(cfg.WorkerID, q, client, log)
	w.Concurrency = cfg.Concurrency

	registerDemoHandlers(w)

	log.Info("worker starting", "server", cfg.ServerGRPCAddr, "concurrency", cfg.Concurrency)
	if err := w.Run(ctx); err != nil {
		log.Error("worker exited with error", "err", err)
		os.Exit(1)
	}
	log.Info("worker shut down cleanly")
}

// registerDemoHandlers wires up the task types used by the example workflows in
// examples/*.yaml and the load test.
func registerDemoHandlers(w *worker.Worker) {
	w.Register("noop", func(ctx context.Context, input json.RawMessage, idemKey string) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})

	w.Register("sleep", func(ctx context.Context, input json.RawMessage, idemKey string) (json.RawMessage, error) {
		var params struct {
			Ms int `json:"ms"`
		}
		_ = json.Unmarshal(input, &params)
		if params.Ms <= 0 {
			params.Ms = 50
		}
		select {
		case <-time.After(time.Duration(params.Ms) * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return json.RawMessage(fmt.Sprintf(`{"slept_ms":%d}`, params.Ms)), nil
	})

	w.Register("transform", func(ctx context.Context, input json.RawMessage, idemKey string) (json.RawMessage, error) {
		out := map[string]any{"received": json.RawMessage(input), "idempotency_key": idemKey}
		b, _ := json.Marshal(out)
		return b, nil
	})

	// flaky fails deterministically on early attempts based on input.fail_attempts, then
	// succeeds — useful for demonstrating retry/backoff in the dashboard. Attempt count is
	// not available to the handler directly, so it uses a random failure rate instead
	// (simpler, and still exercises the retry path realistically under load).
	w.Register("flaky", func(ctx context.Context, input json.RawMessage, idemKey string) (json.RawMessage, error) {
		var params struct {
			FailRate float64 `json:"fail_rate"`
		}
		_ = json.Unmarshal(input, &params)
		if params.FailRate <= 0 {
			params.FailRate = 0.4
		}
		if rand.Float64() < params.FailRate {
			return nil, fmt.Errorf("simulated transient failure")
		}
		return json.RawMessage(`{"ok":true}`), nil
	})

	w.Register("http_fetch", func(ctx context.Context, input json.RawMessage, idemKey string) (json.RawMessage, error) {
		// Deliberately does not make a real network call in this demo build (no external
		// dependency / no fabricated API keys, per project constraints) — it simulates
		// latency so it's still useful for load testing and dashboard demos.
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return json.RawMessage(`{"status":200,"simulated":true}`), nil
	})
}
