// Command server is the orchestrator server: it hosts the gRPC worker API, the HTTP
// dashboard API, and the background services that make durable execution work — the outbox
// relay, the timer poller, the lease reaper, and (optionally) the sharded-scheduler
// leader-election loop. See README.md for the full design and how the pieces
// fit together operationally.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	workerpb "github.com/aryanraj/workflow-orchestrator/gen/workerpb"
	"github.com/aryanraj/workflow-orchestrator/internal/config"
	"github.com/aryanraj/workflow-orchestrator/internal/engine"
	"github.com/aryanraj/workflow-orchestrator/internal/grpcapi"
	"github.com/aryanraj/workflow-orchestrator/internal/httpapi"
	"github.com/aryanraj/workflow-orchestrator/internal/leases"
	"github.com/aryanraj/workflow-orchestrator/internal/outbox"
	"github.com/aryanraj/workflow-orchestrator/internal/queue/redisstream"
	"github.com/aryanraj/workflow-orchestrator/internal/scheduler"
	"github.com/aryanraj/workflow-orchestrator/internal/store/postgres"
	"github.com/aryanraj/workflow-orchestrator/internal/timers"
	"github.com/aryanraj/workflow-orchestrator/internal/workers"
)

func main() {
	// `server healthcheck` does a local HTTP GET to /healthz and exits 0/1 accordingly. This
	// exists so Docker's HEALTHCHECK can probe the server without needing wget/curl/a shell —
	// the runtime image is distroless (see Dockerfile.server) and has none of those.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(runHealthcheck())
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.LoadServerConfig()
	log = log.With("node_id", cfg.NodeID)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()
	log.Info("connected to postgres and applied migrations")

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("failed to connect to redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()
	log.Info("connected to redis")

	q := redisstream.New(rdb, log)
	eng := engine.New(st, q, log)
	registry := workers.NewRegistry(rdb)

	var sched *scheduler.Scheduler
	var shardIDsFunc func() []int
	if cfg.EnableSharding {
		sched = scheduler.New(rdb, cfg.NodeID, log)
		shardIDsFunc = sched.OwnedShards
		go sched.Run(ctx)
		log.Info("sharded scheduler enabled", "num_shards", scheduler.NumShards)
	} else {
		log.Info("sharded scheduler disabled (single-node mode); this node owns all shards")
	}

	relay := outbox.New(st, q, log)
	if shardIDsFunc != nil {
		relay.ShardIDs = shardIDsFunc
	}
	go relay.Run(ctx)

	timerPoller := timers.New(st, eng, log)
	if shardIDsFunc != nil {
		timerPoller.ShardIDs = shardIDsFunc
	}
	go timerPoller.Run(ctx)

	reaper := leases.New(eng, log)
	go reaper.Run(ctx)

	// Recovery sweep: on startup, and periodically thereafter, re-run DAG evaluation for
	// every non-terminal run this node owns. This is what makes a killed-and-restarted
	// server correctly resume mid-flight workflows (see the
	// TestServerRestartMidWorkflow durability test in test/e2e) — reconcile is a pure
	// function of DB state, so recovery is just "call it again".
	go runRecoverySweep(ctx, eng, shardIDsFunc, log)

	grpcServer := grpc.NewServer()
	workerpb.RegisterWorkerServiceServer(grpcServer, grpcapi.New(st, eng, rdb, log, cfg.LeaseDuration))
	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Error("failed to listen for gRPC", "addr", cfg.GRPCAddr, "err", err)
		os.Exit(1)
	}
	go func() {
		log.Info("gRPC server listening", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(grpcLis); err != nil {
			log.Error("grpc server stopped", "err", err)
		}
	}()

	httpSrv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpapi.New(eng, st, registry, sched, log),
	}
	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server stopped", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received, draining")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
	log.Info("shutdown complete")
}

func runRecoverySweep(ctx context.Context, eng *engine.Engine, shardIDsFunc func() []int, log *slog.Logger) {
	sweep := func() {
		var shardIDs []int
		if shardIDsFunc != nil {
			shardIDs = shardIDsFunc()
		}
		n, err := eng.RecoverShards(ctx, shardIDs)
		if err != nil {
			log.Error("recovery sweep failed", "err", err)
			return
		}
		if n > 0 {
			log.Info("recovery sweep reconciled runs", "count", n)
		}
	}
	sweep() // immediately at startup — this is what resumes work after a crash
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

// runHealthcheck reads HTTP_ADDR the same way LoadServerConfig does, GETs /healthz on
// localhost, and returns a process exit code — see the `server healthcheck` branch in main().
func runHealthcheck() int {
	cfg := config.LoadServerConfig()
	addr := cfg.HTTPAddr
	if len(addr) > 0 && addr[0] == ':' {
		addr = "localhost" + addr
	}
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck failed:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck failed: status", resp.StatusCode)
		return 1
	}
	return 0
}
