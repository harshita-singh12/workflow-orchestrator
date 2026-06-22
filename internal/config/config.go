// Package config centralizes environment-variable configuration for the server and worker
// binaries. Every value has a sane local-dev default so `go run ./cmd/server` works without
// any .env file; docker-compose.yml overrides them for the containerized topology.
package config

import (
	"os"
	"strconv"
	"time"
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

type ServerConfig struct {
	NodeID      string
	DatabaseURL string
	RedisAddr   string
	HTTPAddr    string
	GRPCAddr    string
	// EnableSharding turns on the scheduler's leader-election/consistent-hashing loops
	// (Phase 2). Disabled by default so a single `go run ./cmd/server` doesn't need Redis
	// leader-election machinery just to run one instance — see README "Running without
	// Docker" for details on when you'd turn this on.
	EnableSharding bool
	// LeaseDuration is how long a claimed task attempt is held before the reaper considers
	// it abandoned. Overridable (LEASE_DURATION_SECONDS) so tests can shrink it — the
	// durability restart test in test/e2e uses a few seconds instead of the 30s production
	// default so a lease-expiry-triggered retry doesn't make the test suite slow.
	LeaseDuration time.Duration
}

func LoadServerConfig() ServerConfig {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "node"
		}
		nodeID = hostname + "-" + strconv.Itoa(os.Getpid())
	}
	return ServerConfig{
		NodeID:         nodeID,
		DatabaseURL:    getEnv("DATABASE_URL", "postgres://orchestrator:orchestrator@localhost:5432/orchestrator?sslmode=disable"),
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		HTTPAddr:       getEnv("HTTP_ADDR", ":8080"),
		GRPCAddr:       getEnv("GRPC_ADDR", ":9090"),
		EnableSharding: getEnv("ENABLE_SHARDING", "false") == "true",
		LeaseDuration:  time.Duration(getEnvInt("LEASE_DURATION_SECONDS", 30)) * time.Second,
	}
}

type WorkerConfig struct {
	WorkerID       string
	ServerGRPCAddr string
	RedisAddr      string
	Queues         []string
	Concurrency    int
}

func LoadWorkerConfig() WorkerConfig {
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "worker"
		}
		workerID = hostname + "-" + strconv.Itoa(os.Getpid())
	}
	return WorkerConfig{
		WorkerID:       workerID,
		ServerGRPCAddr: getEnv("SERVER_GRPC_ADDR", "localhost:9090"),
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		Concurrency:    getEnvInt("WORKER_CONCURRENCY", 4),
	}
}
