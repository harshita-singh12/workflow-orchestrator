// Package config centralizes environment-variable configuration for the server and worker
// binaries. Every value has a sane local-dev default so `go run ./cmd/server` works without
// any .env file; docker-compose.yml overrides them for the containerized topology.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultDevAPIKey is the shared API key used when WORKFLOW_API_KEY isn't set, so that
// `go run ./cmd/server` / `docker compose up` work out of the box without any extra setup.
// It is intentionally well-known and documented in the README — change it (via WORKFLOW_API_KEY)
// before deploying anywhere the server is reachable by anyone other than you.
const DefaultDevAPIKey = "dev-local-key-change-me"

// DefaultAllowedOrigin is the CORS origin allowed by default, matching the frontend's
// docker-compose host port (see docker-compose.yml's `frontend` service).
const DefaultAllowedOrigin = "http://localhost:3002"

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
	// APIKey is the shared secret required (as `Authorization: Bearer <key>`) on every HTTP
	// API route except /healthz, and on every gRPC worker-API call. Defaults to
	// DefaultDevAPIKey so local dev and docker-compose work unconfigured; set WORKFLOW_API_KEY
	// to anything else before running this somewhere it's reachable by anyone but you.
	APIKey string
	// AllowedOrigins is the CORS allowlist for the HTTP API, reflected verbatim on
	// Access-Control-Allow-Origin when a request's Origin header matches one of these exactly
	// — never a wildcard. Overridable via CORS_ALLOWED_ORIGINS (comma-separated).
	AllowedOrigins []string
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
		APIKey:         getEnv("WORKFLOW_API_KEY", DefaultDevAPIKey),
		AllowedOrigins: getEnvList("CORS_ALLOWED_ORIGINS", []string{DefaultAllowedOrigin}),
	}
}

// getEnvList reads a comma-separated env var into a trimmed, non-empty string slice, falling
// back to def when the var is unset.
func getEnvList(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var out []string
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

type WorkerConfig struct {
	WorkerID       string
	ServerGRPCAddr string
	RedisAddr      string
	Queues         []string
	Concurrency    int
	// APIKey is sent as `authorization: Bearer <key>` gRPC metadata on every call to the
	// server, matching the server's own WORKFLOW_API_KEY (see ServerConfig.APIKey).
	APIKey string
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
		APIKey:         getEnv("WORKFLOW_API_KEY", DefaultDevAPIKey),
	}
}
