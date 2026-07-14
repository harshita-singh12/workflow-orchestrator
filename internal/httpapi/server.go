// Package httpapi is the JSON REST API the React dashboard (and any other external caller)
// talks to: registering workflow definitions, starting runs, inspecting run/step/history
// state, sending signals, and cancelling runs. This is deliberately separate from the
// worker-facing gRPC surface (internal/grpcapi) — different audiences, different transport.
package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"github.com/aryanraj/workflow-orchestrator/internal/engine"
	"github.com/aryanraj/workflow-orchestrator/internal/scheduler"
	"github.com/aryanraj/workflow-orchestrator/internal/store"
	"github.com/aryanraj/workflow-orchestrator/internal/workers"
	"github.com/aryanraj/workflow-orchestrator/internal/workflowdsl"
)

type Server struct {
	Engine    *engine.Engine
	Store     store.Store
	Registry  *workers.Registry
	Scheduler *scheduler.Scheduler // optional (nil in single-node/no-scheduler mode)
	Log       *slog.Logger

	// APIKey is the shared secret required as `Authorization: Bearer <key>` on every route
	// under /api. Every request is rejected (fail closed) when this is empty — an empty key
	// must never mean "auth disabled".
	APIKey string
	// AllowedOrigins is the CORS allowlist; Access-Control-Allow-Origin reflects the request's
	// Origin header only when it exactly matches an entry here. Never a wildcard.
	AllowedOrigins []string

	router chi.Router
}

func New(e *engine.Engine, s store.Store, reg *workers.Registry, sched *scheduler.Scheduler, log *slog.Logger, apiKey string, allowedOrigins []string) *Server {
	if log == nil {
		log = slog.Default()
	}
	srv := &Server{Engine: e, Store: s, Registry: reg, Scheduler: sched, Log: log, APIKey: apiKey, AllowedOrigins: allowedOrigins}
	srv.router = srv.routes()
	return srv
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.router.ServeHTTP(w, r) }

// maxRequestBodyBytes caps how much of a request body we'll read, generous enough for even a
// large hand-authored workflow definition while ruling out an unbounded-body memory DoS
// against handlers that read the whole body into memory (json.Decoder, io.ReadAll).
const maxRequestBodyBytes = 2 << 20 // 2 MiB

// maxListRunsLimit caps GET /api/runs?limit=... so a caller can't force an unbounded table
// scan/response by passing an arbitrarily large limit.
const maxListRunsLimit = 500

func (s *Server) routes() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware(s.AllowedOrigins))
	r.Use(middleware.Logger)
	r.Use(limitBodyMiddleware)

	// /healthz stays unauthenticated on purpose — container/k8s liveness and readiness
	// probes hit this without credentials, and it reveals nothing but process liveness.
	r.Get("/healthz", s.handleHealth)

	r.Route("/api", func(r chi.Router) {
		r.Use(s.authMiddleware)

		r.Post("/definitions", s.handleCreateDefinition)
		r.Get("/definitions", s.handleListDefinitions)

		r.Post("/runs", s.handleCreateRun)
		r.Get("/runs", s.handleListRuns)
		r.Get("/runs/{id}", s.handleGetRun)
		r.Get("/runs/{id}/steps", s.handleGetSteps)
		r.Get("/runs/{id}/history", s.handleGetHistory)
		r.Post("/runs/{id}/signal", s.handleSignal)
		r.Post("/runs/{id}/cancel", s.handleCancel)

		r.Get("/workers", s.handleListWorkers)
		r.Get("/cluster", s.handleClusterStatus)
	})
	return r
}

func limitBodyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		next.ServeHTTP(w, r)
	})
}

// corsMiddleware only ever reflects Access-Control-Allow-Origin back for an Origin that
// exactly matches an entry in allowedOrigins — it never emits "*". A request with no Origin
// header (curl, server-to-server, same-origin) or a non-matching one just gets no CORS
// headers, which is exactly what causes a browser making a cross-origin fetch to reject the
// response; non-browser callers are unaffected either way, since CORS is a browser-enforced
// policy, not a server-side access control.
func corsMiddleware(allowedOrigins []string) func(http.Handler) http.Handler {
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[o] = struct{}{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if origin := r.Header.Get("Origin"); origin != "" {
				if _, ok := allowed[origin]; ok {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Vary", "Origin")
				}
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// authMiddleware requires `Authorization: Bearer <APIKey>` on every route it wraps. An empty
// s.APIKey always fails closed — every request is rejected, never treated as "auth off" — so
// a misconfiguration can't silently reopen the API.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validAPIKey(s.APIKey, r.Header.Get("Authorization")) {
			writeErr(w, http.StatusUnauthorized, "missing or invalid API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// validAPIKey reports whether the `Authorization: Bearer <token>` header carries a token
// equal to expected, using a constant-time comparison so response timing can't be used to
// brute-force the key one byte at a time. Always false when expected is empty.
func validAPIKey(expected, header string) bool {
	if expected == "" {
		return false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	token := strings.TrimPrefix(header, prefix)
	return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- definitions ---

func (s *Server) handleCreateDefinition(w http.ResponseWriter, r *http.Request) {
	body, err := readAll(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	def, err := workflowdsl.Parse(body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	saved, err := s.Engine.RegisterDefinition(r.Context(), def)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeErr(w, http.StatusConflict, fmt.Sprintf("workflow %q version %d already registered", def.Name, def.Version))
			return
		}
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, saved)
}

func (s *Server) handleListDefinitions(w http.ResponseWriter, r *http.Request) {
	defs, err := s.Store.ListWorkflowDefinitions(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, defs)
}

// --- runs ---

type createRunRequest struct {
	Name    string          `json:"name"`
	Version int             `json:"version"`
	Input   json.RawMessage `json:"input"`
}

func (s *Server) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	var req createRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body: "+err.Error())
		return
	}
	if req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name is required")
		return
	}
	run, err := s.Engine.CreateRun(r.Context(), req.Name, req.Version, req.Input)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, run)
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	f := store.RunFilter{Limit: 50}
	q := r.URL.Query()
	if v := q.Get("status"); v != "" {
		st := store.RunStatus(v)
		f.Status = &st
	}
	if v := q.Get("name"); v != "" {
		f.Name = &v
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.Limit = n
		}
	}
	if f.Limit > maxListRunsLimit {
		f.Limit = maxListRunsLimit
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			f.Offset = n
		}
	}
	runs, err := s.Store.ListWorkflowRuns(r.Context(), f)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if runs == nil {
		runs = []store.WorkflowRun{}
	}
	writeJSON(w, http.StatusOK, runs)
}

func parseRunID(r *http.Request) (uuid.UUID, error) {
	return uuid.Parse(chi.URLParam(r, "id"))
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	id, err := parseRunID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid run id")
		return
	}
	run, err := s.Store.GetWorkflowRun(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "run not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleGetSteps(w http.ResponseWriter, r *http.Request) {
	id, err := parseRunID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid run id")
		return
	}
	steps, err := s.Store.GetSteps(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	type stepWithAttempts struct {
		store.Step
		Attempts []store.TaskAttempt `json:"attempts"`
	}
	out := make([]stepWithAttempts, 0, len(steps))
	for _, st := range steps {
		attempts, err := s.Store.ListTaskAttemptsForStep(r.Context(), st.ID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if attempts == nil {
			attempts = []store.TaskAttempt{}
		}
		out = append(out, stepWithAttempts{Step: st, Attempts: attempts})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	id, err := parseRunID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid run id")
		return
	}
	hist, err := s.Store.ListHistory(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if hist == nil {
		hist = []store.HistoryEvent{}
	}
	writeJSON(w, http.StatusOK, hist)
}

type signalRequest struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
}

func (s *Server) handleSignal(w http.ResponseWriter, r *http.Request) {
	id, err := parseRunID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid run id")
		return
	}
	var req signalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body: "+err.Error())
		return
	}
	if req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name is required")
		return
	}
	if err := s.Engine.ApplySignal(r.Context(), id, req.Name, req.Payload); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id, err := parseRunID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid run id")
		return
	}
	if err := s.Engine.CancelRun(r.Context(), id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	if s.Registry == nil {
		writeJSON(w, http.StatusOK, []workers.Info{})
		return
	}
	list, err := s.Registry.List(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if list == nil {
		list = []workers.Info{}
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if s.Scheduler == nil {
		writeJSON(w, http.StatusOK, map[string]any{"sharding_enabled": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sharding_enabled": true,
		"node_id":          s.Scheduler.NodeID(),
		"is_leader":        s.Scheduler.IsLeader(),
		"owned_shards":     len(s.Scheduler.OwnedShards()),
		"total_shards":     scheduler.NumShards,
	})
}

func readAll(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}
