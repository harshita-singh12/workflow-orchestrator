// Package httpapi is the JSON REST API the React dashboard (and any other external caller)
// talks to: registering workflow definitions, starting runs, inspecting run/step/history
// state, sending signals, and cancelling runs. This is deliberately separate from the
// worker-facing gRPC surface (internal/grpcapi) — different audiences, different transport.
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

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

	router chi.Router
}

func New(e *engine.Engine, s store.Store, reg *workers.Registry, sched *scheduler.Scheduler, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	srv := &Server{Engine: e, Store: s, Registry: reg, Scheduler: sched, Log: log}
	srv.router = srv.routes()
	return srv
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.router.ServeHTTP(w, r) }

func (s *Server) routes() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware)
	r.Use(middleware.Logger)

	r.Get("/healthz", s.handleHealth)

	r.Route("/api", func(r chi.Router) {
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

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
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
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
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
