// Package grpcapi implements the worker-facing gRPC surface (proto/worker.proto): the
// exactly-once claim/report boundary, plus worker
// registration/heartbeat for dashboard visibility.
package grpcapi

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	workerpb "github.com/aryanraj/workflow-orchestrator/gen/workerpb"
	"github.com/aryanraj/workflow-orchestrator/internal/engine"
	"github.com/aryanraj/workflow-orchestrator/internal/store"
	"github.com/aryanraj/workflow-orchestrator/internal/workers"
)

type Server struct {
	workerpb.UnimplementedWorkerServiceServer

	Store         store.Store
	Engine        *engine.Engine
	Registry      *workers.Registry
	Log           *slog.Logger
	LeaseDuration time.Duration
}

func New(s store.Store, e *engine.Engine, rdb *redis.Client, log *slog.Logger, leaseDuration time.Duration) *Server {
	if log == nil {
		log = slog.Default()
	}
	if leaseDuration <= 0 {
		leaseDuration = engine.DefaultLeaseDuration
	}
	return &Server{Store: s, Engine: e, Registry: workers.NewRegistry(rdb), Log: log, LeaseDuration: leaseDuration}
}

func (s *Server) RegisterWorker(ctx context.Context, req *workerpb.RegisterWorkerRequest) (*workerpb.RegisterWorkerResponse, error) {
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	if err := s.Registry.Register(ctx, req.WorkerId, req.Queues, int(req.Capacity)); err != nil {
		return nil, status.Errorf(codes.Internal, "register: %v", err)
	}
	return &workerpb.RegisterWorkerResponse{Ok: true}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *workerpb.HeartbeatRequest) (*workerpb.HeartbeatResponse, error) {
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	if err := s.Registry.Heartbeat(ctx, req.WorkerId); err != nil {
		s.Log.Warn("grpcapi: registry heartbeat failed", "err", err)
	}
	resp := &workerpb.HeartbeatResponse{Ok: true}
	if req.TaskAttemptId != "" {
		attemptID, err := uuid.Parse(req.TaskAttemptId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid task_attempt_id")
		}
		exp, ok, err := s.Store.HeartbeatTaskAttempt(ctx, attemptID, req.WorkerId, s.LeaseDuration)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "heartbeat: %v", err)
		}
		resp.Ok = ok
		if ok && exp != nil {
			resp.LeaseExpiresAt = timestamppb.New(*exp)
		}
	}
	return resp, nil
}

func (s *Server) ClaimTask(ctx context.Context, req *workerpb.ClaimTaskRequest) (*workerpb.ClaimTaskResponse, error) {
	attemptID, err := uuid.Parse(req.TaskAttemptId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid task_attempt_id")
	}
	idemKey, err := uuid.Parse(req.IdempotencyKey)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid idempotency_key")
	}
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	attempt, result, err := s.Store.ClaimTaskAttempt(ctx, attemptID, idemKey, req.WorkerId, s.LeaseDuration)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "claim: %v", err)
	}

	switch result {
	case store.ClaimNotFound:
		return &workerpb.ClaimTaskResponse{Status: workerpb.ClaimStatus_NOT_FOUND}, nil
	case store.ClaimAlreadyClaimed:
		return &workerpb.ClaimTaskResponse{Status: workerpb.ClaimStatus_ALREADY_CLAIMED}, nil
	}

	step, err := s.Store.GetStep(ctx, attempt.StepID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load step: %v", err)
	}
	run, err := s.Store.GetWorkflowRun(ctx, attempt.WorkflowRunID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load run: %v", err)
	}
	if run.Status == store.RunCancelled {
		return &workerpb.ClaimTaskResponse{Status: workerpb.ClaimStatus_CANCELLED}, nil
	}

	// Best-effort display transition; not load-bearing for correctness (the
	// task_attempts row, not the steps row, is the source of truth for exactly-once dispatch).
	_, _ = s.Store.UpdateStepStatus(ctx, step.ID, []store.StepStatus{store.StepQueued}, store.StepRunning, nil, nil, false)

	resp := &workerpb.ClaimTaskResponse{
		Status:         workerpb.ClaimStatus_CLAIMED,
		StepName:       step.StepName,
		TaskType:       step.TaskType,
		InputJson:      string(step.Input),
		AttemptNumber:  int32(attempt.AttemptNumber),
		TimeoutSeconds: int32(step.TimeoutSeconds),
	}
	if attempt.LeaseExpiresAt != nil {
		resp.LeaseExpiresAt = timestamppb.New(*attempt.LeaseExpiresAt)
	}
	return resp, nil
}

func (s *Server) ReportResult(ctx context.Context, req *workerpb.ReportResultRequest) (*workerpb.ReportResultResponse, error) {
	attemptID, err := uuid.Parse(req.TaskAttemptId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid task_attempt_id")
	}
	var errMsg *string
	if req.ErrorMessage != "" {
		m := req.ErrorMessage
		errMsg = &m
	}
	var output []byte
	if req.OutputJson != "" {
		output = []byte(req.OutputJson)
	}
	accepted, err := s.Engine.HandleTaskResult(ctx, attemptID, req.WorkerId, req.Success, output, errMsg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "report result: %v", err)
	}
	return &workerpb.ReportResultResponse{Ok: true, Accepted: accepted}, nil
}
