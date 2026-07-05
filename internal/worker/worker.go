// Package worker implements the worker-side runtime: it consumes task messages from Redis
// Streams, performs the exactly-once ClaimTask CAS over gRPC before running anything,
// dispatches to a registered Handler by task type, heartbeats the
// lease while the handler runs, and reports the result back over gRPC.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	workerpb "github.com/aryanraj/workflow-orchestrator/gen/workerpb"
	"github.com/aryanraj/workflow-orchestrator/internal/queue"
	"github.com/aryanraj/workflow-orchestrator/internal/store"
)

// Handler executes one task type. It receives the task's input JSON and the idempotency key
// (which handlers should use as the dedup key for any external side effect they perform)
// and returns either an output payload or an error.
type Handler func(ctx context.Context, input json.RawMessage, idempotencyKey string) (json.RawMessage, error)

type Worker struct {
	ID       string
	Queue    queue.Queue
	Client   workerpb.WorkerServiceClient
	Log      *slog.Logger
	handlers map[string]Handler
	mu       sync.RWMutex

	Concurrency  int
	IdleTimeout  time.Duration
	HeartbeatFor time.Duration
}

func New(id string, q queue.Queue, client workerpb.WorkerServiceClient, log *slog.Logger) *Worker {
	if log == nil {
		log = slog.Default()
	}
	return &Worker{
		ID: id, Queue: q, Client: client, Log: log,
		handlers: map[string]Handler{}, Concurrency: 4,
		IdleTimeout: 15 * time.Second, HeartbeatFor: 10 * time.Second,
	}
}

func (w *Worker) Register(taskType string, h Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[taskType] = h
}

func (w *Worker) handlerFor(taskType string) (Handler, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	h, ok := w.handlers[taskType]
	return h, ok
}

// Run subscribes to a Redis Stream per registered task type ("wf:tasks:<type>") and
// processes messages with `Concurrency` worker goroutines per stream until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	w.mu.RLock()
	taskTypes := make([]string, 0, len(w.handlers))
	for t := range w.handlers {
		taskTypes = append(taskTypes, t)
	}
	w.mu.RUnlock()

	if _, err := w.Client.RegisterWorker(ctx, &workerpb.RegisterWorkerRequest{
		WorkerId: w.ID, Queues: streamNames(taskTypes), Capacity: int32(w.Concurrency),
	}); err != nil {
		w.Log.Warn("worker: register failed (continuing anyway)", "err", err)
	}
	go w.heartbeatLoop(ctx)

	var wg sync.WaitGroup
	for _, tt := range taskTypes {
		stream := "wf:tasks:" + tt
		msgs, err := w.Queue.Consume(ctx, stream, "workers", w.ID, w.IdleTimeout)
		if err != nil {
			return fmt.Errorf("worker: consume %s: %w", stream, err)
		}
		for i := 0; i < w.Concurrency; i++ {
			wg.Add(1)
			go func(stream string, msgs <-chan queue.Message) {
				defer wg.Done()
				w.processLoop(ctx, stream, msgs)
			}(stream, msgs)
		}
	}
	wg.Wait()
	return nil
}

func streamNames(taskTypes []string) []string {
	out := make([]string, len(taskTypes))
	for i, t := range taskTypes {
		out[i] = "wf:tasks:" + t
	}
	return out
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = w.Client.Heartbeat(ctx, &workerpb.HeartbeatRequest{WorkerId: w.ID})
		}
	}
}

func (w *Worker) processLoop(ctx context.Context, stream string, msgs <-chan queue.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			w.process(ctx, stream, msg)
		}
	}
}

func (w *Worker) process(ctx context.Context, stream string, msg queue.Message) {
	var payload store.TaskPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		w.Log.Error("worker: bad task payload, dropping", "err", err)
		_ = w.Queue.Ack(ctx, stream, "workers", msg.ID)
		return
	}
	log := w.Log.With("task_attempt_id", payload.TaskAttemptID, "step", payload.StepName, "attempt", payload.AttemptNumber)

	claim, err := w.Client.ClaimTask(ctx, &workerpb.ClaimTaskRequest{
		TaskAttemptId:  payload.TaskAttemptID.String(),
		IdempotencyKey: payload.IdempotencyKey.String(),
		WorkerId:       w.ID,
	})
	if err != nil {
		log.Error("worker: claim rpc failed, will be redelivered", "err", err)
		return // do not ack; Redis will redeliver after the idle timeout
	}
	// Always ack: whatever ClaimTask said, we've durably resolved this delivery. If it was
	// ALREADY_CLAIMED/CANCELLED/NOT_FOUND, someone else (or nobody) owns the outcome now and
	// redelivering this message again would be pure waste (the CAS would just fail again).
	defer func() { _ = w.Queue.Ack(ctx, stream, "workers", msg.ID) }()

	if claim.Status != workerpb.ClaimStatus_CLAIMED {
		log.Debug("worker: claim not granted", "status", claim.Status.String())
		return
	}

	handler, ok := w.handlerFor(claim.TaskType)
	if !ok {
		errMsg := fmt.Sprintf("no handler registered for task type %q", claim.TaskType)
		log.Error("worker: " + errMsg)
		w.report(ctx, payload, false, nil, errMsg)
		return
	}

	taskCtx := ctx
	var cancel context.CancelFunc
	if claim.TimeoutSeconds > 0 {
		taskCtx, cancel = context.WithTimeout(ctx, time.Duration(claim.TimeoutSeconds)*time.Second)
		defer cancel()
	}
	stopHeartbeat := w.startAttemptHeartbeat(ctx, payload.TaskAttemptID.String())
	defer stopHeartbeat()

	output, err := handler(taskCtx, payload.Input, payload.IdempotencyKey.String())
	if err != nil {
		log.Warn("worker: handler failed", "err", err)
		w.report(ctx, payload, false, nil, err.Error())
		return
	}
	log.Info("worker: handler succeeded")
	w.report(ctx, payload, true, output, "")
}

func (w *Worker) startAttemptHeartbeat(ctx context.Context, attemptID string) func() {
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(w.HeartbeatFor)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = w.Client.Heartbeat(ctx, &workerpb.HeartbeatRequest{WorkerId: w.ID, TaskAttemptId: attemptID})
			}
		}
	}()
	return func() { close(stop) }
}

func (w *Worker) report(ctx context.Context, payload store.TaskPayload, success bool, output json.RawMessage, errMsg string) {
	outJSON := ""
	if output != nil {
		outJSON = string(output)
	}
	_, err := w.Client.ReportResult(ctx, &workerpb.ReportResultRequest{
		TaskAttemptId:  payload.TaskAttemptID.String(),
		WorkerId:       w.ID,
		IdempotencyKey: payload.IdempotencyKey.String(),
		Success:        success,
		OutputJson:     outJSON,
		ErrorMessage:   errMsg,
	})
	if err != nil {
		w.Log.Error("worker: report result rpc failed", "err", err)
	}
}
