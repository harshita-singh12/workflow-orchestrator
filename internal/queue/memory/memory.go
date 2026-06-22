// Package memory is an in-process queue.Queue implementation used by unit tests. It
// broadcasts every published message to all active consumers of that stream (each Consume
// call registers its own channel) and treats Ack as a no-op, which is sufficient for
// exercising engine-level logic without needing real consumer-group redelivery semantics —
// those are covered separately by the Redis Streams implementation and by the fact that the
// exactly-once guarantee is enforced by the store's CAS claim, not by the queue.
package memory

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/aryanraj/workflow-orchestrator/internal/queue"
)

type Queue struct {
	mu      sync.Mutex
	streams map[string][]chan queue.Message
	nextID  int64
	closed  bool
}

func New() *Queue {
	return &Queue{streams: map[string][]chan queue.Message{}}
}

func (q *Queue) EnsureGroup(ctx context.Context, stream, group string) error { return nil }

func (q *Queue) Publish(ctx context.Context, stream string, payload []byte) (string, error) {
	q.mu.Lock()
	q.nextID++
	id := strconv.FormatInt(q.nextID, 10)
	subs := append([]chan queue.Message{}, q.streams[stream]...)
	q.mu.Unlock()

	msg := queue.Message{ID: id, Stream: stream, Payload: payload, Delivery: 1}
	for _, ch := range subs {
		ch := ch
		go func() {
			select {
			case ch <- msg:
			case <-time.After(5 * time.Second):
			}
		}()
	}
	return id, nil
}

func (q *Queue) Consume(ctx context.Context, stream, group, consumer string, idleTimeout time.Duration) (<-chan queue.Message, error) {
	ch := make(chan queue.Message, 64)
	q.mu.Lock()
	q.streams[stream] = append(q.streams[stream], ch)
	q.mu.Unlock()

	go func() {
		<-ctx.Done()
		q.mu.Lock()
		defer q.mu.Unlock()
		subs := q.streams[stream]
		for i, c := range subs {
			if c == ch {
				q.streams[stream] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		close(ch)
	}()

	return ch, nil
}

func (q *Queue) Ack(ctx context.Context, stream, group, id string) error { return nil }

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	return nil
}
