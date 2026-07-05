// Package redisstream implements queue.Queue on Redis Streams consumer groups.
package redisstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aryanraj/workflow-orchestrator/internal/queue"
)

type Queue struct {
	rdb *redis.Client
	log *slog.Logger
}

func New(rdb *redis.Client, log *slog.Logger) *Queue {
	if log == nil {
		log = slog.Default()
	}
	return &Queue{rdb: rdb, log: log}
}

func (q *Queue) EnsureGroup(ctx context.Context, stream, group string) error {
	err := q.rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("redisstream: ensure group: %w", err)
	}
	return nil
}

func (q *Queue) Publish(ctx context.Context, stream string, payload []byte) (string, error) {
	id, err := q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"payload": payload},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("redisstream: publish: %w", err)
	}
	return id, nil
}

func (q *Queue) Ack(ctx context.Context, stream, group, id string) error {
	if err := q.rdb.XAck(ctx, stream, group, id).Err(); err != nil {
		return fmt.Errorf("redisstream: ack: %w", err)
	}
	return nil
}

func (q *Queue) Close() error { return q.rdb.Close() }

// Consume launches two background loops: one reading new messages (">") for this consumer,
// and one periodically running XAUTOCLAIM to steal messages that have been pending (unacked)
// on *any* consumer in the group for longer than idleTimeout — this is what gives us
// automatic recovery when a worker process dies mid-task (Redis Streams redelivery is the
// "at-least-once" half of the exactly-once story, the CAS claim in Postgres is the other
// half).
func (q *Queue) Consume(ctx context.Context, stream, group, consumer string, idleTimeout time.Duration) (<-chan queue.Message, error) {
	if err := q.EnsureGroup(ctx, stream, group); err != nil {
		return nil, err
	}
	out := make(chan queue.Message, 64)

	go q.readLoop(ctx, stream, group, consumer, out)
	go q.reclaimLoop(ctx, stream, group, consumer, idleTimeout, out)

	go func() {
		<-ctx.Done()
		close(out)
	}()

	return out, nil
}

func (q *Queue) readLoop(ctx context.Context, stream, group, consumer string, out chan<- queue.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    16,
			Block:    2 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			q.log.Warn("redisstream: read error", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for _, s := range res {
			for _, m := range s.Messages {
				msg := toMessage(stream, m)
				select {
				case out <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (q *Queue) reclaimLoop(ctx context.Context, stream, group, consumer string, idleTimeout time.Duration, out chan<- queue.Message) {
	ticker := time.NewTicker(idleTimeout / 2)
	defer ticker.Stop()
	var cursor string = "0-0"
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		msgs, next, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: consumer,
			MinIdle:  idleTimeout,
			Start:    cursor,
			Count:    32,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			q.log.Warn("redisstream: reclaim error", "err", err)
			continue
		}
		cursor = next
		for _, m := range msgs {
			msg := toMessage(stream, m)
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

func toMessage(stream string, m redis.XMessage) queue.Message {
	var payload []byte
	if v, ok := m.Values["payload"]; ok {
		switch t := v.(type) {
		case string:
			payload = []byte(t)
		case []byte:
			payload = t
		}
	}
	return queue.Message{ID: m.ID, Stream: stream, Payload: payload, Delivery: 1}
}
