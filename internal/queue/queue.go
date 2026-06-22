// Package queue defines the task-delivery interface used by the outbox relay (producer) and
// workers (consumers). internal/queue/redisstream implements it on Redis Streams consumer
// groups; internal/queue/memory implements it in-process for tests. See DESIGN.md section 7.
package queue

import (
	"context"
	"time"
)

// Message is one delivered queue entry. ID is opaque to callers except that it must be
// passed back to Ack. Redelivered messages (from a crashed consumer's pending-entries list)
// arrive with the same ID as their original delivery — this is what lets a worker recognize
// "this might be a duplicate" even before it talks to the server's CAS claim endpoint.
type Message struct {
	ID       string
	Stream   string
	Payload  []byte
	Delivery int64 // number of times this entry has been delivered (1 = first delivery)
}

// Queue is a minimal at-least-once, consumer-group style message queue abstraction.
type Queue interface {
	// EnsureGroup creates the stream and consumer group if they don't already exist. Safe to
	// call repeatedly (idempotent).
	EnsureGroup(ctx context.Context, stream, group string) error

	// Publish appends a message to the stream, returning its ID.
	Publish(ctx context.Context, stream string, payload []byte) (string, error)

	// Consume returns a channel of messages delivered to `consumer` within `group` on
	// `stream`, including messages reclaimed from other consumers that have been idle for
	// longer than idleTimeout (crash recovery). The channel is closed when ctx is cancelled.
	Consume(ctx context.Context, stream, group, consumer string, idleTimeout time.Duration) (<-chan Message, error)

	// Ack acknowledges successful processing of a message, removing it from the consumer
	// group's pending-entries list so it will not be redelivered.
	Ack(ctx context.Context, stream, group, id string) error

	Close() error
}
