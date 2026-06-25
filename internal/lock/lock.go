// Package lock defines a minimal distributed-locking interface used for the scheduler's
// leader election (DESIGN.md section 5). internal/lock/redislock implements it with Redis
// SET NX PX plus a Lua CAS for safe release/renew.
package lock

import "context"

// Locker is a single named, TTL'd, renewable mutual-exclusion lock.
type Locker interface {
	// TryAcquire attempts to acquire the lock for `holder`, valid for ttl. Returns true if
	// acquired (either fresh or already held by the same holder, which re-extends the TTL).
	TryAcquire(ctx context.Context) (bool, error)
	// Renew extends the TTL if this holder still owns the lock. Returns false if the lock was
	// lost (e.g. expired and taken by someone else).
	Renew(ctx context.Context) (bool, error)
	// Release gives up the lock if this holder still owns it. Best-effort.
	Release(ctx context.Context) error
	// HolderID identifies this locker's own candidate identity.
	HolderID() string
}
