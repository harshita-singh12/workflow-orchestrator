// Package redislock implements lock.Locker with Redis SET NX PX for acquisition and Lua
// scripts for safe (CAS'd on the token value) renewal and release, so a locker instance can
// never accidentally renew/release a lock that a different holder has since acquired after
// this one's TTL lapsed.
package redislock

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end
`)

var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

type Lock struct {
	rdb      *redis.Client
	key      string
	holderID string
	ttl      time.Duration
}

func New(rdb *redis.Client, key, holderID string, ttl time.Duration) *Lock {
	return &Lock{rdb: rdb, key: key, holderID: holderID, ttl: ttl}
}

func (l *Lock) HolderID() string { return l.holderID }

func (l *Lock) TryAcquire(ctx context.Context) (bool, error) {
	ok, err := l.rdb.SetNX(ctx, l.key, l.holderID, l.ttl).Result()
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	// Already held; if it's held by us (e.g. process restarted the loop without losing the
	// key), treat it as a re-acquire + renew rather than a failure.
	return l.Renew(ctx)
}

func (l *Lock) Renew(ctx context.Context) (bool, error) {
	res, err := renewScript.Run(ctx, l.rdb, []string{l.key}, l.holderID, l.ttl.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (l *Lock) Release(ctx context.Context) error {
	_, err := releaseScript.Run(ctx, l.rdb, []string{l.key}, l.holderID).Result()
	return err
}
