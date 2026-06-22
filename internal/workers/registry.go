// Package workers is a tiny Redis-backed registry of live worker processes, used purely for
// dashboard visibility (which workers are up, what queues they serve). It is not on the
// critical path for correctness — a worker that never registers can still claim and execute
// tasks fine; it just won't show up in the fleet view.
package workers

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	membersKey = "orchestrator:workers"
	infoPrefix = "orchestrator:worker:"
	ttl        = 15 * time.Second
)

type Info struct {
	WorkerID string    `json:"worker_id"`
	Queues   []string  `json:"queues"`
	Capacity int       `json:"capacity"`
	LastSeen time.Time `json:"last_seen"`
}

type Registry struct {
	rdb *redis.Client
}

func NewRegistry(rdb *redis.Client) *Registry {
	return &Registry{rdb: rdb}
}

func (r *Registry) Register(ctx context.Context, workerID string, queues []string, capacity int) error {
	if r.rdb == nil {
		return nil
	}
	info := Info{WorkerID: workerID, Queues: queues, Capacity: capacity, LastSeen: time.Now()}
	b, err := json.Marshal(info)
	if err != nil {
		return err
	}
	pipe := r.rdb.TxPipeline()
	pipe.Set(ctx, infoPrefix+workerID, b, ttl)
	pipe.ZAdd(ctx, membersKey, redis.Z{Score: float64(time.Now().UnixMilli()), Member: workerID})
	_, err = pipe.Exec(ctx)
	return err
}

func (r *Registry) Heartbeat(ctx context.Context, workerID string) error {
	if r.rdb == nil {
		return nil
	}
	pipe := r.rdb.TxPipeline()
	pipe.Expire(ctx, infoPrefix+workerID, ttl)
	pipe.ZAdd(ctx, membersKey, redis.Z{Score: float64(time.Now().UnixMilli()), Member: workerID})
	_, err := pipe.Exec(ctx)
	return err
}

// List returns every worker that has heartbeated within the TTL window.
func (r *Registry) List(ctx context.Context) ([]Info, error) {
	if r.rdb == nil {
		return nil, nil
	}
	cutoff := time.Now().Add(-ttl).UnixMilli()
	_ = r.rdb.ZRemRangeByScore(ctx, membersKey, "-inf", "("+strconv.FormatInt(cutoff, 10)).Err()
	ids, err := r.rdb.ZRange(ctx, membersKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Info, 0, len(ids))
	for _, id := range ids {
		raw, err := r.rdb.Get(ctx, infoPrefix+id).Bytes()
		if err != nil {
			continue // expired between ZRANGE and GET; skip
		}
		var info Info
		if err := json.Unmarshal(raw, &info); err != nil {
			continue
		}
		out = append(out, info)
	}
	return out, nil
}
