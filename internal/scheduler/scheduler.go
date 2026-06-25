package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aryanraj/workflow-orchestrator/internal/engine"
	"github.com/aryanraj/workflow-orchestrator/internal/lock"
	"github.com/aryanraj/workflow-orchestrator/internal/lock/redislock"
)

const (
	membersKey  = "orchestrator:members"
	shardMapKey = "orchestrator:shardmap"
	leaderKey   = "orchestrator:leader"

	NumShards = engine.NumShards
)

// Scheduler runs the three cooperating loops described in DESIGN.md section 5: membership
// heartbeating, leader election (whoever holds the lock computes and publishes the shard
// map), and a local shard-map refresh every node uses to compute OwnedShards(). It degrades
// gracefully to "own everything" (nil filter) whenever it has no better information yet —
// this is what makes a single-node deployment (e.g. docker-compose) work identically to a
// sharded cluster without any special-casing.
type Scheduler struct {
	rdb      *redis.Client
	nodeID   string
	log      *slog.Logger
	leaderLk lock.Locker

	heartbeatEvery time.Duration
	rebalanceEvery time.Duration
	staleAfter     time.Duration

	isLeader atomic.Bool
	owned    atomic.Pointer[[]int]

	mu sync.Mutex
}

func New(rdb *redis.Client, nodeID string, log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	s := &Scheduler{
		rdb:            rdb,
		nodeID:         nodeID,
		log:            log,
		leaderLk:       redislock.New(rdb, leaderKey, nodeID, 15*time.Second),
		heartbeatEvery: 3 * time.Second,
		rebalanceEvery: 5 * time.Second,
		staleAfter:     12 * time.Second,
	}
	return s
}

func (s *Scheduler) NodeID() string { return s.nodeID }
func (s *Scheduler) IsLeader() bool { return s.isLeader.Load() }

// OwnedShards returns the shard IDs currently assigned to this node, or nil to mean "no
// filter" (own everything) — the safe default before the first shard map is available.
func (s *Scheduler) OwnedShards() []int {
	p := s.owned.Load()
	if p == nil {
		return nil
	}
	return *p
}

// Run blocks, driving all three loops until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); s.heartbeatLoop(ctx) }()
	go func() { defer wg.Done(); s.leaderLoop(ctx) }()
	go func() { defer wg.Done(); s.shardMapRefreshLoop(ctx) }()
	wg.Wait()
}

func (s *Scheduler) heartbeatLoop(ctx context.Context) {
	s.heartbeat(ctx)
	ticker := time.NewTicker(s.heartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.heartbeat(ctx)
		}
	}
}

func (s *Scheduler) heartbeat(ctx context.Context) {
	now := float64(time.Now().UnixMilli())
	if err := s.rdb.ZAdd(ctx, membersKey, redis.Z{Score: now, Member: s.nodeID}).Err(); err != nil {
		s.log.Warn("scheduler: heartbeat failed", "err", err)
	}
}

func (s *Scheduler) liveMembers(ctx context.Context) ([]string, error) {
	cutoff := time.Now().Add(-s.staleAfter).UnixMilli()
	cutoffStr := strconv.FormatInt(cutoff, 10)
	// Opportunistically prune long-dead members so the ring doesn't accumulate cruft.
	_ = s.rdb.ZRemRangeByScore(ctx, membersKey, "-inf", "("+cutoffStr).Err()
	return s.rdb.ZRangeByScore(ctx, membersKey, &redis.ZRangeBy{Min: cutoffStr, Max: "+inf"}).Result()
}

func (s *Scheduler) leaderLoop(ctx context.Context) {
	ticker := time.NewTicker(s.heartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if s.isLeader.Load() {
				_ = s.leaderLk.Release(context.Background())
			}
			return
		case <-ticker.C:
			ok, err := s.leaderLk.TryAcquire(ctx)
			if err != nil {
				s.log.Warn("scheduler: leader election error", "err", err)
				continue
			}
			wasLeader := s.isLeader.Swap(ok)
			if ok && !wasLeader {
				s.log.Info("scheduler: became leader", "node_id", s.nodeID)
			} else if !ok && wasLeader {
				s.log.Info("scheduler: lost leadership", "node_id", s.nodeID)
			}
			if ok {
				s.publishShardMap(ctx)
			}
		}
	}
}

func (s *Scheduler) publishShardMap(ctx context.Context) {
	members, err := s.liveMembers(ctx)
	if err != nil || len(members) == 0 {
		return
	}
	assignment := AssignShards(members, NumShards)
	b, err := json.Marshal(assignment)
	if err != nil {
		return
	}
	if err := s.rdb.Set(ctx, shardMapKey, b, 0).Err(); err != nil {
		s.log.Warn("scheduler: publish shard map failed", "err", err)
	}
}

func (s *Scheduler) shardMapRefreshLoop(ctx context.Context) {
	ticker := time.NewTicker(s.rebalanceEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshOwnedShards(ctx)
		}
	}
}

func (s *Scheduler) refreshOwnedShards(ctx context.Context) {
	raw, err := s.rdb.Get(ctx, shardMapKey).Bytes()
	if err != nil {
		return // no map yet; keep owning everything (nil)
	}
	var assignment []string
	if err := json.Unmarshal(raw, &assignment); err != nil {
		return
	}
	var mine []int
	for shard, owner := range assignment {
		if owner == s.nodeID {
			mine = append(mine, shard)
		}
	}
	if mine == nil {
		mine = []int{} // present in the map but assigned nothing: own nothing, not everything
	}
	s.owned.Store(&mine)
}
