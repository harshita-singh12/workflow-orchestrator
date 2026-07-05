// Package scheduler implements Phase 2 sharded scheduling: a consistent-hash ring over live
// server nodes, Redis-backed leader election that elects a single writer of the resulting
// shard->node assignment, and a read-side helper every node uses to compute which shards
// (and therefore which workflow runs) it currently owns. See README's "Phase 2 features and
// their tradeoffs" section for the full rationale, in particular why this is a throughput optimization and not a correctness
// mechanism — every state transition it gates is independently protected by a CAS in
// internal/store, so a stale or momentarily-inconsistent shard map can never cause a
// duplicate task execution, only redundant/wasted reconcile work.
package scheduler

import (
	"hash/crc32"
	"sort"
	"strconv"
)

const defaultVirtualReplicas = 32

// Ring is a consistent-hash ring over a set of node IDs with virtual replicas for smoother
// distribution as the node count changes.
type Ring struct {
	replicas     int
	sortedHashes []uint32
	hashToNode   map[uint32]string
}

func NewRing(nodeIDs []string, replicas int) *Ring {
	if replicas <= 0 {
		replicas = defaultVirtualReplicas
	}
	r := &Ring{replicas: replicas, hashToNode: map[uint32]string{}}
	for _, id := range nodeIDs {
		for i := 0; i < replicas; i++ {
			h := hashKey(id, i)
			r.hashToNode[h] = id
			r.sortedHashes = append(r.sortedHashes, h)
		}
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool { return r.sortedHashes[i] < r.sortedHashes[j] })
	return r
}

// Get returns the node owning the given key, or "" if the ring is empty.
func (r *Ring) Get(key uint32) string {
	if len(r.sortedHashes) == 0 {
		return ""
	}
	idx := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= key })
	if idx == len(r.sortedHashes) {
		idx = 0
	}
	return r.hashToNode[r.sortedHashes[idx]]
}

func hashKey(nodeID string, replica int) uint32 {
	return crc32.ChecksumIEEE([]byte(nodeID + "#" + strconv.Itoa(replica)))
}

// AssignShards computes, for a fixed shard count, which node owns each shard by hashing the
// shard index itself as a ring key. Returns a slice where index i holds the owner of shard i.
func AssignShards(nodeIDs []string, numShards int) []string {
	ring := NewRing(nodeIDs, defaultVirtualReplicas)
	out := make([]string, numShards)
	for i := 0; i < numShards; i++ {
		out[i] = ring.Get(crc32.ChecksumIEEE([]byte("shard#" + strconv.Itoa(i))))
	}
	return out
}
