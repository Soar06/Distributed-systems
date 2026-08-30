// Package shard decides which Raft group owns which account, and coordinates
// transfers that span two groups.
//
// Placement uses consistent hashing (Karger et al., STOC 1997 — see
// learn/READING_LIST.md §11). The alternative, hash(key) % N, remaps nearly every
// key when N changes; for a bank that means relocating nearly every account at
// once. Consistent hashing relocates only ~1/n of them.
package shard

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
)

// ID identifies one shard — one independent Raft group.
type ID string

// Ring maps account IDs to shards using a consistent hash ring.
//
// Placement MUST be a pure function of (key, ring config): every node computes
// ownership independently with no coordination. If two nodes disagreed about who
// owns an account, that would be a correctness bug, not a performance one.
type Ring struct {
	// points are virtual node positions on the ring, sorted by hash.
	points []point

	// vnodes is how many virtual points each shard occupies.
	vnodes int

	shards []ID
}

type point struct {
	hash  uint32
	shard ID
}

// DefaultVNodes is the number of virtual nodes per shard.
//
// Virtual nodes are required, not decorative. A handful of shards placed once on a
// 2^32 ring divides it very unevenly — one shard can easily own several times its
// fair share. Many virtual points per shard smooth the distribution. Dynamo uses
// the same technique for the same reason (§11).
const DefaultVNodes = 150

// NewRing builds a ring over the given shards.
func NewRing(shards []ID, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVNodes
	}
	r := &Ring{vnodes: vnodes, shards: append([]ID(nil), shards...)}

	for _, s := range shards {
		for i := range vnodes {
			r.points = append(r.points, point{
				hash:  hashKey(string(s) + "#" + strconv.Itoa(i)),
				shard: s,
			})
		}
	}
	// Sorted so lookup is a binary search, and so the ring is identical on every
	// node regardless of the order shards were listed.
	sort.Slice(r.points, func(i, j int) bool { return r.points[i].hash < r.points[j].hash })
	return r
}

// hashKey maps a string onto the ring. CRC32 is used rather than a cryptographic
// hash: placement needs speed and uniformity, not collision resistance against an
// adversary.
func hashKey(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

// HashKey exposes a key's position on the ring, for visualization.
//
// Exported so the dashboard draws the REAL placement rather than a decorative
// approximation: a ring view that shows a key somewhere other than where the ring
// actually puts it teaches the wrong thing, which for a view whose whole purpose
// is to make placement visible would be worse than showing nothing.
func HashKey(s string) uint32 { return hashKey(s) }

// Lookup returns the shard owning key: the first shard clockwise from hash(key).
func (r *Ring) Lookup(key string) ID {
	if len(r.points) == 0 {
		return ""
	}
	h := hashKey(key)

	// First point with hash >= h.
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0 // wrapped past the end of the ring
	}
	return r.points[i].shard
}

// Shards returns the shards in the ring.
func (r *Ring) Shards() []ID {
	return append([]ID(nil), r.shards...)
}

// Distribution reports how many of the given keys land on each shard. Used by
// tests to verify virtual nodes actually balance the ring, and by the dashboard's
// ring view.
func (r *Ring) Distribution(keys []string) map[ID]int {
	out := make(map[ID]int, len(r.shards))
	for _, s := range r.shards {
		out[s] = 0
	}
	for _, k := range keys {
		out[r.Lookup(k)]++
	}
	return out
}

// Points returns the ring positions, for visualization.
func (r *Ring) Points() []struct {
	Hash  uint32
	Shard ID
} {
	out := make([]struct {
		Hash  uint32
		Shard ID
	}, len(r.points))
	for i, p := range r.points {
		out[i].Hash, out[i].Shard = p.hash, p.shard
	}
	return out
}

// String renders the ring for debugging.
func (r *Ring) String() string {
	return fmt.Sprintf("Ring(%d shards, %d vnodes each, %d points)",
		len(r.shards), r.vnodes, len(r.points))
}
