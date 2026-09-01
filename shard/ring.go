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
	"sync"
)

// ID identifies one shard — one independent Raft group.
type ID string

// Ring maps account IDs to shards using a consistent hash ring.
//
// Placement MUST be a pure function of (key, ring config): every node computes
// ownership independently with no coordination. If two nodes disagreed about who
// owns an account, that would be a correctness bug, not a performance one.
type Ring struct {
	// mu guards points and shards.
	//
	// The ring was immutable after construction until live resharding made
	// ownership changeable (§23). Lookup runs on every operation, so this is an
	// RWMutex: readers never block each other, and the single writer is a
	// completed cutover.
	mu sync.RWMutex

	// points are virtual node positions on the ring, sorted by hash.
	points []point

	// vnodes is how many virtual points each shard occupies.
	vnodes int

	shards []ID
}

type point struct {
	hash  uint32
	shard ID

	// vnode is which of this shard's virtual nodes this point is (0..vnodes-1).
	//
	// Recorded because live resharding moves VIRTUAL NODES, not key ranges: a
	// shard's territory is ~150 separate arcs, and naming the arc is what lets a
	// migration say precisely which keys are moving (migration.go, §23). Without
	// it the only way to describe a moving subset would be a hash range, which
	// would then have to be re-derived on every lookup.
	vnode int
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
				vnode: i,
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

// Segment is one arc of the ring: the span [Start, End) owned by one shard.
//
// Exposed for visualization. The ring's real structure is VIRTUAL NODES — each
// shard placed at many points — and a view that only plots a few account keys
// shows where those keys happen to land while hiding the ownership structure
// that actually decides placement.
type Segment struct {
	Start uint32
	End   uint32
	Shard ID
}

// Segments returns the ring's ownership arcs, in ring order.
//
// Each arc runs from one virtual node to the next, and belongs to the shard at
// its END: a key is owned by the first shard clockwise from its hash, so the arc
// leading up to a virtual node is that node's territory.
func (r *Ring) Segments() []Segment {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.points) == 0 {
		return nil
	}

	out := make([]Segment, 0, len(r.points))
	for i, p := range r.points {
		// The arc preceding this point. The first wraps around from the last, which
		// is what makes it a ring rather than a line.
		var start uint32
		if i == 0 {
			start = r.points[len(r.points)-1].hash
		} else {
			start = r.points[i-1].hash
		}
		out = append(out, Segment{Start: start, End: p.hash, Shard: p.shard})
	}
	return out
}

// VNodes reports how many virtual points each shard occupies.
func (r *Ring) VNodes() int { return r.vnodes }

// HashKey exposes a key's position on the ring, for visualization.
//
// Exported so the dashboard draws the REAL placement rather than a decorative
// approximation: a ring view that shows a key somewhere other than where the ring
// actually puts it teaches the wrong thing, which for a view whose whole purpose
// is to make placement visible would be worse than showing nothing.
func HashKey(s string) uint32 { return hashKey(s) }

// Lookup returns the shard owning key: the first shard clockwise from hash(key).
func (r *Ring) Lookup(key string) ID {
	s, _ := r.LookupVNode(key)
	return s
}

// LookupVNode returns the shard owning a key AND which of that shard's virtual
// nodes owns it.
//
// The vnode index is what live resharding moves (migration.go): it names one arc
// of the ring precisely, so a migration can say exactly which keys are in flight
// without re-deriving a hash range on every lookup.
func (r *Ring) LookupVNode(key string) (ID, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.points) == 0 {
		return "", -1
	}
	h := hashKey(key)

	// First point with hash >= h.
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0 // wrapped past the end of the ring
	}
	return r.points[i].shard, r.points[i].vnode
}

// Shards returns the shards in the ring.
func (r *Ring) Shards() []ID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return append([]ID(nil), r.shards...)
}

// Distribution reports how many of the given keys land on each shard. Used by
// tests to verify virtual nodes actually balance the ring, and by the dashboard's
// ring view.
func (r *Ring) Distribution(keys []string) map[ID]int {
	// Snapshot the shard list under the lock, then release it before calling
	// Lookup — Lookup takes the read lock itself via LookupVNode, and although an
	// RWMutex permits recursive read locks, relying on that deadlocks the moment a
	// writer queues between the two acquisitions.
	shards := r.Shards()

	out := make(map[ID]int, len(shards))
	for _, s := range shards {
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
	r.mu.RLock()
	defer r.mu.RUnlock()

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
	r.mu.RLock()
	defer r.mu.RUnlock()

	return fmt.Sprintf("Ring(%d shards, %d vnodes each, %d points)",
		len(r.shards), r.vnodes, len(r.points))
}

// Reassign permanently moves virtual nodes of one shard to another.
//
// Called at the END of a live reshard, once the cutover is committed (§23).
//
// Why the ring itself must change, and not just the migration table: the table is
// transient state describing a move IN FLIGHT, and a completed cutover is
// permanent. Leaving the new ownership recorded only in the table meant that
// finishing the migration handed the keys straight back to the source — the
// cutover undid itself the instant it completed, which is precisely the bug the
// routing test caught.
//
// Takes the write lock because Lookup runs on every operation; the reassignment
// is a handful of point updates and a re-sort, and it happens once per migration.
func (r *Ring) Reassign(from, to ID, vnodes []int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	want := make(map[int]bool, len(vnodes))
	for _, v := range vnodes {
		want[v] = true
	}

	for i := range r.points {
		if r.points[i].shard == from && want[r.points[i].vnode] {
			r.points[i].shard = to
		}
	}

	// The hash of each point is unchanged — only its owner — so the sort order is
	// already correct and no re-sort is needed. Recording that explicitly because
	// the natural assumption is the opposite.

	if !containsShard(r.shards, to) {
		r.shards = append(r.shards, to)
	}
}

func containsShard(ids []ID, want ID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
