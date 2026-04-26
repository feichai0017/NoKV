package lsm

import (
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/engine/wal"
)

// lsmShard owns one slice of the LSM data plane: the active memtable, the
// queue of immutable memtables awaiting flush, and the WAL manager backing
// both. With multiple shards each pair runs on its own fd, fsync worker,
// and bufio.Writer so writes do not contend on a single Manager.mu. See
// docs/notes/2026-04-26-lsm-data-plane-sharding-design.md for the broader
// plan and the routing/recovery/flush invariants.
type lsmShard struct {
	id int
	// lock guards memTable and immutables for this shard.
	lock       sync.RWMutex
	memTable   *memTable
	immutables []*memTable
	wal        *wal.Manager
	// highestFlushedSeg tracks the largest WAL segment ID flushed from
	// this shard. WAL retention uses it so each Manager keeps only its
	// own unflushed segments — a global high-water mark is unsafe under
	// interleaved per-shard flushes.
	highestFlushedSeg atomic.Uint32
}

// newLSMShard constructs an empty shard bound to walMgr. The memtable and
// immutables slice are populated by recovery / NewMemtable.
func newLSMShard(id int, walMgr *wal.Manager) *lsmShard {
	return &lsmShard{
		id:  id,
		wal: walMgr,
	}
}

// primaryShard returns shards[0]. Used by non-pipeline callers (admin
// tools, recovery glue, tests) that don't have an explicit shard pick.
// Production write paths always route by user-key affinity instead.
func (lsm *LSM) primaryShard() *lsmShard {
	if lsm == nil || len(lsm.shards) == 0 {
		return nil
	}
	return lsm.shards[0]
}
