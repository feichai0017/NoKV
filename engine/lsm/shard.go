package lsm

import (
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/utils"
)

// lsmShard owns one slice of the LSM data plane: the active memtable, the
// queue of immutable memtables awaiting flush, and the WAL manager backing
// both. With multiple shards each pair runs on its own fd, fsync worker,
// and bufio.Writer so writes do not contend on a single Manager.mu.
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

// ShardForInternalKey returns the LSM data-plane shard for an MVCC internal
// key. Callers that pre-group multi-key internal batches must use this router;
// otherwise same-version delete/write pairs for one user key can land on
// different shards and lose last-write-wins semantics during reads.
func ShardForInternalKey(internalKey []byte, shardCount int) int {
	_, userKey, _, ok := kv.SplitInternalKey(internalKey)
	if !ok {
		return 0
	}
	return utils.ShardForUserKey(userKey, shardCount)
}
