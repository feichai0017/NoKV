package lsm

import (
	"bytes"
	"sync"
	"sync/atomic"

	xxhash "github.com/cespare/xxhash/v2"
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

// ---- Shard-routing hint cache ----
//
// shardHintTable accelerates per-key shard lookup so the write path can
// short-circuit the InternalKey -> userKey -> shard hash chain when the same
// user key has been routed before. The hint is approximate: a stale entry just
// triggers a re-route, never wrong-data.

const defaultShardHintBuckets = 1 << 16

type shardHintTable struct {
	mask    uint64
	buckets []shardHintBucket
}

type shardHintBucket struct {
	mu    sync.RWMutex
	hash  uint64
	key   []byte
	shard int
	valid bool
}

func newShardHintTable() *shardHintTable {
	return &shardHintTable{
		mask:    defaultShardHintBuckets - 1,
		buckets: make([]shardHintBucket, defaultShardHintBuckets),
	}
}

func (h *shardHintTable) lookup(internalKey []byte) (int, bool) {
	if h == nil || len(h.buckets) == 0 {
		return 0, false
	}
	baseKey, hash, ok := shardHintKey(internalKey)
	if !ok {
		return 0, false
	}
	b := &h.buckets[hash&h.mask]
	b.mu.RLock()
	defer b.mu.RUnlock()
	if !b.valid || b.hash != hash || !bytes.Equal(b.key, baseKey) {
		return 0, false
	}
	return b.shard, true
}

func (h *shardHintTable) remember(internalKey []byte, shardID int) {
	if h == nil || len(h.buckets) == 0 || shardID < 0 {
		return
	}
	baseKey, hash, ok := shardHintKey(internalKey)
	if !ok {
		return
	}
	b := &h.buckets[hash&h.mask]
	b.mu.Lock()
	b.hash = hash
	b.key = append(b.key[:0], baseKey...)
	b.shard = shardID
	b.valid = true
	b.mu.Unlock()
}

func shardHintKey(internalKey []byte) ([]byte, uint64, bool) {
	baseKey := kv.InternalToBaseKey(internalKey)
	if len(baseKey) == 0 {
		return nil, 0, false
	}
	return baseKey, xxhash.Sum64(baseKey), true
}
