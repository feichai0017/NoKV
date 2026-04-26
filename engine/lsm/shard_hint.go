package lsm

import (
	"bytes"
	"sync"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/feichai0017/NoKV/engine/kv"
)

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
