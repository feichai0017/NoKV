package lsm

import (
	"bytes"
	"sync"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/feichai0017/NoKV/engine/kv"
)

const defaultNegativeCacheBuckets = 1 << 16

type negativeCache struct {
	mask        uint64
	entries     []negativeEntryBucket
	generations []negativeGenerationBucket
}

type negativeEntryBucket struct {
	mu    sync.RWMutex
	hash  uint64
	key   []byte
	gen   uint64
	valid bool
}

type negativeGenerationBucket struct {
	mu    sync.RWMutex
	hash  uint64
	key   []byte
	gen   uint64
	valid bool
}

func newNegativeCache() *negativeCache {
	return &negativeCache{
		mask:        defaultNegativeCacheBuckets - 1,
		entries:     make([]negativeEntryBucket, defaultNegativeCacheBuckets),
		generations: make([]negativeGenerationBucket, defaultNegativeCacheBuckets),
	}
}

func (c *negativeCache) contains(internalKey []byte) bool {
	if c == nil || len(c.entries) == 0 || len(c.generations) == 0 {
		return false
	}
	baseKey, baseHash, keyHash, ok := negativeKey(internalKey)
	if !ok {
		return false
	}
	gen, ok := c.generation(baseKey, baseHash)
	if !ok {
		return false
	}
	b := &c.entries[keyHash&c.mask]
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.valid && b.hash == keyHash && b.gen == gen && bytes.Equal(b.key, internalKey)
}

func (c *negativeCache) remember(internalKey []byte) {
	if c == nil || len(c.entries) == 0 || len(c.generations) == 0 {
		return
	}
	baseKey, baseHash, keyHash, ok := negativeKey(internalKey)
	if !ok {
		return
	}
	gen := c.ensureGeneration(baseKey, baseHash)
	b := &c.entries[keyHash&c.mask]
	b.mu.Lock()
	b.hash = keyHash
	b.key = append(b.key[:0], internalKey...)
	b.gen = gen
	b.valid = true
	b.mu.Unlock()
}

func (c *negativeCache) invalidate(internalKey []byte) {
	if c == nil || len(c.generations) == 0 {
		return
	}
	baseKey, baseHash, _, ok := negativeKey(internalKey)
	if !ok {
		return
	}
	b := &c.generations[baseHash&c.mask]
	b.mu.Lock()
	if !b.valid || b.hash != baseHash || !bytes.Equal(b.key, baseKey) {
		b.hash = baseHash
		b.key = append(b.key[:0], baseKey...)
		b.gen = 1
		b.valid = true
		b.mu.Unlock()
		return
	}
	b.gen++
	if b.gen == 0 {
		b.gen = 1
	}
	b.mu.Unlock()
}

func (c *negativeCache) clear() {
	if c == nil {
		return
	}
	for i := range c.entries {
		b := &c.entries[i]
		b.mu.Lock()
		b.valid = false
		b.key = b.key[:0]
		b.hash = 0
		b.gen = 0
		b.mu.Unlock()
	}
	for i := range c.generations {
		b := &c.generations[i]
		b.mu.Lock()
		b.valid = false
		b.key = b.key[:0]
		b.hash = 0
		b.gen = 0
		b.mu.Unlock()
	}
}

func (c *negativeCache) generation(baseKey []byte, baseHash uint64) (uint64, bool) {
	b := &c.generations[baseHash&c.mask]
	b.mu.RLock()
	defer b.mu.RUnlock()
	if !b.valid || b.hash != baseHash || !bytes.Equal(b.key, baseKey) {
		return 0, false
	}
	return b.gen, true
}

func (c *negativeCache) ensureGeneration(baseKey []byte, baseHash uint64) uint64 {
	b := &c.generations[baseHash&c.mask]
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.valid || b.hash != baseHash || !bytes.Equal(b.key, baseKey) {
		b.hash = baseHash
		b.key = append(b.key[:0], baseKey...)
		b.gen = 1
		b.valid = true
		return b.gen
	}
	if b.gen == 0 {
		b.gen = 1
	}
	return b.gen
}

func negativeKey(internalKey []byte) ([]byte, uint64, uint64, bool) {
	if len(internalKey) == 0 {
		return nil, 0, 0, false
	}
	baseKey := kv.InternalToBaseKey(internalKey)
	if len(baseKey) == 0 {
		return nil, 0, 0, false
	}
	return baseKey, xxhash.Sum64(baseKey), xxhash.Sum64(internalKey), true
}
