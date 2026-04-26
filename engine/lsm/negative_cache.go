package lsm

import (
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/slab/negativecache"
)

// negativeCache is the lsm-side adapter around the generic
// engine/slab/negativecache.Cache. It supplies kv.InternalToBaseKey as
// the GroupKeyFn so Invalidate(internal_key) bumps the user-key group
// — every cached "X@v not found" entry whose base equals X becomes
// stale on the next Has lookup.
//
// The wire format, persistence, and per-bucket cuckoo-style hashing
// all live in engine/slab/negativecache. This file is a 30-line
// adapter; the only lsm-specific knowledge is "internal keys carry a
// timestamp suffix and the group key is the user key without it".
type negativeCache struct {
	inner *negativecache.Cache
}

func newNegativeCache() *negativeCache {
	return &negativeCache{
		inner: negativecache.New(negativecache.Config{
			GroupKeyFn: kv.InternalToBaseKey,
		}),
	}
}

// newNegativeCacheWithInner wraps a pre-built generic cache. Used by
// the persistent variant where Open returns the cache plus a
// Persistence helper as a pair.
func newNegativeCacheWithInner(inner *negativecache.Cache) *negativeCache {
	return &negativeCache{inner: inner}
}

func (c *negativeCache) contains(internalKey []byte) bool {
	if c == nil {
		return false
	}
	return c.inner.Has(internalKey)
}

func (c *negativeCache) remember(internalKey []byte) {
	if c == nil {
		return
	}
	c.inner.Remember(internalKey)
}

func (c *negativeCache) invalidate(internalKey []byte) {
	if c == nil {
		return
	}
	c.inner.Invalidate(internalKey)
}

func (c *negativeCache) clear() {
	if c == nil {
		return
	}
	c.inner.Clear()
}
