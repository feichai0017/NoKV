package client

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
)

const (
	defaultLookupCacheEntries = 4096
	defaultLookupCacheTTL     = time.Second
)

// LookupCacheConfig controls the optional client-side positive dentry cache.
//
// The cache is deliberately bounded and TTL-based. It is an optimization for
// hot path resolution, not an authority source: callers that need externally
// synchronized freshness should disable it or pair it with watch-based
// invalidation at a higher layer.
type LookupCacheConfig struct {
	MaxEntries int
	TTL        time.Duration
}

// LookupCacheStats is a low-cardinality snapshot of client cache behavior.
type LookupCacheStats struct {
	Hits          uint64
	Misses        uint64
	Inserts       uint64
	Invalidations uint64
	Evictions     uint64
	Expired       uint64
}

// CachedClient wraps a typed fsmeta Client with an optional positive Lookup
// cache. Mutations issued through this wrapper update or invalidate exact
// parent/name entries so the local caller does not see its own stale results.
type CachedClient struct {
	base   Client
	lookup *lookupCache
}

var _ Client = (*CachedClient)(nil)

// NewCachedClient wraps base with a bounded Lookup cache. A zero config uses
// conservative defaults; negative capacity or TTL values are rejected.
func NewCachedClient(base Client, cfg LookupCacheConfig) (*CachedClient, error) {
	if base == nil {
		return nil, errCachedClientRequired
	}
	normalized, err := normalizeLookupCacheConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &CachedClient{
		base:   base,
		lookup: newLookupCache(normalized),
	}, nil
}

func normalizeLookupCacheConfig(cfg LookupCacheConfig) (LookupCacheConfig, error) {
	if cfg.MaxEntries < 0 || cfg.TTL < 0 {
		return LookupCacheConfig{}, errLookupCacheInvalidConfig
	}
	if cfg.MaxEntries == 0 {
		cfg.MaxEntries = defaultLookupCacheEntries
	}
	if cfg.TTL == 0 {
		cfg.TTL = defaultLookupCacheTTL
	}
	return cfg, nil
}

// LookupCacheStats returns a point-in-time copy of cache counters.
func (c *CachedClient) LookupCacheStats() LookupCacheStats {
	if c == nil || c.lookup == nil {
		return LookupCacheStats{}
	}
	return c.lookup.Stats()
}

func (c *CachedClient) Create(ctx context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	result, err := c.base.Create(ctx, req)
	if err == nil {
		c.lookup.Put(req.Mount, result.Dentry)
	}
	return result, err
}

func (c *CachedClient) UpdateInode(ctx context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
	return c.base.UpdateInode(ctx, req)
}

func (c *CachedClient) Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	key := lookupCacheKey{mount: req.Mount, parent: req.Parent, name: req.Name}
	if record, ok := c.lookup.Get(key); ok {
		return record, nil
	}
	record, err := c.base.Lookup(ctx, req)
	if err == nil {
		c.lookup.Put(req.Mount, record)
	}
	return record, err
}

func (c *CachedClient) ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	records, err := c.base.ReadDir(ctx, req)
	if err == nil && req.SnapshotVersion == 0 {
		c.lookup.PutMany(req.Mount, records)
	}
	return records, err
}

func (c *CachedClient) ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	pairs, err := c.base.ReadDirPlus(ctx, req)
	if err == nil && req.SnapshotVersion == 0 {
		for _, pair := range pairs {
			c.lookup.Put(req.Mount, pair.Dentry)
		}
	}
	return pairs, err
}

func (c *CachedClient) WatchSubtree(ctx context.Context, req fsmeta.WatchRequest) (WatchSubscription, error) {
	return c.base.WatchSubtree(ctx, req)
}

func (c *CachedClient) GetReadVersion(ctx context.Context, req fsmeta.ReadVersionRequest) (uint64, error) {
	return c.base.GetReadVersion(ctx, req)
}

func (c *CachedClient) SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	return c.base.SnapshotSubtree(ctx, req)
}

func (c *CachedClient) RetireSnapshotSubtree(ctx context.Context, token fsmeta.SnapshotSubtreeToken) error {
	return c.base.RetireSnapshotSubtree(ctx, token)
}

func (c *CachedClient) GetQuotaUsage(ctx context.Context, req fsmeta.QuotaUsageRequest) (fsmeta.UsageRecord, error) {
	return c.base.GetQuotaUsage(ctx, req)
}

func (c *CachedClient) Rename(ctx context.Context, req fsmeta.RenameRequest) error {
	from := lookupCacheKey{mount: req.Mount, parent: req.FromParent, name: req.FromName}
	to := lookupCacheKey{mount: req.Mount, parent: req.ToParent, name: req.ToName}
	record, hadSource := c.lookup.Peek(from)
	if err := c.base.Rename(ctx, req); err != nil {
		return err
	}
	c.lookup.Invalidate(from)
	c.lookup.Invalidate(to)
	if hadSource {
		record.Parent = req.ToParent
		record.Name = req.ToName
		c.lookup.Put(req.Mount, record)
	}
	return nil
}

func (c *CachedClient) RenameSubtree(ctx context.Context, req fsmeta.RenameSubtreeRequest) error {
	from := lookupCacheKey{mount: req.Mount, parent: req.FromParent, name: req.FromName}
	to := lookupCacheKey{mount: req.Mount, parent: req.ToParent, name: req.ToName}
	record, hadSource := c.lookup.Peek(from)
	if err := c.base.RenameSubtree(ctx, req); err != nil {
		return err
	}
	c.lookup.Invalidate(from)
	c.lookup.Invalidate(to)
	if hadSource {
		record.Parent = req.ToParent
		record.Name = req.ToName
		c.lookup.Put(req.Mount, record)
	}
	return nil
}

func (c *CachedClient) Link(ctx context.Context, req fsmeta.LinkRequest) error {
	if err := c.base.Link(ctx, req); err != nil {
		return err
	}
	c.lookup.Invalidate(lookupCacheKey{mount: req.Mount, parent: req.ToParent, name: req.ToName})
	return nil
}

func (c *CachedClient) Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error {
	if err := c.base.Unlink(ctx, req); err != nil {
		return err
	}
	c.lookup.Invalidate(lookupCacheKey{mount: req.Mount, parent: req.Parent, name: req.Name})
	return nil
}

func (c *CachedClient) OpenWriteSession(ctx context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	return c.base.OpenWriteSession(ctx, req)
}

func (c *CachedClient) HeartbeatWriteSession(ctx context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	return c.base.HeartbeatWriteSession(ctx, req)
}

func (c *CachedClient) CloseWriteSession(ctx context.Context, req fsmeta.CloseWriteSessionRequest) error {
	return c.base.CloseWriteSession(ctx, req)
}

func (c *CachedClient) ExpireWriteSessions(ctx context.Context, req fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	return c.base.ExpireWriteSessions(ctx, req)
}

func (c *CachedClient) Close() error {
	if c != nil && c.lookup != nil {
		c.lookup.Clear()
	}
	if c == nil || c.base == nil {
		return nil
	}
	return c.base.Close()
}

type lookupCacheKey struct {
	mount  fsmeta.MountID
	parent fsmeta.InodeID
	name   string
}

type lookupCacheEntry struct {
	key       lookupCacheKey
	record    fsmeta.DentryRecord
	expiresAt time.Time
}

type lookupCache struct {
	mu         sync.Mutex
	maxEntries int
	ttl        time.Duration
	now        func() time.Time
	items      map[lookupCacheKey]*list.Element
	lru        *list.List
	stats      LookupCacheStats
}

func newLookupCache(cfg LookupCacheConfig) *lookupCache {
	return &lookupCache{
		maxEntries: cfg.MaxEntries,
		ttl:        cfg.TTL,
		now:        time.Now,
		items:      make(map[lookupCacheKey]*list.Element, cfg.MaxEntries),
		lru:        list.New(),
	}
}

func (c *lookupCache) Stats() LookupCacheStats {
	if c == nil {
		return LookupCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *lookupCache) Get(key lookupCacheKey) (fsmeta.DentryRecord, bool) {
	if c == nil {
		return fsmeta.DentryRecord{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem := c.items[key]
	if elem == nil {
		c.stats.Misses++
		return fsmeta.DentryRecord{}, false
	}
	entry := elem.Value.(*lookupCacheEntry)
	if !entry.expiresAt.After(c.now()) {
		c.removeElement(elem)
		c.stats.Expired++
		c.stats.Misses++
		return fsmeta.DentryRecord{}, false
	}
	c.lru.MoveToFront(elem)
	c.stats.Hits++
	return entry.record, true
}

func (c *lookupCache) Peek(key lookupCacheKey) (fsmeta.DentryRecord, bool) {
	if c == nil {
		return fsmeta.DentryRecord{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem := c.items[key]
	if elem == nil {
		return fsmeta.DentryRecord{}, false
	}
	entry := elem.Value.(*lookupCacheEntry)
	if !entry.expiresAt.After(c.now()) {
		c.removeElement(elem)
		c.stats.Expired++
		return fsmeta.DentryRecord{}, false
	}
	return entry.record, true
}

func (c *lookupCache) Put(mount fsmeta.MountID, record fsmeta.DentryRecord) {
	if c == nil || c.maxEntries == 0 {
		return
	}
	key := lookupCacheKey{mount: mount, parent: record.Parent, name: record.Name}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.putLocked(key, record)
}

func (c *lookupCache) PutMany(mount fsmeta.MountID, records []fsmeta.DentryRecord) {
	for _, record := range records {
		c.Put(mount, record)
	}
}

func (c *lookupCache) Invalidate(key lookupCacheKey) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem := c.items[key]; elem != nil {
		c.removeElement(elem)
		c.stats.Invalidations++
	}
}

func (c *lookupCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[lookupCacheKey]*list.Element, c.maxEntries)
	c.lru.Init()
}

func (c *lookupCache) putLocked(key lookupCacheKey, record fsmeta.DentryRecord) {
	if elem := c.items[key]; elem != nil {
		entry := elem.Value.(*lookupCacheEntry)
		entry.record = record
		entry.expiresAt = c.now().Add(c.ttl)
		c.lru.MoveToFront(elem)
		c.stats.Inserts++
		return
	}
	entry := &lookupCacheEntry{
		key:       key,
		record:    record,
		expiresAt: c.now().Add(c.ttl),
	}
	c.items[key] = c.lru.PushFront(entry)
	c.stats.Inserts++
	for len(c.items) > c.maxEntries {
		back := c.lru.Back()
		if back == nil {
			break
		}
		c.removeElement(back)
		c.stats.Evictions++
	}
}

func (c *lookupCache) removeElement(elem *list.Element) {
	c.lru.Remove(elem)
	entry := elem.Value.(*lookupCacheEntry)
	delete(c.items, entry.key)
}
