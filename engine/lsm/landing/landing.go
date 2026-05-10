// Package landing implements the per-level landing buffer that absorbs L0
// promotions before they merge into a level's main tables. The buffer is
// fixed-shard (top bits of the first user-key byte) and policy-neutral: it
// stores generic Table values, exposes shard views and per-shard table
// retrieval, and lets the lsm package layer compaction policy on top.
//
// Sharding is intentionally tiny (4 shards) so cross-shard contention stays
// negligible while the planner still has a knob for parallel drains.
package landing

import (
	"fmt"
	"sort"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/rangefilter"
	"github.com/feichai0017/NoKV/utils"
)

const (
	ShardBits  = 2
	ShardCount = 1 << ShardBits
)

// Table is the minimal contract a buffered table must satisfy. Search returns
// utils.ErrKeyNotFound when nothing visible above maxVs exists; the returned
// newMaxVs is the matched version on success and the input maxVs otherwise.
type Table interface {
	rangefilter.Bounded
	Size() int64
	ValueSize() uint64
	MaxVersionVal() uint64
	Search(key []byte, maxVs uint64) (entry *kv.Entry, newMaxVs uint64, err error)
	FID() uint64
	CreatedAt() time.Time
	DecrRef() error
}

// ShardView is a lightweight per-shard summary used by compaction policy
// (plan.PickShardOrder / plan.PickShardByBacklog).
type ShardView struct {
	Index        int
	TableCount   int
	SizeBytes    int64
	ValueBytes   int64
	MaxAgeSec    float64
	ValueDensity float64
}

type tableRange[T Table] struct {
	min []byte
	max []byte
	tbl T
}

type shard[T Table] struct {
	tables    []T
	ranges    []tableRange[T]
	prefixMax [][]byte
	size      int64
	valueSize int64
}

func (sh *shard[T]) rebuildRanges() {
	if sh == nil {
		return
	}
	sh.ranges = sh.ranges[:0]
	sh.prefixMax = sh.prefixMax[:0]
	for _, t := range sh.tables {
		if any(t) == nil {
			continue
		}
		sh.ranges = append(sh.ranges, tableRange[T]{
			min: t.MinKey(),
			max: t.MaxKey(),
			tbl: t,
		})
	}
	if len(sh.ranges) > 1 {
		sort.Slice(sh.ranges, func(i, j int) bool {
			return kv.CompareInternalKeys(sh.ranges[i].min, sh.ranges[j].min) < 0
		})
	}
	var max []byte
	for _, rng := range sh.ranges {
		if max == nil || kv.CompareBaseKeysAssumeValid(rng.max, max) > 0 {
			max = rng.max
		}
		sh.prefixMax = append(sh.prefixMax, max)
	}
}

// Buffer holds tables sharded by user-key prefix.
type Buffer[T Table] struct {
	shards []shard[T]
}

// EnsureInit lazily allocates the shard slots so callers can use a zero-value
// Buffer field directly.
func (b *Buffer[T]) EnsureInit() {
	if b.shards == nil {
		b.shards = make([]shard[T], ShardCount)
	}
}

// ShardIndex picks the shard that owns the given internal-key minimum.
func ShardIndex(minInternalKey []byte) int {
	_, userKey, _, ok := kv.SplitInternalKey(minInternalKey)
	utils.CondPanicFunc(!ok, func() error {
		return fmt.Errorf("landing.ShardIndex expects internal key: %x", minInternalKey)
	})
	if len(userKey) == 0 {
		return 0
	}
	return int(userKey[0] >> (8 - ShardBits))
}

// Add inserts one table into its owning shard.
func (b *Buffer[T]) Add(t T) {
	if any(t) == nil {
		return
	}
	b.EnsureInit()
	idx := ShardIndex(t.MinKey())
	sh := &b.shards[idx]
	sh.tables = append(sh.tables, t)
	sh.size += t.Size()
	sh.valueSize += int64(t.ValueSize())
	sh.rebuildRanges()
}

// AddBatch inserts a batch of tables, rebuilding affected shards once.
func (b *Buffer[T]) AddBatch(ts []T) {
	if len(ts) == 0 {
		return
	}
	b.EnsureInit()
	updated := make(map[int]struct{})
	for _, t := range ts {
		if any(t) == nil {
			continue
		}
		idx := ShardIndex(t.MinKey())
		sh := &b.shards[idx]
		sh.tables = append(sh.tables, t)
		sh.size += t.Size()
		sh.valueSize += int64(t.ValueSize())
		updated[idx] = struct{}{}
	}
	for idx := range updated {
		b.shards[idx].rebuildRanges()
	}
}

// Remove drops every table whose FID is in toDel from every shard.
func (b *Buffer[T]) Remove(toDel map[uint64]struct{}) {
	if len(toDel) == 0 {
		return
	}
	b.EnsureInit()
	for i := range b.shards {
		sh := &b.shards[i]
		if len(sh.tables) == 0 {
			continue
		}
		var kept []T
		for _, t := range sh.tables {
			if any(t) == nil {
				continue
			}
			if _, drop := toDel[t.FID()]; drop {
				sh.size -= t.Size()
				sh.valueSize -= int64(t.ValueSize())
				continue
			}
			kept = append(kept, t)
		}
		sh.tables = kept
		if sh.size < 0 {
			sh.size = 0
		}
		if sh.valueSize < 0 {
			sh.valueSize = 0
		}
		sh.rebuildRanges()
	}
}

// TableCount returns the total number of buffered tables across all shards.
func (b Buffer[T]) TableCount() int {
	var n int
	for _, sh := range b.shards {
		n += len(sh.tables)
	}
	return n
}

// TotalSize returns the cumulative on-disk size of all buffered tables.
func (b Buffer[T]) TotalSize() int64 {
	var n int64
	for _, sh := range b.shards {
		n += sh.size
	}
	return n
}

// TotalValueSize returns the cumulative inline-value bytes across all shards.
func (b Buffer[T]) TotalValueSize() int64 {
	var n int64
	for _, sh := range b.shards {
		n += sh.valueSize
	}
	return n
}

// AllTables returns every buffered table in shard-order then insert-order.
func (b Buffer[T]) AllTables() []T {
	var out []T
	for _, sh := range b.shards {
		out = append(out, sh.tables...)
	}
	return out
}

// ShardTablesByIndex returns the tables in shard idx, or nil if the index is
// out of range or empty.
func (b *Buffer[T]) ShardTablesByIndex(idx int) []T {
	if b == nil {
		return nil
	}
	b.EnsureInit()
	if idx < 0 || idx >= len(b.shards) {
		return nil
	}
	sh := b.shards[idx]
	if len(sh.tables) == 0 {
		return nil
	}
	out := make([]T, len(sh.tables))
	copy(out, sh.tables)
	return out
}

// SortShards sorts each shard's tables by min internal key and rebuilds the
// per-shard prefixMax index. Caller must hold any external locks needed for
// concurrent visibility.
func (b *Buffer[T]) SortShards() {
	b.EnsureInit()
	for i := range b.shards {
		sh := &b.shards[i]
		if len(sh.tables) > 1 {
			sort.Slice(sh.tables, func(a, b int) bool {
				return kv.CompareInternalKeys(sh.tables[a].MinKey(), sh.tables[b].MinKey()) < 0
			})
		}
		sh.rebuildRanges()
	}
}

// ShardViews returns one ShardView per non-empty shard for compaction policy.
func (b Buffer[T]) ShardViews() []ShardView {
	now := time.Now()
	var views []ShardView
	for i, sh := range b.shards {
		if len(sh.tables) == 0 {
			continue
		}
		var maxAge float64
		for _, t := range sh.tables {
			if any(t) == nil {
				continue
			}
			age := now.Sub(t.CreatedAt()).Seconds()
			if age > maxAge {
				maxAge = age
			}
		}
		var density float64
		if sh.size > 0 {
			density = float64(sh.valueSize) / float64(sh.size)
		}
		views = append(views, ShardView{
			Index:        i,
			TableCount:   len(sh.tables),
			SizeBytes:    sh.size,
			ValueBytes:   sh.valueSize,
			MaxAgeSec:    maxAge,
			ValueDensity: density,
		})
	}
	return views
}

// Search walks every shard whose prefix range may contain key and returns the
// most recent visible Entry. Returns utils.ErrKeyNotFound when nothing
// visible above maxVersion exists. newMaxVersion equals the matched
// entry's version on success and the input maxVersion otherwise.
func (b Buffer[T]) Search(key []byte, maxVersion uint64) (*kv.Entry, uint64, error) {
	var best *kv.Entry
	for _, sh := range b.shards {
		if len(sh.ranges) == 0 {
			continue
		}
		ranges := sh.ranges
		lo, hi := 0, len(ranges)
		for lo < hi {
			mid := (lo + hi) / 2
			if kv.CompareBaseKeysAssumeValid(key, ranges[mid].min) >= 0 {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		for i := lo - 1; i >= 0; i-- {
			if i < len(sh.prefixMax) && kv.CompareBaseKeysAssumeValid(key, sh.prefixMax[i]) > 0 {
				break
			}
			rng := ranges[i]
			if any(rng.tbl) == nil {
				continue
			}
			if kv.CompareBaseKeysAssumeValid(key, rng.max) > 0 {
				continue
			}
			if rng.tbl.MaxVersionVal() <= maxVersion {
				continue
			}
			entry, newMax, err := rng.tbl.Search(key, maxVersion)
			if err == nil {
				if best != nil {
					best.DecrRef()
				}
				best = entry
				maxVersion = newMax
				continue
			}
			if err != utils.ErrKeyNotFound {
				if best != nil {
					best.DecrRef()
				}
				return nil, maxVersion, err
			}
		}
	}
	if best != nil {
		return best, maxVersion, nil
	}
	return nil, maxVersion, utils.ErrKeyNotFound
}

// MaxAgeSeconds returns the maximum age (seconds since CreatedAt) of any
// buffered table, or zero if the buffer is empty.
func (b Buffer[T]) MaxAgeSeconds() float64 {
	now := time.Now()
	var maxAge float64
	for _, sh := range b.shards {
		for _, t := range sh.tables {
			if any(t) == nil {
				continue
			}
			age := now.Sub(t.CreatedAt()).Seconds()
			if age > maxAge {
				maxAge = age
			}
		}
	}
	return maxAge
}

// TablesWithinBounds returns all buffered tables whose key range overlaps
// [lower, upper]. Empty bounds match all tables.
func (b Buffer[T]) TablesWithinBounds(lower, upper []byte) []T {
	var out []T
	for _, sh := range b.shards {
		if len(sh.tables) == 0 {
			continue
		}
		matched := rangefilter.FilterByBounds(sh.tables, lower, upper)
		if len(matched) == 0 {
			continue
		}
		out = append(out, matched...)
	}
	return out
}
