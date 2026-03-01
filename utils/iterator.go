package utils

import "github.com/feichai0017/NoKV/kv"

// Iterator abstracts key/value iteration.
type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
	Seek(key []byte)
}

// Item _
type Item interface {
	Entry() *kv.Entry
}

// Options _
type Options struct {
	Prefix []byte
	IsAsc  bool
	// OnlyUseKey instructs iterators to avoid materialising value log entries
	// eagerly. Callers should rely on Item.ValueCopy when value access is
	// required. This keeps the default behaviour (false) for existing users.
	OnlyUseKey bool
	// AccessPattern lets callers hint expected IO behaviour (sequential scans,
	// random point lookups, etc.) so the file layer can tune madvise settings.
	AccessPattern AccessPattern
	// PrefetchBlocks controls how many blocks ahead to prefetch eagerly. Zero
	// disables prefetch.
	PrefetchBlocks int
	// PrefetchWorkers optionally overrides worker count (defaults to
	// min(PrefetchBlocks,4)). Zero uses the default.
	PrefetchWorkers int
	// PrefetchAdaptive enables dynamic tuning of prefetch based on hit/miss stats.
	PrefetchAdaptive bool
	// Metrics tags for observability.
	MetricTag string
	// Inclusive lower bound
	LowerBound []byte
	// Exclusive upper bound
	UpperBound []byte
}
