package table

import "time"

// TestTableSpec describes a synthetic Table for unit-test use only. The
// resulting Table has no on-disk SST handle; callers must not invoke any
// method that performs IO (Search, NewIterator, loadBlock). Tests that only
// need to exercise metadata-driven paths (e.g. compaction picker / landing
// buffer accounting) can safely use this constructor.
type TestTableSpec struct {
	FID           uint64
	MinKey        []byte
	MaxKey        []byte
	Size          int64
	ValueSize     uint64
	CreatedAt     time.Time
	MaxVersion    uint64
	StaleDataSize uint32
	KeyCount      uint32
}

// NewTestTable builds a metadata-only Table for tests. Because the table has
// no SSTable handle, calling Search / NewIterator / Delete on the returned
// value will panic or return errors. This constructor is intentionally
// limited to the metadata-only surface used by compaction-planning tests
// and landing-buffer accounting checks.
func NewTestTable(spec TestTableSpec) *Table {
	return &Table{
		fid:           spec.FID,
		minKey:        spec.MinKey,
		maxKey:        spec.MaxKey,
		size:          spec.Size,
		valueSize:     spec.ValueSize,
		createdAt:     spec.CreatedAt,
		maxVersion:    spec.MaxVersion,
		staleDataSize: spec.StaleDataSize,
		keyCount:      spec.KeyCount,
	}
}

// SetTestStaleDataSize overrides the cached stale-size counter. Test-only:
// used to exercise compaction-picker paths that key off stale-data thresholds
// without rebuilding the SST.
func (t *Table) SetTestStaleDataSize(size uint32) { t.staleDataSize = size }

// SetTestCreatedAt overrides the cached creation timestamp. Test-only:
// used to backdate tables for TTL-based picker behavior.
func (t *Table) SetTestCreatedAt(ts time.Time) { t.createdAt = ts }

// SetTestBloomPresent overrides the bloom-filter-present flag. Test-only.
func (t *Table) SetTestBloomPresent(v bool) { t.hasBloom = v }
