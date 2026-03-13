package lsm

import (
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm/tombstone"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
	"github.com/pkg/errors"
)

type memIndex interface {
	Add(*kv.Entry)
	Search([]byte) ([]byte, kv.ValueStruct)
	NewIterator(*utils.Options) utils.Iterator
	MemSize() int64
	IncrRef()
	DecrRef()
}

// memTable holds the active Skiplist and its WAL segment id.
type memTable struct {
	lsm        *LSM
	segmentID  uint32
	index      memIndex
	maxVersion uint64
	walSize    atomic.Int64
	// reservedSize tracks in-flight write reservations to avoid overcommitting
	// a memtable when multiple writers proceed under read locks.
	reservedSize atomic.Int64

	// rtMu protects rangeTombstones. Written by setBatch (which may run
	// under lsm.lock read-lock or write-lock), read concurrently by
	// isKeyCoveredByRangeTombstone. The cached per-CF span index is rebuilt
	// lazily on demand when rtIndexDirty is set.
	rtMu            sync.RWMutex
	rangeTombstones []tombstone.Range
	rtIndex         map[kv.ColumnFamily][]tombstone.Span
	rtIndexDirty    bool
}

func arenaSizeFor(memTableSize int64) int64 {
	base := memTableSize
	if base <= 0 {
		base = utils.DefaultArenaSize
	}
	if base < utils.DefaultArenaSize {
		base = utils.DefaultArenaSize
	}
	if base > math.MaxInt64/2 {
		return math.MaxInt64
	}
	return base * 2
}

// NewMemtable creates the active MemTable and switches WAL to the new segment.
func (lsm *LSM) NewMemtable() (*memTable, error) {
	newFid := lsm.levels.maxFID.Add(1)
	if err := lsm.wal.SwitchSegment(uint32(newFid), true); err != nil {
		return nil, err
	}
	return &memTable{
		lsm:       lsm,
		segmentID: uint32(newFid),
		index:     newMemIndex(lsm.option),
	}, nil
}

func (m *memTable) close() error {
	if m == nil {
		return nil
	}
	// Range tombstones have already been transferred to rtCollector by the flush path.
	m.rtMu.Lock()
	m.rangeTombstones = nil
	m.rtIndex = nil
	m.rtIndexDirty = false
	m.rtMu.Unlock()
	return nil
}

// Set inserts one entry into the memtable and appends it to WAL.
func (m *memTable) Set(entry *kv.Entry) error {
	if m == nil {
		return errors.New("lsm: memtable not initialized")
	}
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	info, err := m.lsm.wal.AppendEntry(entry)
	if err != nil {
		return err
	}
	m.walSize.Add(int64(info.Length) + 8)
	if m.index != nil {
		m.index.Add(entry)
		m.trackRangeTombstone(entry)
	}
	return nil
}

// Get reads key from the memtable index and returns a pooled entry wrapper.
func (m *memTable) Get(key []byte) (*kv.Entry, error) {
	var (
		foundKey []byte
		vs       kv.ValueStruct
	)
	if m.index != nil {
		foundKey, vs = m.index.Search(key)
	}
	e := kv.EntryPool.Get().(*kv.Entry)
	e.Key = foundKey
	e.Value = vs.Value
	e.ExpiresAt = vs.ExpiresAt
	e.Meta = vs.Meta
	e.CF = kv.CFDefault
	e.Version = 0
	e.Offset = 0
	e.Hlen = 0
	e.ValThreshold = 0
	if cf, _, version, ok := kv.SplitInternalKey(foundKey); ok {
		e.CF = cf
		e.Version = version
	}
	_ = e.PopulateInternalMeta()
	e.IncrRef()
	return e, nil
}

// Size returns the memory footprint reported by the backing mem index.
func (m *memTable) Size() int64 {
	if m == nil || m.index == nil {
		return 0
	}
	return m.index.MemSize()
}

func (m *memTable) setBatch(entries []*kv.Entry) error {
	if m == nil || len(entries) == 0 {
		return nil
	}
	if len(entries) == 1 {
		return m.Set(entries[0])
	}
	info, err := m.lsm.wal.AppendEntryBatch(entries)
	if err != nil {
		return err
	}
	m.walSize.Add(int64(info.Length) + 8)
	if m.index != nil {
		for _, entry := range entries {
			m.index.Add(entry)
			m.trackRangeTombstone(entry)
		}
	}
	return nil
}

// recovery rebuilds memtables from existing WAL segments.
func (lsm *LSM) recovery() (*memTable, []*memTable, error) {
	files, err := lsm.wal.ListSegments()
	if err != nil {
		return nil, nil, err
	}
	var fids []uint64
	maxFid := lsm.levels.maxFID.Load()
	for _, path := range files {
		base := filepath.Base(path)
		if !strings.HasSuffix(base, ".wal") {
			continue
		}
		fid, err := strconv.ParseUint(strings.TrimSuffix(base, ".wal"), 10, 32)
		if err != nil {
			return nil, nil, err
		}
		if fid > maxFid {
			maxFid = fid
		}
		fids = append(fids, fid)
	}
	slices.Sort(fids)

	if seg, _ := lsm.levels.logPointer(); seg > 0 {
		cleaned := make([]uint64, 0, len(fids))
		for _, fid := range fids {
			if fid <= uint64(seg) {
				if !lsm.canRemoveWalSegment(uint32(fid)) {
					cleaned = append(cleaned, fid)
					continue
				}
				if err := lsm.wal.RemoveSegment(uint32(fid)); err != nil && !os.IsNotExist(err) {
					slog.Default().Error("remove wal segment", "segment", fid, "error", err)
				}
				continue
			}
			cleaned = append(cleaned, fid)
		}
		fids = cleaned
	}

	if len(fids) == 0 {
		lsm.levels.maxFID.Store(maxFid)
		active, createErr := lsm.NewMemtable()
		if createErr != nil {
			return nil, nil, createErr
		}
		return active, nil, nil
	}

	tables := make([]*memTable, 0, len(fids))
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		if err != nil {
			return nil, nil, err
		}
		if mt.Size() == 0 {
			continue
		}
		tables = append(tables, mt)
	}
	if len(tables) == 0 {
		lsm.levels.maxFID.Store(maxFid)
		active, createErr := lsm.NewMemtable()
		if createErr != nil {
			return nil, nil, createErr
		}
		return active, nil, nil
	}

	lsm.levels.maxFID.Store(maxFid)
	active := tables[len(tables)-1]
	tables = tables[:len(tables)-1]
	if err := lsm.wal.SwitchSegment(active.segmentID, false); err != nil {
		return nil, nil, err
	}
	return active, tables, nil
}

func (lsm *LSM) openMemTable(fid uint64) (*memTable, error) {
	mt := &memTable{
		lsm:       lsm,
		segmentID: uint32(fid),
		index:     newMemIndex(lsm.option),
	}
	err := lsm.wal.ReplaySegment(uint32(fid), func(info wal.EntryInfo, payload []byte) error {
		applyEntry := func(entry *kv.Entry) {
			if ts := kv.Timestamp(entry.Key); ts > mt.maxVersion {
				mt.maxVersion = ts
			}
			if mt.index != nil {
				mt.index.Add(entry)
			}
			mt.trackRangeTombstone(entry)
			entry.DecrRef()
		}
		switch info.Type {
		case wal.RecordTypeEntry:
			entry, err := kv.DecodeEntry(payload)
			if err != nil {
				return err
			}
			applyEntry(entry)
		case wal.RecordTypeEntryBatch:
			entries, err := wal.DecodeEntryBatch(payload)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				applyEntry(entry)
			}
		default:
			return nil
		}
		mt.walSize.Add(int64(info.Length) + 8)
		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "while updating memtable index")
	}
	return mt, nil
}

func (mt *memTable) canReserve(need, limit int64) bool {
	if mt == nil {
		return false
	}
	if need <= 0 {
		return true
	}
	used := mt.walSize.Load()
	reserved := mt.reservedSize.Load()
	if used < 0 || reserved < 0 || used > limit {
		return false
	}
	remaining := limit - used
	if reserved > remaining {
		return false
	}
	remaining -= reserved
	return need <= remaining
}

func (mt *memTable) tryReserve(need, limit int64) bool {
	if mt == nil {
		return false
	}
	if need <= 0 {
		return true
	}
	for {
		used := mt.walSize.Load()
		reserved := mt.reservedSize.Load()
		if used < 0 || reserved < 0 || used > limit {
			return false
		}
		remaining := limit - used
		if reserved > remaining {
			return false
		}
		remaining -= reserved
		if need > remaining {
			return false
		}
		if mt.reservedSize.CompareAndSwap(reserved, reserved+need) {
			return true
		}
	}
}

func (mt *memTable) releaseReserve(need int64) {
	if mt == nil || need <= 0 {
		return
	}
	if n := mt.reservedSize.Add(-need); n < 0 {
		// Defensive clamping: reservation accounting bug must not crash the whole service.
		// Keep the counter non-negative and surface via diagnostics/tests.
		mt.reservedSize.Store(0)
	}
}

// reference counting helpers, delegate to the backing index.
func (mt *memTable) IncrRef() {
	if mt == nil || mt.index == nil {
		return
	}
	mt.index.IncrRef()
}

// DecrRef decrements the underlying mem index reference count.
func (mt *memTable) DecrRef() {
	if mt == nil || mt.index == nil {
		return
	}
	mt.index.DecrRef()
}

// isKeyCoveredByRangeTombstone checks if userKey@entryVersion in cf is covered
// by any in-memory range tombstone.
func (m *memTable) isKeyCoveredByRangeTombstone(cf kv.ColumnFamily, userKey []byte, entryVersion uint64) bool {
	if m == nil {
		return false
	}
	m.rtMu.RLock()
	dirty := m.rtIndexDirty
	var spans []tombstone.Span
	if !dirty {
		spans = m.rtIndex[cf]
	}
	m.rtMu.RUnlock()
	if !dirty {
		return tombstone.IsKeyCoveredBySpans(spans, userKey, entryVersion)
	}
	m.rtMu.Lock()
	if m.rtIndexDirty {
		m.rtIndex = tombstone.BuildCFSpans(m.rangeTombstones)
		m.rtIndexDirty = false
	}
	spans = m.rtIndex[cf]
	m.rtMu.Unlock()
	return tombstone.IsKeyCoveredBySpans(spans, userKey, entryVersion)
}

func (m *memTable) hasRangeTombstones() bool {
	if m == nil {
		return false
	}
	m.rtMu.RLock()
	n := len(m.rangeTombstones)
	m.rtMu.RUnlock()
	return n > 0
}

// trackRangeTombstone caches range tombstones in memtable-local memory so read
// path coverage checks avoid allocating iterators.
func (m *memTable) trackRangeTombstone(entry *kv.Entry) {
	if m == nil || entry == nil || !entry.IsRangeDelete() {
		return
	}
	_, start, version, ok := kv.SplitInternalKey(entry.Key)
	if !ok {
		return
	}
	m.rtMu.Lock()
	cf := entry.CF
	if !cf.Valid() {
		cf = kv.CFDefault
	}
	m.rangeTombstones = append(m.rangeTombstones, tombstone.Range{
		CF:      cf,
		Start:   kv.SafeCopy(nil, start),
		End:     kv.SafeCopy(nil, entry.RangeEnd()),
		Version: version,
	})
	m.rtIndexDirty = true
	m.rtMu.Unlock()
}

func newMemIndex(opt *Options) memIndex {
	if opt == nil {
		return utils.NewART(arenaSizeFor(0))
	}
	switch opt.MemTableEngine {
	case "skiplist":
		return utils.NewSkiplist(arenaSizeFor(opt.MemTableSize))
	case "", "art":
		fallthrough
	default:
		return utils.NewART(arenaSizeFor(opt.MemTableSize))
	}
}
