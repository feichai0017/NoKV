package lsm

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
	"github.com/pkg/errors"
)

// MemTable is an exported alias for the in-memory mutable table implementation.
type MemTable = memTable

type memIndex interface {
	Add(*kv.Entry)
	Search([]byte) ([]byte, kv.ValueStruct)
	NewIterator(*utils.Options) utils.Iterator
	MemSize() int64
	IncrRef()
	DecrRef()
}

// memRangeTombstone is a lightweight copy of a range tombstone stored
// directly in the memtable for O(M) lookup without iterator allocation.
// segmentID + walOffset form a globally monotonic write sequence number:
// higher segmentID is always newer; within the same segment, higher walOffset is newer.
type memRangeTombstone struct {
	cf        kv.ColumnFamily
	start     []byte
	end       []byte
	version   uint64
	segmentID uint32
	walOffset int64
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
	// isKeyCoveredByRangeTombstone. rtMu is the sole guard for this slice.
	rtMu            sync.RWMutex
	rangeTombstones []memRangeTombstone

	// keyWalOffset maps string(internalKey) -> walSeqKey (segmentID + walOffset)
	// of the most recent write for that key. Used to determine whether a range
	// tombstone postdates a specific key write. ValueStruct.Version is not
	// serialized by the arena so we cannot rely on the index to carry this.
	keyWalOffset sync.Map // value type: walSeqKey
}

// walSeqKey is a globally monotonic write sequence key:
// higher segmentID is always newer; within the same segment, higher walOffset is newer.
type walSeqKey struct {
	segmentID uint32
	offset    int64
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
func (lsm *LSM) NewMemtable() *memTable {
	newFid := lsm.levels.maxFID.Add(1)
	utils.Panic(lsm.wal.SwitchSegment(uint32(newFid), true))
	return &memTable{
		lsm:       lsm,
		segmentID: uint32(newFid),
		index:     newMemIndex(lsm.option),
	}
}

func (m *memTable) close() error {
	if m == nil {
		return nil
	}
	// Release the keyWalOffset side map and range tombstone cache to prevent
	// unbounded memory growth after flush. Range tombstones have already been
	// transferred to rtCollector by the flush path before close is called.
	m.keyWalOffset.Clear()
	m.rtMu.Lock()
	m.rangeTombstones = nil
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
		m.keyWalOffset.Store(string(entry.Key), walSeqKey{segmentID: m.segmentID, offset: info.Offset})
		if entry.IsRangeDelete() {
			_, start, version, ok := kv.SplitInternalKey(entry.Key)
			if ok {
				m.rtMu.Lock()
				m.rangeTombstones = append(m.rangeTombstones, memRangeTombstone{
					cf:        entry.CF,
					start:     kv.SafeCopy(nil, start),
					end:       kv.SafeCopy(nil, entry.RangeEnd()),
					version:   version,
					segmentID: m.segmentID,
					walOffset: info.Offset,
				})
				m.rtMu.Unlock()
			}
		}
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
			// Record the sequence key for this entry. ValueStruct.Version is not
			// serialized by the arena, so we maintain a side map instead.
			m.keyWalOffset.Store(string(entry.Key), walSeqKey{segmentID: m.segmentID, offset: info.Offset})
			if entry.IsRangeDelete() {
				_, start, version, ok := kv.SplitInternalKey(entry.Key)
				if !ok {
					continue
				}
				rt := memRangeTombstone{
					cf:        entry.CF,
					start:     kv.SafeCopy(nil, start),
					end:       kv.SafeCopy(nil, entry.RangeEnd()),
					version:   version,
					segmentID: m.segmentID,
					walOffset: info.Offset,
				}
				m.rtMu.Lock()
				m.rangeTombstones = append(m.rangeTombstones, rt)
				m.rtMu.Unlock()
			}
		}
	}
	return nil
}

// recovery rebuilds memtables from existing WAL segments.
func (lsm *LSM) recovery() (*memTable, []*memTable) {
	files, err := lsm.wal.ListSegments()
	if err != nil {
		utils.Panic(err)
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
			utils.Panic(err)
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
				if !lsm.levels.canRemoveWalSegment(uint32(fid)) {
					cleaned = append(cleaned, fid)
					continue
				}
				if err := lsm.wal.RemoveSegment(uint32(fid)); err != nil && !os.IsNotExist(err) {
					_ = utils.Err(errors.Wrapf(err, "remove wal segment %d", fid))
				}
				continue
			}
			cleaned = append(cleaned, fid)
		}
		fids = cleaned
	}

	if len(fids) == 0 {
		lsm.levels.maxFID.Store(maxFid)
		return lsm.NewMemtable(), nil
	}

	tables := make([]*memTable, 0, len(fids))
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		utils.CondPanic(err != nil, err)
		if mt.Size() == 0 {
			continue
		}
		tables = append(tables, mt)
	}
	if len(tables) == 0 {
		lsm.levels.maxFID.Store(maxFid)
		return lsm.NewMemtable(), nil
	}

	lsm.levels.maxFID.Store(maxFid)
	active := tables[len(tables)-1]
	tables = tables[:len(tables)-1]
	utils.Panic(lsm.wal.SwitchSegment(active.segmentID, false))
	return active, tables
}

func (lsm *LSM) openMemTable(fid uint64) (*memTable, error) {
	mt := &memTable{
		lsm:       lsm,
		segmentID: uint32(fid),
		index:     newMemIndex(lsm.option),
	}
	err := lsm.wal.ReplaySegment(uint32(fid), func(info wal.EntryInfo, payload []byte) error {
		applyEntry := func(entry *kv.Entry, offset int64) {
			if ts := kv.Timestamp(entry.Key); ts > mt.maxVersion {
				mt.maxVersion = ts
			}
			if mt.index != nil {
				mt.index.Add(entry)
			}
			mt.keyWalOffset.Store(string(entry.Key), walSeqKey{segmentID: mt.segmentID, offset: offset})
			if entry.IsRangeDelete() {
				_, start, version, ok := kv.SplitInternalKey(entry.Key)
				if ok {
					mt.rtMu.Lock()
					mt.rangeTombstones = append(mt.rangeTombstones, memRangeTombstone{
						cf:        entry.CF,
						start:     kv.SafeCopy(nil, start),
						end:       kv.SafeCopy(nil, entry.RangeEnd()),
						version:   version,
						segmentID: mt.segmentID,
						walOffset: offset,
					})
					mt.rtMu.Unlock()
				}
			}
			entry.DecrRef()
		}
		switch info.Type {
		case wal.RecordTypeEntry:
			entry, err := kv.DecodeEntry(payload)
			if err != nil {
				return err
			}
			applyEntry(entry, info.Offset)
		case wal.RecordTypeEntryBatch:
			entries, err := wal.DecodeEntryBatch(payload)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				applyEntry(entry, info.Offset)
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
		panic("lsm: memtable reservation underflow")
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

// isKeyCoveredByRangeTombstone checks if userKey in cf is covered by any
// range tombstone. A tombstone covers an entry if:
// 1. tombstone version > entry version (tombstone is definitely newer), OR
// 2. tombstone version == entry version AND tombstone was written after entry (WAL sequence)
// When entryVersion is 0, we can't determine version relationship, so rely on WAL sequence only.
func (m *memTable) isKeyCoveredByRangeTombstone(cf kv.ColumnFamily, userKey []byte, entryVersion uint64, entrySeq walSeqKey, seqFound bool) bool {
	if m == nil {
		return false
	}
	m.rtMu.RLock()
	rts := m.rangeTombstones
	m.rtMu.RUnlock()
	for i := range rts {
		rt := &rts[i]
		if rt.cf != cf {
			continue
		}
		// Check version relationship if both versions are non-zero
		if entryVersion > 0 && rt.version > 0 {
			if rt.version < entryVersion {
				continue // Tombstone is older, doesn't cover
			}
			if rt.version > entryVersion {
				// Tombstone is newer, covers if key is in range
				if kv.KeyInRange(userKey, rt.start, rt.end) {
					return true
				}
				continue
			}
		}
		// Same version or can't determine from version: check WAL sequence
		// A tombstone covers this entry only if it was written after the entry.
		// When seqFound is false we don't know the ordering: all tombstones apply.
		if seqFound && !seqAfter(rt.segmentID, rt.walOffset, entrySeq) {
			continue
		}
		if kv.KeyInRange(userKey, rt.start, rt.end) {
			return true
		}
	}
	return false
}

// seqAfter reports whether (seg, off) is strictly after entry.
// Higher segmentID is always newer; within the same segment, higher offset is newer.
func seqAfter(seg uint32, off int64, entry walSeqKey) bool {
	if seg != entry.segmentID {
		return seg > entry.segmentID
	}
	return off > entry.offset
}

// walSeqForKey returns the composite write sequence key of the most recent
// write for internalKey within this memtable, or a zero walSeqKey and false.
func (m *memTable) walSeqForKey(internalKey []byte) (walSeqKey, bool) {
	if m == nil {
		return walSeqKey{}, false
	}
	if v, ok := m.keyWalOffset.Load(string(internalKey)); ok {
		return v.(walSeqKey), true
	}
	return walSeqKey{}, false
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
