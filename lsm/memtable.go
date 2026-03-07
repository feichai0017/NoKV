package lsm

import (
	"bytes"
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
	Search([]byte) kv.ValueStruct
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
	walSize    int64

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

var walBufferPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

const walBufferMaxReuse = 1 << 22 // 4 MiB guard to avoid retaining huge buffers unnecessarily.

func getWalBuffer() *bytes.Buffer {
	if bufAny := walBufferPool.Get(); bufAny != nil {
		buf := bufAny.(*bytes.Buffer)
		buf.Reset()
		return buf
	}
	return &bytes.Buffer{}
}

func putWalBuffer(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	if buf.Cap() > walBufferMaxReuse {
		return
	}
	buf.Reset()
	walBufferPool.Put(buf)
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
	newFid := atomic.AddUint64(&(lsm.levels.maxFID), 1)
	utils.Panic(lsm.wal.SwitchSegment(uint32(newFid), true))
	return &memTable{
		lsm:       lsm,
		segmentID: uint32(newFid),
		index:     newMemIndex(lsm.option),
		walSize:   0,
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
	entries := [1]*kv.Entry{entry}
	return m.setBatch(entries[:])
}

// Get reads key from the memtable index and returns a pooled entry wrapper.
func (m *memTable) Get(key []byte) (*kv.Entry, error) {
	var vs kv.ValueStruct
	if m.index != nil {
		vs = m.index.Search(key)
	}
	e := kv.EntryPool.Get().(*kv.Entry)
	e.Key = key
	e.Value = vs.Value
	e.ExpiresAt = vs.ExpiresAt
	e.Meta = vs.Meta
	e.Version = vs.Version
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
	payloads := make([][]byte, 0, len(entries))
	buffers := make([]*bytes.Buffer, 0, len(entries))
	releaseBuffers := func() {
		for _, buf := range buffers {
			putWalBuffer(buf)
		}
	}
	for _, entry := range entries {
		buf := getWalBuffer()
		payload, err := kv.EncodeEntry(buf, entry)
		if err != nil {
			putWalBuffer(buf)
			releaseBuffers()
			return err
		}
		payloads = append(payloads, payload)
		buffers = append(buffers, buf)
	}
	infos, err := m.lsm.wal.Append(payloads...)
	releaseBuffers()
	if err != nil {
		return err
	}
	for _, info := range infos {
		atomic.AddInt64(&m.walSize, int64(info.Length)+8)
	}
	// Build a map from entry index → WAL offset for range tombstone sequencing.
	// infos[i] corresponds to payloads[i] which corresponds to entries[i].
	if m.index != nil {
		for i, entry := range entries {
			var offset int64
			if i < len(infos) {
				offset = infos[i].Offset
			}
			m.index.Add(entry)
			// Record the sequence key for this entry. ValueStruct.Version is not
			// serialized by the arena, so we maintain a side map instead.
			m.keyWalOffset.Store(string(entry.Key), walSeqKey{segmentID: m.segmentID, offset: offset})
			if entry.IsRangeDelete() {
				_, start, _ := kv.SplitInternalKey(entry.Key)
				rt := memRangeTombstone{
					cf:        entry.CF,
					start:     kv.SafeCopy(nil, start),
					end:       kv.SafeCopy(nil, entry.RangeEnd()),
					segmentID: m.segmentID,
					walOffset: offset,
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
	maxFid := lsm.levels.maxFID
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
		lsm.levels.maxFID = maxFid
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
		lsm.levels.maxFID = maxFid
		return lsm.NewMemtable(), nil
	}

	lsm.levels.maxFID = maxFid
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
		if info.Type != wal.RecordTypeEntry {
			return nil
		}
		entry, err := kv.DecodeEntry(payload)
		if err != nil {
			return err
		}
		if ts := kv.ParseTs(entry.Key); ts > mt.maxVersion {
			mt.maxVersion = ts
		}
		if mt.index != nil {
			mt.index.Add(entry)
		}
		mt.keyWalOffset.Store(string(entry.Key), walSeqKey{segmentID: mt.segmentID, offset: info.Offset})
		if entry.IsRangeDelete() {
			_, start, version := kv.SplitInternalKey(entry.Key)
			mt.rangeTombstones = append(mt.rangeTombstones, memRangeTombstone{
				cf:        entry.CF,
				start:     kv.SafeCopy(nil, start),
				end:       kv.SafeCopy(nil, entry.RangeEnd()),
				version:   version,
				segmentID: mt.segmentID,
				walOffset: info.Offset,
			})
		}
		entry.DecrRef()
		atomic.AddInt64(&mt.walSize, int64(info.Length)+8)
		return nil
	})
	if err != nil {
		return nil, errors.WithMessage(err, "while updating skiplist")
	}
	return mt, nil
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
		return utils.NewSkiplist(arenaSizeFor(0))
	}
	switch opt.MemTableEngine {
	case "art":
		return utils.NewART(arenaSizeFor(opt.MemTableSize))
	case "", "skiplist":
		fallthrough
	default:
		return utils.NewSkiplist(arenaSizeFor(opt.MemTableSize))
	}
}
