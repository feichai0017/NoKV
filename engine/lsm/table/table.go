package table

import (
	"bytes"
	stderrors "errors"
	"expvar"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/file"
	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	cachepkg "github.com/feichai0017/NoKV/engine/lsm/cache"
	"github.com/feichai0017/NoKV/engine/lsm/rangefilter"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/utils"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	storagepb "github.com/feichai0017/NoKV/pb/storage"
)

var (
	prefetchLaunched  = expvar.NewInt("NoKV.Prefetch.Launched")
	prefetchAborted   = expvar.NewInt("NoKV.Prefetch.Aborted")
	prefetchCompleted = expvar.NewInt("NoKV.Prefetch.Completed")
)

// Table is one open SSTable plus its cached metadata.
type Table struct {
	rt   Runtime
	opts Options
	fid  uint64
	utils.RefCount // For file garbage collection. Atomic.
	lvl atomic.Int32

	minKey []byte
	maxKey []byte
	size   int64

	createdAt     time.Time
	staleDataSize uint32
	valueSize     uint64
	rangeDeletes  uint32
	keyCount      uint32
	maxVersion    uint64
	hasBloom      bool

	idx atomic.Pointer[storagepb.TableIndex]

	mu   sync.Mutex
	ss   *file.SSTable
	pins int32
}

// Open returns a Table backed by tableName. When builder is non-nil the
// builder is flushed to disk first; pass nil to open an existing SST file.
func Open(rt Runtime, tableName string, builder *Builder) (out *Table, err error) {
	defer func() {
		if r := recover(); r != nil {
			if out != nil && out.ss != nil {
				_ = out.ss.Close()
			}
			out = nil
			err = fmt.Errorf("open table %s panic: %v", tableName, r)
		}
	}()

	if rt == nil {
		return nil, errors.New("table.Open: nil runtime")
	}
	opts := rt.Options()
	sstSize := int(opts.SSTableMaxSize)
	var (
		t       *Table
		openErr error
	)
	fid := vfs.FID(tableName)
	// if builder is not nil, flush the buffer to disk
	if builder != nil {
		if t, openErr = builder.flush(rt, opts, tableName, nil); openErr != nil {
			return nil, openErr
		}
	} else {
		t = &Table{rt: rt, opts: opts, fid: fid}
		// if builder is nil, open an existing sst file
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      opts.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    sstSize,
			FS:       opts.FS})
	}
	if t == nil || t.ss == nil {
		return nil, fmt.Errorf("open table %s: nil sstable handle", tableName)
	}
	if err := t.ss.Init(); err != nil {
		_ = t.ss.Close()
		return nil, err
	}
	// first reference, otherwise the reference state will be incorrect
	t.IncrRef()

	idx := t.ss.Indexs()
	if idx != nil {
		t.idx.Store(idx)
		t.keyCount = idx.GetKeyCount()
		t.maxVersion = idx.GetMaxVersion()
		t.staleDataSize = idx.GetStaleDataSize()
		t.valueSize = idx.GetValueSize()
		t.rangeDeletes = idx.GetRangeTombstoneCount()
		if c := t.cache(); c != nil {
			c.AddIndex(t.fid, idx)
		}
	}
	t.hasBloom = t.ss.HasBloomFilter()
	t.size = t.ss.Size()
	if created := t.ss.GetCreatedAt(); created != nil {
		t.createdAt = *created
	}
	t.minKey = kv.SafeCopy(nil, t.ss.MinKey())

	// get the max key of sst, need to use iterator
	itr := t.NewIterator(&index.Options{}) // default is descending
	defer func() { _ = itr.Close() }()
	itr.Rewind()
	if !itr.Valid() {
		t.maxKey = kv.SafeCopy(nil, t.minKey)
		return t, nil
	}
	item := itr.Item()
	if item == nil || item.Entry() == nil {
		t.maxKey = kv.SafeCopy(nil, t.minKey)
		return t, nil
	}
	maxKey := append([]byte(nil), item.Entry().Key...)
	t.maxKey = kv.SafeCopy(nil, maxKey)
	t.ss.SetMaxKey(maxKey)

	out = t
	return out, nil
}

// cache returns the runtime's cache, or nil.
func (t *Table) cache() *cachepkg.Cache {
	if t == nil || t.rt == nil {
		return nil
	}
	return t.rt.Cache()
}

func (t *Table) index() *storagepb.TableIndex {
	if t == nil {
		return nil
	}
	if idx := t.idx.Load(); idx != nil {
		return idx
	}
	if c := t.cache(); c != nil {
		if cached, ok := c.GetIndex(t.fid); ok {
			t.idx.Store(cached)
			return cached
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if idx := t.idx.Load(); idx != nil {
		return idx
	}
	if err := t.openSSTableLocked(true); err != nil {
		slog.Default().Error("open sstable", "fid", t.fid, "error", err)
		return nil
	}
	idx := t.ss.Indexs()
	if idx != nil {
		t.idx.Store(idx)
		if c := t.cache(); c != nil {
			c.AddIndex(t.fid, idx)
		}
		t.keyCount = idx.GetKeyCount()
		t.maxVersion = idx.GetMaxVersion()
		t.staleDataSize = idx.GetStaleDataSize()
		t.valueSize = idx.GetValueSize()
		t.rangeDeletes = idx.GetRangeTombstoneCount()
	}
	return idx
}

// FID returns the SSTable file id.
func (t *Table) FID() uint64 { return t.fid }

// CreatedAt returns the table's creation timestamp (zero if unknown).
func (t *Table) CreatedAt() time.Time { return t.createdAt }

// MinKey returns the smallest user key stored in this SSTable.
func (t *Table) MinKey() []byte { return t.minKey }

// MaxKey returns the largest user key stored in this SSTable.
func (t *Table) MaxKey() []byte { return t.maxKey }

// KeyCount returns the approximate number of keys indexed by this table. It
// will lazy-load the index from disk if not yet cached.
func (t *Table) KeyCount() uint32 {
	if t.keyCount != 0 {
		return t.keyCount
	}
	if idx := t.index(); idx != nil {
		t.keyCount = idx.GetKeyCount()
		return t.keyCount
	}
	return 0
}

// CachedKeyCount returns the cached key count without triggering a disk load.
func (t *Table) CachedKeyCount() uint32 {
	if t == nil {
		return 0
	}
	return t.keyCount
}

// MaxVersionVal returns the maximum MVCC version recorded in this table index.
func (t *Table) MaxVersionVal() uint64 {
	if t.maxVersion != 0 {
		return t.maxVersion
	}
	if idx := t.index(); idx != nil {
		t.maxVersion = idx.GetMaxVersion()
		return t.maxVersion
	}
	return 0
}

// HasBloomFilter reports whether this table carries a bloom filter in its index.
func (t *Table) HasBloomFilter() bool {
	if t.hasBloom {
		return true
	}
	if idx := t.index(); idx != nil && len(idx.BloomFilter) > 0 {
		t.hasBloom = true
		return true
	}
	return false
}

func (t *Table) blockOffset(i int) (*storagepb.BlockOffset, bool) {
	idx := t.index()
	if idx == nil {
		return nil, false
	}
	offsets := idx.GetOffsets()
	if i < 0 || i > len(offsets) {
		return nil, false
	}
	if i == len(offsets) {
		return nil, true
	}
	return offsets[i], true
}

// Size returns the SSTable file size in bytes.
func (t *Table) Size() int64 { return t.size }

// GetCreatedAt returns the table's creation timestamp, or nil if unknown.
func (t *Table) GetCreatedAt() *time.Time {
	if t.createdAt.IsZero() {
		return nil
	}
	created := t.createdAt
	return &created
}

// StaleDataSize is the amount of stale data (droppable by compaction) in this SST.
func (t *Table) StaleDataSize() uint32 { return t.staleDataSize }

// ValueSize reports total inline value bytes referenced by this table.
func (t *Table) ValueSize() uint64 { return t.valueSize }

// RangeTombstoneCount reports range deletion markers stored in this table.
func (t *Table) RangeTombstoneCount() uint32 { return t.rangeDeletes }

// Search looks up key with bloom-filter prefilter. An entry is returned only if its
// version is strictly greater than *maxVs; on success *maxVs is advanced to the matched version.
func (t *Table) Search(key []byte, maxVs *uint64) (entry *kv.Entry, err error) {
	t.IncrRef()
	defer func() {
		_ = t.DecrRef()
	}()
	idx := t.index()
	if idx == nil {
		return nil, errors.New("table index missing")
	}
	if bloomFilter := utils.Filter(idx.BloomFilter); len(bloomFilter) > 0 {
		utils.CondPanicFunc(len(key) <= 8, func() error {
			return fmt.Errorf("table.Search expects internal key: %x", key)
		})
		probe := kv.InternalToBaseKey(key)
		if t.prefixBloomMiss(idx, probe) {
			return nil, utils.ErrKeyNotFound
		}
		if !bloomFilter.MayContainKey(probe) {
			return nil, utils.ErrKeyNotFound
		}
	}
	return t.searchPointWithIndex(idx, key, maxVs)
}

// SearchExactCandidate is like Search but skips bloom filtering.
func (t *Table) SearchExactCandidate(key []byte, maxVs *uint64) (entry *kv.Entry, err error) {
	t.IncrRef()
	defer func() {
		_ = t.DecrRef()
	}()
	idx := t.index()
	if idx == nil {
		return nil, errors.New("table index missing")
	}
	return t.searchPointWithIndex(idx, key, maxVs)
}

func (t *Table) searchPointWithIndex(idx *storagepb.TableIndex, key []byte, maxVs *uint64) (entry *kv.Entry, err error) {
	offsets := idx.GetOffsets()
	if len(offsets) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	blockIdx := searchFirstBlockWithBaseKeyGT(offsets, key)
	if blockIdx == 0 {
		return t.searchPointInBlock(0, key, maxVs)
	}
	entry, err = t.searchPointInBlock(blockIdx-1, key, maxVs)
	if err == nil || err != utils.ErrKeyNotFound || blockIdx >= len(offsets) {
		return entry, err
	}
	return t.searchPointInBlock(blockIdx, key, maxVs)
}

func (t *Table) prefixBloomMiss(idx *storagepb.TableIndex, baseKey []byte) bool {
	if t == nil || t.opts.PrefixExtractor == nil || idx == nil {
		return false
	}
	filter := utils.Filter(idx.GetPrefixBloomFilter())
	if len(filter) == 0 {
		return false
	}
	_, userKey, ok := kv.SplitBaseKey(baseKey)
	if !ok {
		return false
	}
	prefix := t.opts.PrefixExtractor(userKey)
	if len(prefix) == 0 {
		return false
	}
	return !filter.MayContainKey(prefix)
}

func (t *Table) searchPointInBlock(blockIdx int, key []byte, maxVs *uint64) (*kv.Entry, error) {
	block, err := t.loadBlock(blockIdx)
	if err != nil {
		return nil, err
	}
	bi := getBlockIterator()
	defer func() {
		_ = bi.Close()
		putBlockIterator(bi)
	}()
	bi.tableID = t.fid
	bi.blockID = blockIdx
	bi.isAsc = true
	bi.setBlock(block)
	bi.seek(key)
	if !bi.Valid() {
		if err := bi.Error(); err != nil && err != io.EOF {
			return nil, err
		}
		return nil, utils.ErrKeyNotFound
	}
	item := bi.Item()
	if item == nil || item.Entry() == nil {
		return nil, utils.ErrKeyNotFound
	}
	if e := item.Entry(); kv.SameBaseKey(key, e.Key) {
		if version := kv.Timestamp(e.Key); *maxVs < version {
			*maxVs = version
			buf := make([]byte, len(e.Key)+len(e.Value))
			keyCopy := buf[:len(e.Key)]
			copy(keyCopy, e.Key)
			valueCopy := buf[len(e.Key):]
			copy(valueCopy, e.Value)
			clone := kv.NewEntry(keyCopy, valueCopy)
			clone.ExpiresAt = e.ExpiresAt
			clone.Meta = e.Meta
			_ = clone.PopulateInternalMeta()
			clone.Offset = e.Offset
			clone.Hlen = e.Hlen
			clone.ValThreshold = e.ValThreshold
			return clone, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (t *Table) loadBlock(idx int) (*block, error) {
	utils.CondPanicFunc(idx < 0, func() error { return fmt.Errorf("idx=%d", idx) })
	tableIndex := t.index()
	if tableIndex == nil {
		return nil, errors.New("missing table index")
	}
	offsets := tableIndex.GetOffsets()
	if idx >= len(offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	key := t.blockCacheKey(idx)
	lvl := t.Level()
	ko, ok := t.blockOffset(idx)
	utils.CondPanicFunc(!ok || ko == nil, func() error { return fmt.Errorf("block t.offset id=%d", idx) })
	if c := t.cache(); c != nil {
		if cached, ok := c.GetBlock(lvl, key); ok && cached != nil {
			return t.decodeCachedBlock(ko, cached)
		}
	}

	b = &block{
		offset: int(ko.GetOffset()),
		tbl:    t,
	}

	var err error
	if b.diskData, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.fid, b.offset, ko.GetLen())
	}
	b.diskEnd = len(b.diskData)
	b.compression = Compression(ko.GetCompression())
	b.rawLen = int(ko.GetRawLen())
	if b.data, err = decodeBlockPayload(b.diskData, b.compression, b.rawLen); err != nil {
		return nil, err
	}
	if err := decodeBlockMetadata(b); err != nil {
		return nil, err
	}

	if c := t.cache(); c != nil {
		c.AddBlock(lvl, t, key, cachepkg.Block{
			DiskData:    b.diskData,
			Compression: uint32(b.compression),
			RawLen:      b.rawLen,
		})
	}

	return b, nil
}

func (t *Table) decodeCachedBlock(ko *storagepb.BlockOffset, cached *cachepkg.Entry) (*block, error) {
	if cached == nil {
		return nil, errors.New("nil cached block")
	}
	data, err := decodeBlockPayload(cached.DiskData, Compression(cached.Compression), cached.RawLen)
	if err != nil {
		return nil, err
	}
	b := &block{
		offset:      int(ko.GetOffset()),
		tbl:         t,
		data:        data,
		diskData:    cached.DiskData,
		diskEnd:     len(cached.DiskData),
		compression: Compression(cached.Compression),
		rawLen:      cached.RawLen,
	}
	if err := decodeBlockMetadata(b); err != nil {
		return nil, err
	}
	return b, nil
}

func decodeBlockMetadata(b *block) error {
	if b == nil {
		return errors.New("nil block")
	}
	if len(b.data) < 4 {
		return errors.New("block data too small")
	}
	readPos := len(b.data) - 4 // First read checksum length.
	b.chkLen = int(kv.BytesToU32(b.data[readPos : readPos+4]))

	if b.chkLen > len(b.data) || readPos < b.chkLen {
		return errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	b.data = b.data[:readPos]

	if err := b.verifyCheckSum(); err != nil {
		return err
	}

	readPos -= 4
	if readPos < 0 {
		return errors.New("invalid block offsets")
	}
	numEntries := int(kv.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4
	if entriesIndexStart < 0 || entriesIndexEnd > len(b.data) {
		return errors.New("invalid block entry offsets")
	}

	b.entryOffsets = kv.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.entriesIndexStart = entriesIndexStart
	return nil
}

func (t *Table) read(off, sz int) ([]byte, error) {
	ss, release, err := t.pinSSTable()
	if err != nil {
		return nil, err
	}
	defer release()
	return ss.Bytes(off, sz)
}

func decodeBlockPayload(payload []byte, compression Compression, rawLen int) ([]byte, error) {
	switch compression {
	case CompressionNone:
		return payload, nil
	case CompressionSnappy:
		decoded, err := snappy.Decode(nil, payload)
		if err != nil {
			return nil, err
		}
		if rawLen > 0 && len(decoded) != rawLen {
			return nil, fmt.Errorf("snappy block length mismatch: got=%d want=%d", len(decoded), rawLen)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unknown block compression %d", compression)
	}
}

const maxUint32 = uint64(math.MaxUint32)

// blockCacheKey is used to store blocks in the block cache.
func (t *Table) blockCacheKey(idx int) uint64 {
	utils.CondPanicFunc(t.fid > maxUint32, func() error { return fmt.Errorf("table fid %d exceeds 32-bit limit", t.fid) })
	utils.CondPanicFunc(idx < 0 || uint64(idx) > maxUint32, func() error { return fmt.Errorf("invalid block index %d", idx) })
	return (t.fid << 32) | uint64(uint32(idx))
}

// Iterator owns a positioned scan over one Table. Use NewIterator to construct.
type Iterator struct {
	it           index.Item
	opt          *index.Options
	t            *Table
	blockPos     int
	blockStart   int
	blockEnd     int
	lowerUser    []byte
	upperUser    []byte
	bi           *blockIterator
	err          error
	index        *storagepb.TableIndex
	closeCh      chan struct{}
	wg           sync.WaitGroup
	closeOnce    sync.Once
	prefetchRing *utils.Ring[int]
	prefetchPool *utils.Pool
}

func (it *Iterator) fetchBlock(idx int) (*block, error) {
	return it.t.loadBlock(idx)
}

func (it *Iterator) hasBlockRange() bool {
	return it.index != nil && it.blockEnd > it.blockStart
}

func (it *Iterator) inBlockRange(idx int) bool {
	return idx >= it.blockStart && idx < it.blockEnd
}

func (it *Iterator) prefetchNext(idx int) {
	if it.opt == nil || !it.opt.IsAsc || it.opt.PrefetchBlocks <= 0 || it.prefetchRing == nil {
		return
	}
	if !it.hasBlockRange() {
		return
	}
	limit := it.blockEnd
	for n := 1; n <= it.opt.PrefetchBlocks; n++ {
		next := idx + n
		if next >= limit {
			return
		}
		select {
		case <-it.closeCh:
			return
		default:
			if ok := it.prefetchRing.Push(next); ok {
				prefetchLaunched.Add(1)
			} else {
				prefetchAborted.Add(1)
				return
			}
		}
	}
}

// NewIterator opens a table iterator with optional prefetch behavior.
func (t *Table) NewIterator(options *index.Options) index.Iterator {
	t.IncrRef()
	if options == nil {
		options = &index.Options{IsAsc: true}
	}
	idx := t.index()
	blockStart, blockEnd := blockRangeForBounds(idx, options.LowerBound, options.UpperBound)

	it := &Iterator{
		opt:        options,
		t:          t,
		bi:         getBlockIterator(),
		index:      idx,
		blockStart: blockStart,
		blockEnd:   blockEnd,
		lowerUser:  rangefilter.GuideUserKey(options.LowerBound),
		upperUser:  rangefilter.GuideUserKey(options.UpperBound),
	}

	if options.PrefetchBlocks > 0 {
		t.adviseIterator(options)

		if options.IsAsc {
			it.closeCh = make(chan struct{})
			it.prefetchRing = utils.NewRing[int](options.PrefetchBlocks)
			workers := options.PrefetchWorkers
			if workers <= 0 {
				workers = min(options.PrefetchBlocks, 4)
			}
			it.prefetchPool = utils.NewPool(workers, "IteratorPrefetch")
			it.wg.Go(func() {
				for {
					select {
					case <-it.closeCh:
						return
					default:
						idx, ok := it.prefetchRing.Pop()
						if !ok {
							runtime.Gosched()
							continue
						}
						if err := it.prefetchPool.Submit(func() {
							it.t.IncrRef()
							if _, err := it.t.loadBlock(idx); err == nil {
								prefetchCompleted.Add(1)
							} else {
								prefetchAborted.Add(1)
							}
							_ = it.t.DecrRef()
						}); err != nil {
							prefetchAborted.Add(1)
						}
					}
				}
			})
		}
	}

	return it
}

// adviseIterator is an optional helper to issue madvise hints for long scans.
func (t *Table) adviseIterator(options *index.Options) {
	if options == nil {
		return
	}
	pattern := options.AccessPattern
	if pattern == utils.AccessPatternAuto {
		if options.IsAsc {
			pattern = utils.AccessPatternSequential
		} else {
			pattern = utils.AccessPatternRandom
		}
	}
	if pattern == utils.AccessPatternAuto {
		return
	}
	if ss, release, err := t.pinSSTable(); err == nil {
		_ = ss.Advise(pattern)
		release()
	}
}

// Next advances to the next key within the current block or next block.
func (it *Iterator) Next() {
	if !it.hasBlockRange() {
		it.it = nil
		it.err = io.EOF
		return
	}
	if len(it.bi.data) == 0 {
		if it.opt.IsAsc {
			it.seekToFirst()
		} else {
			it.seekToLast()
		}
		return
	}
	it.bi.Next()
	it.advanceToBoundedValid()
}

// Valid reports whether table iterator has a readable current item.
func (it *Iterator) Valid() bool {
	return it.err == nil
}

// Rewind resets iterator position to the first/last key by scan direction.
func (it *Iterator) Rewind() {
	if it.opt.IsAsc {
		it.seekToFirst()
	} else {
		it.seekToLast()
	}
}

// Item returns the current table iterator item.
func (it *Iterator) Item() index.Item {
	return it.it
}

// Close releases block iterators, prefetch workers, and table references.
func (it *Iterator) Close() error {
	it.closeOnce.Do(func() {
		if it.closeCh != nil {
			close(it.closeCh)
		}
		if it.prefetchRing != nil {
			it.prefetchRing.Close()
		}
		it.wg.Wait()
		if it.prefetchPool != nil {
			it.prefetchPool.Release()
		}
	})
	_ = it.bi.Close()
	putBlockIterator(it.bi)
	it.bi = nil
	return it.t.DecrRef()
}

func (it *Iterator) seekToFirst() {
	if !it.hasBlockRange() {
		it.err = io.EOF
		it.it = nil
		return
	}
	it.blockPos = it.blockStart
	block, err := it.fetchBlock(it.blockPos)
	if err != nil {
		it.err = err
		it.it = nil
		return
	}
	it.prefetchNext(it.blockPos)
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.isAsc = it.opt.IsAsc
	it.bi.setBlock(block)
	if len(it.lowerUser) > 0 {
		it.bi.seek(kv.InternalKey(kv.CFDefault, it.lowerUser, kv.MaxVersion))
	} else {
		it.bi.seekToFirst()
	}
	it.advanceToBoundedValid()
}

func (it *Iterator) seekToLast() {
	if !it.hasBlockRange() {
		it.err = io.EOF
		it.it = nil
		return
	}
	it.blockPos = it.blockEnd - 1
	block, err := it.fetchBlock(it.blockPos)
	if err != nil {
		it.err = err
		it.it = nil
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.isAsc = it.opt.IsAsc
	it.bi.setBlock(block)
	if len(it.upperUser) > 0 {
		it.bi.seek(kv.InternalKey(kv.CFDefault, it.upperUser, 0))
	} else {
		it.bi.seekToLast()
	}
	it.advanceToBoundedValid()
}

// searchFirstBlockWithBaseKeyGT returns the first block index whose base key is > key.
func searchFirstBlockWithBaseKeyGT(offsets []*storagepb.BlockOffset, key []byte) int {
	lo, hi := 0, len(offsets)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if kv.CompareInternalKeys(offsets[mid].GetKey(), key) > 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func searchFirstBlockWithBaseKeyGE(offsets []*storagepb.BlockOffset, baseKey []byte) int {
	lo, hi := 0, len(offsets)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if compareOffsetBaseKey(offsets[mid].GetKey(), baseKey) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func compareOffsetBaseKey(internalKey, baseKey []byte) int {
	if len(internalKey) > 8 {
		internalKey = internalKey[:len(internalKey)-8]
	}
	return bytes.Compare(internalKey, baseKey)
}

func blockRangeForBounds(index *storagepb.TableIndex, lower, upper []byte) (int, int) {
	if index == nil {
		return 0, 0
	}
	offsets := index.GetOffsets()
	if len(offsets) == 0 {
		return 0, 0
	}

	start := 0
	end := len(offsets)

	if len(lower) > 0 {
		lower = rangefilter.GuideBaseKey(lower)
		idx := searchFirstBlockWithBaseKeyGE(offsets, lower)
		switch {
		case idx == 0:
			start = 0
		case idx >= len(offsets):
			start = len(offsets) - 1
		case compareOffsetBaseKey(offsets[idx].GetKey(), lower) == 0:
			start = idx
		default:
			start = idx - 1
		}
	}
	if len(upper) > 0 {
		upper = rangefilter.GuideBaseKey(upper)
		end = searchFirstBlockWithBaseKeyGE(offsets, upper)
	}
	if end < start {
		end = start
	}
	return start, end
}

// Seek positions the iterator at the appropriate entry for the given key.
func (it *Iterator) Seek(key []byte) {
	if !it.hasBlockRange() {
		it.err = io.EOF
		it.it = nil
		return
	}
	offsets := it.index.GetOffsets()[it.blockStart:it.blockEnd]
	if len(offsets) == 0 {
		it.err = io.EOF
		it.it = nil
		return
	}
	idx := searchFirstBlockWithBaseKeyGT(offsets, key)

	if it.opt.IsAsc {
		if idx == 0 {
			it.seekHelper(it.blockStart, key)
			return
		}
		it.seekHelper(it.blockStart+idx-1, key)
		if it.err == io.EOF && idx < len(offsets) {
			it.seekHelper(it.blockStart+idx, key)
		}
		return
	}
	if idx == 0 {
		it.err = io.EOF
		it.it = nil
		return
	}
	it.seekHelper(it.blockStart+idx-1, key)
}

func (it *Iterator) seekHelper(blockIdx int, key []byte) {
	if !it.inBlockRange(blockIdx) {
		it.err = io.EOF
		it.it = nil
		return
	}
	it.blockPos = blockIdx
	block, err := it.fetchBlock(blockIdx)
	if err != nil {
		it.err = err
		it.it = nil
		return
	}
	it.prefetchNext(blockIdx)
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.isAsc = it.opt.IsAsc
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.advanceToBoundedValid()
}

func iteratorUserKey(key []byte) []byte {
	if len(key) <= 8 {
		return nil
	}
	_, userKey, ok := kv.SplitBaseKey(kv.InternalToBaseKey(key))
	if !ok {
		return nil
	}
	return userKey
}

func (it *Iterator) advanceToBoundedValid() {
	for {
		if !it.inBlockRange(it.blockPos) {
			it.err = io.EOF
			it.it = nil
			return
		}
		if !it.bi.Valid() {
			if err := it.bi.Error(); err != nil && err != io.EOF {
				it.err = err
				it.it = nil
				return
			}
			if it.opt.IsAsc {
				it.blockPos++
			} else {
				it.blockPos--
			}
			if !it.inBlockRange(it.blockPos) {
				it.err = io.EOF
				it.it = nil
				return
			}
			block, err := it.fetchBlock(it.blockPos)
			if err != nil {
				it.err = err
				it.it = nil
				return
			}
			it.prefetchNext(it.blockPos)
			it.bi.tableID = it.t.fid
			it.bi.blockID = it.blockPos
			it.bi.isAsc = it.opt.IsAsc
			it.bi.setBlock(block)
			if it.opt.IsAsc {
				it.bi.seekToFirst()
			} else {
				it.bi.seekToLast()
			}
			continue
		}

		item := it.bi.Item()
		if item == nil || item.Entry() == nil {
			it.err = io.EOF
			it.it = nil
			return
		}
		userKey := iteratorUserKey(item.Entry().Key)
		if len(userKey) == 0 {
			it.err = io.EOF
			it.it = nil
			return
		}
		if it.opt.IsAsc && len(it.upperUser) > 0 && bytes.Compare(userKey, it.upperUser) >= 0 {
			it.err = io.EOF
			it.it = nil
			return
		}
		if !it.opt.IsAsc && len(it.lowerUser) > 0 && bytes.Compare(userKey, it.lowerUser) < 0 {
			it.err = io.EOF
			it.it = nil
			return
		}
		if len(it.lowerUser) > 0 && bytes.Compare(userKey, it.lowerUser) < 0 {
			it.bi.Next()
			continue
		}
		if len(it.upperUser) > 0 && bytes.Compare(userKey, it.upperUser) >= 0 {
			it.bi.Next()
			continue
		}
		{
			it.it = item
			it.err = nil
			return
		}
	}
}

func (t *Table) shouldPinHandleLocked() bool {
	if t == nil {
		return false
	}
	return true
}

func (t *Table) refreshHandlePolicy() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pins == 0 && !t.shouldPinHandleLocked() {
		t.closeSSTableLocked()
	}
}

// SetLevel records the LSM level the table currently lives in.
func (t *Table) SetLevel(level int) {
	t.lvl.Store(int32(level))
	t.refreshHandlePolicy()
}

// Level returns the most-recently-recorded LSM level for this table.
func (t *Table) Level() int {
	return int(t.lvl.Load())
}

// openSSTableLocked opens the SSTable handle; caller must hold t.mu.
func (t *Table) openSSTableLocked(loadIndex bool) error {
	if t.ss != nil {
		return nil
	}
	opt := &file.Options{
		FileName: vfs.FileNameSSTable(t.opts.WorkDir, t.fid),
		Dir:      t.opts.WorkDir,
		Flag:     os.O_RDONLY,
		MaxSz:    int(t.size),
		FS:       t.opts.FS,
	}
	if opt.MaxSz <= 0 {
		opt.MaxSz = int(t.opts.SSTableMaxSize)
	}
	ss := file.OpenSStable(opt)
	if loadIndex {
		if err := ss.Init(); err != nil {
			return err
		}
		idx := ss.Indexs()
		if idx != nil {
			t.idx.Store(idx)
			t.keyCount = idx.GetKeyCount()
			t.maxVersion = idx.GetMaxVersion()
			t.staleDataSize = idx.GetStaleDataSize()
			t.valueSize = idx.GetValueSize()
			t.rangeDeletes = idx.GetRangeTombstoneCount()
			if c := t.cache(); c != nil {
				c.AddIndex(t.fid, idx)
			}
		}
		t.hasBloom = ss.HasBloomFilter()
		t.size = ss.Size()
		if created := ss.GetCreatedAt(); created != nil {
			t.createdAt = *created
		}
		if len(t.minKey) == 0 {
			t.minKey = kv.SafeCopy(nil, ss.MinKey())
		}
		if len(t.maxKey) == 0 {
			t.maxKey = kv.SafeCopy(nil, ss.MaxKey())
		}
	}
	t.ss = ss
	return nil
}

// closeSSTableLocked closes the SSTable handle; caller must hold t.mu.
func (t *Table) closeSSTableLocked() {
	if t.ss == nil {
		return
	}
	_ = t.ss.Close()
	t.ss = nil
}

// CloseHandle closes the underlying SSTable file handle.
func (t *Table) CloseHandle() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ss == nil {
		return nil
	}
	err := t.ss.Close()
	t.ss = nil
	return err
}

func (t *Table) pinSSTable() (*file.SSTable, func(), error) {
	if t == nil {
		return nil, nil, errors.New("nil table")
	}
	t.mu.Lock()
	if t.ss == nil {
		if err := t.openSSTableLocked(false); err != nil {
			t.mu.Unlock()
			return nil, nil, err
		}
	}
	ss := t.ss
	t.pins++
	t.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			t.mu.Lock()
			if t.pins > 0 {
				t.pins--
			}
			if t.pins == 0 && !t.shouldPinHandleLocked() {
				t.closeSSTableLocked()
			}
			t.mu.Unlock()
		})
	}
	return ss, release, nil
}

// Delete removes the backing SST file and cache metadata for this table.
func (t *Table) Delete() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ss == nil {
		if err := t.openSSTableLocked(false); err != nil {
			return err
		}
	}
	if c := t.cache(); c != nil {
		c.DelIndex(t.fid)
	}
	if t.ss == nil {
		return nil
	}
	if err := t.ss.Detele(); err != nil {
		return err
	}
	t.ss = nil
	return nil
}

// IncrRef increments the table reference count.
func (t *Table) IncrRef() { t.Incr() }

// DecrRef decrements the refcount and deletes the table when it reaches zero.
func (t *Table) DecrRef() error {
	if t.Decr() == 0 {
		return t.Delete()
	}
	return nil
}

// DecrAll calls DecrRef on every table in tables, joining errors.
func DecrAll(tables []*Table) error {
	var decrRefsErr error
	for _, t := range tables {
		if err := t.DecrRef(); err != nil {
			decrRefsErr = stderrors.Join(decrRefsErr, err)
		}
	}
	return decrRefsErr
}
