package lsm

import (
	"expvar"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

var (
	prefetchLaunched  = expvar.NewInt("NoKV.Prefetch.Launched")
	prefetchAborted   = expvar.NewInt("NoKV.Prefetch.Aborted")
	prefetchCompleted = expvar.NewInt("NoKV.Prefetch.Completed")
)

type table struct {
	lm  *levelManager
	fid uint64
	ref int32 // For file garbage collection. Atomic.
	lvl atomic.Int32

	minKey []byte
	maxKey []byte
	size   int64

	createdAt     time.Time
	staleDataSize uint32
	valueSize     uint64
	keyCount      uint32
	maxVersion    uint64
	hasBloom      bool

	idx atomic.Pointer[pb.TableIndex]

	mu   sync.Mutex
	ss   *file.SSTable
	pins int32
}

func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	sstSize := int(lm.opt.SSTableMaxSz)
	if builder != nil {
		sstSize = int(builder.done().size)
	}
	var (
		t   *table
		err error
	)
	fid := utils.FID(tableName)
	// if builder is not nil, flush the buffer to disk
	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			_ = utils.Err(err)
			return nil
		}
	} else {
		t = &table{lm: lm, fid: fid}
		// if builder is nil, open an existing sst file
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize),
			FS:       lm.opt.FS})
	}
	// first reference, otherwise the reference state will be incorrect
	t.IncrRef()
	// initialize the sst file, load the index
	// The Index Block is stored as a Protobuf message (pb.TableIndex).
	// The overall SSTable structure is:
	// +--------------------+ ... +--------------------+--------------------+
	// | Data Block 1       |     | Data Block N       | Index Block (Proto)|
	// +--------------------+ ... +--------------------+--------------------+
	// | Index Block Length (4B) | SSTable Checksum (8B) | SSTable Checksum Length (4B) |
	// +-------------------------+-----------------------+------------------------------+
	if err := t.ss.Init(); err != nil {
		_ = utils.Err(err)
		return nil
	}

	idx := t.ss.Indexs()
	if idx != nil {
		t.idx.Store(idx)
		t.keyCount = idx.GetKeyCount()
		t.maxVersion = idx.GetMaxVersion()
		t.staleDataSize = idx.GetStaleDataSize()
		t.valueSize = idx.GetValueSize()
		t.lm.cache.addIndex(t.fid, idx)
	}
	t.hasBloom = t.ss.HasBloomFilter()
	t.size = t.ss.Size()
	if created := t.ss.GetCreatedAt(); created != nil {
		t.createdAt = *created
	}
	t.minKey = kv.SafeCopy(nil, t.ss.MinKey())

	// get the max key of sst, need to use iterator
	itr := t.NewIterator(&utils.Options{}) // default is descending
	defer func() { _ = itr.Close() }()
	// locate to the initial position is the max key
	itr.Rewind()
	if !itr.Valid() {
		// Empty table should not happen, but keep minKey as maxKey fallback.
		t.maxKey = kv.SafeCopy(nil, t.minKey)
		return t
	}
	item := itr.Item()
	if item == nil || item.Entry() == nil {
		t.maxKey = kv.SafeCopy(nil, t.minKey)
		return t
	}
	maxKey := append([]byte(nil), item.Entry().Key...)
	t.maxKey = kv.SafeCopy(nil, maxKey)
	t.ss.SetMaxKey(maxKey)

	return t
}

func (t *table) index() *pb.TableIndex {
	if t == nil {
		return nil
	}
	if idx := t.idx.Load(); idx != nil {
		return idx
	}
	if cached, ok := t.lm.cache.getIndex(t.fid); ok {
		t.idx.Store(cached)
		return cached
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if idx := t.idx.Load(); idx != nil {
		return idx
	}
	if err := t.openSSTableLocked(true); err != nil {
		_ = utils.Err(err)
		return nil
	}
	idx := t.ss.Indexs()
	if idx != nil {
		t.idx.Store(idx)
		t.lm.cache.addIndex(t.fid, idx)
		t.keyCount = idx.GetKeyCount()
		t.maxVersion = idx.GetMaxVersion()
		t.staleDataSize = idx.GetStaleDataSize()
		t.valueSize = idx.GetValueSize()
	}
	return idx
}

// shouldPinHandleLocked reports handle policy; caller must hold t.mu.
func (t *table) shouldPinHandleLocked() bool {
	if t == nil {
		return false
	}
	// keep SSTable handles pinned for the
	// lifetime of the table to avoid invalidating mmap-backed blocks cached
	// elsewhere. This prevents close/reopen races that can invalidate slices.
	return true
}

func (t *table) refreshHandlePolicy() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pins == 0 && !t.shouldPinHandleLocked() {
		t.closeSSTableLocked()
	}
}

func (t *table) setLevel(level int) {
	t.lvl.Store(int32(level))
	t.refreshHandlePolicy()
}

func (t *table) level() int {
	return int(t.lvl.Load())
}

// openSSTableLocked opens the SSTable handle; caller must hold t.mu.
func (t *table) openSSTableLocked(loadIndex bool) error {
	if t.ss != nil {
		return nil
	}
	opt := &file.Options{
		FileName: utils.FileNameSSTable(t.lm.opt.WorkDir, t.fid),
		Dir:      t.lm.opt.WorkDir,
		Flag:     os.O_RDONLY,
		MaxSz:    int(t.size),
		FS:       t.lm.opt.FS,
	}
	if opt.MaxSz <= 0 {
		opt.MaxSz = int(t.lm.opt.SSTableMaxSz)
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
			t.lm.cache.addIndex(t.fid, idx)
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
func (t *table) closeSSTableLocked() {
	if t.ss == nil {
		return
	}
	_ = t.ss.Close()
	t.ss = nil
}

func (t *table) closeHandle() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ss == nil {
		return nil
	}
	err := t.ss.Close()
	t.ss = nil
	return err
}

func (t *table) pinSSTable() (*file.SSTable, func(), error) {
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

// Search search for a key in the table
func (t *table) Search(key []byte, maxVs *uint64) (entry *kv.Entry, err error) {
	t.IncrRef()
	defer func() {
		_ = t.DecrRef()
	}()
	// get the index
	idx := t.index()
	if idx == nil {
		return nil, errors.New("table index missing")
	}
	if t.hasBloom || len(idx.BloomFilter) > 0 {
		var bloomFilter utils.Filter
		if cached, ok := t.lm.cache.getBloom(t.fid); ok {
			bloomFilter = cached
		} else {
			bloomFilter = utils.Filter(idx.BloomFilter)
			if len(bloomFilter) > 0 {
				t.lm.cache.addBloom(t.fid, bloomFilter)
			}
		}
		probe := key
		if len(key) > 8 {
			probe = kv.ParseKey(key)
		}
		if len(bloomFilter) > 0 && !bloomFilter.MayContainKey(probe) {
			return nil, utils.ErrKeyNotFound
		}
	}
	iter := t.NewIterator(&utils.Options{})
	defer func() { _ = iter.Close() }()

	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}
	item := iter.Item()
	if item == nil || item.Entry() == nil {
		return nil, utils.ErrKeyNotFound
	}

	if e := item.Entry(); kv.SameKey(key, e.Key) {
		if version := kv.ParseTs(e.Key); *maxVs < version {
			*maxVs = version
			clone := kv.NewEntryWithCF(e.CF, kv.SafeCopy(nil, e.Key), kv.SafeCopy(nil, e.Value))
			clone.ExpiresAt = e.ExpiresAt
			clone.Meta = e.Meta
			clone.Version = version
			clone.Offset = e.Offset
			clone.Hlen = e.Hlen
			clone.ValThreshold = e.ValThreshold
			return clone, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (t *table) loadBlock(idx int) (*block, error) {
	utils.CondPanicFunc(idx < 0, func() error { return fmt.Errorf("idx=%d", idx) })
	index := t.index()
	if index == nil {
		return nil, errors.New("missing table index")
	}
	offsets := index.GetOffsets()
	if idx >= len(offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	key := t.blockCacheKey(idx)
	lvl := t.level()
	if cached, ok := t.lm.cache.getBlock(lvl, key); ok && cached != nil {
		return cached, nil
	}

	ko, ok := t.blockOffset(idx)
	utils.CondPanicFunc(!ok || ko == nil, func() error { return fmt.Errorf("block t.offset id=%d", idx) })
	b = &block{
		offset: int(ko.GetOffset()),
		tbl:    t,
	}

	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.fid, b.offset, ko.GetLen())
	}

	// Binary Format for a Data Block (read from disk):
	// +--------------------------------+--------------------------------+
	// | ... (Key-Value Entries) ...    | Entry Offsets List (var length)|
	// +--------------------------------+--------------------------------+
	// | Entry Offsets List Length (4B) | Block Checksum (8B)            |
	// +--------------------------------+--------------------------------+
	// | Block Checksum Length (4B)     |
	// +--------------------------------+

	readPos := len(b.data) - 4 // First read checksum length.
	b.chkLen = int(kv.BytesToU32(b.data[readPos : readPos+4]))

	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	b.data = b.data[:readPos]

	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}

	readPos -= 4
	numEntries := int(kv.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4

	b.entryOffsets = kv.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.entriesIndexStart = entriesIndexStart

	t.lm.cache.addBlock(lvl, t, key, b)

	return b, nil
}

func (t *table) prefetchBlockForKey(key []byte) bool {
	if t == nil || len(key) == 0 {
		return false
	}
	t.IncrRef()
	defer func() {
		_ = t.DecrRef()
	}()

	index := t.index()
	if index == nil {
		return false
	}
	offsets := index.GetOffsets()
	if len(offsets) == 0 {
		return false
	}
	var idx int
	var ko *pb.BlockOffset
	idx = sort.Search(len(offsets), func(i int) bool {
		var ok bool
		ko, ok = t.blockOffset(i)
		utils.CondPanic(!ok, fmt.Errorf("table.prefetch idx=%d", i))
		if i == len(offsets) {
			return true
		}
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	switch {
	case idx <= 0:
		idx = 0
	case idx >= len(offsets):
		idx = len(offsets) - 1
	default:
		idx = idx - 1
	}
	if idx < 0 || idx >= len(offsets) {
		return false
	}
	_, err := t.loadBlock(idx)
	return err == nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	ss, release, err := t.pinSSTable()
	if err != nil {
		return nil, err
	}
	defer release()
	return ss.Bytes(off, sz)
}

const maxUint32 = uint64(math.MaxUint32)

// blockCacheKey is used to store blocks in the block cache.
func (t *table) blockCacheKey(idx int) uint64 {
	utils.CondPanicFunc(t.fid > maxUint32, func() error { return fmt.Errorf("table fid %d exceeds 32-bit limit", t.fid) })
	utils.CondPanicFunc(idx < 0 || uint64(idx) > maxUint32, func() error { return fmt.Errorf("invalid block index %d", idx) })
	return (t.fid << 32) | uint64(uint32(idx))
}

// MinKey returns the smallest user key stored in this SSTable.
func (t *table) MinKey() []byte { return t.minKey }

// MaxKey returns the largest user key stored in this SSTable.
func (t *table) MaxKey() []byte { return t.maxKey }

// KeyCount returns the approximate number of keys indexed by this table.
func (t *table) KeyCount() uint32 {
	if t.keyCount != 0 {
		return t.keyCount
	}
	if idx := t.index(); idx != nil {
		t.keyCount = idx.GetKeyCount()
		return t.keyCount
	}
	return 0
}

// MaxVersionVal returns the maximum MVCC version recorded in this table index.
func (t *table) MaxVersionVal() uint64 {
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
func (t *table) HasBloomFilter() bool {
	if t.hasBloom {
		return true
	}
	if idx := t.index(); idx != nil && len(idx.BloomFilter) > 0 {
		t.hasBloom = true
		return true
	}
	return false
}

type tableIterator struct {
	it           utils.Item
	opt          *utils.Options
	t            *table
	blockPos     int
	bi           *blockIterator
	err          error
	index        *pb.TableIndex
	closeCh      chan struct{}
	wg           sync.WaitGroup
	closeOnce    sync.Once
	prefetchRing *utils.Ring[int]
	prefetchPool *utils.Pool
}

func (it *tableIterator) fetchBlock(idx int) (*block, error) {
	return it.t.loadBlock(idx)
}

func (it *tableIterator) prefetchNext(idx int) {
	if it.opt == nil || it.opt.PrefetchBlocks <= 0 || it.prefetchRing == nil {
		return
	}
	if it.index == nil {
		return
	}
	limit := len(it.index.GetOffsets())
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
func (t *table) NewIterator(options *utils.Options) utils.Iterator {
	t.IncrRef()
	if options == nil {
		options = &utils.Options{}
	}
	if options.IsAsc && options.PrefetchBlocks > 0 {
		// Best-effort advise for long forward scans; ignore errors.
		t.adviseIterator(options)
	}

	it := &tableIterator{
		opt:     options,
		t:       t,
		bi:      getBlockIterator(),
		index:   t.index(),
		closeCh: make(chan struct{}),
	}
	if options.PrefetchBlocks > 0 {
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
	return it
}

// adviseIterator is an optional helper to issue madvise hints for long scans.
func (t *table) adviseIterator(options *utils.Options) {
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
func (it *tableIterator) Next() {
	it.err = nil

	if it.index == nil || it.blockPos >= len(it.index.GetOffsets()) {
		it.err = io.EOF
		return
	}

	if len(it.bi.data) == 0 {
		block, err := it.fetchBlock(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
		it.prefetchNext(it.blockPos)
		it.bi.tableID = it.t.fid
		it.bi.blockID = it.blockPos
		it.bi.setBlock(block)
		it.bi.seekToFirst()
		it.err = it.bi.Error()
		return
	}

	it.bi.Next()
	if !it.bi.Valid() {
		it.blockPos++
		it.bi.data = nil
		it.Next()
		return
	}
	it.it = it.bi.it
}

// Valid reports whether table iterator has a readable current item.
func (it *tableIterator) Valid() bool {
	return it.err == nil
}

// Rewind resets iterator position to the first/last key by scan direction.
func (it *tableIterator) Rewind() {
	if it.opt.IsAsc {
		it.seekToFirst()
	} else {
		it.seekToLast()
	}
}

// Item returns the current table iterator item.
func (it *tableIterator) Item() utils.Item {
	return it.it
}

// Close releases block iterators, prefetch workers, and table references.
func (it *tableIterator) Close() error {
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
func (it *tableIterator) seekToFirst() {
	if it.index == nil {
		it.err = io.EOF
		return
	}
	numBlocks := len(it.index.GetOffsets())
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = 0
	block, err := it.fetchBlock(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.prefetchNext(it.blockPos)
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToFirst()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

func (it *tableIterator) seekToLast() {
	if it.index == nil {
		it.err = io.EOF
		return
	}
	numBlocks := len(it.index.GetOffsets())
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = numBlocks - 1
	block, err := it.fetchBlock(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.prefetchNext(it.blockPos)
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToLast()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

// Seek uses binary search over block offsets.
// If idx == 0, the key can only be in the first block (block[0].MinKey <= key).
// Otherwise block[0].MinKey > key and we probe idx-1 first, then idx if needed.
// If neither block contains the key, it is not present in this table.
func (it *tableIterator) Seek(key []byte) {
	if it.index == nil {
		it.err = io.EOF
		return
	}
	offsets := it.index.GetOffsets()
	idx := sort.Search(len(offsets), func(idx int) bool {
		ko, ok := it.t.blockOffset(idx)
		utils.CondPanicFunc(!ok, func() error { return fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()") })
		if idx == len(offsets) {
			return true
		}
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		it.seekHelper(0, key)
		return
	}
	it.seekHelper(idx-1, key)
}

func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.fetchBlock(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	it.prefetchNext(blockIdx)
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}

func (t *table) blockOffset(i int) (*pb.BlockOffset, bool) {
	index := t.index()
	if index == nil {
		return nil, false
	}
	offsets := index.GetOffsets()
	if i < 0 || i > len(offsets) {
		return nil, false
	}
	if i == len(offsets) {
		return nil, true
	}
	return offsets[i], true
}

// Size is its file size in bytes
func (t *table) Size() int64 { return t.size }

// GetCreatedAt
func (t *table) GetCreatedAt() *time.Time {
	if t.createdAt.IsZero() {
		return nil
	}
	created := t.createdAt
	return &created
}

// Delete removes the backing SST file and cache metadata for this table.
func (t *table) Delete() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ss == nil {
		if err := t.openSSTableLocked(false); err != nil {
			return err
		}
	}
	t.lm.cache.delIndex(t.fid)
	if t.ss == nil {
		return nil
	}
	if err := t.ss.Detele(); err != nil {
		return err
	}
	t.ss = nil
	return nil
}

// StaleDataSize is the amount of stale data (that can be dropped by a compaction )in this SST.
func (t *table) StaleDataSize() uint32 { return t.staleDataSize }

// ValueSize reports total value bytes referenced by this table (inline + vlog pointers).
func (t *table) ValueSize() uint64 { return t.valueSize }

// DecrRef decrements the refcount and possibly deletes the table
func (t *table) DecrRef() error {
	for {
		current := atomic.LoadInt32(&t.ref)
		// 1. Guard check
		utils.CondPanicFunc(current <= 0, func() error {
			return fmt.Errorf("table refcount underflow: fid %d, current_ref %d", t.fid, current)
		})

		newRef := current - 1
		// 2. Atomic transition
		if atomic.CompareAndSwapInt32(&t.ref, current, newRef) {
			if newRef == 0 {
				return t.Delete()
			}
			return nil
		}
		// 3. If CAS failed , the loop will retry
	}
}

// IncrRef increments the table reference count.
func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}
func decrRefs(tables []*table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}
