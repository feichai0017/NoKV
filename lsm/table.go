package lsm

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // For file garbage collection. Atomic.
	lvl int
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
			utils.Err(err)
			return nil
		}
	} else {
		t = &table{lm: lm, fid: fid}
		// if builder is nil, open an existing sst file
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize)})
	}
	// first reference, otherwise the reference state will be incorrect
	t.IncrRef()
	// initialize the sst file, load the index
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}

	// get the max key of sst, need to use iterator
	itr := t.NewIterator(&utils.Options{}) // default is descending
	defer itr.Close()
	// locate to the initial position is the max key
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	maxKey := append([]byte(nil), itr.Item().Entry().Key...)
	t.ss.SetMaxKey(maxKey)

	return t
}

// Search search for a key in the table
func (t *table) Search(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
	t.IncrRef()
	defer t.DecrRef()
	// get the index
	idx := t.ss.Indexs()
	if t.ss.HasBloomFilter() {
		var bloomFilter utils.Filter
		if cached, ok := t.lm.cache.getBloom(t.fid); ok {
			bloomFilter = cached
		} else {
			bloomFilter = utils.Filter(idx.BloomFilter)
			if len(bloomFilter) > 0 {
				t.lm.cache.addBloom(t.fid, bloomFilter)
			}
		}
		if len(bloomFilter) > 0 && !bloomFilter.MayContainKey(key) {
			return nil, utils.ErrKeyNotFound
		}
	}
	iter := t.NewIterator(&utils.Options{})
	defer iter.Close()

	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if utils.SameKey(key, iter.Item().Entry().Key) {
		if version := utils.ParseTs(iter.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (t *table) indexKey() uint64 {
	return t.fid
}

// 去加载sst对应的block
func (t *table) block(idx int) (*block, error) {
	return t.loadBlock(idx, true)
}

func (t *table) loadBlock(idx int, hot bool) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	key := t.blockCacheKey(idx)
	if cached, ok := t.lm.cache.getBlock(t.lvl, key); ok && cached != nil {
		if hot {
			t.lm.cache.addBlockWithTier(t.lvl, key, cached, true)
		}
		return cached, nil
	}

	ko, ok := t.blockOffset(idx)
	utils.CondPanic(!ok || ko == nil, fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(ko.GetOffset()),
	}

	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.ss.FID(), b.offset, ko.GetLen())
	}

	readPos := len(b.data) - 4 // First read checksum length.
	b.chkLen = int(utils.BytesToU32(b.data[readPos : readPos+4]))

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
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4

	b.entryOffsets = utils.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.entriesIndexStart = entriesIndexStart

	t.lm.cache.addBlockWithTier(t.lvl, key, b, hot)

	return b, nil
}

func (t *table) prefetchBlockForKey(key []byte, hot bool) bool {
	if t == nil || len(key) == 0 {
		return false
	}
	t.IncrRef()
	defer t.DecrRef()

	offsets := t.ss.Indexs().GetOffsets()
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
	_, err := t.loadBlock(idx, hot)
	return err == nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}

const maxUint32 = uint64(math.MaxUint32)

// blockCacheKey is used to store blocks in the block cache.
func (t *table) blockCacheKey(idx int) uint64 {
	utils.CondPanic(t.fid > maxUint32, fmt.Errorf("table fid %d exceeds 32-bit limit", t.fid))
	utils.CondPanic(idx < 0 || uint64(idx) > maxUint32, fmt.Errorf("invalid block index %d", idx))
	return (t.fid << 32) | uint64(uint32(idx))
}

type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (t *table) NewIterator(options *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}
func (it *tableIterator) Next() {
	it.err = nil

	if it.blockPos >= len(it.t.ss.Indexs().GetOffsets()) {
		it.err = io.EOF
		return
	}

	if len(it.bi.data) == 0 {
		block, err := it.t.block(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
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
func (it *tableIterator) Valid() bool {
	return it.err != io.EOF // 如果没有的时候 则是EOF
}
func (it *tableIterator) Rewind() {
	if it.opt.IsAsc {
		it.seekToFirst()
	} else {
		it.seekToLast()
	}
}
func (it *tableIterator) Item() utils.Item {
	return it.it
}
func (it *tableIterator) Close() error {
	it.bi.Close()
	return it.t.DecrRef()
}
func (it *tableIterator) seekToFirst() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = 0
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToFirst()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

func (it *tableIterator) seekToLast() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = numBlocks - 1
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToLast()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

// Seek
// 二分法搜索 offsets
// 如果idx == 0 说明key只能在第一个block中 block[0].MinKey <= key
// 否则 block[0].MinKey > key
// 如果在 idx-1 的block中未找到key 那才可能在 idx 中
// 如果都没有，则当前key不再此table
func (it *tableIterator) Seek(key []byte) {
	idx := sort.Search(len(it.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		ko, ok := it.t.blockOffset(idx)
		utils.CondPanic(!ok, fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == len(it.t.ss.Indexs().GetOffsets()) {
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
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}

func (t *table) blockOffset(i int) (*pb.BlockOffset, bool) {
	index := t.ss.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return nil, false
	}
	if i == len(index.GetOffsets()) {
		return nil, true
	}
	return index.GetOffsets()[i], true
}

// Size is its file size in bytes
func (t *table) Size() int64 { return int64(t.ss.Size()) }

// GetCreatedAt
func (t *table) GetCreatedAt() *time.Time {
	return t.ss.GetCreatedAt()
}
func (t *table) Delete() error {
	return t.ss.Detele()
}

// StaleDataSize is the amount of stale data (that can be dropped by a compaction )in this SST.
func (t *table) StaleDataSize() uint32 { return t.ss.Indexs().StaleDataSize }

// DecrRef decrements the refcount and possibly deletes the table
func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		// TODO 从缓存中删除
		for i := 0; i < len(t.ss.Indexs().GetOffsets()); i++ {
			t.lm.cache.dropBlock(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

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
