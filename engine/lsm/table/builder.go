// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/feichai0017/NoKV/engine/file"
	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/pacer"
	"github.com/feichai0017/NoKV/engine/vfs"
	storagepb "github.com/feichai0017/NoKV/pb/storage"
	"github.com/feichai0017/NoKV/utils"
	"github.com/golang/snappy"
	proto "google.golang.org/protobuf/proto"
)

// Builder accumulates entries into in-memory blocks and emits them as one
// SST file via Flush. It is single-goroutine; callers must serialize access.
type Builder struct {
	sstSize       int64
	curBlock      *block
	opts          Options
	blockList     []*block
	keyCount      uint32
	keyHashes     []uint32
	prefixHashes  []uint32
	maxVersion    uint64
	staleDataSize int
	estimateSz    int64
	valueSize     int64
	rangeDeletes  uint32
	pacer         *pacer.Pacer
}

// BuildData is the serialized output of a Builder.Done call. Size is the total
// byte length the builder would write; Copy serializes the data into dst.
type BuildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	pacer     *pacer.Pacer
	// Size is the total length in bytes Copy will write.
	Size int
}

type block struct {
	offset            int // Offset of the block start within the table.
	checksum          []byte
	entriesIndexStart int
	chkLen            int
	data              []byte
	diskData          []byte
	baseKey           []byte
	entryOffsets      []uint32
	end               int
	diskEnd           int
	rawLen            int
	estimateSz        int64
	compression       Compression
	tbl               *Table
	release           func()
}

type header struct {
	overlap uint16
	diff    uint16
}

const headerSize = uint16(unsafe.Sizeof(header{}))

func (h *header) decode(buf []byte) {
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

// blockBufPool reuses the byte buffer that backs a block's encoded
// entries. A typical SST flush produces dozens of these per file; pooling
// them takes the bulk of the per-flush allocation traffic out of the GC.
//
// Buffers larger than maxPooledBlockBuf are not retained — the pool's
// purpose is to amortise allocations at the typical block size, not to
// hold onto outlier buffers that would inflate steady-state RSS.
var blockBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, defaultPooledBlockBuf)
		return &b
	},
}

const (
	defaultPooledBlockBuf = 4 << 10  // 4 KiB matches the most common BlockSize
	maxPooledBlockBuf     = 64 << 10 // refuse to pool >64 KiB blocks
)

func acquireBlockBuf(size int) []byte {
	bp := blockBufPool.Get().(*[]byte)
	if cap(*bp) < size {
		// The pooled buffer is too small; let it return to GC and
		// allocate a fresh slice that's the right size for this block.
		*bp = make([]byte, size)
	} else {
		*bp = (*bp)[:size]
	}
	return *bp
}

func releaseBlockBuf(b []byte) {
	if cap(b) == 0 || cap(b) > maxPooledBlockBuf {
		return
	}
	bp := b[:0]
	blockBufPool.Put(&bp)
}

// Pre-sized to typical per-flush counts: a 64 MiB SST holds ~16 blocks
// at default BlockSize, and ~1024 keys in total. Avoids slice growth
// reallocations on the common path.
const (
	preallocBlocks   = 16
	preallocKeyHash  = 1024
	preallocPrefixes = 1024
)

// NewBuilder constructs a Builder using opts.SSTableMaxSize as the per-file size cap.
func NewBuilder(opts Options) *Builder {
	return &Builder{
		opts:         opts,
		sstSize:      opts.SSTableMaxSize,
		blockList:    make([]*block, 0, preallocBlocks),
		keyHashes:    make([]uint32, 0, preallocKeyHash),
		prefixHashes: make([]uint32, 0, preallocPrefixes),
	}
}

// NewBuilderWithSize constructs a Builder with an explicit per-file size cap.
func NewBuilderWithSize(opts Options, sstSize int64) *Builder {
	return &Builder{
		opts:         opts,
		sstSize:      sstSize,
		blockList:    make([]*block, 0, preallocBlocks),
		keyHashes:    make([]uint32, 0, preallocKeyHash),
		prefixHashes: make([]uint32, 0, preallocPrefixes),
	}
}

// SetPacer attaches a pacer that throttles Copy-time write throughput. Nil disables pacing.
func (tb *Builder) SetPacer(p *pacer.Pacer) { tb.pacer = p }

func (tb *Builder) add(e *kv.Entry, valueLen uint32, isStale bool) {
	key := e.Key
	val := kv.ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}
	if tb.tryFinishBlock(e) {
		if isStale {
			tb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		tb.finishBlock()
		tb.curBlock = &block{
			data: acquireBlockBuf(int(tb.opts.BlockSize)),
		}
	}
	baseKey := kv.InternalToBaseKey(key)
	tb.keyHashes = append(tb.keyHashes, utils.Hash(baseKey))
	if tb.opts.PrefixExtractor != nil {
		if _, userKey, ok := kv.SplitBaseKey(baseKey); ok {
			if prefix := tb.opts.PrefixExtractor(userKey); len(prefix) > 0 {
				tb.prefixHashes = append(tb.prefixHashes, utils.Hash(prefix))
			}
		}
	}

	if version := kv.Timestamp(key); version > tb.maxVersion {
		tb.maxVersion = version
	}
	if e.IsRangeDelete() {
		tb.rangeDeletes++
	}

	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}
	utils.CondPanicFunc(len(key)-len(diffKey) > math.MaxUint16, func() error {
		return fmt.Errorf("builder.add: overlap len(key)-len(diffKey)=%d exceeds math.MaxUint16", len(key)-len(diffKey))
	})
	utils.CondPanicFunc(len(diffKey) > math.MaxUint16, func() error {
		return fmt.Errorf("builder.add: len(diffKey)=%d exceeds math.MaxUint16", len(diffKey))
	})

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	tb.append(h.encode())
	tb.append(diffKey)

	dst := tb.allocate(int(val.EncodedSize()))
	val.EncodeValue(dst)
	tb.valueSize += int64(valueLen)
}

// Empty returns whether the builder has accumulated no entries.
func (tb *Builder) Empty() bool { return len(tb.keyHashes) == 0 }

// Finish serializes the builder into a self-contained byte slice.
func (tb *Builder) Finish() ([]byte, error) {
	bd, err := tb.Done()
	if err != nil {
		return nil, err
	}
	defer bd.Release()
	buf := make([]byte, bd.Size)
	written := bd.Copy(buf)
	utils.CondPanicFunc(written != len(buf), func() error {
		return fmt.Errorf("Builder.Finish: written=%d buf=%d", written, len(buf))
	})
	return buf, nil
}

func (tb *Builder) tryFinishBlock(e *kv.Entry) bool {
	if tb.curBlock == nil {
		return true
	}

	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}
	utils.CondPanicFunc(uint64(len(tb.curBlock.entryOffsets)+1)*4+4+8+4 >= math.MaxUint32, func() error {
		return errors.New("integer overflow")
	})
	entriesOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length
	tb.curBlock.estimateSz = int64(tb.curBlock.end) + int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(e.EncodedValueSize()) + entriesOffsetsSize

	utils.CondPanicFunc(uint64(tb.curBlock.end)+uint64(tb.curBlock.estimateSz) >= math.MaxUint32, func() error {
		return errors.New("integer overflow")
	})

	return tb.curBlock.estimateSz > int64(tb.opts.BlockSize)
}

// AddStaleKey tracks stale key bytes for compaction decisions.
func (tb *Builder) AddStaleKey(e *kv.Entry) {
	tb.AddStaleEntryWithLen(e, entryValueLen(e))
}

// AddStaleEntryWithLen explicit len variant for compaction pipeline.
func (tb *Builder) AddStaleEntryWithLen(e *kv.Entry, valueLen uint32) {
	tb.staleDataSize += len(e.Key) + int(valueLen) + 4 /* entry offset */ + 4 /* header size */
	tb.add(e, valueLen, true)
}

// AddKey adds an entry, computing its value-len at len(e.Value).
func (tb *Builder) AddKey(e *kv.Entry) {
	tb.AddKeyWithLen(e, entryValueLen(e))
}

// AddKeyWithLen adds a key with an explicit value length.
func (tb *Builder) AddKeyWithLen(e *kv.Entry, valueLen uint32) {
	tb.add(e, valueLen, false)
}

// Close releases builder-side resources.
func (tb *Builder) Close() {}

func entryValueLen(e *kv.Entry) uint32 {
	if e == nil {
		return 0
	}
	return uint32(len(e.Value))
}

func (tb *Builder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	tb.append(kv.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(kv.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	tb.append(checksum)
	tb.append(kv.U32ToBytes(uint32(len(checksum))))
	tb.finishBlockEncoding(tb.curBlock)
	tb.estimateSz += int64(tb.curBlock.diskEnd)
	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil
}

func (tb *Builder) finishBlockEncoding(bl *block) {
	if bl == nil {
		return
	}
	raw := bl.data[:bl.end]
	bl.rawLen = len(raw)
	bl.diskData = raw
	bl.diskEnd = len(raw)
	bl.compression = CompressionNone
	if tb.opts.BlockCompression != CompressionSnappy {
		return
	}
	encoded := snappy.Encode(nil, raw)
	if len(encoded) >= len(raw) {
		return
	}
	bl.diskData = encoded
	bl.diskEnd = len(encoded)
	bl.compression = CompressionSnappy
}

func (tb *Builder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanicFunc(len(data) != copy(dst, data), func() error {
		return errors.New("builder.append: short copy")
	})
}

func (tb *Builder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		sz := max(bb.end+need, 2*len(bb.data))
		tmp := make([]byte, sz)
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

func (tb *Builder) calculateChecksum(data []byte) []byte {
	checkSum := kv.CalculateChecksum(data)
	return kv.U64ToBytes(checkSum)
}

func (tb *Builder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

// WriteBuildData copies bd into ss starting at offset zero and emits a
// drop-after-write madvise hint.
func WriteBuildData(ss *file.SSTable, bd BuildData) error {
	return writeBuildDataToSST(ss, bd)
}

func writeBuildDataToSST(ss *file.SSTable, bd BuildData) error {
	dst, err := ss.View(0, bd.Size)
	if err != nil {
		return err
	}
	written := bd.Copy(dst)
	utils.CondPanicFunc(written != len(dst), func() error {
		return fmt.Errorf("writeBuildDataToSST written != len(dst)")
	})
	_ = ss.Advise(utils.AccessPatternDontNeed)
	return nil
}

// Flush writes the builder state into a new SST file at tableName and returns an opened Table over it.
func (tb *Builder) Flush(rt Runtime, tableName string) (t *Table, err error) {
	if rt == nil {
		return nil, errors.New("Builder.Flush: nil runtime")
	}
	opts := rt.Options()
	bd, err := tb.Done()
	if err != nil {
		return nil, err
	}
	return tb.flush(rt, opts, tableName, &bd)
}

func (tb *Builder) flush(rt Runtime, opts Options, tableName string, predone *BuildData) (t *Table, err error) {
	var bd BuildData
	if predone != nil {
		bd = *predone
	} else {
		var derr error
		bd, derr = tb.Done()
		if derr != nil {
			return nil, derr
		}
	}
	defer bd.Release()
	t = &Table{rt: rt, opts: opts, fid: vfs.FID(tableName)}
	// Throughput-first mode: write directly to final SST when manifest sync is disabled.
	if !opts.ManifestSync {
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      opts.WorkDir,
			Flag:     os.O_CREATE | os.O_EXCL | os.O_RDWR,
			MaxSz:    bd.Size,
			FS:       opts.FS,
		})
		if t.ss == nil {
			return nil, fmt.Errorf("failed to open sstable %s", tableName)
		}
		if writeErr := writeBuildDataToSST(t.ss, bd); writeErr != nil {
			_ = t.ss.Close()
			return nil, writeErr
		}
		tb.blockList = nil
		return t, nil
	}

	fs := vfs.Ensure(opts.FS)
	tmpName := fmt.Sprintf("%s.tmp.%d.%d", tableName, os.Getpid(), time.Now().UnixNano())
	tmp := file.OpenSStable(&file.Options{
		FileName: tmpName,
		Dir:      opts.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    bd.Size,
		FS:       opts.FS,
	})
	if tmp == nil {
		return nil, fmt.Errorf("failed to open temp sstable %s", tmpName)
	}
	renamed := false
	defer func() {
		if err == nil {
			return
		}
		_ = tmp.Close()
		if !renamed {
			_ = fs.Remove(tmpName)
		}
	}()

	if err := writeBuildDataToSST(tmp, bd); err != nil {
		return nil, err
	}
	if err := tmp.Sync(); err != nil {
		return nil, err
	}

	if err := fs.RenameNoReplace(tmpName, tableName); errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("sst already exists: %s", tableName)
	} else if err != nil {
		return nil, err
	}
	renamed = true

	tmp.SetFileName(tableName)
	t.ss = tmp
	tb.blockList = nil
	return t, nil
}

// Copy serializes built table blocks, index, and trailer into dst.
func (bd BuildData) Copy(dst []byte) int {
	var written int
	for _, bl := range bd.blockList {
		bd.pacer.Charge(bl.diskEnd)
		written += copy(dst[written:], bl.diskData[:bl.diskEnd])
	}
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], kv.U32ToBytes(uint32(len(bd.index))))

	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], kv.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

// Release returns each block's data buffer to the pool. Callers must
// invoke this after Copy completes — not before, since Copy reads from
// the buffers (or from snappy-allocated diskData that aliases them when
// compression was a no-op). Safe to call once.
func (bd BuildData) Release() {
	for _, bl := range bd.blockList {
		if bl == nil {
			continue
		}
		if bl.data != nil {
			releaseBlockBuf(bl.data)
			bl.data = nil
		}
		// diskData either aliases bl.data (already released) or is a
		// fresh snappy-allocated buffer we let GC reclaim — its size
		// distribution is too unpredictable to pool usefully.
		bl.diskData = nil
	}
}

// Done finalizes the builder and returns the serializable BuildData. The builder must not be reused after Done.
func (tb *Builder) Done() (BuildData, error) {
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return BuildData{}, nil
	}
	bd := BuildData{
		blockList: tb.blockList,
		pacer:     tb.pacer,
	}

	var f utils.Filter
	if tb.opts.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opts.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bits)
	}
	var pf utils.Filter
	if tb.opts.BloomFalsePositive > 0 && len(tb.prefixHashes) > 0 {
		bits := utils.BloomBitsPerKey(len(tb.prefixHashes), tb.opts.BloomFalsePositive)
		pf = utils.NewFilter(tb.prefixHashes, bits)
	}

	idx, dataSize, err := tb.buildIndex(f, pf)
	if err != nil {
		return BuildData{}, err
	}
	checksum := tb.calculateChecksum(idx)
	bd.index = idx
	bd.checksum = checksum
	total := int(dataSize) + len(idx) + len(checksum) + 4 + 4
	bd.Size = total
	tb.estimateSz = int64(total)
	return bd, nil
}

func (tb *Builder) buildIndex(bloom, prefixBloom []byte) ([]byte, uint32, error) {
	tableIndex := &storagepb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	if len(prefixBloom) > 0 {
		tableIndex.PrefixBloomFilter = prefixBloom
	}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion
	if tb.staleDataSize > 0 {
		if tb.staleDataSize > math.MaxUint32 {
			tableIndex.StaleDataSize = math.MaxUint32
		} else {
			tableIndex.StaleDataSize = uint32(tb.staleDataSize)
		}
	}
	tableIndex.ValueSize = uint64(tb.valueSize)
	tableIndex.RangeTombstoneCount = tb.rangeDeletes
	tableIndex.Offsets = tb.writeBlockOffsets()
	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].diskEnd)
	}
	data, err := proto.Marshal(tableIndex)
	if err != nil {
		return nil, 0, err
	}
	return data, dataSize, nil
}

func (tb *Builder) writeBlockOffsets() []*storagepb.BlockOffset {
	var startOffset uint32
	var offsets []*storagepb.BlockOffset
	for _, bl := range tb.blockList {
		offset := tb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.diskEnd)
	}
	return offsets
}

func (b *Builder) writeBlockOffset(bl *block, startOffset uint32) *storagepb.BlockOffset {
	offset := &storagepb.BlockOffset{}
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.diskEnd)
	offset.Offset = startOffset
	offset.Compression = uint32(bl.compression)
	offset.RawLen = uint32(bl.rawLen)
	return offset
}

// ReachedCapacity reports whether builder output has reached its size cap.
func (b *Builder) ReachedCapacity() bool {
	return b.estimateSz > b.sstSize
}

func (b block) verifyCheckSum() error {
	return kv.VerifyChecksum(b.data, b.checksum)
}

// entryItem is a minimal index.Item impl used by blockIterator.
type entryItem struct{ e *kv.Entry }

func (it *entryItem) Entry() *kv.Entry { return it.e }

type blockIterator struct {
	data         []byte
	idx          int
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block

	tableID uint64
	blockID int

	entry     kv.Entry
	valStruct kv.ValueStruct
	item      entryItem

	it    index.Item
	isAsc bool
}

var blockItrPool = sync.Pool{
	New: func() any { return &blockIterator{} },
}

func getBlockIterator() *blockIterator {
	return blockItrPool.Get().(*blockIterator)
}

func putBlockIterator(bi *blockIterator) {
	if bi == nil {
		return
	}
	bi.reset()
	blockItrPool.Put(bi)
}

func (itr *blockIterator) setBlock(b *block) {
	if itr.block != nil && itr.block.release != nil && itr.block != b {
		itr.block.release()
	}
	itr.block = b
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	itr.data = b.data[:b.entriesIndexStart]
	itr.entryOffsets = b.entryOffsets
}

func (itr *blockIterator) seekToFirst() { itr.setIdx(0) }
func (itr *blockIterator) seekToLast()  { itr.setIdx(len(itr.entryOffsets) - 1) }

func (itr *blockIterator) seek(key []byte) {
	itr.err = nil
	n := len(itr.entryOffsets)
	if n == 0 {
		itr.setIdx(0)
		return
	}
	if itr.isAsc {
		lo, hi := 0, n
		for lo < hi {
			mid := lo + (hi-lo)/2
			itr.setIdx(mid)
			if kv.CompareInternalKeys(itr.key, key) >= 0 {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		itr.setIdx(lo)
		return
	}
	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		itr.setIdx(mid)
		if kv.CompareInternalKeys(itr.key, key) > 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if lo == 0 {
		itr.setIdx(-1)
		return
	}
	itr.setIdx(lo - 1)
}

func (itr *blockIterator) setIdx(i int) {
	itr.idx = i
	if i >= len(itr.entryOffsets) || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	startOffset := int(itr.entryOffsets[i])

	if len(itr.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(itr.data)
		itr.baseKey = itr.data[headerSize : headerSize+baseHeader.diff]
	}

	var endOffset int
	if itr.idx+1 == len(itr.entryOffsets) {
		endOffset = len(itr.data)
	} else {
		endOffset = int(itr.entryOffsets[itr.idx+1])
	}
	entryData := itr.data[startOffset:endOffset]
	var h header
	h.decode(entryData)
	valueOff := headerSize + h.diff
	diffKey := entryData[headerSize:valueOff]
	itr.key = append(itr.key[:0], itr.baseKey[:h.overlap]...)
	itr.key = append(itr.key, diffKey...)
	itr.entry.Key = itr.key
	itr.valStruct.DecodeValue(entryData[valueOff:])
	itr.val = itr.valStruct.Value
	itr.entry.Value = itr.valStruct.Value
	itr.entry.ExpiresAt = itr.valStruct.ExpiresAt
	itr.entry.Meta = itr.valStruct.Meta
	_ = itr.entry.PopulateInternalMeta()
	itr.item.e = &itr.entry
	itr.it = &itr.item
}

func (itr *blockIterator) Error() error { return itr.err }

func (itr *blockIterator) Next() {
	if itr.isAsc {
		itr.setIdx(itr.idx + 1)
	} else {
		itr.setIdx(itr.idx - 1)
	}
}

func (itr *blockIterator) Valid() bool { return itr.err == nil }

func (itr *blockIterator) Rewind() bool {
	if itr.isAsc {
		itr.setIdx(0)
	} else {
		itr.setIdx(len(itr.entryOffsets) - 1)
	}
	return true
}

func (itr *blockIterator) Item() index.Item { return itr.it }

func (itr *blockIterator) Close() error {
	if itr.block != nil && itr.block.release != nil {
		itr.block.release()
		itr.block.release = nil
	}
	return nil
}

func (itr *blockIterator) reset() {
	if itr.block != nil && itr.block.release != nil {
		itr.block.release()
		itr.block.release = nil
	}
	itr.data = nil
	itr.idx = 0
	itr.err = nil
	itr.baseKey = itr.baseKey[:0]
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	itr.entryOffsets = nil
	itr.block = nil
	itr.tableID = 0
	itr.blockID = 0
	itr.entry = kv.Entry{}
	itr.valStruct = kv.ValueStruct{}
	itr.item = entryItem{}
	itr.it = nil
	itr.isAsc = true
}
