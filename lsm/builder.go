package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sort"
	"unsafe"

	"github.com/feichai0017/NoKV/file"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
	proto "google.golang.org/protobuf/proto"
)

type tableBuilder struct {
	sstSize       int64
	curBlock      *block
	opt           *Options
	blockList     []*block
	keyCount      uint32
	keyHashes     []uint32
	maxVersion    uint64
	baseKey       []byte
	staleDataSize int
	estimateSz    int64
	valueSize     int64
}
type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}
type block struct {
	offset            int //当前block的offset 首地址
	checksum          []byte
	entriesIndexStart int
	chkLen            int
	data              []byte
	baseKey           []byte
	entryOffsets      []uint32
	end               int
	estimateSz        int64
	tbl               *table
	release           func()
}

type header struct {
	overlap uint16 // Overlap with base key.
	diff    uint16 // Length of the diff.
}

const headerSize = uint16(unsafe.Sizeof(header{}))

// Decode decodes the header.
func (h *header) decode(buf []byte) {
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

func (tb *tableBuilder) add(e *kv.Entry, valueLen uint32, isStale bool) {
	key := e.Key
	val := kv.ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}
	// check if need to allocate a new block
	if tb.tryFinishBlock(e) {
		if isStale {
			// This key will be added to tableIndex and it is stale.
			tb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		tb.finishBlock()
		// Create a new block and start writing.
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize), // TODO encrypt the block, the size of the block will increase, need to reserve some padding position
		}
	}
	// record the hash value of the key
	tb.keyHashes = append(tb.keyHashes, utils.Hash(kv.ParseKey(key)))

	// update the maxVersion
	if version := kv.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	// calculate the diff of the key
	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}
	utils.CondPanicFunc(!(len(key)-len(diffKey) <= math.MaxUint16), func() error {
		return fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16")
	})
	utils.CondPanicFunc(!(len(diffKey) <= math.MaxUint16), func() error {
		return fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16")
	})

	// Binary Format for a single entry within a Data Block:
	// +-----------------+-----------------+-----------------+
	// | Overlap (2B)    | Diff Length (2B)| Diff Key Bytes  |
	// +-----------------+-----------------+-----------------+
	// | Value Meta (1B) | Value ExpAt (8B)| Value Bytes     |
	// +-----------------+-----------------+-----------------+
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
func newTableBuilerWithSSTSize(opt *Options, size int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: size,
	}
}
func newTableBuiler(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSz,
	}
}

// Empty returns whether it's empty.
func (tb *tableBuilder) empty() bool { return len(tb.keyHashes) == 0 }

func (tb *tableBuilder) finish() []byte {
	bd := tb.done()
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written == len(buf), nil)
	return buf
}
func (tb *tableBuilder) tryFinishBlock(e *kv.Entry) bool {
	if tb.curBlock == nil {
		return true
	}

	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}
	utils.CondPanic(!((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("Integer overflow"))
	entriesOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length
	tb.curBlock.estimateSz = int64(tb.curBlock.end) + int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(e.EncodedSize()) + entriesOffsetsSize

	// Integer overflow check for table size.
	utils.CondPanic(!(uint64(tb.curBlock.end)+uint64(tb.curBlock.estimateSz) < math.MaxUint32), errors.New("Integer overflow"))

	return tb.curBlock.estimateSz > int64(tb.opt.BlockSize)
}

// AddStaleKey 记录陈旧key所占用的空间大小，用于日志压缩时的决策
func (tb *tableBuilder) AddStaleKey(e *kv.Entry) {
	tb.AddStaleEntryWithLen(e, entryValueLen(e))
}

// AddStaleEntryWithLen explicit len variant for compaction pipeline.
func (tb *tableBuilder) AddStaleEntryWithLen(e *kv.Entry, valueLen uint32) {
	// Rough estimate based on how much space it will occupy in the SST.
	tb.staleDataSize += len(e.Key) + int(valueLen) + 4 /* entry offset */ + 4 /* header size */
	tb.add(e, valueLen, true)
}

// AddKey _
func (tb *tableBuilder) AddKey(e *kv.Entry) {
	tb.AddKeyWithLen(e, entryValueLen(e))
}

// AddKeyWithLen 添加 key，并显式指定 value 长度（用于区分内联值和 ValuePtr）。
func (tb *tableBuilder) AddKeyWithLen(e *kv.Entry, valueLen uint32) {
	tb.add(e, valueLen, false)
}

// Close closes the TableBuilder.
func (tb *tableBuilder) Close() {
	// combine the memory allocator
}

func entryValueLen(e *kv.Entry) uint32 {
	if e == nil {
		return 0
	}
	if e.Meta&kv.BitValuePointer > 0 {
		var vptr kv.ValuePtr
		req := int(unsafe.Sizeof(vptr))
		if len(e.Value) >= req {
			vptr.Decode(e.Value)
			return vptr.Len
		}
	}
	return uint32(len(e.Value))
}
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// Binary Format for a Data Block (after all entries):
	// +--------------------------------+--------------------------------+
	// | ... (Key-Value Entries) ...    | Entry Offsets List (var length)|
	// +--------------------------------+--------------------------------+
	// | Entry Offsets List Length (4B) | Block Checksum (8B)            |
	// +--------------------------------+--------------------------------+
	// | Block Checksum Length (4B)     |
	// +--------------------------------+

	// Append the entryOffsets and its length.
	tb.append(kv.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(kv.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	// Append the block checksum and its length.
	tb.append(checksum)
	tb.append(kv.U32ToBytes(uint32(len(checksum))))
	tb.estimateSz += tb.curBlock.estimateSz
	tb.blockList = append(tb.blockList, tb.curBlock)
	// TODO: estimate the size of the sst file after the builder is serialized to disk
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil // indicates that the current block has been serialized to memory
}

// append appends to curBlock.data
func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

func (tb *tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		// We need to reallocate.
		sz := max(bb.end+need, 2*len(bb.data))
		tmp := make([]byte, sz) // todo use memory allocator to improve performance
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return kv.U64ToBytes(checkSum)
}

func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (tb *tableBuilder) flush(lm *levelManager, tableName string) (t *table, err error) {
	bd := tb.done()
	t = &table{lm: lm, fid: utils.FID(tableName)}
	// if builder is nil, open an existing sst file
	t.ss = file.OpenSStable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size)})
	dst, err := t.ss.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	written := bd.Copy(dst)
	utils.CondPanicFunc(written != len(dst), func() error {
		return fmt.Errorf("tableBuilder.flush written != len(dst)")
	})
	// Allow GC to reclaim the intermediate blocks once the data is persisted.
	tb.blockList = nil

	// Hint the OS that freshly written pages can be dropped; block cache holds hot copies.
	_ = t.ss.Advise(utils.AccessPatternDontNeed)
	return t, nil
}

func (bd *buildData) Copy(dst []byte) int {
	var written int
	for _, bl := range bd.blockList {
		written += copy(dst[written:], bl.data[:bl.end])
	}
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], kv.U32ToBytes(uint32(len(bd.index))))

	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], kv.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

func (tb *tableBuilder) done() buildData {
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return buildData{}
	}
	bd := buildData{
		blockList: tb.blockList,
	}

	var f utils.Filter
	if tb.opt.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bits)
	}
	// TODO 构建 sst的索引
	// Overall SSTable Binary Format:
	// +--------------------+--------------------+ ... +--------------------+--------------------+
	// | Data Block 1       | Data Block 2       |     | Data Block N       | Index Block (Proto)|
	// +--------------------+--------------------+ ... +--------------------+--------------------+
	// | Index Block Length (4B) | SSTable Checksum (8B) | SSTable Checksum Length (4B) |
	// +-------------------------+-----------------------+------------------------------+

	index, dataSize := tb.buildIndex(f)
	checksum := tb.calculateChecksum(index)
	bd.index = index
	bd.checksum = checksum
	total := int(dataSize) + len(index) + len(checksum) + 4 + 4
	bd.size = total
	tb.estimateSz = int64(total)
	return bd
}

func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
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
	tableIndex.Offsets = tb.writeBlockOffsets()
	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}
	data, err := proto.Marshal(tableIndex)
	utils.Panic(err)
	return data, dataSize
}

func (tb *tableBuilder) writeBlockOffsets() []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range tb.blockList {
		offset := tb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
	}
	return offsets
}

func (b *tableBuilder) writeBlockOffset(bl *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.end)
	offset.Offset = startOffset
	return offset
}

// TODO: 如何能更好的预估builder的长度呢？
func (b *tableBuilder) ReachedCapacity() bool {
	return b.estimateSz > b.sstSize
}

func (b block) verifyCheckSum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

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

	prevOverlap uint16

	entry     kv.Entry
	valStruct kv.ValueStruct
	item      Item

	it utils.Item
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
	itr.prevOverlap = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	// Drop the index from the block. We don't need it anymore.
	itr.data = b.data[:b.entriesIndexStart]
	itr.entryOffsets = b.entryOffsets
}

// seekToFirst brings us to the first element.
func (itr *blockIterator) seekToFirst() {
	itr.setIdx(0)
}
func (itr *blockIterator) seekToLast() {
	itr.setIdx(len(itr.entryOffsets) - 1)
}
func (itr *blockIterator) seek(key []byte) {
	itr.err = nil
	startIndex := 0 // This tells from which index we should start binary search.

	foundEntryIdx := sort.Search(len(itr.entryOffsets), func(idx int) bool {
		// If idx is less than start index then just return false.
		if idx < startIndex {
			return false
		}
		itr.setIdx(idx)
		return utils.CompareKeys(itr.key, key) >= 0
	})
	itr.setIdx(foundEntryIdx)
}

func (itr *blockIterator) setIdx(i int) {
	itr.idx = i
	if i >= len(itr.entryOffsets) || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	startOffset := int(itr.entryOffsets[i])

	// Set base key.
	if len(itr.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(itr.data)
		itr.baseKey = itr.data[headerSize : headerSize+baseHeader.diff]
	}

	var endOffset int
	// idx points to the last entry in the block.
	if itr.idx+1 == len(itr.entryOffsets) {
		endOffset = len(itr.data)
	} else {
		// idx point to some entry other than the last one in the block.
		// EndOffset of the current entry is the start offset of the next entry.
		endOffset = int(itr.entryOffsets[itr.idx+1])
	}
	defer func() {
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			fmt.Fprintf(&debugBuf, "==== Recovered====\n")
			fmt.Fprintf(&debugBuf, "Table ID: %d\nBlock ID: %d\nEntry Idx: %d\nData len: %d\n"+
				"StartOffset: %d\nEndOffset: %d\nEntryOffsets len: %d\nEntryOffsets: %v\n",
				itr.tableID, itr.blockID, itr.idx, len(itr.data), startOffset, endOffset,
				len(itr.entryOffsets), itr.entryOffsets)
			panic(debugBuf.String())
		}
	}()

	entryData := itr.data[startOffset:endOffset]
	var h header
	h.decode(entryData)
	if h.overlap > itr.prevOverlap {
		itr.key = append(itr.key[:itr.prevOverlap], itr.baseKey[itr.prevOverlap:h.overlap]...)
	}

	itr.prevOverlap = h.overlap
	valueOff := headerSize + h.diff
	diffKey := entryData[headerSize:valueOff]
	itr.key = append(itr.key[:h.overlap], diffKey...)
	itr.entry.Key = itr.key
	itr.valStruct.DecodeValue(entryData[valueOff:])
	itr.val = itr.valStruct.Value
	itr.entry.Value = itr.valStruct.Value
	itr.entry.ExpiresAt = itr.valStruct.ExpiresAt
	itr.entry.Meta = itr.valStruct.Meta
	itr.item.e = &itr.entry
	itr.it = &itr.item
}

func (itr *blockIterator) Error() error {
	return itr.err
}

func (itr *blockIterator) Next() {
	itr.setIdx(itr.idx + 1)
}

func (itr *blockIterator) Valid() bool {
	return itr.err == nil
}
func (itr *blockIterator) Rewind() bool {
	itr.setIdx(0)
	return true
}
func (itr *blockIterator) Item() utils.Item {
	return itr.it
}
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
	itr.prevOverlap = 0
	itr.entry = kv.Entry{}
	itr.valStruct = kv.ValueStruct{}
	itr.item = Item{}
	itr.it = nil
}
