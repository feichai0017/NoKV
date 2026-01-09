package utils

import (
	"log"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/feichai0017/NoKV/kv"
	"github.com/pkg/errors"
)

const (
	// DefaultArenaSize is the default allocation size for in-memory indexes.
	DefaultArenaSize = int64(64 << 20)

	minArenaChunkSize = int64(1 << 20)

	offsetSize = int(unsafe.Sizeof(uint32(0)))

	// Always align nodes on 64-bit boundaries, even on 32-bit architectures,
	// so that the node.value field is 64-bit aligned. This is necessary because
	// node.getValueOffset uses atomic.LoadUint64, which expects its input
	// pointer to be 64-bit aligned.
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

// Arena should be lock-free.
type Arena struct {
	n         uint32
	chunkSize uint32
	chunks    atomic.Value // [][]byte
	chunksMu  sync.Mutex
}

// newArena returns a new arena.
func newArena(n int64) *Arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	if n <= 0 {
		n = DefaultArenaSize
	}
	if n > int64(^uint32(0)) {
		n = int64(^uint32(0))
	}
	chunkSize := n
	if chunkSize > DefaultArenaSize {
		chunkSize = DefaultArenaSize
	}
	if chunkSize < minArenaChunkSize {
		chunkSize = minArenaChunkSize
	}
	out := &Arena{
		n:         1,
		chunkSize: uint32(chunkSize),
	}
	first := make([]byte, int(chunkSize))
	out.chunks.Store([][]byte{first})
	return out
}

func (s *Arena) allocate(sz uint32) uint32 {
	AssertTrue(s != nil)
	AssertTrue(sz > 0)
	AssertTrue(sz <= s.chunkSize)
	for {
		cur := atomic.LoadUint32(&s.n)
		start := cur
		end := start + sz
		startChunk := start / s.chunkSize
		endChunk := (end - 1) / s.chunkSize
		if startChunk != endChunk {
			start = (startChunk + 1) * s.chunkSize
			end = start + sz
			endChunk = (end - 1) / s.chunkSize
		}
		if atomic.CompareAndSwapUint32(&s.n, cur, end) {
			s.ensureChunk(endChunk)
			return start
		}
	}
}

func (s *Arena) allocAligned(size, align int) uint32 {
	if s == nil || size <= 0 {
		return 0
	}
	if align <= 0 {
		align = 1
	}
	pad := align - 1
	AssertTrue(uint32(size+pad) <= s.chunkSize)
	offset := s.allocate(uint32(size + pad))
	return (offset + uint32(pad)) & ^uint32(pad)
}

func (s *Arena) allocBytes(length int) []byte {
	if s == nil || length <= 0 {
		return nil
	}
	AssertTrue(uint32(length) <= s.chunkSize)
	offset := s.allocate(uint32(length))
	return s.bytesAt(offset, length)
}

func (s *Arena) allocByteSlice(length, capacity int) []byte {
	if capacity <= 0 {
		return nil
	}
	if length < 0 {
		length = 0
	}
	if length > capacity {
		length = capacity
	}
	buf := s.allocBytes(capacity)
	return buf[:length:capacity]
}

func (s *Arena) allocUint32Slice(length, capacity int) []uint32 {
	if s == nil || capacity <= 0 {
		return nil
	}
	if length < 0 {
		length = 0
	}
	if length > capacity {
		length = capacity
	}
	elemSize := int(unsafe.Sizeof(uint32(0)))
	align := int(unsafe.Alignof(uint32(0)))
	AssertTrue(uint32(elemSize*capacity) <= s.chunkSize)
	offset := s.allocAligned(elemSize*capacity, align)
	ptr := (*uint32)(s.addr(offset))
	if ptr == nil {
		return nil
	}
	raw := unsafe.Slice(ptr, capacity)
	return raw[:length:capacity]
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

// putNode allocates a node in the arena. The node is aligned on a pointer-sized
// boundary. The arena offset of the node is returned.
func (s *Arena) putNode(height int) uint32 {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize

	// Pad the allocation with enough bytes to ensure pointer alignment.
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.allocate(l)

	// Return the aligned offset.
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

// Put will *copy* val into arena. To make better use of this, reuse your input
// val buffer. Returns an offset into buf. User is responsible for remembering
// size of val. We could also store this size inside arena but the encoding and
// decoding will incur some overhead.
func (s *Arena) putVal(v kv.ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	offset := s.allocate(l)
	buf := s.bytesAt(offset, int(l))
	v.EncodeValue(buf)
	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	if keySz == 0 {
		return 0
	}
	offset := s.allocate(keySz)
	buf := s.bytesAt(offset, int(keySz))
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

// getNode returns a pointer to the node located at offset. If the offset is
// zero, then the nil node pointer is returned.
func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(s.addr(offset))
}

// getKey returns byte slice at offset.
func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.bytesAt(offset, int(size))
}

// getVal returns byte slice at offset. The given size should be just the value
// size and should NOT include the meta bytes.
func (s *Arena) getVal(offset uint32, size uint32) (ret kv.ValueStruct) {
	ret.DecodeValue(s.bytesAt(offset, int(size)))
	return
}

// getNodeOffset returns the offset of node in the arena. If the node pointer is
// nil, then the zero offset is returned.
func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0 //return nil pointer
	}
	return nd.self
}

func (s *Arena) bytesAt(offset uint32, length int) []byte {
	if s == nil || length <= 0 || offset == 0 {
		return nil
	}
	chunk, off := s.chunkFor(offset)
	if chunk == nil {
		return nil
	}
	start := int(off)
	end := start + length
	AssertTrue(end <= len(chunk))
	return chunk[start:end]
}

func (s *Arena) addr(offset uint32) unsafe.Pointer {
	if s == nil || offset == 0 {
		return nil
	}
	chunk, off := s.chunkFor(offset)
	if chunk == nil {
		return nil
	}
	return unsafe.Pointer(&chunk[int(off)])
}

func (s *Arena) chunkFor(offset uint32) ([]byte, uint32) {
	if s == nil {
		return nil, 0
	}
	chunks := s.loadChunks()
	if len(chunks) == 0 {
		return nil, 0
	}
	idx := offset / s.chunkSize
	off := offset % s.chunkSize
	if int(idx) >= len(chunks) {
		return nil, 0
	}
	return chunks[idx], off
}

func (s *Arena) loadChunks() [][]byte {
	if s == nil {
		return nil
	}
	val := s.chunks.Load()
	if val == nil {
		return nil
	}
	return val.([][]byte)
}

func (s *Arena) ensureChunk(idx uint32) {
	chunks := s.loadChunks()
	if int(idx) < len(chunks) {
		return
	}
	s.chunksMu.Lock()
	defer s.chunksMu.Unlock()

	chunks = s.loadChunks()
	if int(idx) < len(chunks) {
		return
	}
	next := make([][]byte, len(chunks), int(idx)+1)
	copy(next, chunks)
	for len(next) <= int(idx) {
		next = append(next, make([]byte, int(s.chunkSize)))
	}
	s.chunks.Store(next)
}

// AssertTrue asserts that b is true. Otherwise, it would log fatal.
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
