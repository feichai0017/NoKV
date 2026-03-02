package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/feichai0017/NoKV/kv"
)

const (
	// DefaultArenaSize is the default allocation size for in-memory indexes.
	DefaultArenaSize = int64(64 << 20)

	minArenaChunkSize = int64(1 << 20)
)

// Arena should be lock-free.
type Arena struct {
	n         uint32
	chunkSize uint32
	chunks    []atomic.Pointer[byte]
}

// NewArena returns a new arena.
func NewArena(n int64) *Arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	if n <= 0 {
		n = DefaultArenaSize
	}
	if n > int64(^uint32(0)) {
		n = int64(^uint32(0))
	}
	chunkSize := max(min(n, DefaultArenaSize), minArenaChunkSize)
	AssertTrue(chunkSize > 0)
	out := &Arena{
		n:         1,
		chunkSize: uint32(chunkSize),
		chunks:    make([]atomic.Pointer[byte], maxChunks(uint32(chunkSize))),
	}
	first := make([]byte, int(chunkSize))
	out.chunks[0].Store(&first[0])
	return out
}

func (s *Arena) Allocate(sz uint32) uint32 {
	AssertTrue(s != nil)
	AssertTrue(sz > 0)
	AssertTrue(sz <= s.chunkSize)
	for {
		cur := atomic.LoadUint32(&s.n)
		start := cur
		end := start + sz
		startChunk := start / s.chunkSize
		if startChunk != (end-1)/s.chunkSize {
			start = (startChunk + 1) * s.chunkSize
			end = start + sz
			startChunk = start / s.chunkSize
		}
		if atomic.CompareAndSwapUint32(&s.n, cur, end) {
			s.ensureChunk(startChunk)
			return start
		}
	}
}

func (s *Arena) AllocAligned(size, align int) uint32 {
	if s == nil || size <= 0 {
		return 0
	}
	if align <= 0 {
		align = 1
	}
	pad := align - 1
	AssertTrue(uint32(size+pad) <= s.chunkSize)
	offset := s.Allocate(uint32(size + pad))
	return (offset + uint32(pad)) & ^uint32(pad)
}

func (s *Arena) AllocBytes(length int) []byte {
	if s == nil || length <= 0 {
		return nil
	}
	AssertTrue(uint32(length) <= s.chunkSize)
	offset := s.Allocate(uint32(length))
	return s.bytesAt(offset, length)
}

func (s *Arena) AllocByteSlice(length, capacity int) []byte {
	if capacity <= 0 {
		return nil
	}
	if length < 0 {
		length = 0
	}
	if length > capacity {
		length = capacity
	}
	buf := s.AllocBytes(capacity)
	return buf[:length:capacity]
}

func (s *Arena) AllocUint32Slice(length, capacity int) []uint32 {
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
	offset := s.AllocAligned(elemSize*capacity, align)
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

func (s *Arena) bytesAt(offset uint32, length int) []byte {
	if s == nil || length <= 0 || offset == 0 {
		return nil
	}
	ptr, off := s.chunkFor(offset)
	if ptr == nil {
		return nil
	}
	chunk := unsafe.Slice(ptr, int(s.chunkSize))
	start := int(off)
	end := start + length
	AssertTrue(end <= len(chunk))
	return chunk[start:end]
}

func (s *Arena) addr(offset uint32) unsafe.Pointer {
	if s == nil || offset == 0 {
		return nil
	}
	ptr, off := s.chunkFor(offset)
	if ptr == nil {
		return nil
	}
	return unsafe.Add(unsafe.Pointer(ptr), uintptr(off))
}

func (s *Arena) chunkFor(offset uint32) (*byte, uint32) {
	if s == nil || offset == 0 {
		return nil, 0
	}
	idx := offset / s.chunkSize
	off := offset % s.chunkSize
	if int(idx) >= len(s.chunks) {
		return nil, 0
	}
	return s.chunks[idx].Load(), off
}

func (s *Arena) ensureChunk(idx uint32) {
	if s == nil {
		return
	}
	AssertTrue(int(idx) < len(s.chunks))
	if s.chunks[idx].Load() != nil {
		return
	}
	for {
		if s.chunks[idx].Load() != nil {
			return
		}
		buf := make([]byte, int(s.chunkSize))
		if s.chunks[idx].CompareAndSwap(nil, &buf[0]) {
			return
		}
	}
}

// arenaPutKey copies key bytes into arena and returns the start offset.
func arenaPutKey(arena *Arena, key []byte) uint32 {
	if arena == nil {
		return 0
	}
	keySz := uint32(len(key))
	if keySz == 0 {
		return 0
	}
	offset := arena.Allocate(keySz)
	buf := arena.bytesAt(offset, int(keySz))
	copy(buf, key)
	return offset
}

// arenaGetKey returns key bytes from arena at offset with given size.
func arenaGetKey(arena *Arena, offset uint32, size uint16) []byte {
	if arena == nil {
		return nil
	}
	return arena.bytesAt(offset, int(size))
}

// arenaPutVal encodes and stores value bytes into arena, returning the start offset.
func arenaPutVal(arena *Arena, v kv.ValueStruct) uint32 {
	if arena == nil {
		return 0
	}
	l := uint32(v.EncodedSize())
	if l == 0 {
		return 0
	}
	offset := arena.Allocate(l)
	buf := arena.bytesAt(offset, int(l))
	v.EncodeValue(buf)
	return offset
}

// arenaGetVal decodes a value struct from arena at offset and size.
func arenaGetVal(arena *Arena, offset uint32, size uint32) (ret kv.ValueStruct) {
	if arena == nil {
		return kv.ValueStruct{}
	}
	ret.DecodeValue(arena.bytesAt(offset, int(size)))
	return
}

func maxChunks(chunkSize uint32) int {
	if chunkSize == 0 {
		return 0
	}
	max := uint64(^uint32(0))
	return int(max/uint64(chunkSize) + 1)
}

// AssertTrue asserts that b is true. Otherwise, it would log fatal.
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("Assert failed")
	}
}
