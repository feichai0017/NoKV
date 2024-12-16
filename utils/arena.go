 package utils

 import (
	 "log"
	 "sync/atomic"
	 "unsafe"
 
	 "github.com/pkg/errors"
 )
 
 const (
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
	 n          uint32
	 shouldGrow bool
	 buf        []byte
 }
 
 // newArena returns a new arena.
 func newArena(n int64) *Arena {
	 // Don't store data at position 0 in order to reserve offset=0 as a kind
	 // of nil pointer.
	 out := &Arena{
		 n:   1,
		 buf: make([]byte, n),
	 }
	 return out
 }
 
 func (s *Arena) allocate(sz uint32) uint32 {
	 offset := atomic.AddUint32(&s.n, sz)
	 if !s.shouldGrow {
		 AssertTrue(int(offset) <= len(s.buf))
		 return offset - sz
	 }
 
	 // We are keeping extra bytes in the end so that the checkptr doesn't fail. We apply some
	 // intelligence to reduce the size of the node by only keeping towers upto valid height and not
	 // maxHeight. This reduces the node's size, but checkptr doesn't know about its reduced size.
	 // checkptr tries to verify that the node of size MaxNodeSize resides on a single heap
	 // allocation which causes this error: checkptr:converted pointer straddles multiple allocations
	 if int(offset) > len(s.buf)-MaxNodeSize {
		 growBy := uint32(len(s.buf))
		 if growBy > 1<<30 {
			 growBy = 1 << 30
		 }
		 if growBy < sz {
			 growBy = sz
		 }
		 newBuf := make([]byte, len(s.buf)+int(growBy))
		 AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		 s.buf = newBuf
		 // fmt.Print(len(s.buf), " ")
	 }
	 return offset - sz
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
 func (s *Arena) putVal(v ValueStruct) uint32 {
	 l := uint32(v.EncodedSize())
	 offset := s.allocate(l)
	 v.EncodeValue(s.buf[offset:])
	 return offset
 }
 
 func (s *Arena) putKey(key []byte) uint32 {
	 keySz := uint32(len(key))
	 offset := s.allocate(keySz)
	 buf := s.buf[offset : offset+keySz]
	 AssertTrue(len(key) == copy(buf, key))
	 return offset
 }
 
 // getNode returns a pointer to the node located at offset. If the offset is
 // zero, then the nil node pointer is returned.
 func (s *Arena) getNode(offset uint32) *node {
	 if offset == 0 {
		 return nil
	 }
	 return (*node)(unsafe.Pointer(&s.buf[offset]))
 }
 
 // getKey returns byte slice at offset.
 func (s *Arena) getKey(offset uint32, size uint16) []byte {
	 return s.buf[offset : offset+uint32(size)]
 }
 
 // getVal returns byte slice at offset. The given size should be just the value
 // size and should NOT include the meta bytes.
 func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	 ret.DecodeValue(s.buf[offset : offset+size])
	 return
 }
 
 // getNodeOffset returns the offset of node in the arena. If the node pointer is
 // nil, then the zero offset is returned.
 func (s *Arena) getNodeOffset(nd *node) uint32 {
	 if nd == nil {
		 return 0 //返回空指针
	 }
	 //implement me here！！！
	 //获取某个节点,在 arena 当中的偏移量
	 //unsafe.Pointer等价于void*,uintptr可以专门把void*的对于地址转化为数值型变量
	 return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
 }
 
 // AssertTrue asserts that b is true. Otherwise, it would log fatal.
 func AssertTrue(b bool) {
	 if !b {
		 log.Fatalf("%+v", errors.Errorf("Assert failed"))
	 }
 }