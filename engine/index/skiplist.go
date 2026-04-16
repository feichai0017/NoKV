/*
Adapted from RocksDB inline skiplist.

Key differences:
- No custom comparator.
- Support overwrites. This requires care when we see the same key when inserting.
  For RocksDB or LevelDB, overwrites are implemented as a newer sequence number in the key, so
	there is no need for values. We don't intend to support versioning. In-place updates of values
	would be more efficient.
- We discard all non-concurrent code.
- We do not support Splices. This simplifies the code a lot.
- No AllocateNode or other pointer arithmetic.
- We combine the findLessThan, findGreaterOrEqual, etc into one function.
*/

package index

import (
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3

	offsetSize = int(unsafe.Sizeof(uint32(0)))

	// nodeAlign ensures that the node.value field is 64-bit aligned.
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

// arenaPutNode allocates a node in the arena for the skiplist.
func arenaPutNode(s *Arena, height int) uint32 {
	unusedSize := (maxHeight - height) * offsetSize
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.Allocate(l)
	return (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
}

// slGetNode returns a pointer to a skiplist node at the given offset.
func arenaGetNode(s *Arena, offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(s.addr(offset))
}

// arenaGetNodeOffset returns the arena offset of a skiplist node.
func arenaGetNodeOffset(s *Arena, nd *node) uint32 {
	if nd == nil {
		return 0
	}
	return nd.self
}

type node struct {
	// Multiple parts of the value are encoded as a single uint64 so that it
	// can be atomically loaded and stored:
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-63)
	value uint64

	// A byte slice is 24 bytes. We are trying to save space here.
	keyOffset uint32 // Immutable. No need to lock to access key.
	self      uint32 // Immutable offset of this node in the arena.
	keySize   uint16 // Immutable. No need to lock to access key.

	// Height of the tower.
	height uint16

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]uint32
}

type Skiplist struct {
	height     int32 // Current height. 1 <= height <= kMaxHeight. CAS.
	headOffset uint32
	utils.RefCount
	arena   *Arena
	OnClose func()

	// maxKeyPerLevel stores the offset of the rightmost node at each level.
	// This enables O(1) append at any level during sequential inserts.
	// maxKeyPerLevel[0] is the base level's rightmost node (global max key).
	maxKeyPerLevel [maxHeight]uint32
}

// IncrRef increments the refcount, satisfying the memIndex interface.
func (s *Skiplist) IncrRef() { s.Incr() }

// DecrRef decrements the refcount, deallocating the Skiplist when done using it.
func (s *Skiplist) DecrRef() {
	if s.Decr() > 0 {
		return
	}
	// ref == 0: safe to release.
	if s.OnClose != nil {
		s.OnClose()
	}

	// Indicate we are closed. Good for testing.  Also, lets GC reclaim memory. Race condition
	// here would suggest we are accessing skiplist when we are supposed to have no reference!
	s.arena = nil
}

func newNode(arena *Arena, key []byte, v kv.ValueStruct, height int) *node {
	// The base level is already allocated in the node struct.
	nodeOffset := arenaPutNode(arena, height)
	keyOffset := arenaPutKey(arena, key)
	val := encodeValue(arenaPutVal(arena, v), v.EncodedSize())

	node := arenaGetNode(arena, nodeOffset)
	node.self = nodeOffset
	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = val
	return node
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

// NewSkiplist makes a new empty skiplist, with a given arena size
func NewSkiplist(arenaSize int64) *Skiplist {
	arena := NewArena(arenaSize)
	head := newNode(arena, nil, kv.ValueStruct{}, maxHeight)
	ho := arenaGetNodeOffset(arena, head)
	list := &Skiplist{
		height:     1,
		headOffset: ho,
		arena:      arena,
	}
	list.Init(1)
	return list
}

func (n *node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&n.value)
	return decodeValue(value)
}

func (n *node) key(arena *Arena) []byte {
	return arenaGetKey(arena, n.keyOffset, n.keySize)
}

func (n *node) setValue(arena *Arena, vo uint64) {
	atomic.StoreUint64(&n.value, vo)
}

func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], old, val)
}

// getVs return kv.ValueStruct stored in node
func (n *node) getVs(arena *Arena) kv.ValueStruct {
	valOffset, valSize := n.getValueOffset()
	return arenaGetVal(arena, valOffset, valSize)
}

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand() <= heightIncrease {
		h++
	}
	return h
}

func (s *Skiplist) getNext(nd *node, height int) *node {
	return arenaGetNode(s.arena, nd.getNextOffset(height))
}

func (s *Skiplist) getHead() *node {
	return arenaGetNode(s.arena, s.headOffset)
}

// findNear finds the node near to key.
// If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
// node.key <= key (if allowEqual=true).
// If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
// node.key >= key (if allowEqual=true).
// Returns the node found. The bool returned is true if the node has key equal to given key.
func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.getHead()
	level := int(s.getHeight() - 1)
	for {
		// Assume x.key < key.
		next := s.getNext(x, level)
		if next == nil {
			// x.key < key < END OF LIST
			if level > 0 {
				// Can descend further to iterate closer to the end.
				level--
				continue
			}
			// Level=0. Cannot descend further. Let's return something that makes sense.
			if !less {
				return nil, false
			}
			// Try to return x. Make sure it is not a head node.
			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}

		nextKey := next.key(s.arena)
		cmp := kv.CompareInternalKeys(key, nextKey)
		if cmp > 0 {
			// x.key < next.key < key. We can continue to move right.
			x = next
			continue
		}
		if cmp == 0 {
			// x.key < key == next.key.
			if allowEqual {
				return next, true
			}
			if !less {
				// We want >, so go to base level to grab the next bigger note.
				return s.getNext(next, 0), false
			}
			// We want <. If not base level, we should go closer in the next level.
			if level > 0 {
				level--
				continue
			}
			// On base level. Return x.
			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}
		// cmp < 0. In other words, x.key < key < next.
		if level > 0 {
			level--
			continue
		}
		// At base level. Need to return something.
		if !less {
			return next, false
		}
		// Try to return x. Make sure it is not a head node.
		if x == s.getHead() {
			return nil, false
		}
		return x, false
	}
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return outBefore = outAfter.
// Otherwise, outBefore.key < key < outAfter.key.
func (s *Skiplist) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	for {
		// Assume before.key < key.
		beforeNode := arenaGetNode(s.arena, before)
		next := beforeNode.getNextOffset(level)
		nextNode := arenaGetNode(s.arena, next)
		if nextNode == nil {
			return before, next
		}
		nextKey := nextNode.key(s.arena)
		cmp := kv.CompareInternalKeys(key, nextKey)
		if cmp == 0 {
			// Equality case.
			return next, next
		}
		if cmp < 0 {
			// before.key < key < next.key. We are done for this level.
			return before, next
		}
		before = next // Keep moving right on this level.
	}
}

func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// updateMaxPerLevel updates the maxKeyPerLevel for the given level if the new key is greater.
func (s *Skiplist) updateMaxPerLevel(level int, newNodeOffset uint32, key []byte) {
	for {
		oldMaxOffset := atomic.LoadUint32(&s.maxKeyPerLevel[level])
		if oldMaxOffset != 0 {
			oldMaxNode := arenaGetNode(s.arena, oldMaxOffset)
			if oldMaxNode != nil {
				oldMaxKey := oldMaxNode.key(s.arena)
				if kv.CompareInternalKeys(key, oldMaxKey) <= 0 {
					return
				}
			}
		}
		if atomic.CompareAndSwapUint32(&s.maxKeyPerLevel[level], oldMaxOffset, newNodeOffset) {
			return
		}
	}
}

// Put inserts the key-value pair.
func (s *Skiplist) Add(e *kv.Entry) {
	// Since we allow overwrite, we may not need to create a new node. We might not even need to
	// increase the height. Let's defer these actions.
	key, v := e.Key, kv.ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}

	listHeight := s.getHeight()
	var prev [maxHeight + 1]uint32
	var next [maxHeight + 1]uint32
	prev[listHeight] = s.headOffset

	// For each level, independently determine insertion point
	// If key > maxKey at this level, we can use the maxKey node as a starting
	// point for search (sequential optimization hint).
	for i := int(listHeight) - 1; i >= 0; i-- {
		searchFrom := prev[i+1]
		maxOffset := atomic.LoadUint32(&s.maxKeyPerLevel[i])
		if maxOffset != 0 {
			maxNode := arenaGetNode(s.arena, maxOffset)
			if maxNode != nil {
				maxKey := maxNode.key(s.arena)
				if kv.CompareInternalKeys(key, maxKey) > 0 {
					// key > maxKey: use maxNode as search start for O(1) best-case append
					searchFrom = maxOffset
				}
			}
		}
		// Search from the best known starting point (either prev[i+1] or maxNode)
		prev[i], next[i] = s.findSpliceForLevel(key, searchFrom, i)
		if prev[i] == next[i] {
			// Key already exists, update value
			vo := arenaPutVal(s.arena, v)
			encValue := encodeValue(vo, v.EncodedSize())
			prevNode := arenaGetNode(s.arena, prev[i])
			prevNode.setValue(s.arena, encValue)
			return
		}
	}

	// We do need to create a new node.
	height := s.randomHeight()
	x := newNode(s.arena, key, v, height)
	xOffset := arenaGetNodeOffset(s.arena, x)

	// Try to increase s.height via CAS.
	listHeight = s.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			// Successfully increased skiplist.height.
			break
		}
		listHeight = s.getHeight()
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := range height {
		for {
			if arenaGetNode(s.arena, prev[i]) == nil {
				AssertTrue(i > 1) // This cannot happen in base level.
				// We haven't computed prev, next for this level because height exceeds old listHeight.
				// For these levels, we expect the lists to be sparse, so we can just search from head.
				prev[i], next[i] = s.findSpliceForLevel(key, s.headOffset, i)
				// Someone adds the exact same key before we are able to do so. This can only happen on
				// the base level. But we know we are not on the base level.
				AssertTrue(prev[i] != next[i])
			}

			x.tower[i] = next[i]
			pnode := arenaGetNode(s.arena, prev[i])
			if pnode.casNextOffset(i, next[i], xOffset) {
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				// If inserted at the end of this level, update maxKeyPerLevel
				if next[i] == 0 {
					s.updateMaxPerLevel(i, xOffset, key)
				}
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				vo := arenaPutVal(s.arena, v)
				encValue := encodeValue(vo, v.EncodedSize())
				prevNode := arenaGetNode(s.arena, prev[i])
				prevNode.setValue(s.arena, encValue)
				return
			}
		}
	}
}

// Empty returns if the Skiplist is empty.
func (s *Skiplist) Empty() bool {
	return s.findLast() == nil
}

// findLast returns the last element. If head (empty list), we return nil. All the find functions
// will NEVER return the head nodes.
func (s *Skiplist) findLast() *node {
	n := s.getHead()
	level := int(s.getHeight()) - 1
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == s.getHead() {
				return nil
			}
			return n
		}
		level--
	}
}

// Search returns the matched internal key and value for key (if any).
// It returns (nil, zero) when no matching version exists.
func (s *Skiplist) Search(key []byte) ([]byte, kv.ValueStruct) {
	n, _ := s.findNear(key, false, true) // findGreaterOrEqual.
	if n == nil {
		return nil, kv.ValueStruct{}
	}

	nextKey := arenaGetKey(s.arena, n.keyOffset, n.keySize)
	if !kv.SameBaseKey(key, nextKey) {
		return nil, kv.ValueStruct{}
	}

	valOffset, valSize := n.getValueOffset()
	vs := arenaGetVal(s.arena, valOffset, valSize)
	return nextKey, vs
}

// NewIterator returns a skiplist iterator with options.
func (s *Skiplist) NewIterator(opt *Options) Iterator {
	s.IncrRef()
	isAsc := true
	if opt != nil {
		isAsc = opt.IsAsc
	}
	return &SkipListIterator{list: s, isAsc: isAsc}
}

// MemSize returns the size of the Skiplist in terms of how much memory is used within its internal
// arena.
func (s *Skiplist) MemSize() int64 { return s.arena.size() }

// Draw plot Skiplist, align represents align the same node in different level
func (s *Skiplist) Draw(align bool) {
	reverseTree := make([][]string, s.getHeight())
	head := s.getHead()
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		next := head
		for {
			var nodeStr string
			next = s.getNext(next, level)
			if next != nil {
				key := next.key(s.arena)
				vs := next.getVs(s.arena)
				nodeStr = fmt.Sprintf("%s(%s)", key, vs.Value)
			} else {
				break
			}
			reverseTree[level] = append(reverseTree[level], nodeStr)
		}
	}

	// align
	if align && s.getHeight() > 1 {
		baseFloor := reverseTree[0]
		for level := 1; level < int(s.getHeight()); level++ {
			pos := 0
			for _, ele := range baseFloor {
				if pos == len(reverseTree[level]) {
					break
				}
				if ele != reverseTree[level][pos] {
					newStr := strings.Repeat("-", len(ele))
					reverseTree[level] = append(reverseTree[level][:pos+1], reverseTree[level][pos:]...)
					reverseTree[level][pos] = newStr
				}
				pos++
			}
		}
	}

	// plot
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		fmt.Printf("%d: ", level)
		for pos, ele := range reverseTree[level] {
			if pos == len(reverseTree[level])-1 {
				fmt.Printf("%s  ", ele)
			} else {
				fmt.Printf("%s->", ele)
			}
		}
		fmt.Println()
	}
}

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type SkipListIterator struct {
	list      *Skiplist
	n         *node
	e         kv.Entry
	isAsc     bool
	closeOnce sync.Once
}

func (s *SkipListIterator) Rewind() {
	if s.isAsc {
		s.SeekToFirst()
	} else {
		s.SeekToLast()
	}
}

func (s *SkipListIterator) Item() Item {
	if s == nil || s.list == nil || s.n == nil {
		return nil
	}
	vs := s.Value()
	s.e.Key = s.Key()
	s.e.Value = vs.Value
	s.e.ExpiresAt = vs.ExpiresAt
	s.e.Meta = vs.Meta
	_ = s.e.PopulateInternalMeta()
	return &s.e
}

// Close frees the resources held by the iterator
func (s *SkipListIterator) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		if s.list != nil {
			s.list.DecrRef()
		}
		s.list = nil
		s.n = nil
	})
	return nil
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *SkipListIterator) Valid() bool {
	return s != nil && s.list != nil && s.n != nil
}

// Key returns the key at the current position.
func (s *SkipListIterator) Key() []byte {
	if s == nil || s.list == nil || s.n == nil {
		return nil
	}
	return arenaGetKey(s.list.arena, s.n.keyOffset, s.n.keySize)
}

// Value returns value.
func (s *SkipListIterator) Value() kv.ValueStruct {
	if s == nil || s.list == nil || s.n == nil {
		return kv.ValueStruct{}
	}
	valOffset, valSize := s.n.getValueOffset()
	return arenaGetVal(s.list.arena, valOffset, valSize)
}

// ValueUint64 returns the uint64 value of the current node.
func (s *SkipListIterator) ValueUint64() uint64 {
	if s == nil || s.n == nil {
		return 0
	}
	return s.n.value
}

// Next advances to the next position.
func (s *SkipListIterator) Next() {
	if s == nil || s.list == nil || s.n == nil {
		return
	}
	if s.isAsc {
		s.n = s.list.getNext(s.n, 0)
	} else {
		s.n, _ = s.list.findNear(s.Key(), true, false) // find <. No equality allowed.
	}
}

// Prev advances to the previous position.
func (s *SkipListIterator) Prev() {
	if s == nil || s.list == nil || s.n == nil {
		return
	}
	s.n, _ = s.list.findNear(s.Key(), true, false) // find <. No equality allowed.
}

// Seek advances to the first entry with a key >= target (if ascending) or <= target (if descending).
func (s *SkipListIterator) Seek(target []byte) {
	if s == nil || s.list == nil {
		s.n = nil
		return
	}
	if s.isAsc {
		s.n, _ = s.list.findNear(target, false, true) // find >=.
	} else {
		s.n, _ = s.list.findNear(target, true, true) // find <=.
	}
}

// SeekForPrev finds an entry with key <= target.
func (s *SkipListIterator) SeekForPrev(target []byte) {
	if s == nil || s.list == nil {
		s.n = nil
		return
	}
	s.n, _ = s.list.findNear(target, true, true) // find <=.
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *SkipListIterator) SeekToFirst() {
	if s == nil || s.list == nil {
		s.n = nil
		return
	}
	s.n = s.list.getNext(s.list.getHead(), 0)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *SkipListIterator) SeekToLast() {
	if s == nil || s.list == nil {
		s.n = nil
		return
	}
	s.n = s.list.findLast()
}

// FastRand is a fast thread local random function.
//
//go:linkname FastRand runtime.fastrand
func FastRand() uint32

// AssertTruef is AssertTrue with extra info.
func AssertTruef(b bool, format string, args ...any) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}
