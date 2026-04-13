/*
ART-based memtable index specialized for NoKV internal keys.

Key design points:
- This implementation is internal-key-only. It is not a general-purpose radix tree.
- Leaf nodes store two key forms: a private route key for trie ordering and the
  original canonical internal key for external semantics.
- The primary concurrency model is copy-on-write plus CAS. Published payloads are
  immutable; writers clone payloads, apply updates on the clone, and atomically
  publish the new pointer.
- Arena allocation ties node lifetime to the memtable lifetime, avoiding per-node GC.
*/

package index

import (
	"bytes"
	"math/bits"
	"runtime"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/feichai0017/NoKV/kv"
)

const (
	artLeafKind uint8 = iota
	artNode4Kind
	artNode16Kind
	artNode48Kind
	artNode256Kind
)

const (
	artNode4Cap     = 4
	artNode16Cap    = 16
	artNode48Cap    = 48
	artMaxPrefixLen = 16
	artCmpGroupSize = 8
	artCmpGroupEnd  = 0xFF
)

type artValueRef struct {
	expiresAt uint64
	valueSize uint32
	meta      byte
	_         [3]byte
}

var (
	artValueRefSize  = uint32(unsafe.Sizeof(artValueRef{}))
	artValueRefAlign = int(unsafe.Alignof(artValueRef{}))
)

func arenaAllocNode(arena *Arena) *artNode {
	if arena == nil {
		return nil
	}
	size := int(unsafe.Sizeof(artNode{}))
	align := int(unsafe.Alignof(artNode{}))
	offset := arena.AllocAligned(size, align)
	node := (*artNode)(arena.addr(offset))
	if node == nil {
		return nil
	}
	node.self = offset
	return node
}

func arenaAllocPayload(arena *Arena) *nodePayload {
	if arena == nil {
		return nil
	}
	size := int(unsafe.Sizeof(nodePayload{}))
	align := int(unsafe.Alignof(nodePayload{}))
	offset := arena.AllocAligned(size, align)
	payload := (*nodePayload)(arena.addr(offset))
	if payload == nil {
		return nil
	}
	payload.self = offset
	return payload
}

func arenaNodeFromOffset(arena *Arena, offset uint32) *artNode {
	if arena == nil || offset == 0 {
		return nil
	}
	return (*artNode)(arena.addr(offset))
}

func arenaPutArtValue(arena *Arena, vs kv.ValueStruct) uint32 {
	if arena == nil {
		return 0
	}
	size := int(artValueRefSize) + len(vs.Value)
	if size <= 0 {
		return 0
	}
	offset := arena.AllocAligned(size, artValueRefAlign)
	ref := (*artValueRef)(arena.addr(offset))
	ref.expiresAt = vs.ExpiresAt
	ref.valueSize = uint32(len(vs.Value))
	ref.meta = vs.Meta
	if len(vs.Value) > 0 {
		buf := arena.bytesAt(offset+artValueRefSize, len(vs.Value))
		copy(buf, vs.Value)
	}
	return offset
}

func arenaGetArtValue(arena *Arena, offset uint32) kv.ValueStruct {
	if arena == nil || offset == 0 {
		return kv.ValueStruct{}
	}
	ref := (*artValueRef)(arena.addr(offset))
	if ref == nil {
		return kv.ValueStruct{}
	}
	var value []byte
	if ref.valueSize != 0 {
		value = arena.bytesAt(offset+artValueRefSize, int(ref.valueSize))
	} else if ptr := (*byte)(arena.addr(offset + artValueRefSize)); ptr != nil {
		value = unsafe.Slice(ptr, 0)
	}
	return kv.ValueStruct{
		Meta:      ref.meta,
		Value:     value,
		ExpiresAt: ref.expiresAt,
	}
}

func arenaPayloadFromOffset(arena *Arena, offset uint32) *nodePayload {
	if arena == nil || offset == 0 {
		return nil
	}
	return (*nodePayload)(arena.addr(offset))
}

func node16LowerBoundIndex(keys []byte, count int, key byte) (idx int, exact bool) {
	if count <= 0 {
		return -1, false
	}
	var mask uint16
	for i := range count {
		if keys[i] >= key {
			mask |= uint16(1) << i
		}
	}
	if mask == 0 {
		return -1, false
	}
	idx = bits.TrailingZeros16(mask)
	if idx >= count {
		return -1, false
	}
	return idx, keys[idx] == key
}

func node16ExactIndex(keys []byte, count int, key byte) int {
	if count <= 0 {
		return -1
	}
	var mask uint16
	for i := range count {
		if keys[i] == key {
			mask |= uint16(1) << i
		}
	}
	if mask == 0 {
		return -1
	}
	idx := bits.TrailingZeros16(mask)
	if idx >= count {
		return -1
	}
	return idx
}

func node16UpperBoundIndex(keys []byte, count int, key byte) (idx int, exact bool) {
	if count <= 0 {
		return -1, false
	}
	var mask uint16
	for i := range count {
		if keys[i] <= key {
			mask |= uint16(1) << i
		}
	}
	if mask == 0 {
		return -1, false
	}
	idx = 15 - bits.LeadingZeros16(mask)
	return idx, keys[idx] == key
}

func artEncodeComparableKeyTo(dst []byte, key []byte) []byte {
	artRequireInternalKey(key)
	need := artRouteKeyLen(key)
	if cap(dst) < need {
		dst = make([]byte, need)
	} else {
		dst = dst[:need]
	}
	n := artEncodeComparablePrefixTo(dst, key[:len(key)-8])
	copy(dst[n:], key[len(key)-8:])
	return dst[:n+8]
}

func artPutComparableKey(arena *Arena, key []byte) (uint32, uint16) {
	if arena == nil {
		return 0, 0
	}
	artRequireInternalKey(key)
	size := artRouteKeyLen(key)
	offset := arena.Allocate(uint32(size))
	buf := arena.bytesAt(offset, size)
	n := artEncodeComparablePrefixTo(buf, key[:len(key)-8])
	copy(buf[n:], key[len(key)-8:])
	return offset, uint16(size)
}

func artEncodeComparablePrefixTo(dst []byte, src []byte) int {
	switch {
	case len(src) < artCmpGroupSize:
		return artEncodeOrderedBytes(dst, src)
	case len(src) < 2*artCmpGroupSize:
		copy(dst[:artCmpGroupSize], src[:artCmpGroupSize])
		dst[artCmpGroupSize] = artCmpGroupEnd
		remain := src[artCmpGroupSize:]
		out := dst[artCmpGroupSize+1:]
		copy(out[:len(remain)], remain)
		for i := len(remain); i < artCmpGroupSize; i++ {
			out[i] = 0
		}
		out[artCmpGroupSize] = artCmpGroupEnd - byte(artCmpGroupSize-len(remain))
		return (2 * artCmpGroupSize) + 2
	case len(src) == 2*artCmpGroupSize:
		copy(dst[:artCmpGroupSize], src[:artCmpGroupSize])
		dst[artCmpGroupSize] = artCmpGroupEnd
		out := dst[artCmpGroupSize+1:]
		copy(out[:artCmpGroupSize], src[artCmpGroupSize:])
		out[artCmpGroupSize] = artCmpGroupEnd
		out = out[artCmpGroupSize+1:]
		for i := range artCmpGroupSize {
			out[i] = 0
		}
		out[artCmpGroupSize] = artCmpGroupEnd - artCmpGroupSize
		return (3 * artCmpGroupSize) + 3
	default:
		return artEncodeOrderedBytes(dst, src)
	}
}

func artEncodeOrderedBytes(dst []byte, src []byte) int {
	if len(src) < artCmpGroupSize {
		copy(dst[:len(src)], src)
		for i := len(src); i < artCmpGroupSize; i++ {
			dst[i] = 0
		}
		dst[artCmpGroupSize] = artCmpGroupEnd - byte(artCmpGroupSize-len(src))
		return artCmpGroupSize + 1
	}
	out := dst
	remain := src
	for len(remain) >= artCmpGroupSize {
		copy(out[:artCmpGroupSize], remain[:artCmpGroupSize])
		out[artCmpGroupSize] = artCmpGroupEnd
		out = out[artCmpGroupSize+1:]
		remain = remain[artCmpGroupSize:]
	}

	pad := artCmpGroupSize - len(remain)
	copy(out[:len(remain)], remain)
	for i := len(remain); i < artCmpGroupSize; i++ {
		out[i] = 0
	}
	out[artCmpGroupSize] = artCmpGroupEnd - byte(pad)
	return len(dst) - len(out) + artCmpGroupSize + 1
}

func splitLeaf(arena *Arena, existing, incoming *artNode, depth int) *artNode {
	existingKey := existing.leafKey(arena)
	incomingKey := incoming.leafKey(arena)
	common := longestCommonPrefix(existingKey, incomingKey, depth)
	existingKeyByte := keyByte(existingKey, depth+common)
	incomingKeyByte := keyByte(incomingKey, depth+common)
	return newTwoChildNode(arena, incomingKey[depth:depth+common], existingKeyByte, existing, incomingKeyByte, incoming)
}

func splitPrefix(arena *Arena, node, incoming *artNode, depth int, match int) *artNode {
	nodePrefix := node.prefixBytes(arena)
	existingKey := nodePrefix[match]
	incomingKey := keyByte(incoming.leafKey(arena), depth+match)

	trimmed := cloneInnerNode(arena, node, node.kind, nodePrefix[match+1:], node.payloadPtr(arena))
	if trimmed == nil {
		return nil
	}
	return newTwoChildNode(arena, nodePrefix[:match], existingKey, trimmed, incomingKey, incoming)
}

func newTwoChildNode(arena *Arena, prefix []byte, aKey byte, aChild *artNode, bKey byte, bChild *artNode) *artNode {
	parent := newARTNode(arena, artNode4Kind, prefix, nil)
	if parent == nil {
		return nil
	}
	payload := initPayloadForKind(arena, artNode4Kind)
	if payload == nil {
		return nil
	}
	payload.count = 2
	aOff := aChild.self
	bOff := bChild.self
	if aKey <= bKey {
		payload.keys[0] = aKey
		payload.keys[1] = bKey
		payload.children[0] = aOff
		payload.children[1] = bOff
		parent.setPayload(arena, payload)
		return parent
	}
	payload.keys[0] = bKey
	payload.keys[1] = aKey
	payload.children[0] = bOff
	payload.children[1] = aOff
	parent.setPayload(arena, payload)
	return parent
}

func cloneInnerNode(arena *Arena, src *artNode, kind uint8, prefix []byte, payload *nodePayload) *artNode {
	if arena == nil || src == nil {
		return nil
	}
	n := arenaAllocNode(arena)
	if n == nil {
		return nil
	}
	self := n.self
	*n = artNode{}
	n.self = self
	n.kind = kind
	if prefix == nil {
		n.prefixLen = src.prefixLen
		n.prefix = src.prefix
		n.prefixOverflowOffset = src.prefixOverflowOffset
	} else {
		n.setPrefix(arena, prefix)
	}
	if payload != nil {
		n.setPayload(arena, payload)
	}
	return n
}

func insertChild(arena *Arena, node *artNode, key byte, child *artNode) (*artNode, bool) {
	if arena == nil || node == nil || child == nil {
		return nil, false
	}
	payload := node.payloadPtr(arena)
	newPayload, newKind, ok := clonePayloadInsert(arena, payload, node.kind, key, child)
	if !ok || newPayload == nil {
		return nil, false
	}
	return cloneInnerNode(arena, node, newKind, nil, newPayload), true
}

func clonePayloadReplaceChild(arena *Arena, payload *nodePayload, kind uint8, key byte, oldChild, newChild *artNode) *nodePayload {
	if arena == nil || payload == nil {
		return nil
	}
	oldOff := oldChild.self
	newOff := newChild.self
	newPayload := clonePayloadToKind(arena, payload, kind, kind)
	if newPayload == nil {
		return nil
	}
	if !payloadReplace(kind, newPayload, key, oldOff, newOff) {
		return nil
	}
	return newPayload
}

func clonePayloadInsert(arena *Arena, payload *nodePayload, kind uint8, key byte, child *artNode) (*nodePayload, uint8, bool) {
	if arena == nil || child == nil {
		return nil, kind, false
	}
	count := 0
	if payload != nil {
		count = payload.count
	}
	childOff := child.self
	targetKind := kind
	switch kind {
	case artNode4Kind:
		if count >= artNode4Cap {
			targetKind = artNode16Kind
		}
	case artNode16Kind:
		if count >= artNode16Cap {
			targetKind = artNode48Kind
		}
	case artNode48Kind:
		if count >= artNode48Cap {
			targetKind = artNode256Kind
		}
	case artNode256Kind:
		if count >= 256 {
			return nil, artNode256Kind, false
		}
	}

	newPayload := clonePayloadToKind(arena, payload, kind, targetKind)
	if newPayload == nil {
		return nil, targetKind, false
	}
	if !payloadInsert(targetKind, newPayload, key, childOff) {
		return nil, targetKind, false
	}
	return newPayload, targetKind, true
}

func clonePayloadToKind(arena *Arena, payload *nodePayload, fromKind, toKind uint8) *nodePayload {
	switch toKind {
	case artNode4Kind, artNode16Kind:
		if fromKind != artNode4Kind && fromKind != artNode16Kind {
			return nil
		}
	case artNode48Kind:
		if fromKind != artNode4Kind && fromKind != artNode16Kind && fromKind != artNode48Kind {
			return nil
		}
	case artNode256Kind:
		if fromKind != artNode4Kind && fromKind != artNode16Kind && fromKind != artNode48Kind && fromKind != artNode256Kind {
			return nil
		}
	default:
		return nil
	}

	newPayload := initPayloadForKind(arena, toKind)
	if newPayload == nil {
		return nil
	}
	if payload == nil || payload.count == 0 {
		return newPayload
	}
	newPayload.count = payload.count

	switch toKind {
	case artNode4Kind, artNode16Kind:
		copy(newPayload.keys, payload.keys[:payload.count])
		copy(newPayload.children, payload.children[:payload.count])
	case artNode48Kind:
		if fromKind == artNode48Kind {
			copy(newPayload.idx, payload.idx)
			copy(newPayload.children, payload.children)
			return newPayload
		}
		for i := 0; i < payload.count; i++ {
			k := payload.keys[i]
			newPayload.idx[k] = byte(i + 1)
			newPayload.children[i] = payload.children[i]
		}
	case artNode256Kind:
		switch fromKind {
		case artNode256Kind:
			copy(newPayload.children, payload.children)
		case artNode48Kind:
			for i := 0; i < len(payload.idx); i++ {
				pos := payload.idx[i]
				if pos == 0 {
					continue
				}
				idx := int(pos - 1)
				if idx < len(payload.children) {
					newPayload.children[i] = payload.children[idx]
				}
			}
		default:
			for i := 0; i < payload.count; i++ {
				k := payload.keys[i]
				newPayload.children[k] = payload.children[i]
			}
		}
	}
	return newPayload
}

func payloadInsert(kind uint8, payload *nodePayload, key byte, childOff uint32) bool {
	switch kind {
	case artNode4Kind, artNode16Kind:
		if payload.count > len(payload.keys) || payload.count > len(payload.children) {
			return false
		}
		if payload.count >= len(payload.keys) {
			return false
		}
		idx := sort.Search(payload.count, func(i int) bool { return payload.keys[i] >= key })
		if idx < payload.count && payload.keys[idx] == key {
			return false
		}
		copy(payload.keys[idx+1:], payload.keys[idx:payload.count])
		copy(payload.children[idx+1:], payload.children[idx:payload.count])
		payload.keys[idx] = key
		payload.children[idx] = childOff
		payload.count++
		return true
	case artNode48Kind:
		if int(key) >= len(payload.idx) || len(payload.children) == 0 {
			return false
		}
		if payload.count >= artNode48Cap {
			return false
		}
		if payload.idx[key] != 0 {
			return false
		}
		payload.children[payload.count] = childOff
		payload.idx[key] = byte(payload.count + 1)
		payload.count++
		return true
	case artNode256Kind:
		if int(key) >= len(payload.children) {
			return false
		}
		if payload.children[key] != 0 {
			return false
		}
		payload.children[key] = childOff
		payload.count++
		return true
	default:
		return false
	}
}

func payloadReplace(kind uint8, payload *nodePayload, key byte, oldOff, newOff uint32) bool {
	switch kind {
	case artNode4Kind, artNode16Kind:
		if payload.count > len(payload.keys) || payload.count > len(payload.children) {
			return false
		}
		for i := 0; i < payload.count; i++ {
			if payload.keys[i] != key {
				continue
			}
			if payload.children[i] != oldOff {
				return false
			}
			payload.children[i] = newOff
			return true
		}
	case artNode48Kind:
		if int(key) >= len(payload.idx) || len(payload.children) == 0 {
			return false
		}
		pos := payload.idx[key]
		if pos == 0 {
			return false
		}
		idx := int(pos - 1)
		if idx >= len(payload.children) || payload.children[idx] != oldOff {
			return false
		}
		payload.children[idx] = newOff
		return true
	case artNode256Kind:
		if int(key) >= len(payload.children) {
			return false
		}
		if payload.children[key] != oldOff {
			return false
		}
		payload.children[key] = newOff
		return true
	}
	return false
}

// ART implements an adaptive radix tree for memtable indexing.
// Keys must use the internal-key layout; the tree stores a private
// mem-comparable route key plus the original internal key per leaf.
// Concurrency model: copy-on-write nodes with CAS installs; reads are lock-free
// and observe immutable nodes.
type ART struct {
	tree *artTree
	kv.RefCount
}

// NewART creates a new adaptive radix tree with a default arena size.
func NewART(arenaSize int64) *ART {
	art := &ART{tree: newARTree(arenaSize)}
	art.Init(1)
	return art
}

// Add inserts or replaces the entry.
func (a *ART) Add(entry *kv.Entry) {
	if a == nil || a.tree == nil || entry == nil {
		return
	}
	vs := kv.ValueStruct{
		Meta:      entry.Meta,
		Value:     entry.Value,
		ExpiresAt: entry.ExpiresAt,
	}
	a.tree.Set(entry.Key, vs)
}

// Search returns the matched internal key and value for key (if any).
// It returns (nil, zero) when no matching version exists.
func (a *ART) Search(key []byte) ([]byte, kv.ValueStruct) {
	if a == nil || a.tree == nil {
		return nil, kv.ValueStruct{}
	}
	var scratch [256]byte
	routeKey := artEncodeComparableKeyTo(scratch[:0], key)
	foundKey, vs := a.tree.Get(routeKey)
	if len(foundKey) == 0 {
		return nil, kv.ValueStruct{}
	}
	if !kv.SameBaseKey(key, foundKey) {
		return nil, kv.ValueStruct{}
	}
	return foundKey, vs
}

// NewIterator returns a tree iterator with directional semantics from options.
// Nil options default to ascending iteration.
func (a *ART) NewIterator(opt *Options) Iterator {
	if a == nil || a.tree == nil {
		return nil
	}
	isAsc := true
	if opt != nil {
		isAsc = opt.IsAsc
	}
	a.IncrRef()
	return &artIterator{tree: a.tree, owner: a, isAsc: isAsc}
}

// MemSize returns an approximate memory footprint.
func (a *ART) MemSize() int64 {
	if a == nil || a.tree == nil {
		return 0
	}
	return a.tree.MemSize()
}

// IncrRef increments the reference counter.
// It shadows RefCount.Incr to add nil-receiver safety.
func (a *ART) IncrRef() {
	if a == nil {
		return
	}
	a.RefCount.Incr()
}

// DecrRef decrements the reference counter and releases the tree when it
// reaches zero. It panics on refcount underflow (decrement past zero) which
// indicates a bug in the caller's lifetime management.
func (a *ART) DecrRef() {
	if a == nil {
		return
	}
	if a.Decr() > 0 {
		return
	}
	// ref == 0: last reference dropped — release the tree.
	if a.tree != nil {
		a.tree.release()
	}
	a.tree = nil
}

type artTree struct {
	// COW + CAS: readers are lock-free; writers clone payloads and CAS root/parent.
	root  atomic.Pointer[artNode]
	arena *Arena
}

func newARTree(arenaSize int64) *artTree {
	if arenaSize <= 0 {
		arenaSize = DefaultArenaSize
	}
	return &artTree{arena: NewArena(arenaSize)}
}

func (t *artTree) MemSize() int64 {
	if t == nil || t.arena == nil {
		return 0
	}
	return t.arena.size()
}

func (t *artTree) release() {
	if t == nil {
		return
	}
	t.root.Store(nil)
	t.arena = nil
}

func (t *artTree) Get(key []byte) ([]byte, kv.ValueStruct) {
	if t == nil || t.arena == nil {
		return nil, kv.ValueStruct{}
	}
	leaf := t.lowerBound(key)
	if leaf == nil {
		return nil, kv.ValueStruct{}
	}
	return leaf.originalKey(t.arena), leaf.loadValue(t.arena)
}

func (t *artTree) Set(key []byte, value kv.ValueStruct) {
	if t == nil || len(key) == 0 {
		return
	}
	var scratch [256]byte
	routeKey := artEncodeComparableKeyTo(scratch[:0], key)
	for {
		if t.tryInsert(key, routeKey, value) {
			return
		}
		runtime.Gosched()
	}
}

func (t *artTree) tryInsert(key []byte, routeKey []byte, value kv.ValueStruct) bool {
	root := t.root.Load()
	if root == nil {
		leaf := newARTLeaf(t.arena, key, value)
		return t.root.CompareAndSwap(nil, leaf)
	}

	node := root
	depth := 0
	var parent *artNode
	var parentKey byte

	for {
		if node.isLeaf() {
			return t.insertAtLeaf(parent, parentKey, node, key, routeKey, value, depth)
		}

		prefix, match, cmp := matchPrefix(t.arena, node, routeKey, depth)
		if cmp != 0 {
			return t.insertAtPrefixMismatch(parent, parentKey, node, key, routeKey, value, depth, match, prefix)
		}

		depth += len(prefix)
		nextKey := keyByte(routeKey, depth)
		next := lookupExactPayload(t.arena, node.kind, node.payloadPtr(t.arena), nextKey)
		if next == nil {
			return t.insertAtMissingChild(parent, parentKey, node, nextKey, key, routeKey, value)
		}

		parent = node
		parentKey = nextKey
		node = next
		depth++
	}
}

func (t *artTree) insertAtLeaf(parent *artNode, parentKey byte, leaf *artNode, key []byte, routeKey []byte, value kv.ValueStruct, depth int) bool {
	if bytes.Equal(leaf.leafKey(t.arena), routeKey) {
		leaf.storeValue(t.arena, value)
		return true
	}
	newLeaf := newARTLeaf(t.arena, key, value)
	if newLeaf == nil {
		return false
	}
	newParent := splitLeaf(t.arena, leaf, newLeaf, depth)
	if newParent == nil {
		return false
	}
	return t.replaceChild(parent, parentKey, leaf, newParent)
}

func (t *artTree) insertAtPrefixMismatch(parent *artNode, parentKey byte, node *artNode, key []byte, routeKey []byte, value kv.ValueStruct, depth, match int, prefix []byte) bool {
	newLeaf := newARTLeaf(t.arena, key, value)
	if newLeaf == nil {
		return false
	}
	if len(prefix) == 0 {
		return false
	}
	newParent := splitPrefix(t.arena, node, newLeaf, depth, match)
	if newParent == nil {
		return false
	}
	return t.replaceChild(parent, parentKey, node, newParent)
}

func (t *artTree) insertAtMissingChild(parent *artNode, parentKey byte, node *artNode, childKey byte, key []byte, routeKey []byte, value kv.ValueStruct) bool {
	newLeaf := newARTLeaf(t.arena, key, value)
	if newLeaf == nil {
		return false
	}
	newNode, ok := insertChild(t.arena, node, childKey, newLeaf)
	if !ok || newNode == nil {
		return false
	}
	return t.replaceChild(parent, parentKey, node, newNode)
}

func (t *artTree) replaceChild(parent *artNode, parentKey byte, oldChild, newChild *artNode) bool {
	if parent == nil {
		return t.root.CompareAndSwap(oldChild, newChild)
	}
	oldPayloadOffset := parent.payloadOffset.Load()
	payload := arenaPayloadFromOffset(t.arena, oldPayloadOffset)
	if payload == nil {
		return false
	}
	newPayload := clonePayloadReplaceChild(t.arena, payload, parent.kind, parentKey, oldChild, newChild)
	if newPayload == nil {
		return false
	}
	return parent.payloadOffset.CompareAndSwap(oldPayloadOffset, newPayload.self)
}

func (t *artTree) lowerBound(key []byte) *artNode {
	if t == nil || t.arena == nil {
		return nil
	}
	root := t.root.Load()
	if root == nil {
		return nil
	}
	return lowerBoundNode(t.arena, root, key, 0)
}

func (t *artTree) upperBound(key []byte) *artNode {
	if t == nil || t.arena == nil {
		return nil
	}
	root := t.root.Load()
	if root == nil {
		return nil
	}
	return upperBoundNode(t.arena, root, key, 0)
}

func lowerBoundNode(arena *Arena, node *artNode, key []byte, depth int) *artNode {
	if node == nil {
		return nil
	}
	if node.isLeaf() {
		if bytes.Compare(node.leafKey(arena), key) >= 0 {
			return node
		}
		return nil
	}
	prefix, match, cmp := matchPrefix(arena, node, key, depth)
	prefixLen := len(prefix)
	if cmp < 0 {
		child := node.minChild(arena)
		return minLeafNode(arena, child)
	}
	if cmp > 0 {
		return nil
	}
	depth += match
	if match < prefixLen {
		return nil
	}
	b := keyByte(key, depth)
	eq, gt := lookupGEPayload(arena, node.kind, node.payloadPtr(arena), b)
	if eq != nil {
		res := lowerBoundNode(arena, eq, key, depth+1)
		if res != nil {
			return res
		}
	}
	if gt != nil {
		return minLeafNode(arena, gt)
	}
	return nil
}

func upperBoundNode(arena *Arena, node *artNode, key []byte, depth int) *artNode {
	if node == nil {
		return nil
	}
	if node.isLeaf() {
		if bytes.Compare(node.leafKey(arena), key) <= 0 {
			return node
		}
		return nil
	}
	prefix, match, cmp := matchPrefix(arena, node, key, depth)
	prefixLen := len(prefix)
	if cmp < 0 {
		return nil
	}
	if cmp > 0 {
		child := node.maxChild(arena)
		return maxLeafNode(arena, child)
	}
	depth += match
	if match < prefixLen {
		return nil
	}
	b := keyByte(key, depth)
	eq, lt := lookupLEPayload(arena, node.kind, node.payloadPtr(arena), b)
	if eq != nil {
		res := upperBoundNode(arena, eq, key, depth+1)
		if res != nil {
			return res
		}
	}
	if lt != nil {
		return maxLeafNode(arena, lt)
	}
	return nil
}

func minLeafNode(arena *Arena, node *artNode) *artNode {
	for node != nil && !node.isLeaf() {
		node = node.minChild(arena)
	}
	return node
}

func maxLeafNode(arena *Arena, node *artNode) *artNode {
	for node != nil && !node.isLeaf() {
		node = node.maxChild(arena)
	}
	return node
}

func matchPrefix(arena *Arena, node *artNode, key []byte, depth int) (prefix []byte, match int, cmp int) {
	prefix = node.prefixBytes(arena)
	if len(prefix) == 0 {
		return prefix, 0, 0
	}
	for i := range prefix {
		kb := keyByte(key, depth+i)
		if kb == prefix[i] {
			continue
		}
		if kb < prefix[i] {
			return prefix, i, -1
		}
		return prefix, i, 1
	}
	return prefix, len(prefix), 0
}

func longestCommonPrefix(a, b []byte, depth int) int {
	max := min(len(a), len(b)) - depth
	if max < 0 {
		return 0
	}
	for i := range max {
		if keyByte(a, depth+i) != keyByte(b, depth+i) {
			return i
		}
	}
	return max
}

func keyByte(key []byte, depth int) byte {
	if depth < len(key) {
		return key[depth]
	}
	return 0
}

func artComparableEncodedLen(n int) int {
	return ((n / artCmpGroupSize) + 1) * (artCmpGroupSize + 1)
}

func artRouteKeyLen(key []byte) int {
	artRequireInternalKey(key)
	return artComparableEncodedLen(len(key)-8) + 8
}

func artRequireInternalKey(key []byte) {
	if len(key) <= 8 {
		panic("ART requires internal keys")
	}
}
