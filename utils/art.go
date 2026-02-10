package utils

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
)

func arenaAllocNode(arena *Arena) *artNode {
	if arena == nil {
		return nil
	}
	size := int(unsafe.Sizeof(artNode{}))
	align := int(unsafe.Alignof(artNode{}))
	offset := arena.allocAligned(size, align)
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
	offset := arena.allocAligned(size, align)
	payload := (*nodePayload)(arena.addr(offset))
	if payload == nil {
		return nil
	}
	payload.self = offset
	return payload
}

func arenaNodeOffset(arena *Arena, node *artNode) uint32 {
	if arena == nil || node == nil {
		return 0
	}
	return node.self
}

func arenaNodeFromOffset(arena *Arena, offset uint32) *artNode {
	if arena == nil || offset == 0 {
		return nil
	}
	return (*artNode)(arena.addr(offset))
}

func arenaPayloadOffset(arena *Arena, payload *nodePayload) uint32 {
	if arena == nil || payload == nil {
		return 0
	}
	return payload.self
}

func arenaPayloadFromOffset(arena *Arena, offset uint32) *nodePayload {
	if arena == nil || offset == 0 {
		return nil
	}
	return (*nodePayload)(arena.addr(offset))
}

// ART implements an adaptive radix tree for memtable indexing.
// Concurrency model: copy-on-write nodes with CAS installs; reads are lock-free
// and observe immutable nodes.
type ART struct {
	tree *artTree
	ref  atomic.Int32
}

// NewART creates a new adaptive radix tree with a default arena size.
func NewART(arenaSize int64) *ART {
	art := &ART{tree: newARTree(arenaSize)}
	art.ref.Store(1)
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
		Version:   entry.Version,
	}
	a.tree.Set(entry.Key, vs)
}

// Search returns the value for the earliest key >= target with the same user key.
func (a *ART) Search(key []byte) kv.ValueStruct {
	if a == nil || a.tree == nil {
		return kv.ValueStruct{}
	}
	return a.tree.Get(key)
}

// NewIterator returns a tree iterator. Options are ignored for now.
func (a *ART) NewIterator(_ *Options) Iterator {
	if a == nil || a.tree == nil {
		return nil
	}
	a.IncrRef()
	return &artIterator{tree: a.tree, owner: a}
}

// MemSize returns an approximate memory footprint.
func (a *ART) MemSize() int64 {
	if a == nil || a.tree == nil {
		return 0
	}
	return a.tree.MemSize()
}

// IncrRef increments the reference counter.
func (a *ART) IncrRef() {
	if a == nil {
		return
	}
	a.ref.Add(1)
}

// DecrRef decrements the reference counter.
func (a *ART) DecrRef() {
	if a == nil {
		return
	}
	if a.ref.Add(-1) > 0 {
		return
	}
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
	return &artTree{arena: newArena(arenaSize)}
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

func (t *artTree) Get(key []byte) kv.ValueStruct {
	if t == nil || t.arena == nil {
		return kv.ValueStruct{}
	}
	leaf := t.lowerBound(key)
	if leaf == nil {
		return kv.ValueStruct{}
	}
	if !kv.SameKey(key, leaf.leafKey(t.arena)) {
		return kv.ValueStruct{}
	}
	return leaf.loadValue(t.arena)
}

func (t *artTree) Set(key []byte, value kv.ValueStruct) {
	if t == nil || len(key) == 0 {
		return
	}
	for {
		if t.tryInsert(key, value) {
			return
		}
		runtime.Gosched()
	}
}

func (t *artTree) tryInsert(key []byte, value kv.ValueStruct) bool {
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
			return t.insertAtLeaf(parent, parentKey, node, key, value, depth)
		}

		prefix, match, cmp := matchPrefix(t.arena, node, key, depth)
		if cmp != 0 {
			return t.insertAtPrefixMismatch(parent, parentKey, node, key, value, depth, match, prefix)
		}

		depth += len(prefix)
		nextKey := keyByte(key, depth)
		next, _ := node.findChild(t.arena, nextKey)
		if next == nil {
			return t.insertAtMissingChild(parent, parentKey, node, nextKey, key, value)
		}

		parent = node
		parentKey = nextKey
		node = next
		depth++
	}
}

func (t *artTree) insertAtLeaf(parent *artNode, parentKey byte, leaf *artNode, key []byte, value kv.ValueStruct, depth int) bool {
	if bytes.Equal(leaf.leafKey(t.arena), key) {
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

func (t *artTree) insertAtPrefixMismatch(parent *artNode, parentKey byte, node *artNode, key []byte, value kv.ValueStruct, depth, match int, prefix []byte) bool {
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

func (t *artTree) insertAtMissingChild(parent *artNode, parentKey byte, node *artNode, childKey byte, key []byte, value kv.ValueStruct) bool {
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
	return parent.payloadOffset.CompareAndSwap(oldPayloadOffset, arenaPayloadOffset(t.arena, newPayload))
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

func lowerBoundNode(arena *Arena, node *artNode, key []byte, depth int) *artNode {
	if node == nil {
		return nil
	}
	if node.isLeaf() {
		if CompareKeys(node.leafKey(arena), key) >= 0 {
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
	eq, gt := node.findChild(arena, b)
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

func minLeafNode(arena *Arena, node *artNode) *artNode {
	for node != nil && !node.isLeaf() {
		node = node.minChild(arena)
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
	max := minInt(len(a), len(b)) - depth
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
	aOff := arenaNodeOffset(arena, aChild)
	bOff := arenaNodeOffset(arena, bChild)
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
	oldOff := arenaNodeOffset(arena, oldChild)
	newOff := arenaNodeOffset(arena, newChild)
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
	childOff := arenaNodeOffset(arena, child)
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type nodePayload struct {
	// count tracks active entries within the fixed-size arrays.
	count    int
	self     uint32
	keys     []byte
	children []uint32
	// idx maps [0..255] -> child index+1 for Node48 (0 == empty).
	idx []byte
}

func initPayloadForKind(arena *Arena, kind uint8) *nodePayload {
	payload := arenaAllocPayload(arena)
	if payload == nil {
		return nil
	}
	self := payload.self
	*payload = nodePayload{}
	payload.self = self
	switch kind {
	case artNode4Kind:
		payload.keys = arena.allocByteSlice(artNode4Cap, artNode4Cap)
		payload.children = arena.allocUint32Slice(artNode4Cap, artNode4Cap)
	case artNode16Kind:
		payload.keys = arena.allocByteSlice(artNode16Cap, artNode16Cap)
		payload.children = arena.allocUint32Slice(artNode16Cap, artNode16Cap)
	case artNode48Kind:
		payload.children = arena.allocUint32Slice(artNode48Cap, artNode48Cap)
		payload.idx = arena.allocByteSlice(256, 256)
	case artNode256Kind:
		payload.children = arena.allocUint32Slice(256, 256)
	}
	return payload
}

type artNode struct {
	value         atomic.Uint64
	payloadOffset atomic.Uint32
	self          uint32

	kind                 uint8
	prefixLen            uint16
	prefix               [artMaxPrefixLen]byte
	prefixOverflowOffset uint32

	// Leaf metadata lives in the arena to avoid GC pressure.
	leafKeyOffset uint32
	leafKeySize   uint16
}

func newARTNode(arena *Arena, kind uint8, prefix []byte, payload *nodePayload) *artNode {
	n := arenaAllocNode(arena)
	if n == nil {
		return nil
	}
	self := n.self
	*n = artNode{}
	n.self = self
	n.kind = kind
	n.setPrefix(arena, prefix)
	n.setPayload(arena, payload)
	return n
}

func newARTLeaf(arena *Arena, key []byte, value kv.ValueStruct) *artNode {
	leaf := arenaAllocNode(arena)
	if leaf == nil {
		return nil
	}
	self := leaf.self
	*leaf = artNode{}
	leaf.self = self
	leaf.kind = artLeafKind
	leaf.setLeafKey(arena, key)
	leaf.storeValue(arena, value)
	return leaf
}

func (n *artNode) isLeaf() bool {
	return n != nil && n.kind == artLeafKind
}

func (n *artNode) leafKey(arena *Arena) []byte {
	if n == nil || arena == nil || n.leafKeySize == 0 {
		return nil
	}
	return arena.getKey(n.leafKeyOffset, n.leafKeySize)
}

func (n *artNode) setLeafKey(arena *Arena, key []byte) {
	if n == nil || arena == nil {
		return
	}
	n.leafKeyOffset = arena.putKey(key)
	n.leafKeySize = uint16(len(key))
}

func (n *artNode) payloadPtr(arena *Arena) *nodePayload {
	if n == nil || arena == nil {
		return nil
	}
	return arenaPayloadFromOffset(arena, n.payloadOffset.Load())
}

func (n *artNode) setPayload(arena *Arena, payload *nodePayload) {
	if n == nil || arena == nil || payload == nil {
		return
	}
	n.payloadOffset.Store(arenaPayloadOffset(arena, payload))
}

func (n *artNode) loadValue(arena *Arena) kv.ValueStruct {
	if n == nil || arena == nil {
		return kv.ValueStruct{}
	}
	valOffset, valSize := decodeValue(n.value.Load())
	if valOffset == 0 && valSize == 0 {
		return kv.ValueStruct{}
	}
	return arena.getVal(valOffset, valSize)
}

func (n *artNode) storeValue(arena *Arena, vs kv.ValueStruct) {
	if n == nil || arena == nil {
		return
	}
	valOffset := arena.putVal(vs)
	n.value.Store(encodeValue(valOffset, vs.EncodedSize()))
}

func (n *artNode) prefixBytes(arena *Arena) []byte {
	if n == nil || n.prefixLen == 0 {
		return nil
	}
	if n.prefixLen <= artMaxPrefixLen {
		return n.prefix[:n.prefixLen]
	}
	// Long prefixes spill into the arena.
	if arena == nil || n.prefixOverflowOffset == 0 {
		return n.prefix[:artMaxPrefixLen]
	}
	return arena.getKey(n.prefixOverflowOffset, n.prefixLen)
}

func (n *artNode) setPrefix(arena *Arena, prefix []byte) {
	if n == nil {
		return
	}
	n.prefixLen = uint16(len(prefix))
	n.prefixOverflowOffset = 0
	if len(prefix) == 0 {
		return
	}
	if len(prefix) <= artMaxPrefixLen {
		copy(n.prefix[:], prefix)
		return
	}
	copy(n.prefix[:], prefix[:artMaxPrefixLen])
	if arena == nil {
		return
	}
	n.prefixOverflowOffset = arena.putKey(prefix)
}

func (n *artNode) findChild(arena *Arena, key byte) (*artNode, *artNode) {
	payload := n.payloadPtr(arena)
	if payload == nil {
		return nil, nil
	}
	// Return the exact child (if any) and the next greater child for lowerBound.
	switch n.kind {
	case artNode4Kind:
		for i := 0; i < payload.count; i++ {
			k := payload.keys[i]
			if k == key {
				var gt *artNode
				if i+1 < payload.count {
					gt = arenaNodeFromOffset(arena, payload.children[i+1])
				}
				return arenaNodeFromOffset(arena, payload.children[i]), gt
			}
			if k > key {
				return nil, arenaNodeFromOffset(arena, payload.children[i])
			}
		}
	case artNode16Kind:
		if payload.count == 0 {
			return nil, nil
		}
		var mask uint16
		for i := 0; i < payload.count; i++ {
			if payload.keys[i] >= key {
				mask |= uint16(1) << i
			}
		}
		if mask == 0 {
			return nil, nil
		}
		idx := bits.TrailingZeros16(mask)
		if idx >= payload.count {
			return nil, nil
		}
		if payload.keys[idx] == key {
			var gt *artNode
			if idx+1 < payload.count {
				gt = arenaNodeFromOffset(arena, payload.children[idx+1])
			}
			return arenaNodeFromOffset(arena, payload.children[idx]), gt
		}
		return nil, arenaNodeFromOffset(arena, payload.children[idx])
	case artNode48Kind:
		// Node48 stores child pointers compacted in children[0..count),
		// with idx[key] mapping to child index+1.
		if int(key) < len(payload.idx) {
			pos := payload.idx[key]
			if pos > 0 {
				idx := int(pos - 1)
				if idx < len(payload.children) {
					eq := arenaNodeFromOffset(arena, payload.children[idx])
					gt := findGreaterChild(arena, payload, int(key)+1, artNode48Kind)
					return eq, gt
				}
			}
		}
		return nil, findGreaterChild(arena, payload, int(key)+1, artNode48Kind)
	case artNode256Kind:
		// Node256 stores direct byte -> child mapping.
		if int(key) < len(payload.children) {
			eq := arenaNodeFromOffset(arena, payload.children[key])
			gt := findGreaterChild(arena, payload, int(key)+1, artNode256Kind)
			return eq, gt
		}
	}
	return nil, nil
}

func findGreaterChild(arena *Arena, payload *nodePayload, start int, kind uint8) *artNode {
	if start < 0 {
		start = 0
	}
	switch kind {
	case artNode48Kind:
		if start >= len(payload.idx) {
			return nil
		}
		for i := start; i < len(payload.idx); i++ {
			pos := payload.idx[i]
			if pos == 0 {
				continue
			}
			idx := int(pos - 1)
			if idx < len(payload.children) {
				return arenaNodeFromOffset(arena, payload.children[idx])
			}
		}
	case artNode256Kind:
		if start >= len(payload.children) {
			return nil
		}
		for i := start; i < len(payload.children); i++ {
			if payload.children[i] != 0 {
				return arenaNodeFromOffset(arena, payload.children[i])
			}
		}
	}
	return nil
}

func (n *artNode) minChild(arena *Arena) *artNode {
	payload := n.payloadPtr(arena)
	if payload == nil {
		return nil
	}
	switch n.kind {
	case artNode4Kind, artNode16Kind:
		if payload.count == 0 {
			return nil
		}
		return arenaNodeFromOffset(arena, payload.children[0])
	case artNode48Kind:
		return findGreaterChild(arena, payload, 0, artNode48Kind)
	case artNode256Kind:
		return findGreaterChild(arena, payload, 0, artNode256Kind)
	}
	return nil
}

type artIterator struct {
	tree  *artTree
	owner *ART
	curr  *artNode
	stack []iterFrame
	entry kv.Entry
}

type iterFrame struct {
	node *artNode
	idx  int
}

func (it *artIterator) Next() {
	if it.curr == nil {
		return
	}
	if it.tree == nil || it.tree.arena == nil {
		it.curr = nil
		return
	}
	// Walk to the next leaf by climbing until we find the next child.
	it.advance()
}

func (it *artIterator) Valid() bool {
	return it != nil && it.curr != nil
}

func (it *artIterator) Rewind() {
	if it == nil || it.tree == nil {
		return
	}
	it.stack = it.stack[:0]
	root := it.tree.root.Load()
	it.descendToMin(root)
}

func (it *artIterator) Item() Item {
	if it == nil || it.curr == nil || it.tree == nil || it.tree.arena == nil {
		return nil
	}
	arena := it.tree.arena
	vs := it.curr.loadValue(arena)
	it.entry.Key = it.curr.leafKey(arena)
	it.entry.Value = vs.Value
	it.entry.ExpiresAt = vs.ExpiresAt
	it.entry.Meta = vs.Meta
	it.entry.Version = vs.Version
	return &it.entry
}

func (it *artIterator) Close() error {
	if it == nil || it.owner == nil {
		return nil
	}
	it.owner.DecrRef()
	return nil
}

func (it *artIterator) Seek(key []byte) {
	if it == nil || it.tree == nil {
		return
	}
	it.stack = it.stack[:0]
	leaf := it.tree.lowerBound(key)
	if leaf == nil {
		it.curr = nil
		return
	}
	it.buildStackToLeaf(leaf)
}

func (it *artIterator) descendToMin(node *artNode) {
	if it == nil || it.tree == nil || it.tree.arena == nil {
		it.curr = nil
		return
	}
	for node != nil && !node.isLeaf() {
		child, nextIdx := it.nextChild(node, 0)
		if child == nil {
			it.curr = nil
			return
		}
		it.stack = append(it.stack, iterFrame{node: node, idx: nextIdx})
		node = child
	}
	it.curr = node
}

func (it *artIterator) advance() {
	for len(it.stack) > 0 {
		top := &it.stack[len(it.stack)-1]
		child, nextIdx := it.nextChild(top.node, top.idx)
		if child == nil {
			it.stack = it.stack[:len(it.stack)-1]
			continue
		}
		top.idx = nextIdx
		it.descendToMin(child)
		return
	}
	it.curr = nil
}

func (it *artIterator) nextChild(node *artNode, idx int) (*artNode, int) {
	if node == nil || it.tree == nil || it.tree.arena == nil {
		return nil, 0
	}
	arena := it.tree.arena
	payload := node.payloadPtr(arena)
	if payload == nil {
		return nil, 0
	}
	if idx < 0 {
		idx = 0
	}
	switch node.kind {
	case artNode4Kind, artNode16Kind:
		for i := idx; i < payload.count; i++ {
			child := arenaNodeFromOffset(arena, payload.children[i])
			if child != nil {
				return child, i + 1
			}
		}
	case artNode48Kind:
		for i := idx; i < len(payload.idx); i++ {
			pos := payload.idx[i]
			if pos == 0 {
				continue
			}
			childIdx := int(pos - 1)
			if childIdx >= len(payload.children) {
				continue
			}
			child := arenaNodeFromOffset(arena, payload.children[childIdx])
			if child != nil {
				return child, i + 1
			}
		}
	case artNode256Kind:
		for i := idx; i < len(payload.children); i++ {
			if payload.children[i] == 0 {
				continue
			}
			child := arenaNodeFromOffset(arena, payload.children[i])
			if child != nil {
				return child, i + 1
			}
		}
	}
	return nil, 0
}

func (it *artIterator) childForKey(node *artNode, key byte) (*artNode, int) {
	if node == nil || it.tree == nil || it.tree.arena == nil {
		return nil, 0
	}
	arena := it.tree.arena
	payload := node.payloadPtr(arena)
	if payload == nil {
		return nil, 0
	}
	switch node.kind {
	case artNode4Kind, artNode16Kind:
		for i := 0; i < payload.count; i++ {
			if payload.keys[i] == key {
				child := arenaNodeFromOffset(arena, payload.children[i])
				if child == nil {
					return nil, 0
				}
				return child, i + 1
			}
		}
	case artNode48Kind:
		pos := payload.idx[key]
		if pos == 0 {
			return nil, 0
		}
		childIdx := int(pos - 1)
		if childIdx >= len(payload.children) {
			return nil, 0
		}
		child := arenaNodeFromOffset(arena, payload.children[childIdx])
		if child == nil {
			return nil, 0
		}
		return child, int(key) + 1
	case artNode256Kind:
		if payload.children[key] == 0 {
			return nil, 0
		}
		child := arenaNodeFromOffset(arena, payload.children[key])
		if child == nil {
			return nil, 0
		}
		return child, int(key) + 1
	}
	return nil, 0
}

func (it *artIterator) buildStackToLeaf(leaf *artNode) {
	if leaf == nil || it.tree == nil || it.tree.arena == nil {
		it.curr = nil
		it.stack = it.stack[:0]
		return
	}
	arena := it.tree.arena
	key := leaf.leafKey(arena)
	node := it.tree.root.Load()
	depth := 0
	it.stack = it.stack[:0]
	for node != nil && !node.isLeaf() {
		prefix, match, cmp := matchPrefix(arena, node, key, depth)
		if cmp != 0 || match < len(prefix) {
			it.curr = nil
			it.stack = it.stack[:0]
			return
		}
		depth += len(prefix)
		childKey := keyByte(key, depth)
		child, nextIdx := it.childForKey(node, childKey)
		if child == nil {
			it.curr = nil
			it.stack = it.stack[:0]
			return
		}
		it.stack = append(it.stack, iterFrame{node: node, idx: nextIdx})
		node = child
		depth++
	}
	it.curr = node
}
