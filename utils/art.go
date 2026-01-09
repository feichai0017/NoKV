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
	artNodeLockBit  = 1
)

func arenaAllocNode(arena *Arena) *artNode {
	if arena == nil {
		return nil
	}
	size := int(unsafe.Sizeof(artNode{}))
	align := int(unsafe.Alignof(artNode{}))
	offset := arena.allocAligned(size, align)
	return (*artNode)(unsafe.Pointer(&arena.buf[offset]))
}

func arenaAllocPayload(arena *Arena) *nodePayload {
	if arena == nil {
		return nil
	}
	size := int(unsafe.Sizeof(nodePayload{}))
	align := int(unsafe.Alignof(nodePayload{}))
	offset := arena.allocAligned(size, align)
	return (*nodePayload)(unsafe.Pointer(&arena.buf[offset]))
}

func arenaNodeOffset(arena *Arena, node *artNode) uint32 {
	if arena == nil || node == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(node)) - uintptr(unsafe.Pointer(&arena.buf[0])))
}

func arenaNodeFromOffset(arena *Arena, offset uint32) *artNode {
	if arena == nil || offset == 0 {
		return nil
	}
	return (*artNode)(unsafe.Pointer(&arena.buf[offset]))
}

func arenaPayloadOffset(arena *Arena, payload *nodePayload) uint32 {
	if arena == nil || payload == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(payload)) - uintptr(unsafe.Pointer(&arena.buf[0])))
}

func arenaPayloadFromOffset(arena *Arena, offset uint32) *nodePayload {
	if arena == nil || offset == 0 {
		return nil
	}
	return (*nodePayload)(unsafe.Pointer(&arena.buf[offset]))
}

// ART implements an adaptive radix tree for memtable indexing.
// Concurrency model: optimistic reads (version validation) + write locks on
// the mutation path. Ordered iteration walks the tree in key order.
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
	// OLC: readers validate node versions, writers lock nodes on the path.
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

	// OLC invariants:
	// - Writers lock nodes in top-down order and keep the lock until the mutation is finalized.
	// - Readers use version checks to detect concurrent writers and restart if needed.
	node := root
	node.writeLock()
	if t.root.Load() != root {
		node.writeUnlock()
		return false
	}

	// Lock-coupled descent: hold each node lock until we finalize the mutation.
	locked := []*artNode{node}
	depth := 0
	var parent *artNode
	var parentKey byte

	for {
		if node.isLeaf() {
			return t.insertAtLeaf(locked, parent, parentKey, node, key, value, depth)
		}

		_, match, cmp := matchPrefix(t.arena, node, key, depth)
		if cmp != 0 {
			return t.insertAtPrefixMismatch(locked, parent, parentKey, node, key, value, depth, match)
		}

		next, nextKey, nextDepth := t.prepareChildInsert(node, depth, key)
		if next == nil {
			return t.insertAtMissingChild(locked, node, nextKey, key, value)
		}

		parent = node
		parentKey = nextKey
		node = next
		node.writeLock()
		locked = append(locked, node)
		depth = nextDepth
	}
}

func (t *artTree) insertAtLeaf(locked []*artNode, parent *artNode, parentKey byte, leaf *artNode, key []byte, value kv.ValueStruct, depth int) bool {
	if bytes.Equal(leaf.leafKey(t.arena), key) {
		leaf.storeValue(t.arena, value)
		unlockNodes(locked)
		return true
	}
	newLeaf := newARTLeaf(t.arena, key, value)
	newParent := splitLeaf(t.arena, leaf, newLeaf, depth)
	t.linkNewParent(parent, parentKey, newParent)
	unlockNodes(locked)
	return true
}

func (t *artTree) insertAtPrefixMismatch(locked []*artNode, parent *artNode, parentKey byte, node *artNode, key []byte, value kv.ValueStruct, depth, match int) bool {
	newLeaf := newARTLeaf(t.arena, key, value)
	newParent := splitPrefix(t.arena, node, newLeaf, depth, match)
	t.linkNewParent(parent, parentKey, newParent)
	unlockNodes(locked)
	return true
}

func (t *artTree) prepareChildInsert(node *artNode, depth int, key []byte) (*artNode, byte, int) {
	nextDepth := depth + len(node.prefixBytes(t.arena))
	b := keyByte(key, nextDepth)
	eq, _ := node.findChild(t.arena, b)
	return eq, b, nextDepth + 1
}

func (t *artTree) insertAtMissingChild(locked []*artNode, node *artNode, childKey byte, key []byte, value kv.ValueStruct) bool {
	newLeaf := newARTLeaf(t.arena, key, value)
	node.addChild(t.arena, childKey, newLeaf)
	unlockNodes(locked)
	return true
}

func (t *artTree) linkNewParent(parent *artNode, parentKey byte, newParent *artNode) {
	if parent == nil {
		t.root.Store(newParent)
		return
	}
	parent.replaceChild(t.arena, parentKey, newParent)
}

func (t *artTree) lowerBound(key []byte) *artNode {
	if t == nil || t.arena == nil {
		return nil
	}
	arena := t.arena
	for {
		root := t.root.Load()
		if root == nil {
			return nil
		}
		leaf, ok := lowerBoundNode(arena, root, key, 0)
		if ok {
			return leaf
		}
		runtime.Gosched()
	}
}

func lowerBoundNode(arena *Arena, node *artNode, key []byte, depth int) (*artNode, bool) {
	if node == nil {
		return nil, true
	}
	if node.isLeaf() {
		if CompareKeys(node.leafKey(arena), key) >= 0 {
			return node, true
		}
		return nil, true
	}
	// Optimistic read: grab version, validate before returning.
	version, ok := node.readLockOrRestart()
	if !ok {
		return nil, false
	}
	prefix, match, cmp := matchPrefix(arena, node, key, depth)
	prefixLen := len(prefix)
	if cmp < 0 {
		child := node.minChild(arena)
		if !node.readUnlockOrRestart(version) {
			return nil, false
		}
		return minLeafNode(arena, child)
	}
	if cmp > 0 {
		if !node.readUnlockOrRestart(version) {
			return nil, false
		}
		return nil, true
	}
	depth += match
	if match < prefixLen {
		if !node.readUnlockOrRestart(version) {
			return nil, false
		}
		return nil, true
	}
	b := keyByte(key, depth)
	eq, gt := node.findChild(arena, b)
	if !node.readUnlockOrRestart(version) {
		return nil, false
	}
	if eq != nil {
		res, ok := lowerBoundNode(arena, eq, key, depth+1)
		if !ok {
			return nil, false
		}
		if res != nil {
			return res, true
		}
	}
	if gt != nil {
		return minLeafNode(arena, gt)
	}
	return nil, true
}

func minLeafNode(arena *Arena, node *artNode) (*artNode, bool) {
	for {
		if node == nil {
			return nil, true
		}
		// Optimistically walk to the smallest leaf under this subtree.
		if node.isLeaf() {
			return node, true
		}
		version, ok := node.readLockOrRestart()
		if !ok {
			return nil, false
		}
		child := node.minChild(arena)
		if !node.readUnlockOrRestart(version) {
			return nil, false
		}
		node = child
	}
}

func unlockNodes(nodes []*artNode) {
	for i := len(nodes) - 1; i >= 0; i-- {
		nodes[i].writeUnlock()
	}
}

func comparePrefix(prefix, key []byte, depth int) (match int, cmp int) {
	for i := range prefix {
		kb := keyByte(key, depth+i)
		if kb == prefix[i] {
			continue
		}
		if kb < prefix[i] {
			return i, -1
		}
		return i, 1
	}
	return len(prefix), 0
}

func matchPrefix(arena *Arena, node *artNode, key []byte, depth int) (prefix []byte, match int, cmp int) {
	prefix = node.prefixBytes(arena)
	match, cmp = comparePrefix(prefix, key, depth)
	return prefix, match, cmp
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
	parent := newARTNode(arena, artNode4Kind, incomingKey[depth:depth+common], nil)
	existingKeyByte := keyByte(existingKey, depth+common)
	incomingKeyByte := keyByte(incomingKey, depth+common)
	payload := payloadWithTwoChildren(arena, existingKeyByte, existing, incomingKeyByte, incoming)
	parent.setPayload(arena, payload)
	return parent
}

func splitPrefix(arena *Arena, node, incoming *artNode, depth int, match int) *artNode {
	nodePrefix := node.prefixBytes(arena)
	parent := newARTNode(arena, artNode4Kind, nodePrefix[:match], nil)

	existingKey := nodePrefix[match]
	incomingKey := keyByte(incoming.leafKey(arena), depth+match)

	node.setPrefix(arena, nodePrefix[match+1:])
	payload := payloadWithTwoChildren(arena, existingKey, node, incomingKey, incoming)
	parent.setPayload(arena, payload)
	return parent
}

func payloadWithTwoChildren(arena *Arena, aKey byte, aChild *artNode, bKey byte, bChild *artNode) *nodePayload {
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
		return payload
	}
	payload.keys[0] = bKey
	payload.keys[1] = aKey
	payload.children[0] = bOff
	payload.children[1] = aOff
	return payload
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
	*payload = nodePayload{}
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
	version       atomic.Uint64
	value         atomic.Uint64
	payloadOffset atomic.Uint32

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
	*n = artNode{}
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
	*leaf = artNode{}
	leaf.kind = artLeafKind
	leaf.setLeafKey(arena, key)
	leaf.storeValue(arena, value)
	return leaf
}

func (n *artNode) isLeaf() bool {
	return n != nil && n.kind == artLeafKind
}

func (n *artNode) readLockOrRestart() (uint64, bool) {
	if n == nil {
		return 0, false
	}
	// Readers record a version, perform their lookup, then re-check the version.
	// Any write sets the lock bit and bumps the version on unlock.
	version := n.version.Load()
	if version&artNodeLockBit != 0 {
		return 0, false
	}
	return version, true
}

func (n *artNode) readUnlockOrRestart(version uint64) bool {
	if n == nil {
		return false
	}
	return n.version.Load() == version
}

func (n *artNode) writeLock() {
	if n == nil {
		return
	}
	// Set the low bit as the write lock; version bump happens on unlock.
	for {
		version := n.version.Load()
		if version&artNodeLockBit != 0 {
			runtime.Gosched()
			continue
		}
		if n.version.CompareAndSwap(version, version|artNodeLockBit) {
			return
		}
	}
}

func (n *artNode) writeUnlock() {
	if n == nil {
		return
	}
	version := n.version.Load()
	// Clear lock bit and bump version to invalidate optimistic readers.
	n.version.Store((version + 1) &^ artNodeLockBit)
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

func (n *artNode) ensurePayload(arena *Arena) *nodePayload {
	payload := n.payloadPtr(arena)
	if payload != nil {
		return payload
	}
	payload = initPayloadForKind(arena, n.kind)
	n.setPayload(arena, payload)
	return payload
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
					gt := findGreaterChild48(arena, payload, int(key)+1)
					return eq, gt
				}
			}
		}
		return nil, findGreaterChild48(arena, payload, int(key)+1)
	case artNode256Kind:
		// Node256 stores direct byte -> child mapping.
		if int(key) < len(payload.children) {
			eq := arenaNodeFromOffset(arena, payload.children[key])
			gt := findGreaterChild256(arena, payload, int(key)+1)
			return eq, gt
		}
	}
	return nil, nil
}

func findGreaterChild48(arena *Arena, payload *nodePayload, start int) *artNode {
	if start < 0 {
		start = 0
	}
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
	return nil
}

func findGreaterChild256(arena *Arena, payload *nodePayload, start int) *artNode {
	if start < 0 {
		start = 0
	}
	if start >= len(payload.children) {
		return nil
	}
	for i := start; i < len(payload.children); i++ {
		if payload.children[i] != 0 {
			return arenaNodeFromOffset(arena, payload.children[i])
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
		for i := 0; i < len(payload.idx); i++ {
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
		for i := 0; i < len(payload.children); i++ {
			if payload.children[i] != 0 {
				return arenaNodeFromOffset(arena, payload.children[i])
			}
		}
	}
	return nil
}

func (n *artNode) replaceChild(arena *Arena, key byte, child *artNode) bool {
	payload := n.payloadPtr(arena)
	if payload == nil {
		return false
	}
	childOffset := arenaNodeOffset(arena, child)
	switch n.kind {
	case artNode4Kind, artNode16Kind:
		for i, k := range payload.keys[:payload.count] {
			if k == key {
				payload.children[i] = childOffset
				return true
			}
		}
	case artNode48Kind:
		if int(key) < len(payload.idx) {
			pos := payload.idx[key]
			if pos > 0 {
				idx := int(pos - 1)
				if idx < len(payload.children) {
					payload.children[idx] = childOffset
					return true
				}
			}
		}
	case artNode256Kind:
		if int(key) < len(payload.children) && payload.children[key] != 0 {
			payload.children[key] = childOffset
			return true
		}
	}
	return false
}

func (n *artNode) addChild(arena *Arena, key byte, child *artNode) {
	payload := n.ensurePayload(arena)
	switch n.kind {
	case artNode4Kind:
		if payload.count < artNode4Cap {
			n.addChildSmall(arena, key, child)
			return
		}
		n.growNode(arena, artNode16Kind)
		n.addChildSmall(arena, key, child)
	case artNode16Kind:
		if payload.count < artNode16Cap {
			n.addChildSmall(arena, key, child)
			return
		}
		n.growNode(arena, artNode48Kind)
		n.addChild48(arena, key, child)
	case artNode48Kind:
		if payload.count < artNode48Cap {
			n.addChild48(arena, key, child)
			return
		}
		n.growNode(arena, artNode256Kind)
		n.addChild256(arena, key, child)
	case artNode256Kind:
		n.addChild256(arena, key, child)
	}
}

func (n *artNode) addChildSmall(arena *Arena, key byte, child *artNode) {
	payload := n.ensurePayload(arena)
	childOffset := arenaNodeOffset(arena, child)
	keys := payload.keys
	children := payload.children
	idx := sort.Search(payload.count, func(i int) bool { return keys[i] >= key })
	if idx < payload.count && keys[idx] == key {
		children[idx] = childOffset
		return
	}
	copy(keys[idx+1:], keys[idx:payload.count])
	keys[idx] = key
	copy(children[idx+1:], children[idx:payload.count])
	children[idx] = childOffset
	payload.count++
}

func (n *artNode) addChild48(arena *Arena, key byte, child *artNode) {
	payload := n.ensurePayload(arena)
	// Node48 stores child slots densely and records the slot index in idx.
	if payload.idx[key] != 0 {
		idx := int(payload.idx[key] - 1)
		if idx < len(payload.children) {
			payload.children[idx] = arenaNodeOffset(arena, child)
		}
		return
	}
	if payload.count >= artNode48Cap {
		return
	}
	payload.children[payload.count] = arenaNodeOffset(arena, child)
	payload.idx[key] = byte(payload.count + 1)
	payload.count++
}

func (n *artNode) addChild256(arena *Arena, key byte, child *artNode) {
	payload := n.ensurePayload(arena)
	if payload.children[key] != 0 {
		payload.children[key] = arenaNodeOffset(arena, child)
		return
	}
	payload.children[key] = arenaNodeOffset(arena, child)
	payload.count++
}

func (n *artNode) growNode(arena *Arena, kind uint8) {
	payload := n.ensurePayload(arena)
	switch kind {
	case artNode16Kind:
		if n.kind == artNode4Kind {
			oldKeys := payload.keys
			oldChildren := payload.children
			keys := arena.allocByteSlice(artNode16Cap, artNode16Cap)
			children := arena.allocUint32Slice(artNode16Cap, artNode16Cap)
			copy(keys, oldKeys[:payload.count])
			copy(children, oldChildren[:payload.count])
			payload.keys = keys
			payload.children = children
		}
		n.kind = artNode16Kind
	case artNode48Kind:
		// Expand into a dense Node48 layout with idx indirection.
		oldKeys := payload.keys
		oldChildren := payload.children
		idx := arena.allocByteSlice(256, 256)
		children := arena.allocUint32Slice(artNode48Cap, artNode48Cap)
		for i, k := range oldKeys[:payload.count] {
			idx[k] = byte(i + 1)
			children[i] = oldChildren[i]
		}
		payload.keys = payload.keys[:0]
		payload.idx = idx
		payload.children = children
		n.kind = artNode48Kind
	case artNode256Kind:
		// Expand into direct 256-way child array.
		oldKeys := payload.keys
		oldIdx := payload.idx
		oldChildren := payload.children
		children := arena.allocUint32Slice(256, 256)
		if n.kind == artNode48Kind {
			for i := range oldIdx {
				pos := oldIdx[i]
				if pos == 0 {
					continue
				}
				idx := int(pos - 1)
				if idx < len(oldChildren) {
					children[i] = oldChildren[idx]
				}
			}
			payload.idx = payload.idx[:0]
		} else {
			for i, k := range oldKeys[:payload.count] {
				children[k] = oldChildren[i]
			}
			payload.keys = payload.keys[:0]
		}
		payload.children = children
		n.kind = artNode256Kind
	}
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
		prefix := node.prefixBytes(arena)
		if len(prefix) > 0 {
			match, cmp := comparePrefix(prefix, key, depth)
			if cmp != 0 || match < len(prefix) {
				it.curr = nil
				it.stack = it.stack[:0]
				return
			}
			depth += len(prefix)
		}
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
