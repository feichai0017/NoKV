package index

import (
	"sync/atomic"

	"github.com/feichai0017/NoKV/engine/kv"
)

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
		payload.keys = arena.AllocByteSlice(artNode4Cap, artNode4Cap)
		payload.children = arena.AllocUint32Slice(artNode4Cap, artNode4Cap)
	case artNode16Kind:
		payload.keys = arena.AllocByteSlice(artNode16Cap, artNode16Cap)
		payload.children = arena.AllocUint32Slice(artNode16Cap, artNode16Cap)
	case artNode48Kind:
		payload.children = arena.AllocUint32Slice(artNode48Cap, artNode48Cap)
		payload.idx = arena.AllocByteSlice(256, 256)
	case artNode256Kind:
		payload.children = arena.AllocUint32Slice(256, 256)
	}
	return payload
}

type artNode struct {
	valueOffset   atomic.Uint32
	payloadOffset atomic.Uint32
	self          uint32

	kind                 uint8
	prefixLen            uint16
	prefix               [artMaxPrefixLen]byte
	prefixOverflowOffset uint32

	// Leaf metadata lives in the arena to avoid GC pressure.
	leafKeyOffset uint32
	leafKeySize   uint16
	origKeyOffset uint32
	origKeySize   uint16
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
	leaf.setOriginalKey(arena, key)
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
	return arenaGetKey(arena, n.leafKeyOffset, n.leafKeySize)
}

func (n *artNode) setLeafKey(arena *Arena, key []byte) {
	if n == nil || arena == nil {
		return
	}
	n.leafKeyOffset, n.leafKeySize = artPutComparableKey(arena, key)
}

func (n *artNode) originalKey(arena *Arena) []byte {
	if n == nil || arena == nil || n.origKeySize == 0 {
		return nil
	}
	return arenaGetKey(arena, n.origKeyOffset, n.origKeySize)
}

func (n *artNode) setOriginalKey(arena *Arena, key []byte) {
	if n == nil || arena == nil {
		return
	}
	n.origKeyOffset = arenaPutKey(arena, key)
	n.origKeySize = uint16(len(key))
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
	n.payloadOffset.Store(payload.self)
}

func (n *artNode) loadValue(arena *Arena) kv.ValueStruct {
	if n == nil || arena == nil {
		return kv.ValueStruct{}
	}
	return arenaGetArtValue(arena, n.valueOffset.Load())
}

func (n *artNode) storeValue(arena *Arena, vs kv.ValueStruct) {
	if n == nil || arena == nil {
		return
	}
	n.valueOffset.Store(arenaPutArtValue(arena, vs))
}

func (n *artNode) prefixBytes(arena *Arena) []byte {
	if n == nil || n.prefixLen == 0 {
		return nil
	}
	if n.prefixLen <= artMaxPrefixLen {
		return n.prefix[:n.prefixLen]
	}
	// Long prefixes staging into the arena.
	if arena == nil || n.prefixOverflowOffset == 0 {
		return n.prefix[:artMaxPrefixLen]
	}
	return arenaGetKey(arena, n.prefixOverflowOffset, n.prefixLen)
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
	n.prefixOverflowOffset = arenaPutKey(arena, prefix)
}

func lookupGEPayload(arena *Arena, kind uint8, payload *nodePayload, key byte) (*artNode, *artNode) {
	if payload == nil {
		return nil, nil
	}
	switch kind {
	case artNode4Kind:
		for i := 0; i < payload.count; i++ {
			k := payload.keys[i]
			if k == key {
				var gt *artNode
				if i+1 < payload.count {
					gt = arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i+1]))
				}
				return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i])), gt
			}
			if k > key {
				return nil, arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i]))
			}
		}
	case artNode16Kind:
		idx, exact := node16LowerBoundIndex(payload.keys, payload.count, key)
		if idx < 0 {
			return nil, nil
		}
		if exact {
			var gt *artNode
			if idx+1 < payload.count {
				gt = arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx+1]))
			}
			return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx])), gt
		}
		return nil, arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx]))
	case artNode48Kind:
		if int(key) < len(payload.idx) {
			pos := payload.idx[key]
			if pos > 0 {
				idx := int(pos - 1)
				if idx < len(payload.children) {
					eq := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx]))
					gt := scanGreaterChild(arena, payload, int(key)+1, artNode48Kind)
					return eq, gt
				}
			}
		}
		return nil, scanGreaterChild(arena, payload, int(key)+1, artNode48Kind)
	case artNode256Kind:
		if int(key) < len(payload.children) {
			eq := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[int(key)]))
			gt := scanGreaterChild(arena, payload, int(key)+1, artNode256Kind)
			return eq, gt
		}
	}
	return nil, nil
}

func lookupExactPayload(arena *Arena, kind uint8, payload *nodePayload, key byte) *artNode {
	if payload == nil {
		return nil
	}
	switch kind {
	case artNode4Kind:
		for i := 0; i < payload.count; i++ {
			if payload.keys[i] == key {
				return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i]))
			}
		}
	case artNode16Kind:
		idx := node16ExactIndex(payload.keys, payload.count, key)
		if idx >= 0 {
			return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx]))
		}
	case artNode48Kind:
		pos := payload.idx[key]
		if pos == 0 {
			return nil
		}
		return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[int(pos-1)]))
	case artNode256Kind:
		return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[int(key)]))
	}
	return nil
}

func lookupExactPosPayload(arena *Arena, kind uint8, payload *nodePayload, key byte) (*artNode, int) {
	if payload == nil {
		return nil, -1
	}
	switch kind {
	case artNode4Kind:
		for i := 0; i < payload.count; i++ {
			if payload.keys[i] == key {
				return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i])), i
			}
		}
	case artNode16Kind:
		idx := node16ExactIndex(payload.keys, payload.count, key)
		if idx >= 0 {
			return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx])), idx
		}
	case artNode48Kind:
		pos := payload.idx[key]
		if pos == 0 {
			return nil, -1
		}
		return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[int(pos-1)])), int(key)
	case artNode256Kind:
		return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[int(key)])), int(key)
	}
	return nil, -1
}

func lookupLEPayload(arena *Arena, kind uint8, payload *nodePayload, key byte) (*artNode, *artNode) {
	if payload == nil {
		return nil, nil
	}
	switch kind {
	case artNode4Kind:
		for i := payload.count - 1; i >= 0; i-- {
			k := payload.keys[i]
			if k == key {
				var lt *artNode
				if i > 0 {
					lt = arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i-1]))
				}
				return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i])), lt
			}
			if k < key {
				return nil, arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i]))
			}
		}
	case artNode16Kind:
		idx, exact := node16UpperBoundIndex(payload.keys, payload.count, key)
		if idx < 0 {
			return nil, nil
		}
		if exact {
			var lt *artNode
			if idx > 0 {
				lt = arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx-1]))
			}
			return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx])), lt
		}
		return nil, arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx]))
	case artNode48Kind:
		if int(key) < len(payload.idx) {
			pos := payload.idx[key]
			if pos > 0 {
				idx := int(pos - 1)
				if idx < len(payload.children) {
					eq := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx]))
					lt := scanLesserChild(arena, payload, int(key)-1, artNode48Kind)
					return eq, lt
				}
			}
		}
		return nil, scanLesserChild(arena, payload, int(key)-1, artNode48Kind)
	case artNode256Kind:
		if int(key) < len(payload.children) {
			eq := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[int(key)]))
			lt := scanLesserChild(arena, payload, int(key)-1, artNode256Kind)
			return eq, lt
		}
	}
	return nil, nil
}

func scanGreaterChild(arena *Arena, payload *nodePayload, start int, kind uint8) *artNode {
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
				return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx]))
			}
		}
	case artNode256Kind:
		if start >= len(payload.children) {
			return nil
		}
		for i := start; i < len(payload.children); i++ {
			if child := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i])); child != nil {
				return child
			}
		}
	}
	return nil
}

func scanLesserChild(arena *Arena, payload *nodePayload, start int, kind uint8) *artNode {
	switch kind {
	case artNode48Kind:
		if start >= len(payload.idx) {
			start = len(payload.idx) - 1
		}
		for i := start; i >= 0; i-- {
			pos := payload.idx[i]
			if pos == 0 {
				continue
			}
			idx := int(pos - 1)
			if idx < len(payload.children) {
				return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[idx]))
			}
		}
	case artNode256Kind:
		if start >= len(payload.children) {
			start = len(payload.children) - 1
		}
		for i := start; i >= 0; i-- {
			if child := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i])); child != nil {
				return child
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
		return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[0]))
	case artNode48Kind:
		return scanGreaterChild(arena, payload, 0, artNode48Kind)
	case artNode256Kind:
		return scanGreaterChild(arena, payload, 0, artNode256Kind)
	}
	return nil
}

func (n *artNode) maxChild(arena *Arena) *artNode {
	payload := n.payloadPtr(arena)
	if payload == nil {
		return nil
	}
	switch n.kind {
	case artNode4Kind, artNode16Kind:
		if payload.count == 0 {
			return nil
		}
		return arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[payload.count-1]))
	case artNode48Kind:
		return scanLesserChild(arena, payload, len(payload.idx)-1, artNode48Kind)
	case artNode256Kind:
		return scanLesserChild(arena, payload, len(payload.children)-1, artNode256Kind)
	}
	return nil
}
