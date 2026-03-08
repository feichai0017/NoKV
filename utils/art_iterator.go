package utils

import (
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/kv"
)

type artIterator struct {
	tree      *artTree
	owner     *ART
	isAsc     bool
	curr      *artNode
	stack     []iterFrame
	entry     kv.Entry
	closeOnce sync.Once
}

type iterFrame struct {
	node    *artNode
	payload *nodePayload
	idx     int
}

func (it *artIterator) Next() {
	if it.curr == nil {
		return
	}
	if it.tree == nil || it.tree.arena == nil {
		it.curr = nil
		return
	}
	if it.isAsc {
		// Walk to the next leaf by climbing until we find the next child.
		it.advance()
		return
	}
	// Walk to the previous leaf by climbing until we find the previous child.
	it.retreat()
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
	if it.isAsc {
		it.descendToMin(root)
		return
	}
	it.descendToMax(root)
}

func (it *artIterator) Item() Item {
	if it == nil || it.curr == nil || it.tree == nil || it.tree.arena == nil {
		return nil
	}
	arena := it.tree.arena
	vs := it.curr.loadValue(arena)
	it.entry.Key = it.curr.originalKey(arena)
	it.entry.Value = vs.Value
	it.entry.ExpiresAt = vs.ExpiresAt
	it.entry.Meta = vs.Meta
	_ = it.entry.PopulateInternalMeta()
	return &it.entry
}

func (it *artIterator) Close() error {
	if it == nil {
		return nil
	}
	it.closeOnce.Do(func() {
		if it.owner != nil {
			it.owner.DecrRef()
		}
		it.owner = nil
		it.tree = nil
		it.curr = nil
		it.stack = nil
	})
	return nil
}

func (it *artIterator) Seek(key []byte) {
	if it == nil || it.tree == nil {
		return
	}
	it.stack = it.stack[:0]
	var scratch [256]byte
	routeKey := artEncodeComparableKeyTo(scratch[:0], key)
	var leaf *artNode
	if it.isAsc {
		leaf = it.tree.lowerBound(routeKey)
	} else {
		leaf = it.tree.upperBound(routeKey)
	}
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
		payload := node.payloadPtr(it.tree.arena)
		child, nextIdx := it.nextChild(node, payload, 0)
		if child == nil {
			it.curr = nil
			return
		}
		it.stack = append(it.stack, iterFrame{node: node, payload: payload, idx: nextIdx})
		node = child
	}
	it.curr = node
}

func (it *artIterator) descendToMax(node *artNode) {
	if it == nil || it.tree == nil || it.tree.arena == nil {
		it.curr = nil
		return
	}
	for node != nil && !node.isLeaf() {
		payload := node.payloadPtr(it.tree.arena)
		var child *artNode
		var nextIdx int
		switch node.kind {
		case artNode4Kind, artNode16Kind:
			child, nextIdx = it.prevChild(node, payload, payload.count-1)
		case artNode48Kind:
			child, nextIdx = it.prevChild(node, payload, len(payload.idx)-1)
		case artNode256Kind:
			child, nextIdx = it.prevChild(node, payload, len(payload.children)-1)
		}
		if child == nil {
			it.curr = nil
			return
		}
		it.stack = append(it.stack, iterFrame{node: node, payload: payload, idx: nextIdx})
		node = child
	}
	it.curr = node
}

func (it *artIterator) advance() {
	for len(it.stack) > 0 {
		top := &it.stack[len(it.stack)-1]
		child, nextIdx := it.nextChild(top.node, top.payload, top.idx)
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

func (it *artIterator) retreat() {
	for len(it.stack) > 0 {
		top := &it.stack[len(it.stack)-1]
		child, nextIdx := it.prevChild(top.node, top.payload, top.idx)
		if child == nil {
			it.stack = it.stack[:len(it.stack)-1]
			continue
		}
		top.idx = nextIdx
		it.descendToMax(child)
		return
	}
	it.curr = nil
}

func (it *artIterator) nextChild(node *artNode, payload *nodePayload, idx int) (*artNode, int) {
	if node == nil || payload == nil || it.tree == nil || it.tree.arena == nil {
		return nil, 0
	}
	arena := it.tree.arena
	if idx < 0 {
		idx = 0
	}
	switch node.kind {
	case artNode4Kind, artNode16Kind:
		for i := idx; i < payload.count; i++ {
			child := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i]))
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
			child := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[childIdx]))
			if child != nil {
				return child, i + 1
			}
		}
	case artNode256Kind:
		for i := idx; i < len(payload.children); i++ {
			child := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i]))
			if child != nil {
				return child, i + 1
			}
		}
	}
	return nil, 0
}

func (it *artIterator) prevChild(node *artNode, payload *nodePayload, idx int) (*artNode, int) {
	if node == nil || payload == nil || it.tree == nil || it.tree.arena == nil {
		return nil, 0
	}
	arena := it.tree.arena
	switch node.kind {
	case artNode4Kind, artNode16Kind:
		if payload.count == 0 {
			return nil, 0
		}
		if idx >= payload.count {
			idx = payload.count - 1
		}
		for i := idx; i >= 0; i-- {
			child := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i]))
			if child != nil {
				return child, i - 1
			}
		}
	case artNode48Kind:
		if idx >= len(payload.idx) {
			idx = len(payload.idx) - 1
		}
		for i := idx; i >= 0; i-- {
			pos := payload.idx[i]
			if pos == 0 {
				continue
			}
			childIdx := int(pos - 1)
			if childIdx >= len(payload.children) {
				continue
			}
			child := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[childIdx]))
			if child != nil {
				return child, i - 1
			}
		}
	case artNode256Kind:
		if idx >= len(payload.children) {
			idx = len(payload.children) - 1
		}
		for i := idx; i >= 0; i-- {
			child := arenaNodeFromOffset(arena, atomic.LoadUint32(&payload.children[i]))
			if child != nil {
				return child, i - 1
			}
		}
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
		payload := node.payloadPtr(arena)
		child, pos := lookupExactPosPayload(arena, node.kind, payload, childKey)
		if child == nil {
			it.curr = nil
			it.stack = it.stack[:0]
			return
		}
		nextIdx := pos + 1
		if !it.isAsc {
			nextIdx = pos - 1
		}
		it.stack = append(it.stack, iterFrame{node: node, payload: payload, idx: nextIdx})
		node = child
		depth++
	}
	it.curr = node
}
