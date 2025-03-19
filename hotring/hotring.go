package hotring

type HashFn func(string) uint32

type HotRing struct {
	addrMask uint32
	R        uint8
	hashFn   HashFn
	hashMask uint32

	findCnt    uint32
	maxFindCnt uint32
	minFindCnt uint32

	tables []*Node
}

func (h *HotRing) Search(key string) *Node {
	// 将Key 计算出来一个 Hash 值
	// Hash值被分为两部分，加速比较的 Tag，和定位用的 Index
	hashVal := h.hashFn(key)
	index, tag := hashVal&h.hashMask, hashVal&(^h.hashMask)

	// 需要记录几个地方的访问次数，单个Node被访问的次数，以及整个Ring被访问的次数，统计用
	compareItem := NewCompareItem(key, tag)

	var prev, next, res *Node

	if h.tables[index] == nil { //环中没有元素
		res = nil
	} else if h.tables[index] == h.tables[index] { //环中只有一个元素
		res = h.tables[index]
	} else {
		prev = h.tables[index]
		next = prev.Next()
		for {
			if compareItem.Equal(prev) {
				prev.Increment()
				res = prev
				break
			}

			if prev.Less(compareItem) && compareItem.Less(next) ||
				compareItem.Less(prev) && next.Less(prev) ||
				next.Less(prev) && prev.Less(compareItem) {
				break
			}
			next = next.Next()
			prev = prev.Next()

		}
	}

	return res
}

func (h *HotRing) Insert(key, val string) bool {
	hashVal := h.hashFn(key)
	index, tag := hashVal&h.hashMask, hashVal & ^h.hashMask

	newItem := NewNode(key, val, tag)

	prev, next := &Node{}, &Node{}

	if h.tables[index] == nil {
		h.tables[index] = newItem
		newItem.SetNext(newItem)
	} else if h.tables[index].Next() == h.tables[index] {
		h.tables[index] = newItem
		newItem.SetNext(h.tables[index])
	} else {
		prev = h.tables[index]
		next = prev.Next()
		for {
			if newItem.Equal(prev) {
				return false
			}

			if prev.Less(newItem) && newItem.Less(next) ||
				newItem.Less(next) && next.Less(prev) ||
				next.Less(prev) && prev.Less(newItem) {
				newItem.SetNext(next)
				prev.SetNext(newItem)
				break
			}

			prev = prev.Next()
			next = prev.Next()
		}
	}
	return true

}

func (h *HotRing) Remove(key string) {
	toDel := h.Search(key)
	if toDel == nil {
		return
	}
	hashVal := h.hashFn(key)
	index, _ := hashVal&h.hashMask, hashVal & ^h.hashMask

	prev := toDel

	// 遍历找到待删除节点的前一个节点
	for {
		if !prev.Next().Equal(toDel) {
			prev = prev.Next()
		} else {
			break
		}
	}

	prev.SetNext(toDel.Next())

	if h.tables[index] == toDel {
		if prev == toDel {
			h.tables[index] = nil
		} else {
			h.tables[index] = toDel.Next()
		}
	}

	return

}

func (h *HotRing) Update(key, val string) bool {
	res := h.Search(key)

	if res == nil {
		return false
	}

	hashVal := h.hashFn(key)
	index, _ := hashVal&h.hashMask, hashVal & ^h.hashMask

	res.val = val

	h.tables[index] = res
	return true
}