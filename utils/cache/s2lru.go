package cache

import (
	"container/list"
	"fmt"
)

type segmentedLRU struct {
	data                     map[uint64]*list.Element
	stageOneCap, stageTwoCap int
	stageOne, stageTwo       *list.List
}

const (
	STAGE_ONE = iota + 1
	STAGE_TWO
)

func newSLRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap int) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: stageOneCap,
		stageTwoCap: stageTwoCap,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

func (slru *segmentedLRU) add(newitem storeItem) {
	// first add to stageOne
	newitem.stage = 1

	// if stageOne is not full, or the whole LFU is not full
	if slru.stageOne.Len() < slru.stageOneCap || slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		slru.data[newitem.key] = slru.stageOne.PushFront(&newitem)
		return
	}

	//if we reach here, it means StageOne is full, or the whole LFU is full
	//then we need to evict data from StageOne
	e := slru.stageOne.Back()
	item := e.Value.(*storeItem)

	//here we really evict the item
	delete(slru.data, item.key)

	*item = newitem

	slru.data[item.key] = e
	slru.stageOne.MoveToFront(e)
}

func (slru *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)

	// if the item is already in stageTwo, just move it to the front
	if item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	// if the item is still in stageOne, then we need to move it to stageTwo
	if slru.stageTwo.Len() < slru.stageTwoCap {
		slru.stageOne.Remove(v)
		item.stage = STAGE_TWO
		slru.data[item.key] = slru.stageTwo.PushFront(item)
		return
	}

	// new data add to stageTwo, we need to evict old data
	// the data evicted from stageTwo will not disappear, it will be moved to stageOne
	// the data in stageOne, with lower access frequency, may be evicted
	back := slru.stageTwo.Back()
	bitem := back.Value.(*storeItem)

	*bitem, *item = *item, *bitem

	bitem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	slru.data[item.key] = v
	slru.data[bitem.key] = back

	slru.stageOne.MoveToFront(v)
	slru.stageTwo.MoveToFront(back)
}

func (slru *segmentedLRU) Len() int {
	return slru.stageTwo.Len() + slru.stageOne.Len()
}

func (slru *segmentedLRU) victim() *storeItem {
	//if the capacity of slru is not full, no need to evict
	if slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		return nil
	}

	// if the capacity of slru is full, we need to evict data from 20% of the area
	// here we just take the last element from the tail
	v := slru.stageOne.Back()
	return v.Value.(*storeItem)
}

func (slru *segmentedLRU) String() string {
	var s string
	for e := slru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	s += " | "
	for e := slru.stageOne.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}