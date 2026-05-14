// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package plan owns the LSM compaction policy: key ranges, conflict state,
// level targets, priority calculation, and plan construction. It is policy-
// only — no mmap, no SST, no I/O. The lsm package supplies the live tables
// and executes the plans this package produces.
package plan

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
)

// KeyRange describes a compaction key span. Inf matches all keys.
type KeyRange struct {
	Left  []byte
	Right []byte
	Inf   bool
}

// InfRange matches all keys.
var InfRange = KeyRange{Inf: true}

func (r KeyRange) IsEmpty() bool {
	return len(r.Left) == 0 && len(r.Right) == 0 && !r.Inf
}

func (r KeyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.Left, r.Right, r.Inf)
}

func (r KeyRange) Equals(dst KeyRange) bool {
	return bytes.Equal(r.Left, dst.Left) &&
		bytes.Equal(r.Right, dst.Right) &&
		r.Inf == dst.Inf
}

func (r *KeyRange) Extend(kr KeyRange) {
	if kr.IsEmpty() {
		return
	}
	if r.IsEmpty() {
		*r = kr
	}
	if len(r.Left) == 0 || kv.CompareInternalKeys(kr.Left, r.Left) < 0 {
		r.Left = kr.Left
	}
	if len(r.Right) == 0 || kv.CompareInternalKeys(kr.Right, r.Right) > 0 {
		r.Right = kr.Right
	}
	if kr.Inf {
		r.Inf = true
	}
}

func (r KeyRange) OverlapsWith(dst KeyRange) bool {
	// Empty keyRange always overlaps.
	if r.IsEmpty() {
		return true
	}
	// Empty dst doesn't overlap with anything.
	if dst.IsEmpty() {
		return false
	}
	if r.Inf || dst.Inf {
		return true
	}
	if kv.CompareInternalKeys(r.Left, dst.Right) > 0 {
		return false
	}
	if kv.CompareInternalKeys(r.Right, dst.Left) < 0 {
		return false
	}
	return true
}

// StateEntry captures the metadata tracked during compaction scheduling.
//
// IntraLevel marks the entry as a within-level compaction (e.g. L0→L0) that
// claims its input by table ID only and does NOT register a key range with
// the level. This lets multiple workers run concurrent intra-level
// compactions on disjoint table sets even when the picked SSTs have
// overlapping keyranges (the common case for L0 random writes). Inter-level
// compactions (L0→Lbase, Ln→Ln+1) keep the original range-based locking.
type StateEntry struct {
	ThisLevel  int
	NextLevel  int
	ThisRange  KeyRange
	NextRange  KeyRange
	ThisSize   int64
	TableIDs   []uint64
	IntraLevel bool
}

// LevelsLocked is a marker to indicate level locks are held by the caller.
type LevelsLocked struct{}

// State tracks compaction ranges and in-flight table IDs.
type State struct {
	sync.RWMutex
	levels []*levelState
	tables map[uint64]struct{}
}

type levelState struct {
	ranges  []KeyRange
	delSize int64
}

// NewState allocates a compaction state tracker.
func NewState(maxLevels int) *State {
	cs := &State{
		levels: make([]*levelState, 0, maxLevels),
		tables: make(map[uint64]struct{}),
	}
	for range maxLevels {
		cs.levels = append(cs.levels, &levelState{})
	}
	return cs
}

// Overlaps reports whether the range overlaps with an in-flight compaction.
func (cs *State) Overlaps(level int, kr KeyRange) bool {
	cs.RLock()
	defer cs.RUnlock()
	if level < 0 || level >= len(cs.levels) {
		return false
	}
	return cs.levels[level].overlapsWith(kr)
}

// DelSize returns the accumulated compaction size for a level.
func (cs *State) DelSize(level int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	if level < 0 || level >= len(cs.levels) {
		return 0
	}
	return cs.levels[level].delSize
}

// HasRanges returns true if any level currently tracks a compaction range.
func (cs *State) HasRanges() bool {
	cs.RLock()
	defer cs.RUnlock()
	for _, lvl := range cs.levels {
		if len(lvl.ranges) != 0 {
			return true
		}
	}
	return false
}

// HasTable reports whether a table fid is already being compacted.
func (cs *State) HasTable(fid uint64) bool {
	cs.RLock()
	defer cs.RUnlock()
	_, ok := cs.tables[fid]
	return ok
}

// AddRangeWithTables records a range and table IDs under compaction.
func (cs *State) AddRangeWithTables(level int, kr KeyRange, tableIDs []uint64) {
	cs.Lock()
	defer cs.Unlock()
	if level < 0 || level >= len(cs.levels) {
		return
	}
	cs.levels[level].ranges = append(cs.levels[level].ranges, kr)
	for _, fid := range tableIDs {
		cs.tables[fid] = struct{}{}
	}
}

// Delete clears state for a completed compaction.
//
// IntraLevel entries (L0→L0) only registered table IDs, so Delete only
// undoes the table claims and skips the range bookkeeping.
func (cs *State) Delete(entry StateEntry) error {
	cs.Lock()
	defer cs.Unlock()

	if entry.ThisLevel < 0 || entry.ThisLevel >= len(cs.levels) {
		return nil
	}
	if entry.NextLevel < 0 || entry.NextLevel >= len(cs.levels) {
		return nil
	}

	thisLevel := cs.levels[entry.ThisLevel]
	nextLevel := cs.levels[entry.NextLevel]

	if !entry.IntraLevel {
		// Validate all affected ranges first so Delete remains atomic on error.
		found := thisLevel.contains(entry.ThisRange)
		if entry.ThisLevel != entry.NextLevel && !entry.NextRange.IsEmpty() {
			found = nextLevel.contains(entry.NextRange) && found
		}
		if !found {
			return fmt.Errorf(
				"compact state delete: keyRange not found; this=%s thisLevel=%d thisState=%s next=%s nextLevel=%d nextState=%s",
				entry.ThisRange,
				entry.ThisLevel,
				thisLevel.debug(),
				entry.NextRange,
				entry.NextLevel,
				nextLevel.debug(),
			)
		}
		_ = thisLevel.remove(entry.ThisRange)
		if entry.ThisLevel != entry.NextLevel && !entry.NextRange.IsEmpty() {
			_ = nextLevel.remove(entry.NextRange)
		}
	}

	thisLevel.delSize -= entry.ThisSize

	for _, fid := range entry.TableIDs {
		_, ok := cs.tables[fid]
		utils.CondPanicFunc(!ok, func() error {
			return fmt.Errorf("cs.tables is nil")
		})
		delete(cs.tables, fid)
	}
	return nil
}

// CompareAndAdd reserves ranges and table IDs if they do not overlap.
//
// IntraLevel entries (L0→L0) skip range overlap checks entirely and only
// claim by table ID — multiple concurrent within-level compactions are
// safe as long as their table sets are disjoint (which the picker
// guarantees via state.HasTable).
func (cs *State) CompareAndAdd(_ LevelsLocked, entry StateEntry) bool {
	cs.Lock()
	defer cs.Unlock()

	if entry.ThisLevel < 0 || entry.ThisLevel >= len(cs.levels) {
		return false
	}
	if entry.NextLevel < 0 || entry.NextLevel >= len(cs.levels) {
		return false
	}

	thisLevel := cs.levels[entry.ThisLevel]
	nextLevel := cs.levels[entry.NextLevel]

	if !entry.IntraLevel {
		if thisLevel.overlapsWith(entry.ThisRange) {
			return false
		}
		if nextLevel.overlapsWith(entry.NextRange) {
			return false
		}
		thisLevel.ranges = append(thisLevel.ranges, entry.ThisRange)
		nextLevel.ranges = append(nextLevel.ranges, entry.NextRange)
	}
	thisLevel.delSize += entry.ThisSize
	for _, fid := range entry.TableIDs {
		cs.tables[fid] = struct{}{}
	}
	return true
}

func (ls *levelState) overlapsWith(dst KeyRange) bool {
	for _, r := range ls.ranges {
		if r.OverlapsWith(dst) {
			return true
		}
	}
	return false
}

func (ls *levelState) remove(dst KeyRange) bool {
	final := ls.ranges[:0]
	var found bool
	for _, r := range ls.ranges {
		if !r.Equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	ls.ranges = final
	return found
}

func (ls *levelState) contains(dst KeyRange) bool {
	for _, r := range ls.ranges {
		if r.Equals(dst) {
			return true
		}
	}
	return false
}

func (ls *levelState) debug() string {
	var b bytes.Buffer
	for _, r := range ls.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}
