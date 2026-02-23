package compact

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/feichai0017/NoKV/utils"
)

// KeyRange describes a compaction key span.
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
	if len(r.Left) == 0 || utils.CompareKeys(kr.Left, r.Left) < 0 {
		r.Left = kr.Left
	}
	if len(r.Right) == 0 || utils.CompareKeys(kr.Right, r.Right) > 0 {
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

	// [dst.left, dst.right] ... [r.left, r.right]
	if utils.CompareKeys(r.Left, dst.Right) > 0 {
		return false
	}
	// [r.left, r.right] ... [dst.left, dst.right]
	if utils.CompareKeys(r.Right, dst.Left) < 0 {
		return false
	}
	return true
}

// StateEntry captures the metadata tracked during compaction scheduling.
type StateEntry struct {
	ThisLevel int
	NextLevel int
	ThisRange KeyRange
	NextRange KeyRange
	ThisSize  int64
	TableIDs  []uint64
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
func (cs *State) Delete(entry StateEntry) {
	cs.Lock()
	defer cs.Unlock()

	if entry.ThisLevel < 0 || entry.ThisLevel >= len(cs.levels) {
		return
	}
	if entry.NextLevel < 0 || entry.NextLevel >= len(cs.levels) {
		return
	}

	thisLevel := cs.levels[entry.ThisLevel]
	nextLevel := cs.levels[entry.NextLevel]

	thisLevel.delSize -= entry.ThisSize
	found := thisLevel.remove(entry.ThisRange)
	if entry.ThisLevel != entry.NextLevel && !entry.NextRange.IsEmpty() {
		found = nextLevel.remove(entry.NextRange) && found
	}

	if !found {
		this := entry.ThisRange
		next := entry.NextRange
		fmt.Printf("Looking for: %s in this level %d.\n", this, entry.ThisLevel)
		fmt.Printf("This Level:\n%s\n", thisLevel.debug())
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d.\n", next, entry.NextLevel)
		fmt.Printf("Next Level:\n%s\n", nextLevel.debug())
		log.Fatal("keyRange not found")
	}

	for _, fid := range entry.TableIDs {
		_, ok := cs.tables[fid]
		utils.CondPanic(!ok, fmt.Errorf("cs.tables is nil"))
		delete(cs.tables, fid)
	}
}

// CompareAndAdd reserves ranges and table IDs if they do not overlap.
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

	if thisLevel.overlapsWith(entry.ThisRange) {
		return false
	}
	if nextLevel.overlapsWith(entry.NextRange) {
		return false
	}

	thisLevel.ranges = append(thisLevel.ranges, entry.ThisRange)
	nextLevel.ranges = append(nextLevel.ranges, entry.NextRange)
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

func (ls *levelState) debug() string {
	var b bytes.Buffer
	for _, r := range ls.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}
