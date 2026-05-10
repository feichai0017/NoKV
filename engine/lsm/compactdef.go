package lsm

import (
	"github.com/feichai0017/NoKV/engine/lsm/plan"
)

// compactDef carries one in-flight compaction's executable state. It binds a
// pure plan.Plan (from the plan subpackage) to live tables and level handlers
// for the duration of one compaction run.
//
// Compaction flow: the picker chooses a Priority, the planner produces a
// plan.Plan from snapshot table metadata, then the executor binds that Plan
// into a compactDef so it can resolve table IDs back to live *table handles
// and drive the merge.
type compactDef struct {
	compactorId int
	spec        plan.Plan
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table
	bot []*table

	splits []plan.KeyRange

	thisSize int64

	adjusted float64
}

// targetFileSize returns the per-file size budget configured for the compaction
// destination. SSTable builders use this to know when to flush the current
// builder and start a new file.
func (cd *compactDef) targetFileSize() int64 {
	return cd.fileSize(cd.spec.ThisLevel)
}

// fileSize returns the per-file size budget for the requested level. Returns
// zero when level is neither this nor next, so callers can detect mis-routed
// queries explicitly.
func (cd *compactDef) fileSize(level int) int64 {
	switch level {
	case cd.spec.ThisLevel:
		return cd.spec.ThisFileSize
	case cd.spec.NextLevel:
		return cd.spec.NextFileSize
	default:
		return 0
	}
}

// stateEntry derives the conflict-state entry registered with plan.State so
// concurrent compactors can detect overlap by table id and key range.
func (cd *compactDef) stateEntry() plan.StateEntry {
	return cd.spec.StateEntry(cd.thisSize)
}

// setNextLevel binds the destination level handler for the compaction and
// updates the embedded plan.Plan with the level's per-file size target.
// Passing a nil next leaves the embedded plan untouched.
func (cd *compactDef) setNextLevel(t plan.Targets, next *levelHandler) {
	cd.nextLevel = next
	if next == nil {
		return
	}
	cd.spec.NextLevel = next.levelNum
	cd.spec.NextFileSize = t.FileSizeForLevel(next.levelNum)
}

// applyPlan replaces the embedded plan with p while preserving the
// builder-relevant fields (file sizes, landing mode, drop prefixes, stats
// tag) that the executor already populated.
func (cd *compactDef) applyPlan(p plan.Plan) {
	p.ThisFileSize = cd.spec.ThisFileSize
	p.NextFileSize = cd.spec.NextFileSize
	p.LandingMode = cd.spec.LandingMode
	p.DropPrefixes = cd.spec.DropPrefixes
	p.StatsTag = cd.spec.StatsTag
	cd.spec = p
}

// lockLevels takes read locks on the source and (when distinct) destination
// level handlers. It is intentionally separate from the constructor so the
// caller can choose whether to lock; e.g. L0→L0 takes one shared lock to
// avoid double-RLock deadlocks on the same handler.
func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	if cd.nextLevel != cd.thisLevel {
		cd.nextLevel.RLock()
	}
}

// unlockLevels releases the locks taken by lockLevels in reverse order.
func (cd *compactDef) unlockLevels() {
	if cd.nextLevel != cd.thisLevel {
		cd.nextLevel.RUnlock()
	}
	cd.thisLevel.RUnlock()
}
