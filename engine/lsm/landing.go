package lsm

import (
	"github.com/feichai0017/NoKV/engine/lsm/landing"
	"github.com/feichai0017/NoKV/engine/lsm/plan"
)

// landingBuffer is the concrete landing buffer instantiation used by
// levelHandler. It binds landing.Buffer to the lsm package's *table.
type landingBuffer = landing.Buffer[*table]

// landingShardCount mirrors landing.ShardCount for callers that index shards
// directly inside the lsm package.
const landingShardCount = landing.ShardCount

// landingPickInput converts the landing-package shard summaries into the
// shape expected by the compaction-plan picker. Both types are structurally
// identical; the conversion stays in the lsm adapter so the landing package
// does not need to import plan.
func landingPickInput(views []landing.ShardView) plan.LandingPickInput {
	out := make([]plan.LandingShardView, 0, len(views))
	for _, v := range views {
		out = append(out, plan.LandingShardView(v))
	}
	return plan.LandingPickInput{Shards: out}
}

func (lh *levelHandler) landingShardByBacklog() int {
	lh.landing.EnsureInit()
	return plan.PickShardByBacklog(landingPickInput(lh.landing.ShardViews()))
}

func (lh *levelHandler) landingShardOrderBySize() []int {
	lh.landing.EnsureInit()
	return plan.PickShardOrder(landingPickInput(lh.landing.ShardViews()))
}

// addLanding registers a table into the landing buffer under lh's write lock.
func (lh *levelHandler) addLanding(t *table) {
	if t == nil {
		return
	}
	lh.Lock()
	defer lh.Unlock()
	lh.landing.EnsureInit()
	t.setLevel(lh.levelNum)
	lh.landing.Add(t)
}

func (lh *levelHandler) landingValueBytes() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.landing.TotalValueSize()
}

func (lh *levelHandler) landingValueDensity() float64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.landingDensityLocked()
}

// landingDensityLocked computes landing value density; caller must hold lh lock.
func (lh *levelHandler) landingDensityLocked() float64 {
	total := lh.landing.TotalSize()
	if total <= 0 {
		return 0
	}
	return float64(lh.landing.TotalValueSize()) / float64(total)
}

func (lh *levelHandler) maxLandingAgeSeconds() float64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.landing.MaxAgeSeconds()
}

func (lh *levelHandler) numLandingTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return lh.landing.TableCount()
}

// numLandingTablesLocked returns the landing table count without acquiring the lock.
// Caller must already hold at least a read lock.
func (lh *levelHandler) numLandingTablesLocked() int {
	return lh.landing.TableCount()
}

func (lh *levelHandler) landingDataSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.landing.TotalSize()
}
