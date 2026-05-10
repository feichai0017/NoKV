package lsm

import (
	"sync/atomic"

	"github.com/feichai0017/NoKV/engine/lsm/pacer"
	"github.com/feichai0017/NoKV/engine/lsm/plan"
)

// compactionMetrics holds per-compaction-run duration counters that drive
// the compaction-related fields of the LSM Diagnostics snapshot.
type compactionMetrics struct {
	LastNs atomic.Int64
	MaxNs  atomic.Int64
	Runs   atomic.Uint64
}

// compactor owns all compaction state — the planner conflict-state, the
// write-rate pacer, the per-run metrics, and the scheduler that drives the
// worker pool. It holds a back-reference to its levelManager because
// compaction must read and mutate level state.
type compactor struct {
	lm      *levelManager
	state   *plan.State
	pacer   *pacer.Pacer
	metrics compactionMetrics
	sched   *scheduler
}

func newCompactor(lm *levelManager, opt *Options) *compactor {
	c := &compactor{
		lm:    lm,
		state: plan.NewState(opt.MaxLevelNum),
		pacer: pacer.New(opt.CompactionWriteBytesPerSec),
	}
	c.sched = newScheduler(lm, opt.NumCompactors, opt.CompactionPolicy, lm.getLogger())
	return c
}
