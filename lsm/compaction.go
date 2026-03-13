package lsm

import (
	"log/slog"
	"math/rand"
	"time"

	"github.com/feichai0017/NoKV/lsm/compact"
)

type compaction struct {
	owner     *levelManager
	policy    *compact.SchedulerPolicy
	triggerCh chan struct{}
	maxRuns   int
	logger    *slog.Logger
}

func newCompaction(owner *levelManager, maxRuns int, mode string, logger *slog.Logger) *compaction {
	if maxRuns <= 0 {
		maxRuns = 1
	} else if maxRuns > 4 {
		maxRuns = 4
	}
	if logger == nil {
		logger = slog.Default()
	}
	cr := &compaction{
		owner:     owner,
		policy:    compact.NewSchedulerPolicy(mode),
		triggerCh: make(chan struct{}, 16),
		maxRuns:   maxRuns,
		logger:    logger,
	}
	cr.Trigger()
	return cr
}

func (cr *compaction) Trigger() {
	select {
	case cr.triggerCh <- struct{}{}:
	default:
	}
}

func (cr *compaction) Start(id int, closeCh <-chan struct{}, done func()) {
	if done != nil {
		defer done()
	}
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(500)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-closeCh:
		randomDelay.Stop()
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-closeCh:
			return
		case <-cr.triggerCh:
			cr.runCycle(id)
		case <-ticker.C:
			cr.runCycle(id)
		}
	}
}

func (cr *compaction) runCycle(id int) {
	ranAny := false
	for range cr.maxRuns {
		if id == 0 {
			cr.owner.adjustThrottle()
		}
		if !cr.runOnce(id) {
			break
		}
		ranAny = true
		if id == 0 {
			cr.owner.adjustThrottle()
		}
		if !cr.owner.needsCompaction() {
			break
		}
	}
	if ranAny && cr.owner.needsCompaction() {
		cr.Trigger()
	}
}

func (cr *compaction) runOnce(id int) bool {
	prios := cr.owner.pickCompactLevels()
	prios = cr.policy.Arrange(id, prios)
	for _, p := range prios {
		if id == 0 && p.Level == 0 {
			// keep scanning level zero first for worker #0
		} else if p.Adjusted < 1.0 {
			break
		}
		if cr.run(id, p) {
			return true
		}
	}
	return false
}

func (cr *compaction) RunOnce(id int) bool {
	return cr.runOnce(id)
}

func (cr *compaction) run(id int, p compact.Priority) bool {
	start := time.Now()
	err := cr.owner.doCompact(id, p)
	cr.policy.Observe(compact.FeedbackEvent{
		WorkerID: id,
		Priority: p,
		Err:      err,
		Duration: time.Since(start),
	})
	switch err {
	case nil:
		return true
	case compact.ErrFillTables:
	default:
		cr.logger.Error("doCompact failed", "worker", id, "level", p.Level, "score", p.Score, "adjusted", p.Adjusted, "err", err)
	}
	return false
}

func (lsm *LSM) newCompactStatus() *compact.State {
	return compact.NewState(lsm.option.MaxLevelNum)
}
