package lsm

import (
	"expvar"
	"log"
	"math/rand"
	"time"

	"github.com/feichai0017/NoKV/utils"
)

// compactionManager coordinates background compaction workers, maintains
// scheduling/backpressure, and exposes a single entrypoint for future hooks.
type compactionManager struct {
	lm        *levelManager
	triggerCh chan string
}

var (
	compactionRunsTotal      = expvar.NewInt("NoKV.Compaction.RunsTotal")
	compactionLastDurationMs = expvar.NewInt("NoKV.Compaction.LastDurationMs")
	compactionMaxDurationMs  = expvar.NewInt("NoKV.Compaction.MaxDurationMs")
)

func newCompactionManager(lm *levelManager) *compactionManager {
	cm := &compactionManager{
		lm:        lm,
		triggerCh: make(chan string, 16),
	}
	cm.trigger("bootstrap")
	return cm
}

func (cm *compactionManager) trigger(reason string) {
	select {
	case cm.triggerCh <- reason:
	default:
	}
}

func (cm *compactionManager) start(id int) {
	defer cm.lm.lsm.closer.Done()

	randomDelay := time.NewTimer(time.Duration(rand.Int31n(500)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-cm.lm.lsm.closer.CloseSignal:
		randomDelay.Stop()
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.lm.lsm.closer.CloseSignal:
			return
		case reason := <-cm.triggerCh:
			cm.runCycle(id, reason)
		case <-ticker.C:
			cm.runCycle(id, "periodic")
		}
	}
}

func (cm *compactionManager) runCycle(id int, reason string) {
	_ = reason
	if id == 0 {
		cm.adjustThrottle()
	}
	running := cm.runOnce(id)
	if id == 0 {
		cm.adjustThrottle()
	}
	if running && cm.needsCompaction() {
		cm.trigger("backlog")
	}
}

func (cm *compactionManager) adjustThrottle() {
	if cm == nil || cm.lm == nil || cm.lm.lsm == nil || len(cm.lm.levels) == 0 {
		return
	}
	limit := cm.lm.opt.NumLevelZeroTables
	if limit <= 0 {
		limit = 4
	}
	l0Tables := cm.lm.levels[0].numTables()
	highWatermark := limit * 2
	switch {
	case l0Tables >= highWatermark:
		cm.lm.lsm.throttleWrites(true)
	case l0Tables <= limit:
		cm.lm.lsm.throttleWrites(false)
	}
}

func (cm *compactionManager) needsCompaction() bool {
	return len(cm.lm.pickCompactLevels()) > 0
}

func (cm *compactionManager) runOnce(id int) bool {
	prios := cm.lm.pickCompactLevels()
	if id == 0 {
		prios = moveL0toFront(prios)
	}
	for _, p := range prios {
		if id == 0 && p.level == 0 {
			// keep scanning level zero first for worker #0
		} else if p.adjusted < 1.0 {
			break
		}
		if cm.run(id, p) {
			return true
		}
	}
	return false
}

func (cm *compactionManager) run(id int, p compactionPriority) bool {
	err := cm.lm.doCompact(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
	default:
		log.Printf("[compactor %d] doCompact error: %v", id, err)
	}
	return false
}
