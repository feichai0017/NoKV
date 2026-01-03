package compact

import (
	"expvar"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/feichai0017/NoKV/utils"
)

// Targets describes the compaction size targets for each level.
type Targets struct {
	BaseLevel int
	TargetSz  []int64
	FileSz    []int64
}

// Priority describes a single compaction candidate.
type Priority struct {
	Level        int
	Score        float64
	Adjusted     float64
	DropPrefixes [][]byte
	Target       Targets
	IngestOnly   bool
	IngestMerge  bool
	StatsTag     string
}

// ApplyValueWeight boosts the priority based on value log density.
func (cp *Priority) ApplyValueWeight(weight, valueScore float64) {
	if weight <= 0 || valueScore <= 0 {
		return
	}
	capped := math.Min(valueScore, 16)
	cp.Score += weight * capped
	cp.Adjusted = cp.Score
}

// MoveL0ToFront ensures the first priority is for L0 if one exists.
func MoveL0ToFront(prios []Priority) []Priority {
	idx := -1
	for i, p := range prios {
		if p.Level == 0 {
			idx = i
			break
		}
	}
	if idx > 0 {
		out := append([]Priority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

// Executor provides the compaction hooks used by Manager.
type Executor interface {
	PickCompactLevels() []Priority
	DoCompact(id int, p Priority) error
	NeedsCompaction() bool
	AdjustThrottle()
}

// Manager coordinates background compaction workers.
type Manager struct {
	exec      Executor
	triggerCh chan string
	maxRuns   int
}

var (
	CompactionRunsTotal      = expvar.NewInt("NoKV.Compaction.RunsTotal")
	CompactionLastDurationMs = expvar.NewInt("NoKV.Compaction.LastDurationMs")
	CompactionMaxDurationMs  = expvar.NewInt("NoKV.Compaction.MaxDurationMs")
)

// NewManager creates a compaction manager for the supplied executor.
func NewManager(exec Executor, maxRuns int) *Manager {
	if maxRuns <= 0 {
		maxRuns = 1
	} else if maxRuns > 4 {
		maxRuns = 4
	}
	cm := &Manager{
		exec:      exec,
		triggerCh: make(chan string, 16),
		maxRuns:   maxRuns,
	}
	cm.Trigger("bootstrap")
	return cm
}

// Trigger nudges the manager to run a compaction cycle.
func (cm *Manager) Trigger(reason string) {
	select {
	case cm.triggerCh <- reason:
	default:
	}
}

// Start runs the compaction worker loop until closeCh is closed.
func (cm *Manager) Start(id int, closeCh <-chan struct{}, done func()) {
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
		case reason := <-cm.triggerCh:
			cm.runCycle(id, reason)
		case <-ticker.C:
			cm.runCycle(id, "periodic")
		}
	}
}

func (cm *Manager) runCycle(id int, reason string) {
	_ = reason
	maxRuns := cm.maxRuns
	ranAny := false
	for range maxRuns {
		if id == 0 {
			cm.exec.AdjustThrottle()
		}
		if !cm.runOnce(id) {
			break
		}
		ranAny = true
		if id == 0 {
			cm.exec.AdjustThrottle()
		}
		if !cm.exec.NeedsCompaction() {
			break
		}
	}
	if ranAny && cm.exec.NeedsCompaction() {
		cm.Trigger("backlog")
	}
}

func (cm *Manager) runOnce(id int) bool {
	prios := cm.exec.PickCompactLevels()
	if id == 0 {
		prios = MoveL0ToFront(prios)
	}
	for _, p := range prios {
		if id == 0 && p.Level == 0 {
			// keep scanning level zero first for worker #0
		} else if p.Adjusted < 1.0 {
			break
		}
		if cm.run(id, p) {
			return true
		}
	}
	return false
}

// RunOnce executes a single compaction attempt (useful for tests).
func (cm *Manager) RunOnce(id int) bool {
	return cm.runOnce(id)
}

func (cm *Manager) run(id int, p Priority) bool {
	err := cm.exec.DoCompact(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
	default:
		log.Printf("[compactor %d] doCompact error: %v", id, err)
	}
	return false
}
