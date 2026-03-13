package compact

import (
	"log/slog"
	"math"
	"math/rand"
	"time"
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
	IngestMode   IngestMode
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
	policy    *SchedulerPolicy
	triggerCh chan struct{}
	maxRuns   int
	logger    *slog.Logger
}

// NewManager creates a compaction manager for the supplied executor.
func NewManager(exec Executor, maxRuns int, policy *SchedulerPolicy, logger *slog.Logger) *Manager {
	if maxRuns <= 0 {
		maxRuns = 1
	} else if maxRuns > 4 {
		maxRuns = 4
	}
	if policy == nil {
		policy = NewSchedulerPolicy(PolicyLeveled)
	}
	if logger == nil {
		logger = slog.Default()
	}
	cm := &Manager{
		exec:      exec,
		policy:    policy,
		triggerCh: make(chan struct{}, 16),
		maxRuns:   maxRuns,
		logger:    logger,
	}
	cm.Trigger()
	return cm
}

// Trigger nudges the manager to run a compaction cycle.
func (cm *Manager) Trigger() {
	select {
	case cm.triggerCh <- struct{}{}:
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
		case <-cm.triggerCh:
			cm.runCycle(id)
		case <-ticker.C:
			cm.runCycle(id)
		}
	}
}

func (cm *Manager) runCycle(id int) {
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
		cm.Trigger()
	}
}

func (cm *Manager) runOnce(id int) bool {
	prios := cm.exec.PickCompactLevels()
	prios = cm.policy.Arrange(id, prios)
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
	start := time.Now()
	err := cm.exec.DoCompact(id, p)
	cm.policy.Observe(FeedbackEvent{
		WorkerID: id,
		Priority: p,
		Err:      err,
		Duration: time.Since(start),
	})
	switch err {
	case nil:
		return true
	case ErrFillTables:
	default:
		cm.logger.Error("doCompact failed", "worker", id, "level", p.Level, "score", p.Score, "adjusted", p.Adjusted, "err", err)
	}
	return false
}
