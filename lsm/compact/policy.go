package compact

import (
	"errors"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// PolicyLeveled keeps the legacy leveled-style execution ordering.
	PolicyLeveled = "leveled"
	// PolicyTiered prioritizes ingest-buffer convergence before regular compaction.
	PolicyTiered = "tiered"
	// PolicyHybrid adapts between leveled and tiered ordering by ingest pressure.
	PolicyHybrid = "hybrid"
)

const (
	// l0ReliefScoreMin marks an L0 task as backlog-relief work.
	l0ReliefScoreMin = 1.0
	// l0CriticalScore triggers a hard "L0 first" slot for worker 0.
	l0CriticalScore = 2.0
	// hybridTieredThreshold controls when hybrid switches to tiered behavior.
	hybridTieredThreshold = 3.0
)

// queueQuota controls how many tasks from each queue are emitted per round.
type queueQuota struct {
	l0      int
	keep    int
	drain   int
	regular int
}

type priorityQueues struct {
	l0      []Priority
	keep    []Priority
	drain   []Priority
	regular []Priority
}

// Policy reorders compaction priorities selected by the picker.
// It does not change candidate generation; it only changes execution order.
type Policy interface {
	Arrange(workerID int, priorities []Priority) []Priority
}

// FeedbackEvent captures one compaction execution outcome.
//
// Policies can consume this signal to tune scheduling decisions in subsequent
// rounds without modifying picker behavior.
type FeedbackEvent struct {
	WorkerID int
	Priority Priority
	Err      error
	Duration time.Duration
}

// FeedbackPolicy is an optional extension for policies that support runtime
// closed-loop adjustments.
type FeedbackPolicy interface {
	Observe(event FeedbackEvent)
}

// LeveledPolicy preserves legacy behavior.
//
// Design:
// - Worker 0 keeps L0 relief first to reduce write stalls quickly.
// - Other workers keep picker order untouched.
type LeveledPolicy struct{}

// Arrange reorders priorities according to leveled compaction expectations.
func (LeveledPolicy) Arrange(workerID int, priorities []Priority) []Priority {
	if workerID == 0 {
		return MoveL0ToFront(priorities)
	}
	return priorities
}

// TieredPolicy prioritizes ingest convergence with guarded fairness.
//
// Design:
//   - Split priorities into four queues: L0 relief, ingest-keep, ingest-drain,
//     and regular.
//   - Interleave queues by pressure-aware quotas instead of draining one queue
//     completely, to avoid starvation.
//   - Worker 0 reserves one hard L0 slot under critical backlog.
type TieredPolicy struct {
	// ingestBias tunes how aggressively tiered scheduling prioritizes ingest work.
	// Positive values increase ingest quota; negative values protect regular work.
	ingestBias atomic.Int32
}

// Arrange reorders priorities to favor ingest-buffer workflows while keeping
// regular progress.
func (p *TieredPolicy) Arrange(workerID int, priorities []Priority) []Priority {
	if len(priorities) <= 1 {
		return priorities
	}
	ordered := append([]Priority(nil), priorities...)
	if !hasIngestWork(ordered) {
		return LeveledPolicy{}.Arrange(workerID, ordered)
	}
	if p == nil {
		return LeveledPolicy{}.Arrange(workerID, ordered)
	}
	queues := classifyQueues(ordered)
	ingestScore := maxScore(queues.keep, queues.drain)
	quota := p.effectiveQuota(ingestScore)
	return arrangeByQueues(workerID, queues, quota)
}

// Observe ingests runtime execution feedback and updates tiered scheduling bias.
func (p *TieredPolicy) Observe(event FeedbackEvent) {
	if p == nil {
		return
	}
	if !event.Priority.IngestMode.UsesIngest() {
		// Non-ingest success gradually decays stale ingest bias.
		if event.Err == nil {
			p.decayBiasTowardsZero()
		}
		return
	}
	if event.Err == nil {
		p.shiftBias(1)
		return
	}
	// ErrFillTables is a transient capacity miss; treat as neutral.
	if errors.Is(event.Err, ErrFillTables) {
		p.decayBiasTowardsZero()
		return
	}
	p.shiftBias(-1)
}

// HybridPolicy adapts between leveled and tiered scheduling.
//
// Design:
// - Low ingest pressure: keep leveled behavior for stable mixed workloads.
// - High ingest pressure: switch to tiered queue scheduling.
type HybridPolicy struct {
	tiered TieredPolicy
}

// Arrange selects policy behavior by ingest pressure.
func (p *HybridPolicy) Arrange(workerID int, priorities []Priority) []Priority {
	if len(priorities) <= 1 {
		return priorities
	}
	ordered := append([]Priority(nil), priorities...)
	if !hasIngestWork(ordered) {
		return LeveledPolicy{}.Arrange(workerID, ordered)
	}
	queues := classifyQueues(ordered)
	ingestScore := maxScore(queues.keep, queues.drain)
	if ingestScore < hybridTieredThreshold {
		return LeveledPolicy{}.Arrange(workerID, ordered)
	}
	if p == nil {
		tiered := &TieredPolicy{}
		return tiered.Arrange(workerID, ordered)
	}
	quota := p.tiered.effectiveQuota(ingestScore)
	return arrangeByQueues(workerID, queues, quota)
}

// Observe forwards runtime feedback to the embedded tiered controller.
func (p *HybridPolicy) Observe(event FeedbackEvent) {
	if p == nil {
		return
	}
	p.tiered.Observe(event)
}

// NewPolicy constructs a compaction policy by name.
// Unknown names gracefully fall back to leveled behavior.
func NewPolicy(name string) Policy {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", PolicyLeveled:
		return LeveledPolicy{}
	case PolicyTiered:
		return &TieredPolicy{}
	case PolicyHybrid:
		return &HybridPolicy{}
	default:
		return LeveledPolicy{}
	}
}

func hasIngestWork(priorities []Priority) bool {
	return slices.ContainsFunc(priorities, func(p Priority) bool {
		return p.IngestMode.UsesIngest()
	})
}

func classifyQueues(priorities []Priority) priorityQueues {
	var q priorityQueues
	for _, p := range priorities {
		switch {
		case p.Level == 0 && p.Adjusted >= l0ReliefScoreMin:
			q.l0 = append(q.l0, p)
		case p.IngestMode == IngestKeep:
			q.keep = append(q.keep, p)
		case p.IngestMode == IngestDrain:
			q.drain = append(q.drain, p)
		default:
			q.regular = append(q.regular, p)
		}
	}
	sortByAdjustedDesc(q.l0)
	sortByAdjustedDesc(q.keep)
	sortByAdjustedDesc(q.drain)
	sortByAdjustedDesc(q.regular)
	return q
}

func tieredQuotaByPressure(ingestScore float64) queueQuota {
	switch {
	case ingestScore >= 6:
		// Severe ingest backlog: aggressively drain/merge ingest first.
		return queueQuota{l0: 2, keep: 3, drain: 3, regular: 1}
	case ingestScore >= 3:
		// Balanced mode: keep ingest and regular making progress together.
		return queueQuota{l0: 2, keep: 2, drain: 2, regular: 2}
	default:
		// Mild ingest pressure: preserve regular throughput while still servicing ingest.
		return queueQuota{l0: 2, keep: 1, drain: 1, regular: 3}
	}
}

func (p *TieredPolicy) effectiveQuota(ingestScore float64) queueQuota {
	quota := tieredQuotaByPressure(ingestScore)
	return applyIngestBias(quota, int(p.ingestBias.Load()))
}

func applyIngestBias(quota queueQuota, bias int) queueQuota {
	if bias == 0 {
		return quota
	}
	if bias > 0 {
		shift := min(bias, 2)
		quota.keep += shift
		quota.drain += shift
		quota.regular = max(1, quota.regular-shift)
		return quota
	}
	shift := min(-bias, 2)
	for range shift {
		if quota.keep > 1 {
			quota.keep--
			quota.regular++
		}
		if quota.drain > 1 {
			quota.drain--
			quota.regular++
		}
	}
	return quota
}

func (p *TieredPolicy) shiftBias(delta int32) {
	for {
		old := p.ingestBias.Load()
		next := clampI32(old+delta, -2, 2)
		if p.ingestBias.CompareAndSwap(old, next) {
			return
		}
	}
}

func (p *TieredPolicy) decayBiasTowardsZero() {
	for {
		old := p.ingestBias.Load()
		var next int32
		switch {
		case old > 0:
			next = old - 1
		case old < 0:
			next = old + 1
		default:
			return
		}
		if p.ingestBias.CompareAndSwap(old, next) {
			return
		}
	}
}

func clampI32(v, lo, hi int32) int32 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func maxScore(slices ...[]Priority) float64 {
	maxScore := 0.0
	for _, items := range slices {
		for _, p := range items {
			if p.Adjusted > maxScore {
				maxScore = p.Adjusted
			}
		}
	}
	return maxScore
}

func arrangeByQueues(workerID int, q priorityQueues, quota queueQuota) []Priority {
	total := len(q.l0) + len(q.keep) + len(q.drain) + len(q.regular)
	if total == 0 {
		return nil
	}
	out := make([]Priority, 0, total)

	// Worker 0 reserves one critical L0 slot to quickly relieve write pressure.
	if workerID == 0 && len(q.l0) > 0 && q.l0[0].Adjusted >= l0CriticalScore {
		out = append(out, q.l0[0])
		q.l0 = q.l0[1:]
	}

	emit := func(queue *[]Priority, n int) {
		if n <= 0 {
			return
		}
		for range n {
			if len(*queue) == 0 {
				return
			}
			out = append(out, (*queue)[0])
			*queue = (*queue)[1:]
		}
	}

	for len(out) < total {
		before := len(out)
		emit(&q.l0, quota.l0)
		emit(&q.keep, quota.keep)
		emit(&q.drain, quota.drain)
		emit(&q.regular, quota.regular)
		if len(out) == before {
			// Fallback drain to avoid dead loops when some quotas are zero.
			out = append(out, q.l0...)
			out = append(out, q.keep...)
			out = append(out, q.drain...)
			out = append(out, q.regular...)
			break
		}
	}
	return out
}

func sortByAdjustedDesc(priorities []Priority) {
	sort.SliceStable(priorities, func(i, j int) bool {
		if priorities[i].Adjusted == priorities[j].Adjusted {
			return priorities[i].Score > priorities[j].Score
		}
		return priorities[i].Adjusted > priorities[j].Adjusted
	})
}
