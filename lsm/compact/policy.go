package compact

import "strings"

const (
	// PolicyLeveled keeps the current leveled-priority behavior.
	PolicyLeveled = "leveled"
	// PolicyTiered is reserved for a future tiered planner implementation.
	PolicyTiered = "tiered"
	// PolicyHybrid is reserved for a future hybrid planner implementation.
	PolicyHybrid = "hybrid"
)

// Policy arranges compaction priorities before execution.
//
// NOTE: PR-1 keeps selection behavior identical to legacy logic by default
// (leveled). Additional policy implementations can evolve independently.
type Policy interface {
	Name() string
	Arrange(workerID int, priorities []Priority) []Priority
}

// LeveledPolicy preserves legacy ordering behavior:
// worker#0 prefers L0 first, other workers keep original order.
type LeveledPolicy struct{}

// Name returns the policy identifier.
func (LeveledPolicy) Name() string { return PolicyLeveled }

// Arrange reorders priorities according to leveled compaction expectations.
func (LeveledPolicy) Arrange(workerID int, priorities []Priority) []Priority {
	if workerID == 0 {
		return MoveL0ToFront(priorities)
	}
	return priorities
}

// TieredPolicy currently aliases leveled ordering while the dedicated picker
// lands in follow-up iterations.
type TieredPolicy struct{}

// Name returns the policy identifier.
func (TieredPolicy) Name() string { return PolicyTiered }

// Arrange currently preserves leveled ordering for compatibility.
func (TieredPolicy) Arrange(workerID int, priorities []Priority) []Priority {
	return LeveledPolicy{}.Arrange(workerID, priorities)
}

// HybridPolicy currently aliases leveled ordering while hybrid strategy tuning
// lands in follow-up iterations.
type HybridPolicy struct{}

// Name returns the policy identifier.
func (HybridPolicy) Name() string { return PolicyHybrid }

// Arrange currently preserves leveled ordering for compatibility.
func (HybridPolicy) Arrange(workerID int, priorities []Priority) []Priority {
	return LeveledPolicy{}.Arrange(workerID, priorities)
}

// NewPolicy constructs a compaction policy by name.
// Unknown names gracefully fall back to leveled behavior.
func NewPolicy(name string) Policy {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", PolicyLeveled:
		return LeveledPolicy{}
	case PolicyTiered:
		return TieredPolicy{}
	case PolicyHybrid:
		return HybridPolicy{}
	default:
		return LeveledPolicy{}
	}
}

