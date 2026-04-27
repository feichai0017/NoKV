package kv

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
)

// ValueSeparationStrategy defines how values are separated.
type ValueSeparationStrategy int

const (
	// ThresholdBased separates values based on size threshold.
	ThresholdBased ValueSeparationStrategy = iota
	// AlwaysInline always keeps values in LSM.
	AlwaysInline
	// AlwaysOffload always moves values to vlog.
	AlwaysOffload
)

func (s ValueSeparationStrategy) String() string {
	switch s {
	case ThresholdBased:
		return "threshold_based"
	case AlwaysInline:
		return "always_inline"
	case AlwaysOffload:
		return "always_offload"
	default:
		return "unknown"
	}
}

type ValueSeparationPolicy struct {
	// CF specifies the column family this policy applies to.
	CF ColumnFamily

	// KeyPrefix specifies the key prefix this policy applies to.
	// Empty byte slice matches all keys.
	KeyPrefix []byte

	// Strategy defines the value separation strategy.
	Strategy ValueSeparationStrategy

	// Threshold is the value size threshold in bytes.
	// Only used when Strategy is ThresholdBased.
	Threshold int64
}

// ValueSeparationPolicyMatcher matches entries against policies.
type ValueSeparationPolicyMatcher struct {
	policies       []*ValueSeparationPolicy
	hitCounts      []atomic.Int64
	totalDecisions atomic.Int64
}

// NewValueSeparationPolicyMatcher creates a new policy matcher.
func NewValueSeparationPolicyMatcher(policies []*ValueSeparationPolicy) *ValueSeparationPolicyMatcher {
	hitCounts := make([]atomic.Int64, len(policies))
	for i := range hitCounts {
		hitCounts[i].Store(0)
	}
	return &ValueSeparationPolicyMatcher{
		policies:       policies,
		hitCounts:      hitCounts,
		totalDecisions: atomic.Int64{},
	}
}

// MatchPolicy finds the first matching policy for the given entry.
// Policies are evaluated in the order they are added, so more specific policies should be added first.
// Returns nil if no policy matches. Increments totalDecisions and per-policy hit counters.
func (m *ValueSeparationPolicyMatcher) MatchPolicy(e *Entry) *ValueSeparationPolicy {
	m.totalDecisions.Add(1)
	return m.matchPolicyLocked(e, true)
}

// PeekPolicy returns the matching policy without recording a decision. It is
// used by callers that need to read the policy outcome without inflating the
// stats counters — e.g. the metadata-profile fast path that pre-scans a batch
// to decide whether to call vlog.write at all (the actual decision is later
// recorded by the vlog path itself when it runs).
func (m *ValueSeparationPolicyMatcher) PeekPolicy(e *Entry) *ValueSeparationPolicy {
	return m.matchPolicyLocked(e, false)
}

func (m *ValueSeparationPolicyMatcher) matchPolicyLocked(e *Entry, recordHit bool) *ValueSeparationPolicy {
	if len(m.policies) == 0 {
		return nil
	}

	cf, userKey, _, ok := SplitInternalKey(e.Key)
	if !ok {
		return nil
	}

	for i, p := range m.policies {
		if p == nil {
			continue
		}
		// Check CF match
		if p.CF != cf {
			continue
		}

		// Check key prefix match
		if len(p.KeyPrefix) > 0 && !bytes.HasPrefix(userKey, p.KeyPrefix) {
			continue
		}

		if recordHit {
			m.hitCounts[i].Add(1)
		}
		return p
	}

	return nil
}

// GetStats returns a copy of the current policy statistics.
func (m *ValueSeparationPolicyMatcher) GetStats() map[string]int64 {
	result := make(map[string]int64)

	for i, p := range m.policies {
		prefixStr := string(p.KeyPrefix)
		statsKey := fmt.Sprintf("%s:%s:%s", p.CF.String(), prefixStr, p.Strategy)
		result[statsKey] = m.hitCounts[i].Load()
	}

	result["_total_decisions"] = m.totalDecisions.Load()
	return result
}

// NewAlwaysInlinePolicy creates a policy that always keeps values in LSM.
func NewAlwaysInlinePolicy(cf ColumnFamily, keyPrefix string) (*ValueSeparationPolicy, error) {
	prefixBytes := []byte(keyPrefix)
	return &ValueSeparationPolicy{
		CF:        cf,
		KeyPrefix: prefixBytes,
		Strategy:  AlwaysInline,
	}, nil
}

// NewAlwaysOffloadPolicy creates a policy that always moves values to vlog.
func NewAlwaysOffloadPolicy(cf ColumnFamily, keyPrefix string) (*ValueSeparationPolicy, error) {
	prefixBytes := []byte(keyPrefix)
	return &ValueSeparationPolicy{
		CF:        cf,
		KeyPrefix: prefixBytes,
		Strategy:  AlwaysOffload,
	}, nil
}

// NewThresholdBasedPolicy creates a policy that separates values based on threshold.
func NewThresholdBasedPolicy(cf ColumnFamily, keyPrefix string, threshold int64) (*ValueSeparationPolicy, error) {
	if threshold <= 0 {
		return nil, errors.New("threshold must be positive")
	}
	prefixBytes := []byte(keyPrefix)
	return &ValueSeparationPolicy{
		CF:        cf,
		KeyPrefix: prefixBytes,
		Strategy:  ThresholdBased,
		Threshold: threshold,
	}, nil
}
