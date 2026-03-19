package kv

import (
	"bytes"
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

	hitCount atomic.Int64
}

// ValueSeparationPolicyMatcher matches entries against policies.
type ValueSeparationPolicyMatcher struct {
	policies []*ValueSeparationPolicy
	stats    *ValueSeparationPolicyStats
}

// ValueSeparationPolicyStats tracks policy usage statistics.
type ValueSeparationPolicyStats struct {
	totalDecisions atomic.Int64
}

// NewValueSeparationPolicyMatcher creates a new policy matcher.
func NewValueSeparationPolicyMatcher(policies []*ValueSeparationPolicy) *ValueSeparationPolicyMatcher {
	return &ValueSeparationPolicyMatcher{
		policies: policies,
		stats:    &ValueSeparationPolicyStats{},
	}
}

// MatchPolicy finds the first matching policy for the given entry.
// Policies are evaluated in the order they are added, so more specific policies should be added first.
// Returns nil if no policy matches.
func (m *ValueSeparationPolicyMatcher) MatchPolicy(e *Entry) *ValueSeparationPolicy {
	if len(m.policies) == 0 {
		m.recordPolicyHit(nil)
		return nil
	}

	cf, userKey, _, ok := SplitInternalKey(e.Key)
	if !ok {
		m.recordPolicyHit(nil)
		return nil
	}

	for _, p := range m.policies {
		policy := p

		// Check CF match
		if policy.CF != cf {
			continue
		}

		// Check key prefix match
		if len(policy.KeyPrefix) > 0 && !bytes.HasPrefix(userKey, policy.KeyPrefix) {
			continue
		}

		// Policy matches - update stats
		m.recordPolicyHit(policy)
		return policy
	}

	m.recordPolicyHit(nil)
	return nil
}

func (m *ValueSeparationPolicyMatcher) recordPolicyHit(policy *ValueSeparationPolicy) {
	m.stats.totalDecisions.Add(1)

	if policy == nil {
		return
	}

	policy.hitCount.Add(1)
}

// GetStats returns a copy of the current policy statistics.
func (m *ValueSeparationPolicyMatcher) GetStats() map[string]int64 {
	result := make(map[string]int64)
	for _, p := range m.policies {
		statsKey := fmt.Sprintf("%s:%s:%s", p.CF.String(), p.KeyPrefix, p.Strategy)
		if count := p.hitCount.Load(); count > 0 {
			result[statsKey] = count
		}
	}
	result["_total_decisions"] = m.stats.totalDecisions.Load()
	return result
}

// NewAlwaysInlinePolicy creates a policy that always keeps values in LSM.
func NewAlwaysInlinePolicy(cf ColumnFamily, keyPrefix string) *ValueSeparationPolicy {
	prefixBytes := []byte(keyPrefix)
	return &ValueSeparationPolicy{
		CF:        cf,
		KeyPrefix: prefixBytes,
		Strategy:  AlwaysInline,
	}
}

// NewAlwaysOffloadPolicy creates a policy that always moves values to vlog.
func NewAlwaysOffloadPolicy(cf ColumnFamily, keyPrefix string) *ValueSeparationPolicy {
	prefixBytes := []byte(keyPrefix)
	return &ValueSeparationPolicy{
		CF:        cf,
		KeyPrefix: prefixBytes,
		Strategy:  AlwaysOffload,
	}
}

// NewThresholdBasedPolicy creates a policy that separates values based on threshold.
func NewThresholdBasedPolicy(cf ColumnFamily, keyPrefix string, threshold int64) *ValueSeparationPolicy {
	prefixBytes := []byte(keyPrefix)
	return &ValueSeparationPolicy{
		CF:        cf,
		KeyPrefix: prefixBytes,
		Strategy:  ThresholdBased,
		Threshold: threshold,
	}
}
