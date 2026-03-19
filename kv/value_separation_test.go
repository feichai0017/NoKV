package kv_test

import (
	"os"
	"testing"

	"github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/kv"
	"github.com/stretchr/testify/require"
)

func TestValueSeparationPolicy(t *testing.T) {
	inlinePolicy := kv.NewAlwaysInlinePolicy(kv.CFDefault, "meta_")
	require.Equal(t, kv.CFDefault, inlinePolicy.CF)
	require.Equal(t, []byte("meta_"), inlinePolicy.KeyPrefix)
	require.Equal(t, kv.AlwaysInline, inlinePolicy.Strategy)

	offloadPolicy := kv.NewAlwaysOffloadPolicy(kv.CFDefault, "large_")
	require.Equal(t, kv.CFDefault, offloadPolicy.CF)
	require.Equal(t, []byte("large_"), offloadPolicy.KeyPrefix)
	require.Equal(t, kv.AlwaysOffload, offloadPolicy.Strategy)

	thresholdPolicy := kv.NewThresholdBasedPolicy(kv.CFDefault, "medium_", 2048)
	require.Equal(t, kv.CFDefault, thresholdPolicy.CF)
	require.Equal(t, []byte("medium_"), thresholdPolicy.KeyPrefix)
	require.Equal(t, kv.ThresholdBased, thresholdPolicy.Strategy)
	require.Equal(t, int64(2048), thresholdPolicy.Threshold)
}

func TestValueSeparationPolicyMatcher(t *testing.T) {
	policies := []*kv.ValueSeparationPolicy{
		kv.NewAlwaysInlinePolicy(kv.CFDefault, "meta_"),
		kv.NewAlwaysOffloadPolicy(kv.CFDefault, "large_"),
		kv.NewThresholdBasedPolicy(kv.CFDefault, "medium_", 1024),
		kv.NewAlwaysInlinePolicy(kv.CFLock, ""),
	}

	matcher := kv.NewValueSeparationPolicyMatcher(policies)

	// Test meta_ prefix (should match inline policy)
	metaEntry := kv.NewInternalEntry(kv.CFDefault, []byte("meta_key1"), 1, []byte("value"), 0, 0)
	policy := matcher.MatchPolicy(metaEntry)
	require.NotNil(t, policy)
	require.Equal(t, kv.AlwaysInline, policy.Strategy)
	metaEntry.DecrRef()

	// Test large_ prefix (should match offload policy)
	largeEntry := kv.NewInternalEntry(kv.CFDefault, []byte("large_key1"), 1, []byte("value"), 0, 0)
	policy = matcher.MatchPolicy(largeEntry)
	require.NotNil(t, policy)
	require.Equal(t, kv.AlwaysOffload, policy.Strategy)
	largeEntry.DecrRef()

	// Test medium_ prefix with small value (should match threshold policy)
	mediumEntry := kv.NewInternalEntry(kv.CFDefault, []byte("medium_key1"), 1, []byte("small"), 0, 0)
	policy = matcher.MatchPolicy(mediumEntry)
	require.NotNil(t, policy)
	require.Equal(t, kv.ThresholdBased, policy.Strategy)
	require.Equal(t, int64(1024), policy.Threshold)
	mediumEntry.DecrRef()

	// Test lock CF (should match inline policy)
	lockEntry := kv.NewInternalEntry(kv.CFLock, []byte("any_key"), 1, []byte("value"), 0, 0)
	policy = matcher.MatchPolicy(lockEntry)
	require.NotNil(t, policy)
	require.Equal(t, kv.AlwaysInline, policy.Strategy)
	lockEntry.DecrRef()

	// Test unmatched key (should return nil)
	unmatchedEntry := kv.NewInternalEntry(kv.CFDefault, []byte("unmatched_key"), 1, []byte("value"), 0, 0)
	policy = matcher.MatchPolicy(unmatchedEntry)
	require.Nil(t, policy)
	unmatchedEntry.DecrRef()
}

func TestValueSeparationPolicyStats(t *testing.T) {
	policies := []*kv.ValueSeparationPolicy{
		kv.NewAlwaysInlinePolicy(kv.CFDefault, "meta_"),
		kv.NewAlwaysOffloadPolicy(kv.CFDefault, "large_"),
	}

	matcher := kv.NewValueSeparationPolicyMatcher(policies)

	// Test initial stats
	stats := matcher.GetStats()
	require.Equal(t, int64(0), stats["_total_decisions"])

	// Make some policy matches
	metaEntry := kv.NewInternalEntry(kv.CFDefault, []byte("meta_key1"), 1, []byte("value"), 0, 0)
	matcher.MatchPolicy(metaEntry)
	metaEntry.DecrRef()

	largeEntry := kv.NewInternalEntry(kv.CFDefault, []byte("large_key1"), 1, []byte("value"), 0, 0)
	matcher.MatchPolicy(largeEntry)
	largeEntry.DecrRef()

	// Check updated stats
	stats = matcher.GetStats()
	require.Equal(t, int64(2), stats["_total_decisions"])
	require.Equal(t, int64(1), stats["default:meta_:always_inline"])
	require.Equal(t, int64(1), stats["default:large_:always_offload"])
}

func TestValueSeparationPolicyIntegration(t *testing.T) {
	var err error
	var stats map[string]int64

	workDir, err := os.MkdirTemp("", "nokv-value-separation-test")
	require.NoError(t, err)
	defer func() {
		err = os.RemoveAll(workDir)
		require.NoError(t, err)
	}()

	policies := []*kv.ValueSeparationPolicy{
		kv.NewAlwaysInlinePolicy(kv.CFDefault, "meta_"),
		kv.NewAlwaysOffloadPolicy(kv.CFDefault, "large_"),
		kv.NewThresholdBasedPolicy(kv.CFDefault, "medium_", 64),
	}
	opt := &NoKV.Options{
		WorkDir:                 workDir,
		MaxBatchCount:           3,
		MaxBatchSize:            1024,
		MemTableSize:            1024,
		ValueThreshold:          32, // Global fallback threshold
		ValueSeparationPolicies: policies,
	}

	db := NoKV.Open(opt)
	defer func() {
		err = db.Close()
		require.NoError(t, err)
	}()

	largeValue := make([]byte, 128) // Larger than both thresholds
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Test meta_ prefix (should be inlined regardless of size)
	err = db.Set([]byte("meta_test"), largeValue)
	require.NoError(t, err)
	stats = db.GetValueSeparationPolicyStats()
	require.Equal(t, int64(1), stats["_total_decisions"])
	require.Equal(t, int64(1), stats["default:meta_:always_inline"])

	// Test large_ prefix (should be offloaded regardless of size)
	err = db.Set([]byte("large_test"), []byte("small"))
	require.NoError(t, err)
	stats = db.GetValueSeparationPolicyStats()
	require.Equal(t, int64(2), stats["_total_decisions"])
	require.Equal(t, int64(1), stats["default:large_:always_offload"])

	// Test medium_ prefix with small value (should be inlined due to threshold)
	err = db.Set([]byte("medium_test"), []byte("small"))
	require.NoError(t, err)
	stats = db.GetValueSeparationPolicyStats()
	require.Equal(t, int64(3), stats["_total_decisions"])
	require.Equal(t, int64(1), stats["default:medium_:threshold_based"])

	// Test unmatched key with small value (should use global threshold)
	err = db.Set([]byte("regular_test"), []byte("small"))
	require.NoError(t, err)
	stats = db.GetValueSeparationPolicyStats()
	require.Equal(t, int64(4), stats["_total_decisions"])
	require.Equal(t, int64(1), stats["default:meta_:always_inline"])
	require.Equal(t, int64(1), stats["default:large_:always_offload"])
	require.Equal(t, int64(1), stats["default:medium_:threshold_based"])
}
