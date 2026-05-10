package client

import (
	"testing"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestNormalizeReadOptionsDefaultsToStrongLeaderOnly(t *testing.T) {
	opts := normalizeReadOptions(ReadOptions{})
	require.Equal(t, kvrpcpb.ReadConsistency_READ_CONSISTENCY_STRONG, opts.Consistency)
	require.Equal(t, kvrpcpb.ReadPreference_READ_PREFERENCE_LEADER_ONLY, opts.Preference)
}

func TestDefaultReadOptionsUseStrongLeaderOnly(t *testing.T) {
	opts := DefaultReadOptions()
	require.Equal(t, kvrpcpb.ReadConsistency_READ_CONSISTENCY_STRONG, opts.Consistency)
	require.Equal(t, kvrpcpb.ReadPreference_READ_PREFERENCE_LEADER_ONLY, opts.Preference)
	require.Zero(t, opts.MaxStaleReadIndex)
	require.Zero(t, opts.MaxStaleReadMS)
}

func TestNormalizeReadOptionsPreservesSupportedFollowerModes(t *testing.T) {
	opts := normalizeReadOptions(ReadOptions{
		Consistency:       kvrpcpb.ReadConsistency_READ_CONSISTENCY_BOUNDED_STALE,
		Preference:        kvrpcpb.ReadPreference_READ_PREFERENCE_FOLLOWER_PREFER,
		MaxStaleReadIndex: 9,
		MaxStaleReadMS:    10,
	})
	require.Equal(t, kvrpcpb.ReadConsistency_READ_CONSISTENCY_BOUNDED_STALE, opts.Consistency)
	require.Equal(t, kvrpcpb.ReadPreference_READ_PREFERENCE_FOLLOWER_PREFER, opts.Preference)
	require.Equal(t, uint64(9), opts.MaxStaleReadIndex)
	require.Equal(t, uint64(10), opts.MaxStaleReadMS)
	require.False(t, strongFollowerPrefer(opts))
	require.True(t, followerPrefer(opts))

	opts.Consistency = kvrpcpb.ReadConsistency_READ_CONSISTENCY_STRONG
	require.True(t, strongFollowerPrefer(opts))
}

func TestLeaderReadOptionsUpgradeBoundedStaleToStrong(t *testing.T) {
	opts := leaderReadOptions(ReadOptions{
		Consistency:       kvrpcpb.ReadConsistency_READ_CONSISTENCY_BOUNDED_STALE,
		Preference:        kvrpcpb.ReadPreference_READ_PREFERENCE_FOLLOWER_PREFER,
		MaxStaleReadIndex: 7,
		MaxStaleReadMS:    8,
	})
	require.Equal(t, kvrpcpb.ReadConsistency_READ_CONSISTENCY_STRONG, opts.Consistency)
	require.Equal(t, kvrpcpb.ReadPreference_READ_PREFERENCE_LEADER_ONLY, opts.Preference)
	require.Zero(t, opts.MaxStaleReadIndex)
	require.Zero(t, opts.MaxStaleReadMS)
}
