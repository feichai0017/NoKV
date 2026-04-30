package mvccgc_test

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/percolator"
	"github.com/feichai0017/NoKV/raftstore/mvccgc"
	"github.com/stretchr/testify/require"
)

func TestMVCCGCSafePointPolicyUsesMountScopedSnapshotFloor(t *testing.T) {
	volKey, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)
	dataKey, err := fsmeta.EncodeInodeKey("data", 10)
	require.NoError(t, err)
	otherKey, err := fsmeta.EncodeInodeKey("other", 10)
	require.NoError(t, err)

	policy := mvccgc.SafePointPolicy{
		RequestedSafePoint: 1_000,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			GlobalFloor: 50,
			MountFloors: map[string]uint64{
				"vol":  50,
				"data": 200,
			},
		},
		TxnFloor: 80,
	}

	require.Equal(t, uint64(50), policy.EffectiveForKey(volKey))
	require.Equal(t, uint64(80), policy.EffectiveForKey(dataKey))
	require.Equal(t, uint64(80), policy.EffectiveForKey(otherKey))
}

func TestMVCCGCSafePointPolicyFallsBackToGlobalFloorForUnknownKeys(t *testing.T) {
	policy := mvccgc.SafePointPolicy{
		RequestedSafePoint: 1_000,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			GlobalFloor: 50,
			MountFloors: map[string]uint64{
				"vol": 50,
			},
		},
		TxnFloor: 80,
	}

	require.Equal(t, uint64(50), policy.EffectiveForKey([]byte("raw-user-key")))
}

func TestMVCCGCSafePointPolicyHonorsDisabledRequestedSafePoint(t *testing.T) {
	key, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)

	policy := mvccgc.SafePointPolicy{
		RequestedSafePoint: 0,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			GlobalFloor: 50,
			MountFloors: map[string]uint64{
				"vol": 50,
			},
		},
		TxnFloor: 80,
	}

	require.Zero(t, policy.EffectiveForKey(key))
}

func TestMVCCGCSafePointPolicyUsesRequestedWhenUnblocked(t *testing.T) {
	key, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)

	policy := mvccgc.SafePointPolicy{RequestedSafePoint: 1_000}
	require.Equal(t, uint64(1_000), policy.EffectiveForKey(key))
}

func TestMVCCGCSafePointPolicyPlansWritesWithKeyScopedFloor(t *testing.T) {
	volKey, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)
	otherKey, err := fsmeta.EncodeInodeKey("other", 10)
	require.NoError(t, err)
	versions := []percolator.GCWriteVersion{
		{CommitTs: 150, Write: percolator.Write{Kind: kvrpcpb.Mutation_Put, StartTs: 140}},
		{CommitTs: 90, Write: percolator.Write{Kind: kvrpcpb.Mutation_Put, StartTs: 80}},
		{CommitTs: 40, Write: percolator.Write{Kind: kvrpcpb.Mutation_Put, StartTs: 30}},
	}
	policy := mvccgc.SafePointPolicy{
		RequestedSafePoint: 100,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			GlobalFloor: 50,
			MountFloors: map[string]uint64{
				"vol": 50,
			},
		},
	}

	volPlan := policy.PlanWritesForKey(volKey, versions)
	require.True(t, volPlan[1].Keep)
	require.True(t, volPlan[2].Keep)
	require.True(t, volPlan[2].Anchor)

	otherPlan := policy.PlanWritesForKey(otherKey, versions)
	require.True(t, otherPlan[1].Keep)
	require.True(t, otherPlan[1].Anchor)
	require.False(t, otherPlan[2].Keep)
}
