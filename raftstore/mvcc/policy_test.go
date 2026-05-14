// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mvcc_test

import (
	"testing"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
	"github.com/stretchr/testify/require"
)

func testMountResolver(userKey []byte) (uint64, bool) {
	switch string(userKey) {
	case "vol/key":
		return 1, true
	case "data/key":
		return 2, true
	case "other/key":
		return 3, true
	default:
		return 0, false
	}
}

func TestMVCCGCSafePointPolicyUsesMountScopedSnapshotFloor(t *testing.T) {
	volKey := []byte("vol/key")
	dataKey := []byte("data/key")
	otherKey := []byte("other/key")

	policy := storemvcc.SafePointPolicy{
		RequestedSafePoint: 1_000,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			GlobalFloor: 50,
			MountFloors: map[uint64]uint64{
				1: 50,
				2: 200,
			},
		},
		TxnFloor: 80,
		Mount:    testMountResolver,
	}

	require.Equal(t, uint64(50), policy.EffectiveForKey(volKey))
	require.Equal(t, uint64(80), policy.EffectiveForKey(dataKey))
	require.Equal(t, uint64(80), policy.EffectiveForKey(otherKey))
}

func TestMVCCGCSafePointPolicyKeepsGlobalFloorAsUnknownLayoutFallback(t *testing.T) {
	policy := storemvcc.SafePointPolicy{
		RequestedSafePoint: 1_000,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			GlobalFloor: 40,
			MountFloors: map[uint64]uint64{
				1: 80,
			},
		},
		Mount: testMountResolver,
	}

	require.Equal(t, uint64(80), policy.EffectiveForKey([]byte("vol/key")))
	require.Equal(t, uint64(40), policy.EffectiveForKey([]byte("unknown/key")))
}

func TestMVCCGCSafePointPolicyFallsBackToGlobalFloorForUnknownKeys(t *testing.T) {
	policy := storemvcc.SafePointPolicy{
		RequestedSafePoint: 1_000,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			GlobalFloor: 50,
			MountFloors: map[uint64]uint64{
				1: 50,
			},
		},
		TxnFloor: 80,
	}

	require.Equal(t, uint64(50), policy.EffectiveForKey([]byte("raw-user-key")))
}

func TestMVCCGCSafePointPolicyHonorsDisabledRequestedSafePoint(t *testing.T) {
	policy := storemvcc.SafePointPolicy{
		RequestedSafePoint: 0,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			GlobalFloor: 50,
			MountFloors: map[uint64]uint64{
				1: 50,
			},
		},
		TxnFloor: 80,
		Mount:    testMountResolver,
	}

	require.Zero(t, policy.EffectiveForKey([]byte("vol/key")))
}

func TestMVCCGCSafePointPolicyUsesRequestedWhenUnblocked(t *testing.T) {
	policy := storemvcc.SafePointPolicy{RequestedSafePoint: 1_000}
	require.Equal(t, uint64(1_000), policy.EffectiveForKey([]byte("vol/key")))
}

func TestMVCCGCSafePointPolicyPlansWritesWithKeyScopedFloor(t *testing.T) {
	volKey := []byte("vol/key")
	otherKey := []byte("other/key")
	versions := []txnmvcc.GCWriteVersion{
		{CommitTs: 150, Write: txnmvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: 140}},
		{CommitTs: 90, Write: txnmvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: 80}},
		{CommitTs: 40, Write: txnmvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: 30}},
	}
	policy := storemvcc.SafePointPolicy{
		RequestedSafePoint: 100,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			MountFloors: map[uint64]uint64{
				1: 50,
			},
		},
		Mount: testMountResolver,
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
