package state_test

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
)

func TestApplyRangeChangeToSnapshot(t *testing.T) {
	parent := testDescriptor(40, []byte("a"), []byte("z"))
	left := testDescriptor(40, []byte("a"), []byte("m"))
	right := testDescriptor(41, []byte("m"), []byte("z"))

	snapshot := rootstate.Snapshot{
		State:       rootstate.State{ClusterEpoch: 5},
		Descriptors: map[uint64]descriptor.Descriptor{},
	}
	snapshot.Descriptors[parent.RegionID] = parent

	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&snapshot, rootevent.RegionSplitPlanned(parent.RegionID, []byte("m"), left, right)))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.Contains(t, snapshot.PendingRangeChanges, parent.RegionID)

	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&snapshot, rootevent.RegionSplitCommitted(parent.RegionID, []byte("m"), left, right)))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.NotContains(t, snapshot.PendingRangeChanges, parent.RegionID)

	merged := testDescriptor(50, []byte("a"), []byte("z"))
	leftMerge := testDescriptor(48, []byte("a"), []byte("m"))
	rightMerge := testDescriptor(49, []byte("m"), []byte("z"))
	mergeSnapshot := rootstate.Snapshot{
		State: rootstate.State{ClusterEpoch: 7},
		Descriptors: map[uint64]descriptor.Descriptor{
			leftMerge.RegionID:  leftMerge,
			rightMerge.RegionID: rightMerge,
		},
	}

	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&mergeSnapshot, rootevent.RegionMergePlanned(leftMerge.RegionID, rightMerge.RegionID, merged)))
	require.Equal(t, uint64(8), mergeSnapshot.State.ClusterEpoch)
	require.Contains(t, mergeSnapshot.PendingRangeChanges, merged.RegionID)

	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&mergeSnapshot, rootevent.RegionMerged(leftMerge.RegionID, rightMerge.RegionID, merged)))
	require.Equal(t, uint64(8), mergeSnapshot.State.ClusterEpoch)
	require.NotContains(t, mergeSnapshot.PendingRangeChanges, merged.RegionID)
}

func TestApplySubtreeAuthorityHandoffToSnapshot(t *testing.T) {
	snapshot := rootstate.Snapshot{
		Mounts: map[string]rootstate.MountRecord{
			"vol": {MountID: "vol", RootInode: 1, State: rootstate.MountStateActive},
		},
	}

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 1}, rootevent.SubtreeAuthorityDeclared("vol", 1, "vol", 0, 10))
	key := rootstate.SubtreeAuthorityKey("vol", 1)
	require.Equal(t, rootstate.SubtreeAuthority{
		SubtreeID:   key,
		Mount:       "vol",
		RootInode:   1,
		AuthorityID: "vol",
		Era:         0,
		Frontier:    10,
		State:       rootstate.SubtreeAuthorityActive,
		DeclaredAt:  rootstate.Cursor{Term: 1, Index: 1},
	}, snapshot.Subtrees[key])

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 2}, rootevent.SubtreeHandoffStarted("vol", 1, 12))
	started := snapshot.Subtrees[key]
	require.Equal(t, rootstate.SubtreeAuthorityHandoff, started.State)
	require.Equal(t, "vol", started.PredecessorAuthorityID)
	require.Equal(t, uint64(0), started.PredecessorEra)
	require.Equal(t, uint64(12), started.PredecessorFrontier)
	require.Equal(t, "vol/1#1", started.SuccessorAuthorityID)
	require.Equal(t, uint64(1), started.SuccessorEra)

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 3}, rootevent.SubtreeHandoffCompleted("vol", 1, 13))
	completed := snapshot.Subtrees[key]
	require.Equal(t, rootstate.SubtreeAuthorityActive, completed.State)
	require.Equal(t, "vol/1#1", completed.AuthorityID)
	require.Equal(t, uint64(1), completed.Era)
	require.Equal(t, uint64(13), completed.Frontier)
	require.Equal(t, uint64(13), completed.InheritedFrontier)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 3}, completed.HandoffCompletedAt)
}

func TestApplyMountRegisteredDeclaresRootAuthority(t *testing.T) {
	snapshot := rootstate.Snapshot{}
	cursor := rootstate.Cursor{Term: 1, Index: 1}
	rootstate.ApplyEventToSnapshot(&snapshot, cursor, rootevent.MountRegistered("vol", 1, 1))

	key := rootstate.SubtreeAuthorityKey("vol", 1)
	require.Equal(t, rootstate.SubtreeAuthority{
		SubtreeID:   key,
		Mount:       "vol",
		RootInode:   1,
		AuthorityID: "vol",
		Era:         0,
		Frontier:    0,
		State:       rootstate.SubtreeAuthorityActive,
		DeclaredAt:  cursor,
	}, snapshot.Subtrees[key])
}

func TestApplySubtreeAuthorityRejectsIncompleteCoverage(t *testing.T) {
	snapshot := rootstate.Snapshot{}
	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 1}, rootevent.SubtreeAuthorityDeclared("vol", 1, "vol", 0, 10))
	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 2}, rootevent.SubtreeHandoffStarted("vol", 1, 20))
	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 3}, rootevent.SubtreeHandoffCompleted("vol", 1, 19))

	got := snapshot.Subtrees[rootstate.SubtreeAuthorityKey("vol", 1)]
	require.Equal(t, rootstate.SubtreeAuthorityHandoff, got.State)
	require.Equal(t, uint64(0), got.Era)
}

func TestApplyQuotaFenceUpdatedToSnapshot(t *testing.T) {
	snapshot := rootstate.Snapshot{}
	cursor := rootstate.Cursor{Term: 1, Index: 2}
	rootstate.ApplyEventToSnapshot(&snapshot, cursor, rootevent.QuotaFenceUpdated("vol", 7, 4096, 12, 1, 99))

	key := rootstate.QuotaFenceKey("vol", 7)
	require.Equal(t, rootstate.QuotaFence{
		SubjectID:   key,
		Mount:       "vol",
		RootInode:   7,
		LimitBytes:  4096,
		LimitInodes: 12,
		Era:         1,
		Frontier:    99,
		UpdatedAt:   cursor,
	}, snapshot.Quotas[key])

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 3}, rootevent.QuotaFenceUpdated("vol", 7, 1, 1, 1, 100))
	require.Equal(t, uint64(4096), snapshot.Quotas[key].LimitBytes)

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 4}, rootevent.QuotaFenceUpdated("vol", 7, 0, 0, 2, 101))
	require.Equal(t, uint64(0), snapshot.Quotas[key].LimitBytes)
	require.Equal(t, uint64(2), snapshot.Quotas[key].Era)
}

func TestApplyRangeChangeCancelToSnapshot(t *testing.T) {
	parent := testDescriptor(140, []byte("a"), []byte("z"))
	left := testDescriptor(140, []byte("a"), []byte("m"))
	right := testDescriptor(141, []byte("m"), []byte("z"))
	left.RootEpoch = parent.RootEpoch + 1
	right.RootEpoch = parent.RootEpoch + 1
	left.EnsureHash()
	right.EnsureHash()

	splitSnapshot := rootstate.Snapshot{
		State:       rootstate.State{ClusterEpoch: 10},
		Descriptors: map[uint64]descriptor.Descriptor{parent.RegionID: parent},
	}
	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&splitSnapshot, rootevent.RegionSplitPlanned(parent.RegionID, []byte("m"), left, right)))
	require.Equal(t, parent, splitSnapshot.PendingRangeChanges[parent.RegionID].BaseParent)
	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&splitSnapshot, rootevent.RegionSplitCancelled(parent.RegionID, []byte("m"), left, right, parent)))
	require.Equal(t, parent, splitSnapshot.Descriptors[parent.RegionID])
	require.NotContains(t, splitSnapshot.Descriptors, right.RegionID)
	require.NotContains(t, splitSnapshot.PendingRangeChanges, parent.RegionID)

	baseLeft := testDescriptor(148, []byte("a"), []byte("m"))
	baseRight := testDescriptor(149, []byte("m"), []byte("z"))
	merged := testDescriptor(150, []byte("a"), []byte("z"))
	baseLeft.RootEpoch = 5
	baseRight.RootEpoch = 5
	merged.RootEpoch = 6
	baseLeft.EnsureHash()
	baseRight.EnsureHash()
	merged.EnsureHash()
	mergeSnapshot := rootstate.Snapshot{
		State: rootstate.State{ClusterEpoch: 20},
		Descriptors: map[uint64]descriptor.Descriptor{
			baseLeft.RegionID:  baseLeft,
			baseRight.RegionID: baseRight,
		},
	}
	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&mergeSnapshot, rootevent.RegionMergePlanned(baseLeft.RegionID, baseRight.RegionID, merged)))
	require.Equal(t, baseLeft, mergeSnapshot.PendingRangeChanges[merged.RegionID].BaseLeft)
	require.Equal(t, baseRight, mergeSnapshot.PendingRangeChanges[merged.RegionID].BaseRight)
	require.True(t, rootstate.ApplyRangeChangeToSnapshot(&mergeSnapshot, rootevent.RegionMergeCancelled(baseLeft.RegionID, baseRight.RegionID, merged, baseLeft, baseRight)))
	require.Equal(t, baseLeft, mergeSnapshot.Descriptors[baseLeft.RegionID])
	require.Equal(t, baseRight, mergeSnapshot.Descriptors[baseRight.RegionID])
	require.NotContains(t, mergeSnapshot.Descriptors, merged.RegionID)
	require.NotContains(t, mergeSnapshot.PendingRangeChanges, merged.RegionID)
}

func TestApplyPeerChangeToSnapshot(t *testing.T) {
	current := testDescriptor(11, []byte("a"), []byte("m"))
	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch++
	target.EnsureHash()

	snapshot := rootstate.Snapshot{
		State:       rootstate.State{ClusterEpoch: 5},
		Descriptors: map[uint64]descriptor.Descriptor{current.RegionID: current},
	}

	require.True(t, rootstate.ApplyPeerChangeToSnapshot(&snapshot, rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.Contains(t, snapshot.PendingPeerChanges, target.RegionID)
	require.Equal(t, current, snapshot.PendingPeerChanges[target.RegionID].Base)

	require.True(t, rootstate.ApplyPeerChangeToSnapshot(&snapshot, rootevent.PeerAdded(target.RegionID, 2, 201, target)))
	require.Equal(t, uint64(6), snapshot.State.ClusterEpoch)
	require.NotContains(t, snapshot.PendingPeerChanges, target.RegionID)
}

func TestApplyPeerChangeCancelToSnapshot(t *testing.T) {
	current := testDescriptor(111, []byte("a"), []byte("m"))
	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch++
	target.EnsureHash()

	snapshot := rootstate.Snapshot{
		State:       rootstate.State{ClusterEpoch: 5},
		Descriptors: map[uint64]descriptor.Descriptor{current.RegionID: current},
	}
	require.True(t, rootstate.ApplyPeerChangeToSnapshot(&snapshot, rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)))
	require.True(t, rootstate.ApplyPeerChangeToSnapshot(&snapshot, rootevent.PeerAdditionCancelled(target.RegionID, 2, 201, target, current)))
	require.Equal(t, current, snapshot.Descriptors[current.RegionID])
	require.NotContains(t, snapshot.PendingPeerChanges, current.RegionID)
	require.Equal(t, uint64(7), snapshot.State.ClusterEpoch)
}
