// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/meta/topology"
	"github.com/stretchr/testify/require"
)

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
	rootstate.ApplyEventToSnapshot(&snapshot, cursor, rootevent.MountRegistered("vol", 42, 1, 1))

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
	require.Equal(t, uint64(42), snapshot.State.IDFence)
}

func TestApplyMountRetiredToSnapshot(t *testing.T) {
	snapshot := rootstate.Snapshot{}
	registeredAt := rootstate.Cursor{Term: 1, Index: 1}
	retiredAt := rootstate.Cursor{Term: 1, Index: 2}
	rootstate.ApplyEventToSnapshot(&snapshot, registeredAt, rootevent.MountRegistered("vol", 1, 1, 1))
	rootstate.ApplyEventToSnapshot(&snapshot, retiredAt, rootevent.MountRetired("vol"))

	require.Equal(t, rootstate.MountRecord{
		MountID:       "vol",
		MountKeyID:    1,
		RootInode:     1,
		SchemaVersion: 1,
		State:         rootstate.MountStateRetired,
		RegisteredAt:  registeredAt,
		RetiredAt:     retiredAt,
	}, snapshot.Mounts["vol"])

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 3}, rootevent.MountRetired("missing"))
	require.Equal(t, rootstate.MountRecord{
		MountID:   "missing",
		State:     rootstate.MountStateRetired,
		RetiredAt: rootstate.Cursor{Term: 1, Index: 3},
	}, snapshot.Mounts["missing"])
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

func TestApplyPeerChangeToSnapshot(t *testing.T) {
	current := testDescriptor(11, []byte("a"), []byte("m"))
	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch++
	target.EnsureHash()

	snapshot := rootstate.Snapshot{
		State:       rootstate.State{ClusterEpoch: 5},
		Descriptors: map[uint64]topology.Descriptor{current.RegionID: current},
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
		Descriptors: map[uint64]topology.Descriptor{current.RegionID: current},
	}
	require.True(t, rootstate.ApplyPeerChangeToSnapshot(&snapshot, rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)))
	require.True(t, rootstate.ApplyPeerChangeToSnapshot(&snapshot, rootevent.PeerAdditionCancelled(target.RegionID, 2, 201, target, current)))
	require.Equal(t, current, snapshot.Descriptors[current.RegionID])
	require.NotContains(t, snapshot.PendingPeerChanges, current.RegionID)
	require.Equal(t, uint64(7), snapshot.State.ClusterEpoch)
}
