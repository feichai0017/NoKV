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

func TestApplyEventToStateAdvancesEpochsAndCursor(t *testing.T) {
	var st rootstate.State

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, rootevent.StoreJoined(1))
	require.Equal(t, uint64(1), st.MembershipEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, st.LastCommitted)

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.RegionDescriptorPublished(testDescriptor(10, []byte("a"), []byte("z"))))
	require.Equal(t, uint64(1), st.ClusterEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, st.LastCommitted)

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 3}, rootevent.PeerAdditionPlanned(10, 2, 201, testDescriptor(10, []byte("a"), []byte("z"))))
	require.Equal(t, uint64(2), st.ClusterEpoch)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 3}, st.LastCommitted)
}

func TestCursorHelpers(t *testing.T) {
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, rootstate.NextCursor(rootstate.Cursor{}))
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 8}, rootstate.NextCursor(rootstate.Cursor{Term: 2, Index: 7}))
	require.True(t, rootstate.CursorAfter(rootstate.Cursor{Term: 1, Index: 2}, rootstate.Cursor{Term: 1, Index: 1}))
	require.True(t, rootstate.CursorAfter(rootstate.Cursor{Term: 2, Index: 1}, rootstate.Cursor{Term: 1, Index: 99}))
	require.False(t, rootstate.CursorAfter(rootstate.Cursor{Term: 1, Index: 1}, rootstate.Cursor{Term: 1, Index: 1}))
}

func TestCloneDescriptorsDetachesMapAndValues(t *testing.T) {
	in := map[uint64]topology.Descriptor{
		7: testDescriptor(7, []byte("m"), []byte("z")),
	}
	out := rootstate.CloneDescriptors(in)
	require.Equal(t, in[7].RegionID, out[7].RegionID)

	in[7].StartKey[0] = 'x'
	require.Equal(t, byte('m'), out[7].StartKey[0])
}

func TestCloneSnapshotDetachesAuthorityMapsAndPendingChanges(t *testing.T) {
	base := testDescriptor(7, []byte("a"), []byte("m"))
	target := testDescriptor(8, []byte("a"), []byte("m"))
	snapshot := rootstate.Snapshot{
		State: rootstate.State{ClusterEpoch: 3},
		Mounts: map[string]rootstate.MountRecord{
			"vol": {MountID: "vol", MountKeyID: 1, RootInode: 1, State: rootstate.MountStateActive},
		},
		Subtrees: map[string]rootstate.SubtreeAuthority{
			"vol/1": {SubtreeID: "vol/1", Mount: "vol", RootInode: 1, State: rootstate.SubtreeAuthorityActive},
		},
		Quotas: map[string]rootstate.QuotaFence{
			"vol/0": {SubjectID: "vol/0", Mount: "vol", Era: 1},
		},
		Descriptors: map[uint64]topology.Descriptor{7: base},
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
			7: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 20, Base: base, Target: target},
		},
		PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
			7: {Kind: rootstate.PendingRangeChangeSplit, ParentRegionID: 7, BaseParent: base, Left: target},
		},
	}

	cloned := rootstate.CloneSnapshot(snapshot)
	peerChange := snapshot.PendingPeerChanges[7]
	peerChange.Target.StartKey[0] = 'x'
	snapshot.PendingPeerChanges[7] = peerChange
	rangeChange := snapshot.PendingRangeChanges[7]
	rangeChange.Left.StartKey[0] = 'x'
	snapshot.PendingRangeChanges[7] = rangeChange
	snapshot.Mounts["vol"] = rootstate.MountRecord{MountID: "vol", State: rootstate.MountStateRetired}
	snapshot.Subtrees["vol/1"] = rootstate.SubtreeAuthority{SubtreeID: "mutated"}
	snapshot.Quotas["vol/0"] = rootstate.QuotaFence{SubjectID: "mutated"}
	snapshot.PendingPeerChanges[7] = rootstate.PendingPeerChange{Base: testDescriptor(9, []byte("x"), []byte("z"))}
	snapshot.PendingRangeChanges[7] = rootstate.PendingRangeChange{BaseParent: testDescriptor(10, []byte("x"), []byte("z"))}

	require.Equal(t, rootstate.MountStateActive, cloned.Mounts["vol"].State)
	require.Equal(t, "vol/1", cloned.Subtrees["vol/1"].SubtreeID)
	require.Equal(t, uint64(1), cloned.Quotas["vol/0"].Era)
	require.Equal(t, uint64(7), cloned.PendingPeerChanges[7].Base.RegionID)
	require.Equal(t, uint64(7), cloned.PendingRangeChanges[7].BaseParent.RegionID)
	require.Equal(t, byte('a'), cloned.PendingPeerChanges[7].Target.StartKey[0])
	require.Equal(t, byte('a'), cloned.PendingRangeChanges[7].Left.StartKey[0])
}

func TestCloneEmptyMapsAndDescriptorRevision(t *testing.T) {
	require.Empty(t, rootstate.CloneMounts(nil))
	require.Nil(t, rootstate.CloneSubtreeAuthorities(nil))
	require.Nil(t, rootstate.CloneQuotaFences(nil))
	require.Empty(t, rootstate.ClonePendingPeerChanges(nil))
	require.Empty(t, rootstate.ClonePendingRangeChanges(nil))
	require.Zero(t, rootstate.MaxDescriptorRevision(nil))

	low := testDescriptor(1, []byte("a"), []byte("m"))
	low.RootEpoch = 3
	high := testDescriptor(2, []byte("m"), []byte("z"))
	high.RootEpoch = 9
	require.Equal(t, uint64(9), rootstate.MaxDescriptorRevision(map[uint64]topology.Descriptor{
		low.RegionID:  low,
		high.RegionID: high,
	}))
}

func TestApplyStoreMembershipEventsToSnapshot(t *testing.T) {
	var snapshot rootstate.Snapshot
	joinCursor := rootstate.Cursor{Term: 1, Index: 1}
	retireCursor := rootstate.Cursor{Term: 1, Index: 2}

	rootstate.ApplyEventToSnapshot(&snapshot, joinCursor, rootevent.StoreJoined(7))

	require.Equal(t, uint64(1), snapshot.State.MembershipEpoch)
	require.Equal(t, joinCursor, snapshot.State.LastCommitted)
	require.Equal(t, rootstate.StoreMembership{
		StoreID:  7,
		State:    rootstate.StoreMembershipActive,
		JoinedAt: joinCursor,
	}, snapshot.Stores[7])

	rootstate.ApplyEventToSnapshot(&snapshot, retireCursor, rootevent.StoreRetired(7))

	require.Equal(t, uint64(2), snapshot.State.MembershipEpoch)
	require.Equal(t, retireCursor, snapshot.State.LastCommitted)
	require.Equal(t, rootstate.StoreMembership{
		StoreID:   7,
		State:     rootstate.StoreMembershipRetired,
		JoinedAt:  joinCursor,
		RetiredAt: retireCursor,
	}, snapshot.Stores[7])
}

func TestApplySnapshotEpochPublishedToSnapshot(t *testing.T) {
	var snapshot rootstate.Snapshot
	cursor := rootstate.Cursor{Term: 2, Index: 7}
	event := rootevent.SnapshotEpochPublished("vol", 1, 42, 100)

	rootstate.ApplyEventToSnapshot(&snapshot, cursor, event)

	id := rootevent.SnapshotEpochID("vol", 42, 100)
	require.Equal(t, cursor, snapshot.State.LastCommitted)
	require.Equal(t, rootstate.SnapshotEpoch{
		SnapshotID:  id,
		Mount:       "vol",
		MountKeyID:  1,
		RootInode:   42,
		ReadVersion: 100,
		PublishedAt: cursor,
	}, snapshot.SnapshotEpochs[id])

	retireCursor := rootstate.Cursor{Term: 2, Index: 8}
	rootstate.ApplyEventToSnapshot(&snapshot, retireCursor, rootevent.SnapshotEpochRetired("vol", 1, 42, 100))
	require.Equal(t, retireCursor, snapshot.State.LastCommitted)
	require.NotContains(t, snapshot.SnapshotEpochs, id)
}

func TestCloneStoreMembershipsDetachesMap(t *testing.T) {
	in := map[uint64]rootstate.StoreMembership{
		7: {
			StoreID:  7,
			State:    rootstate.StoreMembershipActive,
			JoinedAt: rootstate.Cursor{Term: 1, Index: 1},
		},
	}

	out := rootstate.CloneStoreMemberships(in)
	in[7] = rootstate.StoreMembership{StoreID: 7, State: rootstate.StoreMembershipRetired}

	require.Equal(t, rootstate.StoreMembershipActive, out[7].State)
}

func TestCloneSnapshotEpochsDetachesMap(t *testing.T) {
	in := map[string]rootstate.SnapshotEpoch{
		"vol/7/9": {
			SnapshotID:  "vol/7/9",
			Mount:       "vol",
			RootInode:   7,
			ReadVersion: 9,
		},
	}

	out := rootstate.CloneSnapshotEpochs(in)
	in["vol/7/9"] = rootstate.SnapshotEpoch{SnapshotID: "mutated"}

	require.Equal(t, uint64(9), out["vol/7/9"].ReadVersion)
}

func TestSnapshotRetentionFloor(t *testing.T) {
	floor, ok := (rootstate.Snapshot{}).SnapshotRetentionFloor()
	require.False(t, ok)
	require.Zero(t, floor)

	snapshot := rootstate.Snapshot{
		SnapshotEpochs: map[string]rootstate.SnapshotEpoch{
			"newer": {Mount: "vol", ReadVersion: 90},
			"old":   {Mount: "data", MountKeyID: 2, ReadVersion: 30},
			"zero":  {Mount: "vol", ReadVersion: 0},
			"vol":   {Mount: "vol", MountKeyID: 1, ReadVersion: 90},
		},
	}
	floor, ok = snapshot.SnapshotRetentionFloor()
	require.True(t, ok)
	require.Equal(t, uint64(30), floor)

	index := snapshot.SnapshotRetentionIndex()
	require.True(t, index.Active())
	require.Equal(t, uint64(30), index.GlobalFloor)
	require.Equal(t, map[uint64]uint64{
		1: 90,
		2: 30,
	}, index.MountFloors)
}

func TestSnapshotRetentionIndexTracksMountFloors(t *testing.T) {
	snapshot := rootstate.Snapshot{
		SnapshotEpochs: map[string]rootstate.SnapshotEpoch{
			"vol/root/90":  {Mount: "vol", MountKeyID: 1, RootInode: 1, ReadVersion: 90},
			"vol/child/30": {Mount: "vol", MountKeyID: 1, RootInode: 7, ReadVersion: 30},
			"data/root/70": {Mount: "data", MountKeyID: 2, RootInode: 1, ReadVersion: 70},
			"data/zero":    {Mount: "data", MountKeyID: 2, RootInode: 9, ReadVersion: 0},
		},
	}

	index := snapshot.SnapshotRetentionIndex()
	require.True(t, index.Active())
	require.Equal(t, uint64(30), index.GlobalFloor)
	require.Equal(t, map[uint64]uint64{
		1: 30,
		2: 70,
	}, index.MountFloors)
	floor, ok := index.FloorForMount(1)
	require.True(t, ok)
	require.Equal(t, uint64(30), floor)
	floor, ok = index.FloorForMount(999)
	require.False(t, ok)
	require.Zero(t, floor)
}

func testDescriptor(id uint64, start, end []byte) topology.Descriptor {
	desc := topology.Descriptor{
		RegionID:  id,
		StartKey:  append([]byte(nil), start...),
		EndKey:    append([]byte(nil), end...),
		Epoch:     metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:     []metaregion.Peer{{StoreID: 1, PeerID: id*10 + 1}},
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return desc
}
