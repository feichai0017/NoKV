// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package scheduling

import (
	"testing"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/meta/topology"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

func TestPlanStoreOperationsReturnsLeaderTransferForSourceHeartbeat(t *testing.T) {
	ops := PlanStoreOperations(1,
		catalog.ClusterSnapshot{
			Stores: []catalog.StoreStats{
				{StoreID: 1, LeaderNum: 10},
				{StoreID: 2, LeaderNum: 1},
			},
			Regions: []catalog.RegionInfo{
				{Descriptor: testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})},
			},
		},
	)

	require.Len(t, ops, 1)
	require.Equal(t, coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER, ops[0].GetType())
	require.Equal(t, uint64(100), ops[0].GetRegionId())
	require.Equal(t, uint64(101), ops[0].GetSourcePeerId())
	require.Equal(t, uint64(201), ops[0].GetTargetPeerId())
}

func TestPlanStoreOperationsIgnoresNonSourceHeartbeat(t *testing.T) {
	ops := PlanStoreOperations(2,
		catalog.ClusterSnapshot{
			Stores: []catalog.StoreStats{
				{StoreID: 1, LeaderNum: 10},
				{StoreID: 2, LeaderNum: 1},
			},
			Regions: []catalog.RegionInfo{
				{Descriptor: testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})},
			},
		},
	)

	require.Nil(t, ops)
}

func TestPlanStoreOperationsRequiresMeaningfulLeaderSkew(t *testing.T) {
	ops := PlanStoreOperations(1,
		catalog.ClusterSnapshot{
			Stores: []catalog.StoreStats{
				{StoreID: 1, LeaderNum: 2},
				{StoreID: 2, LeaderNum: 1},
			},
			Regions: []catalog.RegionInfo{
				{Descriptor: testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})},
			},
		},
	)

	require.Nil(t, ops)
}

func TestPlanStoreOperationsFallsBackToAnySharedRegion(t *testing.T) {
	ops := PlanStoreOperations(1,
		catalog.ClusterSnapshot{
			Stores: []catalog.StoreStats{
				{StoreID: 1, LeaderNum: 10},
				{StoreID: 2, LeaderNum: 1},
			},
			Regions: []catalog.RegionInfo{
				{Descriptor: testDescriptor(100, []metaregion.Peer{{StoreID: 3, PeerID: 301}, {StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})},
			},
		},
	)

	require.Len(t, ops, 1)
	require.Equal(t, uint64(100), ops[0].GetRegionId())
	require.Equal(t, uint64(101), ops[0].GetSourcePeerId())
	require.Equal(t, uint64(201), ops[0].GetTargetPeerId())
}

func TestPlanStoreOperationsSkipsRegionsWithoutTargetPeer(t *testing.T) {
	ops := PlanStoreOperations(1,
		catalog.ClusterSnapshot{
			Stores: []catalog.StoreStats{
				{StoreID: 1, LeaderNum: 10},
				{StoreID: 2, LeaderNum: 1},
			},
			Regions: []catalog.RegionInfo{
				{Descriptor: testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 3, PeerID: 301}})},
			},
		},
	)

	require.Nil(t, ops)
}

func TestPlanStoreOperationsSplitsHotBoundedRegion(t *testing.T) {
	ids := []uint64{1000, 1001, 1002}
	nextID := func() (uint64, bool) {
		if len(ids) == 0 {
			return 0, false
		}
		id := ids[0]
		ids = ids[1:]
		return id, true
	}
	ops := PlanStoreOperationsWithOptions(1,
		catalog.ClusterSnapshot{
			Stores: []catalog.StoreStats{
				{StoreID: 1, LeaderNum: 1, RegionStats: []catalog.RegionStats{{RegionID: 100, WriteQPS: 100, LeaderStoreID: 1}}},
				{StoreID: 2, LeaderNum: 1},
			},
			Regions: []catalog.RegionInfo{
				{Descriptor: testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})},
			},
		},
		PlanOptions{
			HotRegionWriteQPS: 10,
			NextID:            nextID,
			SplitKey: func(desc topology.Descriptor) ([]byte, bool) {
				return []byte("m"), true
			},
		},
	)

	require.Len(t, ops, 1)
	op := ops[0]
	require.Equal(t, coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_SPLIT_REGION, op.GetType())
	require.Equal(t, uint64(100), op.GetRegionId())
	require.Equal(t, []byte("m"), op.GetSplitKey())
	require.Equal(t, uint64(1000), op.GetSplitChild().GetRegionId())
	require.Equal(t, []byte("m"), op.GetSplitChild().GetStartKey())
	require.Equal(t, []byte("z"), op.GetSplitChild().GetEndKey())
	require.Equal(t, uint64(1001), op.GetSplitChild().GetPeers()[0].GetPeerId())
	require.Equal(t, uint64(1002), op.GetSplitChild().GetPeers()[1].GetPeerId())
}

func TestPlanStoreOperationsSkipsHotSingleBoundaryRegionWithoutSplitKey(t *testing.T) {
	ops := PlanStoreOperationsWithOptions(1,
		catalog.ClusterSnapshot{
			Stores: []catalog.StoreStats{
				{StoreID: 1, LeaderNum: 1, RegionStats: []catalog.RegionStats{{RegionID: 100, WriteQPS: 100, LeaderStoreID: 1}}},
				{StoreID: 2, LeaderNum: 1},
			},
			Regions: []catalog.RegionInfo{
				{Descriptor: topology.Descriptor{
					RegionID: 100,
					StartKey: []byte("a"),
					Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}},
					State:    metaregion.ReplicaStateRunning,
				}},
			},
		},
		PlanOptions{
			HotRegionWriteQPS: 10,
			NextID: func() (uint64, bool) {
				return 1000, true
			},
		},
	)

	require.Nil(t, ops)
}

func TestPlanStoreOperationsRejectsInvalidSplitKey(t *testing.T) {
	ids := []uint64{1000, 1001, 1002}
	nextID := func() (uint64, bool) {
		if len(ids) == 0 {
			return 0, false
		}
		id := ids[0]
		ids = ids[1:]
		return id, true
	}
	ops := PlanStoreOperationsWithOptions(1,
		catalog.ClusterSnapshot{
			Stores: []catalog.StoreStats{
				{StoreID: 1, LeaderNum: 1, RegionStats: []catalog.RegionStats{{RegionID: 100, WriteQPS: 100, LeaderStoreID: 1}}},
				{StoreID: 2, LeaderNum: 1},
			},
			Regions: []catalog.RegionInfo{
				{Descriptor: testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})},
			},
		},
		PlanOptions{
			HotRegionWriteQPS: 10,
			NextID:            nextID,
			SplitKey: func(topology.Descriptor) ([]byte, bool) {
				return []byte("z"), true
			},
		},
	)

	require.Nil(t, ops)
}

func TestPlanStoreOperationsMergesColdAdjacentRegions(t *testing.T) {
	left := testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})
	left.StartKey = []byte("a")
	left.EndKey = []byte("m")
	right := testDescriptor(101, []metaregion.Peer{{StoreID: 1, PeerID: 102}, {StoreID: 2, PeerID: 202}})
	right.StartKey = []byte("m")
	right.EndKey = []byte("z")

	ops := PlanStoreOperationsWithOptions(1,
		catalog.ClusterSnapshot{
			Stores: []catalog.StoreStats{
				{StoreID: 1, LeaderNum: 1, RegionStats: []catalog.RegionStats{
					{RegionID: 100, ReadQPS: 1, WriteQPS: 1, ApproxRegionBytes: 1024, LeaderStoreID: 1},
					{RegionID: 101, ReadQPS: 1, WriteQPS: 1, ApproxRegionBytes: 1024, LeaderStoreID: 1},
				}},
				{StoreID: 2, LeaderNum: 1},
			},
			Regions: []catalog.RegionInfo{{Descriptor: right}, {Descriptor: left}},
		},
		PlanOptions{},
	)

	require.Len(t, ops, 1)
	require.Equal(t, coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_MERGE_REGION, ops[0].GetType())
	require.Equal(t, uint64(100), ops[0].GetRegionId())
	require.Equal(t, uint64(101), ops[0].GetSourceRegionId())
}

func TestPlanStoreOperationsSkipsMergeWithPendingAdmin(t *testing.T) {
	left := testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})
	left.StartKey = []byte("a")
	left.EndKey = []byte("m")
	right := testDescriptor(101, []metaregion.Peer{{StoreID: 1, PeerID: 102}, {StoreID: 2, PeerID: 202}})
	right.StartKey = []byte("m")
	right.EndKey = []byte("z")

	ops := PlanStoreOperationsWithOptions(1,
		catalog.ClusterSnapshot{
			Stores: []catalog.StoreStats{
				{StoreID: 1, LeaderNum: 1, RegionStats: []catalog.RegionStats{
					{RegionID: 100, ApproxRegionBytes: 1024, LeaderStoreID: 1},
					{RegionID: 101, ApproxRegionBytes: 1024, LeaderStoreID: 1, PendingAdmin: true},
				}},
				{StoreID: 2, LeaderNum: 1},
			},
			Regions: []catalog.RegionInfo{{Descriptor: left}, {Descriptor: right}},
		},
		PlanOptions{},
	)

	require.Nil(t, ops)
}

func TestPlannerRequiresConsecutiveHotWindowsAndAppliesCooldown(t *testing.T) {
	ids := []uint64{1000, 1001, 1002}
	nextID := func() (uint64, bool) {
		if len(ids) == 0 {
			return 0, false
		}
		id := ids[0]
		ids = ids[1:]
		return id, true
	}
	planner := NewPlanner(PlanOptions{
		HotRegionWriteQPS: 10,
		HotRegionWindows:  2,
		CooldownTicks:     2,
		NextID:            nextID,
		SplitKey: func(topology.Descriptor) ([]byte, bool) {
			return []byte("m"), true
		},
	})
	snapshot := catalog.ClusterSnapshot{
		Stores: []catalog.StoreStats{
			{StoreID: 1, LeaderNum: 1, RegionStats: []catalog.RegionStats{{RegionID: 100, WriteQPS: 100, LeaderStoreID: 1}}},
			{StoreID: 2, LeaderNum: 1},
		},
		Regions: []catalog.RegionInfo{
			{Descriptor: testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})},
		},
	}

	require.Nil(t, planner.PlanStoreOperationsWithOptions(1, snapshot, PlanOptions{}))
	ops := planner.PlanStoreOperationsWithOptions(1, snapshot, PlanOptions{})
	require.Len(t, ops, 1)
	require.Equal(t, coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_SPLIT_REGION, ops[0].GetType())
	require.Nil(t, planner.PlanStoreOperationsWithOptions(1, snapshot, PlanOptions{}))
}

func TestPlannerUsesConfiguredBoundarySplitKey(t *testing.T) {
	ids := []uint64{1000, 1001, 1002}
	nextID := func() (uint64, bool) {
		if len(ids) == 0 {
			return 0, false
		}
		id := ids[0]
		ids = ids[1:]
		return id, true
	}
	planner := NewPlanner(PlanOptions{
		HotRegionWriteQPS: 10,
		HotRegionWindows:  1,
		CooldownTicks:     1,
		NextID:            nextID,
		SplitKey:          SplitKeyFromBoundaries([][]byte{[]byte("b"), []byte("m"), []byte("x")}),
	})
	ops := planner.PlanStoreOperationsWithOptions(1,
		catalog.ClusterSnapshot{
			Stores: []catalog.StoreStats{
				{StoreID: 1, LeaderNum: 1, RegionStats: []catalog.RegionStats{{RegionID: 100, WriteQPS: 100, LeaderStoreID: 1}}},
				{StoreID: 2, LeaderNum: 1},
			},
			Regions: []catalog.RegionInfo{
				{Descriptor: testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})},
			},
		},
		PlanOptions{},
	)

	require.Len(t, ops, 1)
	require.Equal(t, []byte("m"), ops[0].GetSplitKey())
}

func TestPlannerRequiresConsecutiveColdWindows(t *testing.T) {
	planner := NewPlanner(PlanOptions{
		ColdRegionReadQPS:  10,
		ColdRegionWriteQPS: 10,
		ColdRegionWindows:  2,
		CooldownTicks:      1,
	})
	left := testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})
	left.StartKey = []byte("a")
	left.EndKey = []byte("m")
	right := testDescriptor(101, []metaregion.Peer{{StoreID: 1, PeerID: 102}, {StoreID: 2, PeerID: 202}})
	right.StartKey = []byte("m")
	right.EndKey = []byte("z")
	snapshot := catalog.ClusterSnapshot{
		Stores: []catalog.StoreStats{
			{StoreID: 1, LeaderNum: 1, RegionStats: []catalog.RegionStats{
				{RegionID: 100, ReadQPS: 1, WriteQPS: 1, ApproxRegionBytes: 1024, LeaderStoreID: 1},
				{RegionID: 101, ReadQPS: 1, WriteQPS: 1, ApproxRegionBytes: 1024, LeaderStoreID: 1},
			}},
			{StoreID: 2, LeaderNum: 1},
		},
		Regions: []catalog.RegionInfo{{Descriptor: left}, {Descriptor: right}},
	}

	require.Nil(t, planner.PlanStoreOperationsWithOptions(1, snapshot, PlanOptions{}))
	ops := planner.PlanStoreOperationsWithOptions(1, snapshot, PlanOptions{})
	require.Len(t, ops, 1)
	require.Equal(t, coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_MERGE_REGION, ops[0].GetType())
}

func testDescriptor(regionID uint64, peers []metaregion.Peer) topology.Descriptor {
	desc := topology.Descriptor{
		RegionID: regionID,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    append([]metaregion.Peer(nil), peers...),
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}
