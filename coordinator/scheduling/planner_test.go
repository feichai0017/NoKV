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

func TestPlannerAppliesLeaderTransferCooldown(t *testing.T) {
	planner := NewPlanner(PlanOptions{CooldownTicks: 2})
	snapshot := catalog.ClusterSnapshot{
		Stores: []catalog.StoreStats{
			{StoreID: 1, LeaderNum: 10},
			{StoreID: 2, LeaderNum: 1},
		},
		Regions: []catalog.RegionInfo{
			{Descriptor: testDescriptor(100, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})},
		},
	}

	ops := planner.PlanStoreOperationsWithOptions(1, snapshot, PlanOptions{})
	require.Len(t, ops, 1)
	require.Equal(t, coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER, ops[0].GetType())
	require.Nil(t, planner.PlanStoreOperationsWithOptions(1, snapshot, PlanOptions{}))
	require.Nil(t, planner.PlanStoreOperationsWithOptions(1, snapshot, PlanOptions{}))
	ops = planner.PlanStoreOperationsWithOptions(1, snapshot, PlanOptions{})
	require.Len(t, ops, 1)
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
