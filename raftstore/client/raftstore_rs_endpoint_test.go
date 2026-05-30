// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build rust_raftstore

package client

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	coordcatalog "github.com/feichai0017/NoKV/coordinator/catalog"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	coordidalloc "github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	coordtso "github.com/feichai0017/NoKV/coordinator/tso"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metatopology "github.com/feichai0017/NoKV/meta/topology"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	adminclient "github.com/feichai0017/NoKV/raftstore/admin"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
)

func TestRustRaftstoreEndpointClientAtomicMutateGetAndWatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		holt bool
	}{
		{name: "memory"},
		{name: "holt", holt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := startRustRaftstoreEndpoint(t, tc.holt)
			testRustRaftstoreEndpointClientAtomicMutateGetAndWatch(t, addr)
		})
	}
}

func TestRustRaftstoreEndpointHoltApplyStatusSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	addr, stop := startRustRaftstoreProcess(t, dir)

	meta := rustRaftstoreSingleRegion()
	cli, err := New(Config{
		RegionResolver: &mockRegionResolver{region: meta},
		StoreResolver: staticStoreResolver{{
			StoreID: 1,
			Addr:    addr,
			State:   coordpb.StoreState_STORE_STATE_UP,
		}},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:       RetryPolicy{MaxAttempts: 1},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handled, err := cli.TryAtomicMutate(ctx, []byte("agent/restart"), []*kvrpcpb.AtomicPredicate{{
		Key:         []byte("agent/restart"),
		Kind:        kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
		ReadVersion: 9,
	}}, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("agent/restart"),
		Value: []byte("v1"),
	}}, 8, 10)
	require.NoError(t, err)
	require.True(t, handled)
	require.NoError(t, cli.Close())

	admin, closeAdmin, err := adminclient.Dial(ctx, addr)
	require.NoError(t, err)
	statusBefore, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.GreaterOrEqual(t, statusBefore.GetAppliedIndex(), uint64(2))
	require.NoError(t, closeAdmin())
	stop()

	addr, _ = startRustRaftstoreProcess(t, dir)
	admin, closeAdmin, err = adminclient.Dial(ctx, addr)
	require.NoError(t, err)
	status, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.GreaterOrEqual(t, status.GetAppliedIndex(), statusBefore.GetAppliedIndex())
	require.NoError(t, closeAdmin())

	cli, err = New(Config{
		RegionResolver: &mockRegionResolver{region: meta},
		StoreResolver: staticStoreResolver{{
			StoreID: 1,
			Addr:    addr,
			State:   coordpb.StoreState_STORE_STATE_UP,
		}},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:       RetryPolicy{MaxAttempts: 1},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	handled, err = cli.TryAtomicMutate(ctx, []byte("agent/restart2"), []*kvrpcpb.AtomicPredicate{{
		Key:         []byte("agent/restart2"),
		Kind:        kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
		ReadVersion: 9,
	}}, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("agent/restart2"),
		Value: []byte("v2"),
	}}, 11, 12)
	require.NoError(t, err)
	require.True(t, handled)
	got, err := cli.Get(ctx, []byte("agent/restart2"), 12)
	require.NoError(t, err)
	require.False(t, got.GetNotFound())
	require.Equal(t, []byte("v2"), got.GetValue())
}

func TestRustRaftstoreEndpointReportsCoordinatorHeartbeat(t *testing.T) {
	heartbeatCh := make(chan *coordpb.StoreHeartbeatRequest, 16)
	coordAddr, stopCoord := startRustRaftstoreCoordinatorCapture(t, heartbeatCh)
	defer stopCoord()

	storeAddr := reserveLocalAddr(t)
	stopStore := startRustRaftstoreProcessAt(t, storeAddr, "", []string{
		"NOKV_RUST_RAFTSTORE_STORE_ID=11",
		"NOKV_RUST_RAFTSTORE_PEER_ID=101",
		"NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR=" + coordAddr,
		"NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS=50",
	})
	defer stopStore()

	var heartbeat *coordpb.StoreHeartbeatRequest
	require.Eventually(t, func() bool {
		select {
		case heartbeat = <-heartbeatCh:
			return heartbeat.GetStoreId() == 11 &&
				heartbeat.GetRegionNum() == 1 &&
				heartbeat.GetLeaderNum() == 1 &&
				len(heartbeat.GetLeaderRegionIds()) == 1 &&
				heartbeat.GetLeaderRegionIds()[0] == 1
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond)
	require.Equal(t, storeAddr, heartbeat.GetClientAddr())
	require.Equal(t, storeAddr, heartbeat.GetRaftAddr())
	require.Len(t, heartbeat.GetRegionStats(), 1)
	require.Equal(t, uint64(1), heartbeat.GetRegionStats()[0].GetRegionId())
	require.Equal(t, uint64(11), heartbeat.GetRegionStats()[0].GetLeaderStoreId())
}

func TestRustRaftstoreEndpointRoutesThroughCoordinator(t *testing.T) {
	svc := coordserver.NewService(coordcatalog.NewCluster(), coordidalloc.NewIDAllocator(1), coordtso.NewAllocator(1))
	publishRustRaftstoreRootEvent(t, svc, rootevent.StoreJoined(1))
	publishRustRaftstoreRootEvent(t, svc, rootevent.RegionBootstrapped(rustRaftstoreTopologyDescriptor()))
	coordAddr, stopCoord := startRustRaftstoreCoordinatorService(t, svc)
	defer stopCoord()

	storeAddr := reserveLocalAddr(t)
	stopStore := startRustRaftstoreProcessAt(t, storeAddr, "", []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=1",
		"NOKV_RUST_RAFTSTORE_PEER_ID=1",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=true",
		"NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR=" + coordAddr,
		"NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS=50",
	})
	defer stopStore()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		resp, err := svc.GetStore(ctx, &coordpb.GetStoreRequest{StoreId: 1})
		return err == nil &&
			!resp.GetNotFound() &&
			resp.GetStore().GetState() == coordpb.StoreState_STORE_STATE_UP &&
			resp.GetStore().GetClientAddr() == storeAddr
	}, 5*time.Second, 50*time.Millisecond)

	coord, err := coordclient.NewGRPCClient(ctx, coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	cli, err := New(Config{
		RegionResolver: coord,
		StoreResolver:  coord,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:          RetryPolicy{MaxAttempts: 3},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	handled, err := cli.TryAtomicMutate(ctx, []byte("agent/coordinator-route"), []*kvrpcpb.AtomicPredicate{{
		Key:         []byte("agent/coordinator-route"),
		Kind:        kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
		ReadVersion: 9,
	}}, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("agent/coordinator-route"),
		Value: []byte("routed"),
	}}, 8, 10)
	require.NoError(t, err)
	require.True(t, handled)
	got, err := cli.Get(ctx, []byte("agent/coordinator-route"), 10)
	require.NoError(t, err)
	require.False(t, got.GetNotFound())
	require.Equal(t, []byte("routed"), got.GetValue())
}

func TestRustRaftstoreEndpointAdminPublishesCoordinatorDescriptor(t *testing.T) {
	svc := coordserver.NewService(coordcatalog.NewCluster(), coordidalloc.NewIDAllocator(1), coordtso.NewAllocator(1))
	publishRustRaftstoreRootEvent(t, svc, rootevent.StoreJoined(1))
	publishRustRaftstoreRootEvent(t, svc, rootevent.StoreJoined(2))
	publishRustRaftstoreRootEvent(t, svc, rootevent.RegionBootstrapped(rustRaftstoreTopologyDescriptor()))
	coordAddr, stopCoord := startRustRaftstoreCoordinatorService(t, svc)
	defer stopCoord()

	addrs := map[uint64]string{
		1: reserveLocalAddr(t),
		2: reserveLocalAddr(t),
	}
	stop1 := startRustRaftstoreProcessAt(t, addrs[1], "", []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=1",
		"NOKV_RUST_RAFTSTORE_PEER_ID=1",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=true",
		"NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS=2=" + addrs[2],
		"NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR=" + coordAddr,
		"NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS=50",
	})
	defer stop1()
	stop2 := startRustRaftstoreProcessAt(t, addrs[2], "", []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=2",
		"NOKV_RUST_RAFTSTORE_PEER_ID=2",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=false",
		"NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR=" + coordAddr,
		"NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS=50",
	})
	defer stop2()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		resp1, err1 := svc.GetStore(ctx, &coordpb.GetStoreRequest{StoreId: 1})
		resp2, err2 := svc.GetStore(ctx, &coordpb.GetStoreRequest{StoreId: 2})
		return err1 == nil && err2 == nil &&
			resp1.GetStore().GetState() == coordpb.StoreState_STORE_STATE_UP &&
			resp2.GetStore().GetState() == coordpb.StoreState_STORE_STATE_UP
	}, 5*time.Second, 50*time.Millisecond)

	admin, closeAdmin, err := adminclient.Dial(ctx, addrs[1])
	require.NoError(t, err)
	defer func() { require.NoError(t, closeAdmin()) }()
	_, err = admin.AddPeer(ctx, &adminpb.AddPeerRequest{RegionId: 1, StoreId: 2, PeerId: 2})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, err := svc.GetRegionByKey(ctx, &coordpb.GetRegionByKeyRequest{Key: []byte("agent/coordinator-add-peer")})
		if err != nil || resp.GetNotFound() {
			return false
		}
		region := resp.GetRegionDescriptor()
		return region.GetEpoch().GetConfVersion() == 2 &&
			len(region.GetPeers()) == 2 &&
			region.GetPeers()[1].GetStoreId() == 2 &&
			region.GetPeers()[1].GetPeerId() == 2
	}, 5*time.Second, 50*time.Millisecond)
}

func TestRustRaftstoreEndpointRetriesPendingCoordinatorDescriptor(t *testing.T) {
	svc := coordserver.NewService(coordcatalog.NewCluster(), coordidalloc.NewIDAllocator(1), coordtso.NewAllocator(1))
	publishRustRaftstoreRootEvent(t, svc, rootevent.StoreJoined(1))
	publishRustRaftstoreRootEvent(t, svc, rootevent.StoreJoined(2))
	publishRustRaftstoreRootEvent(t, svc, rootevent.RegionBootstrapped(rustRaftstoreTopologyDescriptor()))
	coordAddr := reserveLocalAddr(t)

	addrs := map[uint64]string{
		1: reserveLocalAddr(t),
		2: reserveLocalAddr(t),
	}
	stop1 := startRustRaftstoreProcessAt(t, addrs[1], t.TempDir(), []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=1",
		"NOKV_RUST_RAFTSTORE_PEER_ID=1",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=true",
		"NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS=2=" + addrs[2],
		"NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR=" + coordAddr,
		"NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS=50",
	})
	defer stop1()
	stop2 := startRustRaftstoreProcessAt(t, addrs[2], t.TempDir(), []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=2",
		"NOKV_RUST_RAFTSTORE_PEER_ID=2",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=false",
		"NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR=" + coordAddr,
		"NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS=50",
	})
	defer stop2()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, closeAdmin, err := adminclient.Dial(ctx, addrs[1])
	require.NoError(t, err)
	defer func() { require.NoError(t, closeAdmin()) }()
	_, err = admin.AddPeer(ctx, &adminpb.AddPeerRequest{RegionId: 1, StoreId: 2, PeerId: 2})
	require.NoError(t, err)

	stopCoord := startRustRaftstoreCoordinatorServiceAt(t, coordAddr, svc)
	defer stopCoord()

	require.Eventually(t, func() bool {
		resp, err := svc.GetRegionByKey(ctx, &coordpb.GetRegionByKeyRequest{Key: []byte("agent/pending-coordinator-add-peer")})
		if err != nil || resp.GetNotFound() {
			return false
		}
		region := resp.GetRegionDescriptor()
		return region.GetEpoch().GetConfVersion() == 2 &&
			len(region.GetPeers()) == 2 &&
			region.GetPeers()[1].GetStoreId() == 2 &&
			region.GetPeers()[1].GetPeerId() == 2
	}, 8*time.Second, 50*time.Millisecond)
}

func TestRustRaftstoreEndpointBlocksInvalidCoordinatorDescriptor(t *testing.T) {
	coordAddr, stopCoord := startRustRaftstoreCoordinatorService(t, rejectingRootEventCoordinator{})
	defer stopCoord()

	addrs := map[uint64]string{
		1: reserveLocalAddr(t),
		2: reserveLocalAddr(t),
	}
	dir1 := t.TempDir()
	env1 := []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=1",
		"NOKV_RUST_RAFTSTORE_PEER_ID=1",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=true",
		"NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS=2=" + addrs[2],
		"NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR=" + coordAddr,
		"NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS=50",
	}
	stop1 := startRustRaftstoreProcessAt(t, addrs[1], dir1, env1)
	defer func() {
		if stop1 != nil {
			stop1()
		}
	}()
	stop2 := startRustRaftstoreProcessAt(t, addrs[2], t.TempDir(), []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=2",
		"NOKV_RUST_RAFTSTORE_PEER_ID=2",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=false",
		"NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR=" + coordAddr,
		"NOKV_RUST_RAFTSTORE_COORDINATOR_HEARTBEAT_MS=50",
	})
	defer stop2()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, closeAdmin, err := adminclient.Dial(ctx, addrs[1])
	require.NoError(t, err)
	_, err = admin.AddPeer(ctx, &adminpb.AddPeerRequest{RegionId: 1, StoreId: 2, PeerId: 2})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		execution, statusErr := admin.ExecutionStatus(ctx, &adminpb.ExecutionStatusRequest{})
		if statusErr != nil {
			return false
		}
		restart := execution.GetRestart()
		topology := execution.GetTopology()
		return restart.GetPendingRootEventCount() == 0 &&
			restart.GetBlockedRootEventCount() == 1 &&
			len(topology) == 1 &&
			topology[0].GetPublish() == adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_BLOCKED
	}, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, closeAdmin())

	stop1()
	stop1 = nil
	stop1 = startRustRaftstoreProcessAt(t, addrs[1], dir1, env1)
	admin, closeAdmin, err = adminclient.Dial(ctx, addrs[1])
	require.NoError(t, err)
	defer func() { require.NoError(t, closeAdmin()) }()
	require.Eventually(t, func() bool {
		execution, statusErr := admin.ExecutionStatus(ctx, &adminpb.ExecutionStatusRequest{})
		if statusErr != nil {
			return false
		}
		topology := execution.GetTopology()
		return execution.GetRestart().GetPendingRootEventCount() == 0 &&
			execution.GetRestart().GetBlockedRootEventCount() == 1 &&
			len(topology) == 1 &&
			topology[0].GetRegionId() == 1 &&
			topology[0].GetAction() == "peer change" &&
			topology[0].GetPublish() == adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_BLOCKED &&
			topology[0].GetLastError() != ""
	}, 5*time.Second, 50*time.Millisecond)
}

func TestRustRaftstoreEndpointAdminAddPeerReplicatesAcrossProcesses(t *testing.T) {
	addrs := map[uint64]string{
		1: reserveLocalAddr(t),
		2: reserveLocalAddr(t),
		3: reserveLocalAddr(t),
	}
	startRustRaftstoreProcessAt(t, addrs[1], "", []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=1",
		"NOKV_RUST_RAFTSTORE_PEER_ID=1",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=true",
		"NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS=2=" + addrs[2] + ",3=" + addrs[3],
	})
	startRustRaftstoreProcessAt(t, addrs[2], "", []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=2",
		"NOKV_RUST_RAFTSTORE_PEER_ID=2",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=false",
	})
	startRustRaftstoreProcessAt(t, addrs[3], "", []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=3",
		"NOKV_RUST_RAFTSTORE_PEER_ID=3",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=false",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, closeAdmin, err := adminclient.Dial(ctx, addrs[1])
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeAdmin()) })

	addPeer2, err := admin.AddPeer(ctx, &adminpb.AddPeerRequest{
		RegionId: 1,
		StoreId:  2,
		PeerId:   2,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), addPeer2.GetRegion().GetEpoch().GetConfVersion())
	addPeer3, err := admin.AddPeer(ctx, &adminpb.AddPeerRequest{
		RegionId: 1,
		StoreId:  3,
		PeerId:   3,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), addPeer3.GetRegion().GetEpoch().GetConfVersion())

	meta := rustRaftstoreThreePeerRegion()
	cli, err := New(Config{
		RegionResolver: &mockRegionResolver{region: meta},
		StoreResolver: staticStoreResolver{
			{StoreID: 1, Addr: addrs[1], State: coordpb.StoreState_STORE_STATE_UP},
			{StoreID: 2, Addr: addrs[2], State: coordpb.StoreState_STORE_STATE_UP},
			{StoreID: 3, Addr: addrs[3], State: coordpb.StoreState_STORE_STATE_UP},
		},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:       RetryPolicy{MaxAttempts: 3},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	handled, err := cli.TryAtomicMutate(ctx, []byte("agent/cluster"), []*kvrpcpb.AtomicPredicate{{
		Key:         []byte("agent/cluster"),
		Kind:        kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
		ReadVersion: 90,
	}}, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("agent/cluster"),
		Value: []byte("replicated"),
	}}, 91, 92)
	require.NoError(t, err)
	require.True(t, handled)

	leaderStatus, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.True(t, leaderStatus.GetLeader())
	for peerID := uint64(2); peerID <= 3; peerID++ {
		peerAdmin, closePeerAdmin, err := adminclient.Dial(ctx, addrs[peerID])
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			status, err := peerAdmin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
			return err == nil && status.GetAppliedIndex() >= leaderStatus.GetAppliedIndex()
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(t, closePeerAdmin())
	}

	removePeer3, err := admin.RemovePeer(ctx, &adminpb.RemovePeerRequest{
		RegionId: 1,
		PeerId:   3,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(4), removePeer3.GetRegion().GetEpoch().GetConfVersion())
	require.Equal(t, []*metapb.RegionPeer{
		{StoreId: 1, PeerId: 1},
		{StoreId: 2, PeerId: 2},
	}, removePeer3.GetRegion().GetPeers())
	executionAfterRemove, err := admin.ExecutionStatus(ctx, &adminpb.ExecutionStatusRequest{})
	require.NoError(t, err)
	require.Equal(t, adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_TOPOLOGY, executionAfterRemove.GetLastAdmission().GetClass())
	require.Equal(t, adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_ACCEPTED, executionAfterRemove.GetLastAdmission().GetReason())
	require.Len(t, executionAfterRemove.GetTopology(), 3)
	require.Equal(t, "peer change", executionAfterRemove.GetTopology()[2].GetAction())
	require.Equal(t, adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_APPLIED, executionAfterRemove.GetTopology()[2].GetOutcome())
	require.Equal(t, adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_NOT_REQUIRED, executionAfterRemove.GetTopology()[2].GetPublish())

	metaAfterRemove := rustRaftstoreTwoPeerRegion()
	cliAfterRemove, err := New(Config{
		RegionResolver: &mockRegionResolver{region: metaAfterRemove},
		StoreResolver: staticStoreResolver{
			{StoreID: 1, Addr: addrs[1], State: coordpb.StoreState_STORE_STATE_UP},
			{StoreID: 2, Addr: addrs[2], State: coordpb.StoreState_STORE_STATE_UP},
		},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:       RetryPolicy{MaxAttempts: 3},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cliAfterRemove.Close()) })

	handled, err = cliAfterRemove.TryAtomicMutate(ctx, []byte("agent/after-remove"), []*kvrpcpb.AtomicPredicate{{
		Key:         []byte("agent/after-remove"),
		Kind:        kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
		ReadVersion: 93,
	}}, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("agent/after-remove"),
		Value: []byte("kept-quorum"),
	}}, 94, 95)
	require.NoError(t, err)
	require.True(t, handled)

	leaderStatusAfterRemove, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.True(t, leaderStatusAfterRemove.GetLeader())
	peer2Admin, closePeer2Admin, err := adminclient.Dial(ctx, addrs[2])
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		status, err := peer2Admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
		return err == nil && status.GetAppliedIndex() >= leaderStatusAfterRemove.GetAppliedIndex()
	}, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, closePeer2Admin())
}

func TestRustRaftstoreEndpointHoltMembershipSurvivesRestart(t *testing.T) {
	addrs := map[uint64]string{
		1: reserveLocalAddr(t),
		2: reserveLocalAddr(t),
	}
	dirs := map[uint64]string{
		1: t.TempDir(),
		2: t.TempDir(),
	}
	stop1 := startRustRaftstoreProcessAt(t, addrs[1], dirs[1], []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=1",
		"NOKV_RUST_RAFTSTORE_PEER_ID=1",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=true",
		"NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS=2=" + addrs[2],
	})
	stop2 := startRustRaftstoreProcessAt(t, addrs[2], dirs[2], []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=2",
		"NOKV_RUST_RAFTSTORE_PEER_ID=2",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=false",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, closeAdmin, err := adminclient.Dial(ctx, addrs[1])
	require.NoError(t, err)
	addPeer2, err := admin.AddPeer(ctx, &adminpb.AddPeerRequest{
		RegionId: 1,
		StoreId:  2,
		PeerId:   2,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), addPeer2.GetRegion().GetEpoch().GetConfVersion())

	meta := rustRaftstoreTwoPeerRegionAtConf(2)
	cli, err := newRustRaftstoreTwoPeerClient(addrs, meta)
	require.NoError(t, err)
	handled, err := cli.TryAtomicMutate(ctx, []byte("agent/before-restart"), []*kvrpcpb.AtomicPredicate{{
		Key:         []byte("agent/before-restart"),
		Kind:        kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
		ReadVersion: 100,
	}}, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("agent/before-restart"),
		Value: []byte("v1"),
	}}, 101, 102)
	require.NoError(t, err)
	require.True(t, handled)
	leaderStatus, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	waitForRustRaftstoreApply(t, ctx, addrs[2], leaderStatus.GetAppliedIndex())
	require.NoError(t, cli.Close())
	require.NoError(t, closeAdmin())
	stop2()
	stop1()

	stop2 = startRustRaftstoreProcessAt(t, addrs[2], dirs[2], []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=2",
		"NOKV_RUST_RAFTSTORE_PEER_ID=2",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=false",
	})
	stop1 = startRustRaftstoreProcessAt(t, addrs[1], dirs[1], []string{
		"NOKV_RUST_RAFTSTORE_REGION_ID=1",
		"NOKV_RUST_RAFTSTORE_STORE_ID=1",
		"NOKV_RUST_RAFTSTORE_PEER_ID=1",
		"NOKV_RUST_RAFTSTORE_BOOTSTRAP=true",
		"NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS=2=" + addrs[2],
	})
	_ = stop1
	_ = stop2

	admin, closeAdmin, err = adminclient.Dial(ctx, addrs[1])
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeAdmin()) })
	restartedStatus, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.True(t, restartedStatus.GetKnown())
	require.Equal(t, uint64(2), restartedStatus.GetRegion().GetEpoch().GetConfVersion())
	_, leaderAddr := waitForRustRaftstoreLeader(t, ctx, addrs)

	cli, err = newRustRaftstoreTwoPeerClient(addrs, meta)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	require.Eventually(t, func() bool {
		var mutateErr error
		handled, mutateErr = cli.TryAtomicMutate(ctx, []byte("agent/after-restart"), nil, []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("agent/after-restart"),
			Value: []byte("v2"),
		}}, 104, 105)
		return mutateErr == nil && handled
	}, 5*time.Second, 100*time.Millisecond)
	got, err := cli.Get(ctx, []byte("agent/after-restart"), 105)
	require.NoError(t, err)
	require.False(t, got.GetNotFound())
	require.Equal(t, []byte("v2"), got.GetValue())
	leaderAdmin, closeLeaderAdmin, err := adminclient.Dial(ctx, leaderAddr)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeLeaderAdmin()) }()
	leaderStatus, err = leaderAdmin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	waitForRustRaftstoreApply(t, ctx, addrs[2], leaderStatus.GetAppliedIndex())
}

func TestRustRaftstoreEndpointClientTransactionSurface(t *testing.T) {
	for _, tc := range []struct {
		name string
		holt bool
	}{
		{name: "memory"},
		{name: "holt", holt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := startRustRaftstoreEndpoint(t, tc.holt)
			testRustRaftstoreEndpointClientTransactionSurface(t, addr)
		})
	}
}

func testRustRaftstoreEndpointClientAtomicMutateGetAndWatch(t *testing.T, addr string) {
	t.Helper()
	meta := rustRaftstoreSingleRegion()
	cli, err := New(Config{
		RegionResolver: &mockRegionResolver{region: meta},
		StoreResolver: staticStoreResolver{{
			StoreID: 1,
			Addr:    addr,
			State:   coordpb.StoreState_STORE_STATE_UP,
		}},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:       RetryPolicy{MaxAttempts: 1},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	watch, err := kvrpcpb.NewStoreKVClient(conn).WatchApply(ctx, &kvrpcpb.ApplyWatchRequest{
		KeyPrefix: []byte("agent/"),
		Buffer:    8,
	})
	require.NoError(t, err)

	handled, err := cli.TryAtomicMutate(ctx, []byte("agent/k"), []*kvrpcpb.AtomicPredicate{{
		Key:         []byte("agent/k"),
		Kind:        kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
		ReadVersion: 9,
	}}, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   []byte("agent/k"),
		Value: []byte("v1"),
	}}, 8, 10)
	require.NoError(t, err)
	require.True(t, handled)

	admin, closeAdmin, err := adminclient.Dial(ctx, addr)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeAdmin()) })
	statusAfterWrite, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.GreaterOrEqual(t, statusAfterWrite.GetAppliedIndex(), uint64(2))

	got, err := cli.Get(ctx, []byte("agent/k"), 10)
	require.NoError(t, err)
	require.False(t, got.GetNotFound())
	require.Equal(t, []byte("v1"), got.GetValue())
	statusAfterRead, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.Equal(t, statusAfterWrite.GetAppliedIndex(), statusAfterRead.GetAppliedIndex())

	event, err := watch.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(1), event.GetEvent().GetRegionId())
	require.Equal(t, uint64(10), event.GetEvent().GetCommitVersion())
	require.Equal(t, [][]byte{[]byte("agent/k")}, event.GetEvent().GetKeys())

	handled, err = cli.TryAtomicMutate(ctx, []byte("agent/multi"), nil, []*kvrpcpb.Mutation{
		{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("agent/multi"),
			Value: []byte("v2"),
		},
		{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("other/multi"),
			Value: []byte("ignored"),
		},
	}, 11, 12)
	require.NoError(t, err)
	require.True(t, handled)
	event, err = watch.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(12), event.GetEvent().GetCommitVersion())
	require.Equal(t, [][]byte{[]byte("agent/multi")}, event.GetEvent().GetKeys())

	runtimeStatus, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	require.True(t, runtimeStatus.GetKnown())
	require.True(t, runtimeStatus.GetHosted())
	require.True(t, runtimeStatus.GetLeader())
	require.GreaterOrEqual(t, runtimeStatus.GetAppliedIndex(), uint64(2))

	_, err = admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	execution, err := admin.ExecutionStatus(ctx, &adminpb.ExecutionStatusRequest{})
	require.NoError(t, err)
	lastAdmission := execution.GetLastAdmission()
	require.NotNil(t, lastAdmission)
	require.True(t, lastAdmission.GetObserved())
	require.True(t, lastAdmission.GetAccepted())
	require.Equal(t, adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_WRITE, lastAdmission.GetClass())
	require.Equal(t, adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_ACCEPTED, lastAdmission.GetReason())
	require.Equal(t, uint64(1), lastAdmission.GetRegionId())
	require.Equal(t, uint64(1), lastAdmission.GetPeerId())
	require.Equal(t, adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_READY, execution.GetRestart().GetState())
	require.Equal(t, uint64(1), execution.GetRestart().GetRegionCount())
	require.Equal(t, uint64(1), execution.GetRestart().GetRaftGroupCount())
}

func testRustRaftstoreEndpointClientTransactionSurface(t *testing.T, addr string) {
	t.Helper()
	meta := rustRaftstoreSingleRegion()
	cli, err := New(Config{
		RegionResolver: &mockRegionResolver{region: meta},
		StoreResolver: staticStoreResolver{{
			StoreID: 1,
			Addr:    addr,
			State:   coordpb.StoreState_STORE_STATE_UP,
		}},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:       RetryPolicy{MaxAttempts: 1},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, cli.TwoPhaseCommit(ctx, []byte("agent/txn-a"), []*kvrpcpb.Mutation{
		{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("agent/txn-a"),
			Value: []byte("va"),
		},
		{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("agent/txn-b"),
			Value: []byte("vb"),
		},
	}, 20, 30, 60_000))

	got, err := cli.BatchGet(ctx, [][]byte{
		[]byte("agent/txn-a"),
		[]byte("agent/txn-b"),
		[]byte("agent/txn-missing"),
	}, 30)
	require.NoError(t, err)
	require.Equal(t, []byte("va"), got["agent/txn-a"].GetValue())
	require.Equal(t, []byte("vb"), got["agent/txn-b"].GetValue())
	require.True(t, got["agent/txn-missing"].GetNotFound())

	scanned, err := cli.Scan(ctx, []byte("agent/txn-"), 10, 30)
	require.NoError(t, err)
	require.Len(t, scanned, 2)
	require.Equal(t, []byte("agent/txn-a"), scanned[0].GetKey())
	require.Equal(t, []byte("agent/txn-b"), scanned[1].GetKey())

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = kvrpcpb.NewStoreKVClient(conn).Scan(ctx, &kvrpcpb.KvScanRequest{
		Context: &kvrpcpb.Context{
			RegionId:    meta.GetRegionId(),
			RegionEpoch: meta.GetEpoch(),
			Peer:        meta.GetPeers()[0],
		},
		Request: &kvrpcpb.ScanRequest{
			StartKey: []byte("agent/txn-"),
			Limit:    1,
			Reverse:  true,
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))

	install, err := cli.InstallPreparedMVCCEntries(ctx, []byte("agent/prepared"), &kvrpcpb.InstallPreparedMVCCEntriesRequest{
		CommitVersion: 40,
		Entries: []*kvrpcpb.PreparedMVCCEntry{{
			ColumnFamily: kvrpcpb.PreparedMVCCEntry_DEFAULT,
			Key:          []byte("agent/prepared"),
			Version:      40,
			Value:        []byte("prepared"),
			HasValue:     true,
		}},
		WatchKeys: [][]byte{[]byte("agent/prepared")},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), install.GetAppliedEntries())
	require.Equal(t, uint64(40), install.GetCommitVersion())

	prepared, err := cli.Get(ctx, []byte("agent/prepared"), 40)
	require.NoError(t, err)
	require.False(t, prepared.GetNotFound())
	require.Equal(t, []byte("prepared"), prepared.GetValue())
}

func startRustRaftstoreEndpoint(t *testing.T, holt bool) string {
	t.Helper()
	holtDir := ""
	if holt {
		holtDir = t.TempDir()
	}
	addr, _ := startRustRaftstoreProcess(t, holtDir)
	return addr
}

func startRustRaftstoreProcess(t *testing.T, holtDir string) (string, func()) {
	t.Helper()
	addr := reserveLocalAddr(t)
	return addr, startRustRaftstoreProcessAt(t, addr, holtDir, nil)
}

func startRustRaftstoreProcessAt(t *testing.T, addr, holtDir string, extraEnv []string) func() {
	t.Helper()
	root := findRepoRoot(t)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(
		ctx,
		"cargo",
		"run",
		"--quiet",
		"--manifest-path",
		filepath.Join(root, "raftstore-rs", "Cargo.toml"),
		"-p",
		"nokv-raftstore-server",
	)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "NOKV_RUST_RAFTSTORE_ADDR="+addr)
	cmd.Env = append(cmd.Env, extraEnv...)
	if holtDir != "" {
		cmd.Env = append(cmd.Env, "NOKV_RUST_RAFTSTORE_HOLT_DIR="+holtDir)
	}
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			cancel()
			_ = cmd.Wait()
		})
	}
	t.Cleanup(func() {
		stop()
	})
	go logPipe(t, "raftstore-rs stdout", stdout)
	go logPipe(t, "raftstore-rs stderr", stderr)
	waitForTCP(t, addr, 15*time.Second)
	return stop
}

type rustRaftstoreCoordinatorCapture struct {
	coordpb.UnimplementedCoordinatorServer
	heartbeats chan<- *coordpb.StoreHeartbeatRequest
}

func (s *rustRaftstoreCoordinatorCapture) StoreHeartbeat(_ context.Context, req *coordpb.StoreHeartbeatRequest) (*coordpb.StoreHeartbeatResponse, error) {
	select {
	case s.heartbeats <- proto.Clone(req).(*coordpb.StoreHeartbeatRequest):
	default:
	}
	return &coordpb.StoreHeartbeatResponse{Accepted: true}, nil
}

func startRustRaftstoreCoordinatorCapture(t *testing.T, heartbeats chan<- *coordpb.StoreHeartbeatRequest) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(server, &rustRaftstoreCoordinatorCapture{heartbeats: heartbeats})
	go func() {
		if serveErr := server.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			t.Logf("coordinator capture server error: %v", serveErr)
		}
	}()
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			server.Stop()
			if err := lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				require.NoError(t, err)
			}
		})
	}
	t.Cleanup(stop)
	return lis.Addr().String(), stop
}

func startRustRaftstoreCoordinatorService(t *testing.T, svc coordpb.CoordinatorServer) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	stop := startRustRaftstoreCoordinatorServiceOnListener(t, lis, svc)
	return lis.Addr().String(), stop
}

func startRustRaftstoreCoordinatorServiceAt(t *testing.T, addr string, svc coordpb.CoordinatorServer) func() {
	t.Helper()
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	return startRustRaftstoreCoordinatorServiceOnListener(t, lis, svc)
}

func startRustRaftstoreCoordinatorServiceOnListener(t *testing.T, lis net.Listener, svc coordpb.CoordinatorServer) func() {
	t.Helper()
	server := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(server, svc)
	go func() {
		if serveErr := server.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			t.Logf("coordinator service server error: %v", serveErr)
		}
	}()
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			server.Stop()
			if err := lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				require.NoError(t, err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

type rejectingRootEventCoordinator struct {
	coordpb.UnimplementedCoordinatorServer
}

func (rejectingRootEventCoordinator) PublishRootEvent(context.Context, *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error) {
	return nil, status.Error(codes.InvalidArgument, "reject root event")
}

func publishRustRaftstoreRootEvent(t *testing.T, svc *coordserver.Service, event rootevent.Event) {
	t.Helper()
	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	require.NoError(t, err)
}

func rustRaftstoreTopologyDescriptor() metatopology.Descriptor {
	desc := metatopology.Descriptor{
		RegionID: 1,
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}

func rustRaftstoreSingleRegion() *metapb.RegionDescriptor {
	return &metapb.RegionDescriptor{
		RegionId: 1,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		Peers:    []*metapb.RegionPeer{{StoreId: 1, PeerId: 1}},
	}
}

func rustRaftstoreThreePeerRegion() *metapb.RegionDescriptor {
	return &metapb.RegionDescriptor{
		RegionId: 1,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: 3},
		Peers: []*metapb.RegionPeer{
			{StoreId: 1, PeerId: 1},
			{StoreId: 2, PeerId: 2},
			{StoreId: 3, PeerId: 3},
		},
	}
}

func rustRaftstoreTwoPeerRegion() *metapb.RegionDescriptor {
	return rustRaftstoreTwoPeerRegionAtConf(4)
}

func rustRaftstoreTwoPeerRegionAtConf(confVersion uint64) *metapb.RegionDescriptor {
	return &metapb.RegionDescriptor{
		RegionId: 1,
		Epoch:    &metapb.RegionEpoch{Version: 1, ConfVersion: confVersion},
		Peers: []*metapb.RegionPeer{
			{StoreId: 1, PeerId: 1},
			{StoreId: 2, PeerId: 2},
		},
	}
}

func newRustRaftstoreTwoPeerClient(addrs map[uint64]string, meta *metapb.RegionDescriptor) (*Client, error) {
	return New(Config{
		RegionResolver: &mockRegionResolver{region: meta},
		StoreResolver: staticStoreResolver{
			{StoreID: 1, Addr: addrs[1], State: coordpb.StoreState_STORE_STATE_UP},
			{StoreID: 2, Addr: addrs[2], State: coordpb.StoreState_STORE_STATE_UP},
		},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry:       RetryPolicy{MaxAttempts: 3},
	})
}

func waitForRustRaftstoreApply(t *testing.T, ctx context.Context, addr string, appliedIndex uint64) {
	t.Helper()
	admin, closeAdmin, err := adminclient.Dial(ctx, addr)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeAdmin()) }()
	require.Eventually(t, func() bool {
		status, err := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
		return err == nil && status.GetAppliedIndex() >= appliedIndex
	}, 5*time.Second, 100*time.Millisecond)
}

func waitForRustRaftstoreLeader(t *testing.T, ctx context.Context, addrs map[uint64]string) (uint64, string) {
	t.Helper()
	var leaderPeerID uint64
	var leaderAddr string
	require.Eventually(t, func() bool {
		for peerID, addr := range addrs {
			admin, closeAdmin, err := adminclient.Dial(ctx, addr)
			if err != nil {
				continue
			}
			status, statusErr := admin.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
			_ = closeAdmin()
			if statusErr == nil && status.GetLeader() {
				leaderPeerID = peerID
				leaderAddr = addr
				return true
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond)
	return leaderPeerID, leaderAddr
}

func reserveLocalAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "raftstore-rs", "Cargo.toml")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository root not found")
		}
		dir = parent
	}
}

func waitForTCP(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rust raftstore endpoint %s did not become ready: %v", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func logPipe(t *testing.T, label string, pipe interface{ Read([]byte) (int, error) }) {
	t.Helper()
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		t.Logf("%s: %s", label, scanner.Text())
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Logf("%s read error: %v", label, err)
	}
}
