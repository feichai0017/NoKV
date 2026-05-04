package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/client"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type staticResolver struct {
	regions []*metapb.RegionDescriptor
}

type testStoreEndpoint struct {
	StoreID uint64
	Addr    string
}

type staticStoreResolver []testStoreEndpoint

func (r staticStoreResolver) GetStore(_ context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error) {
	for _, endpoint := range r {
		if endpoint.StoreID == req.GetStoreId() {
			return &coordpb.GetStoreResponse{Store: &coordpb.StoreInfo{
				StoreId:    endpoint.StoreID,
				ClientAddr: endpoint.Addr,
				State:      coordpb.StoreState_STORE_STATE_UP,
			}}, nil
		}
	}
	return &coordpb.GetStoreResponse{NotFound: true}, nil
}

func (r *staticResolver) GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	for _, region := range r.regions {
		if region != nil && containsRegionKey(region, req.GetKey()) {
			return &coordpb.GetRegionByKeyResponse{
				RegionDescriptor: metawire.DescriptorToProto(metawire.DescriptorFromProto(region)),
			}, nil
		}
	}
	return &coordpb.GetRegionByKeyResponse{NotFound: true}, nil
}

func (r *staticResolver) Close() error { return nil }

func cloneRegionMeta(meta *metapb.RegionDescriptor) *metapb.RegionDescriptor {
	if meta == nil {
		return nil
	}
	out := &metapb.RegionDescriptor{
		RegionId: meta.GetRegionId(),
		StartKey: append([]byte(nil), meta.GetStartKey()...),
		EndKey:   append([]byte(nil), meta.GetEndKey()...),
		Epoch:    &metapb.RegionEpoch{Version: meta.GetEpoch().GetVersion(), ConfVersion: meta.GetEpoch().GetConfVersion()},
	}
	out.Peers = make([]*metapb.RegionPeer, 0, len(meta.GetPeers()))
	for _, peer := range meta.GetPeers() {
		if peer == nil {
			continue
		}
		out.Peers = append(out.Peers, &metapb.RegionPeer{
			StoreId: peer.GetStoreId(),
			PeerId:  peer.GetPeerId(),
		})
	}
	return out
}

func regionMetaWithLeaderFirst(meta *metapb.RegionDescriptor, leaderStoreID uint64) *metapb.RegionDescriptor {
	out := cloneRegionMeta(meta)
	if out == nil || leaderStoreID == 0 || len(out.Peers) < 2 {
		return out
	}
	for i, peer := range out.Peers {
		if peer.GetStoreId() != leaderStoreID {
			continue
		}
		out.Peers[0], out.Peers[i] = out.Peers[i], out.Peers[0]
		break
	}
	return out
}

func containsRegionKey(meta *metapb.RegionDescriptor, key []byte) bool {
	if meta == nil {
		return false
	}
	if len(meta.GetStartKey()) > 0 && string(key) < string(meta.GetStartKey()) {
		return false
	}
	if len(meta.GetEndKey()) > 0 && string(key) >= string(meta.GetEndKey()) {
		return false
	}
	return true
}

func runtimeLeaderDescriptor(a, b *adminpb.RegionRuntimeStatusResponse) (*metapb.RegionDescriptor, uint64, bool) {
	if a != nil && a.GetKnown() && a.GetLeader() && a.GetRegion() != nil {
		for _, peer := range a.GetRegion().GetPeers() {
			if peer != nil && peer.GetPeerId() == a.GetLeaderPeerId() {
				return a.GetRegion(), peer.GetStoreId(), true
			}
		}
	}
	if b != nil && b.GetKnown() && b.GetLeader() && b.GetRegion() != nil {
		for _, peer := range b.GetRegion().GetPeers() {
			if peer != nil && peer.GetPeerId() == b.GetLeaderPeerId() {
				return b.GetRegion(), peer.GetStoreId(), true
			}
		}
	}
	return nil, 0, false
}

func TestClientReadWriteHonorContextUnderQuorumLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	key := []byte("ctx-read-key")
	value := []byte("ctx-read-value")
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 1, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []workdirmode.Mode{workdirmode.ModeSeeded, workdirmode.ModeCluster}, true)
	target2 := testcluster.StartNode(t, 2, t.TempDir(), nil, false)
	target3 := testcluster.StartNode(t, 3, t.TempDir(), nil, false)
	defer seed.Close(t)
	defer target2.Close(t)
	defer target3.Close(t)

	seed.WirePeers(map[uint64]string{201: target2.Addr(), 301: target3.Addr()})
	target2.WirePeers(map[uint64]string{101: seed.Addr(), 301: target3.Addr()})
	target3.WirePeers(map[uint64]string{101: seed.Addr(), 201: target2.Addr()})
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 1, 101)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     1,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets: []migrate.PeerTarget{
			{StoreID: 2, PeerID: 201, TargetAdminAddr: target2.Addr()},
			{StoreID: 3, PeerID: 301, TargetAdminAddr: target3.Addr()},
		},
	})
	require.NoError(t, err)
	testcluster.WaitForHostedPeer(t, ctx, target2.Addr(), 1, 201)
	testcluster.WaitForHostedPeer(t, ctx, target3.Addr(), 1, 301)

	leaderNode, leaderStatus := testcluster.FindLeader(t, ctx, 1, seed, target2, target3)
	cli, err := client.New(client.Config{
		StoreResolver: staticStoreResolver{
			{StoreID: 1, Addr: seed.Addr()},
			{StoreID: 2, Addr: target2.Addr()},
			{StoreID: 3, Addr: target3.Addr()},
		},
		RegionResolver: &staticResolver{regions: []*metapb.RegionDescriptor{
			regionMetaWithLeaderFirst(leaderStatus.GetRegion(), leaderNode.StoreID),
		}},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: client.RetryPolicy{
			MaxAttempts:                 128,
			RouteUnavailableBackoff:     5 * time.Millisecond,
			TransportUnavailableBackoff: 5 * time.Millisecond,
			RegionErrorBackoff:          5 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	require.NoError(t, cli.Put(ctx, key, value, 1, 2, 3000))
	getResp, err := cli.Get(ctx, key, 3)
	require.NoError(t, err)
	require.Equal(t, value, getResp.GetValue())

	seed.BlockPeer(201)
	seed.BlockPeer(301)
	target2.BlockPeer(101)
	target2.BlockPeer(301)
	target3.BlockPeer(101)
	target3.BlockPeer(201)
	defer func() {
		seed.UnblockPeer(201)
		seed.UnblockPeer(301)
		target2.UnblockPeer(101)
		target2.UnblockPeer(301)
		target3.UnblockPeer(101)
		target3.UnblockPeer(201)
	}()

	readCtx, readCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer readCancel()
	_, err = cli.Get(readCtx, key, 3)
	require.Error(t, err)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))

	writeCtx, writeCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer writeCancel()
	err = cli.Put(writeCtx, []byte("ctx-write-key"), []byte("ctx-write-value"), 10, 11, 3000)
	require.Error(t, err)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))

	seed.UnblockPeer(201)
	seed.UnblockPeer(301)
	target2.UnblockPeer(101)
	target2.UnblockPeer(301)
	target3.UnblockPeer(101)
	target3.UnblockPeer(201)

	require.Eventually(t, func() bool {
		testCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := cli.Get(testCtx, key, 4)
		return err == nil && string(resp.GetValue()) == string(value)
	}, 5*time.Second, 50*time.Millisecond)
}

func TestClientTwoPhaseCommitHonorsContextAcrossSplitRegionsUnderPartialQuorumLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 91, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []workdirmode.Mode{workdirmode.ModeSeeded, workdirmode.ModeCluster}, true)
	target := testcluster.StartNode(t, 2, t.TempDir(), nil, false)
	defer seed.Close(t)
	defer target.Close(t)

	wireAll := func() {
		seed.WirePeers(map[uint64]string{201: target.Addr(), 202: target.Addr()})
		target.WirePeers(map[uint64]string{101: seed.Addr(), 102: seed.Addr()})
	}
	wireAll()
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 91, 101)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     91,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets: []migrate.PeerTarget{
			{StoreID: 2, PeerID: 201, TargetAdminAddr: target.Addr()},
		},
	})
	require.NoError(t, err)

	parentLeader, _ := testcluster.FindLeader(t, ctx, 91, seed, target)
	childMeta := localmeta.RegionMeta{
		ID:       92,
		StartKey: []byte("m"),
		EndKey:   nil,
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 102},
			{StoreID: 2, PeerID: 202},
		},
	}
	require.NoError(t, parentLeader.Server.Store().ProposeSplit(91, childMeta, childMeta.StartKey))
	require.Eventually(t, func() bool {
		a, errA := testcluster.TryPollRuntimeStatus(ctx, seed.Addr(), 92)
		b, errB := testcluster.TryPollRuntimeStatus(ctx, target.Addr(), 92)
		if errA != nil || errB != nil {
			return false
		}
		return a.GetKnown() && a.GetHosted() && b.GetKnown() && b.GetHosted()
	}, 5*time.Second, 20*time.Millisecond, testcluster.DumpStatus(t, ctx, 92, seed, target))

	parentStatus := testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 91)
	childSeedStatus := testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 92)
	childTargetStatus := testcluster.FetchRuntimeStatus(t, ctx, target.Addr(), 92)
	parentLeaderNode, _ := testcluster.FindLeader(t, ctx, 91, seed, target)
	childLeaderNode, _ := testcluster.FindLeader(t, ctx, 92, seed, target)

	cli, err := client.New(client.Config{
		StoreResolver: staticStoreResolver{
			{StoreID: 1, Addr: seed.Addr()},
			{StoreID: 2, Addr: target.Addr()},
		},
		RegionResolver: &staticResolver{regions: []*metapb.RegionDescriptor{
			regionMetaWithLeaderFirst(parentStatus.GetRegion(), parentLeaderNode.StoreID),
			regionMetaWithLeaderFirst(childSeedStatus.GetRegion(), childLeaderNode.StoreID),
		}},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		// Keep retry budget above txnCtx's timeout so this case validates
		// deadline propagation instead of retry-budget exhaustion.
		Retry: client.RetryPolicy{
			MaxAttempts:                 128,
			RouteUnavailableBackoff:     5 * time.Millisecond,
			TransportUnavailableBackoff: 5 * time.Millisecond,
			RegionErrorBackoff:          5 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	seed.BlockPeer(childTargetStatus.GetLocalPeerId())
	target.BlockPeer(childSeedStatus.GetLocalPeerId())
	defer func() {
		seed.UnblockPeer(childTargetStatus.GetLocalPeerId())
		target.UnblockPeer(childSeedStatus.GetLocalPeerId())
	}()

	txnCtx, txnCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer txnCancel()
	err = cli.TwoPhaseCommit(txnCtx, []byte("bravo"), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("bravo"), Value: []byte("v1")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("tango"), Value: []byte("v2")},
	}, 100, 101, 3000)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded, "expected deadline propagation, got %v", err)

	seed.UnblockPeer(childTargetStatus.GetLocalPeerId())
	target.UnblockPeer(childSeedStatus.GetLocalPeerId())

	var lastRecoveryErr error
	require.Eventuallyf(t, func() bool {
		parentA, errParentA := testcluster.TryPollRuntimeStatus(ctx, seed.Addr(), 91)
		parentB, errParentB := testcluster.TryPollRuntimeStatus(ctx, target.Addr(), 91)
		childA, errChildA := testcluster.TryPollRuntimeStatus(ctx, seed.Addr(), 92)
		childB, errChildB := testcluster.TryPollRuntimeStatus(ctx, target.Addr(), 92)
		if errParentA != nil || errParentB != nil || errChildA != nil || errChildB != nil {
			lastRecoveryErr = errors.Join(errParentA, errParentB, errChildA, errChildB)
			return false
		}
		parentMeta, parentLeaderStoreID, parentReady := runtimeLeaderDescriptor(parentA, parentB)
		childMeta, childLeaderStoreID, childReady := runtimeLeaderDescriptor(childA, childB)
		if !parentReady || !childReady {
			lastRecoveryErr = errors.New("region leaders not ready")
			return false
		}
		recoveryCli, err := client.New(client.Config{
			StoreResolver: staticStoreResolver{
				{StoreID: 1, Addr: seed.Addr()},
				{StoreID: 2, Addr: target.Addr()},
			},
			RegionResolver: &staticResolver{regions: []*metapb.RegionDescriptor{
				regionMetaWithLeaderFirst(parentMeta, parentLeaderStoreID),
				regionMetaWithLeaderFirst(childMeta, childLeaderStoreID),
			}},
			DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
			Retry: client.RetryPolicy{
				MaxAttempts:                 5,
				RouteUnavailableBackoff:     10 * time.Millisecond,
				TransportUnavailableBackoff: 10 * time.Millisecond,
				RegionErrorBackoff:          10 * time.Millisecond,
			},
		})
		if err != nil {
			lastRecoveryErr = err
			return false
		}
		defer func() { _ = recoveryCli.Close() }()

		testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = recoveryCli.TwoPhaseCommit(testCtx, []byte("charlie"), []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: []byte("charlie"), Value: []byte("ok1")},
			{Op: kvrpcpb.Mutation_Put, Key: []byte("yankee"), Value: []byte("ok2")},
		}, 200, 201, 3000)
		lastRecoveryErr = err
		return err == nil
	}, 45*time.Second, 100*time.Millisecond, "cluster did not recover after healing partial quorum loss: %v", lastRecoveryErr)
}
