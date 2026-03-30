package integration

import (
	"context"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type staticResolver struct {
	region *pb.RegionMeta
}

func (r *staticResolver) GetRegionByKey(ctx context.Context, req *pb.GetRegionByKeyRequest) (*pb.GetRegionByKeyResponse, error) {
	return &pb.GetRegionByKeyResponse{Region: cloneRegionMeta(r.region)}, nil
}

func (r *staticResolver) Close() error { return nil }

func cloneRegionMeta(meta *pb.RegionMeta) *pb.RegionMeta {
	if meta == nil {
		return nil
	}
	out := &pb.RegionMeta{
		Id:               meta.GetId(),
		StartKey:         append([]byte(nil), meta.GetStartKey()...),
		EndKey:           append([]byte(nil), meta.GetEndKey()...),
		EpochVersion:     meta.GetEpochVersion(),
		EpochConfVersion: meta.GetEpochConfVersion(),
	}
	out.Peers = make([]*pb.RegionPeer, 0, len(meta.GetPeers()))
	for _, peer := range meta.GetPeers() {
		if peer == nil {
			continue
		}
		out.Peers = append(out.Peers, &pb.RegionPeer{
			StoreId: peer.GetStoreId(),
			PeerId:  peer.GetPeerId(),
		})
	}
	return out
}

func TestClientReadWriteHonorContextUnderQuorumLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, func(opt *NoKV.Options) { opt.ValueThreshold = 16 })
	key := []byte("ctx-read-key")
	value := []byte("ctx-read-value")
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 1, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
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
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 1, 101)

	leaderStatus := testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 1)
	cli, err := client.New(client.Config{
		Stores: []client.StoreEndpoint{
			{StoreID: 1, Addr: seed.Addr()},
			{StoreID: 2, Addr: target2.Addr()},
			{StoreID: 3, Addr: target3.Addr()},
		},
		RegionResolver: &staticResolver{region: leaderStatus.GetRegion()},
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: client.RetryPolicy{
			MaxAttempts:                 1,
			RouteUnavailableBackoff:     0,
			TransportUnavailableBackoff: 0,
			RegionErrorBackoff:          0,
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
