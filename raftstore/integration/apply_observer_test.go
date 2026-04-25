package integration

import (
	"context"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type captureApplyObserver struct {
	ch chan storepkg.ApplyEvent
}

func (o *captureApplyObserver) OnApply(evt storepkg.ApplyEvent) {
	o.ch <- evt
}

func TestApplyObserverReceivesCommittedKeysFromRealRaftApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, func(opt *NoKV.Options) {
		opt.ValueThreshold = 16
	})
	require.NoError(t, standalone.Close())
	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 121, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	defer seed.Close(t)
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 121, 101)

	observer := &captureApplyObserver{ch: make(chan storepkg.ApplyEvent, 4)}
	reg, err := seed.Server.Store().RegisterApplyObserver(observer, 4)
	require.NoError(t, err)
	defer reg.Close()

	status := testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 121)
	cli, err := client.New(client.Config{
		StoreResolver:  staticStoreResolver{{StoreID: 1, Addr: seed.Addr()}},
		RegionResolver: &staticResolver{regions: []*metapb.RegionDescriptor{status.GetRegion()}},
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

	key := []byte("observer-key")
	require.NoError(t, cli.TwoPhaseCommit(ctx, key, []*kvrpcpb.Mutation{{
		Op:    kvrpcpb.Mutation_Put,
		Key:   key,
		Value: []byte("observer-value"),
	}}, 10, 20, 3000))

	select {
	case evt := <-observer.ch:
		require.Equal(t, uint64(121), evt.RegionID)
		require.NotZero(t, evt.Term)
		require.NotZero(t, evt.Index)
		require.Equal(t, storepkg.ApplyEventSourceCommit, evt.Source)
		require.Equal(t, uint64(20), evt.CommitVersion)
		require.Equal(t, [][]byte{key}, evt.Keys)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for apply observer event")
	}
}
