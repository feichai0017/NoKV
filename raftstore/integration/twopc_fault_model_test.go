// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/client"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type twopcFaultAction uint8

const (
	twopcFaultCommit twopcFaultAction = iota
	twopcFaultTransferChildLeader
	twopcFaultPartialChildQuorum
)

type twopcFaultRuntime struct {
	seed   *testcluster.Node
	target *testcluster.Node
	client *client.Client
}

type liveTwoStoreResolver struct {
	seed   *testcluster.Node
	target *testcluster.Node
}

func (r liveTwoStoreResolver) GetStore(_ context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error) {
	var node *testcluster.Node
	switch req.GetStoreId() {
	case r.seed.StoreID:
		node = r.seed
	case r.target.StoreID:
		node = r.target
	default:
		return &coordpb.GetStoreResponse{NotFound: true}, nil
	}
	return &coordpb.GetStoreResponse{Store: &coordpb.StoreInfo{
		StoreId:    node.StoreID,
		ClientAddr: node.Addr(),
		State:      coordpb.StoreState_STORE_STATE_UP,
	}}, nil
}

func TestTwoPhaseCommitFaultScheduleAcrossSplitCluster(t *testing.T) {
	seeds := raftstoreFaultEnvInt("NOKV_RAFTSTORE_FAULT_SEEDS", 1)
	steps := raftstoreFaultEnvInt("NOKV_RAFTSTORE_FAULT_STEPS", 6)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()

			rt := openTwoStoreSplitRuntime(t, ctx)
			model := make(map[string]string)
			startTs := uint64(1000 + seed*1000)
			for step, action := range generate2PCFaultSchedule(seed, steps) {
				switch action {
				case twopcFaultCommit:
					startTs = runSplit2PCCommit(t, ctx, rt.client, model, startTs, step)
				case twopcFaultTransferChildLeader:
					transferRegionLeader(t, ctx, 502, 102, 202, rt.seed, rt.target)
				case twopcFaultPartialChildQuorum:
					startTs = runPartialChildQuorumRollback(t, ctx, rt, model, startTs, step)
				}
				assertCommitted2PCModel(t, ctx, rt.client, model, startTs+10)
			}
		})
	}
}

func openTwoStoreSplitRuntime(t *testing.T, ctx context.Context) *twopcFaultRuntime {
	t.Helper()
	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	require.NoError(t, func() error {
		_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 501, PeerID: 101})
		return err
	}())

	seed := testcluster.StartNode(t, 1, seedDir, []workdirmode.Mode{workdirmode.ModeSeeded, workdirmode.ModeCluster}, true)
	target := testcluster.StartNode(t, 2, t.TempDir(), nil, false)
	t.Cleanup(func() {
		seed.Close(t)
		target.Close(t)
	})
	wireTwoStorePeers(seed, target)
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 501, 101)

	_, err := migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     501,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets: []migrate.PeerTarget{
			{StoreID: 2, PeerID: 201, TargetAdminAddr: target.Addr()},
		},
	})
	require.NoError(t, err)

	parentLeader, _ := testcluster.FindLeader(t, ctx, 501, seed, target)
	childMeta := localmeta.RegionMeta{
		ID:       502,
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
	require.NoError(t, parentLeader.Server.Store().ProposeSplit(501, childMeta, childMeta.StartKey))
	require.Eventually(t, func() bool {
		a, errA := testcluster.TryPollRuntimeStatus(ctx, seed.Addr(), 502)
		b, errB := testcluster.TryPollRuntimeStatus(ctx, target.Addr(), 502)
		if errA != nil || errB != nil {
			return false
		}
		return a.GetKnown() && a.GetHosted() && b.GetKnown() && b.GetHosted()
	}, 5*time.Second, 20*time.Millisecond, testcluster.DumpStatus(t, ctx, 502, seed, target))

	parentStatus := testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 501)
	childSeedStatus := testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 502)
	parentLeaderNode, _ := testcluster.FindLeader(t, ctx, 501, seed, target)
	childLeaderNode, _ := testcluster.FindLeader(t, ctx, 502, seed, target)
	cli, err := client.New(client.Config{
		StoreResolver: liveTwoStoreResolver{seed: seed, target: target},
		RegionResolver: &staticResolver{regions: []*metapb.RegionDescriptor{
			regionMetaWithLeaderFirst(parentStatus.GetRegion(), parentLeaderNode.StoreID),
			regionMetaWithLeaderFirst(childSeedStatus.GetRegion(), childLeaderNode.StoreID),
		}},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: client.RetryPolicy{
			MaxAttempts:                 8,
			RouteUnavailableBackoff:     10 * time.Millisecond,
			TransportUnavailableBackoff: 10 * time.Millisecond,
			RegionErrorBackoff:          10 * time.Millisecond,
			LockResolveBackoff:          10 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return &twopcFaultRuntime{seed: seed, target: target, client: cli}
}

func generate2PCFaultSchedule(seed int64, steps int) []twopcFaultAction {
	if steps < 3 {
		steps = 3
	}
	rng := rand.New(rand.NewSource(seed))
	actions := []twopcFaultAction{twopcFaultCommit, twopcFaultTransferChildLeader, twopcFaultPartialChildQuorum}
	for len(actions) < steps {
		switch rng.Intn(100) {
		case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9:
			actions = append(actions, twopcFaultPartialChildQuorum)
		case 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24:
			actions = append(actions, twopcFaultTransferChildLeader)
		default:
			actions = append(actions, twopcFaultCommit)
		}
	}
	return actions[:steps]
}

func runSplit2PCCommit(t *testing.T, ctx context.Context, cli *client.Client, model map[string]string, startTs uint64, step int) uint64 {
	t.Helper()
	primaryKey := fmt.Sprintf("alpha-%03d", step)
	childKey := fmt.Sprintf("tango-%03d", step)
	primaryValue := fmt.Sprintf("parent-value-%03d", step)
	childValue := fmt.Sprintf("child-value-%03d", step)
	require.NoError(t, cli.TwoPhaseCommit(ctx, []byte(primaryKey), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte(primaryKey), Value: []byte(primaryValue)},
		{Op: kvrpcpb.Mutation_Put, Key: []byte(childKey), Value: []byte(childValue)},
	}, startTs, startTs+1, 3000))
	model[primaryKey] = primaryValue
	model[childKey] = childValue
	return startTs + 10
}

func runPartialChildQuorumRollback(t *testing.T, ctx context.Context, rt *twopcFaultRuntime, model map[string]string, startTs uint64, step int) uint64 {
	t.Helper()
	primaryKey := fmt.Sprintf("bravo-failed-%03d", step)
	childKey := fmt.Sprintf("zulu-failed-%03d", step)
	blockChildPeerLink(rt.seed, rt.target)
	failCtx, cancel := context.WithTimeout(ctx, 350*time.Millisecond)
	err := rt.client.TwoPhaseCommit(failCtx, []byte(primaryKey), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte(primaryKey), Value: []byte("should-not-commit-parent")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte(childKey), Value: []byte("should-not-commit-child")},
	}, startTs, startTs+1, 3000)
	cancel()
	require.Error(t, err)

	unblockChildPeerLink(rt.seed, rt.target)
	_, err = rt.client.ResolveLocks(ctx, startTs, 0, [][]byte{[]byte(primaryKey), []byte(childKey)})
	require.NoError(t, err)
	assertKeyNotCommitted(t, ctx, rt.client, primaryKey, startTs+100)
	assertKeyNotCommitted(t, ctx, rt.client, childKey, startTs+100)
	require.NotContains(t, model, primaryKey)
	require.NotContains(t, model, childKey)
	return startTs + 10
}

func transferRegionLeader(t *testing.T, ctx context.Context, regionID, seedPeerID, targetPeerID uint64, seed, target *testcluster.Node) {
	t.Helper()
	leader, status := testcluster.FindLeader(t, ctx, regionID, seed, target)
	targetNode := target
	targetPeer := targetPeerID
	if leader.StoreID == target.StoreID {
		targetNode = seed
		targetPeer = seedPeerID
	}
	_, err := migrate.TransferLeader(ctx, migrate.TransferLeaderConfig{
		Addr:            leader.Addr(),
		TargetAdminAddr: targetNode.Addr(),
		RegionID:        regionID,
		PeerID:          targetPeer,
		WaitTimeout:     3 * time.Second,
		PollInterval:    20 * time.Millisecond,
	})
	require.NoError(t, err, "transfer leader for region=%d from peer=%d to peer=%d", regionID, status.GetLeaderPeerId(), targetPeer)
}

func assertCommitted2PCModel(t *testing.T, ctx context.Context, cli *client.Client, model map[string]string, readTs uint64) {
	t.Helper()
	for key, want := range model {
		resp, err := cli.Get(ctx, []byte(key), readTs)
		require.NoError(t, err, "key=%s", key)
		require.Equal(t, want, string(resp.GetValue()), "key=%s", key)
	}
}

func assertKeyNotCommitted(t *testing.T, ctx context.Context, cli *client.Client, key string, readTs uint64) {
	t.Helper()
	resp, err := cli.Get(ctx, []byte(key), readTs)
	require.NoError(t, err, "key=%s", key)
	require.True(t, resp.GetNotFound(), "key=%s unexpectedly visible as %q", key, resp.GetValue())
}

func wireTwoStorePeers(seed, target *testcluster.Node) {
	seed.WirePeers(map[uint64]string{201: target.Addr(), 202: target.Addr()})
	target.WirePeers(map[uint64]string{101: seed.Addr(), 102: seed.Addr()})
}

func blockChildPeerLink(seed, target *testcluster.Node) {
	seed.BlockPeer(202)
	target.BlockPeer(102)
}

func unblockChildPeerLink(seed, target *testcluster.Node) {
	seed.UnblockPeer(202)
	target.UnblockPeer(102)
}

func raftstoreFaultEnvInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
