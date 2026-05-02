package integration

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type simulationAction uint8

const (
	simulationCommit simulationAction = iota
	simulationTransferChildLeader
	simulationPartialQuorumRollback
	simulationDelayedChildLink
	simulationRestartTargetStore
	simulationResolveOldLocks
)

func TestDeterministicFaultSimulationAcrossSplitCluster(t *testing.T) {
	seeds := raftstoreFaultEnvInt("NOKV_SIMULATION_SEEDS", 1)
	steps := raftstoreFaultEnvInt("NOKV_SIMULATION_STEPS", 8)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			rt := openTwoStoreSplitRuntime(t, ctx)
			model := make(map[string]string)
			startTs := uint64(10_000 + seed*1_000)
			for step, action := range generateSimulationSchedule(seed, steps) {
				switch action {
				case simulationCommit:
					startTs = runSplit2PCCommit(t, ctx, rt.client, model, startTs, step)
				case simulationTransferChildLeader:
					transferRegionLeader(t, ctx, 502, 102, 202, rt.seed, rt.target)
				case simulationPartialQuorumRollback:
					startTs = runPartialChildQuorumRollback(t, ctx, rt, model, startTs, step)
				case simulationDelayedChildLink:
					startTs = runDelayedChildLinkCommit(t, ctx, rt, model, startTs, step)
				case simulationRestartTargetStore:
					restartTargetStore(t, ctx, rt)
				case simulationResolveOldLocks:
					_, err := rt.client.ResolveLocks(ctx, startTs-500, 0, [][]byte{
						fmt.Appendf(nil, "missing-primary-%03d", step),
						fmt.Appendf(nil, "missing-child-%03d", step),
					})
					require.NoError(t, err)
				}
				assertCommitted2PCModel(t, ctx, rt.client, model, startTs+100)
			}
		})
	}
}

func generateSimulationSchedule(seed int64, steps int) []simulationAction {
	if steps < 6 {
		steps = 6
	}
	actions := []simulationAction{
		simulationCommit,
		simulationDelayedChildLink,
		simulationTransferChildLeader,
		simulationPartialQuorumRollback,
		simulationRestartTargetStore,
		simulationResolveOldLocks,
	}
	rng := rand.New(rand.NewSource(seed))
	for len(actions) < steps {
		switch rng.Intn(100) {
		case 0, 1, 2, 3, 4, 5, 6, 7:
			actions = append(actions, simulationPartialQuorumRollback)
		case 8, 9, 10, 11, 12, 13, 14, 15:
			actions = append(actions, simulationRestartTargetStore)
		case 16, 17, 18, 19, 20, 21, 22, 23:
			actions = append(actions, simulationDelayedChildLink)
		case 24, 25, 26, 27, 28, 29:
			actions = append(actions, simulationTransferChildLeader)
		case 30, 31, 32, 33, 34:
			actions = append(actions, simulationResolveOldLocks)
		default:
			actions = append(actions, simulationCommit)
		}
	}
	return actions[:steps]
}

func runDelayedChildLinkCommit(t *testing.T, ctx context.Context, rt *twopcFaultRuntime, model map[string]string, startTs uint64, step int) uint64 {
	t.Helper()
	primaryKey := fmt.Sprintf("delay-alpha-%03d", step)
	childKey := fmt.Sprintf("delay-tango-%03d", step)
	primaryValue := fmt.Sprintf("delay-parent-value-%03d", step)
	childValue := fmt.Sprintf("delay-child-value-%03d", step)

	blockChildPeerLink(rt.seed, rt.target)
	unblocked := make(chan struct{})
	go func() {
		defer close(unblocked)
		time.Sleep(120 * time.Millisecond)
		unblockChildPeerLink(rt.seed, rt.target)
	}()
	defer func() {
		<-unblocked
		unblockChildPeerLink(rt.seed, rt.target)
	}()

	commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, rt.client.TwoPhaseCommit(commitCtx, []byte(primaryKey), []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte(primaryKey), Value: []byte(primaryValue)},
		{Op: kvrpcpb.Mutation_Put, Key: []byte(childKey), Value: []byte(childValue)},
	}, startTs, startTs+1, 3000))
	model[primaryKey] = primaryValue
	model[childKey] = childValue
	return startTs + 10
}

func restartTargetStore(t *testing.T, ctx context.Context, rt *twopcFaultRuntime) {
	t.Helper()
	rt.target.Restart(t, nil, true)
	wireTwoStorePeers(rt.seed, rt.target)
	testcluster.WaitForHostedPeer(t, ctx, rt.target.Addr(), 501, 201)
	testcluster.WaitForHostedPeer(t, ctx, rt.target.Addr(), 502, 202)
	testcluster.FindLeader(t, ctx, 501, rt.seed, rt.target)
	testcluster.FindLeader(t, ctx, 502, rt.seed, rt.target)
	reopenFaultClient(t, ctx, rt)
}

func reopenFaultClient(t *testing.T, ctx context.Context, rt *twopcFaultRuntime) {
	t.Helper()
	_ = rt.client.Close()

	parentStatus := testcluster.FetchRuntimeStatus(t, ctx, rt.seed.Addr(), 501)
	childSeedStatus := testcluster.FetchRuntimeStatus(t, ctx, rt.seed.Addr(), 502)
	parentLeaderNode, _ := testcluster.FindLeader(t, ctx, 501, rt.seed, rt.target)
	childLeaderNode, _ := testcluster.FindLeader(t, ctx, 502, rt.seed, rt.target)
	cli, err := client.New(client.Config{
		StoreResolver: liveTwoStoreResolver{seed: rt.seed, target: rt.target},
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
	rt.client = cli
	t.Cleanup(func() { _ = cli.Close() })
}
