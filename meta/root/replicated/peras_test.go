package replicated

import (
	"context"
	"errors"
	"fmt"
	"testing"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestReplicatedStoreApplyPerasAuthorityAcquireRetire(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	store := stores[leaderID]
	cmd := testPerasAcquireCommand("holder-a", 1)

	state, grant, err := store.ApplyPerasAuthority(context.Background(), cmd)
	require.NoError(t, err)
	require.Equal(t, uint64(1), grant.EpochID)
	require.Equal(t, grant, state.ActivePerasGrants[0])
	require.Equal(t, uint64(1), state.PerasAuthorityEpoch)

	state, held, err := store.ApplyPerasAuthority(context.Background(), rootproto.PerasAuthorityCommand{
		Kind:            rootproto.PerasAuthorityActAcquire,
		HolderID:        "holder-a",
		GrantID:         grant.GrantID,
		Scope:           cmd.Scope,
		ExpiresUnixNano: cmd.ExpiresUnixNano,
		NowUnixNano:     cmd.NowUnixNano,
	})
	require.NoError(t, err)
	require.Equal(t, grant, held)
	require.Len(t, state.ActivePerasGrants, 1)

	state, _, err = store.ApplyPerasAuthority(context.Background(), rootproto.PerasAuthorityCommand{
		Kind:     rootproto.PerasAuthorityActRetire,
		HolderID: grant.HolderID,
		GrantID:  grant.GrantID,
	})
	require.NoError(t, err)
	require.Empty(t, state.ActivePerasGrants)
	require.Equal(t, uint64(1), state.PerasAuthorityEpoch)
}

func TestReplicatedStoreApplyPerasAuthoritySealPublishesFrontier(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	store := stores[leaderID]
	cmd := testPerasAcquireCommand("holder-a", 1)

	state, grant, err := store.ApplyPerasAuthority(context.Background(), cmd)
	require.NoError(t, err)
	require.Empty(t, state.PerasAuthoritySeals)

	var root [32]byte
	var digest [32]byte
	root[0] = 9
	digest[0] = 8
	state, sealedGrant, err := store.ApplyPerasAuthority(context.Background(), rootproto.PerasAuthorityCommand{
		Kind:                 rootproto.PerasAuthorityActSeal,
		HolderID:             grant.HolderID,
		GrantID:              grant.GrantID,
		NowUnixNano:          200,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		OperationCount:       32,
		EntryCount:           64,
	})
	require.NoError(t, err)
	require.Equal(t, grant, sealedGrant)
	require.Len(t, state.PerasAuthoritySeals, 1)
	require.Equal(t, root, state.PerasAuthoritySeals[0].SegmentRoot)

	expired := testPerasAcquireCommand("holder-b", 1)
	expired.NowUnixNano = cmd.ExpiresUnixNano + 1
	expired.ExpiresUnixNano = expired.NowUnixNano + 1_000
	state, successor, err := store.ApplyPerasAuthority(context.Background(), expired)
	require.NoError(t, err)
	require.Equal(t, uint64(2), successor.EpochID)
	require.Equal(t, root, successor.PredecessorDigest)
	require.Equal(t, state.ActivePerasGrants[0].PredecessorDigest, successor.PredecessorDigest)
}

func TestReplicatedStoreApplyPerasAuthorityRejectsConflicts(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	store := stores[leaderID]

	_, grant, err := store.ApplyPerasAuthority(context.Background(), testPerasAcquireCommand("holder-a", 1))
	require.NoError(t, err)
	_, _, err = store.ApplyPerasAuthority(context.Background(), testPerasAcquireCommand("holder-b", 1))
	require.True(t, errors.Is(err, rootstate.ErrPrimacy))

	_, _, err = store.ApplyPerasAuthority(context.Background(), rootproto.PerasAuthorityCommand{
		Kind:     rootproto.PerasAuthorityActRetire,
		HolderID: "holder-b",
		GrantID:  grant.GrantID,
	})
	require.True(t, errors.Is(err, rootstate.ErrPrimacy))
}

func TestReplicatedStoreApplyPerasAuthorityReplacesExpiredGrant(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	store := stores[leaderID]

	expired := testPerasAcquireCommand("holder-a", 1)
	expired.ExpiresUnixNano = 100
	expired.NowUnixNano = 10
	_, first, err := store.ApplyPerasAuthority(context.Background(), expired)
	require.NoError(t, err)

	next := testPerasAcquireCommand("holder-b", 1)
	next.NowUnixNano = 101
	state, second, err := store.ApplyPerasAuthority(context.Background(), next)
	require.NoError(t, err)
	require.NotEqual(t, first.GrantID, second.GrantID)
	require.Equal(t, uint64(2), second.EpochID)
	require.Equal(t, uint64(2), state.PerasAuthorityEpoch)
	require.Len(t, state.ActivePerasGrants, 1)
	require.Equal(t, second.GrantID, state.ActivePerasGrants[0].GrantID)
}

func BenchmarkApplyPerasAuthorityConflictScan(b *testing.B) {
	stores, _, leaderID := openNetworkTestCluster(b, 4)
	store := stores[leaderID]
	for bucket := range 16 {
		_, _, err := store.ApplyPerasAuthority(context.Background(), testPerasAcquireCommand(fmt.Sprintf("holder-%d", bucket), uint16(bucket)))
		require.NoError(b, err)
	}
	cmd := testPerasAcquireCommand("contender", 11)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _, err := store.ApplyPerasAuthority(context.Background(), cmd)
		if !errors.Is(err, rootstate.ErrPrimacy) {
			b.Fatalf("expected primacy conflict, got %v", err)
		}
	}
}

func testPerasAcquireCommand(holderID string, bucket uint16) rootproto.PerasAuthorityCommand {
	return rootproto.PerasAuthorityCommand{
		Kind:     rootproto.PerasAuthorityActAcquire,
		HolderID: holderID,
		Scope: rootproto.PerasAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{bucket},
			Parents:    []uint64{10},
		},
		ExpiresUnixNano: 1_000,
		NowUnixNano:     100,
	}
}
