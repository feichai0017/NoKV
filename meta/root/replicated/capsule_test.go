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

func TestReplicatedStoreApplyCapsuleAuthorityAcquireRetire(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	store := stores[leaderID]
	cmd := testCapsuleAcquireCommand("holder-a", 1)

	state, grant, err := store.ApplyCapsuleAuthority(context.Background(), cmd)
	require.NoError(t, err)
	require.Equal(t, uint64(1), grant.EpochID)
	require.Equal(t, grant, state.ActiveCapsuleGrants[0])
	require.Equal(t, uint64(1), state.CapsuleAuthorityEpoch)

	state, held, err := store.ApplyCapsuleAuthority(context.Background(), rootproto.CapsuleAuthorityCommand{
		Kind:            rootproto.CapsuleAuthorityActAcquire,
		HolderID:        "holder-a",
		GrantID:         grant.GrantID,
		Scope:           cmd.Scope,
		ExpiresUnixNano: cmd.ExpiresUnixNano,
		NowUnixNano:     cmd.NowUnixNano,
	})
	require.NoError(t, err)
	require.Equal(t, grant, held)
	require.Len(t, state.ActiveCapsuleGrants, 1)

	state, _, err = store.ApplyCapsuleAuthority(context.Background(), rootproto.CapsuleAuthorityCommand{
		Kind:     rootproto.CapsuleAuthorityActRetire,
		HolderID: grant.HolderID,
		GrantID:  grant.GrantID,
	})
	require.NoError(t, err)
	require.Empty(t, state.ActiveCapsuleGrants)
	require.Equal(t, uint64(1), state.CapsuleAuthorityEpoch)
}

func TestReplicatedStoreApplyCapsuleAuthorityRejectsConflicts(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	store := stores[leaderID]

	_, grant, err := store.ApplyCapsuleAuthority(context.Background(), testCapsuleAcquireCommand("holder-a", 1))
	require.NoError(t, err)
	_, _, err = store.ApplyCapsuleAuthority(context.Background(), testCapsuleAcquireCommand("holder-b", 1))
	require.True(t, errors.Is(err, rootstate.ErrPrimacy))

	_, _, err = store.ApplyCapsuleAuthority(context.Background(), rootproto.CapsuleAuthorityCommand{
		Kind:     rootproto.CapsuleAuthorityActRetire,
		HolderID: "holder-b",
		GrantID:  grant.GrantID,
	})
	require.True(t, errors.Is(err, rootstate.ErrPrimacy))
}

func TestReplicatedStoreApplyCapsuleAuthorityReplacesExpiredGrant(t *testing.T) {
	stores, _, leaderID := openNetworkTestCluster(t, 4)
	store := stores[leaderID]

	expired := testCapsuleAcquireCommand("holder-a", 1)
	expired.ExpiresUnixNano = 100
	expired.NowUnixNano = 10
	_, first, err := store.ApplyCapsuleAuthority(context.Background(), expired)
	require.NoError(t, err)

	next := testCapsuleAcquireCommand("holder-b", 1)
	next.NowUnixNano = 101
	state, second, err := store.ApplyCapsuleAuthority(context.Background(), next)
	require.NoError(t, err)
	require.NotEqual(t, first.GrantID, second.GrantID)
	require.Equal(t, uint64(2), second.EpochID)
	require.Equal(t, uint64(2), state.CapsuleAuthorityEpoch)
	require.Len(t, state.ActiveCapsuleGrants, 1)
	require.Equal(t, second.GrantID, state.ActiveCapsuleGrants[0].GrantID)
}

func BenchmarkApplyCapsuleAuthorityConflictScan(b *testing.B) {
	stores, _, leaderID := openNetworkTestCluster(b, 4)
	store := stores[leaderID]
	for bucket := range 16 {
		_, _, err := store.ApplyCapsuleAuthority(context.Background(), testCapsuleAcquireCommand(fmt.Sprintf("holder-%d", bucket), uint16(bucket)))
		require.NoError(b, err)
	}
	cmd := testCapsuleAcquireCommand("contender", 11)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _, err := store.ApplyCapsuleAuthority(context.Background(), cmd)
		if !errors.Is(err, rootstate.ErrPrimacy) {
			b.Fatalf("expected primacy conflict, got %v", err)
		}
	}
}

func testCapsuleAcquireCommand(holderID string, bucket uint16) rootproto.CapsuleAuthorityCommand {
	return rootproto.CapsuleAuthorityCommand{
		Kind:     rootproto.CapsuleAuthorityActAcquire,
		HolderID: holderID,
		Scope: rootproto.CapsuleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{bucket},
			Parents:    []uint64{10},
		},
		ExpiresUnixNano: 1_000,
		NowUnixNano:     100,
	}
}
