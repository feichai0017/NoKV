package state_test

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestApplyPerasAuthorityGrantLifecycleToState(t *testing.T) {
	var st rootstate.State
	cursor := rootstate.Cursor{Term: 1, Index: 1}
	grant := testStatePerasGrant("peras-1", 1)

	rootstate.ApplyEventToState(&st, cursor, rootevent.PerasAuthorityGranted(grant))
	found, ok := st.ActivePerasGrantByID(grant.GrantID)
	require.True(t, ok)
	require.Equal(t, grant, found)
	require.Equal(t, cursor, st.LastCommitted)
	require.Equal(t, grant.EpochID, st.PerasAuthorityEpoch)

	covered, ok := st.ActivePerasGrantFor(rootproto.PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1},
		Parents:    []uint64{10},
	}, 100)
	require.True(t, ok)
	require.Equal(t, grant.GrantID, covered.GrantID)

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.PerasAuthorityRetired(grant))
	_, ok = st.ActivePerasGrantByID(grant.GrantID)
	require.False(t, ok)
	require.Empty(t, st.ActivePerasGrants)
	require.Equal(t, grant.EpochID, st.PerasAuthorityEpoch)
}

func TestApplyPerasAuthoritySealTracksLatestFrontier(t *testing.T) {
	var st rootstate.State
	grant := testStatePerasGrant("peras-1", 1)
	first := testStatePerasSeal(grant, 1)
	second := testStatePerasSeal(grant, 2)

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, rootevent.PerasAuthoritySealed(first))
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.PerasAuthoritySealed(second))

	require.Len(t, st.PerasAuthoritySeals, 1)
	require.Equal(t, second.SegmentRoot, st.PerasAuthoritySeals[0].SegmentRoot)
	found, ok := st.LatestPerasAuthoritySealFor(rootproto.PerasAuthorityScope{MountID: "vol", MountKeyID: 7})
	require.True(t, ok)
	require.Equal(t, second.SegmentRoot, found.SegmentRoot)

	clone := rootstate.CloneState(st)
	clone.PerasAuthoritySeals[0].Scope.Buckets[0] = 9
	require.Equal(t, []uint16{1}, st.PerasAuthoritySeals[0].Scope.Buckets)
}

func TestApplyPerasAuthorityRejectsInvalidAndConflictingGrants(t *testing.T) {
	var st rootstate.State
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, rootevent.PerasAuthorityGranted(rootproto.PerasAuthorityGrant{
		GrantID: "missing-fields",
	}))
	require.Empty(t, st.ActivePerasGrants)

	left := testStatePerasGrant("peras-1", 1)
	right := testStatePerasGrant("peras-2", 1)
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.PerasAuthorityGranted(left))
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 3}, rootevent.PerasAuthorityGranted(right))
	require.Len(t, st.ActivePerasGrants, 1)
	require.Equal(t, left.GrantID, st.ActivePerasGrants[0].GrantID)

	disjoint := testStatePerasGrant("peras-3", 2)
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 4}, rootevent.PerasAuthorityGranted(disjoint))
	require.Len(t, st.ActivePerasGrants, 2)
}

func TestApplyPerasAuthorityGrantLifecycleToSnapshot(t *testing.T) {
	var snapshot rootstate.Snapshot
	grant := testStatePerasGrant("peras-1", 1)

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 1}, rootevent.PerasAuthorityGranted(grant))
	require.Len(t, snapshot.State.ActivePerasGrants, 1)
	require.Equal(t, grant.EpochID, snapshot.State.PerasAuthorityEpoch)

	stateClone := rootstate.CloneState(snapshot.State)
	stateClone.ActivePerasGrants[0].Scope.Buckets[0] = 8
	require.Equal(t, []uint16{1}, snapshot.State.ActivePerasGrants[0].Scope.Buckets)

	clone := rootstate.CloneSnapshot(snapshot)
	clone.State.ActivePerasGrants[0].Scope.Buckets[0] = 9
	require.Equal(t, []uint16{1}, snapshot.State.ActivePerasGrants[0].Scope.Buckets)

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 2}, rootevent.PerasAuthorityRetired(grant))
	require.Empty(t, snapshot.State.ActivePerasGrants)
}

func BenchmarkApplyPerasAuthorityGrantedToState(b *testing.B) {
	b.ReportAllocs()
	grant := testStatePerasGrant("peras-1", 1)
	event := rootevent.PerasAuthorityGranted(grant)
	cursor := rootstate.Cursor{Term: 1, Index: 1}
	for b.Loop() {
		var st rootstate.State
		rootstate.ApplyEventToState(&st, cursor, event)
		if len(st.ActivePerasGrants) != 1 {
			b.Fatal("grant was not applied")
		}
	}
}

func testStatePerasGrant(grantID string, bucket uint16) rootproto.PerasAuthorityGrant {
	return rootproto.PerasAuthorityGrant{
		GrantID:  grantID,
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.PerasAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{bucket},
			Parents:    []uint64{10},
		},
		ExpiresUnixNano: 1_000,
	}
}

func testStatePerasSeal(grant rootproto.PerasAuthorityGrant, marker byte) rootproto.PerasAuthoritySeal {
	var root [32]byte
	var digest [32]byte
	root[0] = marker
	digest[0] = marker + 10
	return rootproto.PerasAuthoritySeal{
		GrantID:              grant.GrantID,
		EpochID:              grant.EpochID,
		HolderID:             grant.HolderID,
		Scope:                grant.Scope,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		OperationCount:       7,
		EntryCount:           11,
		SealedUnixNano:       int64(marker),
	}
}
