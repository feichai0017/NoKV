package state_test

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestApplyCapsuleAuthorityGrantLifecycleToState(t *testing.T) {
	var st rootstate.State
	cursor := rootstate.Cursor{Term: 1, Index: 1}
	grant := testStateCapsuleGrant("capsule-1", 1)

	rootstate.ApplyEventToState(&st, cursor, rootevent.CapsuleAuthorityGranted(grant))
	found, ok := st.ActiveCapsuleGrantByID(grant.GrantID)
	require.True(t, ok)
	require.Equal(t, grant, found)
	require.Equal(t, cursor, st.LastCommitted)
	require.Equal(t, grant.EpochID, st.CapsuleAuthorityEpoch)

	covered, ok := st.ActiveCapsuleGrantFor(rootproto.CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1},
		Parents:    []uint64{10},
	}, 100)
	require.True(t, ok)
	require.Equal(t, grant.GrantID, covered.GrantID)

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.CapsuleAuthorityRetired(grant))
	_, ok = st.ActiveCapsuleGrantByID(grant.GrantID)
	require.False(t, ok)
	require.Empty(t, st.ActiveCapsuleGrants)
	require.Equal(t, grant.EpochID, st.CapsuleAuthorityEpoch)
}

func TestApplyCapsuleAuthorityRejectsInvalidAndConflictingGrants(t *testing.T) {
	var st rootstate.State
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, rootevent.CapsuleAuthorityGranted(rootproto.CapsuleAuthorityGrant{
		GrantID: "missing-fields",
	}))
	require.Empty(t, st.ActiveCapsuleGrants)

	left := testStateCapsuleGrant("capsule-1", 1)
	right := testStateCapsuleGrant("capsule-2", 1)
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.CapsuleAuthorityGranted(left))
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 3}, rootevent.CapsuleAuthorityGranted(right))
	require.Len(t, st.ActiveCapsuleGrants, 1)
	require.Equal(t, left.GrantID, st.ActiveCapsuleGrants[0].GrantID)

	disjoint := testStateCapsuleGrant("capsule-3", 2)
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 4}, rootevent.CapsuleAuthorityGranted(disjoint))
	require.Len(t, st.ActiveCapsuleGrants, 2)
}

func TestApplyCapsuleAuthorityGrantLifecycleToSnapshot(t *testing.T) {
	var snapshot rootstate.Snapshot
	grant := testStateCapsuleGrant("capsule-1", 1)

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 1}, rootevent.CapsuleAuthorityGranted(grant))
	require.Len(t, snapshot.State.ActiveCapsuleGrants, 1)
	require.Equal(t, grant.EpochID, snapshot.State.CapsuleAuthorityEpoch)

	stateClone := rootstate.CloneState(snapshot.State)
	stateClone.ActiveCapsuleGrants[0].Scope.Buckets[0] = 8
	require.Equal(t, []uint16{1}, snapshot.State.ActiveCapsuleGrants[0].Scope.Buckets)

	clone := rootstate.CloneSnapshot(snapshot)
	clone.State.ActiveCapsuleGrants[0].Scope.Buckets[0] = 9
	require.Equal(t, []uint16{1}, snapshot.State.ActiveCapsuleGrants[0].Scope.Buckets)

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 2}, rootevent.CapsuleAuthorityRetired(grant))
	require.Empty(t, snapshot.State.ActiveCapsuleGrants)
}

func BenchmarkApplyCapsuleAuthorityGrantedToState(b *testing.B) {
	b.ReportAllocs()
	grant := testStateCapsuleGrant("capsule-1", 1)
	event := rootevent.CapsuleAuthorityGranted(grant)
	cursor := rootstate.Cursor{Term: 1, Index: 1}
	for b.Loop() {
		var st rootstate.State
		rootstate.ApplyEventToState(&st, cursor, event)
		if len(st.ActiveCapsuleGrants) != 1 {
			b.Fatal("grant was not applied")
		}
	}
}

func testStateCapsuleGrant(grantID string, bucket uint16) rootproto.CapsuleAuthorityGrant {
	return rootproto.CapsuleAuthorityGrant{
		GrantID:  grantID,
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.CapsuleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{bucket},
			Parents:    []uint64{10},
		},
		ExpiresUnixNano: 1_000,
	}
}
