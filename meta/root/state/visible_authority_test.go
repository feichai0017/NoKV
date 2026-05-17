// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestApplyVisibleAuthorityGrantLifecycleToState(t *testing.T) {
	var st rootstate.State
	cursor := rootstate.Cursor{Term: 1, Index: 1}
	grant := testStateVisibleGrant("visible-1", 1)

	rootstate.ApplyEventToState(&st, cursor, rootevent.VisibleAuthorityGranted(grant))
	found, ok := st.ActiveVisibleGrantByID(grant.GrantID)
	require.True(t, ok)
	expected := grant
	expected.RootClusterEpoch = 1
	expected.IssuedRootToken = rootproto.AuthorityRootToken{Term: 1, Index: 1, Revision: 1}
	require.Equal(t, expected, found)
	require.Equal(t, cursor, st.LastCommitted)
	require.Equal(t, grant.EpochID, st.VisibleAuthorityEpoch)

	covered, ok := st.ActiveVisibleGrantFor(rootproto.VisibleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1},
		Parents:    []uint64{10},
	}, 100)
	require.True(t, ok)
	require.Equal(t, grant.GrantID, covered.GrantID)

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.VisibleAuthorityRetired(grant))
	_, ok = st.ActiveVisibleGrantByID(grant.GrantID)
	require.False(t, ok)
	require.Empty(t, st.ActiveVisibleGrants)
	require.Equal(t, grant.EpochID, st.VisibleAuthorityEpoch)
}

func TestApplyVisibleAuthoritySealTracksLatestFrontier(t *testing.T) {
	var st rootstate.State
	grant := testStateVisibleGrant("visible-1", 1)
	first := testStateVisibleSeal(grant, 1)
	second := testStateVisibleSeal(grant, 2)

	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, rootevent.VisibleAuthoritySealed(first))
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.VisibleAuthoritySealed(second))

	require.Len(t, st.VisibleAuthoritySeals, 1)
	require.Equal(t, second.SegmentRoot, st.VisibleAuthoritySeals[0].SegmentRoot)
	found, ok := st.LatestVisibleAuthoritySealFor(rootproto.VisibleAuthorityScope{MountID: "vol", MountKeyID: 7})
	require.True(t, ok)
	require.Equal(t, second.SegmentRoot, found.SegmentRoot)

	clone := rootstate.CloneState(st)
	clone.VisibleAuthoritySeals[0].Scope.Buckets[0] = 9
	require.Equal(t, []uint16{1}, st.VisibleAuthoritySeals[0].Scope.Buckets)
}

func TestApplyVisibleAuthorityRejectsInvalidAndConflictingGrants(t *testing.T) {
	var st rootstate.State
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 1}, rootevent.VisibleAuthorityGranted(rootproto.VisibleAuthorityGrant{
		GrantID: "missing-fields",
	}))
	require.Empty(t, st.ActiveVisibleGrants)

	left := testStateVisibleGrant("visible-1", 1)
	right := testStateVisibleGrant("visible-2", 1)
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 2}, rootevent.VisibleAuthorityGranted(left))
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 3}, rootevent.VisibleAuthorityGranted(right))
	require.Len(t, st.ActiveVisibleGrants, 1)
	require.Equal(t, left.GrantID, st.ActiveVisibleGrants[0].GrantID)

	disjoint := testStateVisibleGrant("visible-3", 2)
	rootstate.ApplyEventToState(&st, rootstate.Cursor{Term: 1, Index: 4}, rootevent.VisibleAuthorityGranted(disjoint))
	require.Len(t, st.ActiveVisibleGrants, 2)
}

func TestApplyVisibleAuthorityGrantLifecycleToSnapshot(t *testing.T) {
	var snapshot rootstate.Snapshot
	grant := testStateVisibleGrant("visible-1", 1)

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 1}, rootevent.VisibleAuthorityGranted(grant))
	require.Len(t, snapshot.State.ActiveVisibleGrants, 1)
	require.Equal(t, grant.EpochID, snapshot.State.VisibleAuthorityEpoch)

	stateClone := rootstate.CloneState(snapshot.State)
	stateClone.ActiveVisibleGrants[0].Scope.Buckets[0] = 8
	require.Equal(t, []uint16{1}, snapshot.State.ActiveVisibleGrants[0].Scope.Buckets)

	clone := rootstate.CloneSnapshot(snapshot)
	clone.State.ActiveVisibleGrants[0].Scope.Buckets[0] = 9
	require.Equal(t, []uint16{1}, snapshot.State.ActiveVisibleGrants[0].Scope.Buckets)

	rootstate.ApplyEventToSnapshot(&snapshot, rootstate.Cursor{Term: 1, Index: 2}, rootevent.VisibleAuthorityRetired(grant))
	require.Empty(t, snapshot.State.ActiveVisibleGrants)
}

func BenchmarkApplyVisibleAuthorityGrantedToState(b *testing.B) {
	b.ReportAllocs()
	grant := testStateVisibleGrant("visible-1", 1)
	event := rootevent.VisibleAuthorityGranted(grant)
	cursor := rootstate.Cursor{Term: 1, Index: 1}
	for b.Loop() {
		var st rootstate.State
		rootstate.ApplyEventToState(&st, cursor, event)
		if len(st.ActiveVisibleGrants) != 1 {
			b.Fatal("grant was not applied")
		}
	}
}

func testStateVisibleGrant(grantID string, bucket uint16) rootproto.VisibleAuthorityGrant {
	return rootproto.VisibleAuthorityGrant{
		GrantID:  grantID,
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.VisibleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{bucket},
			Parents:    []uint64{10},
		},
		ExpiresUnixNano: 1_000,
	}
}

func testStateVisibleSeal(grant rootproto.VisibleAuthorityGrant, marker byte) rootproto.VisibleAuthoritySeal {
	var root [32]byte
	var digest [32]byte
	root[0] = marker
	digest[0] = marker + 10
	return rootproto.VisibleAuthoritySeal{
		GrantID:              grant.GrantID,
		EpochID:              grant.EpochID,
		HolderID:             grant.HolderID,
		Scope:                grant.Scope,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		OperationCount:       7,
		EntryCount:           11,
		SealedUnixNano:       int64(marker),
		InstallRegionID:      uint64(marker) + 100,
		InstallTerm:          uint64(marker) + 200,
		InstallIndex:         uint64(marker) + 300,
		InstallVersion:       uint64(marker) + 400,
	}
}
