// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package rootview

import (
	"testing"
	"time"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestSnapshotPreservesVisibleAuthorityAuthorities(t *testing.T) {
	grant := testRootviewVisibleGrant("visible-1", 1)
	rooted := rootstate.Snapshot{
		State: rootstate.State{
			LastCommitted:         rootstate.Cursor{Term: 1, Index: 3},
			ActiveVisibleGrants:   []rootproto.VisibleAuthorityGrant{grant},
			VisibleAuthorityEpoch: grant.EpochID,
		},
	}

	snapshot := SnapshotFromRoot(rooted)
	found, ok := snapshot.ActiveVisibleGrantByID(grant.GrantID)
	require.True(t, ok)
	require.Equal(t, grant, found)
	covered, ok := snapshot.ActiveVisibleGrantFor(rootproto.VisibleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1},
	}, time.Now().UnixNano())
	require.True(t, ok)
	require.Equal(t, grant.GrantID, covered.GrantID)

	clone := CloneSnapshot(snapshot)
	clone.ActiveVisibleGrants[0].Scope.Buckets[0] = 9
	require.Equal(t, []uint16{1}, snapshot.ActiveVisibleGrants[0].Scope.Buckets)

	roundTrip := snapshot.RootSnapshot()
	require.Equal(t, []rootproto.VisibleAuthorityGrant{grant}, roundTrip.State.ActiveVisibleGrants)
	require.Equal(t, grant.EpochID, roundTrip.State.VisibleAuthorityEpoch)
}

func TestPreserveNewerAuthorityStateKeepsNewerVisibleAuthorityEpoch(t *testing.T) {
	older := Snapshot{VisibleAuthorityEpoch: 1}
	newerGrant := testRootviewVisibleGrant("visible-2", 2)
	newerGrant.EpochID = 2
	current := Snapshot{
		ActiveVisibleGrants:   []rootproto.VisibleAuthorityGrant{newerGrant},
		VisibleAuthorityEpoch: newerGrant.EpochID,
	}

	merged := PreserveNewerAuthorityState(older, current)
	require.Equal(t, current.ActiveVisibleGrants, merged.ActiveVisibleGrants)
	require.Equal(t, current.VisibleAuthorityEpoch, merged.VisibleAuthorityEpoch)
}

func BenchmarkSnapshotActiveVisibleGrantFor(b *testing.B) {
	b.ReportAllocs()
	grants := make([]rootproto.VisibleAuthorityGrant, 0, 16)
	for bucket := range 16 {
		grants = append(grants, testRootviewVisibleGrant("visible-"+string(rune('a'+bucket)), uint16(bucket)))
	}
	snapshot := Snapshot{ActiveVisibleGrants: grants}
	scope := rootproto.VisibleAuthorityScope{MountID: "vol", MountKeyID: 7, Buckets: []uint16{11}}
	now := time.Now().UnixNano()

	for b.Loop() {
		grant, ok := snapshot.ActiveVisibleGrantFor(scope, now)
		if !ok || grant.GrantID == "" {
			b.Fatal("missing visible authority grant")
		}
	}
}

func testRootviewVisibleGrant(grantID string, bucket uint16) rootproto.VisibleAuthorityGrant {
	return rootproto.VisibleAuthorityGrant{
		GrantID:  grantID,
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.VisibleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{bucket},
		},
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
	}
}
