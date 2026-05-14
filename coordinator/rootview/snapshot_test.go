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

func TestSnapshotPreservesPerasAuthorities(t *testing.T) {
	grant := testRootviewPerasGrant("peras-1", 1)
	rooted := rootstate.Snapshot{
		State: rootstate.State{
			LastCommitted:       rootstate.Cursor{Term: 1, Index: 3},
			ActivePerasGrants:   []rootproto.PerasAuthorityGrant{grant},
			PerasAuthorityEpoch: grant.EpochID,
		},
	}

	snapshot := SnapshotFromRoot(rooted)
	found, ok := snapshot.ActivePerasGrantByID(grant.GrantID)
	require.True(t, ok)
	require.Equal(t, grant, found)
	covered, ok := snapshot.ActivePerasGrantFor(rootproto.PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1},
	}, time.Now().UnixNano())
	require.True(t, ok)
	require.Equal(t, grant.GrantID, covered.GrantID)

	clone := CloneSnapshot(snapshot)
	clone.ActivePerasGrants[0].Scope.Buckets[0] = 9
	require.Equal(t, []uint16{1}, snapshot.ActivePerasGrants[0].Scope.Buckets)

	roundTrip := snapshot.RootSnapshot()
	require.Equal(t, []rootproto.PerasAuthorityGrant{grant}, roundTrip.State.ActivePerasGrants)
	require.Equal(t, grant.EpochID, roundTrip.State.PerasAuthorityEpoch)
}

func TestPreserveNewerAuthorityStateKeepsNewerPerasEpoch(t *testing.T) {
	older := Snapshot{PerasAuthorityEpoch: 1}
	newerGrant := testRootviewPerasGrant("peras-2", 2)
	newerGrant.EpochID = 2
	current := Snapshot{
		ActivePerasGrants:   []rootproto.PerasAuthorityGrant{newerGrant},
		PerasAuthorityEpoch: newerGrant.EpochID,
	}

	merged := PreserveNewerAuthorityState(older, current)
	require.Equal(t, current.ActivePerasGrants, merged.ActivePerasGrants)
	require.Equal(t, current.PerasAuthorityEpoch, merged.PerasAuthorityEpoch)
}

func BenchmarkSnapshotActivePerasGrantFor(b *testing.B) {
	b.ReportAllocs()
	grants := make([]rootproto.PerasAuthorityGrant, 0, 16)
	for bucket := range 16 {
		grants = append(grants, testRootviewPerasGrant("peras-"+string(rune('a'+bucket)), uint16(bucket)))
	}
	snapshot := Snapshot{ActivePerasGrants: grants}
	scope := rootproto.PerasAuthorityScope{MountID: "vol", MountKeyID: 7, Buckets: []uint16{11}}
	now := time.Now().UnixNano()

	for b.Loop() {
		grant, ok := snapshot.ActivePerasGrantFor(scope, now)
		if !ok || grant.GrantID == "" {
			b.Fatal("missing peras grant")
		}
	}
}

func testRootviewPerasGrant(grantID string, bucket uint16) rootproto.PerasAuthorityGrant {
	return rootproto.PerasAuthorityGrant{
		GrantID:  grantID,
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.PerasAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{bucket},
		},
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
	}
}
