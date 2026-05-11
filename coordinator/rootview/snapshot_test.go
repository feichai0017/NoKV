package rootview

import (
	"testing"
	"time"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestSnapshotPreservesCapsuleAuthorities(t *testing.T) {
	grant := testRootviewCapsuleGrant("capsule-1", 1)
	rooted := rootstate.Snapshot{
		State: rootstate.State{
			LastCommitted:         rootstate.Cursor{Term: 1, Index: 3},
			ActiveCapsuleGrants:   []rootproto.CapsuleAuthorityGrant{grant},
			CapsuleAuthorityEpoch: grant.EpochID,
		},
	}

	snapshot := SnapshotFromRoot(rooted)
	found, ok := snapshot.ActiveCapsuleGrantByID(grant.GrantID)
	require.True(t, ok)
	require.Equal(t, grant, found)
	covered, ok := snapshot.ActiveCapsuleGrantFor(rootproto.CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1},
	}, time.Now().UnixNano())
	require.True(t, ok)
	require.Equal(t, grant.GrantID, covered.GrantID)

	clone := CloneSnapshot(snapshot)
	clone.ActiveCapsuleGrants[0].Scope.Buckets[0] = 9
	require.Equal(t, []uint16{1}, snapshot.ActiveCapsuleGrants[0].Scope.Buckets)

	roundTrip := snapshot.RootSnapshot()
	require.Equal(t, []rootproto.CapsuleAuthorityGrant{grant}, roundTrip.State.ActiveCapsuleGrants)
	require.Equal(t, grant.EpochID, roundTrip.State.CapsuleAuthorityEpoch)
}

func TestPreserveNewerAuthorityStateKeepsNewerCapsuleEpoch(t *testing.T) {
	older := Snapshot{CapsuleAuthorityEpoch: 1}
	newerGrant := testRootviewCapsuleGrant("capsule-2", 2)
	newerGrant.EpochID = 2
	current := Snapshot{
		ActiveCapsuleGrants:   []rootproto.CapsuleAuthorityGrant{newerGrant},
		CapsuleAuthorityEpoch: newerGrant.EpochID,
	}

	merged := PreserveNewerAuthorityState(older, current)
	require.Equal(t, current.ActiveCapsuleGrants, merged.ActiveCapsuleGrants)
	require.Equal(t, current.CapsuleAuthorityEpoch, merged.CapsuleAuthorityEpoch)
}

func BenchmarkSnapshotActiveCapsuleGrantFor(b *testing.B) {
	b.ReportAllocs()
	grants := make([]rootproto.CapsuleAuthorityGrant, 0, 16)
	for bucket := range 16 {
		grants = append(grants, testRootviewCapsuleGrant("capsule-"+string(rune('a'+bucket)), uint16(bucket)))
	}
	snapshot := Snapshot{ActiveCapsuleGrants: grants}
	scope := rootproto.CapsuleAuthorityScope{MountID: "vol", MountKeyID: 7, Buckets: []uint16{11}}
	now := time.Now().UnixNano()

	for b.Loop() {
		grant, ok := snapshot.ActiveCapsuleGrantFor(scope, now)
		if !ok || grant.GrantID == "" {
			b.Fatal("missing capsule grant")
		}
	}
}

func testRootviewCapsuleGrant(grantID string, bucket uint16) rootproto.CapsuleAuthorityGrant {
	return rootproto.CapsuleAuthorityGrant{
		GrantID:  grantID,
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.CapsuleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{bucket},
		},
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
	}
}
