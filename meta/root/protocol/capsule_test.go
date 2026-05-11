package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCapsuleAuthorityGrantCoversScope(t *testing.T) {
	grant := testCapsuleGrant(CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1, 2},
		Parents:    []uint64{10},
	})

	require.True(t, grant.Covers(CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{2},
		Parents:    []uint64{10},
	}, 100))
	require.False(t, grant.Covers(CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{3},
		Parents:    []uint64{10},
	}, 100))
	require.False(t, grant.Covers(CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 8,
		Buckets:    []uint16{2},
		Parents:    []uint64{10},
	}, 100))
	require.False(t, grant.Covers(CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{2},
		Parents:    []uint64{10},
	}, 2_000))
}

func TestCapsuleAuthorityGrantWildcardScope(t *testing.T) {
	mountWide := testCapsuleGrant(CapsuleAuthorityScope{MountID: "vol", MountKeyID: 7})
	require.True(t, mountWide.Covers(CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{15},
		Parents:    []uint64{100},
		Inodes:     []uint64{200},
	}, 100))

	bucketGrant := testCapsuleGrant(CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1},
	})
	require.False(t, bucketGrant.Covers(CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
	}, 100))
}

func TestCapsuleAuthorityGrantOverlap(t *testing.T) {
	left := testCapsuleGrant(CapsuleAuthorityScope{MountID: "vol", MountKeyID: 7, Buckets: []uint16{1}})
	right := testCapsuleGrant(CapsuleAuthorityScope{MountID: "vol", MountKeyID: 7, Buckets: []uint16{2}})
	require.False(t, left.Overlaps(right))

	right.Scope.Buckets = []uint16{1}
	require.True(t, left.Overlaps(right))

	right.Scope.MountKeyID = 8
	require.False(t, left.Overlaps(right))
}

func TestCloneCapsuleAuthorityGrantIsIsolated(t *testing.T) {
	grant := testCapsuleGrant(CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1},
		Parents:    []uint64{10},
		Inodes:     []uint64{20},
	})
	clone := CloneCapsuleAuthorityGrant(grant)
	clone.Scope.Buckets[0] = 9
	clone.Scope.Parents[0] = 99
	clone.Scope.Inodes[0] = 999

	require.Equal(t, []uint16{1}, grant.Scope.Buckets)
	require.Equal(t, []uint64{10}, grant.Scope.Parents)
	require.Equal(t, []uint64{20}, grant.Scope.Inodes)
}

func TestCapsuleAuthorityGrantValidity(t *testing.T) {
	require.False(t, CapsuleAuthorityScope{}.Valid())
	require.True(t, CapsuleAuthorityScope{MountID: "vol", MountKeyID: 7}.Valid())

	require.False(t, CapsuleAuthorityGrant{}.Valid())
	require.False(t, CapsuleAuthorityGrant{GrantID: "g1", EpochID: 1, HolderID: "h1", ExpiresUnixNano: 1_000}.Valid())
	require.True(t, testCapsuleGrant(CapsuleAuthorityScope{MountID: "vol", MountKeyID: 7}).Valid())
}

func BenchmarkCapsuleAuthorityGrantCovers(b *testing.B) {
	b.ReportAllocs()
	grant := testCapsuleGrant(CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{0, 1, 2, 3, 4, 5, 6, 7},
		Parents:    []uint64{10, 20, 30, 40},
		Inodes:     []uint64{100, 200, 300, 400},
	})
	scope := CapsuleAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{3},
		Parents:    []uint64{20},
		Inodes:     []uint64{300},
	}
	for b.Loop() {
		if !grant.Covers(scope, 100) {
			b.Fatal("grant should cover scope")
		}
	}
}

func testCapsuleGrant(scope CapsuleAuthorityScope) CapsuleAuthorityGrant {
	return CapsuleAuthorityGrant{
		GrantID:         "g1",
		EpochID:         1,
		HolderID:        "holder-a",
		Scope:           scope,
		ExpiresUnixNano: 1_000,
	}
}
