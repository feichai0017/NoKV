// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPerasAuthorityGrantCoversScope(t *testing.T) {
	grant := testPerasGrant(PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1, 2},
		Parents:    []uint64{10},
	})

	require.True(t, grant.Covers(PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{2},
		Parents:    []uint64{10},
	}, 100))
	require.False(t, grant.Covers(PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{3},
		Parents:    []uint64{10},
	}, 100))
	require.False(t, grant.Covers(PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 8,
		Buckets:    []uint16{2},
		Parents:    []uint64{10},
	}, 100))
	require.False(t, grant.Covers(PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{2},
		Parents:    []uint64{10},
	}, 2_000))
}

func TestPerasAuthorityGrantWildcardScope(t *testing.T) {
	mountWide := testPerasGrant(PerasAuthorityScope{MountID: "vol", MountKeyID: 7})
	require.True(t, mountWide.Covers(PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{15},
		Parents:    []uint64{100},
		Inodes:     []uint64{200},
	}, 100))

	bucketGrant := testPerasGrant(PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1},
	})
	require.False(t, bucketGrant.Covers(PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
	}, 100))
}

func TestPerasAuthorityGrantOverlap(t *testing.T) {
	left := testPerasGrant(PerasAuthorityScope{MountID: "vol", MountKeyID: 7, Buckets: []uint16{1}})
	right := testPerasGrant(PerasAuthorityScope{MountID: "vol", MountKeyID: 7, Buckets: []uint16{2}})
	require.False(t, left.Overlaps(right))

	right.Scope.Buckets = []uint16{1}
	require.True(t, left.Overlaps(right))

	right.Scope.MountKeyID = 8
	require.False(t, left.Overlaps(right))
}

func TestClonePerasAuthorityGrantIsIsolated(t *testing.T) {
	grant := testPerasGrant(PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{1},
		Parents:    []uint64{10},
		Inodes:     []uint64{20},
	})
	clone := ClonePerasAuthorityGrant(grant)
	clone.Scope.Buckets[0] = 9
	clone.Scope.Parents[0] = 99
	clone.Scope.Inodes[0] = 999

	require.Equal(t, []uint16{1}, grant.Scope.Buckets)
	require.Equal(t, []uint64{10}, grant.Scope.Parents)
	require.Equal(t, []uint64{20}, grant.Scope.Inodes)
}

func TestPerasAuthorityGrantValidity(t *testing.T) {
	require.False(t, PerasAuthorityScope{}.Valid())
	require.True(t, PerasAuthorityScope{MountID: "vol", MountKeyID: 7}.Valid())

	require.False(t, PerasAuthorityGrant{}.Valid())
	require.False(t, PerasAuthorityGrant{GrantID: "g1", EpochID: 1, HolderID: "h1", ExpiresUnixNano: 1_000}.Valid())
	require.True(t, testPerasGrant(PerasAuthorityScope{MountID: "vol", MountKeyID: 7}).Valid())
}

func BenchmarkPerasAuthorityGrantCovers(b *testing.B) {
	b.ReportAllocs()
	grant := testPerasGrant(PerasAuthorityScope{
		MountID:    "vol",
		MountKeyID: 7,
		Buckets:    []uint16{0, 1, 2, 3, 4, 5, 6, 7},
		Parents:    []uint64{10, 20, 30, 40},
		Inodes:     []uint64{100, 200, 300, 400},
	})
	scope := PerasAuthorityScope{
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

func testPerasGrant(scope PerasAuthorityScope) PerasAuthorityGrant {
	return PerasAuthorityGrant{
		GrantID:         "g1",
		EpochID:         1,
		HolderID:        "holder-a",
		Scope:           scope,
		ExpiresUnixNano: 1_000,
	}
}
