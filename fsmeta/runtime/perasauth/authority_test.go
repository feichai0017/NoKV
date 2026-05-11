package perasauth

import (
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	"github.com/stretchr/testify/require"
)

var (
	testNow   = time.Unix(100, 0)
	testMount = fsmeta.MountIdentity{MountID: "vol", MountKeyID: 7}
)

func TestActiveAuthoritiesFindsCoveringGrant(t *testing.T) {
	table := NewActiveAuthorities()
	request := compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{2},
		Parents:    []fsmeta.InodeID{10},
	}
	grant := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1, 2},
		Parents:    []fsmeta.InodeID{10},
	})
	require.NoError(t, table.Replace([]AuthorityGrant{grant}))

	found, ok, err := table.Find(request, testNow)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "holder-a", found.HolderID)

	holder, ok, err := table.HolderFor(request, testNow)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "holder-a", holder)

	held, err := table.HeldBy("holder-a", request, testNow)
	require.NoError(t, err)
	require.True(t, held)
	held, err = table.HeldBy("holder-b", request, testNow)
	require.NoError(t, err)
	require.False(t, held)
}

func TestActiveAuthoritiesRejectsExpiredAndWrongMount(t *testing.T) {
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]AuthorityGrant{
		testGrant("g1", "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{1},
		}),
	}))

	_, ok, err := table.Find(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	}, testNow.Add(2*time.Hour))
	require.NoError(t, err)
	require.False(t, ok)

	_, ok, err = table.Find(compile.AuthorityScope{
		Mount:      "other",
		MountKeyID: 9,
		Buckets:    []fsmeta.AffinityBucket{1},
	}, testNow)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestActiveAuthoritiesTreatsEmptyGrantDimensionsAsWildcard(t *testing.T) {
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]AuthorityGrant{
		testGrant("g1", "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
		}),
	}))

	_, ok, err := table.Find(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{15},
		Parents:    []fsmeta.InodeID{100},
		Inodes:     []fsmeta.InodeID{200},
	}, testNow)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestActiveAuthoritiesDoesNotLetSpecificBucketCoverMountWideScope(t *testing.T) {
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]AuthorityGrant{
		testGrant("g1", "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{1},
		}),
	}))

	_, ok, err := table.Find(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
	}, testNow)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestActiveAuthoritiesRejectsInvalidDuplicateAndConflictingGrants(t *testing.T) {
	table := NewActiveAuthorities()

	require.ErrorIs(t, table.Replace([]AuthorityGrant{{GrantID: "missing-fields"}}), ErrInvalidGrant)

	left := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})
	duplicate := left
	require.ErrorIs(t, table.Replace([]AuthorityGrant{left, duplicate}), ErrInvalidGrant)

	right := testGrant("g2", "holder-b", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})
	require.ErrorIs(t, table.Replace([]AuthorityGrant{left, right}), ErrConflictingGrant)
}

func TestActiveAuthoritiesAllowsDisjointBuckets(t *testing.T) {
	table := NewActiveAuthorities()
	left := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})
	right := testGrant("g2", "holder-b", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{2},
	})
	require.NoError(t, table.Replace([]AuthorityGrant{left, right}))

	found, ok, err := table.Find(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{2},
	}, testNow)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "holder-b", found.HolderID)
}

func TestActiveAuthoritiesSnapshotIsIsolated(t *testing.T) {
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]AuthorityGrant{
		testGrant("g1", "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{1},
		}),
	}))

	snapshot := table.Snapshot()
	require.Len(t, snapshot, 1)
	snapshot[0].Scope.Buckets[0] = 9

	found, ok, err := table.Find(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	}, testNow)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []uint16{1}, found.Scope.Buckets)
}

func TestActiveAuthoritiesDetectsAmbiguousTableState(t *testing.T) {
	table := &ActiveAuthorities{grants: map[string]AuthorityGrant{
		"g1": testGrant("g1", "holder-a", compile.AuthorityScope{Mount: "vol", MountKeyID: 7}),
		"g2": testGrant("g2", "holder-b", compile.AuthorityScope{Mount: "vol", MountKeyID: 7}),
	}}
	_, ok, err := table.Find(compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 7,
		Buckets:    []fsmeta.AffinityBucket{3},
	}, testNow)
	require.False(t, ok)
	require.True(t, errors.Is(err, ErrAmbiguousAuthority))
}

func TestActiveAuthoritiesAppliesRootEvents(t *testing.T) {
	table := NewActiveAuthorities()
	grant := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})

	require.NoError(t, table.ApplyRootEvent(rootevent.PerasAuthorityGranted(grant)))
	require.Equal(t, grant.GrantID, table.Snapshot()[0].GrantID)

	replacement := grant
	replacement.HolderID = "holder-b"
	replacement.EpochID = 2
	require.NoError(t, table.ApplyRootEvent(rootevent.PerasAuthorityGranted(replacement)))
	require.Equal(t, replacement.HolderID, table.Snapshot()[0].HolderID)

	require.NoError(t, table.ApplyRootEvent(rootevent.PerasAuthorityRetired(replacement)))
	require.Empty(t, table.Snapshot())
}

func TestActiveAuthoritiesRejectsConflictingRootEvent(t *testing.T) {
	table := NewActiveAuthorities()
	left := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})
	right := testGrant("g2", "holder-b", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})

	require.NoError(t, table.ApplyRootEvent(rootevent.PerasAuthorityGranted(left)))
	require.ErrorIs(t, table.ApplyRootEvent(rootevent.PerasAuthorityGranted(right)), ErrConflictingGrant)
	require.Equal(t, left.GrantID, table.Snapshot()[0].GrantID)
}

func TestAuthorityScopeFromDeltaConvertsRootTypes(t *testing.T) {
	scope := AuthorityScopeFromDelta(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1, 2},
		Parents:    []fsmeta.InodeID{10},
		Inodes:     []fsmeta.InodeID{20},
	})

	require.Equal(t, "vol", scope.MountID)
	require.Equal(t, uint64(7), scope.MountKeyID)
	require.Equal(t, []uint16{1, 2}, scope.Buckets)
	require.Equal(t, []uint64{10}, scope.Parents)
	require.Equal(t, []uint64{20}, scope.Inodes)
}

func BenchmarkActiveAuthoritiesApplyRootEvent(b *testing.B) {
	b.ReportAllocs()
	table := NewActiveAuthorities()
	grants := make([]AuthorityGrant, 0, 16)
	for bucket := range 16 {
		grants = append(grants, testGrant("g"+string(rune('a'+bucket)), "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{fsmeta.AffinityBucket(bucket)},
		}))
	}
	require.NoError(b, table.Replace(grants))
	event := rootevent.PerasAuthorityRetired(grants[11])

	for b.Loop() {
		require.NoError(b, table.ApplyRootEvent(rootevent.PerasAuthorityGranted(grants[11])))
		require.NoError(b, table.ApplyRootEvent(event))
	}
}

func BenchmarkActiveAuthoritiesFind(b *testing.B) {
	b.ReportAllocs()
	table := NewActiveAuthorities()
	grants := make([]AuthorityGrant, 0, 16)
	for bucket := range 16 {
		grants = append(grants, testGrant("g"+string(rune('a'+bucket)), "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{fsmeta.AffinityBucket(bucket)},
		}))
	}
	require.NoError(b, table.Replace(grants))
	scope := compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{11},
	}

	for b.Loop() {
		grant, ok, err := table.Find(scope, testNow)
		if err != nil || !ok || grant.HolderID != "holder-a" {
			b.Fatalf("unexpected lookup result: grant=%+v ok=%v err=%v", grant, ok, err)
		}
	}
}

func testGrant(id, holder string, scope compile.AuthorityScope) AuthorityGrant {
	return AuthorityGrant{
		GrantID:         id,
		EpochID:         1,
		HolderID:        holder,
		Scope:           AuthorityScopeFromDelta(scope),
		ExpiresUnixNano: testNow.Add(time.Hour).UnixNano(),
	}
}
