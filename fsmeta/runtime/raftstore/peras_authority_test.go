package raftstore

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
	perasauth "github.com/feichai0017/NoKV/fsmeta/runtime/perasauth"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
)

type fakePerasAuthorityClient struct {
	calls     int
	sealCalls int
	last      rootproto.PerasAuthorityCommand
	resp      *coordpb.ApplyPerasAuthorityResponse
	seals     []*metapb.RootPerasAuthoritySeal
	err       error
}

func (f *fakePerasAuthorityClient) ApplyPerasAuthority(_ context.Context, req *coordpb.ApplyPerasAuthorityRequest) (*coordpb.ApplyPerasAuthorityResponse, error) {
	f.calls++
	f.last = metawire.RootPerasAuthorityCommandFromProto(req.GetCommand())
	return f.resp, f.err
}

func (f *fakePerasAuthorityClient) ListPerasAuthoritySeals(context.Context, *coordpb.ListPerasAuthoritySealsRequest) (*coordpb.ListPerasAuthoritySealsResponse, error) {
	f.sealCalls++
	return &coordpb.ListPerasAuthoritySealsResponse{Seals: f.seals}, f.err
}

func TestPerasAuthorityManagerAcquireInstallsGrantedAuthority(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	client := &fakePerasAuthorityClient{
		resp: &coordpb.ApplyPerasAuthorityResponse{
			Status:       metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_GRANTED,
			Grant:        metawire.RootPerasAuthorityGrantToProto(grant),
			ActiveGrants: []*metapb.RootPerasAuthorityGrant{metawire.RootPerasAuthorityGrantToProto(grant)},
		},
	}
	table := perasauth.NewActiveAuthorities()
	manager, err := NewPerasAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, grant, got)
	require.Equal(t, 1, client.calls)
	require.Equal(t, rootproto.PerasAuthorityActAcquire, client.last.Kind)
	require.Equal(t, "holder-a", client.last.HolderID)
	require.Equal(t, now.Add(time.Minute).UnixNano(), client.last.ExpiresUnixNano)
	require.Empty(t, client.last.Scope.Buckets)
	require.Empty(t, client.last.Scope.Parents)
	require.Empty(t, client.last.Scope.Inodes)
	require.Equal(t, []perasauth.AuthorityGrant{grant}, table.Snapshot())
}

func TestPerasAuthorityManagerAcquireUsesLocalHeldGrant(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	table := perasauth.NewActiveAuthorities()
	require.NoError(t, table.Replace([]perasauth.AuthorityGrant{grant}))
	client := &fakePerasAuthorityClient{}
	manager, err := NewPerasAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, grant, got)
	require.Zero(t, client.calls)
}

func TestPerasAuthorityManagerAcquireHeldUpdatesActiveAuthorityView(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	held := testRuntimePerasGrant("holder-b/1", "holder-b", scope, now.Add(time.Minute))
	client := &fakePerasAuthorityClient{
		resp: &coordpb.ApplyPerasAuthorityResponse{
			Status:       metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_HELD,
			ActiveGrants: []*metapb.RootPerasAuthorityGrant{metawire.RootPerasAuthorityGrantToProto(held)},
		},
	}
	table := perasauth.NewActiveAuthorities()
	manager, err := NewPerasAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.False(t, owned)
	require.Equal(t, held, got)
	require.Equal(t, []perasauth.AuthorityGrant{held}, table.Snapshot())
}

func TestPerasAuthorityManagerRetireAuthority(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	client := &fakePerasAuthorityClient{
		resp: &coordpb.ApplyPerasAuthorityResponse{
			Status: metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_RETIRED,
		},
	}
	table := perasauth.NewActiveAuthorities()
	require.NoError(t, table.Replace([]perasauth.AuthorityGrant{grant}))
	manager, err := NewPerasAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	require.NoError(t, manager.Retire(context.Background(), grant))
	require.Equal(t, rootproto.PerasAuthorityActRetire, client.last.Kind)
	require.Equal(t, grant.GrantID, client.last.GrantID)
	require.Empty(t, table.Snapshot())
}

func TestPerasAuthorityManagerSealPerasSegmentPublishesRootSeal(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	client := &fakePerasAuthorityClient{
		resp: &coordpb.ApplyPerasAuthorityResponse{
			Status:       metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_SEALED,
			ActiveGrants: []*metapb.RootPerasAuthorityGrant{metawire.RootPerasAuthorityGrantToProto(grant)},
		},
	}
	table := perasauth.NewActiveAuthorities()
	require.NoError(t, table.Replace([]perasauth.AuthorityGrant{grant}))
	manager, err := NewPerasAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)
	segment := testRuntimePerasSegment(t)
	var digest [32]byte
	digest[0] = 99
	cursor := PerasInstallCursor{RegionID: 7, Term: 3, Index: 99, InstallVersion: 1234}

	require.NoError(t, manager.SealPerasSegment(context.Background(), grant, segment, digest, cursor))
	require.Equal(t, rootproto.PerasAuthorityActSeal, client.last.Kind)
	require.Equal(t, grant.GrantID, client.last.GrantID)
	require.Equal(t, segment.Root, client.last.SegmentRoot)
	require.Equal(t, digest, client.last.SegmentPayloadDigest)
	require.Equal(t, uint64(1), client.last.OperationCount)
	require.Equal(t, uint64(2), client.last.EntryCount)
	require.Equal(t, cursor.RegionID, client.last.InstallRegionID)
	require.Equal(t, cursor.Term, client.last.InstallTerm)
	require.Equal(t, cursor.Index, client.last.InstallIndex)
	require.Equal(t, cursor.InstallVersion, client.last.InstallVersion)
}

func TestPerasAuthorityManagerListsMatchingRootSeals(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	matching := testRuntimePerasSeal("grant-a", "holder-a", scope, now)
	otherScope := testRuntimePerasScope(2)
	other := testRuntimePerasSeal("grant-b", "holder-a", otherScope, now)
	client := &fakePerasAuthorityClient{
		seals: []*metapb.RootPerasAuthoritySeal{
			metawire.RootPerasAuthoritySealToProto(matching),
			metawire.RootPerasAuthoritySealToProto(other),
		},
	}
	manager, err := NewPerasAuthorityManager(client, perasauth.NewActiveAuthorities(), "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	seals, err := manager.ListPerasAuthoritySeals(context.Background(), scope)
	require.NoError(t, err)
	require.Equal(t, []rootproto.PerasAuthoritySeal{matching}, seals)
	require.Equal(t, 1, client.sealCalls)
}

func TestPerasAuthorityManagerRetirePerasAuthorityFiltersScope(t *testing.T) {
	now := time.Unix(10, 0)
	scopeA := testRuntimePerasScope(1)
	scopeB := testRuntimePerasScope(2)
	grantA := testRuntimePerasGrant("holder-a/1", "holder-a", scopeA, now.Add(time.Minute))
	grantB := testRuntimePerasGrant("holder-a/2", "holder-a", scopeB, now.Add(time.Minute))
	client := &fakePerasAuthorityClient{
		resp: &coordpb.ApplyPerasAuthorityResponse{
			Status:       metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_RETIRED,
			ActiveGrants: []*metapb.RootPerasAuthorityGrant{metawire.RootPerasAuthorityGrantToProto(grantB)},
		},
	}
	table := perasauth.NewActiveAuthorities()
	require.NoError(t, table.Replace([]perasauth.AuthorityGrant{grantA, grantB}))
	manager, err := NewPerasAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	require.NoError(t, manager.RetirePerasAuthority(context.Background(), scopeA))
	require.Equal(t, 1, client.calls)
	require.Equal(t, grantA.GrantID, client.last.GrantID)
	require.Equal(t, []perasauth.AuthorityGrant{grantB}, table.Snapshot())
}

func TestPerasAuthorityManagerMountRetireScopeMatchesBucketGrants(t *testing.T) {
	now := time.Unix(10, 0)
	scopeA := testRuntimePerasScope(1)
	scopeB := testRuntimePerasScope(2)
	grantA := testRuntimePerasGrant("holder-a/1", "holder-a", scopeA, now.Add(time.Minute))
	grantB := testRuntimePerasGrant("holder-a/2", "holder-a", scopeB, now.Add(time.Minute))
	table := perasauth.NewActiveAuthorities()
	require.NoError(t, table.Replace([]perasauth.AuthorityGrant{grantA, grantB}))
	manager, err := NewPerasAuthorityManager(&fakePerasAuthorityClient{}, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	grants := manager.ownedGrantsForScopes(compile.AuthorityScope{Mount: "vol", MountKeyID: 7})
	require.ElementsMatch(t, []perasauth.AuthorityGrant{grantA, grantB}, grants)
}

func TestPerasAuthorityManagerRetirePerasAuthorityIgnoresForeignGrant(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-b/1", "holder-b", scope, now.Add(time.Minute))
	client := &fakePerasAuthorityClient{}
	table := perasauth.NewActiveAuthorities()
	require.NoError(t, table.Replace([]perasauth.AuthorityGrant{grant}))
	manager, err := NewPerasAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	require.NoError(t, manager.RetirePerasAuthority(context.Background(), scope))
	require.Zero(t, client.calls)
	require.Equal(t, []perasauth.AuthorityGrant{grant}, table.Snapshot())
}

func TestPerasAuthorityManagerRejectsInvalidConfigAndResponses(t *testing.T) {
	_, err := NewPerasAuthorityManager(nil, perasauth.NewActiveAuthorities(), "holder-a", time.Minute, nil)
	require.ErrorIs(t, err, errPerasAuthorityClientRequired)
	_, err = NewPerasAuthorityManager(&fakePerasAuthorityClient{}, nil, "holder-a", time.Minute, nil)
	require.ErrorIs(t, err, errPerasAuthorityTableRequired)
	_, err = NewPerasAuthorityManager(&fakePerasAuthorityClient{}, perasauth.NewActiveAuthorities(), "", time.Minute, nil)
	require.ErrorIs(t, err, errPerasAuthorityHolderRequired)
	_, err = NewPerasAuthorityManager(&fakePerasAuthorityClient{}, perasauth.NewActiveAuthorities(), "holder-a", -time.Second, nil)
	require.ErrorIs(t, err, errPerasAuthorityTTLInvalid)

	manager, err := NewPerasAuthorityManager(&fakePerasAuthorityClient{
		resp: &coordpb.ApplyPerasAuthorityResponse{Status: metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_GRANTED},
	}, perasauth.NewActiveAuthorities(), "holder-a", time.Minute, func() time.Time { return time.Unix(10, 0) })
	require.NoError(t, err)
	_, _, err = manager.Acquire(context.Background(), testRuntimePerasScope(1))
	require.ErrorIs(t, err, errPerasAuthorityInvalidResponse)
}

func TestPerasAuthorityManagerRetireRejectsForeignGrant(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-b/1", "holder-b", scope, now.Add(time.Minute))
	manager, err := NewPerasAuthorityManager(&fakePerasAuthorityClient{}, perasauth.NewActiveAuthorities(), "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	err = manager.Retire(context.Background(), grant)
	require.ErrorIs(t, err, errPerasAuthorityNotHeld)
	require.True(t, IsPerasAuthorityNotHeld(err))
}

func BenchmarkPerasAuthorityManagerAcquireLocalHeld(b *testing.B) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	table := perasauth.NewActiveAuthorities()
	if err := table.Replace([]perasauth.AuthorityGrant{grant}); err != nil {
		b.Fatal(err)
	}
	manager, err := NewPerasAuthorityManager(&fakePerasAuthorityClient{}, table, "holder-a", time.Minute, func() time.Time { return now })
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		got, owned, err := manager.Acquire(context.Background(), scope)
		if err != nil || !owned || got.GrantID == "" {
			b.Fatalf("owned=%v grant=%q err=%v", owned, got.GrantID, err)
		}
	}
}

func testRuntimePerasScope(bucket fsmeta.AffinityBucket) compile.AuthorityScope {
	return compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 7,
		Buckets:    []fsmeta.AffinityBucket{bucket},
		Parents:    []fsmeta.InodeID{99},
		Inodes:     []fsmeta.InodeID{100},
	}
}

func testRuntimePerasGrant(id, holder string, scope compile.AuthorityScope, expires time.Time) perasauth.AuthorityGrant {
	return perasauth.AuthorityGrant{
		GrantID:         id,
		EpochID:         1,
		HolderID:        holder,
		Scope:           perasauth.AuthorityScopeFromDelta(scope),
		ExpiresUnixNano: expires.UnixNano(),
	}
}

func testRuntimePerasSeal(id, holder string, scope compile.AuthorityScope, sealed time.Time) rootproto.PerasAuthoritySeal {
	return rootproto.PerasAuthoritySeal{
		GrantID:              id,
		EpochID:              1,
		HolderID:             holder,
		Scope:                perasauth.AuthorityScopeFromDelta(scope),
		SegmentRoot:          [32]byte{1},
		SegmentPayloadDigest: [32]byte{2},
		OperationCount:       3,
		EntryCount:           4,
		SealedUnixNano:       sealed.UnixNano(),
		InstallRegionID:      5,
		InstallTerm:          6,
		InstallIndex:         7,
		InstallVersion:       8,
	}
}

func testRuntimePerasSegment(t *testing.T) fsperas.PerasSegment {
	t.Helper()
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 7}
	dentry, err := fsmeta.EncodeDentryKey(mount, 99, "a")
	require.NoError(t, err)
	inode, err := fsmeta.EncodeInodeKey(mount, 100)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{
			{
				OpID: fsperas.OperationID{ClientID: "client", Seq: 1},
				Mutations: []fsperas.ReplayMutation{
					{Key: dentry, Value: []byte("dentry-value")},
					{Key: inode, Value: []byte("inode-value")},
				},
			},
		},
	})
	require.NoError(t, err)
	return segment
}
