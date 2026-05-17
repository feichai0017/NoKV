// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"testing"
	"time"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
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

func TestManagerAcquireInstallsGrantedAuthority(t *testing.T) {
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
	table := NewActiveAuthorities()
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
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
	require.Equal(t, []rootproto.PerasAuthorityGrant{grant}, table.Snapshot())
}

func TestManagerAcquireMountWideGrantCoversBucket(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	wideScope := scope
	wideScope.Buckets = nil
	wideScope.Parents = nil
	wideScope.Inodes = nil
	grant := testRuntimePerasGrant("holder-a/1", "holder-a", wideScope, now.Add(time.Minute))
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.PerasAuthorityGrant{grant}))
	client := &fakePerasAuthorityClient{}
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, grant.GrantID, got.GrantID)
	require.Equal(t, grant.HolderID, got.HolderID)
	require.Equal(t, grant.Scope.MountKeyID, got.Scope.MountKeyID)
	require.Empty(t, got.Scope.Buckets)
	require.Empty(t, got.Scope.Parents)
	require.Empty(t, got.Scope.Inodes)
	require.Zero(t, client.calls)
}

func TestManagerAcquireUsesLocalHeldGrant(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.PerasAuthorityGrant{grant}))
	client := &fakePerasAuthorityClient{}
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, grant, got)
	require.Zero(t, client.calls)
}

func TestManagerAcquireHeldUpdatesActiveAuthorityView(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	held := testRuntimePerasGrant("holder-b/1", "holder-b", scope, now.Add(time.Minute))
	client := &fakePerasAuthorityClient{
		resp: &coordpb.ApplyPerasAuthorityResponse{
			Status:       metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_HELD,
			ActiveGrants: []*metapb.RootPerasAuthorityGrant{metawire.RootPerasAuthorityGrantToProto(held)},
		},
	}
	table := NewActiveAuthorities()
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.False(t, owned)
	require.Equal(t, held, got)
	require.Equal(t, []rootproto.PerasAuthorityGrant{held}, table.Snapshot())
}

func TestManagerRetireAuthority(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	client := &fakePerasAuthorityClient{
		resp: &coordpb.ApplyPerasAuthorityResponse{
			Status: metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_RETIRED,
		},
	}
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.PerasAuthorityGrant{grant}))
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	require.NoError(t, manager.Retire(context.Background(), grant))
	require.Equal(t, rootproto.PerasAuthorityActRetire, client.last.Kind)
	require.Equal(t, grant.GrantID, client.last.GrantID)
	require.Empty(t, table.Snapshot())
}

func TestManagerPublishesSegmentSeal(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	client := &fakePerasAuthorityClient{
		resp: &coordpb.ApplyPerasAuthorityResponse{
			Status:       metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_SEALED,
			ActiveGrants: []*metapb.RootPerasAuthorityGrant{metawire.RootPerasAuthorityGrantToProto(grant)},
		},
	}
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.PerasAuthorityGrant{grant}))
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)
	segment := testRuntimePerasSegment(t)
	var digest [32]byte
	digest[0] = 99
	cursor := InstallCursor{RegionID: 7, Term: 3, Index: 99, InstallVersion: 1234}

	require.NoError(t, manager.PublishSegmentSeal(context.Background(), grant, segment, digest, cursor))
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

func TestManagerListsMatchingRootSeals(t *testing.T) {
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
	manager, err := NewAuthorityManager(client, NewActiveAuthorities(), "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	seals, err := manager.ListPerasAuthoritySeals(context.Background(), scope)
	require.NoError(t, err)
	require.Equal(t, []rootproto.PerasAuthoritySeal{matching}, seals)
	require.Equal(t, 1, client.sealCalls)
}

func TestManagerRetirePerasAuthorityFiltersScope(t *testing.T) {
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
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.PerasAuthorityGrant{grantA, grantB}))
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	require.NoError(t, manager.RetirePerasAuthority(context.Background(), scopeA))
	require.Equal(t, 1, client.calls)
	require.Equal(t, grantA.GrantID, client.last.GrantID)
	require.Equal(t, []rootproto.PerasAuthorityGrant{grantB}, table.Snapshot())
}

func TestManagerMountRetireScopeMatchesBucketGrants(t *testing.T) {
	now := time.Unix(10, 0)
	scopeA := testRuntimePerasScope(1)
	scopeB := testRuntimePerasScope(2)
	grantA := testRuntimePerasGrant("holder-a/1", "holder-a", scopeA, now.Add(time.Minute))
	grantB := testRuntimePerasGrant("holder-a/2", "holder-a", scopeB, now.Add(time.Minute))
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.PerasAuthorityGrant{grantA, grantB}))
	manager, err := NewAuthorityManager(&fakePerasAuthorityClient{}, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	grants := manager.ownedGrantsForScopes(compile.AuthorityScope{Mount: "vol", MountKeyID: 7})
	require.ElementsMatch(t, []rootproto.PerasAuthorityGrant{grantA, grantB}, grants)
}

func TestManagerRetirePerasAuthorityIgnoresForeignGrant(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-b/1", "holder-b", scope, now.Add(time.Minute))
	client := &fakePerasAuthorityClient{}
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.PerasAuthorityGrant{grant}))
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	require.NoError(t, manager.RetirePerasAuthority(context.Background(), scope))
	require.Zero(t, client.calls)
	require.Equal(t, []rootproto.PerasAuthorityGrant{grant}, table.Snapshot())
}

func TestManagerRejectsInvalidConfigAndResponses(t *testing.T) {
	_, err := NewAuthorityManager(nil, NewActiveAuthorities(), "holder-a", time.Minute, nil)
	require.ErrorIs(t, err, ErrClientRequired)
	_, err = NewAuthorityManager(&fakePerasAuthorityClient{}, nil, "holder-a", time.Minute, nil)
	require.ErrorIs(t, err, ErrTableRequired)
	_, err = NewAuthorityManager(&fakePerasAuthorityClient{}, NewActiveAuthorities(), "", time.Minute, nil)
	require.ErrorIs(t, err, ErrHolderRequired)
	_, err = NewAuthorityManager(&fakePerasAuthorityClient{}, NewActiveAuthorities(), "holder-a", -time.Second, nil)
	require.ErrorIs(t, err, ErrTTLInvalid)

	manager, err := NewAuthorityManager(&fakePerasAuthorityClient{
		resp: &coordpb.ApplyPerasAuthorityResponse{Status: metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_GRANTED},
	}, NewActiveAuthorities(), "holder-a", time.Minute, func() time.Time { return time.Unix(10, 0) })
	require.NoError(t, err)
	_, _, err = manager.Acquire(context.Background(), testRuntimePerasScope(1))
	require.ErrorIs(t, err, ErrInvalidResponse)
}

func TestManagerRetireRejectsForeignGrant(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-b/1", "holder-b", scope, now.Add(time.Minute))
	manager, err := NewAuthorityManager(&fakePerasAuthorityClient{}, NewActiveAuthorities(), "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	err = manager.Retire(context.Background(), grant)
	require.ErrorIs(t, err, ErrNotHeld)
	require.True(t, IsNotHeld(err))
}

func BenchmarkManagerAcquireLocalHeld(b *testing.B) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimePerasGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	table := NewActiveAuthorities()
	if err := table.Replace([]rootproto.PerasAuthorityGrant{grant}); err != nil {
		b.Fatal(err)
	}
	manager, err := NewAuthorityManager(&fakePerasAuthorityClient{}, table, "holder-a", time.Minute, func() time.Time { return now })
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

func testRuntimePerasGrant(id, holder string, scope compile.AuthorityScope, expires time.Time) rootproto.PerasAuthorityGrant {
	return rootproto.PerasAuthorityGrant{
		GrantID:         id,
		EpochID:         1,
		HolderID:        holder,
		Scope:           AuthorityScopeFromDelta(scope),
		ExpiresUnixNano: expires.UnixNano(),
	}
}

func testRuntimePerasSeal(id, holder string, scope compile.AuthorityScope, sealed time.Time) rootproto.PerasAuthoritySeal {
	return rootproto.PerasAuthoritySeal{
		GrantID:              id,
		EpochID:              1,
		HolderID:             holder,
		Scope:                AuthorityScopeFromDelta(scope),
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
