package raftstore

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	perasauth "github.com/feichai0017/NoKV/fsmeta/runtime/perasauth"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
)

type fakePerasAuthorityClient struct {
	calls int
	last  rootproto.PerasAuthorityCommand
	resp  *coordpb.ApplyPerasAuthorityResponse
	err   error
}

func (f *fakePerasAuthorityClient) ApplyPerasAuthority(_ context.Context, req *coordpb.ApplyPerasAuthorityRequest) (*coordpb.ApplyPerasAuthorityResponse, error) {
	f.calls++
	f.last = metawire.RootPerasAuthorityCommandFromProto(req.GetCommand())
	return f.resp, f.err
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
