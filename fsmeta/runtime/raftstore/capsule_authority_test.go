package raftstore

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fscapsule "github.com/feichai0017/NoKV/fsmeta/exec/capsule"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
)

type fakeCapsuleAuthorityClient struct {
	calls int
	last  rootproto.CapsuleAuthorityCommand
	resp  *coordpb.ApplyCapsuleAuthorityResponse
	err   error
}

func (f *fakeCapsuleAuthorityClient) ApplyCapsuleAuthority(_ context.Context, req *coordpb.ApplyCapsuleAuthorityRequest) (*coordpb.ApplyCapsuleAuthorityResponse, error) {
	f.calls++
	f.last = metawire.RootCapsuleAuthorityCommandFromProto(req.GetCommand())
	return f.resp, f.err
}

func TestCapsuleAuthorityManagerAcquireInstallsGrantedAuthority(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimeCapsuleScope(1)
	grant := testRuntimeCapsuleGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	client := &fakeCapsuleAuthorityClient{
		resp: &coordpb.ApplyCapsuleAuthorityResponse{
			Status:       metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_GRANTED,
			Grant:        metawire.RootCapsuleAuthorityGrantToProto(grant),
			ActiveGrants: []*metapb.RootCapsuleAuthorityGrant{metawire.RootCapsuleAuthorityGrantToProto(grant)},
		},
	}
	table := fscapsule.NewActiveAuthorities()
	manager, err := NewCapsuleAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, grant, got)
	require.Equal(t, 1, client.calls)
	require.Equal(t, rootproto.CapsuleAuthorityActAcquire, client.last.Kind)
	require.Equal(t, "holder-a", client.last.HolderID)
	require.Equal(t, now.Add(time.Minute).UnixNano(), client.last.ExpiresUnixNano)
	require.Equal(t, []fscapsule.AuthorityGrant{grant}, table.Snapshot())
}

func TestCapsuleAuthorityManagerAcquireUsesLocalHeldGrant(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimeCapsuleScope(1)
	grant := testRuntimeCapsuleGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	table := fscapsule.NewActiveAuthorities()
	require.NoError(t, table.Replace([]fscapsule.AuthorityGrant{grant}))
	client := &fakeCapsuleAuthorityClient{}
	manager, err := NewCapsuleAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, grant, got)
	require.Zero(t, client.calls)
}

func TestCapsuleAuthorityManagerAcquireHeldUpdatesMirror(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimeCapsuleScope(1)
	held := testRuntimeCapsuleGrant("holder-b/1", "holder-b", scope, now.Add(time.Minute))
	client := &fakeCapsuleAuthorityClient{
		resp: &coordpb.ApplyCapsuleAuthorityResponse{
			Status:       metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_HELD,
			ActiveGrants: []*metapb.RootCapsuleAuthorityGrant{metawire.RootCapsuleAuthorityGrantToProto(held)},
		},
	}
	table := fscapsule.NewActiveAuthorities()
	manager, err := NewCapsuleAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.False(t, owned)
	require.Equal(t, held, got)
	require.Equal(t, []fscapsule.AuthorityGrant{held}, table.Snapshot())
}

func TestCapsuleAuthorityManagerRetireAuthority(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimeCapsuleScope(1)
	grant := testRuntimeCapsuleGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	client := &fakeCapsuleAuthorityClient{
		resp: &coordpb.ApplyCapsuleAuthorityResponse{
			Status: metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_RETIRED,
		},
	}
	table := fscapsule.NewActiveAuthorities()
	require.NoError(t, table.Replace([]fscapsule.AuthorityGrant{grant}))
	manager, err := NewCapsuleAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	require.NoError(t, manager.Retire(context.Background(), grant))
	require.Equal(t, rootproto.CapsuleAuthorityActRetire, client.last.Kind)
	require.Equal(t, grant.GrantID, client.last.GrantID)
	require.Empty(t, table.Snapshot())
}

func TestCapsuleAuthorityManagerRejectsInvalidConfigAndResponses(t *testing.T) {
	_, err := NewCapsuleAuthorityManager(nil, fscapsule.NewActiveAuthorities(), "holder-a", time.Minute, nil)
	require.ErrorIs(t, err, errCapsuleAuthorityClientRequired)
	_, err = NewCapsuleAuthorityManager(&fakeCapsuleAuthorityClient{}, nil, "holder-a", time.Minute, nil)
	require.ErrorIs(t, err, errCapsuleAuthorityTableRequired)
	_, err = NewCapsuleAuthorityManager(&fakeCapsuleAuthorityClient{}, fscapsule.NewActiveAuthorities(), "", time.Minute, nil)
	require.ErrorIs(t, err, errCapsuleAuthorityHolderRequired)
	_, err = NewCapsuleAuthorityManager(&fakeCapsuleAuthorityClient{}, fscapsule.NewActiveAuthorities(), "holder-a", -time.Second, nil)
	require.ErrorIs(t, err, errCapsuleAuthorityTTLInvalid)

	manager, err := NewCapsuleAuthorityManager(&fakeCapsuleAuthorityClient{
		resp: &coordpb.ApplyCapsuleAuthorityResponse{Status: metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_GRANTED},
	}, fscapsule.NewActiveAuthorities(), "holder-a", time.Minute, func() time.Time { return time.Unix(10, 0) })
	require.NoError(t, err)
	_, _, err = manager.Acquire(context.Background(), testRuntimeCapsuleScope(1))
	require.ErrorIs(t, err, errCapsuleAuthorityInvalidResponse)
}

func TestCapsuleAuthorityManagerRetireRejectsForeignGrant(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimeCapsuleScope(1)
	grant := testRuntimeCapsuleGrant("holder-b/1", "holder-b", scope, now.Add(time.Minute))
	manager, err := NewCapsuleAuthorityManager(&fakeCapsuleAuthorityClient{}, fscapsule.NewActiveAuthorities(), "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	err = manager.Retire(context.Background(), grant)
	require.ErrorIs(t, err, errCapsuleAuthorityNotHeld)
	require.True(t, IsCapsuleAuthorityNotHeld(err))
}

func BenchmarkCapsuleAuthorityManagerAcquireLocalHeld(b *testing.B) {
	now := time.Unix(10, 0)
	scope := testRuntimeCapsuleScope(1)
	grant := testRuntimeCapsuleGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	table := fscapsule.NewActiveAuthorities()
	if err := table.Replace([]fscapsule.AuthorityGrant{grant}); err != nil {
		b.Fatal(err)
	}
	manager, err := NewCapsuleAuthorityManager(&fakeCapsuleAuthorityClient{}, table, "holder-a", time.Minute, func() time.Time { return now })
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

func testRuntimeCapsuleScope(bucket fsmeta.AffinityBucket) compile.AuthorityScope {
	return compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 7,
		Buckets:    []fsmeta.AffinityBucket{bucket},
		Parents:    []fsmeta.InodeID{99},
		Inodes:     []fsmeta.InodeID{100},
	}
}

func testRuntimeCapsuleGrant(id, holder string, scope compile.AuthorityScope, expires time.Time) fscapsule.AuthorityGrant {
	return fscapsule.AuthorityGrant{
		GrantID:         id,
		EpochID:         1,
		HolderID:        holder,
		Scope:           fscapsule.AuthorityScopeFromDelta(scope),
		ExpiresUnixNano: expires.UnixNano(),
	}
}
