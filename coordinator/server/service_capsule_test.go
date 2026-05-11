package server

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServiceApplyCapsuleAuthorityAcquire(t *testing.T) {
	now := time.Now()
	store := &fakeStorage{snapshot: rootview.Snapshot{}}
	svc := NewService(nil, nil, nil, store)
	svc.now = func() time.Time { return now }
	cmd := testCapsuleAcquireCommand("holder-a", 1, now)

	resp, err := svc.ApplyCapsuleAuthority(context.Background(), &coordpb.ApplyCapsuleAuthorityRequest{
		Command: metawire.RootCapsuleAuthorityCommandToProto(cmd),
	})
	require.NoError(t, err)
	require.Equal(t, metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_GRANTED, resp.GetStatus())
	require.Equal(t, "holder-a/1", resp.GetGrant().GetGrantId())
	require.Len(t, resp.GetActiveGrants(), 1)
	require.Equal(t, cmd.Scope.MountKeyID, store.lastCapsuleCommand.Scope.MountKeyID)
	require.Equal(t, 1, store.applyCapsuleCalls)
}

func TestServiceApplyCapsuleAuthorityHeld(t *testing.T) {
	now := time.Now()
	active := testServiceGatewayCapsuleGrant()
	active.HolderID = "holder-b"
	active.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	store := &fakeStorage{snapshot: rootview.Snapshot{
		ActiveCapsuleGrants:   []rootproto.CapsuleAuthorityGrant{active},
		CapsuleAuthorityEpoch: active.EpochID,
	}}
	svc := NewService(nil, nil, nil, store)
	cmd := testCapsuleAcquireCommand("holder-a", 1, now)

	resp, err := svc.ApplyCapsuleAuthority(context.Background(), &coordpb.ApplyCapsuleAuthorityRequest{
		Command: metawire.RootCapsuleAuthorityCommandToProto(cmd),
	})
	require.NoError(t, err)
	require.Equal(t, metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_HELD, resp.GetStatus())
	require.Nil(t, resp.GetGrant())
	require.Len(t, resp.GetActiveGrants(), 1)
	require.Equal(t, active.GrantID, resp.GetActiveGrants()[0].GetGrantId())
}

func TestServiceApplyCapsuleAuthorityRetire(t *testing.T) {
	now := time.Now()
	active := testServiceGatewayCapsuleGrant()
	active.HolderID = "holder-a"
	active.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	store := &fakeStorage{snapshot: rootview.Snapshot{
		ActiveCapsuleGrants:   []rootproto.CapsuleAuthorityGrant{active},
		CapsuleAuthorityEpoch: active.EpochID,
	}}
	svc := NewService(nil, nil, nil, store)
	cmd := rootproto.CapsuleAuthorityCommand{
		Kind:        rootproto.CapsuleAuthorityActRetire,
		HolderID:    active.HolderID,
		GrantID:     active.GrantID,
		NowUnixNano: now.UnixNano(),
	}

	resp, err := svc.ApplyCapsuleAuthority(context.Background(), &coordpb.ApplyCapsuleAuthorityRequest{
		Command: metawire.RootCapsuleAuthorityCommandToProto(cmd),
	})
	require.NoError(t, err)
	require.Equal(t, metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_RETIRED, resp.GetStatus())
	require.Empty(t, resp.GetActiveGrants())
	require.Empty(t, store.snapshot.ActiveCapsuleGrants)
}

func TestServiceApplyCapsuleAuthorityRejectsInvalidOrUnavailable(t *testing.T) {
	svc := NewService(nil, nil, nil)
	_, err := svc.ApplyCapsuleAuthority(context.Background(), nil)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	svc = NewService(nil, nil, nil, &fakeStorage{})
	_, err = svc.ApplyCapsuleAuthority(context.Background(), nil)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func BenchmarkServiceApplyCapsuleAuthorityHeld(b *testing.B) {
	now := time.Now()
	active := testServiceGatewayCapsuleGrant()
	active.HolderID = "holder-b"
	active.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	store := &fakeStorage{snapshot: rootview.Snapshot{
		ActiveCapsuleGrants:   []rootproto.CapsuleAuthorityGrant{active},
		CapsuleAuthorityEpoch: active.EpochID,
	}}
	svc := NewService(nil, nil, nil, store)
	req := &coordpb.ApplyCapsuleAuthorityRequest{
		Command: metawire.RootCapsuleAuthorityCommandToProto(testCapsuleAcquireCommand("holder-a", 1, now)),
	}

	b.ReportAllocs()
	for b.Loop() {
		resp, err := svc.ApplyCapsuleAuthority(context.Background(), req)
		if err != nil {
			b.Fatal(err)
		}
		if resp.GetStatus() != metapb.RootCapsuleAuthorityApplyStatus_ROOT_CAPSULE_AUTHORITY_APPLY_STATUS_HELD {
			b.Fatal(resp.GetStatus())
		}
	}
}

func testCapsuleAcquireCommand(holderID string, bucket uint16, now time.Time) rootproto.CapsuleAuthorityCommand {
	return rootproto.CapsuleAuthorityCommand{
		Kind:            rootproto.CapsuleAuthorityActAcquire,
		HolderID:        holderID,
		NowUnixNano:     now.UnixNano(),
		ExpiresUnixNano: now.Add(time.Hour).UnixNano(),
		Scope: rootproto.CapsuleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{bucket},
		},
	}
}
