// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

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

func TestServiceApplyVisibleAuthorityAcquire(t *testing.T) {
	now := time.Now()
	store := &fakeStorage{snapshot: rootview.Snapshot{}}
	svc := NewService(nil, nil, nil, store)
	svc.now = func() time.Time { return now }
	cmd := testVisibleAuthorityAcquireCommand("holder-a", 1, now)

	resp, err := svc.ApplyVisibleAuthority(context.Background(), &coordpb.ApplyVisibleAuthorityRequest{
		Command: metawire.RootVisibleAuthorityCommandToProto(cmd),
	})
	require.NoError(t, err)
	require.Equal(t, metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_GRANTED, resp.GetStatus())
	require.Equal(t, "holder-a/1", resp.GetGrant().GetGrantId())
	require.Len(t, resp.GetActiveGrants(), 1)
	require.Equal(t, cmd.Scope.MountKeyID, store.lastVisibleAuthorityCommand.Scope.MountKeyID)
	require.Equal(t, 1, store.applyVisibleAuthorityCalls)
}

func TestServiceApplyVisibleAuthorityHeld(t *testing.T) {
	now := time.Now()
	active := testServiceGatewayVisibleGrant()
	active.HolderID = "holder-b"
	active.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	store := &fakeStorage{snapshot: rootview.Snapshot{
		ActiveVisibleGrants:   []rootproto.VisibleAuthorityGrant{active},
		VisibleAuthorityEpoch: active.EpochID,
	}}
	svc := NewService(nil, nil, nil, store)
	cmd := testVisibleAuthorityAcquireCommand("holder-a", 1, now)

	resp, err := svc.ApplyVisibleAuthority(context.Background(), &coordpb.ApplyVisibleAuthorityRequest{
		Command: metawire.RootVisibleAuthorityCommandToProto(cmd),
	})
	require.NoError(t, err)
	require.Equal(t, metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_HELD, resp.GetStatus())
	require.Nil(t, resp.GetGrant())
	require.Len(t, resp.GetActiveGrants(), 1)
	require.Equal(t, active.GrantID, resp.GetActiveGrants()[0].GetGrantId())
}

func TestServiceApplyVisibleAuthorityRetire(t *testing.T) {
	now := time.Now()
	active := testServiceGatewayVisibleGrant()
	active.HolderID = "holder-a"
	active.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	store := &fakeStorage{snapshot: rootview.Snapshot{
		ActiveVisibleGrants:   []rootproto.VisibleAuthorityGrant{active},
		VisibleAuthorityEpoch: active.EpochID,
	}}
	svc := NewService(nil, nil, nil, store)
	cmd := rootproto.VisibleAuthorityCommand{
		Kind:        rootproto.VisibleAuthorityActRetire,
		HolderID:    active.HolderID,
		GrantID:     active.GrantID,
		NowUnixNano: now.UnixNano(),
	}

	resp, err := svc.ApplyVisibleAuthority(context.Background(), &coordpb.ApplyVisibleAuthorityRequest{
		Command: metawire.RootVisibleAuthorityCommandToProto(cmd),
	})
	require.NoError(t, err)
	require.Equal(t, metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_RETIRED, resp.GetStatus())
	require.Empty(t, resp.GetActiveGrants())
	require.Empty(t, store.snapshot.ActiveVisibleGrants)
}

func TestServiceApplyVisibleAuthorityRejectsInvalidOrUnavailable(t *testing.T) {
	svc := NewService(nil, nil, nil)
	_, err := svc.ApplyVisibleAuthority(context.Background(), nil)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	svc = NewService(nil, nil, nil, &fakeStorage{})
	_, err = svc.ApplyVisibleAuthority(context.Background(), nil)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func BenchmarkServiceApplyVisibleAuthorityHeld(b *testing.B) {
	now := time.Now()
	active := testServiceGatewayVisibleGrant()
	active.HolderID = "holder-b"
	active.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	store := &fakeStorage{snapshot: rootview.Snapshot{
		ActiveVisibleGrants:   []rootproto.VisibleAuthorityGrant{active},
		VisibleAuthorityEpoch: active.EpochID,
	}}
	svc := NewService(nil, nil, nil, store)
	req := &coordpb.ApplyVisibleAuthorityRequest{
		Command: metawire.RootVisibleAuthorityCommandToProto(testVisibleAuthorityAcquireCommand("holder-a", 1, now)),
	}

	b.ReportAllocs()
	for b.Loop() {
		resp, err := svc.ApplyVisibleAuthority(context.Background(), req)
		if err != nil {
			b.Fatal(err)
		}
		if resp.GetStatus() != metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_HELD {
			b.Fatal(resp.GetStatus())
		}
	}
}

func testVisibleAuthorityAcquireCommand(holderID string, bucket uint16, now time.Time) rootproto.VisibleAuthorityCommand {
	return rootproto.VisibleAuthorityCommand{
		Kind:            rootproto.VisibleAuthorityActAcquire,
		HolderID:        holderID,
		NowUnixNano:     now.UnixNano(),
		ExpiresUnixNano: now.Add(time.Hour).UnixNano(),
		Scope: rootproto.VisibleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{bucket},
		},
	}
}
