// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

func TestServiceListVisibleAuthorityGrants(t *testing.T) {
	grant := testServiceGatewayVisibleGrant()
	store := &fakeStorage{
		snapshot: rootview.Snapshot{
			ActiveVisibleGrants:   []rootproto.VisibleAuthorityGrant{grant},
			VisibleAuthorityEpoch: grant.EpochID,
		},
	}
	svc := NewService(nil, nil, nil, store)

	resp, err := svc.ListVisibleAuthorityGrants(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, resp.GetGrants(), 1)
	require.Equal(t, grant.GrantID, resp.GetGrants()[0].GetGrantId())
	require.Equal(t, 1, store.loadCalls)
}

func TestServiceListVisibleAuthorityGrantsWithoutStorage(t *testing.T) {
	svc := NewService(nil, nil, nil)
	resp, err := svc.ListVisibleAuthorityGrants(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, resp.GetGrants())
}

func TestServiceListVisibleAuthoritySeals(t *testing.T) {
	grant := testServiceGatewayVisibleGrant()
	seal := testServiceGatewayVisibleSeal(grant)
	store := &fakeStorage{
		snapshot: rootview.Snapshot{
			VisibleAuthoritySeals: []rootproto.VisibleAuthoritySeal{seal},
		},
	}
	svc := NewService(nil, nil, nil, store)

	resp, err := svc.ListVisibleAuthoritySeals(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, resp.GetSeals(), 1)
	require.Equal(t, seal.GrantID, resp.GetSeals()[0].GetGrantId())
	require.Equal(t, seal.InstallIndex, resp.GetSeals()[0].GetInstallIndex())
	require.Equal(t, 1, store.loadCalls)
}

func TestServiceListVisibleAuthoritySealsWithoutStorage(t *testing.T) {
	svc := NewService(nil, nil, nil)
	resp, err := svc.ListVisibleAuthoritySeals(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, resp.GetSeals())
}

func testServiceGatewayVisibleGrant() rootproto.VisibleAuthorityGrant {
	return rootproto.VisibleAuthorityGrant{
		GrantID:  "visible-1",
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.VisibleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{1},
		},
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
	}
}

func testServiceGatewayVisibleSeal(grant rootproto.VisibleAuthorityGrant) rootproto.VisibleAuthoritySeal {
	return rootproto.VisibleAuthoritySeal{
		GrantID:              grant.GrantID,
		EpochID:              grant.EpochID,
		HolderID:             grant.HolderID,
		Scope:                grant.Scope,
		SegmentRoot:          [32]byte{1},
		SegmentPayloadDigest: [32]byte{2},
		OperationCount:       7,
		EntryCount:           8,
		SealedUnixNano:       time.Now().UnixNano(),
		InstallRegionID:      10,
		InstallTerm:          11,
		InstallIndex:         12,
		InstallVersion:       13,
	}
}
