package server

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

func TestServiceListPerasAuthorityGrants(t *testing.T) {
	grant := testServiceGatewayPerasGrant()
	store := &fakeStorage{
		snapshot: rootview.Snapshot{
			ActivePerasGrants:   []rootproto.PerasAuthorityGrant{grant},
			PerasAuthorityEpoch: grant.EpochID,
		},
	}
	svc := NewService(nil, nil, nil, store)

	resp, err := svc.ListPerasAuthorityGrants(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, resp.GetGrants(), 1)
	require.Equal(t, grant.GrantID, resp.GetGrants()[0].GetGrantId())
	require.Equal(t, 1, store.loadCalls)
}

func TestServiceListPerasAuthorityGrantsWithoutStorage(t *testing.T) {
	svc := NewService(nil, nil, nil)
	resp, err := svc.ListPerasAuthorityGrants(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, resp.GetGrants())
}

func TestServiceListPerasAuthoritySeals(t *testing.T) {
	grant := testServiceGatewayPerasGrant()
	seal := testServiceGatewayPerasSeal(grant)
	store := &fakeStorage{
		snapshot: rootview.Snapshot{
			PerasAuthoritySeals: []rootproto.PerasAuthoritySeal{seal},
		},
	}
	svc := NewService(nil, nil, nil, store)

	resp, err := svc.ListPerasAuthoritySeals(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, resp.GetSeals(), 1)
	require.Equal(t, seal.GrantID, resp.GetSeals()[0].GetGrantId())
	require.Equal(t, seal.InstallIndex, resp.GetSeals()[0].GetInstallIndex())
	require.Equal(t, 1, store.loadCalls)
}

func TestServiceListPerasAuthoritySealsWithoutStorage(t *testing.T) {
	svc := NewService(nil, nil, nil)
	resp, err := svc.ListPerasAuthoritySeals(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, resp.GetSeals())
}

func testServiceGatewayPerasGrant() rootproto.PerasAuthorityGrant {
	return rootproto.PerasAuthorityGrant{
		GrantID:  "peras-1",
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.PerasAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{1},
		},
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
	}
}

func testServiceGatewayPerasSeal(grant rootproto.PerasAuthorityGrant) rootproto.PerasAuthoritySeal {
	return rootproto.PerasAuthoritySeal{
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
