package server

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/rootview"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

func TestServiceListCapsuleAuthorityGrants(t *testing.T) {
	grant := testServiceGatewayCapsuleGrant()
	store := &fakeStorage{
		snapshot: rootview.Snapshot{
			ActiveCapsuleGrants:   []rootproto.CapsuleAuthorityGrant{grant},
			CapsuleAuthorityEpoch: grant.EpochID,
		},
	}
	svc := NewService(nil, nil, nil, store)

	resp, err := svc.ListCapsuleAuthorityGrants(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, resp.GetGrants(), 1)
	require.Equal(t, grant.GrantID, resp.GetGrants()[0].GetGrantId())
	require.Equal(t, 1, store.loadCalls)
}

func TestServiceListCapsuleAuthorityGrantsWithoutStorage(t *testing.T) {
	svc := NewService(nil, nil, nil)
	resp, err := svc.ListCapsuleAuthorityGrants(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, resp.GetGrants())
}

func testServiceGatewayCapsuleGrant() rootproto.CapsuleAuthorityGrant {
	return rootproto.CapsuleAuthorityGrant{
		GrantID:  "capsule-1",
		EpochID:  1,
		HolderID: "holder-a",
		Scope: rootproto.CapsuleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 7,
			Buckets:    []uint16{1},
		},
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
	}
}
