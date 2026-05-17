// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmeta

import (
	"context"
	"testing"
	"time"

	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestTryBuildWitnessConnectionsRequiresConfiguredStores(t *testing.T) {
	lister := &fakePerasStoreLister{stores: []*coordpb.StoreInfo{
		{StoreId: 1, ClientAddr: "127.0.0.1:1", State: coordpb.StoreState_STORE_STATE_UP},
		{StoreId: 2, ClientAddr: "127.0.0.1:2", State: coordpb.StoreState_STORE_STATE_UP},
	}}
	allowed := map[uint64]struct{}{1: {}, 2: {}, 3: {}}

	conns, complete, err := tryBuildWitnessConnections(context.Background(), lister, testSegmentWitnessDialOptions(), allowed)
	require.NoError(t, err)
	require.False(t, complete)
	require.NotNil(t, conns)
	require.NoError(t, conns.Close())
}

func TestTryBuildWitnessConnectionsAcceptsConfiguredStoresOnceAllUp(t *testing.T) {
	lister := &fakePerasStoreLister{stores: []*coordpb.StoreInfo{
		{StoreId: 1, ClientAddr: "127.0.0.1:1", State: coordpb.StoreState_STORE_STATE_UP},
		{StoreId: 2, ClientAddr: "127.0.0.1:2", State: coordpb.StoreState_STORE_STATE_UP},
		{StoreId: 3, ClientAddr: "127.0.0.1:3", State: coordpb.StoreState_STORE_STATE_UP},
		{StoreId: 4, ClientAddr: "127.0.0.1:4", State: coordpb.StoreState_STORE_STATE_UP},
	}}
	allowed := map[uint64]struct{}{1: {}, 2: {}, 3: {}}

	conns, complete, err := tryBuildWitnessConnections(context.Background(), lister, testSegmentWitnessDialOptions(), allowed)
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, conns.witnesses, 3)
	require.Equal(t, "store-1", conns.witnesses[0].ID())
	require.Equal(t, "store-2", conns.witnesses[1].ID())
	require.Equal(t, "store-3", conns.witnesses[2].ID())
	require.NoError(t, conns.Close())
}

func TestTryBuildWitnessConnectionsAcceptsAllUpStoresByDefault(t *testing.T) {
	lister := &fakePerasStoreLister{stores: []*coordpb.StoreInfo{
		{StoreId: 2, ClientAddr: "127.0.0.1:2", State: coordpb.StoreState_STORE_STATE_UP},
		{StoreId: 1, ClientAddr: "127.0.0.1:1", State: coordpb.StoreState_STORE_STATE_UP},
		{StoreId: 3, ClientAddr: "127.0.0.1:3", State: coordpb.StoreState_STORE_STATE_UP},
		{StoreId: 4, ClientAddr: "127.0.0.1:4", State: coordpb.StoreState_STORE_STATE_TOMBSTONE},
	}}

	conns, complete, err := tryBuildWitnessConnections(context.Background(), lister, testSegmentWitnessDialOptions(), nil)
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, conns.witnesses, 3)
	require.Equal(t, "store-1", conns.witnesses[0].ID())
	require.Equal(t, "store-2", conns.witnesses[1].ID())
	require.Equal(t, "store-3", conns.witnesses[2].ID())
	require.NoError(t, conns.Close())
}

func TestSegmentWitnessDiscoveryWaitsForStableDefaultStoreSet(t *testing.T) {
	now := time.Unix(100, 0)

	ids, since, complete := witnessDiscoverySettled(nil, time.Time{}, []string{"store-1"}, now)
	require.False(t, complete)
	require.Equal(t, []string{"store-1"}, ids)
	require.Equal(t, now, since)

	ids, since, complete = witnessDiscoverySettled(ids, since, []string{"store-1", "store-2", "store-3"}, now.Add(segmentWitnessDiscoveryBackoff))
	require.False(t, complete)
	require.Equal(t, []string{"store-1", "store-2", "store-3"}, ids)
	require.Equal(t, now.Add(segmentWitnessDiscoveryBackoff), since)

	ids, _, complete = witnessDiscoverySettled(ids, since, []string{"store-1", "store-2", "store-3"}, since.Add(segmentWitnessDiscoverySettle))
	require.True(t, complete)
	require.Equal(t, []string{"store-1", "store-2", "store-3"}, ids)
}

func TestSegmentWitnessStoreSelectedIgnoresUnavailableStores(t *testing.T) {
	allowed := map[uint64]struct{}{1: {}}
	require.False(t, witnessStoreSelected(nil, allowed))
	require.False(t, witnessStoreSelected(&coordpb.StoreInfo{StoreId: 1, State: coordpb.StoreState_STORE_STATE_UP}, allowed))
	require.False(t, witnessStoreSelected(&coordpb.StoreInfo{StoreId: 1, ClientAddr: "127.0.0.1:1", State: coordpb.StoreState_STORE_STATE_TOMBSTONE}, allowed))
	require.False(t, witnessStoreSelected(&coordpb.StoreInfo{StoreId: 2, ClientAddr: "127.0.0.1:2", State: coordpb.StoreState_STORE_STATE_UP}, allowed))
	require.True(t, witnessStoreSelected(&coordpb.StoreInfo{StoreId: 1, ClientAddr: "127.0.0.1:1", State: coordpb.StoreState_STORE_STATE_UP}, allowed))
}

type fakePerasStoreLister struct {
	stores []*coordpb.StoreInfo
}

func (f *fakePerasStoreLister) ListStores(context.Context, *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error) {
	return &coordpb.ListStoresResponse{Stores: f.stores}, nil
}

func testSegmentWitnessDialOptions() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
}
