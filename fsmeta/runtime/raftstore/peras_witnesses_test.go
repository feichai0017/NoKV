package raftstore

import (
	"context"
	"testing"

	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestTryBuildRemotePerasWitnessesRequiresConfiguredStores(t *testing.T) {
	lister := &fakePerasStoreLister{stores: []*coordpb.StoreInfo{
		{StoreId: 1, ClientAddr: "127.0.0.1:1", State: coordpb.StoreState_STORE_STATE_UP},
		{StoreId: 2, ClientAddr: "127.0.0.1:2", State: coordpb.StoreState_STORE_STATE_UP},
	}}
	allowed := map[uint64]struct{}{1: {}, 2: {}, 3: {}}

	conns, complete, err := tryBuildRemotePerasWitnesses(context.Background(), lister, testPerasWitnessDialOptions(), allowed)
	require.NoError(t, err)
	require.False(t, complete)
	require.NotNil(t, conns)
	require.NoError(t, conns.Close())
}

func TestTryBuildRemotePerasWitnessesAcceptsConfiguredStoresOnceAllUp(t *testing.T) {
	lister := &fakePerasStoreLister{stores: []*coordpb.StoreInfo{
		{StoreId: 1, ClientAddr: "127.0.0.1:1", State: coordpb.StoreState_STORE_STATE_UP},
		{StoreId: 2, ClientAddr: "127.0.0.1:2", State: coordpb.StoreState_STORE_STATE_UP},
		{StoreId: 3, ClientAddr: "127.0.0.1:3", State: coordpb.StoreState_STORE_STATE_UP},
		{StoreId: 4, ClientAddr: "127.0.0.1:4", State: coordpb.StoreState_STORE_STATE_UP},
	}}
	allowed := map[uint64]struct{}{1: {}, 2: {}, 3: {}}

	conns, complete, err := tryBuildRemotePerasWitnesses(context.Background(), lister, testPerasWitnessDialOptions(), allowed)
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, conns.witnesses, 3)
	require.Equal(t, "store-1", conns.witnesses[0].ID())
	require.Equal(t, "store-2", conns.witnesses[1].ID())
	require.Equal(t, "store-3", conns.witnesses[2].ID())
	require.NoError(t, conns.Close())
}

func TestPerasWitnessStoreSelectedIgnoresUnavailableStores(t *testing.T) {
	allowed := map[uint64]struct{}{1: {}}
	require.False(t, perasWitnessStoreSelected(nil, allowed))
	require.False(t, perasWitnessStoreSelected(&coordpb.StoreInfo{StoreId: 1, State: coordpb.StoreState_STORE_STATE_UP}, allowed))
	require.False(t, perasWitnessStoreSelected(&coordpb.StoreInfo{StoreId: 1, ClientAddr: "127.0.0.1:1", State: coordpb.StoreState_STORE_STATE_TOMBSTONE}, allowed))
	require.False(t, perasWitnessStoreSelected(&coordpb.StoreInfo{StoreId: 2, ClientAddr: "127.0.0.1:2", State: coordpb.StoreState_STORE_STATE_UP}, allowed))
	require.True(t, perasWitnessStoreSelected(&coordpb.StoreInfo{StoreId: 1, ClientAddr: "127.0.0.1:1", State: coordpb.StoreState_STORE_STATE_UP}, allowed))
}

type fakePerasStoreLister struct {
	stores []*coordpb.StoreInfo
}

func (f *fakePerasStoreLister) ListStores(context.Context, *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error) {
	return &coordpb.ListStoresResponse{Stores: f.stores}, nil
}

func testPerasWitnessDialOptions() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
}
