package raftstore

import (
	"testing"
	"time"

	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestAPIConstructorsAndOptions(t *testing.T) {
	router := NewRouter()
	require.NotNil(t, router)

	store1 := NewStore(router)
	require.NotNil(t, store1)
	store.UnregisterStore(store1)

	store2 := NewStoreWithConfig(StoreConfig{})
	require.NotNil(t, store2)
	store.UnregisterStore(store2)

	opts := []GRPCOption{
		WithGRPCDialTimeout(2 * time.Second),
		WithGRPCSendTimeout(500 * time.Millisecond),
		WithGRPCRetry(3, time.Second),
		WithGRPCServerCredentials(insecure.NewCredentials()),
		WithGRPCClientCredentials(insecure.NewCredentials()),
		WithGRPCServerRegistrar(func(reg grpc.ServiceRegistrar) {
			require.NotNil(t, reg)
		}),
	}
	for i, opt := range opts {
		require.NotNilf(t, opt, "option %d should not be nil", i)
	}
}
