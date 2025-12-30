package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore"
)

func openTestDB(t *testing.T) *NoKV.DB {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db := NoKV.Open(opt)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestServerStartsTinyKvService(t *testing.T) {
	db := openTestDB(t)
	srv, err := raftstore.NewServer(raftstore.ServerConfig{
		DB: db,
		Store: raftstore.StoreConfig{
			StoreID: 1,
		},
		TransportAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	addr := srv.Addr()
	require.NotEmpty(t, addr)
	require.NotNil(t, srv.Store())
	require.NotNil(t, srv.Transport())
	require.NotNil(t, srv.Service())

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewTinyKvClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = client.KvGet(ctx, &pb.KvGetRequest{})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}
