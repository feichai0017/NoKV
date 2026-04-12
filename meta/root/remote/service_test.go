package remote_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootremote "github.com/feichai0017/NoKV/meta/root/remote"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

func TestClientRoundTripThroughCoordinatorRootStore(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	client := openBufconnClient(t, backend)

	store, err := coordstorage.OpenRootStore(client)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	desc := testDescriptor(11, []byte("a"), []byte("z"))
	require.NoError(t, store.AppendRootEvent(rootevent.RegionBootstrapped(desc)))
	require.NoError(t, store.SaveAllocatorState(100, 200))

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(100), snapshot.Allocator.IDCurrent)
	require.Equal(t, uint64(200), snapshot.Allocator.TSCurrent)
	got, ok := snapshot.Descriptors[11]
	require.True(t, ok)
	require.True(t, desc.Equal(got))

	subscription := store.SubscribeTail(rootstorage.TailToken{})
	require.NotNil(t, subscription)
	advance, err := subscription.Next(context.Background(), time.Millisecond)
	require.NoError(t, err)
	require.True(t, advance.Advanced())
	require.True(t, rootstate.CursorAfter(advance.Token.Cursor, rootstate.Cursor{}))
}

func TestServiceRejectsFollowerWritesWithLeaderHint(t *testing.T) {
	backend := &followerBackend{leaderID: 7}
	client := openBufconnClient(t, backend)

	_, err := client.Append(rootevent.IDAllocatorFenced(10))
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.True(t, strings.Contains(err.Error(), "leader_id=7"))

	_, err = client.FenceAllocator(rootstate.AllocatorKindID, 10)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.True(t, strings.Contains(err.Error(), "leader_id=7"))
	require.False(t, client.IsLeader())
	require.Equal(t, uint64(7), client.LeaderID())
}

func TestClientRetriesWriteOnLeaderHint(t *testing.T) {
	followerListener := bufconn.Listen(bufSize)
	leaderListener := bufconn.Listen(bufSize)
	t.Cleanup(func() { require.NoError(t, followerListener.Close()) })
	t.Cleanup(func() { require.NoError(t, leaderListener.Close()) })

	followerServer := grpc.NewServer()
	rootremote.Register(followerServer, &followerBackend{leaderID: 2})
	go func() { _ = followerServer.Serve(followerListener) }()
	t.Cleanup(followerServer.GracefulStop)

	leaderBackend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	leaderServer := grpc.NewServer()
	rootremote.Register(leaderServer, leaderBackend)
	go func() { _ = leaderServer.Serve(leaderListener) }()
	t.Cleanup(leaderServer.GracefulStop)

	dialer := func(_ context.Context, target string) (net.Conn, error) {
		switch target {
		case "root-1":
			return followerListener.Dial()
		case "root-2":
			return leaderListener.Dial()
		default:
			return nil, errors.New("unknown target")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := rootremote.DialCluster(ctx, map[uint64]string{
		1: "passthrough:///root-1",
		2: "passthrough:///root-2",
	},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	desc := testDescriptor(21, []byte("a"), []byte("z"))
	_, err = client.Append(rootevent.RegionBootstrapped(desc))
	require.NoError(t, err)
	current, err := client.FenceAllocator(rootstate.AllocatorKindID, 77)
	require.NoError(t, err)
	require.Equal(t, uint64(77), current)

	snapshot, err := client.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(77), snapshot.State.IDFence)
	got, ok := snapshot.Descriptors[21]
	require.True(t, ok)
	require.True(t, desc.Equal(got))
}

func TestServiceRejectsInvalidAllocatorKind(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	listener := bufconn.Listen(bufSize)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	server := grpc.NewServer()
	rootremote.Register(server, backend)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)

	conn := dialBufconn(t, listener)
	defer func() { require.NoError(t, conn.Close()) }()
	rpc := metapb.NewMetadataRootClient(conn)
	_, err = rpc.FenceAllocator(context.Background(), &metapb.MetadataRootFenceAllocatorRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func openBufconnClient(t *testing.T, backend rootremote.Backend) *rootremote.Client {
	t.Helper()
	listener := bufconn.Listen(bufSize)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	server := grpc.NewServer()
	rootremote.Register(server, backend)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)

	conn := dialBufconn(t, listener)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return rootremote.NewClient(conn)
}

func dialBufconn(t *testing.T, listener *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	return conn
}

func testDescriptor(regionID uint64, start, end []byte) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID: regionID,
		StartKey: append([]byte(nil), start...),
		EndKey:   append([]byte(nil), end...),
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []metaregion.Peer{{StoreID: 1, PeerID: 101}},
		State: metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}

type followerBackend struct {
	leaderID uint64
}

func (f *followerBackend) Snapshot() (rootstate.Snapshot, error) {
	return rootstate.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
}

func (f *followerBackend) Append(...rootevent.Event) (rootstate.CommitInfo, error) {
	return rootstate.CommitInfo{}, nil
}

func (f *followerBackend) FenceAllocator(rootstate.AllocatorKind, uint64) (uint64, error) {
	return 0, nil
}

func (f *followerBackend) IsLeader() bool { return false }

func (f *followerBackend) LeaderID() uint64 { return f.leaderID }
