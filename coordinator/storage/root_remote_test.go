package storage

import (
	"context"
	"errors"
	"net"
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootremote "github.com/feichai0017/NoKV/meta/root/remote"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestOpenRootRemoteStoreLoadsSnapshot(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	_, err = backend.Append(rootevent.RegionBootstrapped(remoteTestDescriptor(11)))
	require.NoError(t, err)

	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	server := grpc.NewServer()
	metapb.RegisterMetadataRootServer(server, rootremote.NewService(backend))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	t.Cleanup(func() { _ = listener.Close() })

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	store, err := OpenRootRemoteStore(RemoteRootConfig{
		Targets: map[uint64]string{1: "passthrough:///bufnet"},
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	snapshot, err := store.Load()
	require.NoError(t, err)
	require.Contains(t, snapshot.Descriptors, uint64(11))
}

func TestOpenRootRemoteStoreDoesNotGateWritesOnPreferredEndpointLeadership(t *testing.T) {
	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	server := grpc.NewServer()
	metapb.RegisterMetadataRootServer(server, rootremote.NewService(remoteFollowerBackend{leaderID: 2}))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)
	t.Cleanup(func() { _ = listener.Close() })

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	store, err := OpenRootRemoteStore(RemoteRootConfig{
		Targets: map[uint64]string{1: "passthrough:///bufnet"},
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	require.True(t, store.IsLeader())
	require.Equal(t, uint64(2), store.LeaderID())
	require.Error(t, store.SaveAllocatorState(10, 20))
}

func TestRemoteRootConfigValidate(t *testing.T) {
	require.ErrorContains(t, (RemoteRootConfig{}).Validate(), "requires at least one target")
	require.ErrorContains(t, (RemoteRootConfig{Targets: map[uint64]string{0: "127.0.0.1:1"}}).Validate(), "must be > 0")
	require.ErrorContains(t, (RemoteRootConfig{Targets: map[uint64]string{1: " "}}).Validate(), "missing remote root address")
	require.NoError(t, (RemoteRootConfig{Targets: map[uint64]string{1: "127.0.0.1:1"}}).Validate())
}

func remoteTestDescriptor(id uint64) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID: id,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
		State: metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}

type remoteFollowerBackend struct {
	leaderID uint64
}

func (f remoteFollowerBackend) Snapshot() (rootstate.Snapshot, error) {
	return rootstate.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
}

func (f remoteFollowerBackend) Append(...rootevent.Event) (rootstate.CommitInfo, error) {
	return rootstate.CommitInfo{}, errors.New("unexpected append")
}

func (f remoteFollowerBackend) FenceAllocator(rootstate.AllocatorKind, uint64) (uint64, error) {
	return 0, errors.New("unexpected fence")
}

func (f remoteFollowerBackend) IsLeader() bool {
	return false
}

func (f remoteFollowerBackend) LeaderID() uint64 {
	return f.leaderID
}
