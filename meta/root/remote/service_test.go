package remote_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
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

func TestClientCampaignCoordinatorLease(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	client := openBufconnClient(t, backend)

	leaseState, err := client.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{
		Kind:             rootproto.CoordinatorLeaseCommandIssue,
		HolderID:         "c1",
		ExpiresUnixNano:  1_000,
		NowUnixNano:      100,
		HandoffFrontiers: controlplane.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 30),
	})
	require.NoError(t, err)
	lease := leaseState.Lease
	require.Equal(t, "c1", lease.HolderID)
	require.Equal(t, uint64(1), lease.CertGeneration)
	require.Equal(t, uint32(rootproto.CoordinatorDutyMaskDefault), lease.DutyMask)
	require.NotEqual(t, rootstate.Cursor{}, lease.IssuedCursor)

	heldState, err := client.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{
		Kind:             rootproto.CoordinatorLeaseCommandIssue,
		HolderID:         "c2",
		ExpiresUnixNano:  1_500,
		NowUnixNano:      200,
		HandoffFrontiers: controlplane.Frontiers(rootstate.State{IDFence: 30, TSOFence: 40}, 30),
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, rootstate.ErrCoordinatorLeaseHeld))
	held := heldState.Lease
	require.Equal(t, "c1", held.HolderID)

	leaseState, err = client.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{
		Kind:             rootproto.CoordinatorLeaseCommandIssue,
		HolderID:         "c2",
		ExpiresUnixNano:  2_000,
		NowUnixNano:      1_001,
		HandoffFrontiers: controlplane.Frontiers(rootstate.State{IDFence: 30, TSOFence: 40}, 30),
	})
	require.NoError(t, err)
	lease = leaseState.Lease
	require.Equal(t, "c2", lease.HolderID)
	require.Equal(t, uint64(2), lease.CertGeneration)
}

func TestClientReleaseCoordinatorLease(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	client := openBufconnClient(t, backend)

	_, err = client.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{Kind: rootproto.CoordinatorLeaseCommandIssue, HolderID: "c1", ExpiresUnixNano: 1_000, NowUnixNano: 100, HandoffFrontiers: controlplane.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 30)})
	require.NoError(t, err)

	leaseState, err := client.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{Kind: rootproto.CoordinatorLeaseCommandRelease, HolderID: "c1", NowUnixNano: 200, HandoffFrontiers: controlplane.Frontiers(rootstate.State{IDFence: 30, TSOFence: 40}, 0)})
	require.NoError(t, err)
	lease := leaseState.Lease
	require.Equal(t, "c1", lease.HolderID)
	require.Equal(t, uint64(1), lease.CertGeneration)
	require.Equal(t, int64(200), lease.ExpiresUnixNano)
}

func TestClientSealCoordinatorLease(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	client := openBufconnClient(t, backend)

	_, err = client.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{Kind: rootproto.CoordinatorLeaseCommandIssue, HolderID: "c1", ExpiresUnixNano: 1_000, NowUnixNano: 100, HandoffFrontiers: controlplane.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 30)})
	require.NoError(t, err)

	sealState, err := client.ApplyCoordinatorClosure(rootproto.CoordinatorClosureCommand{Kind: rootproto.CoordinatorClosureCommandSeal, HolderID: "c1", NowUnixNano: 200, Frontiers: controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 56)})
	require.NoError(t, err)
	seal := sealState.Seal
	require.Equal(t, "c1", seal.HolderID)
	require.Equal(t, uint64(1), seal.CertGeneration)
	require.Equal(t, uint32(rootproto.CoordinatorDutyMaskDefault), seal.DutyMask)
	require.Equal(t, uint64(12), seal.Frontiers.Frontier(rootproto.CoordinatorDutyAllocID))
	require.Equal(t, uint64(34), seal.Frontiers.Frontier(rootproto.CoordinatorDutyTSO))
	require.Equal(t, uint64(56), seal.Frontiers.Frontier(rootproto.CoordinatorDutyGetRegionByKey))
	require.NotEqual(t, rootstate.Cursor{}, seal.SealedAtCursor)
}

func TestClientConfirmCoordinatorClosure(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	desc := testDescriptor(1, []byte("a"), []byte("z"))
	desc.RootEpoch = 56
	_, err = backend.Append(rootevent.RegionDescriptorPublished(desc))
	require.NoError(t, err)
	client := openBufconnClient(t, backend)

	_, err = client.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{Kind: rootproto.CoordinatorLeaseCommandIssue, HolderID: "c1", ExpiresUnixNano: 1_000, NowUnixNano: 100, HandoffFrontiers: controlplane.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 56)})
	require.NoError(t, err)
	sealState, err := client.ApplyCoordinatorClosure(rootproto.CoordinatorClosureCommand{Kind: rootproto.CoordinatorClosureCommandSeal, HolderID: "c1", NowUnixNano: 200, Frontiers: controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 56)})
	require.NoError(t, err)
	seal := sealState.Seal
	leaseState, err := client.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{Kind: rootproto.CoordinatorLeaseCommandIssue, HolderID: "c1", ExpiresUnixNano: 1_200, NowUnixNano: 250, PredecessorDigest: rootstate.CoordinatorSealDigest(seal), HandoffFrontiers: controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 56)})
	require.NoError(t, err)
	lease := leaseState.Lease

	closureState, err := client.ApplyCoordinatorClosure(rootproto.CoordinatorClosureCommand{Kind: rootproto.CoordinatorClosureCommandConfirm, HolderID: "c1", NowUnixNano: 260})
	require.NoError(t, err)
	audit := closureState.Closure
	require.Equal(t, "c1", audit.HolderID)
	require.Equal(t, seal.CertGeneration, audit.SealGeneration)
	require.Equal(t, lease.CertGeneration, audit.SuccessorGeneration)
	require.Equal(t, rootstate.CoordinatorSealDigest(seal), audit.SealDigest)
	require.NotEqual(t, rootstate.Cursor{}, audit.ConfirmedAtCursor)
}

func TestClientReattachCoordinatorClosure(t *testing.T) {
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	desc := testDescriptor(1, []byte("a"), []byte("z"))
	desc.RootEpoch = 56
	_, err = backend.Append(rootevent.RegionDescriptorPublished(desc))
	require.NoError(t, err)
	client := openBufconnClient(t, backend)

	_, err = client.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{Kind: rootproto.CoordinatorLeaseCommandIssue, HolderID: "c1", ExpiresUnixNano: 1_000, NowUnixNano: 100, HandoffFrontiers: controlplane.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 56)})
	require.NoError(t, err)
	sealState, err := client.ApplyCoordinatorClosure(rootproto.CoordinatorClosureCommand{Kind: rootproto.CoordinatorClosureCommandSeal, HolderID: "c1", NowUnixNano: 200, Frontiers: controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 56)})
	require.NoError(t, err)
	seal := sealState.Seal
	leaseState, err := client.ApplyCoordinatorLease(rootproto.CoordinatorLeaseCommand{Kind: rootproto.CoordinatorLeaseCommandIssue, HolderID: "c1", ExpiresUnixNano: 1_200, NowUnixNano: 250, PredecessorDigest: rootstate.CoordinatorSealDigest(seal), HandoffFrontiers: controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 56)})
	require.NoError(t, err)
	lease := leaseState.Lease

	_, err = client.ApplyCoordinatorClosure(rootproto.CoordinatorClosureCommand{Kind: rootproto.CoordinatorClosureCommandReattach, HolderID: "c1", NowUnixNano: 255})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	auditState, err := client.ApplyCoordinatorClosure(rootproto.CoordinatorClosureCommand{Kind: rootproto.CoordinatorClosureCommandConfirm, HolderID: "c1", NowUnixNano: 260})
	require.NoError(t, err)
	closeState, err := client.ApplyCoordinatorClosure(rootproto.CoordinatorClosureCommand{Kind: rootproto.CoordinatorClosureCommandClose, HolderID: "c1", NowUnixNano: 265})
	require.NoError(t, err)
	reattachState, err := client.ApplyCoordinatorClosure(rootproto.CoordinatorClosureCommand{Kind: rootproto.CoordinatorClosureCommandReattach, HolderID: "c1", NowUnixNano: 270})
	require.NoError(t, err)
	audit := auditState.Closure
	closeRecord := closeState.Closure
	reattach := reattachState.Closure
	require.Equal(t, "c1", reattach.HolderID)
	require.Equal(t, closeRecord.SuccessorGeneration, reattach.SuccessorGeneration)
	require.Equal(t, closeRecord.SealGeneration, reattach.SealGeneration)
	require.Equal(t, closeRecord.SealDigest, reattach.SealDigest)
	require.NotEqual(t, rootstate.Cursor{}, reattach.ReattachedAtCursor)
	require.Equal(t, lease.CertGeneration, closeRecord.SuccessorGeneration)
	require.Equal(t, audit.SealGeneration, closeRecord.SealGeneration)
	require.Equal(t, audit.SealDigest, closeRecord.SealDigest)
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
