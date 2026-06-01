// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"

	"github.com/feichai0017/NoKV/coordinator/rootview"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/tso"
	"github.com/feichai0017/NoKV/meta/topology"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func notLeaderErrorForTest(leaderID uint64) error {
	metadata := map[string]string{coordinatorReasonMetadata: reasonNotLeader}
	if leaderID != 0 {
		metadata[leaderIDMetadata] = fmt.Sprintf("%d", leaderID)
	}
	return nokverrors.RPCStatusError(nokverrors.KindNotLeader, codes.FailedPrecondition, "coordinator test not leader", metadata)
}

func grantNotHeldErrorForTest() error {
	return nokverrors.RPCStatusError(nokverrors.KindNotLeader, codes.FailedPrecondition, "coordinator test grant not held", map[string]string{
		coordinatorReasonMetadata: reasonGrantNotHeld,
	})
}

func TestNewGRPCClientEmptyAddress(t *testing.T) {
	cli, err := NewGRPCClient(context.Background(), "")
	require.Error(t, err)
	require.Nil(t, cli)
}

func TestGRPCClientCloseIsIdempotent(t *testing.T) {
	conn, err := grpc.NewClient("passthrough:///127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	cli := &GRPCClient{endpoints: []grpcEndpoint{{conn: conn}}}
	require.NoError(t, cli.Close())
	require.NoError(t, cli.Close())
}

func TestGRPCClientRoundTrip(t *testing.T) {
	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	t.Cleanup(func() {
		_ = listener.Close()
	})

	svc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), newClientRootStorage(true))
	svc.ConfigureAuthorityGrant("c1", time.Hour, 30*time.Minute)
	grpcServer := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(grpcServer, svc)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.GracefulStop)

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := NewGRPCClient(ctx, "passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	joinResp, err := cli.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.StoreJoined(1)),
	})
	require.NoError(t, err)
	require.True(t, joinResp.GetAccepted())

	mountResp, err := cli.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.MountRegistered("vol", 1, 1, 1)),
	})
	require.NoError(t, err)
	require.True(t, mountResp.GetAccepted())
	getMountResp, err := cli.GetMount(context.Background(), &coordpb.GetMountRequest{MountId: "vol"})
	require.NoError(t, err)
	require.False(t, getMountResp.GetNotFound())
	require.Equal(t, "vol", getMountResp.GetMount().GetMountId())
	require.Equal(t, coordpb.MountState_MOUNT_STATE_ACTIVE, getMountResp.GetMount().GetState())
	listMountsResp, err := cli.ListMounts(context.Background(), &coordpb.ListMountsRequest{})
	require.NoError(t, err)
	require.Len(t, listMountsResp.GetMounts(), 1)

	storeResp, err := cli.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:   1,
		RegionNum: 2,
		LeaderNum: 1,
		Capacity:  1024,
		Available: 800,
	})
	require.NoError(t, err)
	require.True(t, storeResp.GetAccepted())

	publishResp, err := cli.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionBootstrapped(
			testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{
				Version:     1,
				ConfVersion: 1,
			}),
		)),
	})
	require.NoError(t, err)
	require.True(t, publishResp.GetAccepted())
	require.NotNil(t, publishResp.GetAssessment())

	liveResp, err := cli.RegionLiveness(context.Background(), &coordpb.RegionLivenessRequest{RegionId: 11})
	require.NoError(t, err)
	require.True(t, liveResp.GetAccepted())

	publishResp, err = cli.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdded(
			11,
			2,
			201,
			testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{
				Version:     1,
				ConfVersion: 2,
			}),
		)),
	})
	require.NoError(t, err)
	require.True(t, publishResp.GetAccepted())
	require.NotNil(t, publishResp.GetAssessment())
	require.Equal(t, "peer:11:add:2:201", publishResp.GetAssessment().GetTransitionId())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_APPLY, publishResp.GetAssessment().GetDecision())

	getResp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.Equal(t, uint64(11), getResp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, coordpb.Freshness_FRESHNESS_BEST_EFFORT, getResp.GetServedFreshness())
	require.True(t, getResp.GetServedByLeader())
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_HEALTHY, getResp.GetDegradedMode())

	removeResp, err := cli.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 11})
	require.NoError(t, err)
	require.True(t, removeResp.GetRemoved())

	getResp, err = cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.True(t, getResp.GetNotFound())

	idResp, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, uint64(2), idResp.GetCount())

	tsResp, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, uint64(3), tsResp.GetCount())
}

func TestGRPCClientWriteFailoverAcrossPDs(t *testing.T) {
	const bufSize = 1 << 20
	followerListener := bufconn.Listen(bufSize)
	leaderListener := bufconn.Listen(bufSize)
	t.Cleanup(func() {
		_ = followerListener.Close()
		_ = leaderListener.Close()
	})

	followerSvc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), &followerStorage{})
	followerGRPC := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(followerGRPC, followerSvc)
	go func() { _ = followerGRPC.Serve(followerListener) }()
	t.Cleanup(followerGRPC.GracefulStop)

	leaderSvc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), newClientRootStorage(true))
	leaderSvc.ConfigureAuthorityGrant("c1", time.Hour, 30*time.Minute)
	leaderGRPC := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(leaderGRPC, leaderSvc)
	go func() { _ = leaderGRPC.Serve(leaderListener) }()
	t.Cleanup(leaderGRPC.GracefulStop)

	dialer := func(_ context.Context, target string) (net.Conn, error) {
		switch target {
		case "bufnet-follower":
			return followerListener.Dial()
		case "bufnet-leader":
			return leaderListener.Dial()
		default:
			return nil, errors.New("unknown target: " + target)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := NewGRPCClient(ctx, "passthrough:///bufnet-follower,passthrough:///bufnet-leader",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	idResp, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, uint64(2), idResp.GetCount())

	tsResp, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, uint64(2), tsResp.GetCount())
}

type followerStorage struct{}

func (f *followerStorage) Load() (rootview.Snapshot, error) {
	return rootview.Snapshot{Descriptors: make(map[uint64]topology.Descriptor)}, nil
}
func (f *followerStorage) AppendRootEvent(context.Context, rootevent.Event) error { return nil }
func (f *followerStorage) SaveAllocatorState(context.Context, uint64, uint64) error {
	return nil
}
func (f *followerStorage) ApplyGrant(context.Context, rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, nil
}
func (f *followerStorage) Refresh() error            { return nil }
func (f *followerStorage) Close() error              { return nil }
func (f *followerStorage) CanSubmitRootWrites() bool { return false }
func (f *followerStorage) LeaderID() uint64          { return 2 }

type clientRootStorage struct {
	mu       sync.Mutex
	leader   bool
	leaderID uint64
	snapshot rootview.Snapshot
}

func newClientRootStorage(leader bool) *clientRootStorage {
	return &clientRootStorage{
		leader:   leader,
		leaderID: 1,
		snapshot: rootview.Snapshot{
			CatchUpState:       rootview.CatchUpStateFresh,
			Stores:             make(map[uint64]rootstate.StoreMembership),
			SnapshotEpochs:     make(map[string]rootstate.SnapshotEpoch),
			Mounts:             make(map[string]rootstate.MountRecord),
			Subtrees:           make(map[string]rootstate.SubtreeAuthority),
			Quotas:             make(map[string]rootstate.QuotaFence),
			Descriptors:        make(map[uint64]topology.Descriptor),
			PendingPeerChanges: make(map[uint64]rootstate.PendingPeerChange),
		},
	}
}

func (s *clientRootStorage) Load() (rootview.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return rootview.CloneSnapshot(s.snapshot), nil
}

func (s *clientRootStorage) AppendRootEvent(_ context.Context, event rootevent.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyEventLocked(event)
	return nil
}

func (s *clientRootStorage) SaveAllocatorState(_ context.Context, idCurrent, tsCurrent uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if idCurrent > s.snapshot.Allocator.IDCurrent {
		s.snapshot.Allocator.IDCurrent = idCurrent
	}
	if tsCurrent > s.snapshot.Allocator.TSCurrent {
		s.snapshot.Allocator.TSCurrent = tsCurrent
	}
	return nil
}

func (s *clientRootStorage) ApplyGrant(_ context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	holderID := strings.TrimSpace(cmd.HolderID)
	switch cmd.Kind {
	case rootproto.GrantActIssue:
		active, _ := clientActiveGrantFor(s.snapshot, cmd.RequestedDuties)
		if active.Present() && active.HolderID != holderID && active.ActiveAt(cmd.NowUnixNano) {
			return s.protocolStateLocked(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		var era uint64 = 1
		for _, current := range s.snapshot.ActiveGrants {
			if current.Era >= era {
				era = current.Era + 1
			}
		}
		for _, retirement := range s.snapshot.RetiredGrants {
			if retirement.Era >= era {
				era = retirement.Era + 1
			}
		}
		grantID := strings.TrimSpace(cmd.GrantID)
		if grantID == "" {
			grantID = fmt.Sprintf("%s/%d", holderID, era)
		}
		grant := rootproto.AuthorityGrant{
			GrantID:         grantID,
			HolderID:        holderID,
			Era:             era,
			ExpiresUnixNano: cmd.ExpiresUnixNano,
			IssuedRootToken: rootproto.AuthorityRootToken{
				Term:     s.snapshot.RootToken.Cursor.Term,
				Index:    s.snapshot.RootToken.Cursor.Index,
				Revision: s.snapshot.RootToken.Revision,
			},
			Duties: append([]rootproto.DutyGrant(nil), cmd.RequestedDuties...),
		}
		if active.Present() {
			s.snapshot.ActiveGrants = clientRemoveGrantForTest(s.snapshot.ActiveGrants, active.GrantID)
		}
		s.applyEventLocked(rootevent.GrantIssued(grant))
		issued, _ := s.snapshot.ActiveGrantByID(grant.GrantID)
		return s.protocolStateLocked(), clientGrantCertificateForTest(issued), nil
	case rootproto.GrantActSeal:
		active, ok := s.snapshot.ActiveGrantByID(strings.TrimSpace(cmd.GrantID))
		if !ok || active.HolderID != holderID {
			return s.protocolStateLocked(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		retirement := rootproto.GrantRetirement{
			GrantID:  active.GrantID,
			HolderID: active.HolderID,
			Era:      active.Era,
			Mode:     rootproto.GrantRetirementSealedExact,
			Bounds:   clientDutyGrantsFromUsages(cmd.ExactUsages),
		}
		if len(retirement.Bounds) == 0 {
			retirement.Bounds = append([]rootproto.DutyGrant(nil), active.Duties...)
		}
		s.applyEventLocked(rootevent.GrantSealed(retirement))
		return s.protocolStateLocked(), rootproto.GrantCertificate{}, nil
	case rootproto.GrantActInherit:
		active, ok := clientActiveGrantForHolder(s.snapshot, holderID)
		if !ok {
			return s.protocolStateLocked(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		successor := active.GrantID
		for _, predecessor := range cmd.PredecessorGrantIDs {
			s.applyEventLocked(rootevent.GrantInherited(rootproto.GrantInheritance{
				PredecessorGrantID: predecessor,
				SuccessorGrantID:   successor,
			}))
		}
		return s.protocolStateLocked(), rootproto.GrantCertificate{}, nil
	default:
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, rootstate.ErrInvalidGrant
	}
}

func (s *clientRootStorage) Refresh() error            { return nil }
func (s *clientRootStorage) Close() error              { return nil }
func (s *clientRootStorage) CanSubmitRootWrites() bool { return s.leader }
func (s *clientRootStorage) LeaderID() uint64          { return s.leaderID }

func (s *clientRootStorage) applyEventLocked(event rootevent.Event) {
	rooted := s.snapshot.RootSnapshot()
	cursor := rootstate.NextCursor(rooted.State.LastCommitted)
	rootstate.ApplyEventToSnapshot(&rooted, cursor, event)
	nextRevision := s.snapshot.RootToken.Revision + 1
	s.snapshot = rootview.SnapshotFromRoot(rooted)
	s.snapshot.RootToken.Revision = nextRevision
}

func (s *clientRootStorage) protocolStateLocked() rootstate.EunomiaState {
	return rootstate.EunomiaState{
		ActiveGrants:      append([]rootproto.AuthorityGrant(nil), s.snapshot.ActiveGrants...),
		RetiredGrants:     append([]rootproto.GrantRetirement(nil), s.snapshot.RetiredGrants...),
		GrantInheritances: append([]rootproto.GrantInheritance(nil), s.snapshot.GrantInheritances...),
		RetiredEraFloors:  rootproto.CloneAuthorityRetiredEraFloors(s.snapshot.RetiredEraFloors),
	}
}

func clientActiveGrantFor(snapshot rootview.Snapshot, duties []rootproto.DutyGrant) (rootproto.AuthorityGrant, bool) {
	for _, grant := range snapshot.ActiveGrants {
		for _, duty := range duties {
			if grant.CoversDutyKey(duty.Key()) {
				return grant, true
			}
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func clientActiveGrantForHolder(snapshot rootview.Snapshot, holderID string) (rootproto.AuthorityGrant, bool) {
	for _, grant := range snapshot.ActiveGrants {
		if strings.TrimSpace(grant.HolderID) == holderID {
			return grant, true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func clientRemoveGrantForTest(grants []rootproto.AuthorityGrant, grantID string) []rootproto.AuthorityGrant {
	out := grants[:0]
	for _, grant := range grants {
		if grant.GrantID != grantID {
			out = append(out, grant)
		}
	}
	return out
}

func clientGrantCertificateForTest(grant rootproto.AuthorityGrant) rootproto.GrantCertificate {
	payload, _ := proto.MarshalOptions{Deterministic: true}.Marshal(metawire.RootAuthorityGrantToProto(grant))
	return rootproto.GrantCertificate{
		Grant:       grant,
		SignerKeyID: rootproto.GrantSignerKeyID,
		Signature:   rootproto.SignGrantBytes(payload),
	}
}

func clientDutyGrantsFromUsages(usages []rootproto.AuthorityUsage) []rootproto.DutyGrant {
	out := make([]rootproto.DutyGrant, 0, len(usages))
	for _, usage := range usages {
		if usage.DutyID == "" {
			continue
		}
		out = append(out, rootproto.DutyGrant{
			DutyID: usage.DutyID,
			Scope:  usage.Scope,
			Bound:  usage.Usage,
		})
	}
	return out
}

func TestGRPCClientDoesNotRetryReadOnNotLeaderWriteError(t *testing.T) {
	err := notLeaderErrorForTest(2)
	require.True(t, retryableWrite(err))
	require.False(t, retryableRead(err))
	require.True(t, IsNotLeader(err))
	leaderID, ok := LeaderHint(err)
	require.True(t, ok)
	require.Equal(t, uint64(2), leaderID)
}

func TestCoordinatorClientErrorHelpers(t *testing.T) {
	require.True(t, IsEmptyAddress(errEmptyAddress))
	require.True(t, IsNoReachableAddress(errNoReachableAddress))
	require.True(t, IsConnectionShutdown(errConnectionShutdown))
	require.True(t, IsStaleWitnessEra(errStaleWitnessEra))
	require.True(t, IsInvalidWitness(errInvalidWitness))
	require.False(t, IsNotLeader(errEmptyAddress))
	require.False(t, IsGrantNotHeld(errEmptyAddress))
	_, ok := LeaderHint(errEmptyAddress)
	require.False(t, ok)
}

func TestGRPCClientRetriesWriteOnGrantNotHeld(t *testing.T) {
	err := grantNotHeldErrorForTest()
	require.True(t, IsGrantNotHeld(err))
	require.True(t, retryableWrite(err))
	require.False(t, retryableRead(err))
	require.False(t, IsNotLeader(err))
}

func TestGRPCClientRetriesTSOAcrossGrantNotHeldEndpoint(t *testing.T) {
	grantErr := grantNotHeldErrorForTest()
	servers := map[string]*scriptedCoordinatorServer{
		"standby": {
			tsoErrors: []error{grantErr},
		},
		"holder": {
			tsoResponses: []*coordpb.TsoResponse{{
				Timestamp:        100,
				Count:            2,
				Era:              1,
				ConsumedFrontier: 101,
			}},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"standby", "holder"}, servers)

	resp, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(100), resp.GetTimestamp())
	require.Equal(t, uint64(2), resp.GetCount())
	require.Equal(t, 1, servers["standby"].tsoCalls)
	require.Equal(t, 1, servers["holder"].tsoCalls)
	require.Equal(t, "passthrough:///holder", cli.orderedEndpointsForDuty(rootproto.DutyTSO)[0].addr)
}

func TestGRPCClientRetriesAllocIDAcrossGrantNotHeldEndpoint(t *testing.T) {
	grantErr := grantNotHeldErrorForTest()
	servers := map[string]*scriptedCoordinatorServer{
		"standby": {
			allocErrors: []error{grantErr},
		},
		"holder": {
			allocResponses: []*coordpb.AllocIDResponse{{
				FirstId:          200,
				Count:            3,
				Era:              1,
				ConsumedFrontier: 202,
			}},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"standby", "holder"}, servers)

	resp, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(200), resp.GetFirstId())
	require.Equal(t, uint64(3), resp.GetCount())
	require.Equal(t, 1, servers["standby"].allocCalls)
	require.Equal(t, 1, servers["holder"].allocCalls)
	require.Equal(t, "passthrough:///holder", cli.orderedEndpointsForDuty(rootproto.DutyAllocID)[0].addr)
}

func TestGRPCClientRetriesGetRegionByKeyAcrossGrantNotHeldEndpoint(t *testing.T) {
	grantErr := grantNotHeldErrorForTest()
	servers := map[string]*scriptedCoordinatorServer{
		"standby": {
			getErrors: []error{grantErr},
		},
		"holder": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        3,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"standby", "holder"}, servers)

	resp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
		RequiredDescriptorRevision: 8,
		MaxRootLag:                 proto.Uint64(2),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(11), resp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, 1, servers["standby"].getCalls)
	require.Equal(t, 1, servers["holder"].getCalls)
	require.Equal(t, "passthrough:///holder", cli.orderedEndpointsForDuty(rootproto.DutyRegionLookup)[0].addr)
}

func TestGRPCClientRetriesTSOAfterFullGrantMissRound(t *testing.T) {
	grantErr := grantNotHeldErrorForTest()
	servers := map[string]*scriptedCoordinatorServer{
		"standby": {
			tsoErrors: []error{grantErr, grantErr},
		},
		"holder": {
			tsoErrors: []error{grantErr},
			tsoResponses: []*coordpb.TsoResponse{{
				Timestamp:        300,
				Count:            1,
				Era:              2,
				ConsumedFrontier: 300,
			}},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"standby", "holder"}, servers)

	resp, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(300), resp.GetTimestamp())
	require.Equal(t, 2, servers["standby"].tsoCalls)
	require.Equal(t, 2, servers["holder"].tsoCalls)
	require.Equal(t, "passthrough:///holder", cli.orderedEndpointsForDuty(rootproto.DutyTSO)[0].addr)
}

func TestGRPCClientRetriesDutyRoundWhenHolderTemporarilyUnavailable(t *testing.T) {
	grantErr := grantNotHeldErrorForTest()
	servers := map[string]*scriptedCoordinatorServer{
		"holder": {
			tsoErrors: []error{status.Error(codes.Unavailable, "holder is renewing grant")},
			tsoResponses: []*coordpb.TsoResponse{{
				Timestamp:        400,
				Count:            1,
				Era:              4,
				ConsumedFrontier: 400,
			}},
		},
		"standby": {
			tsoErrors: []error{grantErr},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"holder", "standby"}, servers)

	resp, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(400), resp.GetTimestamp())
	require.Equal(t, 2, servers["holder"].tsoCalls)
	require.Equal(t, 1, servers["standby"].tsoCalls)
	require.Equal(t, "passthrough:///holder", cli.orderedEndpointsForDuty(rootproto.DutyTSO)[0].addr)
}

func TestGRPCClientRetriesStaleWitnessAcrossDutyRounds(t *testing.T) {
	servers := map[string]*scriptedCoordinatorServer{
		"holder": {
			tsoResponses: []*coordpb.TsoResponse{
				{
					Timestamp:               500,
					Count:                   1,
					Era:                     5,
					ConsumedFrontier:        500,
					ObservedRetiredEraFloor: 5,
				},
				{
					Timestamp:        501,
					Count:            1,
					Era:              6,
					ConsumedFrontier: 501,
				},
			},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"holder"}, servers)

	resp, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(501), resp.GetTimestamp())
	require.Equal(t, 2, servers["holder"].tsoCalls)
}

func TestGRPCClientAuthorityMissRetryHonorsContext(t *testing.T) {
	grantErr := grantNotHeldErrorForTest()
	servers := map[string]*scriptedCoordinatorServer{
		"standby": {
			tsoErrors: []error{
				grantErr, grantErr, grantErr, grantErr,
				grantErr, grantErr, grantErr, grantErr,
			},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"standby"}, servers)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	_, err := cli.Tso(ctx, &coordpb.TsoRequest{Count: 1})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, servers["standby"].tsoCalls)
}

func TestGRPCClientRejectsInvalidAllocWitness(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"alloc-invalid"}, map[string]*scriptedCoordinatorServer{
		"alloc-invalid": {
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:          10,
					Count:            2,
					Era:              1,
					ConsumedFrontier: 10,
				},
			},
		},
	})

	_, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "consumed_frontier=10 expected=11")
}

func TestGRPCClientRejectsAttachedEraForMonotoneAuthority(t *testing.T) {
	t.Run("alloc_id", func(t *testing.T) {
		cli := newScriptedCoordinatorClient(t, []string{"alloc-attached"}, map[string]*scriptedCoordinatorServer{
			"alloc-attached": {
				allocResponses: []*coordpb.AllocIDResponse{{
					FirstId:          100,
					Count:            1,
					Era:              rootproto.AuthorityEraAttached,
					ConsumedFrontier: 100,
				}},
			},
		})

		_, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
		require.Error(t, err)
		require.True(t, IsInvalidWitness(err))
		require.Contains(t, err.Error(), "attached era is only valid for metadata witnesses")
	})

	t.Run("tso", func(t *testing.T) {
		cli := newScriptedCoordinatorClient(t, []string{"tso-attached"}, map[string]*scriptedCoordinatorServer{
			"tso-attached": {
				tsoResponses: []*coordpb.TsoResponse{{
					Timestamp:        200,
					Count:            1,
					Era:              rootproto.AuthorityEraAttached,
					ConsumedFrontier: 200,
				}},
			},
		})

		_, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
		require.Error(t, err)
		require.True(t, IsInvalidWitness(err))
		require.Contains(t, err.Error(), "attached era is only valid for metadata witnesses")
	})
}

func TestGRPCClientRetriesStaleWitnessEraAcrossEndpoints(t *testing.T) {
	servers := map[string]*scriptedCoordinatorServer{
		"fresh": {
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:          100,
					Count:            1,
					Era:              2,
					ConsumedFrontier: 100,
				},
				{
					FirstId:          101,
					Count:            1,
					Era:              2,
					ConsumedFrontier: 101,
				},
			},
		},
		"stale": {
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:          50,
					Count:            1,
					Era:              1,
					ConsumedFrontier: 50,
				},
			},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"fresh", "stale"}, servers)

	resp, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(100), resp.GetFirstId())

	cli.markPreferredForDuty(rootproto.DutyAllocID, "passthrough:///stale")

	resp, err = cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(101), resp.GetFirstId())
	require.Equal(t, 1, servers["stale"].allocCalls)
	require.Equal(t, 2, servers["fresh"].allocCalls)
}

func TestGRPCClientRejectsInvalidTSOWitness(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"tso-invalid"}, map[string]*scriptedCoordinatorServer{
		"tso-invalid": {
			tsoResponses: []*coordpb.TsoResponse{
				{
					Timestamp:        90,
					Count:            1,
					Era:              3,
					ConsumedFrontier: 89,
				},
			},
		},
	})

	_, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "consumed_frontier=89 expected=90")
}

func TestGRPCClientRejectsInvalidMetadataWitness(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"metadata-invalid"}, map[string]*scriptedCoordinatorServer{
		"metadata-invalid": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 7},
					ServedRootToken:            &coordpb.RootToken{Term: 1, Index: 4, Revision: 4},
					CurrentRootToken:           &coordpb.RootToken{Term: 1, Index: 5, Revision: 5},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         7,
					RequiredDescriptorRevision: 7,
					Era:                        2,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
	})

	_, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 1, Index: 5, Revision: 5},
		RequiredDescriptorRevision: 7,
		MaxRootLag:                 proto.Uint64(2),
	})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "served_root_token does not satisfy required_root_token")
}

func TestGRPCClientAcceptsValidMetadataWitness(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"metadata-valid"}, map[string]*scriptedCoordinatorServer{
		"metadata-valid": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        3,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
	})

	resp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
		RequiredDescriptorRevision: 8,
		MaxRootLag:                 proto.Uint64(2),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(11), resp.GetRegionDescriptor().GetRegionId())
}

func TestGRPCClientAcceptsLowerDescriptorRevisionFromDifferentRegion(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"metadata-valid"}, map[string]*scriptedCoordinatorServer{
		"metadata-valid": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 21, RootEpoch: 100},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 100, Revision: 100},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 101, Revision: 101},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         100,
					RequiredDescriptorRevision: 1,
					Era:                        7,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 22, RootEpoch: 12},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 101, Revision: 101},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 102, Revision: 102},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         12,
					RequiredDescriptorRevision: 1,
					Era:                        7,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
	})

	resp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("hot-bucket"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 100, Revision: 100},
		RequiredDescriptorRevision: 1,
		MaxRootLag:                 proto.Uint64(2),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(21), resp.GetRegionDescriptor().GetRegionId())

	// Region descriptor revisions are per-region root epochs. A later lookup
	// for a colder bucket can return a lower descriptor revision while still
	// carrying a monotone current root token and valid region-lookup evidence.
	resp, err = cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("cold-bucket"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 101, Revision: 101},
		RequiredDescriptorRevision: 1,
		MaxRootLag:                 proto.Uint64(2),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(22), resp.GetRegionDescriptor().GetRegionId())
}

func TestGRPCClientRetriesStaleMetadataWitnessEraAcrossEndpoints(t *testing.T) {
	servers := map[string]*scriptedCoordinatorServer{
		"fresh": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        3,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 12, RootEpoch: 10},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 10, Revision: 11},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         10,
					RequiredDescriptorRevision: 8,
					Era:                        3,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
		"stale": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 10, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        2,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"fresh", "stale"}, servers)

	resp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
		RequiredDescriptorRevision: 8,
		MaxRootLag:                 proto.Uint64(2),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(11), resp.GetRegionDescriptor().GetRegionId())

	cli.markPreferredForDuty(rootproto.DutyRegionLookup, "passthrough:///stale")

	resp, err = cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
		RequiredDescriptorRevision: 8,
		MaxRootLag:                 proto.Uint64(2),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(12), resp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, 1, servers["stale"].getCalls)
	require.Equal(t, 2, servers["fresh"].getCalls)
}

func TestGRPCClientAcceptsZeroEraMetadataWitnessAfterDetachedEra(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"mixed"}, map[string]*scriptedCoordinatorServer{
		"mixed": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        3,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 12, RootEpoch: 10},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_STRONG,
					RootLag:                    0,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
					DescriptorRevision:         10,
					RequiredDescriptorRevision: 8,
					Era:                        0,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
					ServedByLeader:             true,
				},
			},
		},
	})

	_, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
		RequiredDescriptorRevision: 8,
		MaxRootLag:                 proto.Uint64(2),
	})
	require.NoError(t, err)

	resp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_STRONG,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
		RequiredDescriptorRevision: 8,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(12), resp.GetRegionDescriptor().GetRegionId())
	require.Zero(t, resp.GetEra())
}

func TestGRPCClientRejectsZeroEraMetadataWitnessRegressingAttachedFrontier(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"mixed"}, map[string]*scriptedCoordinatorServer{
		"mixed": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 12, RootEpoch: 10},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_STRONG,
					RootLag:                    0,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
					DescriptorRevision:         10,
					RequiredDescriptorRevision: 8,
					Era:                        0,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
					ServedByLeader:             true,
				},
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 9, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 9},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_STRONG,
					RootLag:                    0,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        0,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
					ServedByLeader:             true,
				},
			},
		},
	})

	resp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_STRONG,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
		RequiredDescriptorRevision: 8,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(12), resp.GetRegionDescriptor().GetRegionId())

	_, err = cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_STRONG,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 9, Revision: 9},
		RequiredDescriptorRevision: 8,
	})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "current_root_token regressed behind attached floor")
}

func TestGRPCClientRejectsZeroEraMetadataWitnessWithoutAuthoritativeAttachedServing(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"mixed"}, map[string]*scriptedCoordinatorServer{
		"mixed": {
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 9, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BOUNDED,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         9,
					RequiredDescriptorRevision: 8,
					Era:                        3,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 12, RootEpoch: 10},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 11, Revision: 11},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_BEST_EFFORT,
					RootLag:                    1,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_LAGGING,
					DescriptorRevision:         10,
					RequiredDescriptorRevision: 8,
					Era:                        0,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_LAGGING,
				},
			},
		},
	})

	_, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 8, Revision: 9},
		RequiredDescriptorRevision: 8,
		MaxRootLag:                 proto.Uint64(2),
	})
	require.NoError(t, err)

	_, err = cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BEST_EFFORT,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
		RequiredDescriptorRevision: 8,
	})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "era=0 requires authoritative attached")
}

func TestGRPCClientRejectsSuppressedReplyEvidence(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"mixed"}, map[string]*scriptedCoordinatorServer{
		"mixed": {
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:          100,
					Count:            1,
					Era:              rootproto.AuthorityEraSuppressed,
					ConsumedFrontier: 0,
				},
			},
			getResponses: []*coordpb.GetRegionByKeyResponse{
				{
					RegionDescriptor:           &metapb.RegionDescriptor{RegionId: 12, RootEpoch: 10},
					ServedRootToken:            &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					CurrentRootToken:           &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
					ServedFreshness:            coordpb.Freshness_FRESHNESS_STRONG,
					RootLag:                    0,
					CatchUpState:               coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
					DescriptorRevision:         10,
					RequiredDescriptorRevision: 8,
					Era:                        rootproto.AuthorityEraSuppressed,
					ServingClass:               coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE,
					SyncHealth:                 coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
					ServedByLeader:             true,
				},
			},
		},
	})

	_, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "reply evidence suppressed")

	_, err = cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("m"),
		Freshness:                  coordpb.Freshness_FRESHNESS_STRONG,
		RequiredRootToken:          &coordpb.RootToken{Term: 2, Index: 10, Revision: 10},
		RequiredDescriptorRevision: 8,
	})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "reply evidence suppressed")
}

func TestGRPCClientRejectsMissingAuthorityEvidence(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"missing-cert"}, map[string]*scriptedCoordinatorServer{
		"missing-cert": {
			disableDefaultAuthorityEvidence: true,
			allocResponses: []*coordpb.AllocIDResponse{
				{
					FirstId:          100,
					Count:            1,
					Era:              2,
					ConsumedFrontier: 100,
				},
			},
		},
	})

	_, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "authority evidence missing grant certificate")
}

func TestValidateAuthorityEvidenceBindsReplyUsageToEvidence(t *testing.T) {
	grant := rootproto.AuthorityGrant{
		GrantID:         "grant-2",
		HolderID:        "holder",
		Era:             2,
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
		Duties: []rootproto.DutyGrant{{
			DutyID: rootproto.DutyAllocID,
			Scope:  rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
			Bound:  rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 1_000},
		}},
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(metawire.RootAuthorityGrantToProto(grant))
	require.NoError(t, err)
	evidence := metawire.RootAuthorityEvidenceToProto(rootproto.AuthorityEvidence{
		Certificate: rootproto.GrantCertificate{
			Grant:       grant,
			SignerKeyID: rootproto.GrantSignerKeyID,
			Signature:   rootproto.SignGrantBytes(payload),
		},
		Usage: rootproto.AuthorityUsage{
			DutyID: rootproto.DutyAllocID,
			Scope:  rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
			Usage:  rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 100},
		},
		ServedUnixNano: time.Now().UnixNano(),
	})

	cli := &GRPCClient{verifierStore: NewMemoryAuthorityVerifierStore(), verifierClusterID: "test", now: time.Now}
	err = cli.validateAuthorityEvidence("alloc_id", rootproto.DutyAllocID, 2, 0, rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 900}, evidence)
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "usage outside grant")
}

func TestAdvanceWitnessEraFloorDoesNotPersistRejectedFloor(t *testing.T) {
	cli := &GRPCClient{}
	floor := witnessEraFloor{maxSeen: 5}

	err := cli.advanceWitnessEraFloor("alloc_id", rootproto.DutyAllocID, 4, 10, &floor)
	require.Error(t, err)
	require.True(t, IsStaleWitnessEra(err))
	require.Zero(t, floor.retiredSeen)
	require.Equal(t, uint64(5), floor.maxSeen)

	require.NoError(t, cli.advanceWitnessEraFloor("alloc_id", rootproto.DutyAllocID, 6, 0, &floor))
	require.Zero(t, floor.retiredSeen)
	require.Equal(t, uint64(6), floor.maxSeen)
}

func TestFileAuthorityVerifierStorePersistsFloorAcrossRestart(t *testing.T) {
	path := t.TempDir() + "/authority-verifier.pb"
	store := NewFileAuthorityVerifierStore(path)
	cli := &GRPCClient{verifierStore: store, verifierClusterID: "cluster-a"}
	floor := witnessEraFloor{}

	require.NoError(t, cli.advanceWitnessEraFloor("alloc_id", rootproto.DutyAllocID, 2, 1, &floor))

	restarted := &GRPCClient{
		verifierStore:     NewFileAuthorityVerifierStore(path),
		verifierClusterID: "cluster-a",
	}
	var restartedFloor witnessEraFloor
	err := restarted.advanceWitnessEraFloor("alloc_id", rootproto.DutyAllocID, 1, 0, &restartedFloor)
	require.Error(t, err)
	require.True(t, IsStaleWitnessEra(err))
	require.Contains(t, err.Error(), "retired_floor=1")
}

// TestGRPCClientKeepsRetiredFloorsScopedByDuty protects the durable verifier
// store layout: each duty advances its own retired floor and never writes into
// another duty's key.
func TestGRPCClientKeepsRetiredFloorsScopedByDuty(t *testing.T) {
	cli := &GRPCClient{verifierStore: NewMemoryAuthorityVerifierStore(), verifierClusterID: "test"}
	var allocFloor witnessEraFloor
	var tsoFloor witnessEraFloor

	require.NoError(t, cli.advanceWitnessEraFloor("alloc_id", rootproto.DutyAllocID, 23, 22, &allocFloor))
	require.NoError(t, cli.advanceWitnessEraFloor("tso", rootproto.DutyTSO, 9, 0, &tsoFloor))
	require.Equal(t, uint64(22), allocFloor.retiredSeen)
	require.Zero(t, tsoFloor.retiredSeen)
}

// TestGRPCClientDoesNotLetAllocRetiredFloorPoisonTSOOrRegionLookup exercises
// the fsmeta-facing coordinator client path: inode allocation can observe a high
// alloc_id floor without making later TSO or region lookup witnesses stale.
func TestGRPCClientDoesNotLetAllocRetiredFloorPoisonTSOOrRegionLookup(t *testing.T) {
	cli := newScriptedCoordinatorClient(t, []string{"holder"}, map[string]*scriptedCoordinatorServer{
		"holder": {
			allocResponses: []*coordpb.AllocIDResponse{{
				FirstId:                 100,
				Count:                   1,
				Era:                     23,
				ConsumedFrontier:        100,
				ObservedRetiredEraFloor: 22,
			}},
			tsoResponses: []*coordpb.TsoResponse{{
				Timestamp:        900,
				Count:            1,
				Era:              9,
				ConsumedFrontier: 900,
			}},
			getResponses: []*coordpb.GetRegionByKeyResponse{{
				RegionDescriptor:   &metapb.RegionDescriptor{RegionId: 11, RootEpoch: 9},
				ServedRootToken:    &coordpb.RootToken{Term: 2, Index: 9, Revision: 9},
				CurrentRootToken:   &coordpb.RootToken{Term: 2, Index: 9, Revision: 9},
				ServedFreshness:    coordpb.Freshness_FRESHNESS_STRONG,
				CatchUpState:       coordpb.CatchUpState_CATCH_UP_STATE_FRESH,
				DescriptorRevision: 9,
				Era:                9,
				ServingClass:       coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE,
				SyncHealth:         coordpb.SyncHealth_SYNC_HEALTH_HEALTHY,
				ServedByLeader:     true,
			}},
		},
	})

	allocResp, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(23), allocResp.GetEra())
	require.Equal(t, uint64(22), allocResp.GetObservedRetiredEraFloor())

	tsoResp, err := cli.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(9), tsoResp.GetEra())

	regionResp, err := cli.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("m"),
		Freshness: coordpb.Freshness_FRESHNESS_STRONG,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(9), regionResp.GetEra())
	require.Zero(t, regionResp.GetObservedRetiredEraFloor())
}

func TestValidateAuthorityEvidenceRejectsMissingServedTime(t *testing.T) {
	grant := rootproto.AuthorityGrant{
		GrantID:         "grant-2",
		HolderID:        "holder",
		Era:             2,
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
		Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10)},
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(metawire.RootAuthorityGrantToProto(grant))
	require.NoError(t, err)
	evidence := metawire.RootAuthorityEvidenceToProto(rootproto.AuthorityEvidence{
		Certificate: rootproto.GrantCertificate{
			Grant:       grant,
			SignerKeyID: rootproto.GrantSignerKeyID,
			Signature:   rootproto.SignGrantBytes(payload),
		},
		Usage: rootproto.AuthorityUsage{
			DutyID: rootproto.DutyAllocID,
			Scope:  rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
			Usage:  rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 10},
		},
	})
	cli := &GRPCClient{verifierStore: NewMemoryAuthorityVerifierStore(), verifierClusterID: "test"}
	err = cli.validateAuthorityEvidence("alloc_id", rootproto.DutyAllocID, 2, 0, rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 10}, evidence)
	require.Error(t, err)
	require.True(t, IsInvalidWitness(err))
	require.Contains(t, err.Error(), "missing served_unix_nano")
}

func TestGRPCClientRejectsReplyAtObservedSealFloor(t *testing.T) {
	cli := &GRPCClient{verifierStore: NewMemoryAuthorityVerifierStore(), verifierClusterID: "test"}
	err := cli.validateAllocIDResponse(&coordpb.AllocIDRequest{Count: 1}, &coordpb.AllocIDResponse{
		FirstId:                 100,
		Count:                   1,
		Era:                     2,
		ConsumedFrontier:        100,
		ObservedRetiredEraFloor: 2,
		AuthorityEvidence:       defaultAuthorityEvidence(rootproto.DutyAllocID, rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 100}, 2, 2),
	})
	require.Error(t, err)
	require.True(t, IsStaleWitnessEra(err))
	require.Contains(t, err.Error(), "retired_floor=2")
}

type scriptedCoordinatorServer struct {
	coordpb.UnimplementedCoordinatorServer

	mu                              sync.Mutex
	disableDefaultAuthorityEvidence bool

	storeResponses []*coordpb.StoreHeartbeatResponse
	storeErrors    []error
	storeCalls     int

	listResponses []*coordpb.ListTransitionsResponse
	listErrors    []error
	listCalls     int

	assessResponses []*coordpb.AssessRootEventResponse
	assessErrors    []error
	assessCalls     int

	allocResponses []*coordpb.AllocIDResponse
	allocErrors    []error
	allocCalls     int

	tsoResponses []*coordpb.TsoResponse
	tsoErrors    []error
	tsoCalls     int

	getResponses []*coordpb.GetRegionByKeyResponse
	getErrors    []error
	getCalls     int
}

func (s *scriptedCoordinatorServer) StoreHeartbeat(_ context.Context, _ *coordpb.StoreHeartbeatRequest) (*coordpb.StoreHeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storeCalls++
	var err error
	if len(s.storeErrors) > 0 {
		err = s.storeErrors[0]
		s.storeErrors = s.storeErrors[1:]
	}
	if len(s.storeResponses) == 0 {
		return nil, err
	}
	resp := s.storeResponses[0]
	s.storeResponses = s.storeResponses[1:]
	return resp, err
}

func (s *scriptedCoordinatorServer) ListTransitions(_ context.Context, _ *coordpb.ListTransitionsRequest) (*coordpb.ListTransitionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls++
	var err error
	if len(s.listErrors) > 0 {
		err = s.listErrors[0]
		s.listErrors = s.listErrors[1:]
	}
	if len(s.listResponses) == 0 {
		return nil, err
	}
	resp := s.listResponses[0]
	s.listResponses = s.listResponses[1:]
	return resp, err
}

func (s *scriptedCoordinatorServer) AssessRootEvent(_ context.Context, _ *coordpb.AssessRootEventRequest) (*coordpb.AssessRootEventResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assessCalls++
	var err error
	if len(s.assessErrors) > 0 {
		err = s.assessErrors[0]
		s.assessErrors = s.assessErrors[1:]
	}
	if len(s.assessResponses) == 0 {
		return nil, err
	}
	resp := s.assessResponses[0]
	s.assessResponses = s.assessResponses[1:]
	return resp, err
}

func (s *scriptedCoordinatorServer) AllocID(_ context.Context, _ *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allocCalls++
	var err error
	if len(s.allocErrors) > 0 {
		err = s.allocErrors[0]
		s.allocErrors = s.allocErrors[1:]
	}
	if err != nil {
		return nil, err
	}
	if len(s.allocResponses) == 0 {
		return nil, status.Error(codes.Internal, "scripted coordinator AllocID response queue is empty")
	}
	resp := s.allocResponses[0]
	s.allocResponses = s.allocResponses[1:]
	s.attachDefaultAuthorityEvidence(resp)
	return resp, nil
}

func (s *scriptedCoordinatorServer) Tso(_ context.Context, _ *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tsoCalls++
	var err error
	if len(s.tsoErrors) > 0 {
		err = s.tsoErrors[0]
		s.tsoErrors = s.tsoErrors[1:]
	}
	if err != nil {
		return nil, err
	}
	if len(s.tsoResponses) == 0 {
		return nil, status.Error(codes.Internal, "scripted coordinator Tso response queue is empty")
	}
	resp := s.tsoResponses[0]
	s.tsoResponses = s.tsoResponses[1:]
	s.attachDefaultAuthorityEvidence(resp)
	return resp, nil
}

func (s *scriptedCoordinatorServer) GetRegionByKey(_ context.Context, _ *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls++
	var err error
	if len(s.getErrors) > 0 {
		err = s.getErrors[0]
		s.getErrors = s.getErrors[1:]
	}
	if len(s.getResponses) == 0 {
		return nil, err
	}
	resp := s.getResponses[0]
	s.getResponses = s.getResponses[1:]
	s.attachDefaultAuthorityEvidence(resp)
	return resp, err
}

func (s *scriptedCoordinatorServer) attachDefaultAuthorityEvidence(resp any) {
	if s == nil || s.disableDefaultAuthorityEvidence {
		return
	}
	switch r := resp.(type) {
	case *coordpb.AllocIDResponse:
		if r == nil || r.GetEra() == 0 || r.GetEra() == rootproto.AuthorityEraSuppressed || r.GetAuthorityEvidence() != nil {
			return
		}
		r.AuthorityEvidence = defaultAuthorityEvidence(rootproto.DutyAllocID, rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: r.GetConsumedFrontier()}, r.GetEra(), r.GetObservedRetiredEraFloor())
	case *coordpb.TsoResponse:
		if r == nil || r.GetEra() == 0 || r.GetEra() == rootproto.AuthorityEraSuppressed || r.GetAuthorityEvidence() != nil {
			return
		}
		r.AuthorityEvidence = defaultAuthorityEvidence(rootproto.DutyTSO, rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: r.GetConsumedFrontier()}, r.GetEra(), r.GetObservedRetiredEraFloor())
	case *coordpb.GetRegionByKeyResponse:
		if r == nil || r.GetEra() == 0 || r.GetEra() == rootproto.AuthorityEraSuppressed || r.GetAuthorityEvidence() != nil {
			return
		}
		r.AuthorityEvidence = defaultAuthorityEvidence(rootproto.DutyRegionLookup, rootproto.DutyBound{Kind: rootproto.DutyBoundVersion, DescriptorRevisionCeiling: r.GetDescriptorRevision()}, r.GetEra(), r.GetObservedRetiredEraFloor())
	}
}

func defaultAuthorityEvidence(duty rootproto.DutyID, usage rootproto.DutyBound, era, observedRetiredEra uint64) *metapb.RootAuthorityEvidence {
	grant := rootproto.AuthorityGrant{
		GrantID:         fmt.Sprintf("grant-%d", era),
		HolderID:        "holder",
		Era:             era,
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
		IssuedRootToken: rootproto.AuthorityRootToken{Term: 1, Index: era},
		Duties: []rootproto.DutyGrant{{
			DutyID: duty,
			Scope:  rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
			Bound:  usage,
		}},
	}
	payload, _ := proto.MarshalOptions{Deterministic: true}.Marshal(metawire.RootAuthorityGrantToProto(grant))
	cert := rootproto.GrantCertificate{
		Grant:       grant,
		SignerKeyID: rootproto.GrantSignerKeyID,
		Signature:   rootproto.SignGrantBytes(payload),
	}
	return metawire.RootAuthorityEvidenceToProto(rootproto.AuthorityEvidence{
		Certificate: cert,
		Usage: rootproto.AuthorityUsage{
			DutyID: duty,
			Scope:  rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
			Usage:  usage,
		},
		ObservedRetiredEraFloor: observedRetiredEra,
		ServedUnixNano:          time.Now().UnixNano(),
	})
}

func newScriptedCoordinatorClient(t *testing.T, order []string, servers map[string]*scriptedCoordinatorServer) *GRPCClient {
	t.Helper()

	const bufSize = 1 << 20
	listeners := make(map[string]*bufconn.Listener, len(order))
	for _, name := range order {
		srv := servers[name]
		require.NotNil(t, srv, "missing scripted server %q", name)
		listener := bufconn.Listen(bufSize)
		listeners[name] = listener
		t.Cleanup(func() {
			_ = listener.Close()
		})

		grpcServer := grpc.NewServer()
		coordpb.RegisterCoordinatorServer(grpcServer, srv)
		go func(l *bufconn.Listener) {
			_ = grpcServer.Serve(l)
		}(listener)
		t.Cleanup(grpcServer.GracefulStop)
	}

	dialer := func(_ context.Context, target string) (net.Conn, error) {
		name := strings.TrimPrefix(target, "passthrough:///")
		listener, ok := listeners[name]
		if !ok {
			return nil, errors.New("unknown target: " + target)
		}
		return listener.Dial()
	}

	addrs := make([]string, 0, len(order))
	for _, name := range order {
		addrs = append(addrs, "passthrough:///"+name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := NewGRPCClient(ctx, strings.Join(addrs, ","),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func TestGRPCClientStoreHeartbeatBroadcastsAndPrefersOperationalResponse(t *testing.T) {
	servers := map[string]*scriptedCoordinatorServer{
		"standby": {
			storeResponses: []*coordpb.StoreHeartbeatResponse{
				{Accepted: true},
			},
		},
		"holder": {
			storeResponses: []*coordpb.StoreHeartbeatResponse{
				{
					Accepted: true,
					Operations: []*coordpb.SchedulerOperation{
						{
							Type:         coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER,
							RegionId:     9,
							SourcePeerId: 101,
							TargetPeerId: 201,
						},
					},
				},
			},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"standby", "holder"}, servers)

	resp, err := cli.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:         2,
		RegionNum:       2,
		LeaderNum:       1,
		LeaderRegionIds: []uint64{9},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetOperations(), 1)
	require.Equal(t, uint64(9), resp.GetOperations()[0].GetRegionId())
	require.Equal(t, 1, servers["standby"].storeCalls)
	require.Equal(t, 1, servers["holder"].storeCalls)
	require.Equal(t, "passthrough:///holder", cli.orderedEndpoints()[0].addr)
}

func TestGRPCClientListTransitionsAndAssessRootEvent(t *testing.T) {
	servers := map[string]*scriptedCoordinatorServer{
		"holder": {
			listResponses: []*coordpb.ListTransitionsResponse{{
				Entries: []*coordpb.TransitionEntry{{
					Key:          7,
					TransitionId: "peer:7:add:2:201",
				}},
			}},
			assessResponses: []*coordpb.AssessRootEventResponse{{
				Assessment: &coordpb.TransitionAssessment{
					Key:          7,
					Decision:     coordpb.TransitionDecision_TRANSITION_DECISION_APPLY,
					TransitionId: "peer:7:add:2:201",
				},
			}},
		},
	}
	cli := newScriptedCoordinatorClient(t, []string{"holder"}, servers)

	listResp, err := cli.ListTransitions(context.Background(), &coordpb.ListTransitionsRequest{})
	require.NoError(t, err)
	require.Len(t, listResp.GetEntries(), 1)
	require.Equal(t, "peer:7:add:2:201", listResp.GetEntries()[0].GetTransitionId())

	assessResp, err := cli.AssessRootEvent(context.Background(), &coordpb.AssessRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionTombstoned(7)),
	})
	require.NoError(t, err)
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_APPLY, assessResp.GetAssessment().GetDecision())
	require.Equal(t, 1, servers["holder"].listCalls)
	require.Equal(t, 1, servers["holder"].assessCalls)
}

func TestClientHelperFunctions(t *testing.T) {
	addrs, err := splitAddresses("  a , b ,, c ")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, addrs)
	_, err = splitAddresses(" , ")
	require.ErrorIs(t, err, errEmptyAddress)

	defaultOpts := normalizeDialOptions(nil)
	require.NotEmpty(t, defaultOpts)

	custom := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	require.Equal(t, custom, normalizeDialOptions(custom))

	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	t.Cleanup(func() {
		_ = listener.Close()
	})
	grpcServer := grpc.NewServer()
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.GracefulStop)

	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	require.NoError(t, waitForReady(ctx, conn))

	closeAllEndpoints([]grpcEndpoint{{addr: "passthrough:///bufnet", conn: conn}})
	require.Eventually(t, func() bool {
		return conn.GetState() == connectivity.Shutdown
	}, time.Second, 10*time.Millisecond)
}

func testDescriptor(id uint64, start, end []byte, epoch metaregion.Epoch) topology.Descriptor {
	desc := topology.Descriptor{
		RegionID:  id,
		StartKey:  append([]byte(nil), start...),
		EndKey:    append([]byte(nil), end...),
		Epoch:     epoch,
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return desc
}
