package integration_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/tso"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type protocolMatrixStorage struct {
	campaignErr error
	confirmErr  error
	closeErr    error
	reattachErr error
	leader      bool
	leaderID    uint64
	snapshot    rootview.Snapshot
	campaigns   int
	confirms    int
	closes      int
	reattaches  int
}

func (s *protocolMatrixStorage) protocolState() rootstate.SuccessionState {
	return rootstate.SuccessionState{
		Tenure:  s.snapshot.Tenure,
		Legacy:  s.snapshot.Legacy,
		Transit: s.snapshot.Transit,
	}
}

func (s *protocolMatrixStorage) Load() (rootview.Snapshot, error) {
	return rootview.CloneSnapshot(s.snapshot), nil
}

func (s *protocolMatrixStorage) AppendRootEvent(context.Context, rootevent.Event) error {
	return nil
}

func (s *protocolMatrixStorage) SaveAllocatorState(_ context.Context, idCurrent, tsCurrent uint64) error {
	if idCurrent > s.snapshot.Allocator.IDCurrent {
		s.snapshot.Allocator.IDCurrent = idCurrent
	}
	if tsCurrent > s.snapshot.Allocator.TSCurrent {
		s.snapshot.Allocator.TSCurrent = tsCurrent
	}
	return nil
}

func (s *protocolMatrixStorage) ApplyTenure(_ context.Context, cmd rootproto.TenureCommand) (rootstate.SuccessionState, error) {
	switch cmd.Kind {
	case rootproto.TenureActIssue:
		s.campaigns++
		if s.campaignErr != nil {
			return rootstate.SuccessionState{}, s.campaignErr
		}
		if err := rootstate.ValidateTenureClaim(s.snapshot.Tenure, s.snapshot.Legacy, cmd.HolderID, cmd.LineageDigest, cmd.ExpiresUnixNano, cmd.NowUnixNano); err != nil {
			return s.protocolState(), err
		}
		if err := rootstate.ValidateInheritance(s.snapshot.Tenure, s.snapshot.Legacy, cmd.InheritedFrontiers); err != nil {
			return s.protocolState(), err
		}
		generation := rootstate.NextTenureEpoch(s.snapshot.Tenure, s.snapshot.Legacy, cmd.HolderID, cmd.NowUnixNano)
		s.snapshot.Tenure = rootstate.Tenure{
			HolderID:        cmd.HolderID,
			ExpiresUnixNano: cmd.ExpiresUnixNano,
			Epoch:           generation,
			Mandate:         rootproto.MandateDefault,
			LineageDigest:   cmd.LineageDigest,
		}
		if idFence := cmd.InheritedFrontiers.Frontier(rootproto.MandateAllocID); idFence > s.snapshot.Allocator.IDCurrent {
			s.snapshot.Allocator.IDCurrent = idFence
		}
		if tsoFence := cmd.InheritedFrontiers.Frontier(rootproto.MandateTSO); tsoFence > s.snapshot.Allocator.TSCurrent {
			s.snapshot.Allocator.TSCurrent = tsoFence
		}
	case rootproto.TenureActRelease:
		if err := rootstate.ValidateTenureYield(s.snapshot.Tenure, cmd.HolderID, cmd.NowUnixNano); err != nil {
			return s.protocolState(), err
		}
		s.snapshot.Tenure = rootstate.Tenure{
			HolderID:        cmd.HolderID,
			ExpiresUnixNano: cmd.NowUnixNano,
			Epoch:           s.snapshot.Tenure.Epoch,
			IssuedAt:        s.snapshot.Tenure.IssuedAt,
			Mandate:         s.snapshot.Tenure.Mandate,
			LineageDigest:   s.snapshot.Tenure.LineageDigest,
		}
		if idFence := cmd.InheritedFrontiers.Frontier(rootproto.MandateAllocID); idFence > s.snapshot.Allocator.IDCurrent {
			s.snapshot.Allocator.IDCurrent = idFence
		}
		if tsoFence := cmd.InheritedFrontiers.Frontier(rootproto.MandateTSO); tsoFence > s.snapshot.Allocator.TSCurrent {
			s.snapshot.Allocator.TSCurrent = tsoFence
		}
	default:
		return rootstate.SuccessionState{}, rootstate.ErrInvalidTenure
	}
	return s.protocolState(), nil
}

func (s *protocolMatrixStorage) ApplyTransit(_ context.Context, cmd rootproto.TransitCommand) (rootstate.SuccessionState, error) {
	switch cmd.Kind {
	case rootproto.TransitActSeal:
		if err := rootstate.ValidateLegacyFormation(s.snapshot.Tenure, cmd.HolderID); err != nil {
			return s.protocolState(), err
		}
		mandate := s.snapshot.Tenure.Mandate
		if mandate == 0 {
			mandate = rootproto.MandateDefault
		}
		s.snapshot.Legacy = rootstate.Legacy{
			HolderID:  cmd.HolderID,
			Epoch:     s.snapshot.Tenure.Epoch,
			Mandate:   mandate,
			Frontiers: cmd.Frontiers,
		}
	case rootproto.TransitActConfirm:
		s.confirms++
		if s.confirmErr != nil {
			return rootstate.SuccessionState{}, s.confirmErr
		}
		if strings.TrimSpace(cmd.HolderID) == "" || strings.TrimSpace(cmd.HolderID) != s.snapshot.Tenure.HolderID {
			return s.protocolState(), rootstate.ErrPrimacy
		}
		auditStatus, err := succession.ValidateTransitConfirmation(
			s.snapshot.Tenure,
			succession.Frontiers(rootstate.State{
				IDFence:  s.snapshot.Allocator.IDCurrent,
				TSOFence: s.snapshot.Allocator.TSCurrent,
			}, rootstate.MaxDescriptorRevision(s.snapshot.Descriptors)),
			s.snapshot.Legacy,
			cmd.NowUnixNano,
		)
		if err != nil {
			return s.protocolState(), err
		}
		s.snapshot.Transit = rootstate.Transit{
			HolderID:       cmd.HolderID,
			LegacyEpoch:    auditStatus.LegacyEpoch,
			SuccessorEpoch: s.snapshot.Tenure.Epoch,
			LegacyDigest:   auditStatus.LegacyDigest,
			Stage:          rootproto.TransitStageConfirmed,
		}
	case rootproto.TransitActClose:
		s.closes++
		if s.closeErr != nil {
			return rootstate.SuccessionState{}, s.closeErr
		}
		if err := succession.ValidateTransitClosure(s.snapshot.Tenure, s.snapshot.Transit, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return s.protocolState(), err
		}
		s.snapshot.Transit.Stage = rootproto.TransitStageClosed
	case rootproto.TransitActReattach:
		s.reattaches++
		if s.reattachErr != nil {
			return rootstate.SuccessionState{}, s.reattachErr
		}
		if err := succession.ValidateTransitReattach(s.snapshot.Tenure, s.snapshot.Transit, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return s.protocolState(), err
		}
		s.snapshot.Transit.Stage = rootproto.TransitStageReattached
	default:
		return rootstate.SuccessionState{}, rootstate.ErrClosure
	}
	return s.protocolState(), nil
}

func (s *protocolMatrixStorage) Refresh() error { return nil }
func (s *protocolMatrixStorage) IsLeader() bool { return s == nil || s.leader || s.leaderID == 0 }
func (s *protocolMatrixStorage) LeaderID() uint64 {
	if s == nil {
		return 0
	}
	return s.leaderID
}
func (s *protocolMatrixStorage) Close() error { return nil }

type allocStep struct {
	resp *coordpb.AllocIDResponse
	err  error
}

type allocSequenceServer struct {
	coordpb.UnimplementedCoordinatorServer

	mu    sync.Mutex
	steps []allocStep
	calls int
}

func (s *allocSequenceServer) AllocID(_ context.Context, _ *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
	if len(s.steps) == 0 {
		return nil, status.Error(codes.Unavailable, "no scripted alloc response")
	}
	step := s.steps[0]
	if len(s.steps) > 1 {
		s.steps = s.steps[1:]
	}
	if step.resp == nil {
		return nil, step.err
	}
	return proto.Clone(step.resp).(*coordpb.AllocIDResponse), step.err
}

func openCoordinatorClient(t *testing.T, order []string, servers map[string]coordpb.CoordinatorServer) *coordclient.GRPCClient {
	t.Helper()

	const bufSize = 1 << 20
	listeners := make(map[string]*bufconn.Listener, len(order))
	for _, name := range order {
		srv := servers[name]
		require.NotNil(t, srv, "missing coordinator server %q", name)

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
			return nil, fmt.Errorf("unknown target %q", target)
		}
		return listener.Dial()
	}

	addresses := make([]string, 0, len(order))
	for _, name := range order {
		addresses = append(addresses, "passthrough:///"+name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := coordclient.NewGRPCClient(
		ctx,
		strings.Join(addresses, ","),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func TestDetachedProtocolFaultMatrix(t *testing.T) {
	type faultCase struct {
		name     string
		store    *protocolMatrixStorage
		run      func(t *testing.T, svc *coordserver.Service)
		diagKeys map[string]any
	}

	newService := func(store *protocolMatrixStorage) *coordserver.Service {
		svc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
		svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
		require.NoError(t, svc.ReloadFromStorage())
		return svc
	}

	baseDescriptors := map[uint64]descriptor.Descriptor{
		1: {RegionID: 1, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
	}
	activeLeaseExpiry := time.Now().Add(20 * time.Second).UnixNano()
	sealWithDescriptor7 := rootstate.Legacy{
		HolderID:  "c1",
		Epoch:     2,
		Mandate:   rootproto.MandateDefault,
		Frontiers: succession.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 7),
	}
	sealWithDescriptor7Digest := rootstate.DigestOfLegacy(sealWithDescriptor7)
	sealWithDescriptor9 := rootstate.Legacy{
		HolderID:  "c1",
		Epoch:     2,
		Mandate:   rootproto.MandateDefault,
		Frontiers: succession.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 9),
	}
	sealWithDescriptor9Digest := rootstate.DigestOfLegacy(sealWithDescriptor9)

	cases := []faultCase{
		// F.revived_holder + F.root_unreach — once the predecessor generation has
		// sealed and no covered successor is present, the old holder must fail-stop
		// for monotone duties instead of continuing on cached authority.
		{
			name: "sealed_generation_cannot_continue_monotone_without_successor",
			store: &protocolMatrixStorage{
				leader:      true,
				campaignErr: rootstate.ErrPrimacy,
				snapshot: rootview.Snapshot{
					Tenure: rootstate.Tenure{
						HolderID:        "c1",
						ExpiresUnixNano: activeLeaseExpiry,
						Epoch:           2,
						Mandate:         rootproto.MandateDefault,
					},
					Legacy: rootstate.Legacy{HolderID: "c1", Epoch: 2, Mandate: rootproto.MandateDefault},
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
				require.Error(t, err)
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
			},
			diagKeys: map[string]any{
				"closure_satisfied": false,
				"closure_stage":     "unspecified",
			},
		},
		// F.root_unreach — metadata answers must also fail-stop once the rooted
		// authority path has sealed but no successor coverage has landed.
		{
			name: "sealed_generation_cannot_continue_metadata_without_successor",
			store: &protocolMatrixStorage{
				leader:      true,
				campaignErr: rootstate.ErrPrimacy,
				snapshot: rootview.Snapshot{
					Tenure: rootstate.Tenure{
						HolderID:        "c1",
						ExpiresUnixNano: activeLeaseExpiry,
						Epoch:           2,
						Mandate:         rootproto.MandateDefault,
					},
					Legacy:      rootstate.Legacy{HolderID: "c1", Epoch: 2, Mandate: rootproto.MandateDefault, Frontiers: succession.Frontiers(rootstate.State{IDFence: 0, TSOFence: 0}, 7)},
					Descriptors: baseDescriptors,
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
				require.Error(t, err)
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
			},
			diagKeys: map[string]any{
				"closure_satisfied": false,
				"closure_stage":     "unspecified",
			},
		},
		// F.successor_campaign + F.budget_exhaustion — a successor generation may
		// campaign, but it cannot confirm closure before covering the predecessor's
		// sealed monotone frontier.
		{
			name: "confirm_rejected_before_monotone_coverage",
			store: &protocolMatrixStorage{
				leader: true,
				snapshot: rootview.Snapshot{
					Tenure: rootstate.Tenure{
						HolderID:        "c1",
						ExpiresUnixNano: activeLeaseExpiry,
						Epoch:           3,
						Mandate:         rootproto.MandateDefault,
						LineageDigest:   sealWithDescriptor7Digest,
					},
					Legacy: sealWithDescriptor7,
					Allocator: rootview.AllocatorState{
						IDCurrent: 11,
						TSCurrent: 34,
					},
					Descriptors: baseDescriptors,
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				err := svc.ConfirmTransit()
				require.ErrorIs(t, err, rootstate.ErrClosure)
			},
			diagKeys: map[string]any{
				"successor_present":            true,
				"successor_lineage_satisfied":  true,
				"successor_monotone_covered":   false,
				"successor_descriptor_covered": true,
				"closure_satisfied":            false,
				"closure_stage":                "unspecified",
			},
		},
		// F.successor_campaign + F.descriptor_publish_race — successor coverage is
		// incomplete until descriptor publication catches up with the sealed frontier.
		{
			name: "confirm_rejected_before_descriptor_coverage",
			store: &protocolMatrixStorage{
				leader: true,
				snapshot: rootview.Snapshot{
					Tenure: rootstate.Tenure{
						HolderID:        "c1",
						ExpiresUnixNano: activeLeaseExpiry,
						Epoch:           3,
						Mandate:         rootproto.MandateDefault,
						LineageDigest:   sealWithDescriptor9Digest,
					},
					Legacy: sealWithDescriptor9,
					Allocator: rootview.AllocatorState{
						IDCurrent: 12,
						TSCurrent: 34,
					},
					Descriptors: baseDescriptors,
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				err := svc.ConfirmTransit()
				require.ErrorIs(t, err, rootstate.ErrClosure)
			},
			diagKeys: map[string]any{
				"successor_present":            true,
				"successor_monotone_covered":   true,
				"successor_descriptor_covered": false,
				"closure_satisfied":            false,
				"closure_stage":                "unspecified",
			},
		},
		// F.lease_expiry + F.successor_campaign — even with a successor lease in
		// place, detached service is not yet reattached until explicit close lands.
		{
			name: "reattach_rejected_before_close",
			store: &protocolMatrixStorage{
				leader: true,
				snapshot: rootview.Snapshot{
					Tenure: rootstate.Tenure{
						HolderID:        "c1",
						ExpiresUnixNano: activeLeaseExpiry,
						Epoch:           3,
						Mandate:         rootproto.MandateDefault,
						LineageDigest:   sealWithDescriptor7Digest,
					},
					Legacy: sealWithDescriptor7,
					Allocator: rootview.AllocatorState{
						IDCurrent: 12,
						TSCurrent: 34,
					},
					Descriptors: baseDescriptors,
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				err := svc.ReattachTransit()
				require.ErrorIs(t, err, rootstate.ErrClosure)
			},
			diagKeys: map[string]any{
				"closure_stage": "unspecified",
			},
		},
		// F.revived_holder + F.successor_campaign — a successor whose lineage no
		// longer matches the sealed predecessor digest cannot reattach.
		{
			name: "reattach_rejected_on_lineage_mismatch",
			store: &protocolMatrixStorage{
				leader: true,
				snapshot: rootview.Snapshot{
					Tenure: rootstate.Tenure{
						HolderID:        "c1",
						ExpiresUnixNano: activeLeaseExpiry,
						Epoch:           3,
						Mandate:         rootproto.MandateDefault,
						LineageDigest:   "other-digest",
					},
					Legacy: sealWithDescriptor7,
					Allocator: rootview.AllocatorState{
						IDCurrent: 12,
						TSCurrent: 34,
					},
					Transit: rootstate.Transit{
						HolderID:       "c1",
						LegacyEpoch:    2,
						SuccessorEpoch: 3,
						LegacyDigest:   sealWithDescriptor7Digest,
						Stage:          rootproto.TransitStageClosed,
					},
					Descriptors: baseDescriptors,
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				err := svc.ReattachTransit()
				require.ErrorIs(t, err, rootstate.ErrClosure)
			},
			diagKeys: map[string]any{
				"closure_stage":               "unspecified",
				"successor_lineage_satisfied": false,
			},
		},
		// F.successor_campaign — once lineage, coverage, close, and reattach all
		// line up, the successor can continue detached duties with the new generation.
		{
			name: "successor_can_confirm_and_reattach_end_to_end",
			store: &protocolMatrixStorage{
				leader: true,
				snapshot: rootview.Snapshot{
					Tenure: rootstate.Tenure{
						HolderID:        "c1",
						ExpiresUnixNano: activeLeaseExpiry,
						Epoch:           3,
						Mandate:         rootproto.MandateDefault,
						LineageDigest:   sealWithDescriptor7Digest,
					},
					Legacy: sealWithDescriptor7,
					Allocator: rootview.AllocatorState{
						IDCurrent: 12,
						TSCurrent: 34,
					},
					Descriptors: baseDescriptors,
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				require.NoError(t, svc.ConfirmTransit())
				require.NoError(t, svc.CloseTransit())
				require.NoError(t, svc.ReattachTransit())
				allocResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
				require.NoError(t, err)
				require.Equal(t, uint64(3), allocResp.GetEpoch())
				resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
				require.NoError(t, err)
				require.Equal(t, uint64(3), resp.GetEpoch())
			},
			diagKeys: map[string]any{
				"closure_satisfied": true,
				"closure_stage":     "reattached",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newService(tc.store)
			tc.run(t, svc)
			diag := svc.DiagnosticsSnapshot()["audit"].(map[string]any)
			for key, expected := range tc.diagKeys {
				require.Equalf(t, expected, diag[key], "diag_key=%s audit=%v", key, diag)
			}
		})
	}
}

func TestDetachedLateReplyAfterSealRejectedByClientVerifier(t *testing.T) {
	// F.delayed_reply — a predecessor reply leaves before seal, then arrives only
	// after the client has already observed the successor generation.
	activeLeaseExpiry := time.Now().Add(20 * time.Second).UnixNano()

	staleStore := &protocolMatrixStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: activeLeaseExpiry,
				Epoch:           1,
				Mandate:         rootproto.MandateDefault,
			},
		},
	}
	staleSvc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), staleStore)
	staleSvc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, staleSvc.ReloadFromStorage())

	oldResp, err := staleSvc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), oldResp.GetEpoch())

	require.NoError(t, staleSvc.SealTenure())
	seal := staleStore.snapshot.Legacy
	legacyDigest := rootstate.DigestOfLegacy(seal)

	successorStore := &protocolMatrixStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c2",
				ExpiresUnixNano: activeLeaseExpiry,
				Epoch:           2,
				Mandate:         rootproto.MandateDefault,
				LineageDigest:   legacyDigest,
			},
			Legacy: seal,
			Allocator: rootview.AllocatorState{
				IDCurrent: seal.Frontiers.Frontier(rootproto.MandateAllocID),
				TSCurrent: seal.Frontiers.Frontier(rootproto.MandateTSO),
			},
		},
	}
	successorSvc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(seal.Frontiers.Frontier(rootproto.MandateAllocID)+1), tso.NewAllocator(100), successorStore)
	successorSvc.ConfigureTenure("c2", 10*time.Second, 3*time.Second)
	require.NoError(t, successorSvc.ReloadFromStorage())
	require.NoError(t, successorSvc.ConfirmTransit())
	require.NoError(t, successorSvc.CloseTransit())
	require.NoError(t, successorSvc.ReattachTransit())

	freshResp1, err := successorSvc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(2), freshResp1.GetEpoch())
	freshResp2, err := successorSvc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(2), freshResp2.GetEpoch())

	freshPrimary := &allocSequenceServer{
		steps: []allocStep{
			{resp: freshResp1},
			{err: status.Error(codes.Unavailable, "fresh primary temporarily unavailable")},
		},
	}
	lateReply := &allocSequenceServer{
		steps: []allocStep{
			{resp: oldResp},
		},
	}
	freshSecondary := &allocSequenceServer{
		steps: []allocStep{
			{resp: freshResp2},
		},
	}

	cli := openCoordinatorClient(t, []string{"fresh-primary", "late-reply", "fresh-secondary"}, map[string]coordpb.CoordinatorServer{
		"fresh-primary":   freshPrimary,
		"late-reply":      lateReply,
		"fresh-secondary": freshSecondary,
	})

	resp, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, freshResp1.GetFirstId(), resp.GetFirstId())
	require.Equal(t, uint64(2), resp.GetEpoch())

	resp, err = cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, freshResp2.GetFirstId(), resp.GetFirstId())
	require.Equal(t, uint64(2), resp.GetEpoch())

	require.Equal(t, 2, freshPrimary.calls)
	require.Equal(t, 1, lateReply.calls)
	require.Equal(t, 1, freshSecondary.calls)
}
