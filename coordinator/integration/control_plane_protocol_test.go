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
	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
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
	snapshot    coordstorage.Snapshot
	campaigns   int
	confirms    int
	closes      int
	reattaches  int
}

func (s *protocolMatrixStorage) protocolState() rootstate.CoordinatorProtocolState {
	return rootstate.CoordinatorProtocolState{
		Lease:   s.snapshot.CoordinatorLease,
		Seal:    s.snapshot.CoordinatorSeal,
		Closure: s.snapshot.CoordinatorClosure,
	}
}

func (s *protocolMatrixStorage) Load() (coordstorage.Snapshot, error) {
	return coordstorage.CloneSnapshot(s.snapshot), nil
}

func (s *protocolMatrixStorage) AppendRootEvent(rootevent.Event) error {
	return nil
}

func (s *protocolMatrixStorage) SaveAllocatorState(idCurrent, tsCurrent uint64) error {
	if idCurrent > s.snapshot.Allocator.IDCurrent {
		s.snapshot.Allocator.IDCurrent = idCurrent
	}
	if tsCurrent > s.snapshot.Allocator.TSCurrent {
		s.snapshot.Allocator.TSCurrent = tsCurrent
	}
	return nil
}

func (s *protocolMatrixStorage) ApplyCoordinatorLease(cmd rootstate.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error) {
	switch cmd.Kind {
	case rootstate.CoordinatorLeaseCommandIssue:
		s.campaigns++
		if s.campaignErr != nil {
			return rootstate.CoordinatorProtocolState{}, s.campaignErr
		}
		if err := rootstate.ValidateCoordinatorLeaseCampaign(s.snapshot.CoordinatorLease, s.snapshot.CoordinatorSeal, cmd.HolderID, cmd.PredecessorDigest, cmd.ExpiresUnixNano, cmd.NowUnixNano); err != nil {
			return s.protocolState(), err
		}
		if err := rootstate.ValidateCoordinatorLeaseSuccessorCoverageFrontiers(s.snapshot.CoordinatorLease, s.snapshot.CoordinatorSeal, cmd.HandoffFrontiers); err != nil {
			return s.protocolState(), err
		}
		generation := rootstate.NextCoordinatorLeaseGeneration(s.snapshot.CoordinatorLease, s.snapshot.CoordinatorSeal, cmd.HolderID, cmd.NowUnixNano)
		s.snapshot.CoordinatorLease = rootstate.CoordinatorLease{
			HolderID:          cmd.HolderID,
			ExpiresUnixNano:   cmd.ExpiresUnixNano,
			CertGeneration:    generation,
			DutyMask:          rootproto.CoordinatorDutyMaskDefault,
			PredecessorDigest: cmd.PredecessorDigest,
		}
		if idFence := cmd.HandoffFrontiers.Frontier(rootproto.CoordinatorDutyAllocID); idFence > s.snapshot.Allocator.IDCurrent {
			s.snapshot.Allocator.IDCurrent = idFence
		}
		if tsoFence := cmd.HandoffFrontiers.Frontier(rootproto.CoordinatorDutyTSO); tsoFence > s.snapshot.Allocator.TSCurrent {
			s.snapshot.Allocator.TSCurrent = tsoFence
		}
	case rootstate.CoordinatorLeaseCommandRelease:
		if err := rootstate.ValidateCoordinatorLeaseRelease(s.snapshot.CoordinatorLease, cmd.HolderID, cmd.NowUnixNano); err != nil {
			return s.protocolState(), err
		}
		s.snapshot.CoordinatorLease = rootstate.CoordinatorLease{
			HolderID:          cmd.HolderID,
			ExpiresUnixNano:   cmd.NowUnixNano,
			CertGeneration:    s.snapshot.CoordinatorLease.CertGeneration,
			IssuedCursor:      s.snapshot.CoordinatorLease.IssuedCursor,
			DutyMask:          s.snapshot.CoordinatorLease.DutyMask,
			PredecessorDigest: s.snapshot.CoordinatorLease.PredecessorDigest,
		}
		if idFence := cmd.HandoffFrontiers.Frontier(rootproto.CoordinatorDutyAllocID); idFence > s.snapshot.Allocator.IDCurrent {
			s.snapshot.Allocator.IDCurrent = idFence
		}
		if tsoFence := cmd.HandoffFrontiers.Frontier(rootproto.CoordinatorDutyTSO); tsoFence > s.snapshot.Allocator.TSCurrent {
			s.snapshot.Allocator.TSCurrent = tsoFence
		}
	default:
		return rootstate.CoordinatorProtocolState{}, rootstate.ErrInvalidCoordinatorLease
	}
	return s.protocolState(), nil
}

func (s *protocolMatrixStorage) ApplyCoordinatorClosure(cmd rootstate.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error) {
	switch cmd.Kind {
	case rootstate.CoordinatorClosureCommandSeal:
		if err := rootstate.ValidateCoordinatorLeaseSeal(s.snapshot.CoordinatorLease, cmd.HolderID); err != nil {
			return s.protocolState(), err
		}
		dutyMask := s.snapshot.CoordinatorLease.DutyMask
		if dutyMask == 0 {
			dutyMask = rootproto.CoordinatorDutyMaskDefault
		}
		s.snapshot.CoordinatorSeal = rootstate.CoordinatorSeal{
			HolderID:       cmd.HolderID,
			CertGeneration: s.snapshot.CoordinatorLease.CertGeneration,
			DutyMask:       dutyMask,
			Frontiers:      rootproto.CloneDutyFrontiers(cmd.Frontiers),
		}
	case rootstate.CoordinatorClosureCommandConfirm:
		s.confirms++
		if s.confirmErr != nil {
			return rootstate.CoordinatorProtocolState{}, s.confirmErr
		}
		if strings.TrimSpace(cmd.HolderID) == "" || strings.TrimSpace(cmd.HolderID) != s.snapshot.CoordinatorLease.HolderID {
			return s.protocolState(), rootstate.ErrCoordinatorLeaseOwner
		}
		auditStatus, err := controlplane.ValidateClosureConfirmation(
			s.snapshot.CoordinatorLease,
			controlplane.Frontiers(rootstate.State{
				IDFence:  s.snapshot.Allocator.IDCurrent,
				TSOFence: s.snapshot.Allocator.TSCurrent,
			}, rootstate.MaxDescriptorRevision(s.snapshot.Descriptors)),
			s.snapshot.CoordinatorSeal,
			cmd.NowUnixNano,
		)
		if err != nil {
			return s.protocolState(), err
		}
		s.snapshot.CoordinatorClosure = rootstate.CoordinatorClosure{
			HolderID:            cmd.HolderID,
			SealGeneration:      auditStatus.SealGeneration,
			SuccessorGeneration: s.snapshot.CoordinatorLease.CertGeneration,
			SealDigest:          auditStatus.SealDigest,
			Stage:               rootproto.CoordinatorClosureStageConfirmed,
		}
	case rootstate.CoordinatorClosureCommandClose:
		s.closes++
		if s.closeErr != nil {
			return rootstate.CoordinatorProtocolState{}, s.closeErr
		}
		if err := controlplane.ValidateClosureClose(s.snapshot.CoordinatorLease, s.snapshot.CoordinatorClosure, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return s.protocolState(), err
		}
		s.snapshot.CoordinatorClosure.Stage = rootproto.CoordinatorClosureStageClosed
	case rootstate.CoordinatorClosureCommandReattach:
		s.reattaches++
		if s.reattachErr != nil {
			return rootstate.CoordinatorProtocolState{}, s.reattachErr
		}
		if err := controlplane.ValidateClosureReattach(s.snapshot.CoordinatorLease, s.snapshot.CoordinatorClosure, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return s.protocolState(), err
		}
		s.snapshot.CoordinatorClosure.Stage = rootproto.CoordinatorClosureStageReattached
	default:
		return rootstate.CoordinatorProtocolState{}, rootstate.ErrCoordinatorLeaseAudit
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
		svc.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)
		require.NoError(t, svc.ReloadFromStorage())
		return svc
	}

	baseDescriptors := map[uint64]descriptor.Descriptor{
		1: {RegionID: 1, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
	}
	activeLeaseExpiry := time.Now().Add(20 * time.Second).UnixNano()
	sealWithDescriptor7 := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 2,
		DutyMask:       rootproto.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 7),
	}
	sealWithDescriptor7Digest := rootstate.CoordinatorSealDigest(sealWithDescriptor7)
	sealWithDescriptor9 := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 2,
		DutyMask:       rootproto.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 9),
	}
	sealWithDescriptor9Digest := rootstate.CoordinatorSealDigest(sealWithDescriptor9)

	cases := []faultCase{
		// F.revived_holder + F.root_unreach — once the predecessor generation has
		// sealed and no covered successor is present, the old holder must fail-stop
		// for monotone duties instead of continuing on cached authority.
		{
			name: "sealed_generation_cannot_continue_monotone_without_successor",
			store: &protocolMatrixStorage{
				leader:      true,
				campaignErr: rootstate.ErrCoordinatorLeaseHeld,
				snapshot: coordstorage.Snapshot{
					CoordinatorLease: rootstate.CoordinatorLease{
						HolderID:        "c1",
						ExpiresUnixNano: activeLeaseExpiry,
						CertGeneration:  2,
						DutyMask:        rootproto.CoordinatorDutyMaskDefault,
					},
					CoordinatorSeal: rootstate.CoordinatorSeal{HolderID: "c1", CertGeneration: 2, DutyMask: rootproto.CoordinatorDutyMaskDefault},
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
				campaignErr: rootstate.ErrCoordinatorLeaseHeld,
				snapshot: coordstorage.Snapshot{
					CoordinatorLease: rootstate.CoordinatorLease{
						HolderID:        "c1",
						ExpiresUnixNano: activeLeaseExpiry,
						CertGeneration:  2,
						DutyMask:        rootproto.CoordinatorDutyMaskDefault,
					},
					CoordinatorSeal: rootstate.CoordinatorSeal{HolderID: "c1", CertGeneration: 2, DutyMask: rootproto.CoordinatorDutyMaskDefault, Frontiers: controlplane.Frontiers(rootstate.State{IDFence: 0, TSOFence: 0}, 7)},
					Descriptors:     baseDescriptors,
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
				snapshot: coordstorage.Snapshot{
					CoordinatorLease: rootstate.CoordinatorLease{
						HolderID:          "c1",
						ExpiresUnixNano:   activeLeaseExpiry,
						CertGeneration:    3,
						DutyMask:          rootproto.CoordinatorDutyMaskDefault,
						PredecessorDigest: sealWithDescriptor7Digest,
					},
					CoordinatorSeal: sealWithDescriptor7,
					Allocator: coordstorage.AllocatorState{
						IDCurrent: 11,
						TSCurrent: 34,
					},
					Descriptors: baseDescriptors,
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				err := svc.ConfirmCoordinatorClosure()
				require.ErrorIs(t, err, rootstate.ErrCoordinatorLeaseAudit)
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
				snapshot: coordstorage.Snapshot{
					CoordinatorLease: rootstate.CoordinatorLease{
						HolderID:          "c1",
						ExpiresUnixNano:   activeLeaseExpiry,
						CertGeneration:    3,
						DutyMask:          rootproto.CoordinatorDutyMaskDefault,
						PredecessorDigest: sealWithDescriptor9Digest,
					},
					CoordinatorSeal: sealWithDescriptor9,
					Allocator: coordstorage.AllocatorState{
						IDCurrent: 12,
						TSCurrent: 34,
					},
					Descriptors: baseDescriptors,
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				err := svc.ConfirmCoordinatorClosure()
				require.ErrorIs(t, err, rootstate.ErrCoordinatorLeaseAudit)
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
				snapshot: coordstorage.Snapshot{
					CoordinatorLease: rootstate.CoordinatorLease{
						HolderID:          "c1",
						ExpiresUnixNano:   activeLeaseExpiry,
						CertGeneration:    3,
						DutyMask:          rootproto.CoordinatorDutyMaskDefault,
						PredecessorDigest: sealWithDescriptor7Digest,
					},
					CoordinatorSeal: sealWithDescriptor7,
					Allocator: coordstorage.AllocatorState{
						IDCurrent: 12,
						TSCurrent: 34,
					},
					Descriptors: baseDescriptors,
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				err := svc.ReattachCoordinatorClosure()
				require.ErrorIs(t, err, rootstate.ErrCoordinatorLeaseReattach)
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
				snapshot: coordstorage.Snapshot{
					CoordinatorLease: rootstate.CoordinatorLease{
						HolderID:          "c1",
						ExpiresUnixNano:   activeLeaseExpiry,
						CertGeneration:    3,
						DutyMask:          rootproto.CoordinatorDutyMaskDefault,
						PredecessorDigest: "other-digest",
					},
					CoordinatorSeal: sealWithDescriptor7,
					Allocator: coordstorage.AllocatorState{
						IDCurrent: 12,
						TSCurrent: 34,
					},
					CoordinatorClosure: rootstate.CoordinatorClosure{
						HolderID:            "c1",
						SealGeneration:      2,
						SuccessorGeneration: 3,
						SealDigest:          sealWithDescriptor7Digest,
						Stage:               rootproto.CoordinatorClosureStageClosed,
					},
					Descriptors: baseDescriptors,
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				err := svc.ReattachCoordinatorClosure()
				require.ErrorIs(t, err, rootstate.ErrCoordinatorLeaseReattach)
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
				snapshot: coordstorage.Snapshot{
					CoordinatorLease: rootstate.CoordinatorLease{
						HolderID:          "c1",
						ExpiresUnixNano:   activeLeaseExpiry,
						CertGeneration:    3,
						DutyMask:          rootproto.CoordinatorDutyMaskDefault,
						PredecessorDigest: sealWithDescriptor7Digest,
					},
					CoordinatorSeal: sealWithDescriptor7,
					Allocator: coordstorage.AllocatorState{
						IDCurrent: 12,
						TSCurrent: 34,
					},
					Descriptors: baseDescriptors,
				},
			},
			run: func(t *testing.T, svc *coordserver.Service) {
				require.NoError(t, svc.ConfirmCoordinatorClosure())
				require.NoError(t, svc.CloseCoordinatorClosure())
				require.NoError(t, svc.ReattachCoordinatorClosure())
				allocResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
				require.NoError(t, err)
				require.Equal(t, uint64(3), allocResp.GetCertGeneration())
				resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
				require.NoError(t, err)
				require.Equal(t, uint64(3), resp.GetCertGeneration())
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
		snapshot: coordstorage.Snapshot{
			CoordinatorLease: rootstate.CoordinatorLease{
				HolderID:        "c1",
				ExpiresUnixNano: activeLeaseExpiry,
				CertGeneration:  1,
				DutyMask:        rootproto.CoordinatorDutyMaskDefault,
			},
		},
	}
	staleSvc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), staleStore)
	staleSvc.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, staleSvc.ReloadFromStorage())

	oldResp, err := staleSvc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), oldResp.GetCertGeneration())

	require.NoError(t, staleSvc.SealCoordinatorLease())
	seal := staleStore.snapshot.CoordinatorSeal
	sealDigest := rootstate.CoordinatorSealDigest(seal)

	successorStore := &protocolMatrixStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			CoordinatorLease: rootstate.CoordinatorLease{
				HolderID:          "c2",
				ExpiresUnixNano:   activeLeaseExpiry,
				CertGeneration:    2,
				DutyMask:          rootproto.CoordinatorDutyMaskDefault,
				PredecessorDigest: sealDigest,
			},
			CoordinatorSeal: seal,
			Allocator: coordstorage.AllocatorState{
				IDCurrent: seal.Frontiers.Frontier(rootproto.CoordinatorDutyAllocID),
				TSCurrent: seal.Frontiers.Frontier(rootproto.CoordinatorDutyTSO),
			},
		},
	}
	successorSvc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(seal.Frontiers.Frontier(rootproto.CoordinatorDutyAllocID)+1), tso.NewAllocator(100), successorStore)
	successorSvc.ConfigureCoordinatorLease("c2", 10*time.Second, 3*time.Second)
	require.NoError(t, successorSvc.ReloadFromStorage())
	require.NoError(t, successorSvc.ConfirmCoordinatorClosure())
	require.NoError(t, successorSvc.CloseCoordinatorClosure())
	require.NoError(t, successorSvc.ReattachCoordinatorClosure())

	freshResp1, err := successorSvc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(2), freshResp1.GetCertGeneration())
	freshResp2, err := successorSvc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(2), freshResp2.GetCertGeneration())

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
	require.Equal(t, uint64(2), resp.GetCertGeneration())

	resp, err = cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, freshResp2.GetFirstId(), resp.GetFirstId())
	require.Equal(t, uint64(2), resp.GetCertGeneration())

	require.Equal(t, 2, freshPrimary.calls)
	require.Equal(t, 1, lateReply.calls)
	require.Equal(t, 1, freshSecondary.calls)
}
