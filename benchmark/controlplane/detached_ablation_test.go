package controlplane

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	coordablation "github.com/feichai0017/NoKV/coordinator/ablation"
	"github.com/feichai0017/NoKV/coordinator/catalog"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	"github.com/feichai0017/NoKV/coordinator/tso"
	rootlocal "github.com/feichai0017/NoKV/meta/root/backend/local"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type detachedAblationVariant struct {
	name              string
	predecessorConfig coordablation.Config
	successorConfig   coordablation.Config
	clientConfig      coordablation.Config
}

type detachedSealResult struct {
	sealRecorded      bool
	campaignSucceeded bool
	confirmSucceeded  bool
	closeSucceeded    bool
	reattachRecorded  bool
}

type detachedLateReplyResult struct {
	staleReplyRejected bool
	secondReplyFirstID uint64
	secondReplyGen     uint64
}

type detachedBudgetResult struct {
	secondAllocSucceeded bool
	saveCalls            int
}

type detachedRootUnreachResult struct {
	bestEffortAllowed bool
}

type detachedAblationHarness struct {
	predecessor      *coordserver.Service
	successor        *coordserver.Service
	predecessorStore *coordstorage.RootStore
	successorStore   *coordstorage.RootStore
	stopRemote       func()
}

func mustDetachedPresetConfig(preset coordablation.Preset) coordablation.Config {
	cfg, err := preset.Config()
	if err != nil {
		panic(err)
	}
	return cfg
}

func TestControlPlaneDetachedAblationRunner(t *testing.T) {
	t.Run("seal_path", func(t *testing.T) {
		cases := []struct {
			name       string
			variant    detachedAblationVariant
			wantResult detachedSealResult
		}{
			{
				name:    "baseline",
				variant: detachedAblationVariant{name: "baseline"},
				wantResult: detachedSealResult{
					sealRecorded:      true,
					campaignSucceeded: true,
					confirmSucceeded:  true,
					closeSucceeded:    true,
					reattachRecorded:  true,
				},
			},
			{
				name: "disable_seal",
				variant: detachedAblationVariant{
					name:              "disable_seal",
					predecessorConfig: mustDetachedPresetConfig(coordablation.PresetNoSeal),
				},
				wantResult: detachedSealResult{
					sealRecorded:      false,
					campaignSucceeded: true,
					confirmSucceeded:  false,
					closeSucceeded:    false,
					reattachRecorded:  false,
				},
			},
			{
				name: "disable_reattach",
				variant: detachedAblationVariant{
					name:            "disable_reattach",
					successorConfig: mustDetachedPresetConfig(coordablation.PresetNoReattach),
				},
				wantResult: detachedSealResult{
					sealRecorded:      true,
					campaignSucceeded: true,
					confirmSucceeded:  true,
					closeSucceeded:    true,
					reattachRecorded:  false,
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := runDetachedSealScenario(t, tc.variant)
				require.Equal(t, tc.wantResult, got)
				t.Logf(
					"detached_ablation scenario=seal_path variant=%s seal_recorded=%t campaign_succeeded=%t confirm_succeeded=%t close_succeeded=%t reattach_succeeded=%t",
					tc.variant.name,
					got.sealRecorded,
					got.campaignSucceeded,
					got.confirmSucceeded,
					got.closeSucceeded,
					got.reattachRecorded,
				)
			})
		}
	})

	t.Run("late_reply", func(t *testing.T) {
		cases := []struct {
			name         string
			variant      detachedAblationVariant
			wantRejected bool
		}{
			{
				name:         "baseline",
				variant:      detachedAblationVariant{name: "baseline"},
				wantRejected: true,
			},
			{
				name: "disable_client_verify",
				variant: detachedAblationVariant{
					name:         "disable_client_verify",
					clientConfig: mustDetachedPresetConfig(coordablation.PresetClientBlind),
				},
				wantRejected: false,
			},
			{
				name: "disable_reply_evidence_disable_client_verify",
				variant: detachedAblationVariant{
					name:              "disable_reply_evidence_disable_client_verify",
					predecessorConfig: coordablation.Config{DisableReplyEvidence: true},
					successorConfig:   coordablation.Config{DisableReplyEvidence: true},
					clientConfig:      mustDetachedPresetConfig(coordablation.PresetReplyBlindClientBlind),
				},
				wantRejected: false,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := runDetachedLateReplyScenario(t, tc.variant)
				require.Equal(t, tc.wantRejected, got.staleReplyRejected)
				t.Logf(
					"detached_ablation scenario=late_reply variant=%s stale_reply_rejected=%t second_reply_first_id=%d second_reply_generation=%d",
					tc.variant.name,
					got.staleReplyRejected,
					got.secondReplyFirstID,
					got.secondReplyGen,
				)
			})
		}
	})

	t.Run("budget_exhaustion", func(t *testing.T) {
		cases := []struct {
			name       string
			variant    detachedAblationVariant
			wantResult detachedBudgetResult
		}{
			{
				name:    "baseline",
				variant: detachedAblationVariant{name: "baseline"},
				wantResult: detachedBudgetResult{
					secondAllocSucceeded: false,
					saveCalls:            2,
				},
			},
			{
				name: "disable_budget",
				variant: detachedAblationVariant{
					name:              "disable_budget",
					predecessorConfig: mustDetachedPresetConfig(coordablation.PresetNoBudget),
				},
				wantResult: detachedBudgetResult{
					secondAllocSucceeded: true,
					saveCalls:            1,
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := runDetachedBudgetScenario(t, tc.variant)
				require.Equal(t, tc.wantResult, got)
				t.Logf(
					"detached_ablation scenario=budget_exhaustion variant=%s second_alloc_succeeded=%t save_calls=%d",
					tc.variant.name,
					got.secondAllocSucceeded,
					got.saveCalls,
				)
			})
		}
	})

	t.Run("root_unreach", func(t *testing.T) {
		cases := []struct {
			name       string
			variant    detachedAblationVariant
			wantResult detachedRootUnreachResult
		}{
			{
				name:    "baseline",
				variant: detachedAblationVariant{name: "baseline"},
				wantResult: detachedRootUnreachResult{
					bestEffortAllowed: true,
				},
			},
			{
				name: "fail_stop_on_root_unreach",
				variant: detachedAblationVariant{
					name:              "fail_stop_on_root_unreach",
					predecessorConfig: mustDetachedPresetConfig(coordablation.PresetFailStopOnRootUnreach),
				},
				wantResult: detachedRootUnreachResult{
					bestEffortAllowed: false,
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := runDetachedRootUnreachScenario(t, tc.variant)
				require.Equal(t, tc.wantResult, got)
				t.Logf(
					"detached_ablation scenario=root_unreach variant=%s best_effort_allowed=%t",
					tc.variant.name,
					got.bestEffortAllowed,
				)
			})
		}
	})
}

func runDetachedSealScenario(t *testing.T, variant detachedAblationVariant) detachedSealResult {
	t.Helper()
	h := openDetachedAblationHarness(t, variant.predecessorConfig, variant.successorConfig)
	t.Cleanup(func() { h.close(t) })

	ctx := context.Background()
	_, err := h.predecessor.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	require.NoError(t, h.predecessor.SealCoordinatorLease())
	snapshot, err := h.predecessorStore.Load()
	require.NoError(t, err)
	result := detachedSealResult{sealRecorded: snapshot.CoordinatorSeal.CertGeneration != 0}

	if !result.sealRecorded {
		require.NoError(t, h.predecessor.ReleaseCoordinatorLease())
	}
	require.NoError(t, h.successor.RefreshFromStorage())

	_, err = h.successor.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	result.campaignSucceeded = err == nil
	if err != nil {
		return result
	}

	err = h.successor.ConfirmCoordinatorClosure()
	result.confirmSucceeded = err == nil
	if err != nil {
		require.NotEqual(t, codes.OK, status.Code(err), "variant=%s confirm err=%v", variant.name, err)
		require.Contains(t, err.Error(), "coordinator lease audit incomplete", "variant=%s confirm err=%v", variant.name, err)
		return result
	}

	err = h.successor.CloseCoordinatorClosure()
	result.closeSucceeded = err == nil
	require.NoError(t, err)

	err = h.successor.ReattachCoordinatorClosure()
	require.NoError(t, err)

	snapshot, err = h.successorStore.Load()
	require.NoError(t, err)
	result.confirmSucceeded = rootproto.ClosureStageAtLeast(snapshot.CoordinatorClosure.Stage, rootproto.CoordinatorClosureStageConfirmed)
	result.closeSucceeded = rootproto.ClosureStageAtLeast(snapshot.CoordinatorClosure.Stage, rootproto.CoordinatorClosureStageClosed)
	result.reattachRecorded = rootproto.ClosureStageAtLeast(snapshot.CoordinatorClosure.Stage, rootproto.CoordinatorClosureStageReattached)
	return result
}

func runDetachedLateReplyScenario(t *testing.T, variant detachedAblationVariant) detachedLateReplyResult {
	t.Helper()
	h := openDetachedAblationHarness(t, variant.predecessorConfig, variant.successorConfig)
	t.Cleanup(func() { h.close(t) })

	ctx := context.Background()
	oldResp, err := h.predecessor.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	require.NoError(t, h.predecessor.SealCoordinatorLease())
	require.NoError(t, h.successor.RefreshFromStorage())

	freshResp1, err := h.successor.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.NoError(t, h.successor.ConfirmCoordinatorClosure())
	require.NoError(t, h.successor.CloseCoordinatorClosure())
	require.NoError(t, h.successor.ReattachCoordinatorClosure())
	freshResp2, err := h.successor.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

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

	client := openDetachedAblationClient(t, []string{"fresh-primary", "late-reply", "fresh-secondary"}, map[string]coordpb.CoordinatorServer{
		"fresh-primary":   freshPrimary,
		"late-reply":      lateReply,
		"fresh-secondary": freshSecondary,
	})
	require.NoError(t, client.ConfigureAblation(variant.clientConfig))

	resp, err := client.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, freshResp1.GetFirstId(), resp.GetFirstId())

	resp, err = client.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	return detachedLateReplyResult{
		staleReplyRejected: freshSecondary.calls == 1 && resp.GetFirstId() == freshResp2.GetFirstId(),
		secondReplyFirstID: resp.GetFirstId(),
		secondReplyGen:     resp.GetCertGeneration(),
	}
}

func runDetachedBudgetScenario(t *testing.T, variant detachedAblationVariant) detachedBudgetResult {
	t.Helper()
	store := &detachedScenarioStorage{
		leader:       true,
		saveErr:      errors.New("root save unavailable"),
		saveErrAfter: 1,
		snapshot: coordstorage.Snapshot{
			CoordinatorLease: rootstate.CoordinatorLease{
				HolderID:        "c1",
				ExpiresUnixNano: time.Now().Add(20 * time.Second).UnixNano(),
				CertGeneration:  1,
				DutyMask:        rootproto.CoordinatorDutyMaskDefault,
			},
		},
	}
	svc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)
	svc.ConfigureAllocatorWindows(1, 1)
	require.NoError(t, svc.ConfigureAblation(variant.predecessorConfig))
	require.NoError(t, svc.ReloadFromStorage())

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	_, err = svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	return detachedBudgetResult{
		secondAllocSucceeded: err == nil,
		saveCalls:            store.saveCalls,
	}
}

func runDetachedRootUnreachScenario(t *testing.T, variant detachedAblationVariant) detachedRootUnreachResult {
	t.Helper()
	cluster := catalog.NewCluster()
	desc := benchmarkRoutingDescriptor()
	desc.RootEpoch = 7
	require.NoError(t, cluster.PublishRegionDescriptor(desc))

	store := &detachedScenarioStorage{
		leader:  true,
		loadErr: errors.New("root unavailable"),
	}
	svc := coordserver.NewService(cluster, idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	require.NoError(t, svc.ConfigureAblation(variant.predecessorConfig))

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("m"),
		Freshness: coordpb.Freshness_FRESHNESS_BEST_EFFORT,
	})
	return detachedRootUnreachResult{
		bestEffortAllowed: err == nil,
	}
}

func openDetachedAblationHarness(t *testing.T, predecessorCfg, successorCfg coordablation.Config) *detachedAblationHarness {
	t.Helper()
	backend, err := rootlocal.Open(t.TempDir(), nil)
	require.NoError(t, err)
	addr, stopRemote := openBenchmarkRemoteRootServerTCP(t, backend)

	openStore := func() *coordstorage.RootStore {
		store, err := coordstorage.OpenRootRemoteStore(coordstorage.RemoteRootConfig{
			Targets: map[uint64]string{1: addr},
		})
		require.NoError(t, err)
		return store
	}

	predecessorStore := openStore()
	successorStore := openStore()

	predecessorCluster := catalog.NewCluster()
	bootstrap, err := coordstorage.Bootstrap(predecessorStore, predecessorCluster.PublishRegionDescriptor, 1, 1)
	require.NoError(t, err)
	successorCluster := catalog.NewCluster()
	_, err = coordstorage.Bootstrap(successorStore, successorCluster.PublishRegionDescriptor, 1, 1)
	require.NoError(t, err)

	predecessor := coordserver.NewService(
		predecessorCluster,
		idalloc.NewIDAllocator(bootstrap.IDStart),
		tso.NewAllocator(bootstrap.TSStart),
		predecessorStore,
	)
	predecessor.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, predecessor.ConfigureAblation(predecessorCfg))
	require.NoError(t, predecessor.ReloadFromStorage())

	successor := coordserver.NewService(
		successorCluster,
		idalloc.NewIDAllocator(bootstrap.IDStart),
		tso.NewAllocator(bootstrap.TSStart),
		successorStore,
	)
	successor.ConfigureCoordinatorLease("c2", 10*time.Second, 3*time.Second)
	require.NoError(t, successor.ConfigureAblation(successorCfg))
	require.NoError(t, successor.ReloadFromStorage())

	return &detachedAblationHarness{
		predecessor:      predecessor,
		successor:        successor,
		predecessorStore: predecessorStore,
		successorStore:   successorStore,
		stopRemote:       stopRemote,
	}
}

type detachedScenarioStorage struct {
	snapshot     coordstorage.Snapshot
	loadErr      error
	saveErr      error
	saveErrAfter int
	saveCalls    int
	leader       bool
	leaderID     uint64
}

func (s *detachedScenarioStorage) Load() (coordstorage.Snapshot, error) {
	if s.loadErr != nil {
		return coordstorage.Snapshot{}, s.loadErr
	}
	return coordstorage.CloneSnapshot(s.snapshot), nil
}

func (s *detachedScenarioStorage) AppendRootEvent(ctx context.Context, event rootevent.Event) error {
	_ = ctx
	_ = event
	return nil
}

func (s *detachedScenarioStorage) SaveAllocatorState(ctx context.Context, idCurrent, tsCurrent uint64) error {
	_ = ctx
	s.saveCalls++
	if s.saveErr != nil && s.saveCalls > s.saveErrAfter {
		return s.saveErr
	}
	if idCurrent > s.snapshot.Allocator.IDCurrent {
		s.snapshot.Allocator.IDCurrent = idCurrent
	}
	if tsCurrent > s.snapshot.Allocator.TSCurrent {
		s.snapshot.Allocator.TSCurrent = tsCurrent
	}
	return nil
}

func (s *detachedScenarioStorage) ApplyCoordinatorLease(ctx context.Context, cmd rootproto.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error) {
	_ = ctx
	_ = cmd
	return rootstate.CoordinatorProtocolState{
		Lease:   s.snapshot.CoordinatorLease,
		Seal:    s.snapshot.CoordinatorSeal,
		Closure: s.snapshot.CoordinatorClosure,
	}, nil
}

func (s *detachedScenarioStorage) ApplyCoordinatorClosure(ctx context.Context, cmd rootproto.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error) {
	_ = ctx
	_ = cmd
	return rootstate.CoordinatorProtocolState{
		Lease:   s.snapshot.CoordinatorLease,
		Seal:    s.snapshot.CoordinatorSeal,
		Closure: s.snapshot.CoordinatorClosure,
	}, nil
}

func (s *detachedScenarioStorage) Refresh() error { return nil }
func (s *detachedScenarioStorage) IsLeader() bool { return s.leader || s.leaderID == 0 }
func (s *detachedScenarioStorage) LeaderID() uint64 {
	return s.leaderID
}
func (s *detachedScenarioStorage) Close() error { return nil }

func (h *detachedAblationHarness) close(t *testing.T) {
	t.Helper()
	if h == nil {
		return
	}
	if h.successorStore != nil {
		require.NoError(t, h.successorStore.Close())
	}
	if h.predecessorStore != nil {
		require.NoError(t, h.predecessorStore.Close())
	}
	if h.stopRemote != nil {
		h.stopRemote()
	}
}

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

func openDetachedAblationClient(t *testing.T, order []string, servers map[string]coordpb.CoordinatorServer) *coordclient.GRPCClient {
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
	client, err := coordclient.NewGRPCClient(
		ctx,
		strings.Join(addresses, ","),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}
