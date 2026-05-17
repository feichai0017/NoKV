// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

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
	"github.com/feichai0017/NoKV/coordinator/rootview"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/tso"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/meta/topology"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
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
	leader      bool
	leaderID    uint64
	snapshot    rootview.Snapshot
	campaigns   int
	reattaches  int
}

func (s *protocolMatrixStorage) protocolState() rootstate.EunomiaState {
	return rootstate.EunomiaState{
		ActiveGrants:      append([]rootproto.AuthorityGrant(nil), s.snapshot.ActiveGrants...),
		RetiredGrants:     append([]rootproto.GrantRetirement(nil), s.snapshot.RetiredGrants...),
		GrantInheritances: append([]rootproto.GrantInheritance(nil), s.snapshot.GrantInheritances...),
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

func (s *protocolMatrixStorage) ApplyGrant(_ context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	holderID := strings.TrimSpace(cmd.HolderID)
	switch cmd.Kind {
	case rootproto.GrantActIssue:
		if s.campaignErr != nil {
			return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, s.campaignErr
		}
		active, _ := protocolTestActiveGrantForDuties(s.snapshot, cmd.RequestedDuties)
		if active.Present() &&
			active.GrantID == strings.TrimSpace(cmd.GrantID) &&
			active.HolderID == holderID &&
			protocolTestDutiesCover(active.Duties, cmd.RequestedDuties) {
			return s.protocolState(), protocolTestGrantCertificate(active), nil
		}
		s.campaigns++
		for _, current := range s.snapshot.ActiveGrants {
			if current.HolderID != holderID && current.ActiveAt(cmd.NowUnixNano) && protocolTestDutiesOverlap(current.Duties, cmd.RequestedDuties) {
				return s.protocolState(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
			}
		}
		var era uint64 = 1
		for _, current := range s.snapshot.ActiveGrants {
			if current.Era >= era {
				era = current.Era + 1
			}
		}
		for _, retired := range s.snapshot.RetiredGrants {
			if retired.Era >= era {
				era = retired.Era + 1
			}
		}
		grantID := cmd.GrantID
		if grantID == "" {
			grantID = fmt.Sprintf("%s/%d", holderID, era)
		}
		predecessors := pendingGrantRetirementsForTest(s.snapshot, cmd.NowUnixNano, cmd.RequestedDuties)
		grant := rootproto.AuthorityGrant{
			GrantID:                grantID,
			HolderID:               holderID,
			Era:                    era,
			ExpiresUnixNano:        cmd.ExpiresUnixNano,
			IssuedRootToken:        rootproto.AuthorityRootToken{Term: s.snapshot.RootToken.Cursor.Term, Index: s.snapshot.RootToken.Cursor.Index, Revision: s.snapshot.RootToken.Revision},
			Duties:                 append([]rootproto.DutyGrant(nil), cmd.RequestedDuties...),
			PredecessorRetirements: append([]rootproto.GrantRetirement(nil), predecessors...),
		}
		for _, predecessor := range predecessors {
			s.snapshot.ActiveGrants = protocolTestRemoveGrant(s.snapshot.ActiveGrants, predecessor.GrantID)
		}
		s.snapshot.ActiveGrants = protocolTestUpsertGrant(s.snapshot.ActiveGrants, grant)
		s.snapshot.RetiredGrants = append([]rootproto.GrantRetirement(nil), predecessors...)
		for _, duty := range cmd.RequestedDuties {
			if duty.Bound.Kind != rootproto.DutyBoundMonotone {
				continue
			}
			switch duty.DutyID {
			case rootproto.DutyAllocID:
				if duty.Bound.MonotoneUpper > s.snapshot.Allocator.IDCurrent {
					s.snapshot.Allocator.IDCurrent = duty.Bound.MonotoneUpper
				}
			case rootproto.DutyTSO:
				if duty.Bound.MonotoneUpper > s.snapshot.Allocator.TSCurrent {
					s.snapshot.Allocator.TSCurrent = duty.Bound.MonotoneUpper
				}
			}
		}
		s.advanceRootToken()
		return s.protocolState(), protocolTestGrantCertificate(grant), nil
	case rootproto.GrantActSeal:
		active, ok := s.snapshot.ActiveGrantByID(strings.TrimSpace(cmd.GrantID))
		if !ok || active.HolderID != strings.TrimSpace(cmd.HolderID) {
			return s.protocolState(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		s.snapshot.RetiredGrants = append(s.snapshot.RetiredGrants, rootproto.GrantRetirement{
			GrantID:  active.GrantID,
			HolderID: active.HolderID,
			Era:      active.Era,
			Mode:     rootproto.GrantRetirementSealedExact,
			Bounds:   dutyGrantsFromUsagesForProtocolTest(cmd.ExactUsages),
		})
		s.snapshot.ActiveGrants = protocolTestRemoveGrant(s.snapshot.ActiveGrants, active.GrantID)
		s.advanceRootToken()
		return s.protocolState(), rootproto.GrantCertificate{}, nil
	case rootproto.GrantActInherit:
		s.reattaches++
		successor, ok := protocolTestActiveGrantForHolder(s.snapshot, holderID)
		if !ok {
			return s.protocolState(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		for _, predecessor := range cmd.PredecessorGrantIDs {
			for i := range s.snapshot.RetiredGrants {
				if s.snapshot.RetiredGrants[i].GrantID == predecessor {
					s.snapshot.RetiredGrants[i].InheritedByGrantID = successor.GrantID
					s.snapshot.GrantInheritances = append(s.snapshot.GrantInheritances, rootproto.GrantInheritance{
						PredecessorGrantID: predecessor,
						SuccessorGrantID:   successor.GrantID,
					})
				}
			}
		}
		s.advanceRootToken()
		return s.protocolState(), rootproto.GrantCertificate{}, nil
	default:
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, rootstate.ErrInvalidGrant
	}
}

func (s *protocolMatrixStorage) ApplyVisibleAuthority(context.Context, rootproto.VisibleAuthorityCommand) (rootstate.State, rootproto.VisibleAuthorityGrant, error) {
	return rootstate.CloneState(s.snapshot.RootSnapshot().State), rootproto.VisibleAuthorityGrant{}, rootstate.ErrInvalidGrant
}

func (s *protocolMatrixStorage) advanceRootToken() {
	if s.snapshot.RootToken.Cursor.Term == 0 {
		s.snapshot.RootToken.Cursor.Term = 1
	}
	s.snapshot.RootToken.Cursor.Index++
	s.snapshot.RootToken.Revision++
}

func pendingGrantRetirementsForTest(snapshot rootview.Snapshot, nowUnixNano int64, duties []rootproto.DutyGrant) []rootproto.GrantRetirement {
	var out []rootproto.GrantRetirement
	for _, grant := range snapshot.ActiveGrants {
		if !grant.Present() || !protocolTestDutiesOverlap(grant.Duties, duties) {
			continue
		}
		mode := rootproto.GrantRetirementSealedExact
		if !grant.ActiveAt(nowUnixNano) {
			mode = rootproto.GrantRetirementExpiredBound
		}
		out = append(out, rootproto.GrantRetirement{
			GrantID:  grant.GrantID,
			HolderID: grant.HolderID,
			Era:      grant.Era,
			Mode:     mode,
			Bounds:   append([]rootproto.DutyGrant(nil), grant.Duties...),
		})
	}
	for _, retired := range snapshot.RetiredGrants {
		if retired.InheritedByGrantID == "" {
			out = append(out, retired)
		}
	}
	return out
}

func dutyGrantsFromUsagesForProtocolTest(usages []rootproto.AuthorityUsage) []rootproto.DutyGrant {
	out := make([]rootproto.DutyGrant, 0, len(usages))
	for _, usage := range usages {
		out = append(out, rootproto.DutyGrant{DutyID: usage.DutyID, Scope: usage.Scope, Bound: usage.Usage})
	}
	return out
}

func protocolTestGrantCertificate(grant rootproto.AuthorityGrant) rootproto.GrantCertificate {
	payload, _ := proto.MarshalOptions{Deterministic: true}.Marshal(metawire.RootAuthorityGrantToProto(grant))
	return rootproto.GrantCertificate{
		Grant:       grant,
		SignerKeyID: rootproto.GrantSignerKeyID,
		Signature:   rootproto.SignGrantBytes(payload),
	}
}

func protocolTestDutiesCover(grants, usages []rootproto.DutyGrant) bool {
	for _, usage := range usages {
		found := false
		for _, grant := range grants {
			if grant.DutyID == usage.DutyID && rootproto.ScopeEqual(grant.Scope, usage.Scope) && rootproto.DutyBoundCovers(grant.Bound, usage.Bound) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func protocolTestDutiesOverlap(left, right []rootproto.DutyGrant) bool {
	for _, a := range left {
		for _, b := range right {
			if rootproto.DutyKeyEqual(a.Key(), b.Key()) {
				return true
			}
		}
	}
	return false
}

func protocolTestActiveGrantForDuties(snapshot rootview.Snapshot, duties []rootproto.DutyGrant) (rootproto.AuthorityGrant, bool) {
	for _, grant := range snapshot.ActiveGrants {
		if protocolTestDutiesOverlap(grant.Duties, duties) {
			return grant, true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func protocolTestActiveGrantForHolder(snapshot rootview.Snapshot, holderID string) (rootproto.AuthorityGrant, bool) {
	for _, grant := range snapshot.ActiveGrants {
		if strings.TrimSpace(grant.HolderID) == strings.TrimSpace(holderID) {
			return grant, true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func protocolTestUpsertGrant(grants []rootproto.AuthorityGrant, grant rootproto.AuthorityGrant) []rootproto.AuthorityGrant {
	out := append([]rootproto.AuthorityGrant(nil), grants...)
	for i := range out {
		if out[i].GrantID == grant.GrantID {
			out[i] = grant
			return out
		}
	}
	return append(out, grant)
}

func protocolTestRemoveGrant(grants []rootproto.AuthorityGrant, grantID string) []rootproto.AuthorityGrant {
	out := grants[:0]
	for _, grant := range grants {
		if grant.GrantID != grantID {
			out = append(out, grant)
		}
	}
	return out
}

func (s *protocolMatrixStorage) Refresh() error { return nil }
func (s *protocolMatrixStorage) CanSubmitRootWrites() bool {
	return s == nil || s.leader || s.leaderID == 0
}
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

func TestDetachedGrantFaultMatrix(t *testing.T) {
	activeExpiry := time.Now().Add(20 * time.Second).UnixNano()
	descriptor := topology.Descriptor{RegionID: 1, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7}
	grant := func(holder string, era uint64, duties ...rootproto.DutyGrant) rootproto.AuthorityGrant {
		return rootproto.AuthorityGrant{
			GrantID:         fmt.Sprintf("%s/%d", holder, era),
			HolderID:        holder,
			Era:             era,
			ExpiresUnixNano: activeExpiry,
			Duties:          duties,
		}
	}

	t.Run("other_active_grant_rejects_local_detached_duty", func(t *testing.T) {
		store := &protocolMatrixStorage{
			leader: true,
			snapshot: rootview.Snapshot{
				ActiveGrants: []rootproto.AuthorityGrant{grant("c2", 1, rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20))},
			},
		}
		svc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
		svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
		require.NoError(t, svc.ReloadFromStorage())

		_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("metadata_descriptor_outside_grant_renews", func(t *testing.T) {
		store := &protocolMatrixStorage{
			leader: true,
			snapshot: rootview.Snapshot{
				ActiveGrants: []rootproto.AuthorityGrant{grant("c1", 1,
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20),
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 120),
					rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 6, 0),
				)},
				Descriptors: map[uint64]topology.Descriptor{1: descriptor},
			},
		}
		cluster := catalog.NewCluster()
		cluster.ReplaceRootSnapshot(rootstate.Snapshot{Descriptors: store.snapshot.Descriptors}, store.snapshot.RootToken)
		svc := coordserver.NewService(cluster, idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
		svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
		require.NoError(t, svc.ReloadFromStorage())

		resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
		require.NoError(t, err)
		require.Equal(t, uint64(1), resp.GetRegionDescriptor().GetRegionId())
		require.Equal(t, uint64(7), resp.GetDescriptorRevision())
		require.NotNil(t, resp.GetAuthorityEvidence())
		require.Equal(t, 1, store.campaigns)
	})

	t.Run("successor_inherits_retired_grant_bound", func(t *testing.T) {
		retired := rootproto.GrantRetirement{
			GrantID:  "c1/1",
			HolderID: "c1",
			Era:      1,
			Mode:     rootproto.GrantRetirementExpiredBound,
			Bounds:   []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20)},
		}
		store := &protocolMatrixStorage{
			leader: true,
			snapshot: rootview.Snapshot{
				ActiveGrants:  []rootproto.AuthorityGrant{grant("c2", 2, rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 40))},
				RetiredGrants: []rootproto.GrantRetirement{retired},
			},
		}
		svc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(21), tso.NewAllocator(100), store)
		svc.ConfigureAuthorityGrant("c2", 10*time.Second, 3*time.Second)
		require.NoError(t, svc.ReloadFromStorage())
		require.NoError(t, svc.InheritRetiredGrants(context.Background()))

		diag := svc.DiagnosticsSnapshot()["audit"].(map[string]any)
		require.Equal(t, true, diag["expired_bound_inherited"])
		require.Equal(t, false, diag["retired_not_inherited"])
	})
}

func TestDetachedLateReplyAfterRetiredGrantRejectedByClientVerifier(t *testing.T) {
	activeExpiry := time.Now().Add(20 * time.Second).UnixNano()
	c1Grant := rootproto.AuthorityGrant{
		GrantID:         "c1/1",
		HolderID:        "c1",
		ExpiresUnixNano: activeExpiry,
		Era:             1,
		Duties: []rootproto.DutyGrant{
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20),
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 120),
			rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 0, 0),
		},
	}
	staleStore := &protocolMatrixStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{c1Grant},
		},
	}
	staleSvc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), staleStore)
	staleSvc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, staleSvc.ReloadFromStorage())

	oldResp, err := staleSvc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), oldResp.GetEra())
	require.NotNil(t, oldResp.GetAuthorityEvidence())

	retired := rootproto.GrantRetirement{
		GrantID:            c1Grant.GrantID,
		HolderID:           c1Grant.HolderID,
		Era:                c1Grant.Era,
		Mode:               rootproto.GrantRetirementExpiredBound,
		Bounds:             append([]rootproto.DutyGrant(nil), c1Grant.Duties...),
		InheritedByGrantID: "c2/2",
	}
	successorStore := &protocolMatrixStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c2/2",
				HolderID:        "c2",
				ExpiresUnixNano: activeExpiry,
				Era:             2,
				Duties: []rootproto.DutyGrant{
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 40),
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 140),
					rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 0, 0),
				},
				PredecessorRetirements: []rootproto.GrantRetirement{retired},
			}},
			RetiredGrants: []rootproto.GrantRetirement{retired},
		},
	}
	successorSvc := coordserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(21), tso.NewAllocator(100), successorStore)
	successorSvc.ConfigureAuthorityGrant("c2", 10*time.Second, 3*time.Second)
	require.NoError(t, successorSvc.ReloadFromStorage())

	freshResp1, err := successorSvc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(2), freshResp1.GetEra())
	freshResp2, err := successorSvc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(2), freshResp2.GetEra())

	freshPrimary := &allocSequenceServer{
		steps: []allocStep{
			{resp: freshResp1},
			{err: status.Error(codes.Unavailable, "fresh primary temporarily unavailable")},
		},
	}
	lateReply := &allocSequenceServer{steps: []allocStep{{resp: oldResp}}}
	freshSecondary := &allocSequenceServer{steps: []allocStep{{resp: freshResp2}}}

	cli := openCoordinatorClient(t, []string{"fresh-primary", "late-reply", "fresh-secondary"}, map[string]coordpb.CoordinatorServer{
		"fresh-primary":   freshPrimary,
		"late-reply":      lateReply,
		"fresh-secondary": freshSecondary,
	})

	resp, err := cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, freshResp1.GetFirstId(), resp.GetFirstId())
	require.Equal(t, uint64(2), resp.GetEra())

	resp, err = cli.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, freshResp2.GetFirstId(), resp.GetFirstId())
	require.Equal(t, uint64(2), resp.GetEra())

	require.Equal(t, 2, freshPrimary.calls)
	require.Equal(t, 1, lateReply.calls)
	require.Equal(t, 1, freshSecondary.calls)
}
