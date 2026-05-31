// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/rootview"
	nokverrors "github.com/feichai0017/NoKV/errors"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/meta/topology"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/tso"
)

type fakeStorage struct {
	mu                          sync.Mutex
	eventCalls                  int
	saveCalls                   int
	loadCalls                   int
	campaignCalls               int
	sealCalls                   int
	reattachCalls               int
	eventErr                    error
	saveErr                     error
	loadErr                     error
	campaignErr                 error
	sealErr                     error
	applyVisibleAuthorityErr    error
	lastID                      uint64
	lastTS                      uint64
	leader                      bool
	leaderID                    uint64
	lastEvent                   rootevent.Event
	lastVisibleAuthorityCommand rootproto.VisibleAuthorityCommand
	applyVisibleAuthorityCalls  int
	snapshot                    rootview.Snapshot
}

func TestTranslateGrantErrorsAsGrantNotHeld(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		contains string
	}{
		{
			name:     "primacy",
			err:      translateGrantError(rootstate.ErrPrimacy),
			contains: rootstate.ErrPrimacy.Error(),
		},
		{
			name:     "expired",
			err:      statusGrant(fmt.Errorf("%w: rooted grant expired era=7", rootstate.ErrInvalidGrant)),
			contains: "rooted grant expired era=7",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, codes.FailedPrecondition, status.Code(tc.err))
			message := status.Convert(tc.err).Message()
			require.Contains(t, message, diagnosticGrantNotHeld)
			require.Contains(t, message, tc.contains)
			require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(tc.err))
			_, metadata, ok := nokverrors.RPCErrorInfo(tc.err)
			require.True(t, ok)
			require.Equal(t, reasonGrantNotHeld, metadata[coordinatorReasonMetadata])
		})
	}
}

func TestTranslateGrantContextErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "canceled", err: translateGrantError(context.Canceled), code: codes.Canceled},
		{name: "deadline", err: translateGrantError(context.DeadlineExceeded), code: codes.DeadlineExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.code, status.Code(tc.err))
		})
	}
}

func TestRegionLookupGrantRevisionIgnoresRootOnlyEvents(t *testing.T) {
	cluster := catalog.NewCluster()
	desc := testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	desc.RootEpoch = 7
	require.NoError(t, cluster.PublishRootEvent(rootevent.RegionBootstrapped(desc)))
	storage := &fakeStorage{snapshot: rootview.Snapshot{
		RootToken: rootstorage.TailToken{
			Cursor:   rootstate.Cursor{Term: 1, Index: 100},
			Revision: 100,
		},
		Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
	}}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)

	require.Equal(t, uint64(7), svc.currentRegionLookupRevision())
}

func (f *fakeStorage) protocolState() rootstate.EunomiaState {
	return rootstate.EunomiaState{
		ActiveGrants:      append([]rootproto.AuthorityGrant(nil), f.snapshot.ActiveGrants...),
		RetiredGrants:     append([]rootproto.GrantRetirement(nil), f.snapshot.RetiredGrants...),
		GrantInheritances: append([]rootproto.GrantInheritance(nil), f.snapshot.GrantInheritances...),
		RetiredEraFloors:  rootproto.CloneAuthorityRetiredEraFloors(f.snapshot.RetiredEraFloors),
	}
}

func testGrantCertificate(grant rootproto.AuthorityGrant) rootproto.GrantCertificate {
	payload, _ := proto.MarshalOptions{Deterministic: true}.Marshal(metawire.RootAuthorityGrantToProto(grant))
	return rootproto.GrantCertificate{
		Grant:       grant,
		SignerKeyID: rootproto.GrantSignerKeyID,
		Signature:   rootproto.SignGrantBytes(payload),
	}
}

func testDutyGrantsCover(grants, usages []rootproto.DutyGrant) bool {
	for _, usage := range usages {
		found := false
		for _, grant := range grants {
			if grant.DutyID == usage.DutyID &&
				rootproto.ScopeEqual(grant.Scope, usage.Scope) &&
				rootproto.DutyBoundCovers(grant.Bound, usage.Bound) {
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

func testActiveGrantForDuties(snapshot rootview.Snapshot, duties []rootproto.DutyGrant) (rootproto.AuthorityGrant, bool) {
	for _, grant := range snapshot.ActiveGrants {
		for _, duty := range duties {
			if grant.CoversDutyKey(duty.Key()) {
				return grant, true
			}
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func testActiveGrantForHolder(snapshot rootview.Snapshot, holderID string) (rootproto.AuthorityGrant, bool) {
	for _, grant := range snapshot.ActiveGrants {
		if strings.TrimSpace(grant.HolderID) == holderID {
			return grant, true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func upsertTestGrant(grants []rootproto.AuthorityGrant, grant rootproto.AuthorityGrant) []rootproto.AuthorityGrant {
	for i := range grants {
		if grants[i].GrantID == grant.GrantID {
			grants[i] = grant
			return grants
		}
	}
	return append(grants, grant)
}

func removeTestGrant(grants []rootproto.AuthorityGrant, grantID string) []rootproto.AuthorityGrant {
	for i := 0; i < len(grants); i++ {
		if grants[i].GrantID == grantID {
			grants = append(grants[:i], grants[i+1:]...)
			i--
		}
	}
	return grants
}

func upsertTestVisibleGrant(grants []rootproto.VisibleAuthorityGrant, grant rootproto.VisibleAuthorityGrant) []rootproto.VisibleAuthorityGrant {
	for i := range grants {
		if grants[i].GrantID == grant.GrantID {
			grants[i] = rootproto.CloneVisibleAuthorityGrant(grant)
			return grants
		}
	}
	return append(grants, rootproto.CloneVisibleAuthorityGrant(grant))
}

func removeTestVisibleGrant(grants []rootproto.VisibleAuthorityGrant, grantID string) []rootproto.VisibleAuthorityGrant {
	for i := 0; i < len(grants); i++ {
		if grants[i].GrantID == grantID {
			grants = append(grants[:i], grants[i+1:]...)
			i--
		}
	}
	return grants
}

func (f *fakeStorage) loadCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadCalls
}

func (f *fakeStorage) resetLoadCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadCalls = 0
}

func (f *fakeStorage) setLoadErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadErr = err
}

func (f *fakeStorage) Load() (rootview.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadCalls++
	if f.loadErr != nil {
		return rootview.Snapshot{}, f.loadErr
	}
	return rootview.CloneSnapshot(f.snapshot), nil
}

func (f *fakeStorage) AppendRootEvent(_ context.Context, event rootevent.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.eventCalls++
	f.lastEvent = event
	if f.eventErr != nil {
		return f.eventErr
	}
	if event.Kind == rootevent.KindUnknown {
		return errors.New("invalid root event")
	}
	snapshot := rootstate.Snapshot{
		State: rootstate.State{
			ClusterEpoch:  f.snapshot.ClusterEpoch,
			IDFence:       f.snapshot.Allocator.IDCurrent,
			TSOFence:      f.snapshot.Allocator.TSCurrent,
			ActiveGrants:  append([]rootproto.AuthorityGrant(nil), f.snapshot.ActiveGrants...),
			RetiredGrants: append([]rootproto.GrantRetirement(nil), f.snapshot.RetiredGrants...),
			RetiredEraFloors: rootproto.CloneAuthorityRetiredEraFloors(
				f.snapshot.RetiredEraFloors,
			),
			LastCommitted: rootstate.Cursor{Term: 1, Index: uint64(f.eventCalls)},
		},
		Stores:              rootstate.CloneStoreMemberships(f.snapshot.Stores),
		SnapshotEpochs:      rootstate.CloneSnapshotEpochs(f.snapshot.SnapshotEpochs),
		Mounts:              rootstate.CloneMounts(f.snapshot.Mounts),
		Subtrees:            rootstate.CloneSubtreeAuthorities(f.snapshot.Subtrees),
		Quotas:              rootstate.CloneQuotaFences(f.snapshot.Quotas),
		Descriptors:         rootCloneDescriptorsForTest(f.snapshot.Descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(f.snapshot.PendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(f.snapshot.PendingRangeChanges),
	}
	rootstate.ApplyEventToSnapshot(&snapshot, snapshot.State.LastCommitted, event)
	f.snapshot = rootview.SnapshotFromRoot(snapshot)
	return nil
}

func (f *fakeStorage) ApplyGrant(_ context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	holderID := strings.TrimSpace(cmd.HolderID)
	switch cmd.Kind {
	case rootproto.GrantActIssue:
		if f.campaignErr != nil {
			return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, f.campaignErr
		}
		active, _ := testActiveGrantForDuties(f.snapshot, cmd.RequestedDuties)
		if active.Present() &&
			active.GrantID == strings.TrimSpace(cmd.GrantID) &&
			active.HolderID == holderID &&
			testDutyGrantsCover(active.Duties, cmd.RequestedDuties) {
			return f.protocolState(), testGrantCertificate(active), nil
		}
		f.campaignCalls++
		if active.Present() &&
			active.HolderID != holderID &&
			active.ActiveAt(cmd.NowUnixNano) {
			return f.protocolState(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		predecessors := []rootproto.GrantRetirement(nil)
		if active.Present() {
			mode := rootproto.GrantRetirementSealedExact
			if !active.ActiveAt(cmd.NowUnixNano) {
				mode = rootproto.GrantRetirementExpiredBound
			}
			predecessors = append(predecessors, rootproto.GrantRetirement{
				GrantID:  active.GrantID,
				HolderID: active.HolderID,
				Era:      active.Era,
				Mode:     mode,
				Bounds:   append([]rootproto.DutyGrant(nil), active.Duties...),
			})
		}
		for _, retired := range f.snapshot.RetiredGrants {
			if retired.InheritedByGrantID == "" {
				predecessors = append(predecessors, retired)
			}
		}
		era := active.Era + 1
		for _, retired := range f.snapshot.RetiredGrants {
			if retired.Era >= era {
				era = retired.Era + 1
			}
		}
		grantID := cmd.GrantID
		if strings.TrimSpace(grantID) == "" {
			grantID = fmt.Sprintf("%s/%d", holderID, era)
		}
		grant := rootproto.AuthorityGrant{
			GrantID:                grantID,
			HolderID:               holderID,
			Era:                    era,
			ExpiresUnixNano:        cmd.ExpiresUnixNano,
			IssuedRootToken:        rootproto.AuthorityRootToken{Term: f.snapshot.RootToken.Cursor.Term, Index: f.snapshot.RootToken.Cursor.Index},
			Duties:                 append([]rootproto.DutyGrant(nil), cmd.RequestedDuties...),
			PredecessorRetirements: append([]rootproto.GrantRetirement(nil), predecessors...),
		}
		for _, predecessor := range predecessors {
			f.snapshot.ActiveGrants = removeTestGrant(f.snapshot.ActiveGrants, predecessor.GrantID)
		}
		f.snapshot.ActiveGrants = upsertTestGrant(f.snapshot.ActiveGrants, grant)
		f.snapshot.RetiredGrants = append([]rootproto.GrantRetirement(nil), predecessors...)
		for _, duty := range cmd.RequestedDuties {
			if duty.Bound.Kind != rootproto.DutyBoundMonotone {
				continue
			}
			switch duty.DutyID {
			case rootproto.DutyAllocID:
				if duty.Bound.MonotoneUpper > f.snapshot.Allocator.IDCurrent {
					f.snapshot.Allocator.IDCurrent = duty.Bound.MonotoneUpper
				}
			case rootproto.DutyTSO:
				if duty.Bound.MonotoneUpper > f.snapshot.Allocator.TSCurrent {
					f.snapshot.Allocator.TSCurrent = duty.Bound.MonotoneUpper
				}
			}
		}
		f.advanceRootToken()
		return f.protocolState(), testGrantCertificate(grant), nil
	case rootproto.GrantActSeal:
		f.sealCalls++
		if f.sealErr != nil {
			return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, f.sealErr
		}
		active, ok := f.snapshot.ActiveGrantByID(strings.TrimSpace(cmd.GrantID))
		if !ok {
			return f.protocolState(), rootproto.GrantCertificate{}, rootstate.ErrInvalidGrant
		}
		if holder := strings.TrimSpace(cmd.HolderID); holder != "" && holder != active.HolderID {
			return f.protocolState(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		retirement := rootproto.GrantRetirement{
			GrantID:  active.GrantID,
			HolderID: active.HolderID,
			Era:      active.Era,
			Mode:     rootproto.GrantRetirementSealedExact,
			Bounds:   dutyGrantsFromAuthorityUsagesForTest(cmd.ExactUsages),
		}
		if len(retirement.Bounds) == 0 {
			retirement.Bounds = append([]rootproto.DutyGrant(nil), active.Duties...)
		}
		f.snapshot.RetiredGrants = append(f.snapshot.RetiredGrants, retirement)
		f.snapshot.ActiveGrants = removeTestGrant(f.snapshot.ActiveGrants, active.GrantID)
		f.advanceRootToken()
		return f.protocolState(), rootproto.GrantCertificate{}, nil
	case rootproto.GrantActRetireExpired:
		active, ok := f.snapshot.ActiveGrantByID(strings.TrimSpace(cmd.GrantID))
		if !ok {
			return f.protocolState(), rootproto.GrantCertificate{}, rootstate.ErrInvalidGrant
		}
		if cmd.NowUnixNano < active.ExpiresUnixNano {
			return f.protocolState(), rootproto.GrantCertificate{}, rootstate.ErrInvalidGrant
		}
		retirement := rootproto.GrantRetirement{
			GrantID:  active.GrantID,
			HolderID: active.HolderID,
			Era:      active.Era,
			Mode:     rootproto.GrantRetirementExpiredBound,
			Bounds:   append([]rootproto.DutyGrant(nil), active.Duties...),
		}
		f.snapshot.RetiredGrants = append(f.snapshot.RetiredGrants, retirement)
		f.snapshot.ActiveGrants = removeTestGrant(f.snapshot.ActiveGrants, active.GrantID)
		f.advanceRootToken()
		return f.protocolState(), rootproto.GrantCertificate{}, nil
	case rootproto.GrantActInherit:
		f.reattachCalls++
		active, ok := testActiveGrantForHolder(f.snapshot, holderID)
		if !ok {
			return f.protocolState(), rootproto.GrantCertificate{}, rootstate.ErrPrimacy
		}
		successor := active.GrantID
		for _, predecessor := range cmd.PredecessorGrantIDs {
			for i := range f.snapshot.RetiredGrants {
				if f.snapshot.RetiredGrants[i].GrantID == predecessor {
					f.snapshot.RetiredGrants[i].InheritedByGrantID = successor
					f.snapshot.RetiredEraFloors = rootproto.AdvanceAuthorityRetiredEraFloorsForBounds(
						f.snapshot.RetiredEraFloors,
						f.snapshot.RetiredGrants[i].Bounds,
						f.snapshot.RetiredGrants[i].Era,
					)
					f.snapshot.GrantInheritances = append(f.snapshot.GrantInheritances, rootproto.GrantInheritance{
						PredecessorGrantID: predecessor,
						SuccessorGrantID:   successor,
					})
				}
			}
		}
		f.advanceRootToken()
		return f.protocolState(), rootproto.GrantCertificate{}, nil
	default:
		return rootstate.EunomiaState{}, rootproto.GrantCertificate{}, rootstate.ErrInvalidGrant
	}
}

func (f *fakeStorage) ApplyVisibleAuthority(_ context.Context, cmd rootproto.VisibleAuthorityCommand) (rootstate.State, rootproto.VisibleAuthorityGrant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applyVisibleAuthorityCalls++
	f.lastVisibleAuthorityCommand = cmd
	if f.applyVisibleAuthorityErr != nil {
		return f.visibleAuthorityState(), rootproto.VisibleAuthorityGrant{}, f.applyVisibleAuthorityErr
	}
	switch cmd.Kind {
	case rootproto.VisibleAuthorityActAcquire:
		holderID := strings.TrimSpace(cmd.HolderID)
		if holderID == "" || cmd.ExpiresUnixNano <= cmd.NowUnixNano || !cmd.Scope.Valid() {
			return f.visibleAuthorityState(), rootproto.VisibleAuthorityGrant{}, rootstate.ErrInvalidGrant
		}
		if active, ok := f.visibleAuthorityState().ActiveVisibleGrantFor(cmd.Scope, cmd.NowUnixNano); ok {
			if active.HolderID == holderID && active.Covers(cmd.Scope, cmd.NowUnixNano) {
				return f.visibleAuthorityState(), active, nil
			}
			return f.visibleAuthorityState(), rootproto.VisibleAuthorityGrant{}, rootstate.ErrPrimacy
		}
		epoch := f.snapshot.VisibleAuthorityEpoch + 1
		grantID := strings.TrimSpace(cmd.GrantID)
		if grantID == "" {
			grantID = fmt.Sprintf("%s/%d", holderID, epoch)
		}
		grant := rootproto.VisibleAuthorityGrant{
			GrantID:           grantID,
			EpochID:           epoch,
			HolderID:          holderID,
			Scope:             rootproto.CloneVisibleAuthorityScope(cmd.Scope),
			ExpiresUnixNano:   cmd.ExpiresUnixNano,
			PredecessorDigest: cmd.PredecessorDigest,
			QuotaCreditBytes:  cmd.QuotaCreditBytes,
			QuotaCreditInodes: cmd.QuotaCreditInodes,
		}
		f.snapshot.ActiveVisibleGrants = upsertTestVisibleGrant(f.snapshot.ActiveVisibleGrants, grant)
		if grant.EpochID > f.snapshot.VisibleAuthorityEpoch {
			f.snapshot.VisibleAuthorityEpoch = grant.EpochID
		}
		f.advanceRootToken()
		return f.visibleAuthorityState(), grant, nil
	case rootproto.VisibleAuthorityActRetire:
		grantID := strings.TrimSpace(cmd.GrantID)
		holderID := strings.TrimSpace(cmd.HolderID)
		active, ok := f.snapshot.ActiveVisibleGrantByID(grantID)
		if !ok {
			return f.visibleAuthorityState(), rootproto.VisibleAuthorityGrant{}, nil
		}
		if active.HolderID != holderID {
			return f.visibleAuthorityState(), rootproto.VisibleAuthorityGrant{}, rootstate.ErrPrimacy
		}
		f.snapshot.ActiveVisibleGrants = removeTestVisibleGrant(f.snapshot.ActiveVisibleGrants, grantID)
		f.advanceRootToken()
		return f.visibleAuthorityState(), rootproto.VisibleAuthorityGrant{}, nil
	default:
		return f.visibleAuthorityState(), rootproto.VisibleAuthorityGrant{}, rootstate.ErrInvalidGrant
	}
}

func (f *fakeStorage) visibleAuthorityState() rootstate.State {
	return rootstate.CloneState(f.snapshot.RootSnapshot().State)
}

func (f *fakeStorage) advanceRootToken() {
	if f.snapshot.RootToken.Cursor.Term == 0 {
		f.snapshot.RootToken.Cursor.Term = 1
	}
	f.snapshot.RootToken.Cursor.Index++
	f.snapshot.RootToken.Revision++
}

func dutyGrantsFromAuthorityUsagesForTest(usages []rootproto.AuthorityUsage) []rootproto.DutyGrant {
	out := make([]rootproto.DutyGrant, 0, len(usages))
	for _, usage := range usages {
		out = append(out, rootproto.DutyGrant{DutyID: usage.DutyID, Scope: usage.Scope, Bound: usage.Usage})
	}
	return out
}

func (f *fakeStorage) SaveAllocatorState(_ context.Context, idCurrent, tsCurrent uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveCalls++
	f.lastID = idCurrent
	f.lastTS = tsCurrent
	if f.saveErr != nil {
		return f.saveErr
	}
	if idCurrent > f.snapshot.Allocator.IDCurrent {
		f.snapshot.Allocator.IDCurrent = idCurrent
	}
	if tsCurrent > f.snapshot.Allocator.TSCurrent {
		f.snapshot.Allocator.TSCurrent = tsCurrent
	}
	return nil
}

func (f *fakeStorage) Close() error {
	return nil
}

func (f *fakeStorage) Refresh() error {
	return nil
}

func (f *fakeStorage) CanSubmitRootWrites() bool {
	if f == nil {
		return true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leader || f.leaderID == 0
}

func (f *fakeStorage) LeaderID() uint64 {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leaderID
}

type staleGrantReloadStorage struct {
	*fakeStorage
	staleSnapshot rootview.Snapshot
	applied       bool
}

func (s *staleGrantReloadStorage) ApplyGrant(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error) {
	state, cert, err := s.fakeStorage.ApplyGrant(ctx, cmd)
	if err == nil {
		s.applied = true
	}
	return state, cert, err
}

func (s *staleGrantReloadStorage) Load() (rootview.Snapshot, error) {
	s.loadCalls++
	if s.loadErr != nil {
		return rootview.Snapshot{}, s.loadErr
	}
	if s.applied {
		return rootview.CloneSnapshot(s.staleSnapshot), nil
	}
	return rootview.CloneSnapshot(s.snapshot), nil
}

type fakeSyncStorage struct {
	fakeStorage
	snapshot rootview.Snapshot
}

type serialAppendStorage struct {
	fakeStorage
	inAppend int32
	entered  chan struct{}
	release  chan struct{}
}

func (f *fakeSyncStorage) Load() (rootview.Snapshot, error) {
	return rootview.CloneSnapshot(f.snapshot), nil
}

func (f *fakeSyncStorage) Refresh() error {
	return nil
}

func (f *serialAppendStorage) AppendRootEvent(ctx context.Context, event rootevent.Event) error {
	if !atomic.CompareAndSwapInt32(&f.inAppend, 0, 1) {
		return errors.New("concurrent append")
	}
	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.release != nil {
		<-f.release
	}
	defer atomic.StoreInt32(&f.inAppend, 0)
	return f.fakeStorage.AppendRootEvent(ctx, event)
}

func rootCloneDescriptorsForTest(in map[uint64]topology.Descriptor) map[uint64]topology.Descriptor {
	out := make(map[uint64]topology.Descriptor, len(in))
	for id, desc := range in {
		out[id] = desc.Clone()
	}
	return out
}

func publishDescriptorEvent(t *testing.T, svc *Service, desc topology.Descriptor, expected uint64) error {
	t.Helper()
	event := rootevent.RegionBootstrapped(desc)
	if svc != nil && svc.cluster != nil && svc.cluster.HasRegion(desc.RegionID) {
		event = rootevent.RegionDescriptorPublished(desc)
	}
	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event:                metawire.RootEventToProto(event),
		ExpectedClusterEpoch: expected,
	})
	return err
}

func joinStores(t *testing.T, svc *Service, storeIDs ...uint64) {
	t.Helper()
	for _, storeID := range storeIDs {
		require.NoError(t, svc.cluster.PublishRootEvent(rootevent.StoreJoined(storeID)))
	}
}

func TestServiceStoreHeartbeatAndGetRegionByKey(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	joinStores(t, svc, 1)

	storeResp, err := svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:           1,
		RegionNum:         3,
		LeaderNum:         1,
		LeaderRegionIds:   []uint64{11},
		Capacity:          1000,
		Available:         800,
		DroppedOperations: 5,
	})
	require.NoError(t, err)
	require.True(t, storeResp.GetAccepted())
	stores := svc.cluster.StoreSnapshot()
	require.Len(t, stores, 1)
	require.Equal(t, uint64(5), stores[0].DroppedOperations)

	desc := testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}})
	desc.RootEpoch = 1
	err = publishDescriptorEvent(t, svc, desc, 0)
	require.NoError(t, err)

	getResp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.NotNil(t, getResp.GetRegionDescriptor())
	require.Equal(t, uint64(11), getResp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, uint64(1), getResp.GetLeaderPeer().GetStoreId())
	require.Equal(t, uint64(101), getResp.GetLeaderPeer().GetPeerId())
	require.Equal(t, coordpb.Freshness_FRESHNESS_BEST_EFFORT, getResp.GetServedFreshness())
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_HEALTHY, getResp.GetDegradedMode())
	require.Equal(t, coordpb.CatchUpState_CATCH_UP_STATE_FRESH, getResp.GetCatchUpState())
	require.True(t, getResp.GetServedByLeader())
	require.Zero(t, getResp.GetRootLag())
	require.Equal(t, uint64(1), getResp.GetDescriptorRevision())
	require.Zero(t, getResp.GetRequiredDescriptorRevision())
}

func TestServiceGetStoreMarksStaleHeartbeatDown(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.ConfigureStoreHeartbeatTTL(5 * time.Second)
	joinStores(t, svc, 7)

	_, err := svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:    7,
		ClientAddr: "127.0.0.1:20160",
		RaftAddr:   "127.0.0.1:20160",
	})
	require.NoError(t, err)

	heartbeatAt := time.Now()
	svc.now = func() time.Time { return heartbeatAt.Add(4 * time.Second) }
	getResp, err := svc.GetStore(context.Background(), &coordpb.GetStoreRequest{StoreId: 7})
	require.NoError(t, err)
	require.Equal(t, coordpb.StoreState_STORE_STATE_UP, getResp.GetStore().GetState())

	svc.now = func() time.Time { return heartbeatAt.Add(6 * time.Second) }
	getResp, err = svc.GetStore(context.Background(), &coordpb.GetStoreRequest{StoreId: 7})
	require.NoError(t, err)
	require.Equal(t, coordpb.StoreState_STORE_STATE_DOWN, getResp.GetStore().GetState())

	listResp, err := svc.ListStores(context.Background(), &coordpb.ListStoresRequest{})
	require.NoError(t, err)
	require.Len(t, listResp.GetStores(), 1)
	require.Equal(t, coordpb.StoreState_STORE_STATE_DOWN, listResp.GetStores()[0].GetState())
}

func TestServiceStoreHeartbeatRequiresRootedMembership(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))

	_, err := svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{StoreId: 8})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	joinStores(t, svc, 8)
	resp, err := svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:    8,
		ClientAddr: "127.0.0.1:20160",
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())

	require.NoError(t, svc.cluster.PublishRootEvent(rootevent.StoreRetired(8)))
	_, err = svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{StoreId: 8})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	getResp, err := svc.GetStore(context.Background(), &coordpb.GetStoreRequest{StoreId: 8})
	require.NoError(t, err)
	require.Equal(t, coordpb.StoreState_STORE_STATE_TOMBSTONE, getResp.GetStore().GetState())
	require.Empty(t, getResp.GetStore().GetClientAddr())
}

// TestServiceRefreshFromStorageRestoresStoreMembership verifies the full
// Stage 2.1 restart-restore chain end-to-end: a freshly constructed Service
// pointed at a populated rooted snapshot must rebuild the in-memory store
// membership view after RefreshFromStorage so that:
//
//   - heartbeats from active members are accepted without re-publishing
//     StoreJoined,
//   - retired members are reported as TOMBSTONE through GetStore, and
//   - heartbeats from retired members are rejected with FailedPrecondition.
//
// This closes the audit gap where every component along the chain
// (state.ApplyEventToSnapshot, fakeStorage.AppendRootEvent,
// publishRootSnapshot, Cluster.ReplaceRootSnapshot,
// Cluster.replaceStoreMemberships) was tested in isolation but the full
// "coordinator restart from durable snapshot" path was not.
func TestServiceRefreshFromStorageRestoresStoreMembership(t *testing.T) {
	storage := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Stores: map[uint64]rootstate.StoreMembership{
				11: {
					StoreID:  11,
					State:    rootstate.StoreMembershipActive,
					JoinedAt: rootstate.Cursor{Term: 1, Index: 1},
				},
				12: {
					StoreID:   12,
					State:     rootstate.StoreMembershipRetired,
					JoinedAt:  rootstate.Cursor{Term: 1, Index: 2},
					RetiredAt: rootstate.Cursor{Term: 1, Index: 3},
				},
			},
			Descriptors: make(map[uint64]topology.Descriptor),
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)

	require.NoError(t, svc.RefreshFromStorage())

	// Active member: heartbeat accepted without re-publishing StoreJoined.
	resp, err := svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:    11,
		ClientAddr: "127.0.0.1:20171",
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())

	getActive, err := svc.GetStore(context.Background(), &coordpb.GetStoreRequest{StoreId: 11})
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:20171", getActive.GetStore().GetClientAddr())

	// Retired member: surfaced as TOMBSTONE; heartbeat rejected.
	getRetired, err := svc.GetStore(context.Background(), &coordpb.GetStoreRequest{StoreId: 12})
	require.NoError(t, err)
	require.Equal(t, coordpb.StoreState_STORE_STATE_TOMBSTONE, getRetired.GetStore().GetState())

	_, err = svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:    12,
		ClientAddr: "127.0.0.1:20172",
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	// A store that was never joined remains rejected even after restore.
	_, err = svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{StoreId: 99})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestServiceDiagnosticsSnapshot(t *testing.T) {
	now := time.Unix(100, 0)
	storage := &fakeStorage{
		leader:   true,
		leaderID: 7,
		snapshot: rootview.Snapshot{
			RootToken: rootstorage.TailToken{
				Cursor:   rootstate.Cursor{Term: 2, Index: 9},
				Revision: 4,
			},
			CatchUpState: rootview.CatchUpStateLagging,
			Allocator: rootview.AllocatorState{
				IDCurrent: 55,
				TSCurrent: 88,
			},
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c1/3",
				HolderID:        "c1",
				ExpiresUnixNano: now.Add(5 * time.Second).UnixNano(),
				Era:             3,
				IssuedAt:        rootproto.Cursor{Term: 2, Index: 9},
				IssuedRootToken: rootproto.AuthorityRootToken{Term: 2, Index: 9, Revision: 4},
				Duties: []rootproto.DutyGrant{
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 55),
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 88),
					rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{Term: 2, Index: 9, Revision: 4}, 5, 0),
				},
			}},
			RetiredGrants: []rootproto.GrantRetirement{
				{
					GrantID:   "c0/2",
					HolderID:  "c0",
					Era:       2,
					Mode:      rootproto.GrantRetirementSealedExact,
					Bounds:    []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 44), rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 77), rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 5, 0)},
					RetiredAt: rootproto.Cursor{Term: 2, Index: 8},
				},
			},
			SnapshotEpochs: map[string]rootstate.SnapshotEpoch{
				"vol/9/33": {
					SnapshotID:  "vol/9/33",
					Mount:       "vol",
					MountKeyID:  1,
					RootInode:   9,
					ReadVersion: 33,
				},
			},
			Descriptors: map[uint64]topology.Descriptor{
				11: testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}}),
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(55), tso.NewAllocator(88), storage)
	svc.now = func() time.Time { return now }
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, svc.ReloadFromStorage())
	joinStores(t, svc, 1)
	_, err := svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:         1,
		RegionNum:       1,
		LeaderNum:       1,
		LeaderRegionIds: []uint64{11},
	})
	require.NoError(t, err)

	snapshot := svc.DiagnosticsSnapshot()
	allocator := snapshot["allocator"].(map[string]any)
	root := snapshot["root"].(map[string]any)
	grant := snapshot["grant"].(map[string]any)
	authority := snapshot["authority"].(map[string]any)
	retirement := snapshot["retirement"].(map[string]any)
	audit := snapshot["audit"].(map[string]any)
	regions := snapshot["region_descriptors"].([]map[string]any)

	require.Equal(t, uint64(54), allocator["id_current"])
	require.Equal(t, uint64(87), allocator["tso_current"])
	require.Equal(t, true, root["configured"])
	require.Equal(t, "CATCH_UP_STATE_FRESH", root["catch_up_state"])
	require.Equal(t, "DEGRADED_MODE_HEALTHY", root["degraded_mode"])
	require.Equal(t, uint64(7), root["storage_leader_id"])
	require.NotZero(t, root["last_reload_unix_nano"])
	require.Equal(t, 1, root["snapshot_epochs"])
	require.Equal(t, map[string]any{
		"active":             true,
		"min_read_version":   uint64(33),
		"mount_floors":       map[uint64]uint64{1: 33},
		"enforcement_target": "mvcc_gc",
	}, root["snapshot_retention"])
	require.Equal(t, true, grant["enabled"])
	require.Equal(t, "c1", grant["holder_id"])
	require.Equal(t, true, grant["active"])
	require.Equal(t, true, grant["held_by_self"])
	require.Equal(t, true, grant["usable_by_self"])
	require.Equal(t, uint64(3), grant["era"])
	require.Equal(t, map[string]any{"term": uint64(2), "index": uint64(9)}, grant["issued_at"])
	require.Len(t, grant["duties"], 3)
	activeGrant := authority["active_grant"].(map[string]any)
	require.Equal(t, "c1/3", activeGrant["grant_id"])
	require.Equal(t, "c1", activeGrant["holder_id"])
	require.Equal(t, uint64(3), activeGrant["era"])
	require.Equal(t, true, activeGrant["active"])
	require.Equal(t, true, activeGrant["held_by_self"])
	require.Len(t, activeGrant["duties"], 3)
	require.Len(t, authority["retired_grants"], 1)
	require.Equal(t, uint64(1), authority["remaining_id_bound"])
	require.Equal(t, "c0/2", retirement["grant_id"])
	require.Equal(t, "c0", retirement["holder_id"])
	require.Equal(t, uint64(2), retirement["era"])
	require.Equal(t, "sealed_exact", retirement["mode"])
	require.Equal(t, map[string]any{"term": uint64(2), "index": uint64(8)}, retirement["retired_at"])
	require.Equal(t, true, audit["retired_not_inherited"])
	require.Equal(t, false, audit["sealed_exact_completed"])
	require.Equal(t, false, audit["invalid_successor_bound"])
	require.Len(t, regions, 1)
	require.Equal(t, uint64(11), regions[0]["region_id"])
	require.Equal(t, uint64(1), regions[0]["leader_store_id"])
	require.NotZero(t, regions[0]["leader_reported_unix"])
}

func TestServiceGetRegionByKeyStrongReadRejectsFollower(t *testing.T) {
	storage := &fakeStorage{
		leader:   false,
		leaderID: 7,
		snapshot: rootview.Snapshot{Descriptors: make(map[uint64]topology.Descriptor)},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)
	require.NoError(t, svc.cluster.PublishRegionDescriptor(testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)))

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("a"),
		Freshness: coordpb.Freshness_FRESHNESS_STRONG,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(err))
	_, metadata, ok := nokverrors.RPCErrorInfo(err)
	require.True(t, ok)
	require.Equal(t, reasonNotLeader, metadata[coordinatorReasonMetadata])
}

func TestServiceGetRegionByKeyGrantModeIssuesLookupGrant(t *testing.T) {
	cluster := catalog.NewCluster()
	desc := testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	desc.RootEpoch = 5
	token := rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5}
	cluster.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
	}, token)
	storage := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			RootToken:   token,
			Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)
	svc.ConfigureAuthorityGrant("c1", time.Second, 300*time.Millisecond)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Equal(t, 1, storage.campaignCalls)
	require.NotZero(t, resp.GetEra())
	require.NotNil(t, resp.GetAuthorityEvidence())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE, resp.GetServingClass())
}

func TestServiceGetRegionByKeyRenewsSelfHeldExpiringGrant(t *testing.T) {
	cluster := catalog.NewCluster()
	desc := testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	desc.RootEpoch = 5
	token := rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5}
	cluster.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
	}, token)
	storage := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			RootToken:   token,
			Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c1/1",
				HolderID:        "c1",
				ExpiresUnixNano: 100,
				Era:             1,
				Duties: []rootproto.DutyGrant{
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10),
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 10),
					rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 5, 0),
				},
			}},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)
	svc.ConfigureAuthorityGrant("c1", time.Second, 300*time.Millisecond)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.False(t, resp.GetNotFound())
	require.Equal(t, uint64(11), resp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, 1, storage.campaignCalls)
	renewedGrant, ok := storage.snapshot.ActiveGrantFor(rootproto.DutyRegionLookup, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	require.True(t, ok)
	require.Equal(t, "c1", renewedGrant.HolderID)
	require.Equal(t, uint64(2), renewedGrant.Era)
	require.Equal(t, uint64(2), resp.GetEra())
	require.True(t, renewedGrant.ActiveAt(200))
	require.NotNil(t, resp.GetAuthorityEvidence())
}

func TestServiceGetRegionByKeyRejectsOtherActiveGrant(t *testing.T) {
	cluster := catalog.NewCluster()
	desc := testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	desc.RootEpoch = 5
	token := rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5}
	cluster.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
	}, token)
	storage := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			RootToken:   token,
			Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c2/4",
				HolderID:        "c2",
				ExpiresUnixNano: time.Second.Nanoseconds(),
				Era:             4,
				Duties:          []rootproto.DutyGrant{rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 5, 0)},
			}},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)
	svc.ConfigureAuthorityGrant("c1", time.Second, 300*time.Millisecond)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, storage.campaignCalls)
	otherGrant, ok := storage.snapshot.ActiveGrantFor(rootproto.DutyRegionLookup, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	require.True(t, ok)
	require.Equal(t, "c2", otherGrant.HolderID)
}

func TestServiceGetRegionByKeyRequiredRootToken(t *testing.T) {
	cluster := catalog.NewCluster()
	desc := testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	desc.RootEpoch = 5
	cluster.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
	}, rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5})
	storage := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			RootToken:   rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5},
			Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c1/4",
				HolderID:        "c1",
				ExpiresUnixNano: time.Second.Nanoseconds(),
				Era:             4,
				Duties: []rootproto.DutyGrant{
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10),
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 10),
					rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{Term: 1, Index: 3, Revision: 5}, 5, 0),
				},
			}},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)
	svc.ConfigureAuthorityGrant("c1", time.Second, 300*time.Millisecond)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key: []byte("a"),
		RequiredRootToken: &coordpb.RootToken{
			Term:     1,
			Index:    10,
			Revision: 10,
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "required rooted token not satisfied")

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key: []byte("a"),
		RequiredRootToken: &coordpb.RootToken{
			Term:     1,
			Index:    1,
			Revision: 1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), resp.GetServedRootToken().GetRevision())
	require.Equal(t, uint64(1), resp.GetServedRootToken().GetTerm())
	require.Equal(t, uint64(3), resp.GetServedRootToken().GetIndex())
	require.Equal(t, uint64(5), resp.GetDescriptorRevision())
	require.Zero(t, resp.GetRequiredDescriptorRevision())
	require.Equal(t, uint64(4), resp.GetEra())
	require.NotNil(t, resp.GetAuthorityEvidence())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_HEALTHY, resp.GetSyncHealth())
}

func TestServiceGetRegionByKeyRequiredDescriptorRevision(t *testing.T) {
	cluster := catalog.NewCluster()
	desc := testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	desc.RootEpoch = 7
	cluster.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
	}, rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5})
	storage := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			RootToken:   rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5},
			Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("a"),
		RequiredDescriptorRevision: 8,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errRequiredDescriptorNotSatisfied)

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("a"),
		RequiredDescriptorRevision: 7,
	})
	require.NoError(t, err)
	require.False(t, resp.GetNotFound())
	require.Equal(t, uint64(7), resp.GetDescriptorRevision())
	require.Equal(t, uint64(7), resp.GetRequiredDescriptorRevision())
	require.Equal(t, uint64(7), resp.GetRegionDescriptor().GetRootEpoch())
	require.Zero(t, resp.GetEra())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_HEALTHY, resp.GetSyncHealth())
}

func TestServiceGetRegionByKeyRejectsSplitPendingDescriptor(t *testing.T) {
	cluster := catalog.NewCluster()
	left := testDescriptor(41, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil)
	right := testDescriptor(42, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	left.RootEpoch = 6
	right.RootEpoch = 6
	cluster.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{
			left.RegionID:  left,
			right.RegionID: right,
		},
		PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
			40: {
				Kind:           rootstate.PendingRangeChangeSplit,
				ParentRegionID: 40,
				LeftRegionID:   left.RegionID,
				RightRegionID:  right.RegionID,
				Left:           left,
				Right:          right,
			},
		},
	}, rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 6})
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("b")})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errRangeChangePending)
	require.Contains(t, err.Error(), "split")
}

func TestServiceGetRegionByKeyRejectsMergePendingDescriptor(t *testing.T) {
	cluster := catalog.NewCluster()
	merged := testDescriptor(51, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 2}, nil)
	merged.RootEpoch = 9
	cluster.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{
			merged.RegionID: merged,
		},
		PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
			merged.RegionID: {
				Kind:          rootstate.PendingRangeChangeMerge,
				LeftRegionID:  49,
				RightRegionID: 50,
				Merged:        merged,
			},
		},
	}, rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 9})
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("b")})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errRangeChangePending)
	require.Contains(t, err.Error(), "merge")
}

func TestServiceGetRegionByKeyUsesCachedRootSnapshot(t *testing.T) {
	desc := topology.Descriptor{
		RegionID:  10,
		StartKey:  []byte("a"),
		EndKey:    []byte("z"),
		RootEpoch: 7,
	}
	desc.EnsureHash()
	store := &fakeStorage{
		snapshot: rootview.Snapshot{
			Descriptors: map[uint64]topology.Descriptor{10: desc},
			RootToken:   rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 3},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	require.NoError(t, svc.ReloadFromStorage())
	store.loadCalls = 0

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("b")})
	require.NoError(t, err)
	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("c")})
	require.NoError(t, err)
	require.Equal(t, 0, store.loadCalls)
}

func TestServiceGetRegionByKeyRefreshesCachedRootSnapshotAsync(t *testing.T) {
	desc := topology.Descriptor{
		RegionID:  10,
		StartKey:  []byte("a"),
		EndKey:    []byte("z"),
		RootEpoch: 7,
	}
	desc.EnsureHash()
	store := &fakeStorage{
		snapshot: rootview.Snapshot{
			Descriptors: map[uint64]topology.Descriptor{10: desc},
			RootToken:   rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 3},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureRootSnapshotRefresh(10 * time.Millisecond)
	require.NoError(t, svc.ReloadFromStorage())
	store.resetLoadCalls()

	time.Sleep(20 * time.Millisecond)
	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("b")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return store.loadCallCount() >= 1
	}, time.Second, 10*time.Millisecond)
}

func TestServiceGetRegionByKeyBestEffortWithUnavailableRoot(t *testing.T) {
	storage := &fakeStorage{
		leader:   true,
		snapshot: rootview.Snapshot{Descriptors: make(map[uint64]topology.Descriptor)},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)
	svc.ConfigureRootSnapshotRefresh(10 * time.Millisecond)
	err := publishDescriptorEvent(t, svc, testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)
	storage.resetLoadCalls()
	storage.setLoadErr(errors.New("root unavailable"))

	time.Sleep(20 * time.Millisecond)
	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return storage.loadCallCount() >= 1
	}, time.Second, 10*time.Millisecond)

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.False(t, resp.GetNotFound())
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE, resp.GetDegradedMode())
	require.Equal(t, coordpb.Freshness_FRESHNESS_BEST_EFFORT, resp.GetServedFreshness())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_DEGRADED, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_ROOT_UNAVAILABLE, resp.GetSyncHealth())

	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("a"),
		Freshness: coordpb.Freshness_FRESHNESS_BOUNDED,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, nokverrors.KindUnavailable, nokverrors.KindOf(err))
	_, metadata, ok := nokverrors.RPCErrorInfo(err)
	require.True(t, ok)
	require.Equal(t, reasonRootUnavailable, metadata[coordinatorReasonMetadata])
}

func TestServiceGetRegionByKeyReportsRootLagging(t *testing.T) {
	cluster := catalog.NewCluster()
	cluster.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{
			11: testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
		},
	}, rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 3})
	storage := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			RootToken: rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 5}, Revision: 7},
			Descriptors: map[uint64]topology.Descriptor{
				11: testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING, resp.GetDegradedMode())
	require.Equal(t, uint64(4), resp.GetRootLag())
	require.Equal(t, coordpb.CatchUpState_CATCH_UP_STATE_LAGGING, resp.GetCatchUpState())
	require.Equal(t, uint64(3), resp.GetServedRootToken().GetRevision())
	require.Equal(t, uint64(7), resp.GetCurrentRootToken().GetRevision())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_LAGGING, resp.GetSyncHealth())

	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:        []byte("a"),
		Freshness:  coordpb.Freshness_FRESHNESS_BOUNDED,
		MaxRootLag: proto.Uint64(3),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "root lag exceeds bound")

	resp, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:        []byte("a"),
		Freshness:  coordpb.Freshness_FRESHNESS_BOUNDED,
		MaxRootLag: proto.Uint64(4),
	})
	require.NoError(t, err)
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING, resp.GetDegradedMode())

	resp, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("a"),
		Freshness: coordpb.Freshness_FRESHNESS_BOUNDED,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(4), resp.GetRootLag())

	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("a"),
		Freshness: coordpb.Freshness_FRESHNESS_STRONG,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "root lag exceeds strong freshness")
}

func TestServiceGetRegionByKeyBoundedRejectsBootstrapRequired(t *testing.T) {
	cluster := catalog.NewCluster()
	desc := testDescriptor(21, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	cluster.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{desc.RegionID: desc},
	}, rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 2}, Revision: 2})
	storage := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			RootToken:    rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 2, Index: 9}, Revision: 7},
			CatchUpState: rootview.CatchUpStateBootstrapRequired,
			Descriptors:  map[uint64]topology.Descriptor{desc.RegionID: desc},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Equal(t, coordpb.CatchUpState_CATCH_UP_STATE_BOOTSTRAP_REQUIRED, resp.GetCatchUpState())
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING, resp.GetDegradedMode())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_DEGRADED, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_BOOTSTRAP_REQUIRED, resp.GetSyncHealth())

	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:        []byte("a"),
		Freshness:  coordpb.Freshness_FRESHNESS_BOUNDED,
		MaxRootLag: proto.Uint64(16),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "bootstrap required before bounded freshness")
}

func TestRootLagOnlyCountsServedBehindCurrent(t *testing.T) {
	require.Equal(t, uint64(1), rootLag(
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 9}, Revision: 7},
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 8}, Revision: 7},
	))
	require.Equal(t, uint64(0), rootLag(
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 8}, Revision: 7},
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 9}, Revision: 7},
	))
	require.Equal(t, uint64(0), rootLag(
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 8}, Revision: 6},
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 9}, Revision: 7},
	))
}

func TestServiceRemoveRegion(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	err := publishDescriptorEvent(t, svc, testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)

	resp, err := svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 11})
	require.NoError(t, err)
	require.True(t, resp.GetRemoved())

	getResp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.True(t, getResp.GetNotFound())

	resp, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 11})
	require.NoError(t, err)
	require.False(t, resp.GetRemoved())
}

func TestServiceGetRegionByKeyNotFoundCarriesAnswerabilityContract(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("missing"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredDescriptorRevision: 7,
	})
	require.NoError(t, err)
	require.True(t, resp.GetNotFound())
	require.Equal(t, coordpb.Freshness_FRESHNESS_BOUNDED, resp.GetServedFreshness())
	require.Equal(t, uint64(7), resp.GetRequiredDescriptorRevision())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_HEALTHY, resp.GetSyncHealth())
}

func TestServiceRegionDescriptorUpdateRejectsStaleAndOverlap(t *testing.T) {
	svc := NewService(catalog.NewCluster(), nil, nil)
	err := publishDescriptorEvent(t, svc, testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 2}, nil), 0)
	require.NoError(t, err)

	err = publishDescriptorEvent(t, svc, testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, nil), 0)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	err = publishDescriptorEvent(t, svc, testDescriptor(2, []byte("l"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestServiceAllocIDAndTSO(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(100), tso.NewAllocator(500))

	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), idResp.GetFirstId())
	require.Equal(t, uint64(3), idResp.GetCount())
	require.Zero(t, idResp.GetEra())
	require.Equal(t, uint64(102), idResp.GetConsumedFrontier())

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(500), tsResp.GetTimestamp())
	require.Equal(t, uint64(2), tsResp.GetCount())
	require.Zero(t, tsResp.GetEra())
	require.Equal(t, uint64(501), tsResp.GetConsumedFrontier())
}

func TestServiceRequestValidation(t *testing.T) {
	svc := NewService(nil, nil, nil)

	_, err := svc.StoreHeartbeat(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.RegionLiveness(context.Background(), &coordpb.RegionLivenessRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.AllocID(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.Tso(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.RemoveRegion(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 0})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestServiceRegionLivenessTouchesExistingRegion(t *testing.T) {
	svc := NewService(catalog.NewCluster(), nil, nil)
	err := publishDescriptorEvent(t, svc, testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)

	resp, err := svc.RegionLiveness(context.Background(), &coordpb.RegionLivenessRequest{RegionId: 11})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())

	resp, err = svc.RegionLiveness(context.Background(), &coordpb.RegionLivenessRequest{RegionId: 99})
	require.NoError(t, err)
	require.False(t, resp.GetAccepted())
}

func TestServiceStoreHeartbeatReturnsLeaderTransferHint(t *testing.T) {
	svc := NewService(catalog.NewCluster(), nil, nil)
	joinStores(t, svc, 1, 2)
	err := publishDescriptorEvent(t, svc, testDescriptor(100, []byte(""), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}}), 0)
	require.NoError(t, err)

	_, err = svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:   2,
		LeaderNum: 1,
		RegionNum: 1,
	})
	require.NoError(t, err)

	resp, err := svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:   1,
		LeaderNum: 10,
		RegionNum: 1,
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Len(t, resp.GetOperations(), 1)
	op := resp.GetOperations()[0]
	require.Equal(t, coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER, op.GetType())
	require.Equal(t, uint64(100), op.GetRegionId())
	require.Equal(t, uint64(101), op.GetSourcePeerId())
	require.Equal(t, uint64(201), op.GetTargetPeerId())
}

func TestServicePersistsRegionCatalog(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	err := publishDescriptorEvent(t, svc, testDescriptor(42, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, rootevent.KindRegionBootstrap, store.lastEvent.Kind)
	require.Equal(t, uint64(1), store.lastEvent.RegionDescriptor.Descriptor.RootEpoch)

	_, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 42})
	require.NoError(t, err)
	require.Equal(t, 2, store.eventCalls)
	require.Equal(t, rootevent.KindRegionTombstoned, store.lastEvent.Kind)
}

func TestServiceRegionLivenessSkipsTruthPersistence(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	desc := testDescriptor(42, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	err := publishDescriptorEvent(t, svc, desc, 0)
	require.NoError(t, err)
	require.Equal(t, 1, store.eventCalls)

	before, ok := svc.cluster.RegionLastHeartbeat(42)
	require.True(t, ok)
	time.Sleep(10 * time.Millisecond)

	_, err = svc.RegionLiveness(context.Background(), &coordpb.RegionLivenessRequest{RegionId: desc.RegionID})
	require.NoError(t, err)
	require.Equal(t, 1, store.eventCalls)
	after, ok := svc.cluster.RegionLastHeartbeat(42)
	require.True(t, ok)
	require.True(t, after.After(before) || after.Equal(before))

	lookup, ok := svc.cluster.GetRegionDescriptor(42)
	require.True(t, ok)
	require.Equal(t, uint64(1), lookup.RootEpoch)
	_, ok = svc.cluster.GetRegionDescriptorByKey([]byte("m"))
	require.True(t, ok)
}

func TestServicePublishRootEvent(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	event := rootevent.RegionSplitCommitted(
		41,
		[]byte("m"),
		testDescriptor(41, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil),
		testDescriptor(42, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
	)
	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 1, store.eventCalls)

	left, ok := svc.cluster.GetRegionDescriptorByKey([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(41), left.RegionID)

	right, ok := svc.cluster.GetRegionDescriptorByKey([]byte("x"))
	require.True(t, ok)
	require.Equal(t, uint64(42), right.RegionID)
}

func TestServicePublishRootEventAppliedPeerChangeMarksPendingApplied(t *testing.T) {
	cluster := catalog.NewCluster()
	target := testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
		{StoreID: 2, PeerID: 201},
	})
	target.RootEpoch = 5
	target.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(target))

	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch:       5,
			Descriptors:        map[uint64]topology.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target}},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	applied := rootevent.PeerAdded(target.RegionID, 2, 201, func() topology.Descriptor {
		desc := target.Clone()
		desc.RootEpoch = 0
		return desc
	}())
	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(applied),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, uint64(5), store.snapshot.ClusterEpoch)
	require.NotContains(t, store.snapshot.PendingPeerChanges, target.RegionID)
	transitions := svc.cluster.TransitionSnapshot()
	require.NotContains(t, transitions.PendingPeerChanges, target.RegionID)
}

func TestServicePublishRootEventPersistsPeerPlan(t *testing.T) {
	cluster := catalog.NewCluster()
	current := testDescriptor(12, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(current))

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 0
	target.EnsureHash()

	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch: 5,
			Descriptors:  map[uint64]topology.Descriptor{current.RegionID: current},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.NotNil(t, resp.GetAssessment())
	require.Equal(t, "peer:12:add:2:201", resp.GetAssessment().GetTransitionId())
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_OPEN, resp.GetAssessment().GetStatus())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_APPLY, resp.GetAssessment().GetDecision())
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, rootevent.KindPeerAdditionPlanned, store.lastEvent.Kind)
	require.Equal(t, uint64(6), store.lastEvent.PeerChange.Region.RootEpoch)
	transitions := svc.cluster.TransitionSnapshot()
	require.Contains(t, transitions.PendingPeerChanges, target.RegionID)
	require.Equal(t, rootstate.PendingPeerChangeAddition, transitions.PendingPeerChanges[target.RegionID].Kind)
}

func TestServicePublishRootEventSkipsDuplicatePeerPlan(t *testing.T) {
	cluster := catalog.NewCluster()
	current := testDescriptor(13, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(current))

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 6
	target.EnsureHash()

	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]topology.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
				target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.NotNil(t, resp.GetAssessment())
	require.Equal(t, "peer:13:add:2:201", resp.GetAssessment().GetTransitionId())
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_PENDING, resp.GetAssessment().GetStatus())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_SKIP, resp.GetAssessment().GetDecision())
	require.Equal(t, 0, store.eventCalls)
	require.Equal(t, uint64(6), store.snapshot.ClusterEpoch)
	require.Len(t, store.snapshot.PendingPeerChanges, 1)
}

func TestServicePublishRootEventSkipsCompletedPeerPlan(t *testing.T) {
	cluster := catalog.NewCluster()
	target := testDescriptor(131, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
		{StoreID: 2, PeerID: 201},
	})
	target.RootEpoch = 6
	target.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(target))

	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]topology.Descriptor{target.RegionID: target},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.NotNil(t, resp.GetAssessment())
	require.Equal(t, "peer:131:add:2:201", resp.GetAssessment().GetTransitionId())
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_COMPLETED, resp.GetAssessment().GetStatus())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_SKIP, resp.GetAssessment().GetDecision())
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventRejectsConflictingPeerPlan(t *testing.T) {
	cluster := catalog.NewCluster()
	current := testDescriptor(14, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(current))

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 6
	target.EnsureHash()

	conflicting := current.Clone()
	conflicting.Peers = append(conflicting.Peers, metaregion.Peer{StoreID: 3, PeerID: 301})
	conflicting.Epoch.ConfVersion++
	conflicting.RootEpoch = 6
	conflicting.EnsureHash()

	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]topology.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
				target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(conflicting.RegionID, 3, 301, conflicting)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventRejectsMismatchedPeerApply(t *testing.T) {
	cluster := catalog.NewCluster()
	current := testDescriptor(15, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(current))

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 6
	target.EnsureHash()

	mismatched := current.Clone()
	mismatched.Peers = append(mismatched.Peers, metaregion.Peer{StoreID: 3, PeerID: 301})
	mismatched.Epoch.ConfVersion++
	mismatched.RootEpoch = 6
	mismatched.EnsureHash()

	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]topology.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
				target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdded(mismatched.RegionID, 3, 301, mismatched)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventSkipsDuplicateSplitPlan(t *testing.T) {
	cluster := catalog.NewCluster()
	left := testDescriptor(41, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil)
	right := testDescriptor(42, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	left.RootEpoch = 6
	right.RootEpoch = 6
	left.EnsureHash()
	right.EnsureHash()
	require.NoError(t, cluster.PublishRootEvent(rootevent.RegionSplitPlanned(40, []byte("m"), left, right)))

	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch: 6,
			Descriptors: map[uint64]topology.Descriptor{
				left.RegionID:  left,
				right.RegionID: right,
			},
			PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
				40: {Kind: rootstate.PendingRangeChangeSplit, ParentRegionID: 40, LeftRegionID: left.RegionID, RightRegionID: right.RegionID, Left: left, Right: right},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionSplitPlanned(40, []byte("m"), left, right)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 0, store.eventCalls)
}

func TestServiceRefreshFromStorageReplacesPendingTransitions(t *testing.T) {
	cluster := catalog.NewCluster()
	left := testDescriptor(61, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil)
	right := testDescriptor(62, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch: 9,
			Descriptors: map[uint64]topology.Descriptor{
				left.RegionID:  left,
				right.RegionID: right,
			},
			PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
				60: {Kind: rootstate.PendingRangeChangeSplit, ParentRegionID: 60, LeftRegionID: left.RegionID, RightRegionID: right.RegionID, Left: left, Right: right},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	require.NoError(t, svc.RefreshFromStorage())
	transitions := svc.cluster.TransitionSnapshot()
	require.Contains(t, transitions.PendingRangeChanges, uint64(60))
	require.Len(t, svc.cluster.RegionSnapshot(), 2)
}

func TestServiceListTransitionsReturnsOperatorView(t *testing.T) {
	cluster := catalog.NewCluster()
	current := testDescriptor(160, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 6
	target.EnsureHash()

	cluster.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{target.RegionID: target},
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
			target.RegionID: {
				Kind:    rootstate.PendingPeerChangeAddition,
				StoreID: 2,
				PeerID:  201,
				Base:    current,
				Target:  target,
			},
		},
	}, rootstorage.TailToken{})

	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	resp, err := svc.ListTransitions(context.Background(), &coordpb.ListTransitionsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	require.Equal(t, coordpb.TransitionKind_TRANSITION_KIND_PEER_CHANGE, resp.GetEntries()[0].GetKind())
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_PENDING, resp.GetEntries()[0].GetStatus())
	require.Equal(t, "peer:160:add:2:201", resp.GetEntries()[0].GetTransitionId())
	require.NotNil(t, resp.GetEntries()[0].GetPendingPeerChange())
}

func TestServiceAssessRootEventReturnsConflictAssessment(t *testing.T) {
	cluster := catalog.NewCluster()
	current := testDescriptor(161, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 6
	target.EnsureHash()

	cluster.ReplaceRootSnapshot(rootstate.Snapshot{
		Descriptors: map[uint64]topology.Descriptor{target.RegionID: target},
		PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
			target.RegionID: {
				Kind:    rootstate.PendingPeerChangeAddition,
				StoreID: 2,
				PeerID:  201,
				Base:    current,
				Target:  target,
			},
		},
	}, rootstorage.TailToken{})

	conflicting := current.Clone()
	conflicting.Peers = append(conflicting.Peers, metaregion.Peer{StoreID: 3, PeerID: 301})
	conflicting.Epoch.ConfVersion++
	conflicting.RootEpoch = 0
	conflicting.EnsureHash()

	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	resp, err := svc.AssessRootEvent(context.Background(), &coordpb.AssessRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(conflicting.RegionID, 3, 301, conflicting)),
	})
	require.NoError(t, err)
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_CONFLICT, resp.GetAssessment().GetStatus())
	require.Equal(t, coordpb.TransitionRetryClass_TRANSITION_RETRY_CLASS_CONFLICT, resp.GetAssessment().GetRetryClass())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_APPLY, resp.GetAssessment().GetDecision())
	require.Equal(t, "peer:161:add:3:301", resp.GetAssessment().GetTransitionId())
}

func TestServiceAssessRootEventUsesStorageSnapshot(t *testing.T) {
	cluster := catalog.NewCluster()
	target := testDescriptor(171, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
		{StoreID: 2, PeerID: 201},
	})
	target.RootEpoch = 6
	target.EnsureHash()

	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]topology.Descriptor{target.RegionID: target},
		},
	}

	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.AssessRootEvent(context.Background(), &coordpb.AssessRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_COMPLETED, resp.GetAssessment().GetStatus())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_SKIP, resp.GetAssessment().GetDecision())
	require.Equal(t, "peer:171:add:2:201", resp.GetAssessment().GetTransitionId())
}

func TestServicePublishRootEventValidatesAgainstStorageSnapshot(t *testing.T) {
	cluster := catalog.NewCluster()
	require.NoError(t, cluster.PublishRootEvent(rootevent.MountRegistered("default", 1, 1, 1)))

	var rooted rootstate.Snapshot
	rootstate.ApplyEventToSnapshot(&rooted, rootstate.Cursor{Term: 1, Index: 1}, rootevent.MountRegistered("default", 1, 1, 1))
	rootstate.ApplyEventToSnapshot(&rooted, rootstate.Cursor{Term: 1, Index: 2}, rootevent.SubtreeHandoffStarted("default", 1, 21))
	store := &fakeStorage{
		leader:   true,
		snapshot: rootview.SnapshotFromRoot(rooted),
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.SubtreeHandoffCompleted("default", 1, 21)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 1, store.eventCalls)

	subtree := store.snapshot.Subtrees[rootstate.SubtreeAuthorityKey("default", 1)]
	require.Equal(t, rootstate.SubtreeAuthorityActive, subtree.State)
	require.Equal(t, uint64(21), subtree.Frontier)
	require.Equal(t, "default/1#1", subtree.AuthorityID)
}

func TestServicePublishRootEventSkipsCompletedSplitPlan(t *testing.T) {
	cluster := catalog.NewCluster()
	left := testDescriptor(141, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil)
	right := testDescriptor(142, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	left.RootEpoch = 6
	right.RootEpoch = 6
	left.EnsureHash()
	right.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(left))
	require.NoError(t, cluster.PublishRegionDescriptor(right))

	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch: 6,
			Descriptors: map[uint64]topology.Descriptor{
				left.RegionID:  left,
				right.RegionID: right,
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionSplitPlanned(140, []byte("m"), left, right)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventSkipsCompletedMergePlan(t *testing.T) {
	cluster := catalog.NewCluster()
	merged := testDescriptor(151, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil)
	merged.RootEpoch = 7
	merged.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(merged))

	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch: 7,
			Descriptors: map[uint64]topology.Descriptor{
				merged.RegionID: merged,
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionMergePlanned(149, 150, merged)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventRejectsMismatchedMergeApply(t *testing.T) {
	cluster := catalog.NewCluster()
	merged := testDescriptor(50, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil)
	merged.RootEpoch = 7
	merged.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(merged))

	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ClusterEpoch: 7,
			Descriptors:  map[uint64]topology.Descriptor{merged.RegionID: merged},
			PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
				merged.RegionID: {Kind: rootstate.PendingRangeChangeMerge, LeftRegionID: 50, RightRegionID: 51, Merged: merged},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	mismatched := merged.Clone()
	mismatched.RootEpoch = 0
	mismatched.EnsureHash()
	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionMerged(50, 52, mismatched)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventValidationAndPersistenceError(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))

	_, err := svc.PublishRootEvent(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	store := &fakeStorage{eventErr: errors.New("persist root event failed")}
	svc = NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)
	event := rootevent.RegionMerged(
		10,
		11,
		testDescriptor(10, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil),
	)
	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	_, ok := svc.cluster.GetRegionDescriptorByKey([]byte("m"))
	require.False(t, ok)
}

func TestServicePublishRootEventSerializesStorageAppend(t *testing.T) {
	store := &serialAppendStorage{
		fakeStorage: fakeStorage{snapshot: rootview.Snapshot{Descriptors: make(map[uint64]topology.Descriptor)}},
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	req1 := &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionBootstrapped(testDescriptor(
			41, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}},
		))),
	}
	req2 := &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionBootstrapped(testDescriptor(
			42, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 102}},
		))),
	}

	errCh := make(chan error, 2)
	go func() {
		_, err := svc.PublishRootEvent(context.Background(), req1)
		errCh <- err
	}()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("first append did not start")
	}

	go func() {
		_, err := svc.PublishRootEvent(context.Background(), req2)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("second publish finished before first append released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(store.release)
	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)
	require.Equal(t, 2, store.eventCalls)
}

func TestServiceRefreshFromStorageSerializesWithWrites(t *testing.T) {
	store := &serialAppendStorage{
		fakeStorage: fakeStorage{snapshot: rootview.Snapshot{Descriptors: make(map[uint64]topology.Descriptor)}},
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	req := &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionBootstrapped(testDescriptor(
			41, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}},
		))),
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.PublishRootEvent(context.Background(), req)
		errCh <- err
	}()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("append did not start")
	}

	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- svc.RefreshFromStorage()
	}()
	select {
	case err := <-refreshDone:
		t.Fatalf("refresh completed while write was in progress: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(store.release)
	require.NoError(t, <-errCh)
	require.NoError(t, <-refreshDone)
}

func TestServiceRegionCatalogPersistenceErrors(t *testing.T) {
	store := &fakeStorage{eventErr: errors.New("persist update failed")}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	err := publishDescriptorEvent(t, svc, testDescriptor(8, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	_, ok := svc.cluster.GetRegionDescriptorByKey([]byte("b"))
	require.False(t, ok)

	store.eventErr = nil
	err = publishDescriptorEvent(t, svc, testDescriptor(8, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)
	store.eventErr = errors.New("persist delete failed")
	_, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 8})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	resp, lookupErr := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("b")})
	require.NoError(t, lookupErr)
	require.False(t, resp.GetNotFound())
}

func TestServicePersistsAllocatorState(t *testing.T) {
	store := &fakeStorage{}
	cluster := catalog.NewCluster()
	desc := testDescriptor(1, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	desc.RootEpoch = 7
	require.NoError(t, cluster.PublishRegionDescriptor(desc))
	svc := NewService(cluster, idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)

	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(10009), store.lastID)
	require.Equal(t, uint64(99), store.lastTS)

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, 2, store.saveCalls)
	require.Equal(t, uint64(10009), store.lastID)
	require.Equal(t, uint64(10099), store.lastTS)
}

func TestServiceAuthorityEvidenceUsesRootedAllocatorFence(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)

	resp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	evidence := metawire.RootAuthorityEvidenceFromProto(resp.GetAuthorityEvidence())
	require.True(t, evidence.Certificate.Grant.Present())
	require.Equal(t, "c1", evidence.Certificate.Grant.HolderID)
	require.Equal(t, uint64(1), evidence.Certificate.Grant.Era)
	require.Equal(t, rootproto.DutyAllocID, evidence.Usage.DutyID)
	require.Equal(t, resp.GetConsumedFrontier(), uint64(11))
	require.GreaterOrEqual(t, authorityEvidenceFrontierForTest(evidence, rootproto.DutyAllocID), resp.GetConsumedFrontier())
	require.Equal(t, store.snapshot.Allocator.IDCurrent, authorityEvidenceFrontierForTest(evidence, rootproto.DutyAllocID))
}

func TestServiceAuthorityEvidenceRejectsUnrootedAllocatorFrontier(t *testing.T) {
	store := &fakeStorage{
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "grant-1",
				HolderID:        "c1",
				ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
				Era:             1,
				Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 5)},
			}},
			Allocator: rootview.AllocatorState{IDCurrent: 5},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, svc.RefreshFromStorage())

	grant, ok := store.snapshot.ActiveGrantFor(rootproto.DutyAllocID, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	require.True(t, ok)
	admission := dutyAdmission{
		grant:          grant,
		certificate:    rootproto.GrantCertificate{Grant: grant, SignerKeyID: rootproto.GrantSignerKeyID, Signature: []byte("test-signature")},
		duty:           rootproto.DutyAllocID,
		servedUnixNano: time.Now().UnixNano(),
	}
	proof, err := admission.authorityEvidence(rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 6})
	require.Error(t, err)
	require.Nil(t, proof.Evidence)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func authorityEvidenceFrontierForTest(evidence rootproto.AuthorityEvidence, duty rootproto.DutyID) uint64 {
	for _, grant := range evidence.Certificate.Grant.Duties {
		if grant.DutyID == duty && grant.Bound.Kind == rootproto.DutyBoundMonotone {
			return grant.Bound.MonotoneUpper
		}
	}
	return 0
}

func TestServiceIDWindowPersistsFenceOncePerWindow(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.idWindowSize = 5

	first, err := svc.reserveIDs(context.Background(), 3)
	require.NoError(t, err)
	require.Equal(t, uint64(10), first)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(14), store.lastID)
	require.Equal(t, uint64(99), store.lastTS)
	require.Equal(t, uint64(12), svc.ids.Current())

	first, err = svc.reserveIDs(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(13), first)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(14), store.lastID)
	require.Equal(t, uint64(14), svc.ids.Current())

	first, err = svc.reserveIDs(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(15), first)
	require.Equal(t, 2, store.saveCalls)
	require.Equal(t, uint64(19), store.lastID)
	require.Equal(t, uint64(15), svc.ids.Current())
}

func TestServiceTSOWindowPersistsFenceOncePerWindow(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.tsoWindowSize = 5

	first, count, err := svc.reserveTSO(context.Background(), 3)
	require.NoError(t, err)
	require.Equal(t, uint64(100), first)
	require.Equal(t, uint64(3), count)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(104), store.lastTS)
	require.Equal(t, uint64(102), svc.tso.Current())

	first, count, err = svc.reserveTSO(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(103), first)
	require.Equal(t, uint64(2), count)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(104), store.lastTS)
	require.Equal(t, uint64(104), svc.tso.Current())

	first, count, err = svc.reserveTSO(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(105), first)
	require.Equal(t, uint64(1), count)
	require.Equal(t, 2, store.saveCalls)
	require.Equal(t, uint64(109), store.lastTS)
	require.Equal(t, uint64(105), svc.tso.Current())
}

func TestServiceReloadDoesNotConsumeActiveIDWindow(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.idWindowSize = 5

	first, err := svc.reserveIDs(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(10), first)
	require.Equal(t, uint64(14), store.lastID)
	require.Equal(t, uint64(11), svc.ids.Current())

	require.NoError(t, svc.ReloadFromStorage())
	require.Equal(t, uint64(11), svc.ids.Current())

	first, err = svc.reserveIDs(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(12), first)
	require.Equal(t, 1, store.saveCalls)
}

func TestServiceReloadDoesNotConsumeActiveTSOWindow(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.tsoWindowSize = 5

	first, _, err := svc.reserveTSO(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(100), first)
	require.Equal(t, uint64(104), store.lastTS)
	require.Equal(t, uint64(101), svc.tso.Current())

	require.NoError(t, svc.ReloadFromStorage())
	require.Equal(t, uint64(101), svc.tso.Current())

	first, _, err = svc.reserveTSO(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(102), first)
	require.Equal(t, 1, store.saveCalls)
}

func TestServiceAllocatorStatePersistenceError(t *testing.T) {
	store := &fakeStorage{saveErr: errors.New("persist failed")}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))

	store.saveErr = nil
	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), idResp.GetFirstId())

	store.saveErr = errors.New("persist failed")
	_, err = svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))

	store.saveErr = nil
	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), tsResp.GetTimestamp())
}

func TestServiceGrantReusedAcrossAllocatorRequests(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, uint64(1), idResp.GetEra())
	require.Equal(t, uint64(10), idResp.GetConsumedFrontier())
	require.Equal(t, 1, store.campaignCalls)
	allocGrant, ok := store.snapshot.ActiveGrantFor(rootproto.DutyAllocID, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	require.True(t, ok)
	require.Equal(t, "c1", allocGrant.HolderID)
	require.Equal(t, uint64(1), allocGrant.Era)
	require.NotNil(t, idResp.GetAuthorityEvidence())

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, uint64(1), tsResp.GetEra())
	require.Equal(t, uint64(100), tsResp.GetConsumedFrontier())
	require.Equal(t, 2, store.campaignCalls)
	require.NotNil(t, tsResp.GetAuthorityEvidence())
}

func TestServicePreferredDutyHolderSpreadsDefaultDuties(t *testing.T) {
	candidates := []string{"coord-1", "coord-2", "coord-3"}
	assignments := map[rootproto.DutyID]string{
		rootproto.DutyAllocID:      preferredDutyHolder(rootproto.DutyAllocID, candidates),
		rootproto.DutyTSO:          preferredDutyHolder(rootproto.DutyTSO, candidates),
		rootproto.DutyRegionLookup: preferredDutyHolder(rootproto.DutyRegionLookup, candidates),
	}
	holders := make(map[string]struct{}, len(assignments))
	for _, holder := range assignments {
		holders[holder] = struct{}{}
	}
	require.Len(t, holders, 3)
}

func TestServicePerDutyAdmissionIsIsolated(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{
				{
					GrantID:         "c1/alloc_id/1",
					HolderID:        "c1",
					Era:             1,
					ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
					Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20)},
				},
				{
					GrantID:         "c2/tso/2",
					HolderID:        "c2",
					Era:             2,
					ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
					Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 200)},
				},
			},
			Allocator: rootview.AllocatorState{IDCurrent: 20, TSCurrent: 200},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrantDuties("c1", nil, []rootproto.DutyID{rootproto.DutyAllocID, rootproto.DutyTSO}, 10*time.Second, 3*time.Second)
	require.NoError(t, svc.ReloadFromStorage())

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	_, err = svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(err))
	require.Equal(t, 0, store.campaignCalls)
}

func TestServiceGrantRenewsInsideRenewWindow(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 100*time.Millisecond, 20*time.Millisecond)

	now := time.Unix(0, 0)
	svc.now = func() time.Time { return now }

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, 1, store.campaignCalls)
	firstGrant, ok := store.snapshot.ActiveGrantFor(rootproto.DutyAllocID, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	require.True(t, ok)

	now = now.Add(85 * time.Millisecond)
	_, err = svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, 2, store.campaignCalls)
	renewedGrant, ok := store.snapshot.ActiveGrantFor(rootproto.DutyAllocID, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	require.True(t, ok)
	require.Equal(t, uint64(2), renewedGrant.Era)
	require.NotEqual(t, firstGrant.GrantID, renewedGrant.GrantID)
	require.NotEmpty(t, renewedGrant.PredecessorRetirements)
}

func TestServiceGrantLoopSkipsFollower(t *testing.T) {
	store := &fakeStorage{leader: false, leaderID: 2}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 80*time.Millisecond, 30*time.Millisecond)

	ctx := t.Context()
	go svc.RunGrantLoop(ctx)

	time.Sleep(80 * time.Millisecond)
	require.Equal(t, 0, store.campaignCalls)
}

func TestServiceGrantLoopDoesNotCampaignOverActiveOtherHolder(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c2/1",
				HolderID:        "c2",
				ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
				Era:             1,
				Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10)},
			}},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 80*time.Millisecond, 30*time.Millisecond)
	require.NoError(t, svc.ReloadFromStorage())

	ctx := t.Context()
	go svc.RunGrantLoop(ctx)

	time.Sleep(80 * time.Millisecond)
	require.Equal(t, 0, store.campaignCalls)
}

func TestServiceDrainAndSealBlocksNewAuthorityRequests(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", time.Second, 300*time.Millisecond)

	doneAdmission, err := svc.beginDutyAdmission(context.Background(), rootproto.DutyAllocID)
	require.NoError(t, err)

	drainDone := make(chan error, 1)
	go func() {
		drainDone <- svc.DrainAndSealGrant(context.Background())
	}()

	require.Eventually(t, func() bool {
		state, inflight := svc.authorityServingSnapshot()
		return state == authorityDraining && inflight == 1
	}, 300*time.Millisecond, 10*time.Millisecond)

	_, err = svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(err))
	_, metadata, ok := nokverrors.RPCErrorInfo(err)
	require.True(t, ok)
	require.Equal(t, reasonGrantNotHeld, metadata[coordinatorReasonMetadata])

	doneAdmission.Done()
	require.NoError(t, <-drainDone)
	state, inflight := svc.authorityServingSnapshot()
	require.Equal(t, authoritySealed, state)
	require.Zero(t, inflight)
	require.Equal(t, 1, store.sealCalls)
	require.NotEmpty(t, store.snapshot.RetiredGrants)
	require.Equal(t, rootproto.GrantRetirementSealedExact, store.snapshot.RetiredGrants[0].Mode)
}

func TestServiceRejectsDrainingAdmissionBeforeGrantRenewal(t *testing.T) {
	now := time.Unix(100, 0)
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c1/1",
				HolderID:        "c1",
				Era:             1,
				ExpiresUnixNano: now.Add(10 * time.Millisecond).UnixNano(),
				Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10)},
			}},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", time.Second, 500*time.Millisecond)
	svc.now = func() time.Time { return now }
	require.NoError(t, svc.ReloadFromStorage())
	svc.authorityMu.Lock()
	svc.setAuthorityDutyLocked(rootproto.DutyAllocID, authorityDutyServing{state: authorityDraining})
	svc.authorityMu.Unlock()

	_, err := svc.beginDutyAdmission(context.Background(), rootproto.DutyAllocID)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(err))
	_, metadata, ok := nokverrors.RPCErrorInfo(err)
	require.True(t, ok)
	require.Equal(t, reasonGrantNotHeld, metadata[coordinatorReasonMetadata])
	require.Zero(t, store.campaignCalls)
}

func TestServiceRejectsDrainingMetadataBeforeGrantRenewal(t *testing.T) {
	now := time.Unix(100, 0)
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c1/1",
				HolderID:        "c1",
				Era:             1,
				ExpiresUnixNano: now.Add(10 * time.Millisecond).UnixNano(),
				Duties:          []rootproto.DutyGrant{rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 1, 0)},
			}},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", time.Second, 500*time.Millisecond)
	svc.now = func() time.Time { return now }
	require.NoError(t, svc.ReloadFromStorage())
	svc.authorityMu.Lock()
	svc.setAuthorityDutyLocked(rootproto.DutyRegionLookup, authorityDutyServing{state: authorityDraining})
	svc.authorityMu.Unlock()

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("k"),
		Freshness: coordpb.Freshness_FRESHNESS_BOUNDED,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(err))
	_, metadata, ok := nokverrors.RPCErrorInfo(err)
	require.True(t, ok)
	require.Equal(t, reasonGrantNotHeld, metadata[coordinatorReasonMetadata])
	require.Zero(t, store.campaignCalls)
}

func TestServiceInheritRetiredGrants(t *testing.T) {
	retired := rootproto.GrantRetirement{
		GrantID:  "c1/1",
		HolderID: "c1",
		Era:      1,
		Mode:     rootproto.GrantRetirementExpiredBound,
		Bounds:   []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10)},
	}
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c2/2",
				HolderID:        "c2",
				Era:             2,
				ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
				Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20)},
			}},
			RetiredGrants: []rootproto.GrantRetirement{retired},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c2", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	loadsBefore := store.loadCalls
	require.NoError(t, svc.InheritRetiredGrants(context.Background()))
	require.Equal(t, 1, store.reattachCalls)
	require.Equal(t, loadsBefore, store.loadCalls)
	require.Equal(t, "c2/2", store.snapshot.RetiredGrants[0].InheritedByGrantID)
	require.Len(t, store.snapshot.GrantInheritances, 1)
}

func TestServiceInheritRetiredGrantsSubmitsOnlyCoveredPredecessors(t *testing.T) {
	allocRetired := rootproto.GrantRetirement{
		GrantID:  "c1/alloc/1",
		HolderID: "c1",
		Era:      1,
		Mode:     rootproto.GrantRetirementExpiredBound,
		Bounds:   []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10)},
	}
	tsoRetired := rootproto.GrantRetirement{
		GrantID:  "c1/tso/2",
		HolderID: "c1",
		Era:      2,
		Mode:     rootproto.GrantRetirementExpiredBound,
		Bounds:   []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 10)},
	}
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c2/alloc/3",
				HolderID:        "c2",
				Era:             3,
				ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
				Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20)},
			}},
			RetiredGrants: []rootproto.GrantRetirement{allocRetired, tsoRetired},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c2", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	require.NoError(t, svc.InheritRetiredGrants(context.Background()))
	require.Equal(t, 1, store.reattachCalls)
	require.Equal(t, "c2/alloc/3", store.snapshot.RetiredGrants[0].InheritedByGrantID)
	require.Empty(t, store.snapshot.RetiredGrants[1].InheritedByGrantID)
	require.Len(t, store.snapshot.GrantInheritances, 1)
	require.Equal(t, allocRetired.GrantID, store.snapshot.GrantInheritances[0].PredecessorGrantID)
}

func TestServiceDutyAdmissionSkipsInheritanceWhenNoPendingRetirement(t *testing.T) {
	now := time.Unix(0, 200)
	grant := rootproto.AuthorityGrant{
		GrantID:         "c1/1",
		HolderID:        "c1",
		Era:             1,
		ExpiresUnixNano: now.Add(time.Hour).UnixNano(),
		Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 100)},
	}
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{grant},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", time.Hour, 30*time.Minute)
	svc.now = func() time.Time { return now }
	require.NoError(t, svc.ReloadFromStorage())
	svc.cacheGrantCertificate(testGrantCertificate(grant))

	admission, err := svc.beginDutyAdmission(context.Background(), rootproto.DutyAllocID)
	require.NoError(t, err)
	admission.Done()
	require.Zero(t, store.reattachCalls)
	require.Equal(t, uint64(1), svc.eunomiaMetrics.grantInheritanceSkippedTotal.Load())
	require.Equal(t, uint64(0), svc.eunomiaMetrics.grantInheritanceSubmittedTotal.Load())
}

func TestServiceAuthorityEvidenceUsesRetiredFloorInsteadOfInheritedHistory(t *testing.T) {
	now := time.Unix(0, 200)
	retired := rootproto.GrantRetirement{
		GrantID:            "c0/1",
		HolderID:           "c0",
		Era:                1,
		Mode:               rootproto.GrantRetirementExpiredBound,
		Bounds:             []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10)},
		InheritedByGrantID: "c1/2",
	}
	grant := rootproto.AuthorityGrant{
		GrantID:         "c1/2",
		HolderID:        "c1",
		Era:             2,
		ExpiresUnixNano: now.Add(time.Hour).UnixNano(),
		Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 100)},
	}
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants:  []rootproto.AuthorityGrant{grant},
			RetiredGrants: []rootproto.GrantRetirement{retired},
			RetiredEraFloors: []rootproto.AuthorityRetiredEraFloor{{
				DutyID:          rootproto.DutyAllocID,
				Scope:           rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
				RetiredEraFloor: 1,
			}},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", time.Hour, 30*time.Minute)
	svc.now = func() time.Time { return now }
	require.NoError(t, svc.ReloadFromStorage())
	svc.cacheGrantCertificate(testGrantCertificate(grant))

	admission, err := svc.beginDutyAdmission(context.Background(), rootproto.DutyAllocID)
	require.NoError(t, err)
	defer admission.Done()
	proof, err := admission.authorityEvidence(rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 20})
	require.NoError(t, err)
	evidence := metawire.RootAuthorityEvidenceFromProto(proof.Evidence)
	require.Equal(t, uint64(1), evidence.ObservedRetiredEraFloor)
	require.Empty(t, evidence.ObservedRetirements)
}

// TestServiceTsoEvidenceIgnoresAllocRetiredFloor verifies the serving-side
// witness payload: a TSO reply must not attach an alloc_id retired floor.
func TestServiceTsoEvidenceIgnoresAllocRetiredFloor(t *testing.T) {
	now := time.Unix(0, 200)
	retired := rootproto.GrantRetirement{
		GrantID:            "c0/22",
		HolderID:           "c0",
		Era:                22,
		Mode:               rootproto.GrantRetirementExpiredBound,
		Bounds:             []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10)},
		InheritedByGrantID: "c1/23",
	}
	grant := rootproto.AuthorityGrant{
		GrantID:         "c1/9",
		HolderID:        "c1",
		Era:             9,
		ExpiresUnixNano: now.Add(time.Hour).UnixNano(),
		Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 100)},
	}
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants:  []rootproto.AuthorityGrant{grant},
			RetiredGrants: []rootproto.GrantRetirement{retired},
			RetiredEraFloors: []rootproto.AuthorityRetiredEraFloor{{
				DutyID:          rootproto.DutyAllocID,
				Scope:           rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
				RetiredEraFloor: 22,
			}},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", time.Hour, 30*time.Minute)
	svc.now = func() time.Time { return now }
	require.NoError(t, svc.ReloadFromStorage())
	svc.cacheGrantCertificate(testGrantCertificate(grant))

	admission, err := svc.beginDutyAdmission(context.Background(), rootproto.DutyTSO)
	require.NoError(t, err)
	defer admission.Done()
	proof, err := admission.authorityEvidence(rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: 20})
	require.NoError(t, err)
	evidence := metawire.RootAuthorityEvidenceFromProto(proof.Evidence)
	require.Zero(t, evidence.ObservedRetiredEraFloor)
	require.Empty(t, evidence.ObservedRetirements)
}

// TestServiceRegionLookupEvidenceIgnoresAllocRetiredFloor covers raftstore route
// lookup witnesses, which are consumed downstream by the KV client route cache.
func TestServiceRegionLookupEvidenceIgnoresAllocRetiredFloor(t *testing.T) {
	now := time.Unix(100, 0)
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c1/region_lookup/9",
				HolderID:        "c1",
				Era:             9,
				ExpiresUnixNano: now.Add(time.Hour).UnixNano(),
				Duties: []rootproto.DutyGrant{
					rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 12, 0),
				},
			}},
			RetiredEraFloors: []rootproto.AuthorityRetiredEraFloor{{
				DutyID:          rootproto.DutyAllocID,
				Scope:           rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
				RetiredEraFloor: 22,
			}},
			Descriptors: map[uint64]topology.Descriptor{
				11: {RegionID: 11, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", time.Hour, 30*time.Minute)
	svc.now = func() time.Time { return now }
	require.NoError(t, svc.ReloadFromStorage())

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.Equal(t, uint64(9), resp.GetEra())
	require.Zero(t, resp.GetObservedRetiredEraFloor())
}

// TestServiceAllocAdmissionRejectsAllocGrantAtScopedRetiredFloor proves Silence
// still holds for the duty that actually owns the scoped floor.
func TestServiceAllocAdmissionRejectsAllocGrantAtScopedRetiredFloor(t *testing.T) {
	now := time.Unix(100, 0)
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.now = func() time.Time { return now }
	svc.ConfigureAuthorityGrant("coord-1", time.Hour, 10*time.Minute)
	svc.refreshGrantMirror(rootview.Snapshot{
		ActiveGrants: []rootproto.AuthorityGrant{{
			GrantID:         "coord-1/alloc/12",
			HolderID:        "coord-1",
			Era:             12,
			ExpiresUnixNano: now.Add(time.Hour).UnixNano(),
			Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 100)},
		}},
		RetiredEraFloors: []rootproto.AuthorityRetiredEraFloor{{
			DutyID:          rootproto.DutyAllocID,
			Scope:           rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal},
			RetiredEraFloor: 12,
		}},
	})

	_, err := svc.admitDutyFromCachedGrant(rootproto.DutyAllocID)

	require.Error(t, err)
	require.Contains(t, err.Error(), "silence violated")
	require.Contains(t, err.Error(), "retired_floor=12")
}

// TestCoordinatorGrantViewRetiredEraFloorForIsScopedOnly documents the breaking
// cleanup: only explicit duty/scope floors are authoritative.
func TestCoordinatorGrantViewRetiredEraFloorForIsScopedOnly(t *testing.T) {
	global := rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}
	var view coordinatorGrantView
	view.Refresh(rootview.Snapshot{})

	require.Zero(t, view.RetiredEraFloorFor(rootproto.DutyTSO, global))

	view.Refresh(rootview.Snapshot{
		RetiredEraFloors: []rootproto.AuthorityRetiredEraFloor{{
			DutyID:          rootproto.DutyAllocID,
			Scope:           global,
			RetiredEraFloor: 22,
		}},
	})
	require.Equal(t, uint64(22), view.RetiredEraFloorFor(rootproto.DutyAllocID, global))
	require.Zero(t, view.RetiredEraFloorFor(rootproto.DutyTSO, global))
}

func TestServiceGrantAdmissionSurvivesStaleReloadAfterIssue(t *testing.T) {
	stale := rootview.Snapshot{
		RootToken: rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 10}, Revision: 10},
		ActiveGrants: []rootproto.AuthorityGrant{{
			GrantID:         "c2/1",
			HolderID:        "c2",
			Era:             1,
			ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
			Duties: []rootproto.DutyGrant{
				rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10),
				rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 100),
			},
		}},
	}
	store := &staleGrantReloadStorage{
		fakeStorage: &fakeStorage{
			leader: true,
			snapshot: rootview.Snapshot{
				RootToken: rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 11}, Revision: 11},
				ActiveGrants: []rootproto.AuthorityGrant{{
					GrantID:         "expired/1",
					HolderID:        "expired",
					Era:             1,
					ExpiresUnixNano: 1,
					Duties: []rootproto.DutyGrant{
						rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 10),
						rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 100),
					},
				}},
				Allocator: rootview.AllocatorState{IDCurrent: 10, TSCurrent: 100},
			},
		},
		staleSnapshot: stale,
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", time.Second, 300*time.Millisecond)
	svc.now = func() time.Time { return time.Unix(0, 200) }

	resp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(100), resp.GetTimestamp())
	current := svc.currentGrant(rootproto.DutyTSO)
	require.Equal(t, "c1", current.HolderID)
	require.Equal(t, uint64(2), current.Era)
	require.Equal(t, 1, store.campaignCalls)
}

func TestServiceGrantRetryDelayBacksOff(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100))
	svc.now = func() time.Time { return time.Unix(0, 123456789) }

	first := svc.coordinatorGrantRetryDelay(1)
	fourth := svc.coordinatorGrantRetryDelay(4)
	sixteenth := svc.coordinatorGrantRetryDelay(16)

	require.GreaterOrEqual(t, first, 160*time.Millisecond)
	require.LessOrEqual(t, first, 240*time.Millisecond)
	require.GreaterOrEqual(t, fourth, 1280*time.Millisecond)
	require.LessOrEqual(t, fourth, 1920*time.Millisecond)
	require.GreaterOrEqual(t, sixteenth, 48*time.Second)
	require.LessOrEqual(t, sixteenth, 60*time.Second)
}

func TestServiceReleaseGrantSealsGrant(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	require.NoError(t, svc.ReleaseGrant())
	require.Equal(t, 1, store.sealCalls)
	_, ok := store.snapshot.ActiveGrantFor(rootproto.DutyAllocID, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	require.False(t, ok)
	require.Len(t, store.snapshot.RetiredGrants, 1)
	require.Equal(t, rootproto.GrantRetirementSealedExact, store.snapshot.RetiredGrants[0].Mode)
}

func TestServiceSealGrant(t *testing.T) {
	store := &fakeStorage{leader: true}
	cluster := catalog.NewCluster()
	svc := NewService(cluster, idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }

	allocResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), allocResp.GetFirstId())

	tsoResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsoResp.GetTimestamp())
	campaignCallsBeforeSeal := store.campaignCalls

	require.NoError(t, svc.SealGrant())
	require.Equal(t, 2, store.sealCalls)
	require.Empty(t, store.snapshot.ActiveGrants)
	require.Len(t, store.snapshot.RetiredGrants, 2)
	retired := grantRetirementForDuty(t, store.snapshot.RetiredGrants, rootproto.DutyAllocID)
	require.Equal(t, "c1/alloc_id/1", retired.GrantID)
	require.Equal(t, "c1", retired.HolderID)
	require.Equal(t, uint64(1), retired.Era)
	require.Equal(t, rootproto.GrantRetirementSealedExact, retired.Mode)
	require.Equal(t, uint64(11), grantMonotoneBoundForTest(retired.Bounds, rootproto.DutyAllocID))
	tsoRetired := grantRetirementForDuty(t, store.snapshot.RetiredGrants, rootproto.DutyTSO)
	require.Equal(t, uint64(102), grantMonotoneBoundForTest(tsoRetired.Bounds, rootproto.DutyTSO))

	nextID, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(2), nextID.GetEra())
	require.Equal(t, campaignCallsBeforeSeal+1, store.campaignCalls)
	nextGrant, ok := store.snapshot.ActiveGrantFor(rootproto.DutyAllocID, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	require.True(t, ok)
	require.NotEmpty(t, nextGrant.PredecessorRetirements)
	require.Equal(t, "c1/alloc_id/1", nextGrant.PredecessorRetirements[0].GrantID)
}

func TestServiceSealGrantIgnoresOtherHolder(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c2/2",
				HolderID:        "c2",
				ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
				Era:             2,
				Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20)},
			}},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	err := svc.SealGrant()
	require.NoError(t, err)
	require.Equal(t, 0, store.sealCalls)
	otherGrant, ok := store.snapshot.ActiveGrantFor(rootproto.DutyAllocID, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	require.True(t, ok)
	require.Equal(t, "c2/2", otherGrant.GrantID)
}

func TestServiceDutyAdmissionRejectsOtherActiveGrant(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c2/1",
				HolderID:        "c2",
				ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
				Era:             1,
				Duties:          []rootproto.DutyGrant{rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20)},
			}},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, svc.ReloadFromStorage())

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(err))
	_, metadata, ok := nokverrors.RPCErrorInfo(err)
	require.True(t, ok)
	require.Equal(t, reasonGrantNotHeld, metadata[coordinatorReasonMetadata])
	require.Equal(t, 0, store.campaignCalls)
}

func TestServiceAllocIDIssuesMissingDutyGrant(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c1/2",
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(10, 0).UnixNano(),
				Era:             2,
				Duties: []rootproto.DutyGrant{
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 200),
					rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 0, 0),
				},
			}},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())
	_, hasAllocID := svc.currentGrant(rootproto.DutyAllocID).Duty(rootproto.DutyAllocID)
	require.False(t, hasAllocID)

	resp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(10), resp.GetFirstId())
	require.Equal(t, 1, store.campaignCalls)
}

func TestServiceGetRegionByKeyIssuesMissingDutyGrant(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c1/2",
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(10, 0).UnixNano(),
				Era:             2,
				Duties: []rootproto.DutyGrant{
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20),
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 200),
				},
			}},
			Descriptors: map[uint64]topology.Descriptor{
				11: {RegionID: 11, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())
	_, hasRegionLookup := svc.currentGrant(rootproto.DutyRegionLookup).Duty(rootproto.DutyRegionLookup)
	require.False(t, hasRegionLookup)

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("m"),
		Freshness: coordpb.Freshness_FRESHNESS_BEST_EFFORT,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(11), resp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, 1, store.campaignCalls)
}

func TestServiceGetRegionByKeyRenewsWhenDescriptorOutsideGrant(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c1/2",
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(10, 0).UnixNano(),
				Era:             2,
				Duties: []rootproto.DutyGrant{
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, 20),
					rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, 200),
					rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 6, 0),
				},
			}},
			Descriptors: map[uint64]topology.Descriptor{
				11: {RegionID: 11, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("m"),
		Freshness: coordpb.Freshness_FRESHNESS_BEST_EFFORT,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(11), resp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, uint64(7), resp.GetDescriptorRevision())
	require.NotNil(t, resp.GetAuthorityEvidence())
	require.Equal(t, 1, store.campaignCalls)
}

func TestServiceGetRegionByKeyRenewCoversRetiredRegionLookupBound(t *testing.T) {
	now := time.Unix(100, 0)
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "c1/region_lookup/10",
				HolderID:        "c1",
				ExpiresUnixNano: now.Add(-time.Second).UnixNano(),
				Era:             10,
				Duties: []rootproto.DutyGrant{
					rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 100, 0),
				},
			}},
			RetiredGrants: []rootproto.GrantRetirement{{
				GrantID:  "c1/region_lookup/9",
				HolderID: "c1",
				Era:      9,
				Mode:     rootproto.GrantRetirementSealedExact,
				Bounds: []rootproto.DutyGrant{
					rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 96, 0),
				},
			}},
			Descriptors: map[uint64]topology.Descriptor{
				11: {RegionID: 11, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			},
			RootToken: rootstorage.TailToken{Revision: 8},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return now }
	require.NoError(t, svc.ReloadFromStorage())

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("m"),
		Freshness: coordpb.Freshness_FRESHNESS_BEST_EFFORT,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(11), resp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, 1, store.campaignCalls)
	renewed, ok := store.snapshot.ActiveGrantFor(rootproto.DutyRegionLookup, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	require.True(t, ok)
	require.Equal(t, uint64(100), regionLookupRevisionBound(renewed.Duties))
	require.NotEmpty(t, renewed.PredecessorRetirements)
}

func TestServiceStoreHeartbeatSuppressesOperationsWithoutGrant(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Stores: map[uint64]rootstate.StoreMembership{
				1: {StoreID: 1, State: rootstate.StoreMembershipActive},
				2: {StoreID: 2, State: rootstate.StoreMembershipActive},
			},
			ActiveGrants: []rootproto.AuthorityGrant{{
				GrantID:         "other/1",
				HolderID:        "other",
				ExpiresUnixNano: 10_000,
				Era:             1,
				Duties:          []rootproto.DutyGrant{rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, 0, 0)},
			}},
		},
	}
	svc := NewService(catalog.NewCluster(), nil, nil, store)
	svc.ConfigureAuthorityGrant("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }
	joinStores(t, svc, 1, 2)

	err := publishDescriptorEvent(t, svc, testDescriptor(100, []byte(""), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}}), 0)
	require.NoError(t, err)

	_, err = svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:   2,
		LeaderNum: 1,
		RegionNum: 1,
	})
	require.NoError(t, err)

	resp, err := svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:   1,
		LeaderNum: 10,
		RegionNum: 1,
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Empty(t, resp.GetOperations())
}

func grantMonotoneBoundForTest(duties []rootproto.DutyGrant, duty rootproto.DutyID) uint64 {
	for _, candidate := range duties {
		if candidate.DutyID == duty && candidate.Bound.Kind == rootproto.DutyBoundMonotone {
			return candidate.Bound.MonotoneUpper
		}
	}
	return 0
}

func grantRetirementForDuty(t *testing.T, retirements []rootproto.GrantRetirement, duty rootproto.DutyID) rootproto.GrantRetirement {
	t.Helper()
	for _, retirement := range retirements {
		for _, bound := range retirement.Bounds {
			if bound.DutyID == duty {
				return retirement
			}
		}
	}
	t.Fatalf("missing retirement for duty %s", rootproto.DutyName(duty))
	return rootproto.GrantRetirement{}
}

func TestServiceRejectsWritesOnFollower(t *testing.T) {
	store := &fakeStorage{leader: false, leaderID: 2}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)

	err := publishDescriptorEvent(t, svc, testDescriptor(8, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(err))
	_, metadata, ok := nokverrors.RPCErrorInfo(err)
	require.True(t, ok)
	require.Equal(t, reasonNotLeader, metadata[coordinatorReasonMetadata])

	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionTombstoned(8)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 8})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	joinStores(t, svc, 1)
	_, err = svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{StoreId: 1})
	require.NoError(t, err)
	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
}

func TestServiceRefreshFromStorageReloadsViewAndAllocatorState(t *testing.T) {
	store := &fakeSyncStorage{
		fakeStorage: fakeStorage{leader: false, leaderID: 2},
		snapshot: rootview.Snapshot{
			ClusterEpoch: 4,
			Descriptors: map[uint64]topology.Descriptor{
				9: testDescriptor(9, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil),
			},
			Allocator: rootview.AllocatorState{
				IDCurrent: 120,
				TSCurrent: 450,
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	require.NoError(t, svc.RefreshFromStorage())

	getResp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.Equal(t, uint64(9), getResp.GetRegionDescriptor().GetRegionId())

	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Nil(t, idResp)

	store.leader = true
	store.leaderID = 0

	idResp, err = svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(121), idResp.GetFirstId())

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(451), tsResp.GetTimestamp())
}

func TestServicePublishRootEventAssignsRootEpoch(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	err := publishDescriptorEvent(t, svc, testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)

	event := rootevent.PeerAdded(1, 2, 201, testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, nil))
	event.PeerChange.Region.RootEpoch = 0
	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	require.NoError(t, err)
	require.Equal(t, rootevent.KindPeerAdded, store.lastEvent.Kind)
	require.Equal(t, uint64(2), store.lastEvent.PeerChange.Region.RootEpoch)
	require.Equal(t, uint64(2), store.snapshot.ClusterEpoch)
}

func TestServiceMutatingWritesRespectExpectedClusterEpoch(t *testing.T) {
	store := &fakeStorage{snapshot: rootview.Snapshot{ClusterEpoch: 7}}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	err := publishDescriptorEvent(t, svc, testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 6)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, store.eventCalls)

	err = publishDescriptorEvent(t, svc, testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 7)
	require.NoError(t, err)
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, uint64(8), store.snapshot.ClusterEpoch)

	event := rootevent.PeerAdded(11, 2, 201, testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, nil))
	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event:                metawire.RootEventToProto(event),
		ExpectedClusterEpoch: 7,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 1, store.eventCalls)

	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event:                metawire.RootEventToProto(event),
		ExpectedClusterEpoch: 8,
	})
	require.NoError(t, err)
	require.Equal(t, 2, store.eventCalls)
	require.Equal(t, uint64(9), store.snapshot.ClusterEpoch)

	_, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{
		RegionId:             11,
		ExpectedClusterEpoch: 8,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 2, store.eventCalls)

	resp, err := svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{
		RegionId:             11,
		ExpectedClusterEpoch: 9,
	})
	require.NoError(t, err)
	require.True(t, resp.GetRemoved())
	require.Equal(t, 3, store.eventCalls)
	require.Equal(t, uint64(10), store.snapshot.ClusterEpoch)
}

func testDescriptor(id uint64, start, end []byte, epoch metaregion.Epoch, peers []metaregion.Peer) topology.Descriptor {
	desc := topology.Descriptor{
		RegionID: id,
		StartKey: append([]byte(nil), start...),
		EndKey:   append([]byte(nil), end...),
		Epoch:    epoch,
		Peers:    append([]metaregion.Peer(nil), peers...),
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}
