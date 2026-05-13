package server

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
	"strings"
	"time"

	coordfailpoints "github.com/feichai0017/NoKV/coordinator/failpoints"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	"github.com/feichai0017/NoKV/coordinator/scheduling"
	rootfailpoints "github.com/feichai0017/NoKV/meta/root/failpoints"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) requireRootWriteAccess() error {
	if s == nil || s.storage == nil {
		return nil
	}
	if s.storage.CanSubmitRootWrites() {
		return nil
	}
	leaderID := s.storage.LeaderID()
	if leaderID != 0 {
		return statusNotLeader(leaderID)
	}
	return statusNotLeader(0)
}

func (s *Service) grantScopedStoreOperations(ctx context.Context, storeID uint64) []*coordpb.SchedulerOperation {
	if s == nil || !s.coordinatorGrantEnabled() {
		return s.storeControlOperations(ctx, storeID)
	}
	if s.storage != nil && !s.storage.CanSubmitRootWrites() {
		return nil
	}
	if err := s.ensureGrant(ctx, rootproto.DutyRegionLookup); err != nil {
		return nil
	}
	return s.storeControlOperations(ctx, storeID)
}

func (s *Service) storeControlOperations(ctx context.Context, storeID uint64) []*coordpb.SchedulerOperation {
	if s == nil || s.cluster == nil || storeID == 0 {
		return nil
	}
	opts := scheduling.PlanOptions{
		NextID: s.schedulerNextID(ctx),
	}
	if s.scheduler != nil {
		return s.scheduler.PlanStoreOperationsWithOptions(storeID, s.cluster.Snapshot(), opts)
	}
	return scheduling.PlanStoreOperationsWithOptions(storeID, s.cluster.Snapshot(), opts)
}

func (s *Service) ConfigureSchedulerSplitBoundaries(boundaries [][]byte) {
	if s == nil {
		return
	}
	if s.scheduler == nil {
		s.scheduler = scheduling.NewPlanner(scheduling.PlanOptions{})
	}
	s.scheduler.ConfigureOptions(scheduling.PlanOptions{
		SplitKey: scheduling.SplitKeyFromBoundaries(boundaries),
	})
}

func (s *Service) schedulerNextID(ctx context.Context) func() (uint64, bool) {
	return func() (uint64, bool) {
		if s == nil {
			return 0, false
		}
		if s.coordinatorGrantEnabled() {
			if err := s.ensureGrant(ctx, rootproto.DutyAllocID); err != nil {
				return 0, false
			}
		}
		id, err := s.reserveIDs(ctx, 1)
		if err != nil {
			return 0, false
		}
		return id, true
	}
}

// RunGrantLoop keeps the local coordinator grant renewed while ctx
// remains alive. The loop is explicit so callers can decide lifecycle and avoid
// hidden background goroutines in constructors.
func (s *Service) RunGrantLoop(ctx context.Context) {
	if s == nil || ctx == nil || !s.coordinatorGrantEnabled() || s.storage == nil {
		return
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			if s.storage.CanSubmitRootWrites() {
				releaseCtx, cancel := context.WithTimeout(context.Background(), defaultGrantReleaseTimeout)
				_ = s.DrainAndSealGrant(releaseCtx)
				cancel()
			}
			return
		case <-timer.C:
			next := s.coordinatorGrantLoopInterval()
			if s.storage.CanSubmitRootWrites() {
				if err := s.ensureConfiguredGrants(ctx); err != nil {
					failures++
					next = s.coordinatorGrantRetryDelay(failures)
				} else {
					_ = s.InheritRetiredGrants(ctx)
					failures = 0
					next = s.jitterDuration(next, 20)
				}
			} else {
				failures = 0
				next = s.jitterDuration(next, 20)
			}
			timer.Reset(next)
		}
	}
}

// ReleaseGrant explicitly releases the current rooted coordinator
// grant for the configured holder. It is intended for graceful shutdown.
func (s *Service) ReleaseGrant() error {
	return s.releaseGrant(context.Background())
}

// DrainAndSealGrant stops admitting new authority-bearing requests, waits for
// requests already in service, then records one rooted exact retirement.
func (s *Service) DrainAndSealGrant(ctx context.Context) error {
	if s == nil || !s.coordinatorGrantEnabled() || s.storage == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.storage.CanSubmitRootWrites() {
		return nil
	}
	duties := s.localActiveAuthorityDuties()
	if len(duties) == 0 {
		duties = s.localGrantDuties()
	}
	if s.localGrantAlreadySealed() {
		s.markAuthoritySealed(duties)
		return nil
	}
	if s.allAuthorityDutiesSealed(duties) {
		return nil
	}
	s.markAuthorityDraining(duties)

	if err := s.waitAuthorityInflightDrained(ctx, duties); err != nil {
		s.markAuthorityServing(duties)
		return err
	}
	if s.localGrantAlreadySealed() {
		s.markAuthoritySealed(duties)
		return nil
	}
	if err := s.sealGrant(ctx); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			_ = s.reloadAndFenceAllocators(true)
		}
		if s.localGrantAlreadySealed() {
			s.markAuthoritySealed(duties)
			return nil
		}
		s.markAuthorityServing(duties)
		return err
	}
	s.markAuthoritySealed(duties)
	return nil
}

func (s *Service) waitAuthorityInflightDrained(ctx context.Context, duties []rootproto.DutyID) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		inflight := s.authorityInflightForDuties(duties)
		if inflight == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) localGrantAlreadySealed() bool {
	s.grantMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	grants := s.grantView.Grants()
	retirements := s.grantView.Retirements()
	s.grantMu.RUnlock()
	if holderID == "" {
		return false
	}
	retiredByGrant := make(map[string]struct{}, len(retirements))
	hasLocalRetirement := false
	for _, retirement := range retirements {
		if strings.TrimSpace(retirement.HolderID) != holderID || !retirement.Present() {
			continue
		}
		hasLocalRetirement = true
		retiredByGrant[retirement.GrantID] = struct{}{}
	}
	hasLocalActive := false
	for _, grant := range grants {
		if strings.TrimSpace(grant.HolderID) != holderID {
			continue
		}
		hasLocalActive = true
		if _, ok := retiredByGrant[grant.GrantID]; !ok {
			return false
		}
	}
	if hasLocalActive {
		return true
	}
	return hasLocalRetirement
}

func (s *Service) markAuthorityServing(duties []rootproto.DutyID) {
	if s == nil {
		return
	}
	s.authorityMu.Lock()
	for _, duty := range duties {
		slot := s.authorityDuties[duty]
		if slot.state != authoritySealed {
			slot.state = authorityServing
		}
		s.setAuthorityDutyLocked(duty, slot)
	}
	s.authorityMu.Unlock()
}

func (s *Service) markAuthorityDraining(duties []rootproto.DutyID) {
	if s == nil {
		return
	}
	s.authorityMu.Lock()
	for _, duty := range duties {
		slot := s.authorityDuties[duty]
		if slot.state != authoritySealed {
			slot.state = authorityDraining
		}
		s.setAuthorityDutyLocked(duty, slot)
	}
	s.authorityMu.Unlock()
}

func (s *Service) markAuthoritySealed(duties []rootproto.DutyID) {
	if s == nil {
		return
	}
	s.authorityMu.Lock()
	for _, duty := range duties {
		slot := s.authorityDuties[duty]
		slot.state = authoritySealed
		s.setAuthorityDutyLocked(duty, slot)
	}
	s.authorityMu.Unlock()
}

func (s *Service) allAuthorityDutiesSealed(duties []rootproto.DutyID) bool {
	if s == nil || len(duties) == 0 {
		return false
	}
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	for _, duty := range duties {
		if s.authorityDuties[duty].state != authoritySealed {
			return false
		}
	}
	return true
}

func (s *Service) authorityInflightForDuties(duties []rootproto.DutyID) uint64 {
	if s == nil {
		return 0
	}
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	var inflight uint64
	for _, duty := range duties {
		inflight += s.authorityDuties[duty].inflight
	}
	return inflight
}

func (s *Service) releaseGrant(ctx context.Context) error {
	return s.sealGrant(ctx)
}

// SealGrant records one rooted exact retirement for the current
// authority era using the frontiers already consumed by this service.
func (s *Service) SealGrant() error {
	return s.sealGrant(context.Background())
}

func (s *Service) sealGrant(ctx context.Context) error {
	if s == nil || !s.coordinatorGrantEnabled() || s.storage == nil {
		return nil
	}
	if !s.storage.CanSubmitRootWrites() {
		return nil
	}
	if err := rootfailpoints.InjectBeforeGrantStorageRead(); err != nil {
		return statusInternal(err.Error())
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return statusInternalf("load rooted snapshot: %v", err)
	}
	s.refreshCurrentRootSnapshot(snapshot)
	s.grantMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.grantMu.RUnlock()
	for _, grant := range snapshot.ActiveGrants {
		if !grant.Present() || strings.TrimSpace(grant.HolderID) != holderID {
			continue
		}
		if err := s.sealGrantInstance(ctx, holderID, grant); err != nil {
			return err
		}
	}
	return s.reloadAndFenceAllocators(true)
}

func (s *Service) sealGrantInstance(ctx context.Context, holderID string, grant rootproto.AuthorityGrant) error {
	exactUsages := s.exactUsagesForGrant(grant)
	protocolState, _, err := s.storage.ApplyGrant(ctx, rootproto.GrantCommand{
		Kind:        rootproto.GrantActSeal,
		HolderID:    holderID,
		GrantID:     grant.GrantID,
		NowUnixNano: s.nowUnixNano(),
		ExactUsages: exactUsages,
	})
	if err != nil {
		s.eunomiaMetrics.recordGuaranteeViolationForError(err)
		return err
	}
	s.publishEunomiaState(protocolState)
	if err := coordfailpoints.InjectAfterSealGrantBeforeReload(); err != nil {
		return err
	}
	return nil
}

func (s *Service) exactUsagesForGrant(grant rootproto.AuthorityGrant) []rootproto.AuthorityUsage {
	s.allocMu.Lock()
	consumedIDFrontier := s.ids.Current()
	consumedTSOFrontier := s.tso.Current()
	s.allocMu.Unlock()
	out := make([]rootproto.AuthorityUsage, 0, len(grant.Duties))
	for _, duty := range grant.Duties {
		switch duty.DutyID {
		case rootproto.DutyAllocID:
			out = append(out, rootproto.AuthorityUsage{DutyID: duty.DutyID, Scope: duty.Scope, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: consumedIDFrontier}})
		case rootproto.DutyTSO:
			out = append(out, rootproto.AuthorityUsage{DutyID: duty.DutyID, Scope: duty.Scope, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: consumedTSOFrontier}})
		case rootproto.DutyRegionLookup:
			out = append(out, rootproto.AuthorityUsage{DutyID: duty.DutyID, Scope: duty.Scope, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundVersion, DescriptorRevisionCeiling: s.currentDescriptorRevision()}})
		}
	}
	return out
}

func (s *Service) InheritRetiredGrants(ctx context.Context) error {
	if s == nil || !s.coordinatorGrantEnabled() || s.storage == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.storage.CanSubmitRootWrites() {
		return nil
	}
	s.grantMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.grantMu.RUnlock()
	if holderID == "" {
		return nil
	}
	snapshot, ok := s.cachedRootSnapshot()
	if !ok {
		var err error
		snapshot, err = s.storage.Load()
		if err != nil {
			return err
		}
		s.refreshCurrentRootSnapshot(snapshot)
	}
	pending := pendingGrantInheritanceIDs(snapshot, holderID)
	if len(pending) == 0 {
		return nil
	}
	protocolState, _, err := s.storage.ApplyGrant(ctx, rootproto.GrantCommand{
		Kind:                rootproto.GrantActInherit,
		HolderID:            holderID,
		PredecessorGrantIDs: pending,
	})
	if err != nil {
		if grantInheritanceIgnorable(err) {
			return nil
		}
		return err
	}
	s.publishEunomiaState(protocolState)
	// Inheritance only advances the grant lifecycle. Allocator fences and
	// descriptor state are unchanged, so a full rooted reload would put every
	// hot TSO/AllocID request back on the checkpoint-read path.
	return nil
}

func (s *Service) grantInheritanceCandidate() bool {
	if s == nil || !s.coordinatorGrantEnabled() || s.storage == nil {
		return false
	}
	snapshot, ok := s.cachedRootSnapshot()
	if !ok {
		return false
	}
	s.grantMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.grantMu.RUnlock()
	return len(pendingGrantInheritanceIDs(snapshot, holderID)) > 0
}

func pendingGrantInheritanceIDs(snapshot rootview.Snapshot, holderID string) []string {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" || !snapshotHasLocalGrant(snapshot, holderID) {
		return nil
	}
	grants := localActiveGrants(snapshot, holderID)
	pending := make([]string, 0, len(snapshot.RetiredGrants))
	for _, retirement := range snapshot.RetiredGrants {
		if retirement.InheritedByGrantID == "" && retirement.GrantID != "" && localGrantCoversRetirement(grants, retirement) {
			pending = append(pending, retirement.GrantID)
		}
	}
	return pending
}

func localActiveGrants(snapshot rootview.Snapshot, holderID string) []rootproto.AuthorityGrant {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return nil
	}
	out := make([]rootproto.AuthorityGrant, 0, len(snapshot.ActiveGrants))
	for _, grant := range snapshot.ActiveGrants {
		if strings.TrimSpace(grant.HolderID) == holderID {
			out = append(out, grant)
		}
	}
	return out
}

func localGrantCoversRetirement(grants []rootproto.AuthorityGrant, retirement rootproto.GrantRetirement) bool {
	for _, grant := range grants {
		if dutyGrantSetCovers(grant.Duties, retirement.Bounds) {
			return true
		}
	}
	return false
}

func dutyGrantSetCovers(grants, required []rootproto.DutyGrant) bool {
	for _, req := range required {
		found := false
		for _, grant := range grants {
			if grant.DutyID == req.DutyID &&
				rootproto.ScopeEqual(grant.Scope, req.Scope) &&
				rootproto.DutyBoundCovers(grant.Bound, req.Bound) {
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

func grantInheritanceIgnorable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, rootstate.ErrFinality) ||
		errors.Is(err, rootstate.ErrInheritance) ||
		errors.Is(err, rootstate.ErrPrimacy) {
		return true
	}
	code := status.Code(err)
	return code == codes.FailedPrecondition || code == codes.InvalidArgument
}

func (s *Service) ensureConfiguredGrants(ctx context.Context) error {
	for _, duty := range s.localGrantDuties() {
		if err := s.ensureGrant(ctx, duty); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ensureGrant(ctx context.Context, duty rootproto.DutyID) error {
	if s == nil || !s.coordinatorGrantEnabled() || s.storage == nil {
		return nil
	}
	if duty == "" || !s.localCoordinatorOwnsDuty(duty) {
		return nil
	}
	// Fast path: avoid serializing read traffic behind the campaign lock while
	// the current grant is still outside the renew and clock-skew windows.
	nowUnixNano, _, holderID, renewIn, clockSkew := s.grantCampaignBounds()
	if s.coordinatorGrantStillValid(duty, holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}
	if err := s.activeOtherGrantError(duty, holderID, nowUnixNano); err != nil {
		return err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// Another request or the background renew loop may have refreshed grant
	// while this caller waited for writeMu.
	nowUnixNano, _, holderID, renewIn, clockSkew = s.grantCampaignBounds()
	if s.coordinatorGrantStillValid(duty, holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}
	if err := s.refreshCurrentGrantCertificateIfNeeded(ctx, duty, holderID, nowUnixNano); err != nil {
		return err
	}
	if s.coordinatorGrantStillValid(duty, holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}
	if err := s.activeOtherGrantError(duty, holderID, nowUnixNano); err != nil {
		return err
	}

	requestedDuty, exactUsage := s.grantDutyRequest(duty)
	// Recompute time and expiry after sampling allocator fences so the grant
	// command carries fresh bounds and does not campaign unnecessarily.
	nowUnixNano, expiresUnixNano, holderID, renewIn, clockSkew := s.grantCampaignBounds()
	if s.coordinatorGrantStillValid(duty, holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}
	currentEra := s.currentGrant(duty).Era
	protocolState, cert, err := s.storage.ApplyGrant(ctx, rootproto.GrantCommand{
		Kind:            rootproto.GrantActIssue,
		HolderID:        holderID,
		GrantID:         nextCoordinatorGrantID(holderID, duty, currentEra),
		ExpiresUnixNano: expiresUnixNano,
		NowUnixNano:     nowUnixNano,
		RequestedDuties: []rootproto.DutyGrant{requestedDuty},
		ExactUsages:     []rootproto.AuthorityUsage{exactUsage},
	})
	if err != nil {
		s.eunomiaMetrics.recordGuaranteeViolationForError(err)
		return err
	}
	s.publishEunomiaState(protocolState)
	s.cacheGrantCertificate(cert)
	if cert.Grant.Present() {
		s.eunomiaMetrics.recordGrantEraTransition(currentEra, cert.Grant.Era)
	}
	return s.reloadAndFenceAllocators(true)
}

func nextCoordinatorGrantID(holderID string, duty rootproto.DutyID, currentEra uint64) string {
	holderID = strings.TrimSpace(holderID)
	if holderID == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s/%d", holderID, rootproto.DutyName(duty), currentEra+1)
}

func (s *Service) grantDutyRequest(duty rootproto.DutyID) (rootproto.DutyGrant, rootproto.AuthorityUsage) {
	scope := rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}
	switch duty {
	case rootproto.DutyAllocID:
		s.allocMu.Lock()
		consumed := s.ids.Current()
		upper, _ := addUint64(s.currentIDFenceLocked(), s.effectiveIDWindowSize())
		s.allocMu.Unlock()
		return rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, upper),
			rootproto.AuthorityUsage{DutyID: duty, Scope: scope, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: consumed}}
	case rootproto.DutyTSO:
		s.allocMu.Lock()
		consumed := s.tso.Current()
		upper, _ := addUint64(s.currentTSOFenceLocked(), s.effectiveTSOWindowSize())
		s.allocMu.Unlock()
		return rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, upper),
			rootproto.AuthorityUsage{DutyID: duty, Scope: scope, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: consumed}}
	case rootproto.DutyRegionLookup:
		revision := s.currentRegionLookupRevision()
		return rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, revision, 0),
			rootproto.AuthorityUsage{DutyID: duty, Scope: scope, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundVersion, DescriptorRevisionCeiling: revision}}
	default:
		return rootproto.DutyGrant{}, rootproto.AuthorityUsage{}
	}
}

func (s *Service) currentRegionLookupRevision() uint64 {
	// RegionLookup grants fence route descriptors, not every root-log event.
	// Using the root token revision here makes a grant self-invalidating:
	// issuing the grant appends a root event, advances the root token, and then
	// immediately forces another renewal even though no descriptor changed.
	revision := s.currentDescriptorRevision()
	if floor := s.currentRegionLookupGrantFloor(); floor > revision {
		revision = floor
	}
	return revision
}

func (s *Service) currentRegionLookupGrantFloor() uint64 {
	if s == nil {
		return 0
	}
	s.grantMu.RLock()
	view := s.grantView
	s.grantMu.RUnlock()
	var floor uint64
	// Version-bounded duties must be successor-monotone. A restarted
	// coordinator can rebuild a smaller in-memory descriptor revision than the
	// ceiling carried by an expired or sealed predecessor, but root will reject
	// a successor grant that does not cover that predecessor. Keep the requested
	// region_lookup bound at least as high as every non-inherited predecessor
	// and currently rooted grant.
	for _, grant := range view.Grants() {
		floor = maxUint64(floor, regionLookupRevisionBound(grant.Duties))
	}
	for _, retirement := range view.Retirements() {
		if retirement.InheritedByGrantID != "" {
			continue
		}
		floor = maxUint64(floor, regionLookupRevisionBound(retirement.Bounds))
	}
	return floor
}

func regionLookupRevisionBound(duties []rootproto.DutyGrant) uint64 {
	var out uint64
	for _, duty := range duties {
		if duty.DutyID != rootproto.DutyRegionLookup || duty.Scope.Kind != rootproto.DutyScopeGlobal || duty.Bound.Kind != rootproto.DutyBoundVersion {
			continue
		}
		out = maxUint64(out, duty.Bound.DescriptorRevisionCeiling)
	}
	return out
}

func (s *Service) localGrantDuties() []rootproto.DutyID {
	if s == nil {
		return nil
	}
	s.grantMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	candidates := append([]string(nil), s.grantCandidates...)
	duties := append([]rootproto.DutyID(nil), s.grantDuties...)
	s.grantMu.RUnlock()
	if len(duties) == 0 {
		duties = []rootproto.DutyID{rootproto.DutyAllocID, rootproto.DutyTSO, rootproto.DutyRegionLookup}
	}
	if holderID == "" {
		return nil
	}
	if len(candidates) <= 1 {
		return duties
	}
	out := make([]rootproto.DutyID, 0, len(duties))
	for _, duty := range duties {
		if preferredDutyHolder(duty, candidates) == holderID {
			out = append(out, duty)
		}
	}
	return out
}

func (s *Service) localCoordinatorOwnsDuty(duty rootproto.DutyID) bool {
	return slices.Contains(s.localGrantDuties(), duty)
}

func (s *Service) localActiveAuthorityDuties() []rootproto.DutyID {
	if s == nil {
		return nil
	}
	s.grantMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	grants := s.grantView.Grants()
	s.grantMu.RUnlock()
	seen := make(map[rootproto.DutyID]struct{})
	out := make([]rootproto.DutyID, 0, len(grants))
	for _, grant := range grants {
		if strings.TrimSpace(grant.HolderID) != holderID {
			continue
		}
		for _, duty := range grant.Duties {
			if duty.Scope.Kind != rootproto.DutyScopeGlobal {
				continue
			}
			if _, ok := seen[duty.DutyID]; ok {
				continue
			}
			seen[duty.DutyID] = struct{}{}
			out = append(out, duty.DutyID)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return rootproto.DutyName(out[i]) < rootproto.DutyName(out[j])
	})
	return out
}

func preferredDutyHolder(duty rootproto.DutyID, candidates []string) string {
	normalized := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		normalized = append(normalized, candidate)
	}
	if len(normalized) == 0 {
		return ""
	}
	sort.Strings(normalized)
	var (
		bestHolder string
		bestScore  uint64
	)
	for _, candidate := range normalized {
		h := fnv.New64a()
		// The first scoped-duty release only enables the global scope. Keep a
		// cluster salt in the rendezvous key so the default three-duty compose
		// deployment spreads alloc_id, tso, and region_lookup instead of letting
		// FNV's short duty strings cluster on one coordinator.
		_, _ = h.Write([]byte("cluster"))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(rootproto.DutyName(duty)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(candidate))
		score := h.Sum64()
		if bestHolder == "" || score > bestScore {
			bestHolder = candidate
			bestScore = score
		}
	}
	return bestHolder
}

func snapshotHasLocalGrant(snapshot rootview.Snapshot, holderID string) bool {
	for _, grant := range snapshot.ActiveGrants {
		if strings.TrimSpace(grant.HolderID) == holderID && grant.Present() {
			return true
		}
	}
	return false
}

func (s *Service) activeOtherGrantError(duty rootproto.DutyID, holderID string, nowUnixNano int64) error {
	current := s.currentGrant(duty)
	currentHolder := strings.TrimSpace(current.HolderID)
	localHolder := strings.TrimSpace(holderID)
	if currentHolder == "" || currentHolder == localHolder || !current.ActiveAt(nowUnixNano) || s.currentGrantRetiredAtFloor(current) {
		return nil
	}
	// A live rooted holder is the authority. Standby coordinators must not
	// campaign over it just because their local grant loop or a client request
	// arrived; clients should fail over to the current holder until expiry.
	return fmt.Errorf("%w: rooted holder=%s local_holder=%s expires_unix_nano=%d", rootstate.ErrPrimacy, currentHolder, localHolder, current.ExpiresUnixNano)
}

func (s *Service) coordinatorGrantStillValid(duty rootproto.DutyID, holderID string, nowUnixNano int64, renewIn, clockSkew time.Duration) bool {
	if s == nil {
		return false
	}
	current := s.currentGrant(duty)
	if strings.TrimSpace(holderID) == "" ||
		strings.TrimSpace(current.HolderID) != strings.TrimSpace(holderID) ||
		!current.ActiveAt(nowUnixNano) ||
		s.currentGrantRetiredAtFloor(current) {
		return false
	}
	if current.ExpiresUnixNano <= nowUnixNano+renewIn.Nanoseconds() ||
		current.ExpiresUnixNano <= nowUnixNano+clockSkew.Nanoseconds() {
		return false
	}
	if !s.currentGrantCertificateValid(current) {
		return false
	}
	return !s.coordinatorGrantNeedsRenewal(current)
}

func (s *Service) currentGrantRetiredAtFloor(grant rootproto.AuthorityGrant) bool {
	if s == nil {
		return false
	}
	s.grantMu.RLock()
	retiredFloor := s.grantView.retiredEraFloor
	s.grantMu.RUnlock()
	return authorityGrantRetiredAtFloor(grant, retiredFloor)
}

func (s *Service) currentGrantCertificateValid(grant rootproto.AuthorityGrant) bool {
	if s == nil || !grant.Present() {
		return false
	}
	s.grantMu.RLock()
	cert := s.grantView.CertificateFor(grant)
	s.grantMu.RUnlock()
	return grantCertificateMatches(cert, grant)
}

func (s *Service) refreshCurrentGrantCertificateIfNeeded(ctx context.Context, duty rootproto.DutyID, holderID string, nowUnixNano int64) error {
	current := s.currentGrant(duty)
	if !current.Present() ||
		strings.TrimSpace(current.HolderID) != strings.TrimSpace(holderID) ||
		!current.ActiveAt(nowUnixNano) ||
		s.currentGrantCertificateValid(current) {
		return nil
	}
	protocolState, cert, err := s.storage.ApplyGrant(ctx, rootproto.GrantCommand{
		Kind:            rootproto.GrantActIssue,
		HolderID:        holderID,
		GrantID:         current.GrantID,
		ExpiresUnixNano: current.ExpiresUnixNano,
		NowUnixNano:     nowUnixNano,
		RequestedDuties: append([]rootproto.DutyGrant(nil), current.Duties...),
	})
	if err != nil {
		return err
	}
	s.publishEunomiaState(protocolState)
	s.cacheGrantCertificate(cert)
	return nil
}

func (s *Service) coordinatorGrantNeedsRenewal(grant rootproto.AuthorityGrant) bool {
	if s == nil || !grant.Present() {
		return true
	}
	if lookup, ok := grant.Duty(rootproto.DutyRegionLookup); ok &&
		(lookup.Bound.Kind != rootproto.DutyBoundVersion || lookup.Bound.DescriptorRevisionCeiling < s.currentDescriptorRevision()) {
		return true
	}
	s.allocMu.Lock()
	idCurrent := s.ids.Current()
	tsoCurrent := s.tso.Current()
	s.allocMu.Unlock()
	if idDuty, ok := grant.Duty(rootproto.DutyAllocID); ok &&
		(idDuty.Bound.Kind != rootproto.DutyBoundMonotone || idDuty.Bound.MonotoneUpper <= idCurrent) {
		return true
	}
	if tsoDuty, ok := grant.Duty(rootproto.DutyTSO); ok &&
		(tsoDuty.Bound.Kind != rootproto.DutyBoundMonotone || tsoDuty.Bound.MonotoneUpper <= tsoCurrent) {
		return true
	}
	return false
}

func (s *Service) coordinatorGrantLoopInterval() time.Duration {
	if s == nil {
		return time.Second
	}
	s.grantMu.RLock()
	defer s.grantMu.RUnlock()
	interval := s.grantRenewIn / 2
	if interval <= 0 {
		interval = time.Second
	}
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	return interval
}

func (s *Service) coordinatorGrantRetryDelay(failures int) time.Duration {
	if failures <= 0 {
		return s.jitterDuration(s.coordinatorGrantLoopInterval(), 20)
	}
	delay := defaultGrantRetryMin
	for i := 1; i < failures; i++ {
		if delay >= maxGrantRetry/2 {
			delay = maxGrantRetry
			break
		}
		delay *= 2
	}
	if delay > maxGrantRetry {
		delay = maxGrantRetry
	}
	return s.jitterDuration(delay, 20)
}

func (s *Service) jitterDuration(base time.Duration, percent int64) time.Duration {
	if base <= 0 || percent <= 0 {
		return base
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	window := percent*2 + 1
	offsetPercent := (nowFn().UnixNano() % window) - percent
	jittered := base + time.Duration(int64(base)*offsetPercent/100)
	if jittered < 10*time.Millisecond {
		return 10 * time.Millisecond
	}
	return jittered
}

func (s *Service) coordinatorGrantEnabled() bool {
	if s == nil {
		return false
	}
	s.grantMu.RLock()
	defer s.grantMu.RUnlock()
	return s.coordinatorID != "" && s.grantTTL > 0
}

func (s *Service) currentGrant(duty rootproto.DutyID) rootproto.AuthorityGrant {
	if s == nil {
		return rootproto.AuthorityGrant{}
	}
	s.grantMu.RLock()
	defer s.grantMu.RUnlock()
	grant, _ := s.grantView.GrantFor(duty, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	return grant
}

func (s *Service) grantCampaignBounds() (nowUnixNano, expiresUnixNano int64, holderID string, renewIn, clockSkew time.Duration) {
	if s == nil {
		return 0, 0, "", 0, 0
	}
	s.grantMu.RLock()
	defer s.grantMu.RUnlock()
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()
	return now.UnixNano(), now.Add(s.grantTTL).UnixNano(), s.coordinatorID, s.grantRenewIn, s.grantClockSkew
}

func (s *Service) nowUnixNano() int64 {
	nowFn := time.Now
	if s != nil && s.now != nil {
		nowFn = s.now
	}
	return nowFn().UnixNano()
}

func translateGrantError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return statusContext(err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return statusContext(err)
	}
	if errors.Is(err, rootstate.ErrPrimacy) || errors.Is(err, rootstate.ErrInheritance) {
		return statusGrant(err)
	}
	return statusInternalf("campaign coordinator grant: %v", err)
}
