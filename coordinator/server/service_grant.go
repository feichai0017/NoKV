package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	coordfailpoints "github.com/feichai0017/NoKV/coordinator/failpoints"
	"github.com/feichai0017/NoKV/coordinator/scheduling"
	rootfailpoints "github.com/feichai0017/NoKV/meta/root/failpoints"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) requireLeaderForWrite() error {
	if s == nil || s.storage == nil {
		return nil
	}
	if s.storage.IsLeader() {
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
		return s.storeControlOperations(storeID)
	}
	if s.storage != nil && !s.storage.IsLeader() {
		return nil
	}
	if err := s.ensureGrant(ctx); err != nil {
		return nil
	}
	return s.storeControlOperations(storeID)
}

func (s *Service) storeControlOperations(storeID uint64) []*coordpb.SchedulerOperation {
	if s == nil || s.cluster == nil || storeID == 0 {
		return nil
	}
	return scheduling.PlanStoreOperations(storeID, s.cluster.Snapshot())
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
			if s.storage.IsLeader() {
				releaseCtx, cancel := context.WithTimeout(context.Background(), defaultGrantReleaseTimeout)
				_ = s.DrainAndSealGrant(releaseCtx)
				cancel()
			}
			return
		case <-timer.C:
			next := s.coordinatorGrantLoopInterval()
			if s.storage.IsLeader() {
				if err := s.ensureGrant(ctx); err != nil {
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
	if !s.storage.IsLeader() {
		return nil
	}
	if s.localGrantAlreadySealed() {
		s.markAuthoritySealed()
		return nil
	}
	s.authorityMu.Lock()
	if s.authorityState == authoritySealed {
		s.authorityMu.Unlock()
		return nil
	}
	s.authorityState = authorityDraining
	s.authorityMu.Unlock()

	if err := s.waitAuthorityInflightDrained(ctx); err != nil {
		s.markAuthorityServing()
		return err
	}
	if s.localGrantAlreadySealed() {
		s.markAuthoritySealed()
		return nil
	}
	if err := s.sealGrant(ctx); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			_ = s.reloadAndFenceAllocators(true)
		}
		if s.localGrantAlreadySealed() {
			s.markAuthoritySealed()
			return nil
		}
		s.markAuthorityServing()
		return err
	}
	s.markAuthoritySealed()
	return nil
}

func (s *Service) waitAuthorityInflightDrained(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, inflight := s.authorityServingSnapshot()
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
	grant := s.grantView.Grant()
	retirements := s.grantView.Retirements()
	s.grantMu.RUnlock()
	if holderID == "" {
		return false
	}
	if !grant.Present() {
		for _, retirement := range retirements {
			if strings.TrimSpace(retirement.HolderID) == holderID && retirement.Present() {
				return true
			}
		}
		return false
	}
	if strings.TrimSpace(grant.HolderID) != holderID {
		return false
	}
	for _, retirement := range retirements {
		if retirement.GrantID == grant.GrantID && retirement.Present() {
			return true
		}
	}
	return false
}

func (s *Service) markAuthorityServing() {
	if s == nil {
		return
	}
	s.authorityMu.Lock()
	if s.authorityState != authoritySealed {
		s.authorityState = authorityServing
	}
	s.authorityMu.Unlock()
}

func (s *Service) markAuthoritySealed() {
	if s == nil {
		return
	}
	s.authorityMu.Lock()
	s.authorityState = authoritySealed
	s.authorityMu.Unlock()
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
	if !s.storage.IsLeader() {
		return nil
	}
	s.allocMu.Lock()
	consumedIDFrontier := s.ids.Current()
	consumedTSOFrontier := s.tso.Current()
	s.allocMu.Unlock()
	if err := rootfailpoints.InjectBeforeGrantStorageRead(); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return status.Error(codes.Internal, "load rooted snapshot: "+err.Error())
	}
	s.refreshCurrentRootSnapshot(snapshot)
	grant := snapshot.ActiveGrant
	if !grant.Present() {
		return nil
	}
	s.grantMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.grantMu.RUnlock()
	if holderID == "" || grant.HolderID != holderID {
		return statusGrant(fmt.Errorf("%w: rooted holder=%s local_holder=%s", rootstate.ErrPrimacy, grant.HolderID, holderID))
	}
	protocolState, _, err := s.storage.ApplyGrant(ctx, rootproto.GrantCommand{
		Kind:        rootproto.GrantActSeal,
		HolderID:    holderID,
		GrantID:     grant.GrantID,
		NowUnixNano: s.nowUnixNano(),
		ExactUsages: []rootproto.AuthorityUsage{
			{DutyID: rootproto.DutyAllocID, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: consumedIDFrontier}},
			{DutyID: rootproto.DutyTSO, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: consumedTSOFrontier}},
			{DutyID: rootproto.DutyRegionLookup, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundVersion, DescriptorRevisionCeiling: s.currentDescriptorRevision()}},
		},
	})
	if err != nil {
		s.eunomiaMetrics.recordGuaranteeViolationForError(err)
		return err
	}
	s.publishEunomiaState(protocolState)
	if err := coordfailpoints.InjectAfterSealGrantBeforeReload(); err != nil {
		return err
	}
	return s.reloadAndFenceAllocators(true)
}

func (s *Service) InheritRetiredGrants(ctx context.Context) error {
	if s == nil || !s.coordinatorGrantEnabled() || s.storage == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.storage.IsLeader() {
		return nil
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return err
	}
	s.refreshCurrentRootSnapshot(snapshot)
	s.grantMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.grantMu.RUnlock()
	if holderID == "" || strings.TrimSpace(snapshot.ActiveGrant.HolderID) != holderID || !snapshot.ActiveGrant.Present() {
		return nil
	}
	pending := make([]string, 0, len(snapshot.RetiredGrants))
	for _, retirement := range snapshot.RetiredGrants {
		if retirement.InheritedByGrantID == "" && retirement.GrantID != "" {
			pending = append(pending, retirement.GrantID)
		}
	}
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
	return s.reloadAndFenceAllocators(true)
}

func (s *Service) grantInheritanceCandidate() bool {
	if s == nil || !s.coordinatorGrantEnabled() || s.storage == nil {
		return false
	}
	grant := s.currentGrant()
	return grant.Present()
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

func (s *Service) ensureGrant(ctx context.Context) error {
	if s == nil || !s.coordinatorGrantEnabled() || s.storage == nil {
		return nil
	}
	// Fast path: avoid serializing read traffic behind the campaign lock while
	// the current grant is still outside the renew and clock-skew windows.
	nowUnixNano, _, holderID, renewIn, clockSkew := s.grantCampaignBounds()
	if s.coordinatorGrantStillValid(holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}
	if err := s.activeOtherGrantError(holderID, nowUnixNano); err != nil {
		return err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// Another request or the background renew loop may have refreshed grant
	// while this caller waited for writeMu.
	nowUnixNano, _, holderID, renewIn, clockSkew = s.grantCampaignBounds()
	if s.coordinatorGrantStillValid(holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}
	if err := s.activeOtherGrantError(holderID, nowUnixNano); err != nil {
		return err
	}

	s.allocMu.Lock()
	consumedIDFrontier := s.ids.Current()
	consumedTSOFrontier := s.tso.Current()
	idUpper, _ := addUint64(s.currentIDFenceLocked(), s.effectiveIDWindowSize())
	tsoUpper, _ := addUint64(s.currentTSOFenceLocked(), s.effectiveTSOWindowSize())
	s.allocMu.Unlock()
	descriptorRevision := s.currentDescriptorRevision()
	// Recompute time and expiry after sampling allocator fences so the grant
	// command carries fresh bounds and does not campaign unnecessarily.
	nowUnixNano, expiresUnixNano, holderID, renewIn, clockSkew := s.grantCampaignBounds()
	if s.coordinatorGrantStillValid(holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}
	currentEra := s.currentGrant().Era
	protocolState, _, err := s.storage.ApplyGrant(ctx, rootproto.GrantCommand{
		Kind:            rootproto.GrantActIssue,
		HolderID:        holderID,
		ExpiresUnixNano: expiresUnixNano,
		NowUnixNano:     nowUnixNano,
		RequestedDuties: []rootproto.DutyGrant{
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyAllocID, idUpper),
			rootproto.NewGlobalMonotoneDuty(rootproto.DutyTSO, tsoUpper),
			rootproto.NewGlobalVersionDuty(rootproto.DutyRegionLookup, rootproto.AuthorityRootToken{}, descriptorRevision, 0),
		},
		ExactUsages: []rootproto.AuthorityUsage{
			{DutyID: rootproto.DutyAllocID, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: consumedIDFrontier}},
			{DutyID: rootproto.DutyTSO, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundMonotone, MonotoneUpper: consumedTSOFrontier}},
			{DutyID: rootproto.DutyRegionLookup, Scope: rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}, Usage: rootproto.DutyBound{Kind: rootproto.DutyBoundVersion, DescriptorRevisionCeiling: descriptorRevision}},
		},
	})
	if err != nil {
		s.eunomiaMetrics.recordGuaranteeViolationForError(err)
		return err
	}
	s.publishEunomiaState(protocolState)
	s.eunomiaMetrics.recordGrantEraTransition(currentEra, protocolState.ActiveGrant.Era)
	return s.reloadAndFenceAllocators(true)
}

func (s *Service) activeOtherGrantError(holderID string, nowUnixNano int64) error {
	current := s.currentGrant()
	currentHolder := strings.TrimSpace(current.HolderID)
	localHolder := strings.TrimSpace(holderID)
	if currentHolder == "" || currentHolder == localHolder || !current.ActiveAt(nowUnixNano) {
		return nil
	}
	// A live rooted holder is the authority. Standby coordinators must not
	// campaign over it just because their local grant loop or a client request
	// arrived; clients should fail over to the current holder until expiry.
	return fmt.Errorf("%w: rooted holder=%s local_holder=%s expires_unix_nano=%d", rootstate.ErrPrimacy, currentHolder, localHolder, current.ExpiresUnixNano)
}

func (s *Service) coordinatorGrantStillValid(holderID string, nowUnixNano int64, renewIn, clockSkew time.Duration) bool {
	if s == nil {
		return false
	}
	current := s.currentGrant()
	if strings.TrimSpace(holderID) == "" ||
		strings.TrimSpace(current.HolderID) != strings.TrimSpace(holderID) ||
		!current.ActiveAt(nowUnixNano) {
		return false
	}
	if current.ExpiresUnixNano <= nowUnixNano+renewIn.Nanoseconds() ||
		current.ExpiresUnixNano <= nowUnixNano+clockSkew.Nanoseconds() {
		return false
	}
	return !s.coordinatorGrantNeedsRenewal(current)
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

func (s *Service) currentGrant() rootproto.AuthorityGrant {
	if s == nil {
		return rootproto.AuthorityGrant{}
	}
	s.grantMu.RLock()
	defer s.grantMu.RUnlock()
	return s.grantView.Grant()
}

func (s *Service) observedRetiredEraFloor() uint64 {
	if s == nil {
		return 0
	}
	s.grantMu.RLock()
	retirements := s.grantView.Retirements()
	s.grantMu.RUnlock()
	var floor uint64
	for _, retirement := range retirements {
		if retirement.Era > floor {
			floor = retirement.Era
		}
	}
	return floor
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
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if errors.Is(err, rootstate.ErrPrimacy) || errors.Is(err, rootstate.ErrInheritance) {
		return statusGrant(err)
	}
	return status.Error(codes.Internal, "campaign coordinator grant: "+err.Error())
}
