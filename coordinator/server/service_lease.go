package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	coordfailpoints "github.com/feichai0017/NoKV/coordinator/failpoints"
	"github.com/feichai0017/NoKV/coordinator/scheduling"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	eunomia "github.com/feichai0017/NoKV/meta/root/protocol/eunomia"
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

func (s *Service) leaseScopedStoreOperations(ctx context.Context, storeID uint64) []*coordpb.SchedulerOperation {
	if s == nil || !s.coordinatorLeaseEnabled() {
		return s.storeControlOperations(storeID)
	}
	if s.storage != nil && !s.storage.IsLeader() {
		return nil
	}
	if err := s.ensureTenure(ctx); err != nil {
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

func (s *Service) requireDutyAdmission(ctx context.Context, mandate uint32) error {
	done, err := s.beginDutyAdmission(ctx, mandate)
	if err != nil {
		return err
	}
	done()
	return nil
}

// RunTenureLoop keeps the local coordinator lease renewed while ctx
// remains alive. The loop is explicit so callers can decide lifecycle and avoid
// hidden background goroutines in constructors.
func (s *Service) RunTenureLoop(ctx context.Context) {
	if s == nil || ctx == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			if s.storage.IsLeader() {
				releaseCtx, cancel := context.WithTimeout(context.Background(), defaultTenureReleaseTimeout)
				_ = s.DrainAndSealTenure(releaseCtx)
				cancel()
			}
			return
		case <-timer.C:
			next := s.coordinatorLeaseLoopInterval()
			if s.storage.IsLeader() {
				if err := s.ensureTenure(ctx); err != nil {
					failures++
					next = s.coordinatorLeaseRetryDelay(failures)
				} else {
					_ = s.FinalizeHandover(ctx)
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

// ReleaseTenure explicitly releases the current rooted coordinator
// lease for the configured holder. It is intended for graceful shutdown.
func (s *Service) ReleaseTenure() error {
	return s.releaseTenure(context.Background())
}

// DrainAndSealTenure stops admitting new authority-bearing requests, waits for
// requests already in service, then records one rooted legacy seal.
func (s *Service) DrainAndSealTenure(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.storage.IsLeader() {
		return nil
	}
	if s.localTenureAlreadySealed() {
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
	if s.localTenureAlreadySealed() {
		s.markAuthoritySealed()
		return nil
	}
	if err := s.sealTenure(ctx); err != nil {
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

func (s *Service) localTenureAlreadySealed() bool {
	current, seal := s.currentTenureView()
	if !rootstate.TenureSealed(current, seal) {
		return false
	}
	s.leaseMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.leaseMu.RUnlock()
	return holderID != "" && strings.TrimSpace(current.HolderID) == holderID
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

func (s *Service) releaseTenure(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	nowUnixNano := nowFn().UnixNano()

	s.leaseMu.RLock()
	holderID := s.coordinatorID
	s.leaseMu.RUnlock()
	if strings.TrimSpace(holderID) == "" {
		return nil
	}

	s.allocMu.Lock()
	inheritedFrontiers := eunomia.Frontiers(rootstate.State{
		IDFence:  s.currentIDFenceLocked(),
		TSOFence: s.currentTSOFenceLocked(),
	}, s.currentDescriptorRevision())
	s.allocMu.Unlock()

	if _, err := s.storage.ApplyTenure(ctx, rootproto.TenureCommand{
		Kind:               rootproto.TenureActRelease,
		HolderID:           holderID,
		NowUnixNano:        nowUnixNano,
		InheritedFrontiers: inheritedFrontiers,
	}); err != nil {
		s.eunomiaMetrics.recordGuaranteeViolationForError(err)
		return err
	}
	return s.reloadAndFenceAllocators(true)
}

// SealTenure records one rooted legacy point for the current
// authority era using the frontiers already consumed by this service.
func (s *Service) SealTenure() error {
	return s.sealTenure(context.Background())
}

func (s *Service) sealTenure(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	s.allocMu.Lock()
	consumedIDFrontier := s.ids.Current()
	consumedTSOFrontier := s.tso.Current()
	s.allocMu.Unlock()
	return s.applyHandoverCommand(
		ctx,
		rootproto.HandoverActSeal,
		gateLegacyFormation,
		eunomia.Frontiers(rootstate.State{
			IDFence:  consumedIDFrontier,
			TSOFence: consumedTSOFrontier,
		}, s.currentDescriptorRevision()),
	)
}

// ConfirmHandover explicitly records one rooted audit confirmation
// after a sealed era has been covered by a successor authority instance.
func (s *Service) ConfirmHandover() error {
	return s.confirmHandover(context.Background())
}

func (s *Service) confirmHandover(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	return s.applyHandoverCommand(ctx, rootproto.HandoverActConfirm, gateHandoverMutation, rootproto.NewMandateFrontiers())
}

// CloseHandover explicitly records that the current successor
// era has been explicitly finalized after rooted handover confirmation.
func (s *Service) CloseHandover() error {
	return s.closeHandover(context.Background())
}

func (s *Service) closeHandover(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	return s.applyHandoverCommand(ctx, rootproto.HandoverActClose, gateHandoverMutation, rootproto.NewMandateFrontiers())
}

// ReattachHandover explicitly records that the current successor
// era has been reattached after rooted finality has already landed.
func (s *Service) ReattachHandover() error {
	return s.reattachHandover(context.Background())
}

func (s *Service) reattachHandover(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	return s.applyHandoverCommand(ctx, rootproto.HandoverActReattach, gateHandoverMutation, rootproto.NewMandateFrontiers())
}

// FinalizeHandover advances a successor handoff through the rooted finality
// stages once the inherited frontier coverage is already visible in root.
func (s *Service) FinalizeHandover(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
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
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	nowUnixNano := nowFn().UnixNano()
	s.leaseMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.leaseMu.RUnlock()
	if holderID == "" ||
		strings.TrimSpace(snapshot.Tenure.HolderID) != holderID ||
		!snapshot.Tenure.ActiveAt(nowUnixNano) ||
		!snapshot.Legacy.Present() ||
		snapshot.Legacy.Era >= snapshot.Tenure.Era {
		return nil
	}
	if !rootproto.HandoverStageAtLeast(snapshot.Handover.Stage, rootproto.HandoverStageConfirmed) {
		if err := s.confirmHandover(ctx); err != nil {
			if handoverFinalizerIgnorable(err) {
				return nil
			}
			return err
		}
	}
	snapshot, err = s.storage.Load()
	if err != nil {
		return err
	}
	s.refreshCurrentRootSnapshot(snapshot)
	if !rootproto.HandoverStageAtLeast(snapshot.Handover.Stage, rootproto.HandoverStageClosed) {
		if err := s.closeHandover(ctx); err != nil {
			if handoverFinalizerIgnorable(err) {
				return nil
			}
			return err
		}
	}
	snapshot, err = s.storage.Load()
	if err != nil {
		return err
	}
	s.refreshCurrentRootSnapshot(snapshot)
	if !rootproto.HandoverStageAtLeast(snapshot.Handover.Stage, rootproto.HandoverStageReattached) {
		if err := s.reattachHandover(ctx); err != nil {
			if handoverFinalizerIgnorable(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (s *Service) handoverFinalizerCandidate() bool {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return false
	}
	current, legacy := s.currentTenureView()
	return legacy.Present() && current.Era > legacy.Era
}

func handoverFinalizerIgnorable(err error) bool {
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

func (s *Service) applyHandoverCommand(ctx context.Context, kind rootproto.HandoverAct, gate gateKind, frontiers rootproto.MandateFrontiers) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	nowUnixNano := nowFn().UnixNano()

	s.leaseMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.leaseMu.RUnlock()
	if holderID == "" {
		return nil
	}
	beforeStage := s.currentHandover().Stage
	if err := s.eunomiaGate(gate, 0); err != nil {
		return err
	}
	protocolState, err := s.storage.ApplyHandover(ctx, rootproto.HandoverCommand{
		Kind:        kind,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
		Frontiers:   frontiers,
	})
	if err != nil {
		s.eunomiaMetrics.recordGuaranteeViolationForError(err)
		return err
	}
	if err := coordfailpoints.InjectAfterApplyHandoverBeforeReload(); err != nil {
		return err
	}
	s.eunomiaMetrics.recordHandoverStageTransition(beforeStage, protocolState.Handover.Stage)
	return s.reloadAndFenceAllocators(true)
}

func (s *Service) ensureTenure(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	// Fast path: avoid serializing read traffic behind the campaign lock while
	// the current tenure is still outside the renew and clock-skew windows.
	nowUnixNano, _, holderID, renewIn, clockSkew := s.leaseCampaignBounds()
	if s.coordinatorLeaseStillValid(holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}
	if err := s.activeOtherTenureError(holderID, nowUnixNano); err != nil {
		return err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// Another request or the background renew loop may have refreshed tenure
	// while this caller waited for writeMu.
	nowUnixNano, _, holderID, renewIn, clockSkew = s.leaseCampaignBounds()
	if s.coordinatorLeaseStillValid(holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}
	if err := s.activeOtherTenureError(holderID, nowUnixNano); err != nil {
		return err
	}

	s.allocMu.Lock()
	inheritedFrontiers := eunomia.Frontiers(rootstate.State{IDFence: s.currentIDFenceLocked(), TSOFence: s.currentTSOFenceLocked()}, s.currentDescriptorRevision())
	s.allocMu.Unlock()
	// Recompute time and expiry after sampling allocator fences so the tenure
	// command carries fresh bounds and does not campaign unnecessarily.
	nowUnixNano, expiresUnixNano, holderID, renewIn, clockSkew := s.leaseCampaignBounds()
	if s.coordinatorLeaseStillValid(holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}
	current, seal := s.currentTenureView()
	lineageDigest := rootstate.ResolveLineageDigest(current, seal, holderID, nowUnixNano)

	protocolState, err := s.storage.ApplyTenure(ctx, rootproto.TenureCommand{
		Kind:               rootproto.TenureActIssue,
		HolderID:           holderID,
		ExpiresUnixNano:    expiresUnixNano,
		NowUnixNano:        nowUnixNano,
		LineageDigest:      lineageDigest,
		InheritedFrontiers: inheritedFrontiers,
	})
	if err != nil {
		s.eunomiaMetrics.recordGuaranteeViolationForError(err)
		return err
	}
	s.eunomiaMetrics.recordTenureEraTransition(current.Era, protocolState.Tenure.Era)
	return s.reloadAndFenceAllocators(true)
}

func (s *Service) activeOtherTenureError(holderID string, nowUnixNano int64) error {
	current, seal := s.currentTenureView()
	currentHolder := strings.TrimSpace(current.HolderID)
	localHolder := strings.TrimSpace(holderID)
	if currentHolder == "" || currentHolder == localHolder || !current.ActiveAt(nowUnixNano) {
		return nil
	}
	if rootstate.TenureSealed(current, seal) {
		return nil
	}
	// A live rooted holder is the authority. Standby coordinators must not
	// campaign over it just because their local tenure loop or a client request
	// arrived; clients should fail over to the current holder until expiry.
	return fmt.Errorf("%w: rooted holder=%s local_holder=%s expires_unix_nano=%d", rootstate.ErrPrimacy, currentHolder, localHolder, current.ExpiresUnixNano)
}

func (s *Service) coordinatorLeaseStillValid(holderID string, nowUnixNano int64, renewIn, clockSkew time.Duration) bool {
	if s == nil {
		return false
	}
	current, seal := s.currentTenureView()
	if !rootstate.TenureRenewable(current, seal, holderID, nowUnixNano) {
		return false
	}
	return current.ExpiresUnixNano > nowUnixNano+renewIn.Nanoseconds() &&
		current.ExpiresUnixNano > nowUnixNano+clockSkew.Nanoseconds()
}

func (s *Service) coordinatorLeaseLoopInterval() time.Duration {
	if s == nil {
		return time.Second
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	interval := s.leaseRenewIn / 2
	if interval <= 0 {
		interval = time.Second
	}
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	return interval
}

func (s *Service) coordinatorLeaseRetryDelay(failures int) time.Duration {
	if failures <= 0 {
		return s.jitterDuration(s.coordinatorLeaseLoopInterval(), 20)
	}
	delay := defaultTenureRetryMin
	for i := 1; i < failures; i++ {
		if delay >= maxTenureRetry/2 {
			delay = maxTenureRetry
			break
		}
		delay *= 2
	}
	if delay > maxTenureRetry {
		delay = maxTenureRetry
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

func (s *Service) coordinatorLeaseEnabled() bool {
	if s == nil {
		return false
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	return s.coordinatorID != "" && s.leaseTTL > 0
}

func (s *Service) currentTenure() rootstate.Tenure {
	if s == nil {
		return rootstate.Tenure{}
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	return s.leaseView.Tenure()
}

func (s *Service) currentTenureView() (rootstate.Tenure, rootstate.Legacy) {
	if s == nil {
		return rootstate.Tenure{}, rootstate.Legacy{}
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	return s.leaseView.Current()
}

func (s *Service) currentHandover() rootstate.Handover {
	if s == nil {
		return rootstate.Handover{}
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	return s.leaseView.Handover()
}

func (s *Service) leaseCampaignBounds() (nowUnixNano, expiresUnixNano int64, holderID string, renewIn, clockSkew time.Duration) {
	if s == nil {
		return 0, 0, "", 0, 0
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()
	return now.UnixNano(), now.Add(s.leaseTTL).UnixNano(), s.coordinatorID, s.leaseRenewIn, s.leaseClockSkew
}

func translateTenureError(err error) error {
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
		return statusTenure(err)
	}
	return status.Error(codes.Internal, "campaign coordinator lease: "+err.Error())
}
