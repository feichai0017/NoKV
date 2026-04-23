package server

import (
	"context"
	"errors"
	"strings"
	"time"

	coordfailpoints "github.com/feichai0017/NoKV/coordinator/failpoints"
	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
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

func (s *Service) leaseScopedStoreOperations(ctx context.Context, storeID uint64) []*coordpb.SchedulerOperation {
	if s == nil || !s.coordinatorLeaseEnabled() {
		return s.planStoreOperations(storeID)
	}
	if s.storage != nil && !s.storage.IsLeader() {
		return nil
	}
	if err := s.ensureTenure(ctx); err != nil {
		return nil
	}
	return s.planStoreOperations(storeID)
}

func (s *Service) requireDutyAdmission(ctx context.Context, mandate uint32) error {
	if s == nil || !s.coordinatorLeaseEnabled() {
		return nil
	}
	if err := s.ensureTenure(ctx); err != nil {
		return translateTenureError(err)
	}
	return s.successionGate(gateMandateAdmission, mandate)
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
				_ = s.releaseTenure(releaseCtx)
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
	inheritedFrontiers := succession.Frontiers(rootstate.State{
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
		s.successionMetrics.recordGuaranteeViolationForError(err)
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
	if s.ablation.DisableSeal {
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
		succession.Frontiers(rootstate.State{
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
	if s.ablation.DisableReattach {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	return s.applyHandoverCommand(ctx, rootproto.HandoverActReattach, gateHandoverMutation, rootproto.NewMandateFrontiers())
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
	if err := s.successionGate(gate, 0); err != nil {
		return err
	}
	protocolState, err := s.storage.ApplyHandover(ctx, rootproto.HandoverCommand{
		Kind:        kind,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
		Frontiers:   frontiers,
	})
	if err != nil {
		s.successionMetrics.recordGuaranteeViolationForError(err)
		return err
	}
	if err := coordfailpoints.InjectAfterApplyHandoverBeforeReload(); err != nil {
		return err
	}
	s.successionMetrics.recordHandoverStageTransition(beforeStage, protocolState.Handover.Stage)
	return s.reloadAndFenceAllocators(true)
}

func (s *Service) ensureTenure(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	nowUnixNano, expiresUnixNano, holderID, renewIn, clockSkew := s.leaseCampaignBounds()
	if s.coordinatorLeaseStillValid(holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.coordinatorLeaseStillValid(holderID, nowUnixNano, renewIn, clockSkew) {
		return nil
	}

	s.allocMu.Lock()
	inheritedFrontiers := succession.Frontiers(rootstate.State{IDFence: s.currentIDFenceLocked(), TSOFence: s.currentTSOFenceLocked()}, s.currentDescriptorRevision())
	s.allocMu.Unlock()
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
		s.successionMetrics.recordGuaranteeViolationForError(err)
		return err
	}
	s.successionMetrics.recordTenureEraTransition(current.Era, protocolState.Tenure.Era)
	return s.reloadAndFenceAllocators(true)
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
	if errors.Is(err, rootstate.ErrPrimacy) || errors.Is(err, rootstate.ErrInheritance) || errors.Is(err, rootstate.ErrInheritance) {
		return statusTenure(err)
	}
	return status.Error(codes.Internal, "campaign coordinator lease: "+err.Error())
}
