package server

import (
	"context"
	"errors"
	"strings"
	"time"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
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
	if err := s.ensureCoordinatorLease(ctx); err != nil {
		return nil
	}
	return s.planStoreOperations(storeID)
}

func (s *Service) requireDutyAdmission(ctx context.Context, dutyMask uint32) error {
	if s == nil || !s.coordinatorLeaseEnabled() {
		return nil
	}
	if err := s.ensureCoordinatorLease(ctx); err != nil {
		return translateCoordinatorLeaseError(err)
	}
	return s.preActionGate(preActionDutyAdmission, dutyMask)
}

// RunCoordinatorLeaseLoop keeps the local coordinator lease renewed while ctx
// remains alive. The loop is explicit so callers can decide lifecycle and avoid
// hidden background goroutines in constructors.
func (s *Service) RunCoordinatorLeaseLoop(ctx context.Context) {
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
				releaseCtx, cancel := context.WithTimeout(context.Background(), defaultCoordinatorLeaseReleaseTimeout)
				_ = s.releaseCoordinatorLease(releaseCtx)
				cancel()
			}
			return
		case <-timer.C:
			next := s.coordinatorLeaseLoopInterval()
			if s.storage.IsLeader() {
				if err := s.ensureCoordinatorLease(ctx); err != nil {
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

// ReleaseCoordinatorLease explicitly releases the current rooted coordinator
// lease for the configured holder. It is intended for graceful shutdown.
func (s *Service) ReleaseCoordinatorLease() error {
	return s.releaseCoordinatorLease(context.Background())
}

func (s *Service) releaseCoordinatorLease(ctx context.Context) error {
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
	handoffFrontiers := controlplane.Frontiers(rootstate.State{
		IDFence:  s.currentIDFenceLocked(),
		TSOFence: s.currentTSOFenceLocked(),
	}, s.currentDescriptorRevision())
	s.allocMu.Unlock()

	if _, err := s.storage.ApplyCoordinatorLease(ctx, rootproto.CoordinatorLeaseCommand{
		Kind:             rootproto.CoordinatorLeaseCommandRelease,
		HolderID:         holderID,
		NowUnixNano:      nowUnixNano,
		HandoffFrontiers: handoffFrontiers,
	}); err != nil {
		return err
	}
	return s.reloadAndFenceAllocators(true)
}

// SealCoordinatorLease records one rooted closure point for the current
// authority generation using the frontiers already consumed by this service.
func (s *Service) SealCoordinatorLease() error {
	return s.sealCoordinatorLease(context.Background())
}

func (s *Service) sealCoordinatorLease(ctx context.Context) error {
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
	return s.applyClosureCommand(
		ctx,
		rootproto.CoordinatorClosureCommandSeal,
		preActionSealCurrentGeneration,
		controlplane.Frontiers(rootstate.State{
			IDFence:  consumedIDFrontier,
			TSOFence: consumedTSOFrontier,
		}, s.currentDescriptorRevision()),
	)
}

// ConfirmCoordinatorClosure explicitly records one rooted audit confirmation
// after a sealed generation has been covered by a successor authority instance.
func (s *Service) ConfirmCoordinatorClosure() error {
	return s.confirmCoordinatorClosure(context.Background())
}

func (s *Service) confirmCoordinatorClosure(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	return s.applyClosureCommand(ctx, rootproto.CoordinatorClosureCommandConfirm, preActionLifecycleMutation, rootproto.NewCoordinatorDutyFrontiers())
}

// CloseCoordinatorClosure explicitly records that the current successor
// generation has been explicitly closed after rooted closure confirmation.
func (s *Service) CloseCoordinatorClosure() error {
	return s.closeCoordinatorClosure(context.Background())
}

func (s *Service) closeCoordinatorClosure(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	return s.applyClosureCommand(ctx, rootproto.CoordinatorClosureCommandClose, preActionLifecycleMutation, rootproto.NewCoordinatorDutyFrontiers())
}

// ReattachCoordinatorClosure explicitly records that the current successor
// generation has been reattached after rooted close has already landed.
func (s *Service) ReattachCoordinatorClosure() error {
	return s.reattachCoordinatorClosure(context.Background())
}

func (s *Service) reattachCoordinatorClosure(ctx context.Context) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	if s.ablation.DisableReattach {
		return nil
	}
	if !s.storage.IsLeader() {
		return nil
	}
	return s.applyClosureCommand(ctx, rootproto.CoordinatorClosureCommandReattach, preActionLifecycleMutation, rootproto.NewCoordinatorDutyFrontiers())
}

func (s *Service) applyClosureCommand(ctx context.Context, kind rootproto.CoordinatorClosureCommandKind, gate preActionKind, frontiers rootproto.CoordinatorDutyFrontiers) error {
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
	if err := s.preActionGate(gate, 0); err != nil {
		return err
	}
	if _, err := s.storage.ApplyCoordinatorClosure(ctx, rootproto.CoordinatorClosureCommand{
		Kind:        kind,
		HolderID:    holderID,
		NowUnixNano: nowUnixNano,
		Frontiers:   frontiers,
	}); err != nil {
		return err
	}
	return s.reloadAndFenceAllocators(true)
}

func (s *Service) ensureCoordinatorLease(ctx context.Context) error {
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
	handoffFrontiers := controlplane.Frontiers(rootstate.State{IDFence: s.currentIDFenceLocked(), TSOFence: s.currentTSOFenceLocked()}, s.currentDescriptorRevision())
	s.allocMu.Unlock()
	current, seal := s.currentCoordinatorLeaseView()
	predecessorDigest := rootstate.ResolveCoordinatorLeasePredecessorDigest(current, seal, holderID, nowUnixNano)

	if _, err := s.storage.ApplyCoordinatorLease(ctx, rootproto.CoordinatorLeaseCommand{
		Kind:              rootproto.CoordinatorLeaseCommandIssue,
		HolderID:          holderID,
		ExpiresUnixNano:   expiresUnixNano,
		NowUnixNano:       nowUnixNano,
		PredecessorDigest: predecessorDigest,
		HandoffFrontiers:  handoffFrontiers,
	}); err != nil {
		return err
	}
	return s.reloadAndFenceAllocators(true)
}

func (s *Service) coordinatorLeaseStillValid(holderID string, nowUnixNano int64, renewIn, clockSkew time.Duration) bool {
	if s == nil {
		return false
	}
	current, seal := s.currentCoordinatorLeaseView()
	if !rootstate.CoordinatorLeaseContinuable(current, seal, holderID, nowUnixNano) {
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
	delay := defaultCoordinatorLeaseRetryMin
	for i := 1; i < failures; i++ {
		if delay >= maxCoordinatorLeaseRetry/2 {
			delay = maxCoordinatorLeaseRetry
			break
		}
		delay *= 2
	}
	if delay > maxCoordinatorLeaseRetry {
		delay = maxCoordinatorLeaseRetry
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

func (s *Service) currentCoordinatorLease() rootstate.CoordinatorLease {
	if s == nil {
		return rootstate.CoordinatorLease{}
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	return s.leaseView.Lease()
}

func (s *Service) currentCoordinatorLeaseView() (rootstate.CoordinatorLease, rootstate.CoordinatorSeal) {
	if s == nil {
		return rootstate.CoordinatorLease{}, rootstate.CoordinatorSeal{}
	}
	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	return s.leaseView.Current()
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

func translateCoordinatorLeaseError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, rootstate.ErrCoordinatorLeaseHeld) || errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage) || errors.Is(err, rootstate.ErrCoordinatorLeaseLineage) {
		return statusCoordinatorLease(err)
	}
	return status.Error(codes.Internal, "campaign coordinator lease: "+err.Error())
}
