package replicated

import (
	"strings"
	"sync"
	"time"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
)

const defaultRetainedRecords = 64

// Config wires one committed ordered log and checkpoint store into the
// replicated metadata-root backend. Replication and transport are supplied by
// the injected log implementation, not by this package.
type Config struct {
	Driver             Driver
	MaxRetainedRecords int
}

// Store hosts the rooted state machine on top of an injected committed log and
// checkpoint store. It is the future Delos-lite landing point for a replicated
// metadata backend, without baking protocol concerns into the root domain.
type Store struct {
	driver Driver

	mu                 sync.RWMutex
	state              rootstate.State
	descs              map[uint64]descriptor.Descriptor
	pending            map[uint64]rootstate.PendingPeerChange
	pendingRange       map[uint64]rootstate.PendingRangeChange
	records            []rootstorage.CommittedEvent
	retainFrom         rootstate.Cursor
	maxRetainedRecords int
}

func Open(cfg Config) (*Store, error) {
	if cfg.Driver == nil {
		return nil, errDriverRequired
	}
	if cfg.MaxRetainedRecords <= 0 {
		cfg.MaxRetainedRecords = defaultRetainedRecords
	}
	observed, err := rootstorage.ObserveCommitted(cfg.Driver, 0)
	if err != nil {
		return nil, err
	}
	bootstrap := rootmaterialize.BootstrapFromObserved(observed)
	return &Store{
		driver:             cfg.Driver,
		state:              bootstrap.Snapshot.State,
		descs:              bootstrap.Snapshot.Descriptors,
		pending:            bootstrap.Snapshot.PendingPeerChanges,
		pendingRange:       bootstrap.Snapshot.PendingRangeChanges,
		records:            bootstrap.Tail.Records,
		retainFrom:         bootstrap.RetainFrom,
		maxRetainedRecords: cfg.MaxRetainedRecords,
	}, nil
}

func (s *Store) IsLeader() bool {
	if s == nil || s.driver == nil {
		return true
	}
	return s.driver.IsLeader()
}

func (s *Store) LeaderID() uint64 {
	if s == nil || s.driver == nil {
		return 0
	}
	return s.driver.LeaderID()
}

func (s *Store) Campaign() error {
	if s == nil || s.driver == nil {
		return nil
	}
	return s.driver.Campaign()
}

// Refresh reloads the rooted checkpoint plus retained committed tail from the
// backing driver. Followers use this to catch up their in-memory view without
// reopening the store.
func (s *Store) Refresh() error {
	if s == nil {
		return nil
	}
	observed, err := s.ObserveCommitted()
	if err != nil {
		return err
	}
	s.applyObserved(observed)
	return nil
}

// ObserveCommitted returns the current compact checkpoint plus retained
// committed tail observed from the replicated root substrate.
func (s *Store) ObserveCommitted() (rootstorage.ObservedCommitted, error) {
	if s == nil {
		return rootstorage.ObservedCommitted{}, nil
	}
	return rootstorage.ObserveCommitted(s.driver, 0)
}

// WaitForTail waits until the durable committed tail view changes past after.
// This is the current catch-up primitive for follower services until a more
// direct push/watch path exists.
func (s *Store) WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
	if s == nil {
		return rootstorage.TailAdvance{}, nil
	}
	return s.driver.WaitForTail(after, timeout)
}

func (s *Store) ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	if s == nil {
		return rootstorage.TailAdvance{}, nil
	}
	return s.driver.ObserveTail(after)
}

func (s *Store) TailNotify() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.driver.TailNotify()
}

// SubscribeTail returns one watch-like rooted tail subscription over the
// replicated metadata substrate.
func (s *Store) SubscribeTail(after rootstorage.TailToken) *rootstorage.TailSubscription {
	if s == nil {
		return nil
	}
	return rootstorage.NewWatchedTailSubscription(after, s.ObserveTail, s.TailNotify(), s.WaitForTail)
}

func (s *Store) Current() (rootstate.State, error) {
	if s == nil {
		return rootstate.State{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state, nil
}

func (s *Store) Snapshot() (rootstate.Snapshot, error) {
	if s == nil {
		return rootstate.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return rootstate.CloneSnapshot(rootstate.Snapshot{
		State:               s.state,
		Descriptors:         s.descs,
		PendingPeerChanges:  s.pending,
		PendingRangeChanges: s.pendingRange,
	}), nil
}

func (s *Store) ReadSince(cursor rootstate.Cursor) ([]rootevent.Event, rootstate.Cursor, error) {
	if s == nil {
		return nil, rootstate.Cursor{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rootstate.CursorAfter(s.retainFrom, cursor) {
		return rootmaterialize.SnapshotDescriptorEvents(s.descs), s.state.LastCommitted, nil
	}
	out := make([]rootevent.Event, 0, len(s.records))
	for _, rec := range s.records {
		if rootstate.CursorAfter(rec.Cursor, cursor) {
			out = append(out, rootevent.CloneEvent(rec.Event))
		}
	}
	return out, s.state.LastCommitted, nil
}

func (s *Store) Append(events ...rootevent.Event) (rootstate.CommitInfo, error) {
	if s == nil || len(events) == 0 {
		state, _ := s.Current()
		return rootstate.CommitInfo{Cursor: state.LastCommitted, State: state}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(events...)
}

func (s *Store) appendLocked(events ...rootevent.Event) (rootstate.CommitInfo, error) {
	var next rootstate.Cursor
	snapshot := rootstate.Snapshot{
		State:               s.state,
		Descriptors:         rootstate.CloneDescriptors(s.descs),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(s.pending),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(s.pendingRange),
	}
	records := make([]rootstorage.CommittedEvent, 0, len(events))
	for _, evt := range events {
		next = rootstate.NextCursor(snapshot.State.LastCommitted)
		rootstate.ApplyEventToSnapshot(&snapshot, next, evt)
		records = append(records, rootstorage.CommittedEvent{Cursor: next, Event: rootevent.CloneEvent(evt)})
	}
	logEnd, err := s.driver.AppendCommitted(records...)
	if err != nil {
		return rootstate.CommitInfo{}, err
	}
	if err := s.driver.SaveCheckpoint(rootstorage.Checkpoint{
		Snapshot:   rootstate.CloneSnapshot(snapshot),
		TailOffset: logEnd,
	}); err != nil {
		return rootstate.CommitInfo{}, err
	}
	s.state = snapshot.State
	s.descs = snapshot.Descriptors
	s.pending = snapshot.PendingPeerChanges
	s.pendingRange = snapshot.PendingRangeChanges
	s.records = append(s.records, records...)
	s.retainFrom = (rootstorage.CommittedTail{Records: s.records}).RetainFrom(snapshot.State.LastCommitted)
	s.maybeCompactLocked()
	return rootstate.CommitInfo{Cursor: snapshot.State.LastCommitted, State: snapshot.State}, nil
}

func (s *Store) ApplyCoordinatorLease(cmd rootstate.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error) {
	if s == nil {
		return rootstate.CoordinatorProtocolState{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Kind {
	case rootstate.CoordinatorLeaseCommandIssue:
		if err := rootstate.ValidateCoordinatorLeaseCampaign(s.state.CoordinatorLease, s.state.CoordinatorSeal, cmd.HolderID, cmd.PredecessorDigest, cmd.ExpiresUnixNano, cmd.NowUnixNano); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		if err := rootstate.ValidateCoordinatorLeaseSuccessorCoverageFrontiers(
			s.state.CoordinatorLease,
			s.state.CoordinatorSeal,
			cmd.HandoffFrontiers,
		); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		generation := rootstate.NextCoordinatorLeaseGeneration(s.state.CoordinatorLease, s.state.CoordinatorSeal, cmd.HolderID, cmd.NowUnixNano)
		commit, err := s.appendLocked(rootevent.CoordinatorLeaseGranted(
			cmd.HolderID,
			cmd.ExpiresUnixNano,
			generation,
			rootproto.CoordinatorDutyMaskDefault,
			cmd.PredecessorDigest,
			rootproto.CloneDutyFrontiers(cmd.HandoffFrontiers),
		))
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	case rootstate.CoordinatorLeaseCommandRelease:
		if err := rootstate.ValidateCoordinatorLeaseRelease(s.state.CoordinatorLease, cmd.HolderID, cmd.NowUnixNano); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		current := s.state.CoordinatorLease
		dutyMask := current.DutyMask
		if dutyMask == 0 {
			dutyMask = rootproto.CoordinatorDutyMaskDefault
		}
		commit, err := s.appendLocked(rootevent.CoordinatorLeaseReleased(
			cmd.HolderID,
			cmd.NowUnixNano,
			current.CertGeneration,
			dutyMask,
			current.PredecessorDigest,
			rootproto.CloneDutyFrontiers(cmd.HandoffFrontiers),
		))
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	default:
		return s.state.CoordinatorProtocol(), rootstate.ErrInvalidCoordinatorLease
	}
}

func (s *Store) ApplyCoordinatorClosure(cmd rootstate.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error) {
	if s == nil {
		return rootstate.CoordinatorProtocolState{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Kind {
	case rootstate.CoordinatorClosureCommandSeal:
		current := s.state.CoordinatorLease
		if s.state.CoordinatorSeal.CertGeneration != 0 &&
			s.state.CoordinatorSeal.CertGeneration == current.CertGeneration &&
			s.state.CoordinatorSeal.HolderID == strings.TrimSpace(cmd.HolderID) {
			return s.state.CoordinatorProtocol(), nil
		}
		if err := rootstate.ValidateCoordinatorLeaseSeal(current, cmd.HolderID); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		dutyMask := current.DutyMask
		if dutyMask == 0 {
			dutyMask = rootproto.CoordinatorDutyMaskDefault
		}
		commit, err := s.appendLocked(rootevent.CoordinatorLeaseSealed(
			cmd.HolderID,
			current.CertGeneration,
			dutyMask,
			rootproto.CloneDutyFrontiers(cmd.Frontiers),
		))
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	case rootstate.CoordinatorClosureCommandConfirm:
		if strings.TrimSpace(cmd.HolderID) == "" || strings.TrimSpace(cmd.HolderID) != s.state.CoordinatorLease.HolderID {
			return s.state.CoordinatorProtocol(), rootstate.ErrCoordinatorLeaseOwner
		}
		auditStatus, err := controlplane.ValidateClosureConfirmation(
			s.state.CoordinatorLease,
			controlplane.Frontiers(s.state, rootstate.MaxDescriptorRevision(s.descs)),
			s.state.CoordinatorSeal,
			cmd.NowUnixNano,
		)
		if err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		if rootproto.ClosureStageAtLeast(s.state.CoordinatorClosure.Stage, rootproto.CoordinatorClosureStageConfirmed) &&
			s.state.CoordinatorClosure.SealGeneration == auditStatus.SealGeneration &&
			s.state.CoordinatorClosure.SuccessorGeneration == s.state.CoordinatorLease.CertGeneration &&
			s.state.CoordinatorClosure.SealDigest == auditStatus.SealDigest {
			return s.state.CoordinatorProtocol(), nil
		}
		commit, err := s.appendLocked(rootevent.CoordinatorClosureConfirmed(
			cmd.HolderID,
			auditStatus.SealGeneration,
			s.state.CoordinatorLease.CertGeneration,
			auditStatus.SealDigest,
		))
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	case rootstate.CoordinatorClosureCommandClose:
		if err := controlplane.ValidateClosureClose(s.state.CoordinatorLease, s.state.CoordinatorClosure, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		if rootproto.ClosureStageAtLeast(s.state.CoordinatorClosure.Stage, rootproto.CoordinatorClosureStageClosed) {
			return s.state.CoordinatorProtocol(), nil
		}
		commit, err := s.appendLocked(rootevent.CoordinatorClosureClosed(
			cmd.HolderID,
			s.state.CoordinatorClosure.SealGeneration,
			s.state.CoordinatorClosure.SuccessorGeneration,
			s.state.CoordinatorClosure.SealDigest,
		))
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	case rootstate.CoordinatorClosureCommandReattach:
		if err := controlplane.ValidateClosureReattach(s.state.CoordinatorLease, s.state.CoordinatorClosure, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return s.state.CoordinatorProtocol(), err
		}
		if rootproto.ClosureStageAtLeast(s.state.CoordinatorClosure.Stage, rootproto.CoordinatorClosureStageReattached) {
			return s.state.CoordinatorProtocol(), nil
		}
		commit, err := s.appendLocked(rootevent.CoordinatorClosureReattached(
			cmd.HolderID,
			s.state.CoordinatorClosure.SealGeneration,
			s.state.CoordinatorClosure.SuccessorGeneration,
			s.state.CoordinatorClosure.SealDigest,
		))
		if err != nil {
			return rootstate.CoordinatorProtocolState{}, err
		}
		return commit.State.CoordinatorProtocol(), nil
	default:
		return s.state.CoordinatorProtocol(), rootstate.ErrCoordinatorLeaseAudit
	}
}

func (s *Store) FenceAllocator(kind rootstate.AllocatorKind, min uint64) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
	switch kind {
	case rootstate.AllocatorKindID:
		if state.IDFence >= min {
			return state.IDFence, nil
		}
		commit, err := s.Append(rootevent.IDAllocatorFenced(min))
		if err != nil {
			return 0, err
		}
		return commit.State.IDFence, nil
	case rootstate.AllocatorKindTSO:
		if state.TSOFence >= min {
			return state.TSOFence, nil
		}
		commit, err := s.Append(rootevent.TSOAllocatorFenced(min))
		if err != nil {
			return 0, err
		}
		return commit.State.TSOFence, nil
	default:
		return 0, errUnknownAllocatorKind(uint32(kind))
	}
}

// InstallBootstrap replaces the rooted checkpoint and retained committed tail.
// It is the future landing point for replicated snapshot installation.
func (s *Store) InstallBootstrap(observed rootstorage.ObservedCommitted) error {
	if s == nil {
		return nil
	}
	if err := s.driver.InstallBootstrap(observed); err != nil {
		return err
	}
	s.applyObserved(observed)
	return nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	if closer, ok := s.driver.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) maybeCompactLocked() {
	if s == nil {
		return
	}
	snapshot := rootstate.Snapshot{
		State:               s.state,
		Descriptors:         rootstate.CloneDescriptors(s.descs),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(s.pending),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(s.pendingRange),
	}
	plan := rootstorage.PlanTailCompaction(s.records, s.state.LastCommitted, s.maxRetainedRecords)
	if !plan.Compacted {
		s.records = plan.Tail.Records
		s.retainFrom = plan.RetainFrom
		return
	}
	if err := s.driver.InstallBootstrap(plan.Observed(snapshot)); err != nil {
		return
	}
	s.records = plan.Tail.Records
	s.retainFrom = plan.RetainFrom
}

func (s *Store) applyObserved(observed rootstorage.ObservedCommitted) {
	if s == nil {
		return
	}
	bootstrap := rootmaterialize.BootstrapFromObserved(observed)
	s.mu.Lock()
	s.state = bootstrap.Snapshot.State
	s.descs = bootstrap.Snapshot.Descriptors
	s.pending = bootstrap.Snapshot.PendingPeerChanges
	s.pendingRange = bootstrap.Snapshot.PendingRangeChanges
	s.records = bootstrap.Tail.Records
	s.retainFrom = bootstrap.RetainFrom
	s.mu.Unlock()
}
