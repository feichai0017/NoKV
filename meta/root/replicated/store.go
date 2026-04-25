package replicated

import (
	"context"
	"strings"
	"sync"
	"time"

	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootfailpoints "github.com/feichai0017/NoKV/meta/root/failpoints"
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
	stores             map[uint64]rootstate.StoreMembership
	snapshots          map[string]rootstate.SnapshotEpoch
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
	stores := bootstrap.Snapshot.Stores
	if stores == nil {
		stores = make(map[uint64]rootstate.StoreMembership)
	}
	snapshots := bootstrap.Snapshot.SnapshotEpochs
	if snapshots == nil {
		snapshots = make(map[string]rootstate.SnapshotEpoch)
	}
	return &Store{
		driver:             cfg.Driver,
		state:              bootstrap.Snapshot.State,
		stores:             stores,
		snapshots:          snapshots,
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
		Stores:              s.stores,
		SnapshotEpochs:      s.snapshots,
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

func (s *Store) Append(ctx context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error) {
	if s == nil || len(events) == 0 {
		state, _ := s.Current()
		return rootstate.CommitInfo{Cursor: state.LastCommitted, State: state}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return rootstate.CommitInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(ctx, events...)
}

func (s *Store) appendLocked(ctx context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error) {
	var next rootstate.Cursor
	snapshot := rootstate.Snapshot{
		State:               s.state,
		Stores:              rootstate.CloneStoreMemberships(s.stores),
		SnapshotEpochs:      rootstate.CloneSnapshotEpochs(s.snapshots),
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
	logEnd, err := s.driver.AppendCommitted(ctx, records...)
	if err != nil {
		return rootstate.CommitInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return rootstate.CommitInfo{}, err
	}
	if err := rootfailpoints.InjectAfterAppendCommittedBeforeCheckpoint(); err != nil {
		return rootstate.CommitInfo{}, err
	}
	if err := s.driver.SaveCheckpoint(rootstorage.Checkpoint{
		Snapshot:   rootstate.CloneSnapshot(snapshot),
		TailOffset: logEnd,
	}); err != nil {
		return rootstate.CommitInfo{}, err
	}
	s.state = snapshot.State
	s.stores = snapshot.Stores
	s.snapshots = snapshot.SnapshotEpochs
	s.descs = snapshot.Descriptors
	s.pending = snapshot.PendingPeerChanges
	s.pendingRange = snapshot.PendingRangeChanges
	s.records = append(s.records, records...)
	s.retainFrom = (rootstorage.CommittedTail{Records: s.records}).RetainFrom(snapshot.State.LastCommitted)
	s.maybeCompactLocked()
	return rootstate.CommitInfo{Cursor: snapshot.State.LastCommitted, State: snapshot.State}, nil
}

func (s *Store) ApplyTenure(ctx context.Context, cmd rootproto.TenureCommand) (rootstate.SuccessionState, error) {
	if s == nil {
		return rootstate.SuccessionState{}, nil
	}
	if err := rootfailpoints.InjectBeforeApplyTenure(); err != nil {
		return rootstate.SuccessionState{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return rootstate.SuccessionState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Kind {
	case rootproto.TenureActIssue:
		if err := rootstate.ValidateTenureClaim(s.state.Tenure, s.state.Legacy, cmd.HolderID, cmd.LineageDigest, cmd.ExpiresUnixNano, cmd.NowUnixNano); err != nil {
			return s.state.Succession(), err
		}
		if err := rootstate.ValidateInheritance(
			s.state.Tenure,
			s.state.Legacy,
			cmd.InheritedFrontiers,
		); err != nil {
			return s.state.Succession(), err
		}
		era := rootstate.NextTenureEra(s.state.Tenure, s.state.Legacy, cmd.HolderID, cmd.NowUnixNano)
		commit, err := s.appendLocked(ctx, rootevent.TenureGranted(
			cmd.HolderID,
			cmd.ExpiresUnixNano,
			era,
			rootproto.MandateDefault,
			cmd.LineageDigest,
			cmd.InheritedFrontiers,
		))
		if err != nil {
			return rootstate.SuccessionState{}, err
		}
		return commit.State.Succession(), nil
	case rootproto.TenureActRelease:
		if err := rootstate.ValidateTenureYield(s.state.Tenure, cmd.HolderID, cmd.NowUnixNano); err != nil {
			return s.state.Succession(), err
		}
		current := s.state.Tenure
		mandate := current.Mandate
		if mandate == 0 {
			mandate = rootproto.MandateDefault
		}
		commit, err := s.appendLocked(ctx, rootevent.TenureReleased(
			cmd.HolderID,
			cmd.NowUnixNano,
			current.Era,
			mandate,
			current.LineageDigest,
			cmd.InheritedFrontiers,
		))
		if err != nil {
			return rootstate.SuccessionState{}, err
		}
		return commit.State.Succession(), nil
	default:
		return s.state.Succession(), rootstate.ErrInvalidTenure
	}
}

func (s *Store) ApplyHandover(ctx context.Context, cmd rootproto.HandoverCommand) (rootstate.SuccessionState, error) {
	if s == nil {
		return rootstate.SuccessionState{}, nil
	}
	if err := rootfailpoints.InjectBeforeApplyHandover(); err != nil {
		return rootstate.SuccessionState{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return rootstate.SuccessionState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Kind {
	case rootproto.HandoverActSeal:
		current := s.state.Tenure
		if s.state.Legacy.Era != 0 &&
			s.state.Legacy.Era == current.Era &&
			s.state.Legacy.HolderID == strings.TrimSpace(cmd.HolderID) {
			return s.state.Succession(), nil
		}
		if err := rootstate.ValidateLegacyFormation(current, cmd.HolderID); err != nil {
			return s.state.Succession(), err
		}
		mandate := current.Mandate
		if mandate == 0 {
			mandate = rootproto.MandateDefault
		}
		commit, err := s.appendLocked(ctx, rootevent.TenureSealed(
			cmd.HolderID,
			current.Era,
			mandate,
			cmd.Frontiers,
		))
		if err != nil {
			return rootstate.SuccessionState{}, err
		}
		return commit.State.Succession(), nil
	case rootproto.HandoverActConfirm:
		if strings.TrimSpace(cmd.HolderID) == "" || strings.TrimSpace(cmd.HolderID) != s.state.Tenure.HolderID {
			return s.state.Succession(), rootstate.ErrPrimacy
		}
		auditStatus, err := succession.ValidateHandoverConfirmation(
			s.state.Tenure,
			succession.Frontiers(s.state, rootstate.MaxDescriptorRevision(s.descs)),
			s.state.Legacy,
			cmd.NowUnixNano,
		)
		if err != nil {
			return s.state.Succession(), err
		}
		if rootproto.HandoverStageAtLeast(s.state.Handover.Stage, rootproto.HandoverStageConfirmed) &&
			s.state.Handover.LegacyEra == auditStatus.LegacyEra &&
			s.state.Handover.SuccessorEra == s.state.Tenure.Era &&
			s.state.Handover.LegacyDigest == auditStatus.LegacyDigest {
			return s.state.Succession(), nil
		}
		commit, err := s.appendLocked(ctx, rootevent.HandoverConfirmed(
			cmd.HolderID,
			auditStatus.LegacyEra,
			s.state.Tenure.Era,
			auditStatus.LegacyDigest,
		))
		if err != nil {
			return rootstate.SuccessionState{}, err
		}
		return commit.State.Succession(), nil
	case rootproto.HandoverActClose:
		if err := succession.ValidateHandoverFinality(s.state.Tenure, s.state.Handover, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return s.state.Succession(), err
		}
		if rootproto.HandoverStageAtLeast(s.state.Handover.Stage, rootproto.HandoverStageClosed) {
			return s.state.Succession(), nil
		}
		commit, err := s.appendLocked(ctx, rootevent.HandoverClosed(
			cmd.HolderID,
			s.state.Handover.LegacyEra,
			s.state.Handover.SuccessorEra,
			s.state.Handover.LegacyDigest,
		))
		if err != nil {
			return rootstate.SuccessionState{}, err
		}
		return commit.State.Succession(), nil
	case rootproto.HandoverActReattach:
		if err := succession.ValidateHandoverReattach(s.state.Tenure, s.state.Handover, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return s.state.Succession(), err
		}
		if rootproto.HandoverStageAtLeast(s.state.Handover.Stage, rootproto.HandoverStageReattached) {
			return s.state.Succession(), nil
		}
		commit, err := s.appendLocked(ctx, rootevent.HandoverReattached(
			cmd.HolderID,
			s.state.Handover.LegacyEra,
			s.state.Handover.SuccessorEra,
			s.state.Handover.LegacyDigest,
		))
		if err != nil {
			return rootstate.SuccessionState{}, err
		}
		return commit.State.Succession(), nil
	default:
		return s.state.Succession(), rootstate.ErrFinality
	}
}

func (s *Store) FenceAllocator(ctx context.Context, kind rootstate.AllocatorKind, min uint64) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
	switch kind {
	case rootstate.AllocatorKindID:
		if state.IDFence >= min {
			return state.IDFence, nil
		}
		commit, err := s.Append(ctx, rootevent.IDAllocatorFenced(min))
		if err != nil {
			return 0, err
		}
		return commit.State.IDFence, nil
	case rootstate.AllocatorKindTSO:
		if state.TSOFence >= min {
			return state.TSOFence, nil
		}
		commit, err := s.Append(ctx, rootevent.TSOAllocatorFenced(min))
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
		Stores:              rootstate.CloneStoreMemberships(s.stores),
		SnapshotEpochs:      rootstate.CloneSnapshotEpochs(s.snapshots),
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
	s.stores = bootstrap.Snapshot.Stores
	if s.stores == nil {
		s.stores = make(map[uint64]rootstate.StoreMembership)
	}
	s.snapshots = bootstrap.Snapshot.SnapshotEpochs
	if s.snapshots == nil {
		s.snapshots = make(map[string]rootstate.SnapshotEpoch)
	}
	s.descs = bootstrap.Snapshot.Descriptors
	s.pending = bootstrap.Snapshot.PendingPeerChanges
	s.pendingRange = bootstrap.Snapshot.PendingRangeChanges
	s.records = bootstrap.Tail.Records
	s.retainFrom = bootstrap.RetainFrom
	s.mu.Unlock()
}
