package replicated

import (
	"fmt"
	"sync"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
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
		return nil, fmt.Errorf("meta/root/backend/replicated: driver is required")
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

func (s *Store) CampaignCoordinatorLease(holderID string, expiresUnixNano, nowUnixNano int64, idFence, tsoFence uint64) (rootstate.CoordinatorLease, error) {
	if s == nil {
		return rootstate.CoordinatorLease{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := rootstate.ValidateCoordinatorLeaseCampaign(s.state.CoordinatorLease, holderID, expiresUnixNano, nowUnixNano); err != nil {
		return s.state.CoordinatorLease, err
	}
	event := rootevent.CoordinatorLeaseGranted(holderID, expiresUnixNano, idFence, tsoFence)
	commit, err := s.appendLocked(event)
	if err != nil {
		return rootstate.CoordinatorLease{}, err
	}
	return commit.State.CoordinatorLease, nil
}

func (s *Store) ReleaseCoordinatorLease(holderID string, nowUnixNano int64, idFence, tsoFence uint64) (rootstate.CoordinatorLease, error) {
	if s == nil {
		return rootstate.CoordinatorLease{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := rootstate.ValidateCoordinatorLeaseRelease(s.state.CoordinatorLease, holderID, nowUnixNano); err != nil {
		return s.state.CoordinatorLease, err
	}
	event := rootevent.CoordinatorLeaseReleased(holderID, nowUnixNano, idFence, tsoFence)
	commit, err := s.appendLocked(event)
	if err != nil {
		return rootstate.CoordinatorLease{}, err
	}
	return commit.State.CoordinatorLease, nil
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
		return 0, fmt.Errorf("meta/root/backend/replicated: unknown allocator kind %d", kind)
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
