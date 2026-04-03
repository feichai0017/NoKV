package replicated

import (
	"fmt"
	"sync"
	"time"

	rootpkg "github.com/feichai0017/NoKV/meta/root"
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
	driver  Driver
	storage rootstorage.Substrate

	mu                 sync.RWMutex
	state              rootstate.State
	descs              map[uint64]descriptor.Descriptor
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
	bootstrap, err := rootmaterialize.LoadBootstrap(cfg.Driver)
	if err != nil {
		return nil, err
	}
	return &Store{
		driver:             cfg.Driver,
		storage:            cfg.Driver,
		state:              bootstrap.Snapshot.State,
		descs:              bootstrap.Snapshot.Descriptors,
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

// Refresh reloads the rooted checkpoint plus retained committed tail from the
// backing driver. Followers use this to catch up their in-memory view without
// reopening the store.
func (s *Store) Refresh() error {
	if s == nil {
		return nil
	}
	bootstrap, err := rootmaterialize.LoadBootstrap(s.storage)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.state = bootstrap.Snapshot.State
	s.descs = bootstrap.Snapshot.Descriptors
	s.records = bootstrap.Tail.Records
	s.retainFrom = bootstrap.RetainFrom
	s.mu.Unlock()
	return nil
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
		State:       s.state,
		Descriptors: s.descs,
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

	var next rootstate.Cursor
	state := s.state
	descs := rootstate.CloneDescriptors(s.descs)
	records := make([]rootstorage.CommittedEvent, 0, len(events))
	for _, evt := range events {
		next = rootstate.NextCursor(state.LastCommitted)
		rootstate.ApplyEventToState(&state, next, evt)
		rootmaterialize.ApplyEventToDescriptors(descs, evt)
		records = append(records, rootstorage.CommittedEvent{Cursor: next, Event: rootevent.CloneEvent(evt)})
	}
	logEnd, err := s.storage.AppendCommitted(records...)
	if err != nil {
		return rootstate.CommitInfo{}, err
	}
	if err := s.storage.SaveCheckpoint(rootstorage.Checkpoint{
		Snapshot:   rootstate.Snapshot{State: state, Descriptors: descs},
		TailOffset: logEnd,
	}); err != nil {
		return rootstate.CommitInfo{}, err
	}
	s.state = state
	s.descs = descs
	s.records = append(s.records, records...)
	s.retainFrom = (rootstorage.CommittedTail{Records: s.records}).RetainFrom(state.LastCommitted)
	s.maybeCompactLocked()
	return rootstate.CommitInfo{Cursor: state.LastCommitted, State: state}, nil
}

func (s *Store) FenceAllocator(kind rootpkg.AllocatorKind, min uint64) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
	switch kind {
	case rootpkg.AllocatorKindID:
		if state.IDFence >= min {
			return state.IDFence, nil
		}
		commit, err := s.Append(rootevent.IDAllocatorFenced(min))
		if err != nil {
			return 0, err
		}
		return commit.State.IDFence, nil
	case rootpkg.AllocatorKindTSO:
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
func (s *Store) InstallBootstrap(snapshot rootstate.Snapshot, records []rootstorage.CommittedEvent) error {
	if s == nil {
		return nil
	}
	checkpoint := rootstorage.Checkpoint{
		Snapshot:   rootstate.CloneSnapshot(snapshot),
		TailOffset: 0,
	}
	retained := rootstorage.CloneCommittedEvents(records)
	if err := s.storage.InstallBootstrap(checkpoint, rootstorage.CommittedTail{Records: retained}); err != nil {
		return err
	}
	return s.Refresh()
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	if closer, ok := s.storage.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) maybeCompactLocked() {
	if s == nil || len(s.records) <= s.maxRetainedRecords {
		return
	}
	start := len(s.records) - s.maxRetainedRecords
	retained := rootmaterialize.CloneCommittedEvents(s.records[start:])
	snapshot := rootstate.Snapshot{
		State:       s.state,
		Descriptors: rootstate.CloneDescriptors(s.descs),
	}
	if err := s.storage.CompactCommitted(rootstorage.CommittedTail{Records: retained}); err != nil {
		return
	}
	if err := s.storage.SaveCheckpoint(rootstorage.Checkpoint{Snapshot: snapshot, TailOffset: 0}); err != nil {
		return
	}
	s.records = retained
	s.retainFrom = (rootstorage.CommittedTail{Records: retained}).RetainFrom(s.state.LastCommitted)
}
