package replicated

import (
	"fmt"
	"sync"

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
	Log                rootstorage.EventLog
	Checkpoint         rootstorage.CheckpointStore
	MaxRetainedRecords int
}

// Store hosts the rooted state machine on top of an injected committed log and
// checkpoint store. It is the future Delos-lite landing point for a replicated
// metadata backend, without baking protocol concerns into the root domain.
type Store struct {
	log     rootstorage.EventLog
	checkpt rootstorage.CheckpointStore

	mu                 sync.RWMutex
	state              rootstate.State
	descs              map[uint64]descriptor.Descriptor
	records            []rootstorage.CommittedEvent
	logBase            int64
	retainFrom         rootstate.Cursor
	maxRetainedRecords int
}

var _ rootpkg.Root = (*Store)(nil)

func Open(cfg Config) (*Store, error) {
	if cfg.Log == nil {
		return nil, fmt.Errorf("meta/root/backend/replicated: log is required")
	}
	if cfg.Checkpoint == nil {
		return nil, fmt.Errorf("meta/root/backend/replicated: checkpoint store is required")
	}
	if cfg.MaxRetainedRecords <= 0 {
		cfg.MaxRetainedRecords = defaultRetainedRecords
	}
	bootstrap, err := rootmaterialize.LoadBootstrap(cfg.Checkpoint, cfg.Log)
	if err != nil {
		return nil, err
	}
	return &Store{
		log:                cfg.Log,
		checkpt:            cfg.Checkpoint,
		state:              bootstrap.Snapshot.State,
		descs:              bootstrap.Snapshot.Descriptors,
		records:            bootstrap.Records,
		logBase:            bootstrap.LogOffset,
		retainFrom:         bootstrap.RetainFrom,
		maxRetainedRecords: cfg.MaxRetainedRecords,
	}, nil
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
	logEnd, err := s.log.Append(records...)
	if err != nil {
		return rootstate.CommitInfo{}, err
	}
	if err := s.checkpt.Save(rootstate.Snapshot{State: state, Descriptors: descs}, uint64(logEnd)); err != nil {
		return rootstate.CommitInfo{}, err
	}
	s.state = state
	s.descs = descs
	s.records = append(s.records, records...)
	s.logBase = logEnd
	s.retainFrom = rootmaterialize.RetainedFloor(s.records, state.LastCommitted)
	s.maybeCompactLocked()
	return rootstate.CommitInfo{Cursor: state.LastCommitted, State: state}, nil
}

func (s *Store) FenceAllocator(kind rootpkg.AllocatorKind, min uint64) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.state
	var out *uint64
	switch kind {
	case rootpkg.AllocatorKindID:
		out = &state.IDFence
	case rootpkg.AllocatorKindTSO:
		out = &state.TSOFence
	default:
		return 0, fmt.Errorf("meta/root/backend/replicated: unknown allocator kind %d", kind)
	}
	if *out >= min {
		return *out, nil
	}
	*out = min
	logEnd, err := s.log.Size()
	if err != nil {
		return 0, err
	}
	if err := s.checkpt.Save(rootstate.Snapshot{State: state, Descriptors: rootstate.CloneDescriptors(s.descs)}, uint64(logEnd)); err != nil {
		return 0, err
	}
	s.state = state
	s.logBase = logEnd
	s.maybeCompactLocked()
	return *out, nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	if closer, ok := s.log.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	if closer, ok := s.checkpt.(interface{ Close() error }); ok {
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
	if err := s.log.Rewrite(retained); err != nil {
		return
	}
	if err := s.checkpt.Save(snapshot, 0); err != nil {
		return
	}
	s.records = retained
	s.logBase = 0
	s.retainFrom = rootmaterialize.RetainedFloor(retained, s.state.LastCommitted)
}
