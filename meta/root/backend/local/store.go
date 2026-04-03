package local

import (
	"fmt"
	"strings"
	"sync"

	rootpkg "github.com/feichai0017/NoKV/meta/root"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	rootfile "github.com/feichai0017/NoKV/meta/root/storage/file"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/feichai0017/NoKV/vfs"
)

const maxRetainedRecords = 64

// Store is a file-backed local metadata-root implementation.
//
// It is intentionally minimal: an append-only event log, a compact protobuf
// checkpoint, and an in-memory event index for ReadSince.
type Store struct {
	checkpt rootstorage.CheckpointStore
	log     rootstorage.EventLog

	mu         sync.RWMutex
	state      rootstate.State
	descs      map[uint64]descriptor.Descriptor
	records    []rootstorage.CommittedEvent
	logBase    int64
	retainFrom rootstate.Cursor
}

// Open opens or creates a local metadata-root store in workdir.
func Open(workdir string, fs vfs.FS) (*Store, error) {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return nil, fmt.Errorf("meta/root/backend/local: workdir is required")
	}
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(workdir, 0o755); err != nil {
		return nil, err
	}
	checkpt := rootfile.NewCheckpointStore(fs, workdir)
	log := rootfile.NewEventLog(fs, workdir)
	bootstrap, err := rootmaterialize.LoadBootstrap(checkpt, log)
	if err != nil {
		return nil, err
	}
	return &Store{
		checkpt:    checkpt,
		log:        log,
		state:      bootstrap.Snapshot.State,
		descs:      bootstrap.Snapshot.Descriptors,
		records:    bootstrap.Records,
		logBase:    bootstrap.LogOffset,
		retainFrom: bootstrap.RetainFrom,
	}, nil
}

// Current returns the current compact root state.
func (s *Store) Current() (rootstate.State, error) {
	if s == nil {
		return rootstate.State{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state, nil
}

// Snapshot returns the compact rooted metadata snapshot.
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

// ReadSince returns all events after cursor together with the current tail cursor.
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

// Append appends ordered metadata events and advances the compact root state.
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
	if err := s.checkpt.Save(rootstorage.Checkpoint{
		Snapshot:  rootstate.Snapshot{State: state, Descriptors: descs},
		LogOffset: logEnd,
	}); err != nil {
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

// FenceAllocator advances one global allocator fence monotonically.
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
		return 0, fmt.Errorf("meta/root/backend/local: unknown allocator kind %d", kind)
	}
	if *out >= min {
		return *out, nil
	}
	*out = min
	logEnd, err := s.log.Size()
	if err != nil {
		return 0, err
	}
	if err := s.checkpt.Save(rootstorage.Checkpoint{
		Snapshot:  rootstate.Snapshot{State: state, Descriptors: rootstate.CloneDescriptors(s.descs)},
		LogOffset: logEnd,
	}); err != nil {
		return 0, err
	}
	s.state = state
	s.logBase = logEnd
	s.maybeCompactLocked()
	return *out, nil
}

func (s *Store) Close() error { return nil }

func (s *Store) maybeCompactLocked() {
	if s == nil || len(s.records) <= maxRetainedRecords {
		return
	}
	start := len(s.records) - maxRetainedRecords
	retained := rootmaterialize.CloneCommittedEvents(s.records[start:])
	snapshot := rootstate.Snapshot{
		State:       s.state,
		Descriptors: rootstate.CloneDescriptors(s.descs),
	}
	if err := s.log.Compact(retained); err != nil {
		return
	}
	if err := s.checkpt.Save(rootstorage.Checkpoint{Snapshot: snapshot, LogOffset: 0}); err != nil {
		return
	}
	s.records = retained
	s.logBase = 0
	s.retainFrom = rootmaterialize.RetainedFloor(retained, s.state.LastCommitted)
}
