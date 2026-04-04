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
	storage rootstorage.Substrate

	mu           sync.RWMutex
	state        rootstate.State
	descs        map[uint64]descriptor.Descriptor
	pending      map[uint64]rootstate.PendingPeerChange
	pendingRange map[uint64]rootstate.PendingRangeChange
	records      []rootstorage.CommittedEvent
	retainFrom   rootstate.Cursor
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
	storage := rootfile.NewStore(fs, workdir)
	bootstrap, err := rootmaterialize.LoadBootstrap(storage)
	if err != nil {
		return nil, err
	}
	return &Store{
		storage:      storage,
		state:        bootstrap.Snapshot.State,
		descs:        bootstrap.Snapshot.Descriptors,
		pending:      bootstrap.Snapshot.PendingPeerChanges,
		pendingRange: bootstrap.Snapshot.PendingRangeChanges,
		records:      bootstrap.Tail.Records,
		retainFrom:   bootstrap.RetainFrom,
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
		State:               s.state,
		Descriptors:         s.descs,
		PendingPeerChanges:  s.pending,
		PendingRangeChanges: s.pendingRange,
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
	snapshot := rootstate.Snapshot{
		State:               state,
		Descriptors:         rootstate.CloneDescriptors(s.descs),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(s.pending),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(s.pendingRange),
	}
	records := make([]rootstorage.CommittedEvent, 0, len(events))
	for _, evt := range events {
		next = rootstate.NextCursor(snapshot.State.LastCommitted)
		rootmaterialize.ApplyEventToSnapshot(&snapshot, next, evt)
		records = append(records, rootstorage.CommittedEvent{Cursor: next, Event: rootevent.CloneEvent(evt)})
	}
	logEnd, err := s.storage.AppendCommitted(records...)
	if err != nil {
		return rootstate.CommitInfo{}, err
	}
	if err := s.storage.SaveCheckpoint(rootstorage.Checkpoint{
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

// FenceAllocator advances one global allocator fence monotonically.
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
		return 0, fmt.Errorf("meta/root/backend/local: unknown allocator kind %d", kind)
	}
}

func (s *Store) Close() error { return nil }

func (s *Store) maybeCompactLocked() {
	if s == nil || len(s.records) <= maxRetainedRecords {
		return
	}
	start := len(s.records) - maxRetainedRecords
	retained := rootmaterialize.CloneCommittedEvents(s.records[start:])
	snapshot := rootstate.Snapshot{
		State:               s.state,
		Descriptors:         rootstate.CloneDescriptors(s.descs),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(s.pending),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(s.pendingRange),
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
