package local

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	rootpkg "github.com/feichai0017/NoKV/meta/root"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/feichai0017/NoKV/vfs"
)

const (
	CheckpointFileName = "metadata-root-checkpoint.pb"
	LogFileName        = "metadata-root.log"
	maxRetainedRecords = 64
)

// Store is a file-backed local metadata-root implementation.
//
// It is intentionally minimal: an append-only event log, a compact protobuf
// checkpoint, and an in-memory event index for ReadSince.
type Store struct {
	fs      vfs.FS
	workdir string

	mu         sync.RWMutex
	state      rootpkg.State
	descs      map[uint64]descriptor.Descriptor
	records    []record
	logBase    int64
	retainFrom rootpkg.Cursor
}

var _ rootpkg.Root = (*Store)(nil)

// Open opens or creates a local metadata-root store in workdir.
func Open(workdir string, fs vfs.FS) (*Store, error) {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return nil, fmt.Errorf("meta/root/local: workdir is required")
	}
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(workdir, 0o755); err != nil {
		return nil, err
	}
	snapshot, logBase, err := loadCheckpoint(fs, workdir)
	if err != nil {
		return nil, err
	}
	records, err := loadLog(fs, workdir, logBase)
	if err != nil {
		return nil, err
	}
	for _, rec := range records {
		if after(rec.cursor, snapshot.State.LastCommitted) {
			applyEvent(&snapshot.State, rec.cursor, rec.event)
			rootpkg.ApplyEventToDescriptors(snapshot.Descriptors, rec.event)
		}
	}
	return &Store{
		fs:         fs,
		workdir:    workdir,
		state:      snapshot.State,
		descs:      snapshot.Descriptors,
		records:    records,
		logBase:    logBase,
		retainFrom: retainedFloor(records, snapshot.State.LastCommitted),
	}, nil
}

// Current returns the current compact root state.
func (s *Store) Current() (rootpkg.State, error) {
	if s == nil {
		return rootpkg.State{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneState(s.state), nil
}

// Snapshot returns the compact rooted metadata snapshot.
func (s *Store) Snapshot() (rootpkg.Snapshot, error) {
	if s == nil {
		return rootpkg.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return rootpkg.CloneSnapshot(rootpkg.Snapshot{
		State:       s.state,
		Descriptors: s.descs,
	}), nil
}

// ReadSince returns all events after cursor together with the current tail cursor.
func (s *Store) ReadSince(cursor rootpkg.Cursor) ([]rootpkg.Event, rootpkg.Cursor, error) {
	if s == nil {
		return nil, rootpkg.Cursor{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if after(s.retainFrom, cursor) {
		return snapshotEvents(s.descs), s.state.LastCommitted, nil
	}
	out := make([]rootpkg.Event, 0, len(s.records))
	for _, rec := range s.records {
		if after(rec.cursor, cursor) {
			out = append(out, cloneEvent(rec.event))
		}
	}
	return out, s.state.LastCommitted, nil
}

// Append appends ordered metadata events and advances the compact root state.
func (s *Store) Append(events ...rootpkg.Event) (rootpkg.CommitInfo, error) {
	if s == nil || len(events) == 0 {
		state, _ := s.Current()
		return rootpkg.CommitInfo{Cursor: state.LastCommitted, State: state}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	logPath := filepath.Join(s.workdir, LogFileName)
	f, err := s.fs.OpenFileHandle(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return rootpkg.CommitInfo{}, err
	}
	var next rootpkg.Cursor
	state := cloneState(s.state)
	descs := cloneDescriptors(s.descs)
	records := make([]record, 0, len(events))
	for _, evt := range events {
		next = nextCursor(state.LastCommitted)
		if err := writeRecord(f, next, evt); err != nil {
			_ = f.Close()
			return rootpkg.CommitInfo{}, err
		}
		applyEvent(&state, next, evt)
		rootpkg.ApplyEventToDescriptors(descs, evt)
		records = append(records, record{cursor: next, event: cloneEvent(evt)})
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return rootpkg.CommitInfo{}, err
	}
	logEnd, err := fileSize(f)
	if err != nil {
		_ = f.Close()
		return rootpkg.CommitInfo{}, err
	}
	if err := f.Close(); err != nil {
		return rootpkg.CommitInfo{}, err
	}
	if err := persistCheckpoint(s.fs, s.workdir, rootpkg.Snapshot{State: state, Descriptors: descs}, uint64(logEnd)); err != nil {
		return rootpkg.CommitInfo{}, err
	}
	s.state = state
	s.descs = descs
	s.records = append(s.records, records...)
	s.logBase = logEnd
	s.retainFrom = retainedFloor(s.records, state.LastCommitted)
	s.maybeCompactLocked()
	return rootpkg.CommitInfo{Cursor: state.LastCommitted, State: cloneState(state)}, nil
}

// FenceAllocator advances one global allocator fence monotonically.
func (s *Store) FenceAllocator(kind rootpkg.AllocatorKind, min uint64) (uint64, error) {
	if s == nil {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := cloneState(s.state)
	var out *uint64
	switch kind {
	case rootpkg.AllocatorKindID:
		out = &state.IDFence
	case rootpkg.AllocatorKindTSO:
		out = &state.TSOFence
	default:
		return 0, fmt.Errorf("meta/root/local: unknown allocator kind %d", kind)
	}
	if *out >= min {
		return *out, nil
	}
	*out = min
	logEnd, err := currentLogSize(s.fs, s.workdir)
	if err != nil {
		return 0, err
	}
	if err := persistCheckpoint(s.fs, s.workdir, rootpkg.Snapshot{State: state, Descriptors: cloneDescriptors(s.descs)}, uint64(logEnd)); err != nil {
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
	retained := cloneRecords(s.records[start:])
	snapshot := rootpkg.Snapshot{
		State:       cloneState(s.state),
		Descriptors: cloneDescriptors(s.descs),
	}
	if err := rewriteLog(s.fs, s.workdir, retained); err != nil {
		return
	}
	if err := persistCheckpoint(s.fs, s.workdir, snapshot, 0); err != nil {
		return
	}
	s.records = retained
	s.logBase = 0
	s.retainFrom = retainedFloor(retained, s.state.LastCommitted)
}
