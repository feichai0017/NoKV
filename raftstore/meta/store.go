package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/feichai0017/NoKV/vfs"
)

// StateFileName is the durable local peer catalog file used by one store.
const StateFileName = "RAFTSTORE_STATE.json"

type diskState struct {
	Regions      map[uint64]RegionMeta `json:"regions"`
	RaftPointers map[uint64]RaftLogPointer      `json:"raft_pointers"`
}

// Store persists store-local region metadata used only for local recovery.
// It is not cluster authority and must not be treated as routing truth.
type Store struct {
	fs      vfs.FS
	workdir string

	mu    sync.RWMutex
	state diskState
}

// WorkDir returns the local metadata directory backing this store.
func (s *Store) WorkDir() string {
	if s == nil {
		return ""
	}
	return s.workdir
}

// OpenLocalStore opens the local raftstore metadata store in workdir.
func OpenLocalStore(workdir string, fs vfs.FS) (*Store, error) {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return nil, fmt.Errorf("raftstore/meta: workdir is required")
	}
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(workdir, 0o755); err != nil {
		return nil, err
	}
	state, err := loadState(fs, workdir)
	if err != nil {
		return nil, err
	}
	return &Store{
		fs:      fs,
		workdir: workdir,
		state:   state,
	}, nil
}

// Snapshot returns a deep copy of the local peer catalog.
func (s *Store) Snapshot() map[uint64]RegionMeta {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CloneRegionMetas(s.state.Regions)
}

// RaftPointer returns the last persisted local WAL pointer for one raft group.
func (s *Store) RaftPointer(groupID uint64) (RaftLogPointer, bool) {
	if s == nil || groupID == 0 {
		return RaftLogPointer{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ptr, ok := s.state.RaftPointers[groupID]
	return ptr, ok
}

// RaftPointerSnapshot returns a copy of all persisted local WAL pointers.
func (s *Store) RaftPointerSnapshot() map[uint64]RaftLogPointer {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CloneRaftPointers(s.state.RaftPointers)
}

// Empty reports whether the local peer catalog is empty.
func (s *Store) Empty() bool {
	if s == nil {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.state.Regions) == 0
}

// SaveRegion persists one local region metadata update.
func (s *Store) SaveRegion(meta RegionMeta) error {
	if s == nil {
		return nil
	}
	if meta.ID == 0 {
		return fmt.Errorf("raftstore/meta: region id is zero")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Regions == nil {
		s.state.Regions = make(map[uint64]RegionMeta)
	}
	s.state.Regions[meta.ID] = CloneRegionMeta(meta)
	return s.persistLocked()
}

// DeleteRegion removes one region metadata entry from the local catalog.
func (s *Store) DeleteRegion(regionID uint64) error {
	if s == nil || regionID == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state.Regions, regionID)
	return s.persistLocked()
}

// SaveRaftPointer persists the local WAL checkpoint for one raft group.
func (s *Store) SaveRaftPointer(ptr RaftLogPointer) error {
	if s == nil {
		return nil
	}
	if ptr.GroupID == 0 {
		return fmt.Errorf("raftstore/meta: raft pointer group id is zero")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.RaftPointers == nil {
		s.state.RaftPointers = make(map[uint64]RaftLogPointer)
	}
	s.state.RaftPointers[ptr.GroupID] = ptr
	return s.persistLocked()
}

// Close releases resources associated with the metadata store.
func (s *Store) Close() error {
	return nil
}

func loadState(fs vfs.FS, workdir string) (diskState, error) {
	path := filepath.Join(workdir, StateFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return diskState{
				Regions:      make(map[uint64]RegionMeta),
				RaftPointers: make(map[uint64]RaftLogPointer),
			}, nil
		}
		return diskState{}, err
	}
	if len(data) == 0 {
		return diskState{
			Regions:      make(map[uint64]RegionMeta),
			RaftPointers: make(map[uint64]RaftLogPointer),
		}, nil
	}
	var state diskState
	if err := json.Unmarshal(data, &state); err != nil {
		return diskState{}, err
	}
	if state.Regions == nil {
		state.Regions = make(map[uint64]RegionMeta)
	}
	if state.RaftPointers == nil {
		state.RaftPointers = make(map[uint64]RaftLogPointer)
	}
	for id, meta := range state.Regions {
		state.Regions[id] = CloneRegionMeta(meta)
	}
	state.RaftPointers = CloneRaftPointers(state.RaftPointers)
	return state, nil
}

func (s *Store) persistLocked() error {
	payload, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	path := filepath.Join(s.workdir, StateFileName)
	tmp := path + ".tmp"
	f, err := s.fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	writeErr := writeAll(f, payload)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		_ = s.fs.Remove(tmp)
		return writeErr
	}
	if syncErr != nil {
		_ = s.fs.Remove(tmp)
		return syncErr
	}
	if closeErr != nil {
		_ = s.fs.Remove(tmp)
		return closeErr
	}
	if err := s.fs.Rename(tmp, path); err != nil {
		return err
	}
	return vfs.SyncDir(s.fs, s.workdir)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
