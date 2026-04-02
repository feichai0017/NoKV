package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/feichai0017/NoKV/vfs"
)

type diskState struct {
	Regions   map[uint64]localmeta.RegionMeta `json:"regions"`
	Allocator AllocatorState                  `json:"allocator"`
}

// LocalStore persists PD metadata to a local state file owned by the PD
// package. It does not depend on the storage manifest.
type LocalStore struct {
	fs      vfs.FS
	workdir string

	stateMu sync.Mutex
	state   diskState
}

// OpenLocalStore opens a file-backed PD storage in workdir.
func OpenLocalStore(workdir string, fs vfs.FS) (*LocalStore, error) {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return nil, fmt.Errorf("pd/storage: workdir is required")
	}
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(workdir, 0o755); err != nil {
		return nil, err
	}
	state, err := loadDiskState(fs, workdir)
	if err != nil {
		return nil, err
	}
	return &LocalStore{
		fs:      fs,
		workdir: workdir,
		state:   state,
	}, nil
}

// Load returns persisted region metadata and allocator counters.
func (s *LocalStore) Load() (Snapshot, error) {
	if s == nil {
		return Snapshot{Regions: make(map[uint64]localmeta.RegionMeta)}, nil
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return Snapshot{
		Regions:   localmeta.CloneRegionMetas(s.state.Regions),
		Allocator: s.state.Allocator,
	}, nil
}

// SaveRegion persists one region metadata update.
func (s *LocalStore) SaveRegion(meta localmeta.RegionMeta) error {
	if s == nil {
		return nil
	}
	if meta.ID == 0 {
		return fmt.Errorf("pd/storage: region id is zero")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.state.Regions == nil {
		s.state.Regions = make(map[uint64]localmeta.RegionMeta)
	}
	s.state.Regions[meta.ID] = localmeta.CloneRegionMeta(meta)
	return s.persistLocked()
}

// DeleteRegion persists one region metadata delete.
func (s *LocalStore) DeleteRegion(regionID uint64) error {
	if s == nil || regionID == 0 {
		return nil
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	delete(s.state.Regions, regionID)
	return s.persistLocked()
}

// SaveAllocatorState persists latest allocator counters atomically.
func (s *LocalStore) SaveAllocatorState(idCurrent, tsCurrent uint64) error {
	if s == nil {
		return nil
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.state.Allocator = AllocatorState{
		IDCurrent: idCurrent,
		TSCurrent: tsCurrent,
	}
	return s.persistLocked()
}

// Close releases storage resources.
func (s *LocalStore) Close() error {
	return nil
}

func loadDiskState(fs vfs.FS, workdir string) (diskState, error) {
	path := filepath.Join(workdir, StateFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return diskState{Regions: make(map[uint64]localmeta.RegionMeta)}, nil
		}
		return diskState{}, err
	}
	if len(data) == 0 {
		return diskState{Regions: make(map[uint64]localmeta.RegionMeta)}, nil
	}
	var out diskState
	if err := json.Unmarshal(data, &out); err != nil {
		return diskState{}, err
	}
	if out.Regions == nil {
		out.Regions = make(map[uint64]localmeta.RegionMeta)
	}
	for id, meta := range out.Regions {
		out.Regions[id] = localmeta.CloneRegionMeta(meta)
	}
	return out, nil
}

func (s *LocalStore) persistLocked() error {
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
