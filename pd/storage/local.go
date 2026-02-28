package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/vfs"
)

// LocalStore persists PD metadata to local files.
//
// Region metadata is stored in manifest edits.
// Allocator counters are stored in StateFileName.
type LocalStore struct {
	fs       vfs.FS
	workdir  string
	manifest *manifest.Manager
	stateMu  sync.Mutex
}

// OpenLocalStore opens a file-backed PD storage in workdir.
func OpenLocalStore(workdir string, fs vfs.FS) (*LocalStore, error) {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return nil, fmt.Errorf("pd/storage: workdir is required")
	}
	fs = vfs.Ensure(fs)
	mgr, err := manifest.Open(workdir, fs)
	if err != nil {
		return nil, err
	}
	return &LocalStore{
		fs:       fs,
		workdir:  workdir,
		manifest: mgr,
	}, nil
}

// Load returns persisted region metadata and allocator counters.
func (s *LocalStore) Load() (Snapshot, error) {
	out := Snapshot{
		Regions: make(map[uint64]manifest.RegionMeta),
	}
	if s == nil {
		return out, nil
	}
	if s.manifest != nil {
		out.Regions = s.manifest.RegionSnapshot()
		if out.Regions == nil {
			out.Regions = make(map[uint64]manifest.RegionMeta)
		}
	}
	state, err := s.loadAllocatorState()
	if err != nil {
		return Snapshot{}, err
	}
	out.Allocator = state
	return out, nil
}

// SaveRegion persists one region metadata update.
func (s *LocalStore) SaveRegion(meta manifest.RegionMeta) error {
	if s == nil || s.manifest == nil {
		return nil
	}
	return s.manifest.LogRegionUpdate(meta)
}

// DeleteRegion persists one region metadata delete.
func (s *LocalStore) DeleteRegion(regionID uint64) error {
	if s == nil || s.manifest == nil {
		return nil
	}
	return s.manifest.LogRegionDelete(regionID)
}

// SaveAllocatorState persists latest allocator counters atomically.
func (s *LocalStore) SaveAllocatorState(idCurrent, tsCurrent uint64) error {
	if s == nil {
		return nil
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	payload, err := json.Marshal(AllocatorState{
		IDCurrent: idCurrent,
		TSCurrent: tsCurrent,
	})
	if err != nil {
		return err
	}

	path := filepath.Join(s.workdir, StateFileName)
	tmp := path + ".tmp"
	if err := s.fs.WriteFile(tmp, payload, 0o644); err != nil {
		return err
	}
	return s.fs.Rename(tmp, path)
}

// Close closes the underlying manifest manager.
func (s *LocalStore) Close() error {
	if s == nil || s.manifest == nil {
		return nil
	}
	return s.manifest.Close()
}

func (s *LocalStore) loadAllocatorState() (AllocatorState, error) {
	path := filepath.Join(s.workdir, StateFileName)
	data, err := s.fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return AllocatorState{}, nil
		}
		return AllocatorState{}, err
	}
	if len(data) == 0 {
		return AllocatorState{}, nil
	}
	var out AllocatorState
	if err := json.Unmarshal(data, &out); err != nil {
		return AllocatorState{}, err
	}
	return out, nil
}
