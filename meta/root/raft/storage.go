package rootraft

import (
	"math"
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/vfs"
)

// Storage is the raft-log storage for metadata-root replication.
//
// The first implementation keeps entries in memory while preserving the same
// API shape that a persisted WAL/checkpoint backend will need later.
type Storage struct {
	mu   sync.RWMutex
	hard myraft.HardState
	mem  *myraft.MemoryStorage
	disk *persistedStorage
}

var _ myraft.Storage = (*Storage)(nil)

func NewStorage() *Storage {
	return &Storage{mem: myraft.NewMemoryStorage()}
}

func OpenStorage(workdir string, fs vfs.FS) (*Storage, error) {
	disk, err := openPersistedStorage(workdir, fs)
	if err != nil {
		return nil, err
	}
	mem := myraft.NewMemoryStorage()
	hard, snap, entries, err := disk.load()
	if err != nil {
		return nil, err
	}
	if !myraft.IsEmptySnap(snap) {
		if err := mem.ApplySnapshot(snap); err != nil {
			return nil, err
		}
	}
	if len(entries) > 0 {
		if err := mem.Append(entries); err != nil {
			return nil, err
		}
	}
	return &Storage{hard: hard, mem: mem, disk: disk}, nil
}

func (s *Storage) InitialState() (myraft.HardState, myraft.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap, err := s.mem.Snapshot()
	if err != nil {
		return myraft.HardState{}, myraft.ConfState{}, err
	}
	return s.hard, snap.Metadata.ConfState, nil
}

func (s *Storage) Entries(lo, hi, maxSize uint64) ([]myraft.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.Entries(lo, hi, maxSize)
}

func (s *Storage) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.Term(i)
}

func (s *Storage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.LastIndex()
}

func (s *Storage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.FirstIndex()
}

func (s *Storage) Snapshot() (myraft.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.Snapshot()
}

func (s *Storage) CreateSnapshot(index uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.mem.Snapshot()
	if err != nil {
		return err
	}
	snap, err := s.mem.CreateSnapshot(index, &current.Metadata.ConfState, data)
	if err != nil {
		return err
	}
	if s.disk != nil {
		first, err := s.mem.FirstIndex()
		if err != nil {
			return err
		}
		last, err := s.mem.LastIndex()
		if err != nil {
			return err
		}
		var entries []myraft.Entry
		if last >= first {
			entries, err = s.mem.Entries(first, last+1, math.MaxUint64)
			if err != nil {
				return err
			}
		}
		if err := s.disk.save(s.hard, snap, entries); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) Compact(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Compact(index); err != nil {
		return err
	}
	if s.disk != nil {
		first, err := s.mem.FirstIndex()
		if err != nil {
			return err
		}
		last, err := s.mem.LastIndex()
		if err != nil {
			return err
		}
		var entries []myraft.Entry
		if last >= first {
			entries, err = s.mem.Entries(first, last+1, math.MaxUint64)
			if err != nil {
				return err
			}
		}
		snap, err := s.mem.Snapshot()
		if err != nil {
			return err
		}
		if err := s.disk.save(s.hard, snap, entries); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) AppendReady(rd myraft.Ready) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !myraft.IsEmptySnap(rd.Snapshot) {
		if err := s.mem.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if !myraft.IsEmptyHardState(rd.HardState) {
		s.hard = rd.HardState
	}
	if len(rd.Entries) > 0 {
		if err := s.mem.Append(rd.Entries); err != nil {
			return err
		}
	}
	if s.disk != nil {
		first, err := s.mem.FirstIndex()
		if err != nil {
			return err
		}
		last, err := s.mem.LastIndex()
		if err != nil {
			return err
		}
		var entries []myraft.Entry
		if last >= first {
			entries, err = s.mem.Entries(first, last+1, math.MaxUint64)
			if err != nil {
				return err
			}
		}
		snap, err := s.mem.Snapshot()
		if err != nil {
			return err
		}
		if err := s.disk.save(s.hard, snap, entries); err != nil {
			return err
		}
	}
	return nil
}
