package rootraft

import (
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
)

// Storage is the raft-log storage for metadata-root replication.
//
// The first implementation keeps entries in memory while preserving the same
// API shape that a persisted WAL/checkpoint backend will need later.
type Storage struct {
	mu   sync.RWMutex
	hard myraft.HardState
	mem  *myraft.MemoryStorage
}

var _ myraft.Storage = (*Storage)(nil)

func NewStorage() *Storage {
	return &Storage{mem: myraft.NewMemoryStorage()}
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
	return nil
}
