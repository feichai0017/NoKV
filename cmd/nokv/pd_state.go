package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

const pdStateFileName = "PD_STATE.json"

type pdAllocatorState struct {
	IDCurrent uint64 `json:"id_current"`
	TSCurrent uint64 `json:"ts_current"`
}

type pdStateStore struct {
	path string
	mu   sync.Mutex
}

func newPDStateStore(workdir string) *pdStateStore {
	return &pdStateStore{
		path: filepath.Join(workdir, pdStateFileName),
	}
}

func (s *pdStateStore) Load() (pdAllocatorState, error) {
	if s == nil {
		return pdAllocatorState{}, nil
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return pdAllocatorState{}, nil
		}
		return pdAllocatorState{}, err
	}
	if len(data) == 0 {
		return pdAllocatorState{}, nil
	}
	var out pdAllocatorState
	if err := json.Unmarshal(data, &out); err != nil {
		return pdAllocatorState{}, err
	}
	return out, nil
}

func (s *pdStateStore) Save(idCurrent, tsCurrent uint64) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	payload, err := json.Marshal(pdAllocatorState{
		IDCurrent: idCurrent,
		TSCurrent: tsCurrent,
	})
	if err != nil {
		return err
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func resolveAllocatorStarts(idStart, tsStart uint64, state pdAllocatorState) (uint64, uint64) {
	if next := state.IDCurrent + 1; next > idStart {
		idStart = next
	}
	if next := state.TSCurrent + 1; next > tsStart {
		tsStart = next
	}
	return idStart, tsStart
}
