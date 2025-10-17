package raftstore

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
)

const (
	logFileName  = "raft.log"
	hardFileName = "raft.hard"
	snapFileName = "raft.snap"
)

// DiskStorage persists raft entries, hard state, and snapshot to disk while
// delegating the Storage interface to an in-memory MemoryStorage.
type DiskStorage struct {
	mu        sync.Mutex
	dir       string
	mem       *myraft.MemoryStorage
	entries   []myraft.Entry
	hardState myraft.HardState
	snapshot  myraft.Snapshot
}

// OpenDiskStorage loads or initialises raft storage in the provided directory.
func OpenDiskStorage(dir string) (*DiskStorage, error) {
	if dir == "" {
		return nil, fmt.Errorf("raftstore: storage dir required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ds := &DiskStorage{
		dir: dir,
		mem: myraft.NewMemoryStorage(),
	}
	if err := ds.load(); err != nil {
		return nil, err
	}
	return ds, nil
}

func (ds *DiskStorage) load() error {
	if err := ds.loadSnapshot(); err != nil {
		return err
	}
	if err := ds.loadEntries(); err != nil {
		return err
	}
	if err := ds.loadHardState(); err != nil {
		return err
	}
	return nil
}

func (ds *DiskStorage) loadSnapshot() error {
	path := filepath.Join(ds.dir, snapFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var snap myraft.Snapshot
	if err := snap.Unmarshal(data); err != nil {
		return fmt.Errorf("raftstore: decode snapshot: %w", err)
	}
	if myraft.IsEmptySnap(snap) {
		return nil
	}
	if err := ds.mem.ApplySnapshot(snap); err != nil {
		return err
	}
	ds.snapshot = snap
	return nil
}

func (ds *DiskStorage) loadEntries() error {
	path := filepath.Join(ds.dir, logFileName)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	var entries []myraft.Entry
	for {
		var length uint32
		if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("raftstore: read log length: %w", err)
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(f, buf); err != nil {
			return fmt.Errorf("raftstore: read log payload: %w", err)
		}
		var entry myraft.Entry
		if err := entry.Unmarshal(buf); err != nil {
			return fmt.Errorf("raftstore: decode entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if len(entries) > 0 {
		if err := ds.mem.Append(entries); err != nil {
			return err
		}
	}
	ds.entries = entries
	return nil
}

func (ds *DiskStorage) loadHardState() error {
	path := filepath.Join(ds.dir, hardFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var hs myraft.HardState
	if err := hs.Unmarshal(data); err != nil {
		return fmt.Errorf("raftstore: decode hard state: %w", err)
	}
	if err := ds.mem.SetHardState(hs); err != nil {
		return err
	}
	ds.hardState = hs
	return nil
}

// Append persists new entries and forwards them to the underlying MemoryStorage.
func (ds *DiskStorage) Append(entries []myraft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if err := ds.mem.Append(entries); err != nil {
		return err
	}
	if err := ds.refreshEntriesLocked(); err != nil {
		return err
	}
	return ds.persistEntriesLocked()
}

// ApplySnapshot persists the snapshot.
func (ds *DiskStorage) ApplySnapshot(snap myraft.Snapshot) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if err := ds.mem.ApplySnapshot(snap); err != nil {
		return err
	}
	ds.snapshot = snap
	if err := ds.refreshEntriesLocked(); err != nil {
		return err
	}
	return ds.persistSnapshotLocked()
}

// SetHardState persists the raft hard state.
func (ds *DiskStorage) SetHardState(st myraft.HardState) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if err := ds.mem.SetHardState(st); err != nil {
		return err
	}
	ds.hardState = st
	return ds.persistHardStateLocked()
}

// Snapshot delegates to the underlying storage.
func (ds *DiskStorage) Snapshot() (myraft.Snapshot, error) {
	return ds.mem.Snapshot()
}

// Entries returns a slice of log entries between [lo,hi).
func (ds *DiskStorage) Entries(lo, hi, maxSize uint64) ([]myraft.Entry, error) {
	return ds.mem.Entries(lo, hi, maxSize)
}

// Term returns the term of entry i, which must be in the log.
func (ds *DiskStorage) Term(i uint64) (uint64, error) {
	return ds.mem.Term(i)
}

// LastIndex returns the last index of the log entries.
func (ds *DiskStorage) LastIndex() (uint64, error) {
	return ds.mem.LastIndex()
}

// FirstIndex returns the index of the first log entry that is possible to return in Entries (older entries have been incorporated into the latest snapshot; if storage only contains the snapshot, it returns snapshot.Metadata.Index + 1).
func (ds *DiskStorage) FirstIndex() (uint64, error) {
	return ds.mem.FirstIndex()
}

// InitialState returns the HardState and ConfState information.
func (ds *DiskStorage) InitialState() (myraft.HardState, myraft.ConfState, error) {
	return ds.mem.InitialState()
}

func (ds *DiskStorage) refreshEntriesLocked() error {
	first, err := ds.mem.FirstIndex()
	if err != nil {
		ds.entries = nil
		return nil
	}
	last, err := ds.mem.LastIndex()
	if err != nil {
		return err
	}
	if last < first {
		ds.entries = nil
		return nil
	}
	ents, err := ds.mem.Entries(first, last+1, math.MaxUint64)
	if err != nil {
		return err
	}
	ds.entries = ents
	return nil
}

func (ds *DiskStorage) persistEntriesLocked() error {
	path := filepath.Join(ds.dir, logFileName)
	if len(ds.entries) == 0 {
		return removeFile(path)
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, entry := range ds.entries {
		data, err := entry.Marshal()
		if err != nil {
			f.Close()
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, uint32(len(data))); err != nil {
			f.Close()
			return err
		}
		if _, err := f.Write(data); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (ds *DiskStorage) persistHardStateLocked() error {
	path := filepath.Join(ds.dir, hardFileName)
	if myraft.IsEmptyHardState(ds.hardState) {
		return removeFile(path)
	}
	data, err := ds.hardState.Marshal()
	if err != nil {
		return err
	}
	return atomicWriteFile(path, data)
}

func (ds *DiskStorage) persistSnapshotLocked() error {
	path := filepath.Join(ds.dir, snapFileName)
	if myraft.IsEmptySnap(ds.snapshot) {
		return removeFile(path)
	}
	data, err := ds.snapshot.Marshal()
	if err != nil {
		return err
	}
	return atomicWriteFile(path, data)
}

func removeFile(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func atomicWriteFile(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
