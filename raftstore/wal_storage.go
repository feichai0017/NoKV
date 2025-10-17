package raftstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/wal"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
)

const walRecordOverhead = 8 // length (4 bytes) + checksum (4 bytes)

// WalStorageConfig configures WAL-backed raft storage.
type WalStorageConfig struct {
	GroupID  uint64
	WAL      *wal.Manager
	Manifest *manifest.Manager
}

type walStorage struct {
	mu       sync.Mutex
	groupID  uint64
	wal      *wal.Manager
	manifest *manifest.Manager
	mem      *myraft.MemoryStorage
	pointer  manifest.RaftLogPointer
}

func openWalStorage(cfg WalStorageConfig) (*walStorage, error) {
	if cfg.GroupID == 0 {
		return nil, fmt.Errorf("raftstore: wal storage requires group id")
	}
	if cfg.WAL == nil {
		return nil, fmt.Errorf("raftstore: wal storage requires WAL manager")
	}
	ws := &walStorage{
		groupID:  cfg.GroupID,
		wal:      cfg.WAL,
		manifest: cfg.Manifest,
		mem:      myraft.NewMemoryStorage(),
	}
	if cfg.Manifest != nil {
		if ptr, ok := cfg.Manifest.RaftPointer(cfg.GroupID); ok {
			ws.pointer = ptr
		}
	}

	var replayPtr manifest.RaftLogPointer

	if err := cfg.WAL.Replay(func(info wal.EntryInfo, payload []byte) error {
		switch info.Type {
		case wal.RecordTypeEntry:
			return nil
		case wal.RecordTypeRaftEntry:
			gid, entries, err := decodeRaftEntries(payload)
			if err != nil {
				return err
			}
			if gid != cfg.GroupID || len(entries) == 0 {
				return nil
			}
			if err := ws.mem.Append(entries); err != nil {
				return err
			}
			last := entries[len(entries)-1]
			replayPtr.GroupID = cfg.GroupID
			replayPtr.Segment = info.SegmentID
			replayPtr.Offset = recordEnd(info)
			replayPtr.AppliedIndex = last.Index
			replayPtr.AppliedTerm = last.Term
		case wal.RecordTypeRaftState:
			gid, st, err := decodeRaftHardState(payload)
			if err != nil {
				return err
			}
			if gid != cfg.GroupID {
				return nil
			}
			if err := ws.mem.SetHardState(st); err != nil {
				return err
			}
			replayPtr.GroupID = cfg.GroupID
			replayPtr.Segment = info.SegmentID
			replayPtr.Offset = recordEnd(info)
			if st.Term > replayPtr.AppliedTerm {
				replayPtr.AppliedTerm = st.Term
			}
			if st.Commit > replayPtr.Committed {
				replayPtr.Committed = st.Commit
			}
		case wal.RecordTypeRaftSnapshot:
			gid, snap, err := decodeRaftSnapshot(payload)
			if err != nil {
				return err
			}
			if gid != cfg.GroupID || myraft.IsEmptySnap(snap) {
				return nil
			}
			if err := ws.mem.ApplySnapshot(snap); err != nil {
				return err
			}
			meta := snap.Metadata
			replayPtr.GroupID = cfg.GroupID
			replayPtr.Segment = info.SegmentID
			replayPtr.Offset = recordEnd(info)
			replayPtr.SnapshotIndex = meta.Index
			replayPtr.SnapshotTerm = meta.Term
			if meta.Index > 0 {
				replayPtr.AppliedIndex = meta.Index
				replayPtr.AppliedTerm = meta.Term
			}
		default:
			return nil
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if isPointerAhead(replayPtr, ws.pointer) {
		if err := ws.updatePointer(replayPtr); err != nil {
			return nil, err
		}
	}

	return ws, nil
}

// Append persists raft entries via WAL.
func (ws *walStorage) Append(entries []myraft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	payload, err := encodeRaftEntries(ws.groupID, entries)
	if err != nil {
		return err
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()

	infos, err := ws.wal.AppendRecords(wal.Record{
		Type:    wal.RecordTypeRaftEntry,
		Payload: payload,
	})
	if err != nil {
		return err
	}
	if len(infos) != 1 {
		return fmt.Errorf("raftstore: expected single entry record, got %d", len(infos))
	}
	if err := ws.mem.Append(entries); err != nil {
		return err
	}
	last := entries[len(entries)-1]
	ptr := ws.pointer
	ptr.GroupID = ws.groupID
	ptr.Segment = infos[0].SegmentID
	ptr.Offset = recordEnd(infos[0])
	ptr.AppliedIndex = last.Index
	ptr.AppliedTerm = last.Term
	return ws.updatePointer(ptr)
}

// ApplySnapshot persists and applies a raft snapshot.
func (ws *walStorage) ApplySnapshot(snap myraft.Snapshot) error {
	if myraft.IsEmptySnap(snap) {
		return nil
	}
	payload, err := encodeRaftSnapshot(ws.groupID, snap)
	if err != nil {
		return err
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()

	infos, err := ws.wal.AppendRecords(wal.Record{
		Type:    wal.RecordTypeRaftSnapshot,
		Payload: payload,
	})
	if err != nil {
		return err
	}
	if len(infos) != 1 {
		return fmt.Errorf("raftstore: expected single snapshot record, got %d", len(infos))
	}
	if err := ws.mem.ApplySnapshot(snap); err != nil {
		return err
	}
	meta := snap.Metadata
	ptr := ws.pointer
	ptr.GroupID = ws.groupID
	ptr.Segment = infos[0].SegmentID
	ptr.Offset = recordEnd(infos[0])
	ptr.SnapshotIndex = meta.Index
	ptr.SnapshotTerm = meta.Term
	if meta.Index > 0 {
		ptr.AppliedIndex = meta.Index
		ptr.AppliedTerm = meta.Term
	}
	return ws.updatePointer(ptr)
}

// SetHardState persists the raft hard state.
func (ws *walStorage) SetHardState(st myraft.HardState) error {
	if myraft.IsEmptyHardState(st) {
		ws.mu.Lock()
		defer ws.mu.Unlock()
		return ws.mem.SetHardState(st)
	}

	payload, err := encodeRaftHardState(ws.groupID, st)
	if err != nil {
		return err
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()

	infos, err := ws.wal.AppendRecords(wal.Record{
		Type:    wal.RecordTypeRaftState,
		Payload: payload,
	})
	if err != nil {
		return err
	}
	if len(infos) != 1 {
		return fmt.Errorf("raftstore: expected single hard state record, got %d", len(infos))
	}
	if err := ws.mem.SetHardState(st); err != nil {
		return err
	}
	ptr := ws.pointer
	ptr.GroupID = ws.groupID
	ptr.Segment = infos[0].SegmentID
	ptr.Offset = recordEnd(infos[0])
	if st.Commit > ptr.Committed {
		ptr.Committed = st.Commit
	}
	if st.Term > ptr.AppliedTerm {
		ptr.AppliedTerm = st.Term
	}
	return ws.updatePointer(ptr)
}

func (ws *walStorage) updatePointer(ptr manifest.RaftLogPointer) error {
	if ptr.Segment == 0 {
		return nil
	}
	ptr.GroupID = ws.groupID
	if ws.pointer == ptr {
		return nil
	}
	if shouldSkipManifestUpdate() {
		// Simulate a crash after WAL append but before the manifest pointer
		// advances. The in-memory pointer intentionally stays stale so that
		// recovery logic must replay WAL records to catch up.
		return nil
	}
	if ws.manifest != nil {
		if err := ws.manifest.LogRaftPointer(ptr); err != nil {
			return err
		}
	}
	ws.pointer = ptr
	return nil
}

// Delegated Storage interface methods.
func (ws *walStorage) InitialState() (myraft.HardState, myraft.ConfState, error) {
	return ws.mem.InitialState()
}

func (ws *walStorage) Entries(lo, hi, maxSize uint64) ([]myraft.Entry, error) {
	return ws.mem.Entries(lo, hi, maxSize)
}

func (ws *walStorage) Term(i uint64) (uint64, error) {
	return ws.mem.Term(i)
}

func (ws *walStorage) LastIndex() (uint64, error) {
	return ws.mem.LastIndex()
}

func (ws *walStorage) FirstIndex() (uint64, error) {
	return ws.mem.FirstIndex()
}

func (ws *walStorage) Snapshot() (myraft.Snapshot, error) {
	return ws.mem.Snapshot()
}

// Helpers

func recordEnd(info wal.EntryInfo) uint64 {
	return uint64(info.Offset) + uint64(info.Length) + walRecordOverhead
}

func isPointerAhead(newPtr, oldPtr manifest.RaftLogPointer) bool {
	if newPtr.Segment == 0 {
		return false
	}
	if oldPtr.Segment == 0 {
		return true
	}
	if newPtr.Segment != oldPtr.Segment {
		return newPtr.Segment > oldPtr.Segment
	}
	return newPtr.Offset > oldPtr.Offset
}

func encodeRaftEntries(groupID uint64, entries []myraft.Entry) ([]byte, error) {
	var buf bytes.Buffer
	writeUvarint(&buf, groupID)
	writeUvarint(&buf, uint64(len(entries)))
	for _, entry := range entries {
		data, err := entry.Marshal()
		if err != nil {
			return nil, err
		}
		writeUvarint(&buf, uint64(len(data)))
		if _, err := buf.Write(data); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeRaftEntries(data []byte) (uint64, []myraft.Entry, error) {
	idx := 0
	groupID, err := readUvarint(data, &idx)
	if err != nil {
		return 0, nil, err
	}
	count64, err := readUvarint(data, &idx)
	if err != nil {
		return 0, nil, err
	}
	if count64 > uint64(len(data)) {
		return 0, nil, io.ErrUnexpectedEOF
	}
	count := int(count64)
	entries := make([]myraft.Entry, 0, count)
	for i := 0; i < count; i++ {
		size, err := readUvarint(data, &idx)
		if err != nil {
			return 0, nil, err
		}
		if idx+int(size) > len(data) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		var entry myraft.Entry
		if err := entry.Unmarshal(data[idx : idx+int(size)]); err != nil {
			return 0, nil, err
		}
		idx += int(size)
		entries = append(entries, entry)
	}
	return groupID, entries, nil
}

func encodeRaftHardState(groupID uint64, st myraft.HardState) ([]byte, error) {
	data, err := st.Marshal()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	writeUvarint(&buf, groupID)
	writeUvarint(&buf, uint64(len(data)))
	if _, err := buf.Write(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeRaftHardState(data []byte) (uint64, myraft.HardState, error) {
	var st myraft.HardState
	idx := 0
	groupID, err := readUvarint(data, &idx)
	if err != nil {
		return 0, st, err
	}
	size, err := readUvarint(data, &idx)
	if err != nil {
		return 0, st, err
	}
	if idx+int(size) > len(data) {
		return 0, st, io.ErrUnexpectedEOF
	}
	if err := st.Unmarshal(data[idx : idx+int(size)]); err != nil {
		return 0, st, err
	}
	return groupID, st, nil
}

func encodeRaftSnapshot(groupID uint64, snap myraft.Snapshot) ([]byte, error) {
	pbSnap := raftpb.Snapshot(snap)
	data, err := pbSnap.Marshal()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	writeUvarint(&buf, groupID)
	writeUvarint(&buf, uint64(len(data)))
	if _, err := buf.Write(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeRaftSnapshot(data []byte) (uint64, myraft.Snapshot, error) {
	var snap myraft.Snapshot
	idx := 0
	groupID, err := readUvarint(data, &idx)
	if err != nil {
		return 0, snap, err
	}
	size, err := readUvarint(data, &idx)
	if err != nil {
		return 0, snap, err
	}
	if idx+int(size) > len(data) {
		return 0, snap, io.ErrUnexpectedEOF
	}
	if err := (*raftpb.Snapshot)(&snap).Unmarshal(data[idx : idx+int(size)]); err != nil {
		return 0, snap, err
	}
	return groupID, snap, nil
}

func writeUvarint(buf *bytes.Buffer, value uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], value)
	buf.Write(tmp[:n])
}

func readUvarint(data []byte, idx *int) (uint64, error) {
	if *idx >= len(data) {
		return 0, io.ErrUnexpectedEOF
	}
	val, n := binary.Uvarint(data[*idx:])
	if n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	*idx += n
	return val, nil
}
