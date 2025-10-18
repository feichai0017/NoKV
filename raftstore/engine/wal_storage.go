package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/failpoints"
	"github.com/feichai0017/NoKV/wal"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
)

const walRecordOverhead = 8 // length (4 bytes) + checksum (4 bytes)

// WALStorageConfig configures WAL-backed raft storage.
type WALStorageConfig struct {
	GroupID  uint64
	WAL      *wal.Manager
	Manifest *manifest.Manager
}

type entrySpan struct {
	firstIndex uint64
	lastIndex  uint64
	segmentID  uint32
}

// WALStorage implements the PeerStorage interface using the shared WAL manager
// plus manifest metadata. It mirrors the storage layout used in TinyKV/TiKV,
// tracking segments so WAL GC can coordinate across raft groups.
type WALStorage struct {
	mu         sync.Mutex
	groupID    uint64
	wal        *wal.Manager
	manifest   *manifest.Manager
	mem        *myraft.MemoryStorage
	pointer    manifest.RaftLogPointer
	entrySpans []entrySpan
}

// OpenWALStorage constructs a WAL-backed raft storage.
func OpenWALStorage(cfg WALStorageConfig) (*WALStorage, error) {
	if cfg.GroupID == 0 {
		return nil, fmt.Errorf("raftstore: wal storage requires group id")
	}
	if cfg.WAL == nil {
		return nil, fmt.Errorf("raftstore: wal storage requires WAL manager")
	}
	ws := &WALStorage{
		groupID:    cfg.GroupID,
		wal:        cfg.WAL,
		manifest:   cfg.Manifest,
		mem:        myraft.NewMemoryStorage(),
		entrySpans: make([]entrySpan, 0, 16),
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
			ws.recordEntrySpan(info.SegmentID, entries)
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
			replayPtr.TruncatedIndex = meta.Index
			replayPtr.TruncatedTerm = meta.Term
			if meta.Index > 0 {
				if seg, ok := ws.segmentForIndex(meta.Index); ok {
					replayPtr.SegmentIndex = uint64(seg)
				} else {
					replayPtr.SegmentIndex = uint64(info.SegmentID)
				}
				ws.pruneEntrySpans(meta.Index)
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

	if ws.pointer.TruncatedIndex > 0 {
		ws.pruneEntrySpans(ws.pointer.TruncatedIndex)
	}

	return ws, nil
}

// Append persists raft entries via WAL.
func (ws *WALStorage) Append(entries []myraft.Entry) error {
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
	ws.recordEntrySpan(infos[0].SegmentID, entries)
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
func (ws *WALStorage) ApplySnapshot(snap myraft.Snapshot) error {
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
	ptr.TruncatedIndex = meta.Index
	ptr.TruncatedTerm = meta.Term
	var truncSegment uint32
	if meta.Index > 0 {
		if seg, ok := ws.segmentForIndex(meta.Index); ok {
			truncSegment = seg
		} else {
			truncSegment = infos[0].SegmentID
		}
	}
	ptr.SegmentIndex = uint64(truncSegment)
	if err := ws.updatePointer(ptr); err != nil {
		return err
	}
	ws.pruneEntrySpans(meta.Index)
	if ws.manifest != nil && ws.pointer == ptr {
		if err := ws.manifest.LogRaftTruncate(ws.groupID, meta.Index, meta.Term, truncSegment); err != nil {
			return err
		}
	}
	return nil
}

// SetHardState persists the raft hard state.
func (ws *WALStorage) SetHardState(st myraft.HardState) error {
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

// MaybeCompact retains only the newest portion of the WAL, mirroring TinyKV's
// behaviour of compacting applied raft log entries.
func (ws *WALStorage) MaybeCompact(applied, retain uint64) error {
	if retain == 0 || applied == 0 || applied <= retain {
		return nil
	}
	target := applied - retain
	if target <= ws.pointer.TruncatedIndex {
		return nil
	}
	return ws.compactTo(target)
}

// Delegated Storage interface methods.
func (ws *WALStorage) InitialState() (myraft.HardState, myraft.ConfState, error) {
	return ws.mem.InitialState()
}

func (ws *WALStorage) Entries(lo, hi, maxSize uint64) ([]myraft.Entry, error) {
	return ws.mem.Entries(lo, hi, maxSize)
}

func (ws *WALStorage) Term(i uint64) (uint64, error) {
	return ws.mem.Term(i)
}

func (ws *WALStorage) LastIndex() (uint64, error) {
	return ws.mem.LastIndex()
}

func (ws *WALStorage) FirstIndex() (uint64, error) {
	return ws.mem.FirstIndex()
}

func (ws *WALStorage) Snapshot() (myraft.Snapshot, error) {
	return ws.mem.Snapshot()
}

// Internal helpers ----------------------------------------------------------

func (ws *WALStorage) updatePointer(ptr manifest.RaftLogPointer) error {
	if ptr.Segment == 0 {
		return nil
	}
	ptr.GroupID = ws.groupID
	if ws.pointer == ptr {
		return nil
	}
	if failpoints.ShouldSkipManifestUpdate() {
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

func (ws *WALStorage) compactTo(index uint64) error {
	if index == 0 {
		return nil
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if index <= ws.pointer.TruncatedIndex {
		return nil
	}
	term, err := ws.mem.Term(index)
	if err != nil && !errors.Is(err, myraft.ErrCompacted) {
		return err
	}
	if err := ws.mem.Compact(index); err != nil && !errors.Is(err, myraft.ErrCompacted) {
		return err
	}
	var segment uint32
	if seg, ok := ws.segmentForIndex(index); ok {
		segment = seg
	} else if ws.pointer.SegmentIndex > 0 {
		segment = uint32(ws.pointer.SegmentIndex)
	} else {
		segment = ws.pointer.Segment
	}
	ptr := ws.pointer
	ptr.TruncatedIndex = index
	if term != 0 {
		ptr.TruncatedTerm = term
	}
	ptr.SegmentIndex = uint64(segment)
	if err := ws.updatePointer(ptr); err != nil {
		return err
	}
	ws.pruneEntrySpans(index)
	if ws.manifest != nil && ws.pointer == ptr {
		if err := ws.manifest.LogRaftTruncate(ws.groupID, index, ptr.TruncatedTerm, segment); err != nil {
			return err
		}
	}
	return nil
}

func (ws *WALStorage) recordEntrySpan(segment uint32, entries []myraft.Entry) {
	if len(entries) == 0 {
		return
	}
	first := entries[0].Index
	last := entries[len(entries)-1].Index
	if first == 0 || last == 0 {
		return
	}
	trimmed := ws.entrySpans[:0]
	for _, span := range ws.entrySpans {
		if span.lastIndex < first {
			trimmed = append(trimmed, span)
			continue
		}
		if span.firstIndex < first {
			span.lastIndex = first - 1
			trimmed = append(trimmed, span)
		}
		break
	}
	ws.entrySpans = append(trimmed, entrySpan{
		firstIndex: first,
		lastIndex:  last,
		segmentID:  segment,
	})
}

func (ws *WALStorage) segmentForIndex(index uint64) (uint32, bool) {
	if index == 0 {
		return 0, false
	}
	for _, span := range ws.entrySpans {
		if index < span.firstIndex {
			break
		}
		if index <= span.lastIndex {
			return span.segmentID, true
		}
	}
	return 0, false
}

func (ws *WALStorage) pruneEntrySpans(index uint64) {
	if index == 0 {
		return
	}
	n := 0
	for _, span := range ws.entrySpans {
		if span.lastIndex <= index {
			continue
		}
		if span.firstIndex <= index {
			span.firstIndex = index + 1
			if span.firstIndex > span.lastIndex {
				continue
			}
		}
		ws.entrySpans[n] = span
		n++
	}
	ws.entrySpans = ws.entrySpans[:n]
}

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
