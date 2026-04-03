package local

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	metacodec "github.com/feichai0017/NoKV/meta/codec"
	rootpkg "github.com/feichai0017/NoKV/meta/root"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/feichai0017/NoKV/vfs"
	"google.golang.org/protobuf/proto"
)

const (
	CheckpointFileName = "metadata-root-checkpoint.pb"
	LogFileName        = "metadata-root.log"
	recordHeaderSize   = 24
	maxRetainedRecords = 64
)

type record struct {
	cursor rootpkg.Cursor
	event  rootpkg.Event
}

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

func loadCheckpoint(fs vfs.FS, workdir string) (rootpkg.Snapshot, int64, error) {
	path := filepath.Join(workdir, CheckpointFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return rootpkg.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, 0, nil
		}
		return rootpkg.Snapshot{}, 0, err
	}
	if len(data) == 0 {
		return rootpkg.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}, 0, nil
	}
	var pbCheckpoint metapb.RootCheckpoint
	if err := proto.Unmarshal(data, &pbCheckpoint); err != nil {
		return rootpkg.Snapshot{}, 0, err
	}
	if pbCheckpoint.State == nil && len(pbCheckpoint.Descriptors) == 0 {
		var pbState metapb.RootState
		if err := proto.Unmarshal(data, &pbState); err == nil {
			return rootpkg.Snapshot{
				State:       metacodec.RootStateFromProto(&pbState),
				Descriptors: make(map[uint64]descriptor.Descriptor),
			}, 0, nil
		}
	}
	snapshot, logOffset := metacodec.RootSnapshotFromProto(&pbCheckpoint)
	if snapshot.Descriptors == nil {
		snapshot.Descriptors = make(map[uint64]descriptor.Descriptor)
	}
	return snapshot, int64(logOffset), nil
}

func loadLog(fs vfs.FS, workdir string, offset int64) ([]record, error) {
	path := filepath.Join(workdir, LogFileName)
	f, err := fs.OpenHandle(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	var out []record
	for {
		rec, ok, err := readRecord(f)
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		out = append(out, rec)
	}
}

func persistCheckpoint(fs vfs.FS, workdir string, snapshot rootpkg.Snapshot, logOffset uint64) error {
	payload, err := proto.Marshal(metacodec.RootSnapshotToProto(snapshot, logOffset))
	if err != nil {
		return err
	}
	path := filepath.Join(workdir, CheckpointFileName)
	tmp := path + ".tmp"
	f, err := fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := writeAll(f, payload); err != nil {
		_ = f.Close()
		_ = fs.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = fs.Remove(tmp)
		return err
	}
	if err := fs.Rename(tmp, path); err != nil {
		return err
	}
	return vfs.SyncDir(fs, workdir)
}

func rewriteLog(fs vfs.FS, workdir string, records []record) error {
	path := filepath.Join(workdir, LogFileName)
	tmp := path + ".tmp"
	f, err := fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, rec := range records {
		if err := writeRecord(f, rec.cursor, rec.event); err != nil {
			_ = f.Close()
			_ = fs.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = fs.Remove(tmp)
		return err
	}
	if err := fs.Rename(tmp, path); err != nil {
		return err
	}
	return vfs.SyncDir(fs, workdir)
}

func writeRecord(w io.Writer, cursor rootpkg.Cursor, event rootpkg.Event) error {
	payload, err := proto.Marshal(metacodec.RootEventToProto(event))
	if err != nil {
		return err
	}
	hdr := make([]byte, recordHeaderSize)
	binary.LittleEndian.PutUint64(hdr[0:8], cursor.Term)
	binary.LittleEndian.PutUint64(hdr[8:16], cursor.Index)
	binary.LittleEndian.PutUint32(hdr[16:20], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[20:24], crc32.ChecksumIEEE(payload))
	if err := writeAll(w, hdr); err != nil {
		return err
	}
	return writeAll(w, payload)
}

func readRecord(r io.Reader) (record, bool, error) {
	hdr := make([]byte, recordHeaderSize)
	n, err := io.ReadFull(r, hdr)
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return record{}, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return record{}, false, nil
		}
		return record{}, false, err
	}
	payloadLen := binary.LittleEndian.Uint32(hdr[16:20])
	expectedCRC := binary.LittleEndian.Uint32(hdr[20:24])
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return record{}, false, nil
		}
		return record{}, false, err
	}
	if crc32.ChecksumIEEE(payload) != expectedCRC {
		return record{}, false, fmt.Errorf("meta/root/local: root log checksum mismatch")
	}
	var pbEvent metapb.RootEvent
	if err := proto.Unmarshal(payload, &pbEvent); err != nil {
		return record{}, false, err
	}
	return record{
		cursor: rootpkg.Cursor{Term: binary.LittleEndian.Uint64(hdr[0:8]), Index: binary.LittleEndian.Uint64(hdr[8:16])},
		event:  metacodec.RootEventFromProto(&pbEvent),
	}, true, nil
}

func applyEvent(state *rootpkg.State, cursor rootpkg.Cursor, event rootpkg.Event) {
	if state == nil {
		return
	}
	switch event.Kind {
	case rootpkg.EventKindStoreJoined, rootpkg.EventKindStoreLeft, rootpkg.EventKindStoreMarkedDraining:
		state.MembershipEpoch++
	case rootpkg.EventKindRegionBootstrap,
		rootpkg.EventKindRegionDescriptorPublished,
		rootpkg.EventKindRegionTombstoned,
		rootpkg.EventKindRegionSplitRequested,
		rootpkg.EventKindRegionSplitCommitted,
		rootpkg.EventKindRegionMerged,
		rootpkg.EventKindPeerAdded,
		rootpkg.EventKindPeerRemoved:
		state.ClusterEpoch++
	case rootpkg.EventKindPlacementPolicyChanged:
		if event.PlacementPolicy != nil && event.PlacementPolicy.Version > state.PolicyVersion {
			state.PolicyVersion = event.PlacementPolicy.Version
		} else {
			state.PolicyVersion++
		}
	}
	state.LastCommitted = cursor
}

func nextCursor(prev rootpkg.Cursor) rootpkg.Cursor {
	term := prev.Term
	if term == 0 {
		term = 1
	}
	return rootpkg.Cursor{Term: term, Index: prev.Index + 1}
}

func snapshotEvents(descs map[uint64]descriptor.Descriptor) []rootpkg.Event {
	if len(descs) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(descs))
	for id := range descs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	events := make([]rootpkg.Event, 0, len(ids))
	for _, id := range ids {
		events = append(events, rootpkg.RegionDescriptorPublished(descs[id]))
	}
	return events
}

func cloneDescriptors(in map[uint64]descriptor.Descriptor) map[uint64]descriptor.Descriptor {
	if len(in) == 0 {
		return make(map[uint64]descriptor.Descriptor)
	}
	out := make(map[uint64]descriptor.Descriptor, len(in))
	for id, desc := range in {
		out[id] = desc.Clone()
	}
	return out
}

func cloneRecords(in []record) []record {
	if len(in) == 0 {
		return nil
	}
	out := make([]record, 0, len(in))
	for _, rec := range in {
		out = append(out, record{
			cursor: rec.cursor,
			event:  cloneEvent(rec.event),
		})
	}
	return out
}

func currentLogSize(fs vfs.FS, workdir string) (int64, error) {
	info, err := fs.Stat(filepath.Join(workdir, LogFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

func fileSize(f vfs.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func after(a, b rootpkg.Cursor) bool {
	if a.Term != b.Term {
		return a.Term > b.Term
	}
	return a.Index > b.Index
}

func previousCursor(in rootpkg.Cursor) rootpkg.Cursor {
	if in.Index <= 1 {
		return rootpkg.Cursor{}
	}
	return rootpkg.Cursor{Term: in.Term, Index: in.Index - 1}
}

func retainedFloor(records []record, fallback rootpkg.Cursor) rootpkg.Cursor {
	if len(records) == 0 {
		return fallback
	}
	return previousCursor(records[0].cursor)
}

func cloneState(in rootpkg.State) rootpkg.State { return in }

func cloneEvent(in rootpkg.Event) rootpkg.Event {
	out := in
	if in.StoreMembership != nil {
		cp := *in.StoreMembership
		out.StoreMembership = &cp
	}
	if in.RegionDescriptor != nil {
		cp := *in.RegionDescriptor
		cp.Descriptor = in.RegionDescriptor.Descriptor.Clone()
		out.RegionDescriptor = &cp
	}
	if in.RegionRemoval != nil {
		cp := *in.RegionRemoval
		out.RegionRemoval = &cp
	}
	if in.RangeSplit != nil {
		cp := *in.RangeSplit
		if in.RangeSplit.SplitKey != nil {
			cp.SplitKey = append([]byte(nil), in.RangeSplit.SplitKey...)
		}
		cp.Left = in.RangeSplit.Left.Clone()
		cp.Right = in.RangeSplit.Right.Clone()
		out.RangeSplit = &cp
	}
	if in.RangeMerge != nil {
		cp := *in.RangeMerge
		cp.Merged = in.RangeMerge.Merged.Clone()
		out.RangeMerge = &cp
	}
	if in.PeerChange != nil {
		cp := *in.PeerChange
		cp.Region = in.PeerChange.Region.Clone()
		out.PeerChange = &cp
	}
	if in.LeaderTransfer != nil {
		cp := *in.LeaderTransfer
		out.LeaderTransfer = &cp
	}
	if in.PlacementPolicy != nil {
		cp := *in.PlacementPolicy
		out.PlacementPolicy = &cp
	}
	return out
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
