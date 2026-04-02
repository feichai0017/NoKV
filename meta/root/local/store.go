package local

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

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

	mu      sync.RWMutex
	state   rootpkg.State
	records []record
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
	state, err := loadCheckpoint(fs, workdir)
	if err != nil {
		return nil, err
	}
	records, err := loadLog(fs, workdir)
	if err != nil {
		return nil, err
	}
	for _, rec := range records {
		if after(rec.cursor, state.LastCommitted) {
			applyEvent(&state, rec.cursor, rec.event)
		}
	}
	return &Store{fs: fs, workdir: workdir, state: state, records: records}, nil
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

// ReadSince returns all events after cursor together with the current tail cursor.
func (s *Store) ReadSince(cursor rootpkg.Cursor) ([]rootpkg.Event, rootpkg.Cursor, error) {
	if s == nil {
		return nil, rootpkg.Cursor{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	records := make([]record, 0, len(events))
	for _, evt := range events {
		next = nextCursor(state.LastCommitted)
		if err := writeRecord(f, next, evt); err != nil {
			_ = f.Close()
			return rootpkg.CommitInfo{}, err
		}
		applyEvent(&state, next, evt)
		records = append(records, record{cursor: next, event: cloneEvent(evt)})
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return rootpkg.CommitInfo{}, err
	}
	if err := f.Close(); err != nil {
		return rootpkg.CommitInfo{}, err
	}
	if err := persistCheckpoint(s.fs, s.workdir, state); err != nil {
		return rootpkg.CommitInfo{}, err
	}
	s.state = state
	s.records = append(s.records, records...)
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
	if err := persistCheckpoint(s.fs, s.workdir, state); err != nil {
		return 0, err
	}
	s.state = state
	return *out, nil
}

func loadCheckpoint(fs vfs.FS, workdir string) (rootpkg.State, error) {
	path := filepath.Join(workdir, CheckpointFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return rootpkg.State{}, nil
		}
		return rootpkg.State{}, err
	}
	if len(data) == 0 {
		return rootpkg.State{}, nil
	}
	var pbState metapb.RootState
	if err := proto.Unmarshal(data, &pbState); err != nil {
		return rootpkg.State{}, err
	}
	return stateFromPB(&pbState), nil
}

func loadLog(fs vfs.FS, workdir string) ([]record, error) {
	path := filepath.Join(workdir, LogFileName)
	f, err := fs.OpenHandle(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
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

func persistCheckpoint(fs vfs.FS, workdir string, state rootpkg.State) error {
	payload, err := proto.Marshal(stateToPB(state))
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

func writeRecord(w io.Writer, cursor rootpkg.Cursor, event rootpkg.Event) error {
	payload, err := proto.Marshal(eventToPB(event))
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
		event:  eventFromPB(&pbEvent),
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

func after(a, b rootpkg.Cursor) bool {
	if a.Term != b.Term {
		return a.Term > b.Term
	}
	return a.Index > b.Index
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

func stateToPB(state rootpkg.State) *metapb.RootState {
	return &metapb.RootState{
		ClusterEpoch:    state.ClusterEpoch,
		MembershipEpoch: state.MembershipEpoch,
		PolicyVersion:   state.PolicyVersion,
		LastCommitted:   &metapb.RootCursor{Term: state.LastCommitted.Term, Index: state.LastCommitted.Index},
		IdFence:         state.IDFence,
		TsoFence:        state.TSOFence,
	}
}

func stateFromPB(pbState *metapb.RootState) rootpkg.State {
	if pbState == nil {
		return rootpkg.State{}
	}
	var cursor rootpkg.Cursor
	if pbState.LastCommitted != nil {
		cursor = rootpkg.Cursor{Term: pbState.LastCommitted.Term, Index: pbState.LastCommitted.Index}
	}
	return rootpkg.State{
		ClusterEpoch:    pbState.ClusterEpoch,
		MembershipEpoch: pbState.MembershipEpoch,
		PolicyVersion:   pbState.PolicyVersion,
		LastCommitted:   cursor,
		IDFence:         pbState.IdFence,
		TSOFence:        pbState.TsoFence,
	}
}

func eventToPB(event rootpkg.Event) *metapb.RootEvent {
	pbEvent := &metapb.RootEvent{Kind: eventKindToPB(event.Kind)}
	switch {
	case event.StoreMembership != nil:
		pbEvent.Payload = &metapb.RootEvent_StoreMembership{StoreMembership: &metapb.RootStoreMembership{StoreId: event.StoreMembership.StoreID, Address: event.StoreMembership.Address}}
	case event.RegionDescriptor != nil:
		pbEvent.Payload = &metapb.RootEvent_RegionDescriptor{
			RegionDescriptor: &metapb.RootRegionDescriptor{Descriptor_: event.RegionDescriptor.Descriptor.ToProto()},
		}
	case event.RegionRemoval != nil:
		pbEvent.Payload = &metapb.RootEvent_RegionRemoval{
			RegionRemoval: &metapb.RootRegionRemoval{RegionId: event.RegionRemoval.RegionID},
		}
	case event.RangeSplit != nil:
		pbEvent.Payload = &metapb.RootEvent_RangeSplit{RangeSplit: &metapb.RootRangeSplit{
			ParentRegionId: event.RangeSplit.ParentRegionID,
			SplitKey:       append([]byte(nil), event.RangeSplit.SplitKey...),
			Left:           event.RangeSplit.Left.ToProto(),
			Right:          event.RangeSplit.Right.ToProto(),
		}}
	case event.RangeMerge != nil:
		pbEvent.Payload = &metapb.RootEvent_RangeMerge{RangeMerge: &metapb.RootRangeMerge{
			LeftRegionId:  event.RangeMerge.LeftRegionID,
			RightRegionId: event.RangeMerge.RightRegionID,
			Merged:        event.RangeMerge.Merged.ToProto(),
		}}
	case event.PeerChange != nil:
		pbEvent.Payload = &metapb.RootEvent_PeerChange{PeerChange: &metapb.RootPeerChange{
			RegionId:   event.PeerChange.RegionID,
			StoreId:    event.PeerChange.StoreID,
			PeerId:     event.PeerChange.PeerID,
			Descriptor_: event.PeerChange.Region.ToProto(),
		}}
	case event.LeaderTransfer != nil:
		pbEvent.Payload = &metapb.RootEvent_LeaderTransfer{LeaderTransfer: &metapb.RootLeaderTransfer{RegionId: event.LeaderTransfer.RegionID, FromPeerId: event.LeaderTransfer.FromPeerID, ToPeerId: event.LeaderTransfer.ToPeerID, TargetStoreId: event.LeaderTransfer.TargetStoreID}}
	case event.PlacementPolicy != nil:
		pbEvent.Payload = &metapb.RootEvent_PlacementPolicy{PlacementPolicy: &metapb.RootPlacementPolicy{Version: event.PlacementPolicy.Version, Name: event.PlacementPolicy.Name}}
	}
	return pbEvent
}

func eventFromPB(pbEvent *metapb.RootEvent) rootpkg.Event {
	if pbEvent == nil {
		return rootpkg.Event{}
	}
	event := rootpkg.Event{Kind: eventKindFromPB(pbEvent.Kind)}
	if body := pbEvent.GetStoreMembership(); body != nil {
		event.StoreMembership = &rootpkg.StoreMembership{StoreID: body.StoreId, Address: body.Address}
	}
	if body := pbEvent.GetRegionDescriptor(); body != nil {
		event.RegionDescriptor = &rootpkg.RegionDescriptorRecord{Descriptor: descriptor.FromProto(body.GetDescriptor_())}
	}
	if body := pbEvent.GetRegionRemoval(); body != nil {
		event.RegionRemoval = &rootpkg.RegionRemoval{RegionID: body.RegionId}
	}
	if body := pbEvent.GetRangeSplit(); body != nil {
		event.RangeSplit = &rootpkg.RangeSplit{
			ParentRegionID: body.ParentRegionId,
			SplitKey:       append([]byte(nil), body.SplitKey...),
			Left:           descriptor.FromProto(body.Left),
			Right:          descriptor.FromProto(body.Right),
		}
	}
	if body := pbEvent.GetRangeMerge(); body != nil {
		event.RangeMerge = &rootpkg.RangeMerge{
			LeftRegionID:  body.LeftRegionId,
			RightRegionID: body.RightRegionId,
			Merged:        descriptor.FromProto(body.Merged),
		}
	}
	if body := pbEvent.GetPeerChange(); body != nil {
		event.PeerChange = &rootpkg.PeerChange{
			RegionID: body.RegionId,
			StoreID:  body.StoreId,
			PeerID:   body.PeerId,
			Region:   descriptor.FromProto(body.GetDescriptor_()),
		}
	}
	if body := pbEvent.GetLeaderTransfer(); body != nil {
		event.LeaderTransfer = &rootpkg.LeaderTransfer{RegionID: body.RegionId, FromPeerID: body.FromPeerId, ToPeerID: body.ToPeerId, TargetStoreID: body.TargetStoreId}
	}
	if body := pbEvent.GetPlacementPolicy(); body != nil {
		event.PlacementPolicy = &rootpkg.PlacementPolicy{Version: body.Version, Name: body.Name}
	}
	return event
}

func eventKindToPB(kind rootpkg.EventKind) metapb.RootEventKind {
	switch kind {
	case rootpkg.EventKindStoreJoined:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_JOINED
	case rootpkg.EventKindStoreLeft:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_LEFT
	case rootpkg.EventKindStoreMarkedDraining:
		return metapb.RootEventKind_ROOT_EVENT_KIND_STORE_MARKED_DRAINING
	case rootpkg.EventKindRegionBootstrap:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_BOOTSTRAP
	case rootpkg.EventKindRegionDescriptorPublished:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_DESCRIPTOR_PUBLISHED
	case rootpkg.EventKindRegionTombstoned:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_TOMBSTONED
	case rootpkg.EventKindRegionSplitRequested:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_REQUESTED
	case rootpkg.EventKindRegionSplitCommitted:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_COMMITTED
	case rootpkg.EventKindRegionMerged:
		return metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGED
	case rootpkg.EventKindPeerAdded:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDED
	case rootpkg.EventKindPeerRemoved:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVED
	case rootpkg.EventKindLeaderTransferIntent:
		return metapb.RootEventKind_ROOT_EVENT_KIND_LEADER_TRANSFER_INTENT
	case rootpkg.EventKindPlacementPolicyChanged:
		return metapb.RootEventKind_ROOT_EVENT_KIND_PLACEMENT_POLICY_CHANGED
	default:
		return metapb.RootEventKind_ROOT_EVENT_KIND_UNSPECIFIED
	}
}

func eventKindFromPB(kind metapb.RootEventKind) rootpkg.EventKind {
	switch kind {
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_JOINED:
		return rootpkg.EventKindStoreJoined
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_LEFT:
		return rootpkg.EventKindStoreLeft
	case metapb.RootEventKind_ROOT_EVENT_KIND_STORE_MARKED_DRAINING:
		return rootpkg.EventKindStoreMarkedDraining
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_BOOTSTRAP:
		return rootpkg.EventKindRegionBootstrap
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_DESCRIPTOR_PUBLISHED:
		return rootpkg.EventKindRegionDescriptorPublished
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_TOMBSTONED:
		return rootpkg.EventKindRegionTombstoned
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_REQUESTED:
		return rootpkg.EventKindRegionSplitRequested
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_SPLIT_COMMITTED:
		return rootpkg.EventKindRegionSplitCommitted
	case metapb.RootEventKind_ROOT_EVENT_KIND_REGION_MERGED:
		return rootpkg.EventKindRegionMerged
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_ADDED:
		return rootpkg.EventKindPeerAdded
	case metapb.RootEventKind_ROOT_EVENT_KIND_PEER_REMOVED:
		return rootpkg.EventKindPeerRemoved
	case metapb.RootEventKind_ROOT_EVENT_KIND_LEADER_TRANSFER_INTENT:
		return rootpkg.EventKindLeaderTransferIntent
	case metapb.RootEventKind_ROOT_EVENT_KIND_PLACEMENT_POLICY_CHANGED:
		return rootpkg.EventKindPlacementPolicyChanged
	default:
		return rootpkg.EventKindUnknown
	}
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
