package localmeta

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/vfs"
	"google.golang.org/protobuf/proto"
)

const (
	// ReplicaStateFileName is the durable local replica catalog file used by one
	// store for restart recovery.
	ReplicaStateFileName = "replica-local-state.pb"
	// RaftProgressFileName is the durable local raft progress file used by one
	// store for WAL/apply recovery.
	RaftProgressFileName = "raft-progress.pb"
)

type diskState struct {
	Regions      map[uint64]RegionMeta
	RaftPointers map[uint64]RaftLogPointer
}

// Store persists store-local region metadata used only for local recovery.
// It is not cluster authority and must not be treated as routing truth.
type Store struct {
	fs      vfs.FS
	workdir string

	mu    sync.RWMutex
	state diskState
}

// WorkDir returns the local metadata directory backing this store.
func (s *Store) WorkDir() string {
	if s == nil {
		return ""
	}
	return s.workdir
}

// OpenLocalStore opens the local raftstore metadata store in workdir.
func OpenLocalStore(workdir string, fs vfs.FS) (*Store, error) {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return nil, fmt.Errorf("raftstore/localmeta: workdir is required")
	}
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(workdir, 0o755); err != nil {
		return nil, err
	}
	state, err := loadState(fs, workdir)
	if err != nil {
		return nil, err
	}
	return &Store{
		fs:      fs,
		workdir: workdir,
		state:   state,
	}, nil
}

// Snapshot returns a deep copy of the local peer catalog.
func (s *Store) Snapshot() map[uint64]RegionMeta {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CloneRegionMetas(s.state.Regions)
}

// RaftPointer returns the last persisted local WAL pointer for one raft group.
func (s *Store) RaftPointer(groupID uint64) (RaftLogPointer, bool) {
	if s == nil || groupID == 0 {
		return RaftLogPointer{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ptr, ok := s.state.RaftPointers[groupID]
	return ptr, ok
}

// RaftPointerSnapshot returns a copy of all persisted local WAL pointers.
func (s *Store) RaftPointerSnapshot() map[uint64]RaftLogPointer {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CloneRaftPointers(s.state.RaftPointers)
}

// Empty reports whether the local peer catalog is empty.
func (s *Store) Empty() bool {
	if s == nil {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.state.Regions) == 0
}

// SaveRegion persists one local region metadata update.
func (s *Store) SaveRegion(meta RegionMeta) error {
	if s == nil {
		return nil
	}
	if meta.ID == 0 {
		return fmt.Errorf("raftstore/localmeta: region id is zero")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Regions == nil {
		s.state.Regions = make(map[uint64]RegionMeta)
	}
	s.state.Regions[meta.ID] = CloneRegionMeta(meta)
	return s.persistLocked()
}

// DeleteRegion removes one region metadata entry from the local catalog.
func (s *Store) DeleteRegion(regionID uint64) error {
	if s == nil || regionID == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state.Regions, regionID)
	return s.persistLocked()
}

// SaveRaftPointer persists the local WAL checkpoint for one raft group.
func (s *Store) SaveRaftPointer(ptr RaftLogPointer) error {
	if s == nil {
		return nil
	}
	if ptr.GroupID == 0 {
		return fmt.Errorf("raftstore/localmeta: raft pointer group id is zero")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.RaftPointers == nil {
		s.state.RaftPointers = make(map[uint64]RaftLogPointer)
	}
	s.state.RaftPointers[ptr.GroupID] = ptr
	return s.persistLocked()
}

// Close releases resources associated with the metadata store.
func (s *Store) Close() error {
	return nil
}

func loadState(fs vfs.FS, workdir string) (diskState, error) {
	regions, err := loadReplicaCatalog(fs, workdir)
	if err != nil {
		return diskState{}, err
	}
	progress, err := loadRaftProgressCatalog(fs, workdir)
	if err != nil {
		return diskState{}, err
	}
	return diskState{
		Regions:      regions,
		RaftPointers: progress,
	}, nil
}

func (s *Store) persistLocked() error {
	if err := persistReplicaCatalog(s.fs, s.workdir, s.state.Regions); err != nil {
		return err
	}
	if err := persistRaftProgressCatalog(s.fs, s.workdir, s.state.RaftPointers); err != nil {
		return err
	}
	return nil
}

func loadReplicaCatalog(fs vfs.FS, workdir string) (map[uint64]RegionMeta, error) {
	path := filepath.Join(workdir, ReplicaStateFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[uint64]RegionMeta), nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return make(map[uint64]RegionMeta), nil
	}
	var pbCatalog metapb.ReplicaLocalCatalog
	if err := proto.Unmarshal(data, &pbCatalog); err != nil {
		return nil, err
	}
	out := make(map[uint64]RegionMeta, len(pbCatalog.Regions))
	for _, item := range pbCatalog.Regions {
		if item == nil || item.RegionId == 0 {
			continue
		}
		out[item.RegionId] = regionMetaFromPB(item)
	}
	return out, nil
}

func loadRaftProgressCatalog(fs vfs.FS, workdir string) (map[uint64]RaftLogPointer, error) {
	path := filepath.Join(workdir, RaftProgressFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[uint64]RaftLogPointer), nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return make(map[uint64]RaftLogPointer), nil
	}
	var pbCatalog metapb.RaftProgressCatalog
	if err := proto.Unmarshal(data, &pbCatalog); err != nil {
		return nil, err
	}
	out := make(map[uint64]RaftLogPointer, len(pbCatalog.Entries))
	for _, item := range pbCatalog.Entries {
		if item == nil || item.GroupId == 0 {
			continue
		}
		out[item.GroupId] = raftProgressFromPB(item)
	}
	return out, nil
}

func persistReplicaCatalog(fs vfs.FS, workdir string, regions map[uint64]RegionMeta) error {
	keys := sortedRegionIDs(regions)
	pbCatalog := &metapb.ReplicaLocalCatalog{
		Regions: make([]*metapb.LocalRegionMeta, 0, len(keys)),
	}
	for _, id := range keys {
		pbCatalog.Regions = append(pbCatalog.Regions, regionMetaToPB(regions[id]))
	}
	payload, err := proto.Marshal(pbCatalog)
	if err != nil {
		return err
	}
	return persistProtoFile(fs, workdir, ReplicaStateFileName, payload)
}

func persistRaftProgressCatalog(fs vfs.FS, workdir string, progress map[uint64]RaftLogPointer) error {
	keys := sortedRaftGroupIDs(progress)
	pbCatalog := &metapb.RaftProgressCatalog{
		Entries: make([]*metapb.RaftProgress, 0, len(keys)),
	}
	for _, id := range keys {
		pbCatalog.Entries = append(pbCatalog.Entries, raftProgressToPB(progress[id]))
	}
	payload, err := proto.Marshal(pbCatalog)
	if err != nil {
		return err
	}
	return persistProtoFile(fs, workdir, RaftProgressFileName, payload)
}

func persistProtoFile(fs vfs.FS, workdir, name string, payload []byte) error {
	path := filepath.Join(workdir, name)
	tmp := path + ".tmp"
	f, err := fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(payload); err != nil {
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

func regionMetaToPB(meta RegionMeta) *metapb.LocalRegionMeta {
	out := &metapb.LocalRegionMeta{
		RegionId: meta.ID,
		StartKey: append([]byte(nil), meta.StartKey...),
		EndKey:   append([]byte(nil), meta.EndKey...),
		Epoch: &metapb.RegionEpoch{
			Version:     meta.Epoch.Version,
			ConfVersion: meta.Epoch.ConfVersion,
		},
		State: regionStateToPB(meta.State),
	}
	if len(meta.Peers) > 0 {
		out.Peers = make([]*metapb.RegionPeer, 0, len(meta.Peers))
		for _, peer := range meta.Peers {
			out.Peers = append(out.Peers, &metapb.RegionPeer{
				StoreId: peer.StoreID,
				PeerId:  peer.PeerID,
			})
		}
	}
	return out
}

func regionMetaFromPB(meta *metapb.LocalRegionMeta) RegionMeta {
	if meta == nil {
		return RegionMeta{}
	}
	out := RegionMeta{
		ID:       meta.RegionId,
		StartKey: append([]byte(nil), meta.StartKey...),
		EndKey:   append([]byte(nil), meta.EndKey...),
		State:    regionStateFromPB(meta.State),
	}
	if meta.Epoch != nil {
		out.Epoch = RegionEpoch{
			Version:     meta.Epoch.Version,
			ConfVersion: meta.Epoch.ConfVersion,
		}
	}
	if len(meta.Peers) > 0 {
		out.Peers = make([]PeerMeta, 0, len(meta.Peers))
		for _, peer := range meta.Peers {
			if peer == nil {
				continue
			}
			out.Peers = append(out.Peers, PeerMeta{
				StoreID: peer.StoreId,
				PeerID:  peer.PeerId,
			})
		}
	}
	return out
}

func raftProgressToPB(ptr RaftLogPointer) *metapb.RaftProgress {
	return &metapb.RaftProgress{
		GroupId:         ptr.GroupID,
		Segment:         ptr.Segment,
		Offset:          ptr.Offset,
		AppliedIndex:    ptr.AppliedIndex,
		AppliedTerm:     ptr.AppliedTerm,
		Committed:       ptr.Committed,
		SnapshotIndex:   ptr.SnapshotIndex,
		SnapshotTerm:    ptr.SnapshotTerm,
		TruncatedIndex:  ptr.TruncatedIndex,
		TruncatedTerm:   ptr.TruncatedTerm,
		SegmentIndex:    ptr.SegmentIndex,
		TruncatedOffset: ptr.TruncatedOffset,
	}
}

func raftProgressFromPB(ptr *metapb.RaftProgress) RaftLogPointer {
	if ptr == nil {
		return RaftLogPointer{}
	}
	return RaftLogPointer{
		GroupID:         ptr.GroupId,
		Segment:         ptr.Segment,
		Offset:          ptr.Offset,
		AppliedIndex:    ptr.AppliedIndex,
		AppliedTerm:     ptr.AppliedTerm,
		Committed:       ptr.Committed,
		SnapshotIndex:   ptr.SnapshotIndex,
		SnapshotTerm:    ptr.SnapshotTerm,
		TruncatedIndex:  ptr.TruncatedIndex,
		TruncatedTerm:   ptr.TruncatedTerm,
		SegmentIndex:    ptr.SegmentIndex,
		TruncatedOffset: ptr.TruncatedOffset,
	}
}

func regionStateToPB(state RegionState) metapb.RegionReplicaState {
	switch state {
	case RegionStateNew:
		return metapb.RegionReplicaState_REGION_REPLICA_STATE_NEW
	case RegionStateRunning:
		return metapb.RegionReplicaState_REGION_REPLICA_STATE_RUNNING
	case RegionStateRemoving:
		return metapb.RegionReplicaState_REGION_REPLICA_STATE_REMOVING
	case RegionStateTombstone:
		return metapb.RegionReplicaState_REGION_REPLICA_STATE_TOMBSTONE
	default:
		return metapb.RegionReplicaState_REGION_REPLICA_STATE_UNSPECIFIED
	}
}

func regionStateFromPB(state metapb.RegionReplicaState) RegionState {
	switch state {
	case metapb.RegionReplicaState_REGION_REPLICA_STATE_NEW:
		return RegionStateNew
	case metapb.RegionReplicaState_REGION_REPLICA_STATE_RUNNING:
		return RegionStateRunning
	case metapb.RegionReplicaState_REGION_REPLICA_STATE_REMOVING:
		return RegionStateRemoving
	case metapb.RegionReplicaState_REGION_REPLICA_STATE_TOMBSTONE:
		return RegionStateTombstone
	default:
		return RegionStateNew
	}
}

func sortedRegionIDs(regions map[uint64]RegionMeta) []uint64 {
	keys := make([]uint64, 0, len(regions))
	for id := range regions {
		keys = append(keys, id)
	}
	slices.Sort(keys)
	return keys
}

func sortedRaftGroupIDs(progress map[uint64]RaftLogPointer) []uint64 {
	keys := make([]uint64, 0, len(progress))
	for id := range progress {
		keys = append(keys, id)
	}
	slices.Sort(keys)
	return keys
}
