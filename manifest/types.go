package manifest

// FileMeta describes an SST file.
type FileMeta struct {
	Level     int
	FileID    uint64
	Size      uint64
	Smallest  []byte
	Largest   []byte
	CreatedAt uint64
	ValueSize uint64
	Ingest    bool
}

// ValueLogMeta describes a value log segment.
type ValueLogMeta struct {
	Bucket uint32
	FileID uint32
	Offset uint64
	Valid  bool
}

// ValueLogID identifies a value log segment within a bucket.
type ValueLogID struct {
	Bucket uint32
	FileID uint32
}

// PeerMeta describes a peer replica for a region.
type PeerMeta struct {
	StoreID uint64
	PeerID  uint64
}

// RegionState enumerates region lifecycle states.
type RegionState uint8

const (
	RegionStateNew RegionState = iota
	RegionStateRunning
	RegionStateRemoving
	RegionStateTombstone
)

// RegionEpoch tracks metadata versioning.
type RegionEpoch struct {
	Version     uint64
	ConfVersion uint64
}

// RegionMeta captures region key range and peer membership.
type RegionMeta struct {
	ID       uint64
	StartKey []byte
	EndKey   []byte
	Epoch    RegionEpoch
	Peers    []PeerMeta
	State    RegionState
}

// RegionEdit represents an update or deletion.
type RegionEdit struct {
	Meta   RegionMeta
	Delete bool
}

// CloneRegionMeta returns a deep copy of the provided RegionMeta.
func CloneRegionMeta(meta RegionMeta) RegionMeta {
	cp := meta
	if meta.StartKey != nil {
		cp.StartKey = append([]byte(nil), meta.StartKey...)
	}
	if meta.EndKey != nil {
		cp.EndKey = append([]byte(nil), meta.EndKey...)
	}
	if len(meta.Peers) > 0 {
		cp.Peers = append([]PeerMeta(nil), meta.Peers...)
	}
	return cp
}

// CloneRegionMetas deep copies a map of RegionMeta keyed by region ID.
func CloneRegionMetas(metaMap map[uint64]RegionMeta) map[uint64]RegionMeta {
	if len(metaMap) == 0 {
		return nil
	}
	out := make(map[uint64]RegionMeta, len(metaMap))
	for id, meta := range metaMap {
		out[id] = CloneRegionMeta(meta)
	}
	return out
}

// CloneRegionMetaPtr returns a deep copy of the provided RegionMeta pointer,
// or nil when meta is nil.
func CloneRegionMetaPtr(meta *RegionMeta) *RegionMeta {
	if meta == nil {
		return nil
	}
	clone := CloneRegionMeta(*meta)
	return &clone
}

// RaftLogPointer tracks WAL progress for a raft group.
type RaftLogPointer struct {
	GroupID         uint64
	Segment         uint32
	Offset          uint64
	AppliedIndex    uint64
	AppliedTerm     uint64
	Committed       uint64
	SnapshotIndex   uint64
	SnapshotTerm    uint64
	TruncatedIndex  uint64
	TruncatedTerm   uint64
	SegmentIndex    uint64
	TruncatedOffset uint64
}

// Version represents current manifest state.
type Version struct {
	Levels       map[int][]FileMeta
	LogSegment   uint32
	LogOffset    uint64
	ValueLogs    map[ValueLogID]ValueLogMeta
	ValueLogHead map[uint32]ValueLogMeta
	RaftPointers map[uint64]RaftLogPointer
	Regions      map[uint64]RegionMeta
}

// Edit operation types.
type EditType uint8

const (
	EditAddFile EditType = iota
	EditDeleteFile
	EditLogPointer
	EditValueLogHead
	EditDeleteValueLog
	EditUpdateValueLog
	EditRaftPointer
	EditRegion
)

// Edit describes a single metadata operation.
type Edit struct {
	Type      EditType
	File      *FileMeta
	LogSeg    uint32
	LogOffset uint64
	ValueLog  *ValueLogMeta
	Raft      *RaftLogPointer
	Region    *RegionEdit
}
