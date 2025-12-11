package storage

import (
	"io"
)

// WAL defines the write-ahead log surface the DB relies on.
type WAL interface {
	AppendRecords(records ...Record) ([]EntryInfo, error)
	Replay(fn func(info EntryInfo, payload []byte) error) error
	RemoveSegment(id uint32) error
	Sync() error
	Close() error
	Metrics() WALMetrics
	SegmentMetrics() map[uint32]WALRecordMetrics
}

// Record wraps a typed WAL payload.
type Record struct {
	Type    RecordType
	Payload []byte
}

// RecordType identifies the kind of payload stored in the WAL.
type RecordType uint8

// EntryInfo describes an entry written to WAL.
type EntryInfo struct {
	SegmentID uint32
	Offset    int64
	Length    uint32
	Type      RecordType
}

// WALMetrics captures runtime information about WAL manager state.
type WALMetrics interface{}

// WALRecordMetrics summarises counts per record type.
type WALRecordMetrics interface{}

// Manifest defines the metadata journal surface used by LSM/raft/vlog.
type Manifest interface {
	LogEdits(edits ...Edit) error
	LogValueLogHead(fid uint32, offset uint64) error
	LogValueLogDelete(fid uint32) error
	LogValueLogUpdate(meta ValueLogMeta) error
	LogRaftPointer(ptr RaftLogPointer) error
	LogRaftTruncate(groupID uint64, truncatedIdx, truncatedTerm, truncatedOffset uint64) error
	ValueLogHead() ValueLogMeta
	ValueLogStatus() map[uint32]ValueLogMeta
	RaftPointerSnapshot() map[uint64]RaftLogPointer
	RaftPointer(groupID uint64) (RaftLogPointer, bool)
	Current() Version
	Close() error
}

// Edit represents a single manifest operation.
type Edit struct {
	Type      EditType
	File      *FileMeta
	LogSeg    uint32
	LogOffset uint64
	ValueLog  *ValueLogMeta
	Raft      *RaftLogPointer
	Region    *RegionEdit
}

// EditType enumerates manifest op kinds.
type EditType uint8

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
	FileID uint32
	Offset uint64
	Valid  bool
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

// RegionEdit represents an update or deletion.
type RegionEdit struct {
	Meta   RegionMeta
	Delete bool
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

// RegionEpoch tracks metadata versioning.
type RegionEpoch struct {
	Version     uint64
	ConfVersion uint64
}

// PeerMeta describes a peer replica for a region.
type PeerMeta struct {
	StoreID uint64
	PeerID  uint64
}

// RegionState enumerates region lifecycle states.
type RegionState uint8

// Version represents current manifest state.
type Version struct {
	Levels       map[int][]FileMeta
	LogSegment   uint32
	LogOffset    uint64
	ValueLogs    map[uint32]ValueLogMeta
	ValueLogHead ValueLogMeta
	RaftPointers map[uint64]RaftLogPointer
	Regions      map[uint64]RegionMeta
}

// ValueLog defines the value-log interface used by the DB.
type ValueLog interface {
	WriteRequests(reqs []*Request) error
	Metrics() ValueLogMetrics
	Head() ValuePtr
}

// ValuePtr describes a value log pointer.
type ValuePtr interface {
	IsZero() bool
}

// Request represents a write batch the vlog can accept.
type Request interface {
	Entries() [][]byte
}

// ValueLogMetrics summarises vlog backlog stats.
type ValueLogMetrics interface{}

// LSM defines the interface exposed by the LSM layer.
type LSM interface {
	NewIterator(opt *IteratorOptions) Iterator
	FlushPending() int64
	FlushMetrics() FlushMetrics
	CompactionStats() (int64, float64)
	CompactionDurations() (float64, float64, uint64)
	LevelMetrics() []LevelMetrics
	CurrentVersion() Version
	ManifestManager() Manifest
	LogValueLogHead(fid uint32, offset uint64) error
	LogValueLogDelete(fid uint32) error
	LogValueLogUpdate(meta *ValueLogMeta) error
	MaxVersion() uint64
	SetDiscardStatsCh(ch *chan map[uint32]int64)
	SetThrottleCallback(fn func(bool))
	Close() error
}

// IteratorOptions mirrors utils.Options for now.
type IteratorOptions struct {
	IsAsc bool
}

// Iterator defines the minimal iterator interface DB requires.
type Iterator interface {
	Rewind()
	Valid() bool
	Item() IteratorItem
	Next()
	Close()
}

// IteratorItem wraps access to the current entry.
type IteratorItem interface {
	Entry() IteratorEntry
}

// IteratorEntry describes an entry returned by the iterator.
type IteratorEntry interface {
	Key() []byte
	Value() []byte
}

// FlushMetrics captures flush manager stats.
type FlushMetrics interface {
	Pending() int64
}

// LevelMetrics aggregates stats per level.
type LevelMetrics interface {
	GetLevel() int
	GetTableCount() int
	GetSizeBytes() int64
	GetValueBytes() int64
	GetStaleBytes() int64
	GetValueDensity() float64
}

// MVCCReader abstracts read paths.
type MVCCReader interface {
	GetLock(key []byte) (*Lock, error)
	GetValue(key []byte, version uint64) ([]byte, error)
}

// Lock mirrors mvcc.Lock.
type Lock struct {
	Primary []byte
	Key     []byte
	Ts      uint64
	TTL     uint64
	Kind    uint8
}

// MVCC defines transaction helpers.
type MVCC interface {
	Prewrite(db any, latches any, req any) []error
	Commit(db any, latches any, req any) error
	BatchRollback(db any, latches any, req any) error
	ResolveLock(db any, latches any, req any) (int, error)
	CheckTxnStatus(db any, latches any, req any) any
	NewReader(db any) MVCCReader
}

// LatchManager abstracts the latch manager used by MVCC paths.
type LatchManager interface{}

// Closer represents a shutdown handle.
type Closer interface {
	Close()
	Done()
	Add(delta int)
}

// DirLock guards exclusive workdir access.
type DirLock interface {
	Release() error
}

// File describes the subset of os.File used by WAL/manifest to avoid import leakage.
type File interface {
	io.Closer
}
